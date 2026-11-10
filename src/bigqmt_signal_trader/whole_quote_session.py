"""Client-side whole-quote subscription session.

Owns the per-process state for ``subscribe_whole_quote``: the local
subscription table, the shared push-channel subscriber thread, and the
keepalive heartbeat thread. One session is shared by every ``subscribe_whole_quote``
call in the process (``BigQmtXtData`` delegates here), so all subscriptions ride
a single push-channel connection and a single heartbeat loop.

The big-QMT whole-quote callback is INCREMENTAL (only changed symbols), so a
subscription does not by itself deliver an initial full snapshot — callers layer
a ``get_full_tick`` prime on top (done in ``BigQmtXtData.subscribe_whole_quote``).
"""

import logging
import threading
import time


def _norm_topic(code_list):
    return ",".join(sorted({str(c).strip().upper() for c in (code_list or []) if str(c or "").strip()}))


class WholeQuoteClientSession(object):
    def __init__(self, rpc_call, push_channel, client_id, heartbeat_interval_seconds=3.0, sub_id_func=None,
                 push_silence_replay_heartbeats=10, push_channel_factory=None,
                 bar_subscribe_batch_size=8, bar_subscribe_timeout_seconds=30.0):
        """保存订阅会话；rpc_call 接受方法及参数，推送通道由本会话启动和停止。

        heartbeat_interval_seconds 为心跳秒数；sub_id_func 生成稳定的订阅编号。
        push_silence_replay_heartbeats 为无推送后重放的心跳轮数。
        push_channel_factory 仅供确认型通道损坏后重建，须保留原订阅者身份。
        bar_subscribe_batch_size 为每次 QMT 调度最多创建的分钟句柄数，范围 1..32。
        bar_subscribe_timeout_seconds 为整次批订阅的总秒数预算。
        """
        self._rpc = rpc_call
        self._channel = push_channel
        self.client_id = str(client_id or "")
        self._heartbeat_interval = float(heartbeat_interval_seconds)
        self._push_silence_replay_heartbeats = int(push_silence_replay_heartbeats)
        self._sub_id_func = sub_id_func
        self._seq = 0
        self._lock = threading.RLock()
        self._subscriptions = {}  # sub_id -> {"topic": str, "callback": fn, "codes": [...]}
        self._started = False
        self._subscriber_active = False
        self._push_channel_used = False
        self._subscribed_topics = frozenset()  # topic set the subscriber covers now
        self._heartbeat_thread = None
        self._heartbeat_stop = threading.Event()
        self._last_push_time = None  # monotonic time of last incoming push
        self._acknowledged_push = getattr(push_channel, "supports_acknowledgements", False) is True
        self._push_channel_factory = push_channel_factory
        self._push_health_error = ""
        self._push_callback_failed = False
        self._push_reconciliation_required = False
        self._push_recoveries = 0
        self._push_health_busy = False
        self._bar_subscribe_batch_size = int(bar_subscribe_batch_size)
        self._bar_subscribe_timeout = float(bar_subscribe_timeout_seconds)
        if not 1 <= self._bar_subscribe_batch_size <= 32 or self._bar_subscribe_timeout <= 0:
            raise ValueError("native minute control batch must be 1..32 and timeout positive")

    # -- subscription lifecycle ---------------------------------------------
    def subscribe_whole_quote(self, code_list, callback=None):
        codes = [str(c) for c in (code_list or []) if str(c or "").strip()]
        if not codes:
            raise ValueError("code_list is required")
        with self._lock:
            sub_id = self._next_sub_id()
        result = self._rpc(
            "subscribe_whole_quote",
            {"client_id": self.client_id, "sub_id": sub_id, "codes": codes},
        ) or {}
        topic = str(result.get("topic") or result.get("combo_key") or _norm_topic(codes))
        with self._lock:
            self._subscriptions[sub_id] = {"topic": topic, "callback": callback, "codes": codes}
            self._sync_subscriber_locked()
        return sub_id

    def unsubscribe_quote(self, sub_id):
        """释放指定订阅，分钟订阅使用对应原生控制 RPC。"""
        with self._lock:
            entry = self._subscriptions.pop(sub_id, None)
        if entry is None:
            return 0
        try:
            self._rpc(entry.get("unsubscribe_method", "unsubscribe_whole_quote"),
                      {"client_id": self.client_id, "sub_id": sub_id})
        finally:
            with self._lock:
                self._sync_subscriber_locked()
        return 0

    def subscribe_bar(self, stock_code, callback=None):
        """先监听固定主题再申请原生分钟句柄，避免同步首个回调丢失。"""
        from .native_bar_subscription import NativeBarDelivery, bar_topic

        with self._lock:
            sub_id = self._next_sub_id()
            delivery = NativeBarDelivery(stock_code, callback)
            entry = {"topic": bar_topic(self.client_id, sub_id), "callback": delivery,
                     "codes": [stock_code], "subscribe_method": "subscribe_bar",
                     "unsubscribe_method": "unsubscribe_bar", "keepalive_method": "bar_keepalive"}
            self._subscriptions[sub_id] = entry
            try:
                self._sync_subscriber_locked()
            except Exception:
                self._subscriptions.pop(sub_id, None)
                raise
        try:
            result = self._rpc("subscribe_bar", {
                "client_id": self.client_id, "sub_id": sub_id, "codes": [stock_code], "period": "1m"})
            if not isinstance(result, dict) or result.get("native_minute_push") is not True:
                raise RuntimeError("server did not confirm native minute push")
            delivery.confirm(result.get("generation"))
            entry["server_generation"] = result.get("generation")
            return sub_id
        except Exception:
            # 请求超时也可能已经申请成功；尽力撤销，最终仍由服务端租约清理。
            try:
                self.unsubscribe_quote(sub_id)
            except Exception:
                pass
            raise

    def unsubscribe_bars(self):
        """显式释放全部原生分钟订阅，保留 Tick 会话与其订阅。"""
        with self._lock:
            sub_ids = [sid for sid, entry in self._subscriptions.items()
                       if entry.get("subscribe_method") == "subscribe_bar"]
        self._unsubscribe_bar_ids(sub_ids)
        return len(sub_ids)

    def unsubscribe_bar_handles(self, sub_ids):
        """仅批量释放参数中属于原生分钟的句柄，并返回已处理编号集合。"""
        with self._lock:
            native_ids = [sid for sid in dict.fromkeys(sub_ids)
                          if self._subscriptions.get(sid, {}).get("subscribe_method") == "subscribe_bar"]
        self._unsubscribe_bar_ids(native_ids)
        return set(native_ids)

    def _unsubscribe_bar_ids(self, sub_ids):
        """先撤销本地路由，再按 256 项一次释放远端句柄，失败交由租约兜底。"""
        with self._lock:
            for sub_id in sub_ids:
                self._subscriptions.pop(sub_id, None)
            self._sync_subscriber_locked()
        for offset in range(0, len(sub_ids), 256):
            self._rpc("unsubscribe_bar_many", {"client_id": self.client_id, "sub_ids": sub_ids[offset:offset + 256]})

    def subscribe_bars(self, codes, callback=None, timeout_seconds=None):
        """分短批创建至多 256 个原生订阅，全部批次共用 timeout_seconds 秒预算。"""
        from .native_bar_subscription import NativeBarDelivery, bar_topic

        codes = list(dict.fromkeys(codes))
        if not codes or len(codes) > 256 or any(not isinstance(code, str) or not code.strip() for code in codes):
            raise ValueError("native minute batch requires 1..256 instrument codes")
        entries, handles = {}, {}
        with self._lock:
            for code in codes:
                sub_id = self._next_sub_id()
                entry = {"topic": bar_topic(self.client_id, sub_id), "callback": NativeBarDelivery(code, callback),
                         "codes": [code], "subscribe_method": "subscribe_bar",
                         "unsubscribe_method": "unsubscribe_bar", "keepalive_method": "bar_keepalive"}
                entries[sub_id], handles[code] = entry, sub_id
            self._subscriptions.update(entries)
            try:
                self._sync_subscriber_locked()
            except Exception:
                for sub_id in entries:
                    self._subscriptions.pop(sub_id, None)
                raise
        try:
            self._subscribe_bar_batches(list(entries.items()), timeout_seconds)
            return handles
        except Exception:
            try:
                self._unsubscribe_bar_ids(list(entries))
            except Exception:
                pass
            raise

    def _subscribe_bar_batches(self, items, timeout_seconds=None):
        """每批默认八项后归还 QMT 调度；原生 C++ 单次调用仍不能被抢占。"""
        budget = self._bar_subscribe_timeout if timeout_seconds is None else float(timeout_seconds)
        deadline = time.monotonic() + budget
        size = self._bar_subscribe_batch_size
        for offset in range(0, len(items), size):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("native minute subscription deadline exceeded")
            batch = items[offset:offset + size]
            results = self._rpc("subscribe_bar_many", {
                "client_id": self.client_id,
                "subscriptions": [{"sub_id": sid, "code": entry["codes"][0]} for sid, entry in batch],
                "timeout_seconds": min(remaining, 30.0),
            })
            for sub_id, entry in batch:
                result = (results or {}).get(str(sub_id))
                if not isinstance(result, dict) or result.get("native_minute_push") is not True:
                    raise RuntimeError("server did not confirm complete native minute batch")
                entry["callback"].confirm(result.get("generation"))
                with self._lock:
                    current = self._subscriptions.get(sub_id)
                    if current is not None:
                        current["server_generation"] = result.get("generation")
            if time.monotonic() >= deadline:
                raise TimeoutError("native minute subscription deadline exceeded")

    def has_subscription(self, sub_id):
        with self._lock:
            return sub_id in self._subscriptions

    def replay_subscriptions(self):
        """重放有效订阅，分钟服务端换代时明确报告缺口而不伪造补齐。"""
        with self._lock:
            items = [(sid, dict(entry)) for sid, entry in self._subscriptions.items()]
        bars = []
        for sub_id, entry in items:
            if entry.get("subscribe_method") == "subscribe_bar":
                # 首次注册尚在其他控制批中执行，心跳不能同时重放尚未确认的句柄。
                if entry.get("server_generation"):
                    bars.append((sub_id, entry))
                continue
            self._rpc(
                "subscribe_whole_quote",
                {"client_id": self.client_id, "sub_id": sub_id, "codes": entry["codes"]},
            )
        self._subscribe_bar_batches(bars)

    def _keepalive_bars(self, items):
        """每批一次续租，失败逐项通知缺口但不终止唯一心跳线程。"""
        failures = 0
        for offset in range(0, len(items), 256):
            batch = items[offset:offset + 256]
            try:
                result = self._rpc("bar_keepalive_many", {
                    "client_id": self.client_id, "sub_ids": [sid for sid, _entry in batch]})
                missing = set(result["missing"])
            except Exception:
                missing = {str(sid) for sid, _entry in batch}
            for sub_id, entry in batch:
                if str(sub_id) not in missing:
                    continue
                failures += 1
                try:
                    entry["callback"].gap()
                except Exception as error:
                    with self._lock:
                        self._push_callback_failed = True
                        self._push_reconciliation_required = True
                        self._push_health_error = "native minute gap callback failed: %s" % error
        return failures

    # -- heartbeat -------------------------------------------------------------
    def start(self):
        """启动唯一心跳线程；上一代尚未退出时禁止清除其停止信号。"""
        with self._lock:
            if self._started:
                return
            if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
                raise RuntimeError("行情心跳线程尚未停止，不能重启")
            self._heartbeat_stop.clear()
            self._started = True
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop, name="bigqmt-quote-keepalive", daemon=True
            )
            self._heartbeat_thread.start()

    def stop(self):
        """唤醒等待中的心跳并有限等待；超时保留线程句柄，不能报告已经回收。"""
        with self._lock:
            self._started = False
            self._heartbeat_stop.set()
            thread = self._heartbeat_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)
            if thread.is_alive():
                raise RuntimeError("行情心跳线程未能在停止期限内退出")
        with self._lock:
            if thread is self._heartbeat_thread and (thread is None or not thread.is_alive()):
                self._heartbeat_thread = None

    def close(self):
        """关闭本地推送和心跳；服务端注册由既有租约过期机制清理。"""
        with self._lock:
            self._subscriptions.clear()
            channel = self._channel if self._push_channel_used else None
            self._subscriber_active = False
            self._subscribed_topics = frozenset()
        # 推送回调也需要会话锁，不能在持锁时等待其线程退出。
        # 即使退订已经请求过停止，最终关闭仍重试回收，不把超时当作成功。
        try:
            self.stop()
        finally:
            if channel is not None:
                try:
                    channel.stop()
                except Exception:
                    if self._acknowledged_push:
                        raise

    def _heartbeat_loop(self):
        """定期续租及重放订阅，暂时恢复失败时保留重试，不结束心跳线程。"""
        consecutive_failures = 0
        silence_rounds = 0
        prev_last_push = None
        while True:
            with self._lock:
                if not self._started:
                    return
                sub_ids = list(self._subscriptions.keys())
                last_push = self._last_push_time
            if self._acknowledged_push and not self._check_push_health():
                self._heartbeat_stop.wait(self._heartbeat_interval)
                continue
            if not sub_ids:
                self._heartbeat_stop.wait(self._heartbeat_interval)
                continue
            failures = 0
            bars = []
            for sub_id in sub_ids:
                if self._heartbeat_stop.is_set():
                    return
                try:
                    with self._lock:
                        entry = self._subscriptions.get(sub_id)
                    if entry is None:
                        continue
                    if entry.get("subscribe_method") == "subscribe_bar":
                        if entry.get("server_generation"):
                            bars.append((sub_id, entry))
                        continue
                    self._rpc("quote_keepalive", {"client_id": self.client_id, "sub_id": sub_id})
                except Exception:
                    failures += 1
            failures += self._keepalive_bars(bars)
            if failures:
                consecutive_failures += 1
                # 分钟租约失效可能来自终端重启；不能等所有续租先恢复才尝试重建。
                self._replay_for_heartbeat()
            elif consecutive_failures >= 3:
                # Server is back after a restart window: replay subscriptions so
                # the restarted server re-creates the big-QMT subscriptions (its
                # state is gone). Idempotent on the server, so replays are safe.
                if self._replay_for_heartbeat():
                    consecutive_failures = 0
            else:
                consecutive_failures = 0
            # Push-silence detection: a server restart can survive with keepalive
            # succeeding (the redis request queue buffers during the restart
            # window) while the subscription table was reset, so pushes stop.
            # Replay when no push arrived for several heartbeat rounds (also
            # covers the case where the very first prime push never arrived).
            if last_push != prev_last_push:
                silence_rounds = 0  # a push arrived since the last round
            else:
                silence_rounds += 1
            prev_last_push = last_push
            if silence_rounds >= self._push_silence_replay_heartbeats:
                if self._replay_for_heartbeat():
                    silence_rounds = 0
            self._heartbeat_stop.wait(self._heartbeat_interval)

    def _replay_for_heartbeat(self):
        """重放暂时失败时保留重试计数，不能让整个心跳线程退出并失去全部订阅。"""
        try:
            self.replay_subscriptions()
            return True
        except Exception:
            logging.getLogger(__name__).debug("行情订阅重放失败，下轮心跳重试", exc_info=True)
            return False

    # -- push routing ------------------------------------------------------------
    def _on_push(self, topic, data):
        import time

        now = time.monotonic()
        with self._lock:
            self._last_push_time = now
            callbacks = [
                entry["callback"]
                for entry in self._subscriptions.values()
                if entry["topic"] == topic and entry["callback"] is not None
            ]
        for callback in callbacks:
            try:
                callback(data)
            except Exception:
                if self._acknowledged_push:
                    # 回调可能已有副作用；失败留给通道保留游标，不自行无限重放。
                    raise
                pass

    def push_health(self):
        """返回消费端状态；通道重建不等同于丢失区间已经完成对账。"""
        with self._lock:
            return {
                "healthy": not self._push_health_error and not self._push_reconciliation_required,
                "last_error": self._push_health_error, "callback_failed": self._push_callback_failed,
                "reconciliation_required": self._push_reconciliation_required,
                "recoveries": self._push_recoveries,
                "health_check_busy": self._push_health_busy,
            }

    def _check_push_health(self):
        """确认型订阅检查真实接收线程；传输故障重建，回调故障保留而不自动重复。"""
        with self._lock:
            if not self._subscriber_active:
                return True
            if self._push_callback_failed:
                return False
            channel = self._channel
        try:
            channel.check_health()
            with self._lock:
                if self._push_health_busy:
                    self._push_health_busy = False
                    self._push_health_error = ""
            return True
        except Exception as exc:
            with self._lock:
                self._push_health_error = "%s: %s" % (type(exc).__name__, exc)
                if getattr(exc, "retryable", False):
                    # 发布与确认争抢锁时只暂缓心跳，不销毁完整的未确认游标。
                    self._push_health_busy = True
                    return False
                self._push_health_busy = False
                self._push_callback_failed = bool(getattr(exc, "callback_failed", False))
                self._push_reconciliation_required = True
                if self._push_callback_failed:
                    return False
            if self._push_channel_factory is None:
                return False
            try:
                channel.stop()
                with self._lock:
                    self._channel = self._push_channel_factory()
                    self._subscriber_active = False
                    self._push_channel_used = False
                    self._subscribed_topics = frozenset()
                    self._sync_subscriber_locked()
                self.replay_subscriptions()
                with self._lock:
                    self._push_recoveries += 1
                    self._push_health_error = ""
                return True
            except Exception as recovery_error:
                with self._lock:
                    self._push_health_error = "%s: %s" % (type(recovery_error).__name__, recovery_error)
                return False

    def _sync_subscriber_locked(self):
        """同步组合主题；确认型通道原地更改过滤器，保留未确认共享游标。

        调用方持有会话锁；全部退订正常停止，同实例显式新订阅才允许更改保留指纹。
        """
        topics = sorted({entry["topic"] for entry in self._subscriptions.values()})
        active = frozenset(topics)
        if active == self._subscribed_topics:
            return
        if not active:
            # 空集合相等已在上面返回；到这里必然是最后一个主题退出，直接幂等关闭。
            try:
                self._channel.stop()
            except Exception:
                pass
            self._subscriber_active = False
            self._subscribed_topics = active
            return
        update_topics = getattr(self._channel, "update_topics", None)
        if self._acknowledged_push and callable(update_topics):
            if self._subscriber_active:
                update_topics(topics)
                self._subscribed_topics = active
                return
            if self._push_channel_used:
                # 同实例正常退订后用户主动订阅新组合，与故障恢复的新实例明确分开。
                update_topics(topics)
        if self._subscriber_active:
            try:
                self._channel.stop()
            except Exception:
                pass
        self._channel.start_subscriber(topics, self._on_push)
        self._subscriber_active = True
        self._push_channel_used = True
        self._subscribed_topics = active

    def _next_sub_id(self):
        if self._sub_id_func is not None:
            return self._sub_id_func()
        self._seq += 1
        return self._seq
