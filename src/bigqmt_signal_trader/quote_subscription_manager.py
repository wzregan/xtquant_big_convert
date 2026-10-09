"""Reference-counted quote subscription manager (server side).

One big-QMT subscription group is shared by every client that asked for the same
(normalized) code combination. Normal instruments use
``ContextInfo.subscribe_whole_quote``. Explicit .SHO/.SZO option contracts use
``ContextInfo.subscribe_quote(result_type="list")`` because some full Big-QMT
versions do not push those contracts through the whole-quote API. The group is
only created for the first client and only torn down after the last client either
unsubscribes or goes silent (keepalive timeout).

The manager talks to big QMT exclusively through a :class:`QuoteSourceAdapter`;
it never touches ``ContextInfo`` directly so the real-environment wiring (method
names / handle shape) stays isolated to the adapter.

Threading: ``subscribe``/``unsubscribe``/``keepalive`` run on the RPC thread,
``reap_expired`` on the scheduler thread and ``on_push`` on big QMT's quote
thread. Shared state is guarded by one re-entrant lock; calls out to the quote
source and to the push publisher happen OUTSIDE the lock so a slow/blocking
publish never stalls quote-thread state, and no callback can deadlock.
"""

import copy
import sys
import threading
import time
import uuid
from collections import deque

from .code_utils import normalize_stock_code
from .quote_push_channel import QuotePushChannel
from .quote_utils import is_option_code, latest_quote_batch
from .transports.base import TransportError


def normalize_subscription_code(code):
    """One code as big QMT wants it -- and futures keep their case (#95).

    This used to be a plain ``.upper()``, which is right for the exchange
    tokens (SH / SZ / IF ...) and wrong for every futures contract, because
    those symbols are lowercase: ``cu2610.SF`` went to QMT as ``CU2610.SF``,
    a contract it does not have, so the subscription produced no pushes at
    all. The reporter saw exactly that -- ``CF701.ZF`` ticking every 250ms
    while ``cu2610.SF`` delivered one frame and then nothing. The one frame
    was the initial snapshot, which takes the case-preserving get_full_tick
    path; the subscription behind it was already dead.

    normalize_stock_code keeps the caller's symbol verbatim for the
    case-sensitive suffixes (.SF .DF .IF .ZF .INE .GF -- issue #58) but
    rejects a bare exchange token, so tokens are handled here instead.
    """
    text = str(code or "").strip()
    if not text:
        return ""
    if "." not in text:
        return text.upper()          # exchange token: SH / SZ / BJ / IF / SF ...
    try:
        return normalize_stock_code(text)
    except Exception:
        return text.upper()


def combo_key(code_list):
    """Normalize a code list into an order-independent combination key.

    Strips whitespace, drops empties and duplicates, sorts. Exchange tokens are
    uppercased, so ``["SH","SZ"]``, ``["sz","sh"]`` and ``["SH","SH","SZ"]``
    all map to ``"SH,SZ"`` and share one big-QMT subscription.

    Contract codes keep their case, because big QMT does: ``cu2610.SF`` and
    ``CU2610.SF`` are not the same subscription there -- only one of them
    exists -- so collapsing them into one key would hand the wrong string to
    the exchange (#95).
    """
    normalized = {normalize_subscription_code(code) for code in (code_list or [])
                  if str(code or "").strip()}
    return ",".join(sorted(normalized - {""}))


class QuoteSourceAdapter(object):
    """Big-QMT whole-quote source. ContextInfo-backed implementation lives in the
    server runtime; tests substitute a fake. ``subscribe`` must return a handle
    usable by ``unsubscribe``."""

    def subscribe(self, codes, on_push):
        raise NotImplementedError

    def unsubscribe(self, handle):
        raise NotImplementedError


class ContextInfoQuoteSource(QuoteSourceAdapter):
    """Real big-QMT source backed by the strategy's ``ContextInfo``.

    Verified against the real environment: ``ContextInfo.subscribe_whole_quote``
    returns an int subscription id and pushes ``{code: tick}`` batches. Explicit
    option contracts are subscribed one-by-one with ``subscribe_quote`` and the
    safe ``result_type='list'`` wrapper; its column arrays are collapsed to the
    newest tick before publishing. ``unsubscribe_quote`` cancels either handle.
    """

    def __init__(self, context_info):
        self._context = context_info

    def subscribe(self, codes, on_push):
        codes = list(codes)
        option_codes = [code for code in codes if is_option_code(code)]
        whole_codes = [code for code in codes if not is_option_code(code)]
        handles = []

        def checked_handle(sub_id, method, method_codes):
            try:
                value = int(sub_id)
            except (TypeError, ValueError):
                value = -1
            if value <= 0:
                raise RuntimeError(
                    "ContextInfo.%s failed for codes=%s" % (method, list(method_codes))
                )
            return value

        try:
            if whole_codes:
                sub_id = self._context.subscribe_whole_quote(
                    whole_codes, callback=on_push
                )
                handles.append(checked_handle(
                    sub_id, "subscribe_whole_quote", whole_codes
                ))

            def on_option_push(data):
                batch = latest_quote_batch(data)
                if batch:
                    on_push(batch)

            for code in option_codes:
                sub_id = self._context.subscribe_quote(
                    code, "tick", "none", "list", on_option_push
                )
                handles.append(checked_handle(
                    sub_id, "subscribe_quote", [code]
                ))
        except Exception:
            for handle in handles:
                try:
                    self._context.unsubscribe_quote(handle)
                except Exception:
                    pass
            raise

        if not handles:
            raise RuntimeError("no quote codes supplied")
        return handles[0] if len(handles) == 1 else tuple(handles)

    def unsubscribe(self, handle):
        handles = handle if isinstance(handle, (list, tuple, set)) else [handle]
        for sub_id in handles:
            try:
                self._context.unsubscribe_quote(sub_id)
            except Exception:
                pass


class _Combo(object):
    __slots__ = ("key", "codes", "handle", "topic", "clients")

    def __init__(self, key, codes, handle, topic):
        self.key = key
        self.codes = codes
        self.handle = handle
        self.topic = topic
        # (client_id, sub_id) -> last_seen. Sub-id granularity: one client may
        # hold several subscriptions to the same combination, and each one keeps
        # the shared big-QMT subscription alive independently.
        self.clients = {}  # (client_id, sub_id) -> last_seen timestamp


class QuoteSubscriptionManager(object):
    def __init__(self, source, heartbeat_timeout_seconds=30.0, time_func=None, on_push_publisher=None, push_endpoint=""):
        self._source = source
        self._heartbeat_timeout = float(heartbeat_timeout_seconds)
        self._now = time_func or _monotonic
        # Optional callable(topic, data) invoked when big QMT pushes a tick batch.
        # Wired to the QuotePushChannel in a later stage; None keeps dispatch inert.
        self._on_push_publisher = on_push_publisher
        # Advertised to clients in subscribe responses so a zmq subscriber knows
        # where to connect (redis subscribers derive the channel locally instead).
        self._push_endpoint = str(push_endpoint or "")
        self._lock = threading.RLock()
        self._combos = {}            # combo_key -> _Combo
        self._sub_index = {}         # (client_id, sub_id) -> combo_key

    # -- subscription lifecycle ---------------------------------------------
    def subscribe(self, client_id, sub_id, code_list):
        """Register (client_id, sub_id) against its combination; create the shared
        big-QMT subscription on first use. Idempotent for replayed subscribes."""
        client_id = str(client_id or "")
        sub_id = str(sub_id or "")
        key = combo_key(code_list)
        now = self._now()

        with self._lock:
            combo = self._combos.get(key)
            if combo is None:
                codes = sorted({normalize_subscription_code(c) for c in (code_list or [])
                                if str(c or "").strip()} - {""})
                # source.subscribe registers the on_push callback with big QMT; it
                # does not call back into the manager, so it is safe under the lock.
                handle = self._source.subscribe(codes, self._make_on_push(key))
                combo = _Combo(key, codes, handle, key)
                self._combos[key] = combo

            combo.clients[(client_id, sub_id)] = now
            self._sub_index[(client_id, sub_id)] = key
            return {"combo_key": key, "topic": combo.topic, "push_endpoint": self._push_endpoint}

    def unsubscribe(self, client_id, sub_id):
        """Drop (client_id, sub_id); tear the big-QMT subscription down when the
        last subscription of the combination leaves. Unknown sub_ids are a no-op."""
        client_id = str(client_id or "")
        sub_id = str(sub_id or "")
        with self._lock:
            key = self._sub_index.pop((client_id, sub_id), None)
            if key is None:
                return
            handle_to_close = self._remove_subscription_locked(key, client_id, sub_id)
        self._close_source(handle_to_close)

    def keepalive(self, client_id, sub_id):
        """Refresh last_seen for (client_id, sub_id). Unknown sub_ids are a no-op."""
        client_id = str(client_id or "")
        key = self._sub_index.get((client_id, str(sub_id or "")))
        if key is None:
            return
        with self._lock:
            combo = self._combos.get(key)
            if combo is None:
                return
            combo.clients[(client_id, str(sub_id or ""))] = self._now()

    # -- reaper ---------------------------------------------------------------
    def reap_expired(self, now=None):
        """Remove subscriptions silent for longer than the keepalive timeout;
        tear down combos that end up empty. Returns the number reaped."""
        now = self._now() if now is None else now
        reaped = 0
        handles_to_close = []
        with self._lock:
            for key in list(self._combos.keys()):
                combo = self._combos.get(key)
                if combo is None:
                    continue
                for (client_id, sub_id), last_seen in list(combo.clients.items()):
                    if now - last_seen > self._heartbeat_timeout:
                        self._sub_index.pop((client_id, sub_id), None)
                        handle = self._remove_subscription_locked(key, client_id, sub_id)
                        if handle is not None:
                            handles_to_close.append(handle)
                        reaped += 1
        for handle in handles_to_close:
            self._close_source(handle)
        return reaped

    # -- internals -------------------------------------------------------------
    def _make_on_push(self, key):
        def on_push(data):
            publisher = self._on_push_publisher
            if publisher is None:
                return
            with self._lock:
                combo = self._combos.get(key)
                topic = combo.topic if combo is not None else None
            if topic is None:
                return
            # Publish outside the lock: it is network IO and must not stall the
            # quote thread or block reaper/RPC threads waiting on the lock.
            publisher(topic, data)

        return on_push

    def _remove_subscription_locked(self, key, client_id, sub_id):
        """Remove one (client_id, sub_id) from a combo. If the combo has no
        subscriptions left, detach it and return its source handle for the
        caller to close OUTSIDE the lock; else return None. Caller must hold
        the lock."""
        combo = self._combos.get(key)
        if combo is None:
            return None
        combo.clients.pop((client_id, sub_id), None)
        if combo.clients:
            return None
        self._combos.pop(key, None)
        return combo.handle

    def _close_source(self, handle):
        if handle is None:
            return
        try:
            self._source.unsubscribe(handle)
        except Exception:
            pass


def _monotonic():
    import time

    return time.monotonic()


def _retained_payload_bytes(value, seen=None):
    """保守统计快照保留的内置容器内存，seen 用于避免重复对象和循环引用。"""
    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return 0
    seen.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        size += sum(_retained_payload_bytes(key, seen) + _retained_payload_bytes(item, seen)
                    for key, item in value.items())
    elif isinstance(value, (list, tuple, set, frozenset)):
        size += sum(_retained_payload_bytes(item, seen) for item in value)
    return size


class BufferedAcknowledgedPublisher(QuotePushChannel):
    """将原生回调与确认型推送隔开，以独立有界队列保留尚未提交的消息。

    入队成功只表示本进程接收；共享日志提交成功才从队首移除，不代表订阅者已确认。
    本地队列不承诺在进程崩溃后保留，粘滞故障需要显式对账或重启处理。
    """

    supports_acknowledgements = True

    def __init__(self, channel, config=None):
        """包装服务端通道；config 控制每类队列条数、内存及积压门槛。"""
        settings = dict(config or {})
        self._channel = channel
        self._lock = threading.RLock()
        self._queues = {"exec": deque(), "quote": deque()}
        self._bytes = {"exec": 0, "quote": 0}
        self._capacities = {
            "exec": int(settings.get("local_exec_queue_items", 512)),
            "quote": int(settings.get("local_quote_queue_items", 256)),
        }
        self._byte_limits = {
            "exec": int(settings.get("local_exec_queue_bytes", 8 * 1024 * 1024)),
            "quote": int(settings.get("local_quote_queue_bytes", 64 * 1024 * 1024)),
        }
        self._high_water = float(settings.get("local_queue_high_water", 0.75))
        if min(list(self._capacities.values()) + list(self._byte_limits.values())) <= 0:
            raise ValueError("local acknowledged push queue limits must be positive")
        if not 0 < self._high_water <= 1:
            raise ValueError("local_queue_high_water must be in (0, 1]")
        self._closed = False
        self._sticky_error = ""
        self._last_error = ""
        self._backpressure = False
        self._health_busy = False
        self._publishing_failed = False
        self._local_queued = 0
        self._shared_committed = 0
        self._overflow_count = 0
        self._pending_at_stop = 0

    @property
    def publisher_endpoint(self):
        """沿用实际共享日志端点用于诊断。"""
        return self._channel.publisher_endpoint

    def start_publisher(self):
        """启动实际发布端；构造失败仍由上层成套启动流程回收。"""
        self._channel.start_publisher()

    def start_subscriber(self, topics, on_msg):
        """服务端包装器不承担订阅角色。"""
        raise RuntimeError("buffered publisher cannot start a subscriber")

    def publish(self, topic, data):
        """保存事件快照并立即返回；满或关闭时显式失败且不覆盖旧消息。"""
        lane = "exec" if str(topic).startswith("exec:") else "quote"
        try:
            # 配置复制会保留注入对象身份，事件快照必须连可变叶子也独立持有。
            snapshot = copy.deepcopy(data)
            retained_bytes = _retained_payload_bytes(snapshot)
        except Exception as exc:
            with self._lock:
                self._sticky_error = "push snapshot failed: %s: %s" % (type(exc).__name__, exc)
                self._last_error = self._sticky_error
            raise TransportError(self._sticky_error)
        entry = (uuid.uuid4().hex, str(topic), snapshot, retained_bytes)
        with self._lock:
            if self._closed:
                self._sticky_error = "acknowledged publisher is closed"
                raise TransportError(self._sticky_error)
            if (len(self._queues[lane]) >= self._capacities[lane]
                    or self._bytes[lane] + retained_bytes > self._byte_limits[lane]):
                self._overflow_count += 1
                self._sticky_error = "%s local acknowledged push queue is full" % lane
                self._last_error = self._sticky_error
                raise TransportError(self._sticky_error)
            self._queues[lane].append(entry)
            self._bytes[lane] += retained_bytes
            self._local_queued += 1
        return True

    def flush(self, max_items=32, max_wall_seconds=0.01):
        """策略线程按条数及秒数预算提交，交易优先，失败的队首保留同一消息编号。"""
        deadline = time.monotonic() + max(0.0, float(max_wall_seconds))
        committed = 0
        with self._lock:
            if self._closed or self._publishing_failed:
                return 0
        try:
            self._channel.check_health()
        except Exception as exc:
            with self._lock:
                self._last_error = "%s: %s" % (type(exc).__name__, exc)
                if getattr(exc, "retryable", False):
                    # 健康元数据锁繁忙不表示游标丢失，下一轮重查且保留现有队列。
                    self._health_busy = True
                else:
                    self._sticky_error = self._last_error
                    self._publishing_failed = True
            return 0
        with self._lock:
            if self._health_busy:
                self._health_busy = False
                self._last_error = ""
        while committed < max(0, int(max_items)) and time.monotonic() < deadline:
            with self._lock:
                if self._closed:
                    return committed
                lane = "exec" if self._queues["exec"] else "quote"
                if not self._queues[lane]:
                    self._backpressure = False
                    return committed
                entry = self._queues[lane][0]
            try:
                # 消费者确认回报也会短暂持有共享锁；立即尝试会把正常竞争误判成背压。
                # 只使用本轮剩余时间等待，不能让每条消息重新获得完整的刷新预算。
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return committed
                self._channel.publish(entry[1], entry[2], message_id=entry[0],
                                      timeout_seconds=remaining)
            except Exception as exc:
                with self._lock:
                    self._last_error = "%s: %s" % (type(exc).__name__, exc)
                    self._backpressure = True
                    if not getattr(exc, "retryable", False):
                        self._sticky_error = self._last_error
                        self._publishing_failed = True
                return committed
            with self._lock:
                # stop 或并发回调不能让迟到的成功结果删除另一条消息。
                if self._queues[lane] and self._queues[lane][0][0] == entry[0]:
                    self._queues[lane].popleft()
                    self._bytes[lane] -= entry[3]
                    self._shared_committed += 1
                    committed += 1
                    self._backpressure = False
                    self._last_error = ""
        return committed

    def status(self):
        """返回本地接收、共享提交和故障状态，不把入队等同于对端确认。"""
        with self._lock:
            pending = {lane: len(items) for lane, items in self._queues.items()}
            high_water = any(
                pending[lane] >= max(1, self._capacities[lane] * self._high_water)
                or self._bytes[lane] >= self._byte_limits[lane] * self._high_water
                for lane in self._queues
            )
            blocked = bool(self._closed or self._sticky_error or self._backpressure
                           or self._health_busy or high_water)
            return {
                "healthy": not blocked, "new_orders_blocked": blocked,
                "local_pending": pending, "local_pending_bytes": dict(self._bytes),
                "local_queued": self._local_queued, "shared_committed": self._shared_committed,
                "sticky_error": self._sticky_error, "last_error": self._last_error,
                "overflow_count": self._overflow_count, "pending_at_stop": self._pending_at_stop,
                "health_check_busy": self._health_busy,
            }

    def check_health(self):
        """查询实际通道健康；本地不可恢复故障继续明确报错。"""
        state = self.status()
        if state["sticky_error"]:
            raise TransportError(state["sticky_error"])
        return self._channel.check_health()

    def stop(self):
        """限时停止，保留待提交计数；停止不会自动清空或无限发送队列。"""
        with self._lock:
            self._closed = True
            self._pending_at_stop = sum(len(items) for items in self._queues.values())
            if self._pending_at_stop:
                self._sticky_error = "publisher stopped with %d locally queued messages" % self._pending_at_stop
                print("[bigqmt_quote_push] %s; reconciliation required" % self._sticky_error)
        self._channel.stop()


def build_quote_subscription_service(
    context_info, transport_name="redis", account_id="", redis_client=None,
    zmq_bind_address=None, enabled=True, heartbeat_timeout_seconds=30.0,
    time_func=None, rpc_config=None, backend=None,
):
    """组装行情源和后端推送通道，调用方负责启动发布端和清理过期订阅。

    rpc_config 为后端参数；zmq_bind_address 兼容已有推送地址覆盖项。
    heartbeat_timeout_seconds 为客户端心跳过期秒数。
    backend 为服务启动时固定的后端定义，省略时按名称选择。
    """
    if not enabled:
        return None
    from .transports.backend import get_backend

    source = ContextInfoQuoteSource(context_info)
    config = dict(rpc_config or {})
    if redis_client is not None:
        config["redis_client"] = redis_client
    if zmq_bind_address:
        settings = dict(config.get("zmq") or {})
        settings["quote_push_bind_address"] = zmq_bind_address
        config["zmq"] = settings
    selected_backend = backend if backend is not None else get_backend(transport_name)
    channel = selected_backend.build_push_channel(
        "server", config, account_id=account_id,
    )
    try:
        if getattr(selected_backend, "acknowledged_push", False):
            channel = BufferedAcknowledgedPublisher(channel, config.get("shm"))
        manager = QuoteSubscriptionManager(
            source, heartbeat_timeout_seconds=heartbeat_timeout_seconds,
            time_func=time_func, on_push_publisher=channel.publish,
            push_endpoint=channel.publisher_endpoint,
        )
    except Exception:
        # 通道工厂可能已分配原生资源，后续组装失败也要归还所有权。
        try:
            channel.stop()
        except Exception:
            pass
        raise
    return manager, channel


def _default_quote_push_zmq_bind(account_id):
    """保留原有辅助入口，地址规则统一由后端负责。"""
    from .transports.backend import zmq_push_address

    return zmq_push_address("server", account_id=account_id)
