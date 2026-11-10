"""原生分钟订阅的句柄、租约及有界回调转发，不保存行情缓存或补数策略。"""

import copy
import hashlib
import logging
import threading
import time
import uuid
from collections import deque


def bar_topic(client_id, sub_id):
    """由客户端和订阅编号生成固定主题，允许客户端先监听再请求原生订阅。"""
    identity = "%s\0%s" % (client_id, sub_id)
    return "bar:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()


class NativeBarSubscriptions(object):
    """按客户端订阅保存原生句柄，去重和行情恢复由 qmt-grpc 完成。"""

    def __init__(self, context, publisher, timeout_seconds=30.0, now=None,
                 max_items=4096, max_bytes=32 * 1024 * 1024):
        """publisher 仅接收主题和原始数据；容量分别为条数、保留内存字节数。"""
        if timeout_seconds <= 0 or max_items <= 0 or max_bytes <= 0:
            raise ValueError("native minute limits must be positive")
        self._context, self._publisher = context, publisher
        self._timeout, self._now = float(timeout_seconds), now or time.monotonic
        self._max_items, self._max_bytes = int(max_items), int(max_bytes)
        self._lock = threading.RLock()
        self._entries, self._queue = {}, deque()
        self._bytes = 0
        self._wake, self._stop = threading.Event(), threading.Event()
        self._thread = None
        self._closed = False

    @property
    def supported(self):
        """仅声明当前 ContextInfo 确实提供的原生接口。"""
        return (callable(getattr(self._context, "subscribe_quote", None))
                and callable(getattr(self._context, "unsubscribe_quote", None)))

    def subscribe(self, client_id, sub_id, code, period="1m"):
        """在 QMT 策略线程订阅；同一编号重放幂等，不合并不同调用者的需求。"""
        key = (str(client_id), str(sub_id))
        if not all(key) or not code or period != "1m":
            raise ValueError("native bar subscription requires client, id, code and 1m")
        if not self.supported:
            raise RuntimeError("ContextInfo native minute subscription is unavailable")
        with self._lock:
            if self._closed:
                raise RuntimeError("native minute subscriptions are closed")
            entry = self._entries.get(key)
            if entry is not None:
                if not entry["active"]:
                    raise RuntimeError("native minute subscription cleanup pending")
                if entry["code"] != code or entry["period"] != period:
                    raise ValueError("subscription id cannot change instrument or period")
                entry["seen"] = self._now()
                return self._response(entry)
            if self._thread is None:
                thread = threading.Thread(target=self._send_loop, name="bigqmt-minute-push", daemon=True)
                thread.start()
                self._thread = thread
            entry = {"code": code, "period": period, "handle": None,
                     "topic": bar_topic(*key), "generation": uuid.uuid4().hex,
                     "sequence": 0, "seen": self._now(), "active": True,
                     "gap": False}
            # 原生 subscribe_quote 可能同步交付首个回调，必须先登记所有权。
            self._entries[key] = entry
        try:
            handle = self._context.subscribe_quote(
                code, period, "none", "list", lambda data: self._enqueue(entry, data))
            if isinstance(handle, bool) or not isinstance(handle, int) or handle <= 0:
                raise RuntimeError("ContextInfo.subscribe_quote returned invalid handle")
            with self._lock:
                entry["handle"] = handle
            return self._response(entry)
        except Exception:
            with self._lock:
                entry["active"] = False
                self._entries.pop(key, None)
            raise

    def subscribe_many(self, client_id, subscriptions, timeout_seconds=30.0):
        """按请求剩余秒数限制后续调用；C++ 单次调用无法抢占，超时回滚本批新增项。"""
        if (not isinstance(subscriptions, list) or not subscriptions or len(subscriptions) > 32
                or not 0 < timeout_seconds <= 30):
            raise ValueError("native minute batch requires 1..32 entries and timeout in (0, 30]")
        keys = [(str(client_id), str(item["sub_id"])) for item in subscriptions]
        if len(set(keys)) != len(keys):
            raise ValueError("duplicate subscription id in native minute batch")
        started, created, results = time.monotonic(), [], {}
        try:
            for key, item in zip(keys, subscriptions):
                if time.monotonic() - started >= timeout_seconds:
                    raise TimeoutError("native minute batch deadline exceeded")
                with self._lock:
                    existed = key in self._entries
                result = self.subscribe(client_id, key[1], item["code"])
                if not existed:
                    created.append(key)
                if time.monotonic() - started >= timeout_seconds:
                    raise TimeoutError("native minute batch deadline exceeded")
                results[key[1]] = result
            return results
        except Exception:
            for key in reversed(created):
                try:
                    self.unsubscribe(*key)
                except Exception:
                    # 失败句柄仍归注册表所有，下一次租约清理继续退订。
                    pass
            raise
        finally:
            logging.getLogger(__name__).info(
                "native minute subscribe batch count=%d created=%d elapsed=%.6f",
                len(subscriptions), len(created), time.monotonic() - started)

    @staticmethod
    def _response(entry):
        """返回客户端需要的推送身份，不暴露原生句柄。"""
        return {"topic": entry["topic"], "generation": entry["generation"],
                "native_minute_push": True}

    def unsubscribe(self, client_id, sub_id):
        """在 QMT 策略线程退订；失败保留句柄，允许租约清理重试。"""
        key = (str(client_id), str(sub_id))
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return
            entry["active"] = False
        if entry["handle"] is not None:
            self._context.unsubscribe_quote(entry["handle"])
        with self._lock:
            self._entries.pop(key, None)

    def keepalive(self, client_id, sub_id):
        """续租时明确报告失效句柄，使客户端可立即重建而非等待行情静默。"""
        with self._lock:
            entry = self._entries.get((str(client_id), str(sub_id)))
            if entry is None or not entry["active"]:
                raise RuntimeError("native minute subscription expired")
            entry["seen"] = self._now()

    def keepalive_many(self, client_id, sub_ids):
        """一次续租一批句柄，逐项报告已失效编号供客户端重建。"""
        missing = []
        for sub_id in sub_ids:
            try:
                self.keepalive(client_id, sub_id)
            except RuntimeError:
                missing.append(str(sub_id))
        return {"missing": missing}

    def unsubscribe_many(self, client_id, sub_ids):
        """批量释放仍逐个保留失败句柄；单项异常不能跳过其余资源清理。"""
        errors = []
        for sub_id in sub_ids:
            try:
                self.unsubscribe(client_id, sub_id)
            except Exception as error:
                errors.append(error)
        if errors:
            raise RuntimeError("native minute batch cleanup failed: %s" % errors[0])

    def reap_expired(self, now=None):
        """在 QMT 策略线程回收过期句柄，now 为同一单调时钟的秒数。"""
        current = self._now() if now is None else now
        with self._lock:
            expired = [key for key, entry in self._entries.items()
                       if not entry["active"] or current - entry["seen"] > self._timeout]
        errors = []
        for key in expired:
            try:
                self.unsubscribe(*key)
            except Exception as error:
                errors.append(error)
        if errors:
            raise RuntimeError("native minute lease cleanup failed: %s" % errors[0])
        return len(expired)

    def close(self):
        """在 QMT 策略线程释放全部句柄并停止唯一转发线程。"""
        with self._lock:
            self._closed = True
            keys = list(self._entries)
        self._stop.set()
        self._wake.set()
        failures = []
        try:
            for key in keys:
                try:
                    self.unsubscribe(*key)
                except Exception as error:
                    failures.append(error)
        finally:
            thread = self._thread
            if thread is not None and thread is not threading.current_thread():
                thread.join(1.0)
                if thread.is_alive():
                    raise RuntimeError("native minute publisher did not stop")
            with self._lock:
                self._queue.clear()
                self._bytes = 0
        if failures:
            raise RuntimeError("native minute cleanup failed: %s" % failures[0])

    def _enqueue(self, entry, data):
        """原生回调仅复制并入有界队列；溢出显式通知缺口，不进行网络调用。"""
        from .quote_subscription_manager import _retained_payload_bytes

        with self._lock:
            if not entry["active"] or self._closed:
                return
            entry["sequence"] += 1
            sequence = entry["sequence"]
        try:
            snapshot = copy.deepcopy(data)
            size = _retained_payload_bytes(snapshot)
            with self._lock:
                if not entry["active"] or self._closed:
                    return
                if len(self._queue) >= self._max_items or self._bytes + size > self._max_bytes:
                    entry["gap"] = True
                else:
                    self._queue.append((entry, sequence, snapshot, size))
                    self._bytes += size
        except Exception:
            with self._lock:
                entry["gap"] = True
        self._wake.set()

    def _send_loop(self):
        """唯一后台线程只转发消息；发送失败保留缺口通知并有限退避。"""
        while not self._stop.is_set():
            self._wake.clear()
            while not self._stop.is_set() and self._send_one():
                pass
            self._wake.wait(0.1)

    def _send_one(self):
        """发送一条原始消息或缺口通知，返回是否可继续排空队列。"""
        with self._lock:
            fault = next((entry for entry in self._entries.values()
                          if entry["active"] and entry["gap"]), None)
            if fault is not None:
                fault["gap"] = False
                entry, sequence, data = fault, fault["sequence"], {fault["code"]: {"__gap": True}}
            elif self._queue:
                entry, sequence, data, size = self._queue.popleft()
                self._bytes -= size
                if not entry["active"]:
                    return True
            else:
                return False
        envelope = {"generation": entry["generation"], "sequence": sequence, "data": data}
        try:
            self._publisher(entry["topic"], envelope)
            return True
        except Exception:
            with self._lock:
                entry["gap"] = True
            return False


class NativeBarDelivery(object):
    """校验推送序列并转发原始数据，缺口交由 qmt-grpc 恢复。"""

    def __init__(self, code, callback):
        """callback 接收 SDK 证券字典；缺口为 {code: {'__gap': True}}。"""
        self.code, self.callback = code, callback
        self.generation, self.sequence = None, 0
        self.expected_generation = None
        self._lock = threading.RLock()

    def confirm(self, generation):
        """绑定控制响应确认的服务端代次，拒绝重连前排队的迟到消息。"""
        with self._lock:
            if not generation:
                raise RuntimeError("native minute response has no generation")
            if self.expected_generation is not None and self.expected_generation != generation:
                self.gap()
            self.expected_generation = generation
            if self.generation != generation:
                self.generation, self.sequence = generation, 0

    def gap(self):
        """明确通知跨连接或序列缺口，不生成替代行情。"""
        with self._lock:
            if self.callback is not None:
                self.callback({self.code: {"__gap": True}})

    def __call__(self, envelope):
        """重复及乱序消息不重放，换代和丢帧先通知缺口再交付数据。"""
        with self._lock:
            generation, sequence = envelope.get("generation"), envelope.get("sequence")
            if not generation or isinstance(sequence, bool) or not isinstance(sequence, int) or sequence <= 0:
                self.gap()
                return
            if self.expected_generation is not None and generation != self.expected_generation:
                return
            if generation == self.generation and sequence <= self.sequence:
                return
            if (self.generation is not None and generation != self.generation) or sequence != self.sequence + 1:
                self.gap()
            self.generation, self.sequence = generation, sequence
            if self.callback is not None:
                self.callback(envelope["data"])
