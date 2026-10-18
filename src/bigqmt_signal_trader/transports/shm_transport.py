"""同机共享内存 RPC：有界槽位、提交校验及执行前截止时间门禁。"""

import contextlib
import json
import math
import struct
import threading
import time
import uuid
import zlib

from .base import RpcTransport, TransportError, TransportTimeout
from .shm_primitives import (
    NamedSignal, SharedRegion, cleanup, endpoint_name, process_alive, process_identity,
)


class SharedMemoryTimeout(TransportTimeout):
    """请求超过总预算；outcome_unknown 为真时必须核对结果，不能直接重报。"""

    def __init__(self, message, request_id="", outcome_unknown=False):
        """保存原请求编号及业务是否可能已执行，供上层核对。"""
        super(SharedMemoryTimeout, self).__init__(message)
        self.request_id = request_id
        self.outcome_unknown = bool(outcome_unknown)
        self.error_code = "SHM_OUTCOME_UNKNOWN" if outcome_unknown else "SHM_NOT_STARTED"


class SharedMemoryPeerRestarted(TransportError):
    """对端退出或会话换代，未取得响应的操作可能已经执行。"""

    error_code = "SHM_PEER_CHANGED"

    def __init__(self, message, request_id="", outcome_unknown=True):
        """记录待核对请求；会话换代绝不触发隐式重发。"""
        super(SharedMemoryPeerRestarted, self).__init__(message)
        self.request_id = request_id
        self.outcome_unknown = bool(outcome_unknown)


class SharedMemoryCorruption(TransportError):
    """检测到半提交或损坏的数据，拒绝猜测并继续执行。"""

    error_code = "SHM_CORRUPT"
    outcome_unknown = True


_MAGIC = b"BQSHMR01"
_VERSION = 1
_HEADER_SIZE = 256
_HEADER = struct.Struct("<8sIIIIIQQ16s")
_SLOT_SIZE = 128
_SLOT = struct.Struct("<I16sQQdQII")
_U32 = struct.Struct("<I")
_EMPTY, _WRITING, _REQUEST, _CLAIMED, _EXECUTING, _RESPONSE = range(6)
_PREPARING = 6
_EXTERNAL = 1 << 31
_ZERO_TOKEN = b"\x00" * 16
_BLOB = struct.Struct("<8sII")
_BLOB_MAGIC = b"BQSHMB01"


def _crc(data):
    """返回无符号 CRC32，用于检测半写入和意外损坏。"""
    return zlib.crc32(data) & 0xffffffff


def _seconds(value, name):
    """校验秒数，避免 NaN、无穷和负值造成无界等待。"""
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError("%s must be finite and positive" % name)
    return result


class SharedMemoryTransport(RpcTransport):
    """独立进程共用账户端点；单次调用只发布一次，响应以随机令牌隔离。"""

    name = "shm"
    supports_execution_gate = True

    def __init__(self, account_id="", print_prefix="[bigqmt_rpc]", namespace="bigqmt",
                 directory=None, rpc_slots=32, rpc_slot_bytes=65536,
                 max_payload_bytes=64 * 1024 * 1024, wait_poll_seconds=0.05,
                 connect_timeout_seconds=5.0, **kwargs):
        """配置映射，暂不打开资源。

        namespace 与 account_id 在两端须相同；directory 仅供 POSIX 使用。
        rpc_slots 限制并发，rpc_slot_bytes 是每槽内联字节数；较大消息另开令牌独有映射，
        上限由 max_payload_bytes 指定。等待单位为秒，唤醒信号只作提示。
        """
        super(SharedMemoryTransport, self).__init__(account_id, print_prefix)
        self.namespace = str(namespace)
        self.directory = directory
        self.rpc_slots = int(rpc_slots)
        self.rpc_slot_bytes = int(rpc_slot_bytes)
        self.max_payload_bytes = int(max_payload_bytes)
        if not 1 <= self.rpc_slots <= 1024:
            raise ValueError("rpc_slots must be between 1 and 1024")
        if not 1024 <= self.rpc_slot_bytes <= self.max_payload_bytes <= 256 * 1024 * 1024:
            raise ValueError("invalid shared memory payload limits")
        self._size = _HEADER_SIZE + self.rpc_slots * (_SLOT_SIZE + self.rpc_slot_bytes)
        if self._size > 512 * 1024 * 1024:
            raise ValueError("RPC inline mapping exceeds 512 MiB")
        self.wait_poll_seconds = _seconds(wait_poll_seconds, "wait_poll_seconds")
        self.connect_timeout_seconds = _seconds(connect_timeout_seconds, "connect_timeout_seconds")
        self._name = endpoint_name(self.namespace, self.account_id, "rpc")
        self._pid, self._birth = process_identity()
        self._region = None
        self._request_signal = None
        self._space_signal = None
        self._reply_signals = {}
        self._response_blobs = {}
        # 同一槽的重复回调必须串行；不同槽的大响应不能互相耗尽提交等待预算。
        self._response_locks = [threading.Lock() for _ in range(self.rpc_slots)]
        self._init_lock = threading.RLock()
        self._lifecycle = threading.Condition(threading.RLock())
        self._active = 0
        self._closed = False
        self._resources_closed = False
        self._epoch = None
        self._thread = None
        self._error = None
        self._next_slot = 0
        self._drain_contended = False
        self.defer_execution = False
        self._stats = {"published": 0, "executed": 0, "responses": 0,
                       "timeouts": 0, "late_responses": 0, "signal_errors": 0, "lock_contentions": 0,
                       "execution_lock_retries": 0, "response_lock_retries": 0}

    @contextlib.contextmanager
    def _operation(self, deadline=None):
        """引用在途操作；最后一个操作者退出后才允许关闭映射。"""
        with self._lifecycle:
            if self._closed:
                raise TransportError("shared memory transport is closed")
            self._active += 1
        try:
            self._open(deadline)
            yield
        finally:
            with self._lifecycle:
                self._active -= 1
                self._lifecycle.notify_all()
                if self._closed and self._active == 0:
                    self._close_resources()

    def _open(self, deadline=None):
        """首次使用才建立映射；初始化也计入调用方剩余预算。"""
        if self._region is not None:
            return
        deadline = deadline if deadline is not None else time.monotonic() + self.connect_timeout_seconds
        if not self._init_lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
            raise SharedMemoryTimeout("shared memory initialization timed out")
        try:
            if self._region is not None:
                return
            region = SharedRegion(self._name, self._size, self.directory,
                                  timeout_seconds=max(0.0, deadline - time.monotonic()))
            signals = []
            try:
                with region.locked(timeout_seconds=max(0.0, deadline - time.monotonic())):
                    if region.buffer[:_HEADER_SIZE] == b"\x00" * _HEADER_SIZE:
                        self._write_header(region.buffer, 0, 0, 0, _ZERO_TOKEN)
                    self._read_header(region.buffer)
                signals.append(NamedSignal(self._name + "_request", self.directory))
                signals.append(NamedSignal(self._name + "_space", self.directory))
            except BaseException:
                for signal in signals:
                    signal.close()
                region.close()
                raise
            self._region = region
            self._request_signal, self._space_signal = signals
        finally:
            self._init_lock.release()

    def _read_header(self, buffer=None):
        """核对协议、尺寸和头校验，禁止配置不匹配或半写入后继续读取。"""
        buffer = self._region.buffer if buffer is None else buffer
        raw = buffer[:_HEADER.size]
        checksum = _U32.unpack(buffer[_HEADER.size:_HEADER.size + 4])[0]
        values = _HEADER.unpack(raw)
        if checksum != _crc(raw) or values[0] != _MAGIC:
            raise SharedMemoryCorruption("shared memory RPC header is corrupt")
        if values[1:5] != (_VERSION, self.rpc_slots, self.rpc_slot_bytes, self.max_payload_bytes):
            raise TransportError("shared memory RPC ABI/configuration mismatch")
        return values[5], values[6], values[7], values[8]

    def _write_header(self, buffer, ready, pid, birth, epoch):
        """只在启动、停止时修改身份，稳态不争用全局计数。"""
        raw = _HEADER.pack(_MAGIC, _VERSION, self.rpc_slots, self.rpc_slot_bytes,
                           self.max_payload_bytes, ready, pid, birth, epoch)
        buffer[:_HEADER.size + 4] = raw + _U32.pack(_crc(raw))

    def _offset(self, index):
        """计算固定布局的槽起点。"""
        return _HEADER_SIZE + index * (_SLOT_SIZE + self.rpc_slot_bytes)

    def _read_slot(self, index):
        """核对提交元数据；遗留 WRITING 表示异常退出，不能猜测结果。"""
        offset = self._offset(index)
        raw = self._region.buffer[offset:offset + _SLOT.size]
        state = _U32.unpack(raw[:4])[0]
        if state == _EMPTY:
            return None
        if state == _WRITING or state not in (_PREPARING, _REQUEST, _CLAIMED, _EXECUTING, _RESPONSE):
            raise SharedMemoryCorruption("uncommitted/corrupt RPC slot %d" % index)
        checksum = _U32.unpack(self._region.buffer[offset + _SLOT.size:offset + _SLOT.size + 4])[0]
        if checksum != _crc(raw):
            raise SharedMemoryCorruption("RPC slot metadata checksum mismatch")
        value = _SLOT.unpack(raw)
        length = value[6] & ~_EXTERNAL
        if length > self.max_payload_bytes or (not value[6] & _EXTERNAL and length > self.rpc_slot_bytes):
            raise SharedMemoryCorruption("RPC slot payload length is invalid")
        return value

    def _write_slot(self, index, value, payload=None):
        """先标写入、写数据及校验，最后提交状态；进程中途退出会被检测。"""
        offset = self._offset(index)
        raw = _SLOT.pack(*value)
        self._region.buffer[offset:offset + 4] = _U32.pack(_WRITING)
        if payload is not None:
            self._region.buffer[offset + _SLOT_SIZE:offset + _SLOT_SIZE + len(payload)] = payload
        self._region.buffer[offset + 4:offset + _SLOT.size + 4] = raw[4:] + _U32.pack(_crc(raw))
        self._region.buffer[offset:offset + 4] = raw[:4]

    def _free_slot(self, index):
        """只清提交状态；旧字节在新令牌提交前不可见。"""
        offset = self._offset(index)
        self._region.buffer[offset:offset + 4] = _U32.pack(_EMPTY)

    @contextlib.contextmanager
    def _locked(self, timeout_seconds):
        """遗弃锁恢复后先检查已提交元数据，绝不直接当普通成功继续。"""
        with self._region.locked(timeout_seconds=max(0.0, timeout_seconds)) as abandoned:
            self._read_header()
            if abandoned:
                for index in range(self.rpc_slots):
                    self._read_slot(index)
            yield

    def _signal(self, signal):
        """提交后唤醒失败仅记诊断；接收方重查谓词，发送方不能因此重发。"""
        try:
            signal.set()
        except Exception:
            self._stats["signal_errors"] += 1

    def _reply_signal(self, index):
        """每槽独立唤醒，减少并发响应竞争。"""
        with self._init_lock:
            signal = self._reply_signals.get(index)
            if signal is None:
                signal = NamedSignal(self._name + "_reply_%d" % index, self.directory)
                self._reply_signals[index] = signal
            return signal

    def _reply_hint(self, index):
        """提示对象创建失败也降级为检查谓词，不能掩盖已经提交的业务请求。"""
        try:
            return self._reply_signal(index)
        except Exception:
            self._stats["signal_errors"] += 1
            return None

    def _wait_hint(self, signal, seconds):
        """唤醒失败只影响等待效率，协议正确性始终由共享状态保证。"""
        if signal is not None:
            try:
                signal.wait(seconds)
                return
            except Exception:
                self._stats["signal_errors"] += 1
        time.sleep(max(0.0, seconds))

    def _blob_name(self, token, direction):
        """按本次令牌命名大报文，防止旧响应覆盖新请求。"""
        return self._name + "_" + direction + "_" + token.hex()

    def _create_blob(self, token, direction, payload, checksum, deadline):
        """单独映射大报文；返回句柄保留到对端读取或超时。"""
        name = self._blob_name(token, direction)
        region = SharedRegion(name, _BLOB.size + len(payload), self.directory,
                              timeout_seconds=max(0.0, deadline - time.monotonic()))
        try:
            region.buffer[_BLOB.size:] = payload
            region.buffer[:_BLOB.size] = _BLOB.pack(_BLOB_MAGIC, len(payload), checksum)
            return region
        except BaseException:
            region.close()
            cleanup(name, self.directory)
            raise

    def _payload(self, index, value, direction):
        """在槽受互斥保护时取得快照，CRC 失败不交给业务代码。"""
        deadline = value[4]
        if time.monotonic() >= deadline:
            raise SharedMemoryTimeout("SHM payload deadline expired", outcome_unknown=direction == "response")
        length = value[6] & ~_EXTERNAL
        if value[6] & _EXTERNAL:
            name = self._blob_name(value[1], direction)
            region = SharedRegion(name, _BLOB.size + length, self.directory,
                                  timeout_seconds=max(0.0, deadline - time.monotonic()))
            try:
                if time.monotonic() >= deadline:
                    raise SharedMemoryTimeout("SHM payload open exceeded deadline", outcome_unknown=direction == "response")
                if _BLOB.unpack(region.buffer[:_BLOB.size]) != (_BLOB_MAGIC, length, value[7]):
                    raise SharedMemoryCorruption("RPC external payload header mismatch")
                payload = region.buffer[_BLOB.size:]
            finally:
                region.close()
        else:
            offset = self._offset(index) + _SLOT_SIZE
            payload = self._region.buffer[offset:offset + length]
        if _crc(payload) != value[7]:
            raise SharedMemoryCorruption("RPC payload checksum mismatch")
        if time.monotonic() >= deadline:
            raise SharedMemoryTimeout("SHM payload copy exceeded deadline", outcome_unknown=direction == "response")
        return payload

    def _cleanup_slot_blobs(self, value):
        """过期/死亡请求的令牌记录仍在槽内，按此记录清理两种方向的大报文。"""
        if value is not None:
            for direction in ("request", "response"):
                cleanup(self._blob_name(value[1], direction), self.directory)

    def _drop_blob(self, region, token, direction):
        """释放本实例的大报文，唯一令牌允许显式清除 POSIX 残留。"""
        try:
            region.close()
        finally:
            cleanup(self._blob_name(token, direction), self.directory)

    def send_request(self, request, timeout_seconds):
        """发送一次并等待匹配响应；总预算含排队，超时或重启不重发。"""
        timeout = _seconds(timeout_seconds, "timeout_seconds")
        deadline = time.monotonic() + timeout
        envelope = dict(request)
        request_id = str(envelope.get("request_id") or uuid.uuid4().hex)
        envelope["request_id"] = request_id
        envelope.setdefault("account_id", self.account_id)
        envelope.pop("_shm_route", None)
        try:
            payload = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise TransportError("SHM request serialization failed: %s" % exc)
        if len(payload) > self.max_payload_bytes:
            raise TransportError("SHM request exceeds max_payload_bytes")
        token, checksum = uuid.uuid4().bytes, _crc(payload)
        external = len(payload) > self.rpc_slot_bytes
        blob, slot, epoch = None, None, None
        try:
            with self._operation(deadline):
                try:
                    while slot is None:
                        if self._closed or time.monotonic() >= deadline:
                            raise SharedMemoryTimeout("SHM request was not published before deadline", request_id)
                        with self._locked(deadline - time.monotonic()):
                            ready, pid, birth, epoch = self._read_header()
                            if ready and process_alive(pid, birth):
                                now = time.monotonic()
                                for step in range(self.rpc_slots):
                                    index = (self._next_slot + step) % self.rpc_slots
                                    value = self._read_slot(index)
                                    if value is not None and value[4] <= now:
                                        self._cleanup_slot_blobs(value)
                                        self._free_slot(index)
                                        value = None
                                    if value is None:
                                        slot = index
                                        self._next_slot = (index + 1) % self.rpc_slots
                                        # 大报文先登记令牌再分配映射，客户端途中崩溃仍有回收依据。
                                        value = (_PREPARING if external else _REQUEST, token, self._pid, self._birth, deadline,
                                                 int(now * 1e9), len(payload) | (_EXTERNAL if external else 0), checksum)
                                        self._write_slot(index, value, None if external else payload)
                                        if not external:
                                            self._stats["published"] += 1
                                        break
                        if slot is None:
                            self._wait_hint(self._space_signal, min(self.wait_poll_seconds, max(0.0, deadline - time.monotonic())))
                    if external:
                        blob = self._create_blob(token, "request", payload, checksum, deadline)
                        with self._locked(deadline - time.monotonic()):
                            ready, _, _, current_epoch = self._read_header()
                            value = self._read_slot(slot)
                            if not ready or current_epoch != epoch or value is None or value[1] != token:
                                raise SharedMemoryPeerRestarted("SHM server changed before request publication", request_id, False)
                            if time.monotonic() >= deadline:
                                self._free_slot(slot)
                                raise SharedMemoryTimeout("SHM request preparation exceeded deadline", request_id)
                            self._write_slot(slot, (_REQUEST,) + value[1:])
                            self._stats["published"] += 1
                    self._signal(self._request_signal)
                    return self._wait_response(slot, token, epoch, request_id, deadline)
                finally:
                    if blob is not None:
                        self._drop_blob(blob, token, "request")
        except SharedMemoryTimeout as exc:
            # 深层报文复制只知道槽令牌；在公开边界补回业务编号，便于核对不确定结果。
            if not exc.request_id:
                exc.request_id = request_id
            self._stats["timeouts"] += 1
            raise
        except TransportTimeout:
            unknown = self._cancel(slot, token) if slot is not None and not self._resources_closed else slot is not None
            self._stats["timeouts"] += 1
            raise SharedMemoryTimeout("SHM total request budget exhausted", request_id, unknown)

    def _wait_response(self, index, token, epoch, request_id, deadline):
        """按令牌与会话读取响应，迟到信号不能冒充另一请求的响应。"""
        from ..redis_rpc import loads_rpc_response

        signal = self._reply_hint(index)
        while True:
            payload = None
            with self._locked(max(0.0, deadline - time.monotonic())):
                ready, pid, birth, current_epoch = self._read_header()
                if not ready or current_epoch != epoch:
                    raise SharedMemoryPeerRestarted("SHM server stopped or restarted; reconcile request", request_id)
                value = self._read_slot(index)
                if value is None or value[1] != token:
                    raise SharedMemoryTimeout("SHM request slot expired; reconcile if executed", request_id, True)
                if value[0] == _RESPONSE:
                    payload = self._payload(index, value, "response")
                    self._free_slot(index)
            if payload is not None:
                self._signal(self._space_signal)
                self._signal(self._request_signal)
                response = loads_rpc_response(payload)
                # 正文损坏时服务端读不到 request_id；槽令牌和会话已匹配，允许唯一的传输拒绝包补回编号。
                if (isinstance(response, dict) and response.get("request_id") is None
                        and response.get("error_code") == "SHM_REQUEST_CORRUPT" and response.get("ok") is False):
                    response["request_id"] = request_id
                if not isinstance(response, dict) or str(response.get("request_id")) != request_id:
                    raise SharedMemoryCorruption("SHM response request_id mismatch")
                if time.monotonic() >= deadline:
                    raise SharedMemoryTimeout("SHM response decode exceeded deadline", request_id, True)
                return response
            if self._closed or time.monotonic() >= deadline:
                unknown = self._cancel(index, token)
                raise SharedMemoryTimeout("SHM response deadline exceeded; no request was resent", request_id, unknown)
            if not process_alive(pid, birth):
                unknown = self._cancel(index, token)
                raise SharedMemoryPeerRestarted("SHM server process exited; no request was resent", request_id, unknown)
            self._wait_hint(signal, min(self.wait_poll_seconds, max(0.0, deadline - time.monotonic())))

    def _cancel(self, index, token):
        """撤销未开始请求或释放等待；已执行的业务不能由通信层撤回。"""
        try:
            with self._locked(0.0):
                value = self._read_slot(index)
                if value is None or value[1] != token:
                    return True
                unknown = value[0] in (_EXECUTING, _RESPONSE)
                self._free_slot(index)
            self._signal(self._space_signal)
            return unknown
        except Exception:
            # 无法原子确认状态时宁可要求核对，不能让调用方以为可安全重报。
            return True

    def start_receiving(self, on_request, background_threads=True):
        """独占服务端身份；后台接收或由 QMT 策略线程调用 drain。"""
        with self._operation():
            with self._locked(self.connect_timeout_seconds):
                ready, pid, birth, epoch = self._read_header()
                if self._running and self._epoch == epoch:
                    return
                if ready and process_alive(pid, birth):
                    raise TransportError("SHM RPC endpoint already has a live server")
                self._epoch = uuid.uuid4().bytes
                for index in range(self.rpc_slots):
                    self._cleanup_slot_blobs(self._read_slot(index))
                    self._free_slot(index)
                self._write_header(self._region.buffer, 1, self._pid, self._birth, self._epoch)
                self._on_request = on_request
                self._running = True
            self._signal(self._space_signal)
            if background_threads:
                self._thread = threading.Thread(target=self._receive_loop, name="bigqmt-shm-rpc", daemon=True)
                try:
                    self._thread.start()
                except BaseException:
                    # 启动失败的线程不可 join，否则清理异常会掩盖真正原因。
                    self._thread = None
                    self.stop()
                    raise

    def _receive_loop(self):
        """事件驱动收取，定时检查保证遗漏唤醒提示也不会永久卡住。"""
        try:
            while self._running and not self._closed:
                count = self.drain_request_queue(self.rpc_slots)
                if not count and self._running:
                    # 唤醒已消费但锁被客户端短暂占用时，不能误当空队列再睡满50ms。
                    # 有界短等保留低CPU开销；策略线程的 drain 本身仍保持非阻塞。
                    wait = min(self.wait_poll_seconds, 0.001) if self._drain_contended else self.wait_poll_seconds
                    self._wait_hint(self._request_signal, wait)
        except Exception as exc:
            if not self._closed:
                self._error = exc
                self._running = False

    def drain_request_queue(self, max_items=20):
        """非阻塞领取有界批次，业务回调在调用本方法的线程执行。"""
        if not self._running or self._closed:
            return 0
        processed = 0
        self._drain_contended = False
        with self._operation():
            self._reap_blobs()
            for _ in range(max(0, int(max_items))):
                try:
                    with self._locked(0.0):
                        ready, _, _, epoch = self._read_header()
                        if not ready or epoch != self._epoch:
                            raise SharedMemoryPeerRestarted("SHM server ownership changed")
                        candidates = []
                        now = time.monotonic()
                        for index in range(self.rpc_slots):
                            value = self._read_slot(index)
                            if value is None:
                                continue
                            if value[4] <= now:
                                self._cleanup_slot_blobs(value)
                                self._free_slot(index)
                                continue
                            if value[0] not in (_REQUEST, _PREPARING):
                                continue
                            if not process_alive(value[2], value[3]):
                                self._cleanup_slot_blobs(value)
                                self._free_slot(index)
                                continue
                            if value[0] == _PREPARING:
                                continue
                            candidates.append((value[5], index, value))
                        if not candidates:
                            break
                        _, index, value = min(candidates, key=lambda item: item[0])
                        payload_error = None
                        try:
                            raw = self._payload(index, value, "request")
                        except SharedMemoryCorruption as exc:
                            # 槽头可信而正文不可读时只拒绝此请求；客户端取消可能已释放大报文。
                            payload_error = exc
                        self._write_slot(index, (_CLAIMED,) + value[1:])
                except TransportTimeout:
                    self._drain_contended = True
                    self._stats["lock_contentions"] += 1
                    break
                if payload_error is not None:
                    request = {"_shm_route": {"index": index, "token": value[1].hex(), "epoch": self._epoch.hex(), "deadline": value[4]}}
                    self.send_response(request, {"ok": False, "error": str(payload_error),
                                                 "error_code": "SHM_REQUEST_CORRUPT", "outcome_unknown": False})
                    processed += 1
                    continue
                try:
                    request = json.loads(raw.decode("utf-8"))
                    if not isinstance(request, dict):
                        raise ValueError("SHM request must be an object")
                except (ValueError, UnicodeError) as exc:
                    request = {"_shm_route": {"index": index, "token": value[1].hex(), "epoch": self._epoch.hex(), "deadline": value[4]}}
                    self.send_response(request, {"ok": False, "error": str(exc),
                                                 "error_code": "SHM_REQUEST_CORRUPT", "outcome_unknown": False})
                    processed += 1
                    continue
                request["_shm_route"] = {"index": index, "token": value[1].hex(), "epoch": self._epoch.hex(), "deadline": value[4]}
                if self.defer_execution or self.begin_request(request):
                    self.deliver(request)
                processed += 1
        return processed

    def begin_request(self, request):
        """真正执行前原子校验；本地排队超时的请求不会迟到执行。"""
        try:
            return self._retry_transition_lock(
                request, lambda: self._begin_request(request), "execution_lock_retries")
        except TransportTimeout:
            # 有界争锁仍失败时拒绝执行；不能杀掉接收线程，也不能重报业务请求。
            return False

    def _retry_transition_lock(self, request, action, counter):
        """最多约百毫秒重查同一状态转换；action 只能含门禁或既有响应提交，不能执行业务。"""
        deadline = time.monotonic() + 0.1
        request_deadline = (request.get("_shm_route") or {}).get("deadline")
        if isinstance(request_deadline, (int, float)) and math.isfinite(request_deadline):
            deadline = min(deadline, request_deadline)
        while True:
            try:
                return action()
            except TransportTimeout as exc:
                # 数据复制超时已有业务语义，不能把它当成单纯互斥争用重新提交。
                remaining = deadline - time.monotonic()
                if isinstance(exc, SharedMemoryTimeout) or self._closed or remaining <= 0:
                    raise
                self._stats[counter] += 1
                time.sleep(min(0.001, remaining))

    @contextlib.contextmanager
    def execution_scope(self, request):
        """覆盖本地排队后实际业务执行的生命周期，旧回调退出前不交出服务端身份。"""
        if self._closed:
            yield False
            return
        with self._operation():
            yield self.begin_request(request)

    def _begin_request(self, request):
        """持互斥锁完成领取到执行的转换；调用者负责把锁超时限定到当前请求。"""
        route = request.get("_shm_route") or {}
        if self._closed or not self._running:
            return False
        with self._operation():
            with self._locked(self.wait_poll_seconds):
                ready, _, _, epoch = self._read_header()
                if not ready or epoch != self._epoch or route.get("epoch") != epoch.hex():
                    return False
                index = int(route.get("index", -1))
                if not 0 <= index < self.rpc_slots:
                    return False
                value = self._read_slot(index)
                if value is None or value[1].hex() != route.get("token") or value[0] != _CLAIMED:
                    return False
                if value[4] <= time.monotonic():
                    self._free_slot(index)
                    self._signal(self._space_signal)
                    return False
                self._write_slot(index, (_EXECUTING,) + value[1:])
                self._stats["executed"] += 1
                return True

    def send_response(self, request, response):
        """超时或换代后的旧回调只丢弃响应，不能污染新请求。"""
        route = request.get("_shm_route") or {}
        if self._closed or route.get("epoch") != (self._epoch or b"").hex():
            self._stats["late_responses"] += 1
            return False
        result = dict(response)
        result.setdefault("request_id", request.get("request_id"))
        try:
            payload = json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
            if len(payload) > self.max_payload_bytes:
                raise ValueError("response exceeds max_payload_bytes")
        except (TypeError, ValueError) as exc:
            # 业务可能已执行，编码失败需返回明确错误，不能假装操作未发生。
            payload = json.dumps({"ok": False, "request_id": request.get("request_id"),
                                  "error": str(exc), "error_code": "SHM_RESPONSE_ENCODING",
                                  "outcome_unknown": True, "data": None}).encode("utf-8")
        index = int(route.get("index", -1))
        if not 0 <= index < self.rpc_slots:
            return False
        token = bytes.fromhex(route["token"])
        with self._operation():
            # 原生调用已完成，短暂争锁只能延后同一响应，不能丢弃响应或重执行处理器。
            return self._retry_transition_lock(
                request, lambda: self._publish_response_payload(index, token, payload),
                "response_lock_retries")

    def _publish_response_payload(self, index, token, payload):
        """串行提交同槽既有响应，每次重查仍验证代次、令牌、截止时间及首次响应所有权。"""
        response_lock = self._response_locks[index]
        if not response_lock.acquire(timeout=self.wait_poll_seconds):
            raise TransportTimeout("SHM response publication lock timed out")
        try:
            return self._commit_response(index, token, payload)
        finally:
            response_lock.release()

    def _response_slot(self, index, token):
        """在主区域锁内核对响应所有权，重复、过期或换代回调都不能改动 blob。"""
        ready, _, _, epoch = self._read_header()
        value = self._read_slot(index)
        if (not ready or epoch != self._epoch or value is None or value[1] != token
                or value[0] not in (_CLAIMED, _EXECUTING) or value[4] <= time.monotonic()):
            self._stats["late_responses"] += 1
            return None
        return value

    def _commit_response(self, index, token, payload):
        """串行核对并提交响应；分配大映射前后都检查槽，避免重复回调覆盖首次结果。"""
        external, checksum, blob = len(payload) > self.rpc_slot_bytes, _crc(payload), None
        try:
            # 大报文映射按令牌命名，分配前必须拒绝已提交响应；否则会覆写其内容。
            with self._locked(self.wait_poll_seconds):
                if self._response_slot(index, token) is None:
                    return False
            if external:
                blob = self._create_blob(token, "response", payload, checksum,
                                         time.monotonic() + self.wait_poll_seconds)
            with self._locked(self.wait_poll_seconds):
                value = self._response_slot(index, token)
                if value is None:
                    return False
                if blob is not None:
                    self._response_blobs[token] = (index, blob)
                value = (_RESPONSE,) + value[1:6] + (len(payload) | (_EXTERNAL if external else 0), checksum)
                self._write_slot(index, value, None if external else payload)
                blob = None
                self._stats["responses"] += 1
            hint = self._reply_hint(index)
            if hint is not None:
                self._signal(hint)
            return True
        finally:
            if blob is not None:
                self._drop_blob(blob, token, "response")

    def _reap_blobs(self):
        """客户端 ACK 或超时后回收大响应，保留数量受并发槽位限制。"""
        if not self._response_blobs:
            return
        expired = []
        try:
            with self._locked(0.0):
                for token, (index, region) in list(self._response_blobs.items()):
                    value = self._read_slot(index)
                    if value is None or value[1] != token or value[4] <= time.monotonic():
                        expired.append((token, region))
                        del self._response_blobs[token]
        except TransportTimeout:
            return
        for token, region in expired:
            self._drop_blob(region, token, "response")

    def check_health(self):
        """暴露接收线程故障，避免把已经停收的服务误判为健康。"""
        if self._error is not None:
            raise self._error
        if self._closed:
            raise TransportError("shared memory transport is closed")

    def status(self):
        """返回诊断计数，不包含消息正文或账号凭据。"""
        result = dict(self._stats)
        result.update({"transport": "shm", "running": self._running, "closed": self._closed,
                       "error": str(self._error or ""), "inline_mapping_bytes": self._size})
        return result

    def stop(self):
        """唤醒等待者；在途回调结束后才交出身份，清理失败可重复调用重试。"""
        with self._lifecycle:
            self._closed = True
            self._running = False
        if self._region is not None and not self._resources_closed:
            self._signal(self._request_signal)
            self._signal(self._space_signal)
            for signal in list(self._reply_signals.values()):
                self._signal(signal)
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(1.0)
        with self._lifecycle:
            if self._active == 0:
                self._close_resources()

    def _close_resources(self):
        """最后一个在途操作离开后清理一次，端点保留给重连使用。"""
        if self._resources_closed:
            return
        if self._region is not None and self._epoch is not None:
            # 不能吞掉争锁失败再关闭：否则同进程存活的旧 PID 会永久占据端点。
            # 直到所有业务回调结束才释放身份，防止新旧服务同时调用 QMT。
            with self._locked(self.connect_timeout_seconds):
                _, _, _, epoch = self._read_header()
                if epoch == self._epoch:
                    self._write_header(self._region.buffer, 0, 0, 0, epoch)
        for token, (_, region) in list(self._response_blobs.items()):
            self._drop_blob(region, token, "response")
        self._response_blobs.clear()
        for signal in [self._request_signal, self._space_signal] + list(self._reply_signals.values()):
            if signal is not None:
                signal.close()
        if self._region is not None:
            self._region.close()
        self._resources_closed = True
