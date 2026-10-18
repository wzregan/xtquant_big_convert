"""有界共享内存推送日志：独立消费者确认、显式背压以及故障可见性。"""

import contextlib
import hashlib
import json
import os
import struct
import threading
import time
import uuid
import zlib
from collections import namedtuple

from .quote_push_channel import QuotePushChannel
from .transports.base import TransportError, TransportTimeout
from .transports.shm_primitives import (
    NamedSignal, SharedRegion, endpoint_name, process_alive, process_identity,
)


class SharedMemoryPushError(TransportError):
    """共享推送永久错误；必须显式检查并恢复，不能当作未提交请求盲目重试。"""

    retryable = False


class PushBackpressureError(SharedMemoryPushError):
    """容量或锁预算暂时不足，本次消息尚未提交，可以有界重试。"""

    retryable = True


class PushLockBusyError(PushBackpressureError):
    """共享元数据锁暂不可用；不代表日志容量已满，原消息尚未提交。"""

    lock_busy = True


class PushGapError(SharedMemoryPushError):
    """发布代次改变、保留期过期或日志缺口，不能假装连续消费。"""


class PushCallbackError(SharedMemoryPushError):
    """业务回调未成功，消息保持未确认，等待调用者明确处理。"""

    code = "callback_failed"
    callback_failed = True


_MAGIC = b"BQPSH001"
_VERSION = 2
_HEADER = struct.Struct("<8sI4x16sQQQQIIII")
_Header = namedtuple("PushHeader", "magic version epoch pid birth market_bytes trade_bytes market_slots trade_slots consumers health")
_LANE = struct.Struct("<QQQQ")
_Lane = namedtuple("PushLane", "head next_seq write_pos reclaim_pos")
_RECORD = struct.Struct("<QQIIQ32s")
_Record = namedtuple("PushRecord", "seq pos length crc commit message_id")
_CONSUMER = struct.Struct("<II32s32sQQQQQQ16s")
_Consumer = namedtuple("PushConsumer", "state stable identity topics pid birth seen expires market_cursor trade_cursor token")
_CONSUMER_BYTES = 256
_GENERATION = struct.Struct("<Q")
_CODEC = struct.Struct("<I")
_CODEC_IDS = {"json": 1, "msgpack": 2}
_EMPTY_ID = b"\0" * 32
_ACTIVE, _RETAINED, _EXPIRED, _FAILED = 1, 2, 3, 4
_CALLBACK_FAILED_FLAG = 1 << 8


def _positive_int(config, key, default):
    """读取正整数容量，拒绝零容量和可能导致无界分配的非法配置。"""
    value = int(config.get(key, default))
    if value <= 0:
        raise ValueError("%s must be positive" % key)
    return value


def _seconds(config, key, default):
    """读取非负、有限的秒数，避免 NaN 或无穷等待。"""
    value = float(config.get(key, default))
    if not 0 <= value < float("inf"):
        raise ValueError("%s must be finite and non-negative" % key)
    return value


class SharedMemoryQuotePushChannel(QuotePushChannel):
    """共享推送通道，config 为展开后的 shm 配置；role 取 server 或 client。

    每个账号、namespace 仅允许一个发布者。exec: 主题使用独立交易日志。
    稳定 subscriber_id 只保证同一发布代次、保留期内的至少一次交付；不承诺断电持久化。
    """

    supports_acknowledgements = True

    def __init__(self, role="client", config=None, account_id="", print_prefix="[bigqmt_shm_push]"):
        """读取 shm 子配置并规划固定布局；实例持有的映射和线程由 stop 回收。

        role 为 server 或 client；两端 account_id、namespace 和容量须匹配。
        容量以字节或条数计，等待和身份保留期限以秒计；构造时尚不创建内核对象。
        """
        self.role = str(role).lower()
        if self.role not in ("server", "client"):
            raise ValueError("role must be server or client")
        self.config = dict(config or {})
        self.account_id = str(account_id or "")
        self.print_prefix = print_prefix
        self.namespace = str(self.config.get("namespace") or "bigqmt")
        self.directory = self.config.get("directory")
        self.push_codec = str(self.config.get("push_codec") or "json").strip().lower()
        if self.push_codec not in _CODEC_IDS:
            raise ValueError("push_codec must be json or msgpack")
        self._codec_id = _CODEC_IDS[self.push_codec]
        self._msgpack = None
        if self.push_codec == "msgpack":
            try:
                import msgpack
            except ImportError as exc:
                raise SharedMemoryPushError("push_codec='msgpack' requires msgpack in both processes: %s" % exc)
            self._msgpack = msgpack
        self.capacities = (
            _positive_int(self.config, "market_push_bytes", 24 * 1024 * 1024),
            _positive_int(self.config, "trade_push_bytes", 8 * 1024 * 1024),
        )
        self.slot_counts = (
            _positive_int(self.config, "market_push_slots", 4096),
            _positive_int(self.config, "trade_push_slots", 1024),
        )
        self.max_consumers = _positive_int(self.config, "max_push_subscribers", 64)
        self.publish_timeout = _seconds(self.config, "publish_timeout_seconds", 0.02)
        self.init_timeout = _seconds(self.config, "init_timeout_seconds", 5.0)
        self.lock_timeout = _seconds(self.config, "lock_timeout_seconds", 1.0)
        self.retention = _seconds(self.config, "subscriber_retention_seconds", 300.0)
        self.poll_interval = max(0.001, _seconds(self.config, "push_poll_seconds", 0.05))
        identity = self.config.get("subscriber_id") or self.config.get("client_id")
        self.stable = bool(identity)
        self.subscriber_id = str(identity or uuid.uuid4().hex)
        self._identity = hashlib.sha256(self.subscriber_id.encode("utf-8")).digest()
        self._token = uuid.uuid4().bytes
        self._pid, self._birth = process_identity()
        self._name = endpoint_name(self.namespace, self.account_id, "push")
        self._space_name = endpoint_name(self.namespace, self.account_id, "push-space")
        indexes = 256 + self.max_consumers * _CONSUMER_BYTES
        self._index_offsets = (indexes, indexes + self.slot_counts[0] * _RECORD.size)
        data = self._index_offsets[1] + self.slot_counts[1] * _RECORD.size
        self._data_offsets = (data, data + self.capacities[0])
        self._size = data + sum(self.capacities)
        self._region = None
        self._space_signal = None
        self._data_signal = None
        self._signals = {}
        self._signals_lock = threading.Lock()
        self._epoch = None
        self._slot = None
        self._topics = set()
        self._lanes = ()
        self._pending_topic_update = None
        self._topic_update_owner = None
        self._thread = None
        self._running = False
        self._started = False
        self._error = None
        self._published_ids = {}
        self._consumer_cache = []
        self._consumer_generation = None
        self._next_maintenance = 0.0
        self._signal_errors = 0
        self._missed_hints = 0
        self._published = 0
        self._acknowledged = 0

    @property
    def publisher_endpoint(self):
        """返回不含网络地址的共享日志名称。"""
        return "shm://" + self._name

    def _open(self, timeout_seconds=None):
        """按固定布局打开映射及容量唤醒信号，失败时关闭部分分配资源。"""
        if self._region is not None:
            return
        region = SharedRegion(self._name, self._size, directory=self.directory,
                              timeout_seconds=self.init_timeout if timeout_seconds is None else timeout_seconds)
        try:
            signal = NamedSignal(self._space_name, directory=self.directory)
        except Exception:
            region.close()
            raise
        self._region, self._space_signal = region, signal

    def _encode_payload(self, topic, data):
        """使用已明确配置的编码，禁止根据本端可选库自动改变线上协议。"""
        envelope = {"combo_key": topic, "data": data}
        if self._msgpack is not None:
            return self._msgpack.packb(envelope, use_bin_type=True)
        return json.dumps(envelope, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")

    def _decode_payload(self, payload):
        """仅按启动阶段已校验的线上编码解码，不使用环境相关的猜测回退。"""
        if self._msgpack is not None:
            return self._msgpack.unpackb(payload, raw=False)
        return json.loads(payload.decode("utf-8"))

    def _header(self):
        """读取固定头；调用者必须持有区域锁。"""
        return _Header(*_HEADER.unpack_from(self._region.buffer, 0))

    def _lane(self, lane):
        """读取指定日志的递增序号与字节游标。"""
        state = _Lane(*_LANE.unpack_from(self._region.buffer, 128 + lane * _LANE.size))
        if not (1 <= state.head <= state.next_seq and state.next_seq - state.head <= self.slot_counts[lane]
                and state.reclaim_pos <= state.write_pos and state.write_pos - state.reclaim_pos <= self.capacities[lane]):
            raise PushGapError("shared push lane metadata is inconsistent")
        return state

    def _write_lane(self, lane, value):
        """原位写入日志游标，仅在消息提交或回收时调用。"""
        _LANE.pack_into(self._region.buffer, 128 + lane * _LANE.size, *value)

    def _consumer(self, slot):
        """读取固定消费者槽位。"""
        return _Consumer(*_CONSUMER.unpack_from(self._region.buffer, 256 + slot * _CONSUMER_BYTES))

    def _write_consumer(self, slot, consumer):
        """更新确认游标或租约状态，整个记录由同一互斥锁保护。"""
        _CONSUMER.pack_into(self._region.buffer, 256 + slot * _CONSUMER_BYTES, *consumer)

    def _bump_consumers(self):
        """身份或状态变化时递增表版本；单条 ACK 不唤醒不相关发布缓存。"""
        generation = _GENERATION.unpack_from(self._region.buffer, 96)[0]
        _GENERATION.pack_into(self._region.buffer, 96, generation + 1)

    def _subscribers(self, refresh=False):
        """按身份版本复用通知目标；满载时强制刷新 ACK，避免每次扫描 64 个槽。"""
        generation = _GENERATION.unpack_from(self._region.buffer, 96)[0]
        now = time.monotonic()
        if refresh or generation != self._consumer_generation or now >= self._next_maintenance:
            self._consumer_cache = self._maintain_consumers()
            self._consumer_generation = _GENERATION.unpack_from(self._region.buffer, 96)[0]
            self._next_maintenance = now + min(1.0, max(0.01, self.retention))
        return self._consumer_cache

    def _record(self, lane, seq):
        """读取序号对应的日志索引，并检查提交标记与容量边界。"""
        offset = self._index_offsets[lane] + (seq % self.slot_counts[lane]) * _RECORD.size
        record = _Record(*_RECORD.unpack_from(self._region.buffer, offset))
        state = self._lane(lane)
        if (record.seq != seq or record.commit != seq or not 0 < record.length <= self.capacities[lane]
                or not state.head <= seq < state.next_seq
                or record.pos < state.reclaim_pos or record.pos + record.length > state.write_pos):
            raise PushGapError("uncommitted or overwritten push record lane=%s sequence=%s" % (lane, seq))
        return record

    def _read_bytes(self, lane, record):
        """从可跨越环尾的日志复制消息，并验证 CRC 后交给业务解码器。"""
        capacity = self.capacities[lane]
        start = record.pos % capacity
        count = min(record.length, capacity - start)
        offset = self._data_offsets[lane]
        payload = bytes(self._region.buffer[offset + start:offset + start + count])
        if count < record.length:
            payload += bytes(self._region.buffer[offset:offset + record.length - count])
        if zlib.crc32(payload) & 0xffffffff != record.crc:
            raise PushGapError("push payload CRC mismatch lane=%s sequence=%s" % (lane, record.seq))
        return payload

    def _validate(self, abandoned=False):
        """在锁内只核对协议、代次与故障位；存活检查由 _validated_lock 在锁外完成。"""
        if abandoned:
            header = self._header()
            if header.magic == _MAGIC:
                # 异常持锁退出可能留下半写元数据，故障必须跨实例保留到发布者重建。
                _HEADER.pack_into(self._region.buffer, 0, *header._replace(health=1))
            raise PushGapError("shared push mutex owner exited during an operation")
        header = self._header()
        if header.magic != _MAGIC:
            raise SharedMemoryPushError("shared push publisher has not initialized")
        if header.version != _VERSION:
            raise SharedMemoryPushError("shared push protocol version mismatch")
        if _CODEC.unpack_from(self._region.buffer, 104)[0] != self._codec_id:
            raise SharedMemoryPushError("shared push codec mismatch: local push_codec=%s" % self.push_codec)
        expected = self.capacities + self.slot_counts + (self.max_consumers,)
        actual = (header.market_bytes, header.trade_bytes, header.market_slots, header.trade_slots, header.consumers)
        if actual != expected:
            raise SharedMemoryPushError("shared push layout mismatch")
        if self._epoch is not None and header.epoch != self._epoch:
            raise PushGapError("shared push publisher epoch changed; explicit recovery required")
        if header.health or not header.pid:
            raise PushGapError("shared push publisher is stopped or unhealthy")
        return header

    def _peer_alive(self, pid, birth):
        """当前执行进程的完整身份可直接证明存活，其余身份每轮重新向操作系统查询。"""
        if pid == os.getpid() == self._pid and birth == self._birth:
            return True
        return process_alive(pid, birth)

    @contextlib.contextmanager
    def _validated_lock(self, timeout_seconds, maintain=False, allow_missing=False,
                        force_maintenance=False, deadline=None):
        """先快照、锁外探活、再锁内核对身份并交付锁，所有等待共用秒数预算。

        maintain 仅在消费者表版本变化或维护到期时探活；回收使用同槽位同token的死亡证据。
        force_maintenance 用于日志满时立即重查死亡消费者，不能等待维护周期才回收容量。
        deadline 为上层循环已有的单调时钟截止；保留原始正预算，不能把到期剩余零秒当成显式零预算。
        allow_missing 用于首次订阅，发布者尚未出现或已退出时返回 None，由启动期限控制重试。
        零预算仍允许两次无等待抢锁；正预算包含系统探活，过期后不得继续交付可写锁。
        """
        budget = max(0.0, float(timeout_seconds))
        if deadline is None:
            deadline = time.monotonic() + budget
        if budget and time.monotonic() >= deadline:
            raise TransportTimeout("shared push operation budget exhausted")
        region = self._region
        if region is None:
            raise SharedMemoryPushError("shared push channel is not started")
        # 两段锁之间仍保有映射生命周期；并发close只能等待或使第二次加锁明确失败。
        with region._operation():
            with region.locked(min(self.lock_timeout, max(0.0, deadline - time.monotonic()))) as abandoned:
                if self._region is not region:
                    raise PushGapError("shared push mapping changed during operation")
                if abandoned:
                    self._validate(abandoned=True)
                header = self._header() if allow_missing else self._validate()
                owner = (header.epoch, header.pid, header.birth)
                ready = header.magic == _MAGIC and bool(header.pid)
                if allow_missing and ready:
                    self._validate()
                generation = _GENERATION.unpack_from(region.buffer, 96)[0]
                refresh = maintain and ready and (
                    force_maintenance or generation != self._consumer_generation
                    or time.monotonic() >= self._next_maintenance
                )
                candidates = [
                    (slot, self._consumer(slot)) for slot in range(self.max_consumers)
                ] if refresh else ()
            # /proc与Windows进程句柄调用可能释放GIL，绝不能让它们阻塞共享元数据锁。
            alive = ready and self._peer_alive(header.pid, header.birth)
            dead = {
                (slot, consumer.identity, consumer.token, consumer.pid, consumer.birth)
                for slot, consumer in candidates
                if consumer.state in (_ACTIVE, _FAILED)
                and not self._peer_alive(consumer.pid, consumer.birth)
            }
            if budget and time.monotonic() >= deadline:
                raise TransportTimeout("shared push liveness check exhausted operation budget")
            with region.locked(min(self.lock_timeout, max(0.0, deadline - time.monotonic()))) as abandoned:
                if self._region is not region:
                    raise PushGapError("shared push mapping changed during operation")
                if abandoned:
                    self._validate(abandoned=True)
                if budget and time.monotonic() >= deadline:
                    raise TransportTimeout("shared push liveness check exhausted operation budget")
                current = self._header()
                if (current.epoch, current.pid, current.birth) != owner:
                    if allow_missing:
                        yield None
                        return
                    raise PushGapError("shared push publisher identity changed during liveness check")
                if allow_missing and not alive:
                    yield None
                    return
                self._validate()
                if not alive:
                    raise PushGapError("shared push publisher is stopped or unhealthy")
                if refresh:
                    self._consumer_cache = self._maintain_consumers(dead)
                    self._consumer_generation = _GENERATION.unpack_from(region.buffer, 96)[0]
                    self._next_maintenance = time.monotonic() + min(1.0, max(0.01, self.retention))
                yield current

    def start_publisher(self):
        """取得单发布者所有权；重启显式开启新代次，使旧消费者报告不连续。"""
        if self.role != "server":
            raise SharedMemoryPushError("only server may start publisher")
        if self._started:
            self.check_health()
            return
        deadline = time.monotonic() + self.init_timeout
        self._open(max(0.0, deadline - time.monotonic()))
        try:
            with self._region._operation():
                with self._region.locked(max(0.0, deadline - time.monotonic())):
                    previous = self._header()
                alive = previous.pid and self._peer_alive(previous.pid, previous.birth)
                if self.init_timeout and time.monotonic() >= deadline:
                    raise TransportTimeout("shared push publisher initialization budget exhausted")
                with self._region.locked(max(0.0, deadline - time.monotonic())):
                    # 竞争者可能已完成接管，旧探活结果绝不能覆盖它的新代次。
                    if self._header() != previous:
                        raise SharedMemoryPushError("shared push publisher ownership changed during initialization")
                    if self.init_timeout and time.monotonic() >= deadline:
                        raise TransportTimeout("shared push publisher initialization budget exhausted")
                    if alive:
                        raise SharedMemoryPushError("shared push publisher already owns this account and namespace")
                    self._epoch = uuid.uuid4().bytes
                    # 只初始化元数据；数据区由提交索引限定，无须每次清零 32 MiB。
                    self._region.buffer[:self._data_offsets[0]] = b"\0" * self._data_offsets[0]
                    header = _Header(_MAGIC, _VERSION, self._epoch, self._pid, self._birth,
                                     self.capacities[0], self.capacities[1],
                                     self.slot_counts[0], self.slot_counts[1], self.max_consumers, 0)
                    _HEADER.pack_into(self._region.buffer, 0, *header)
                    _CODEC.pack_into(self._region.buffer, 104, self._codec_id)
                    self._write_lane(0, _Lane(1, 1, 0, 0))
                    self._write_lane(1, _Lane(1, 1, 0, 0))
            self._error = None
            self._published_ids.clear()
            self._consumer_generation = None
            self._started = self._running = True
        except Exception:
            self._close_handles()
            raise

    def _maintain_consumers(self, dead=()):
        """锁内仅应用锁外取得的同身份死亡证据及到期规则，保留期间推进的最新ACK。"""
        now = int(time.time() * 1000000000)
        consumers = []
        changed = False
        for slot in range(self.max_consumers):
            consumer = self._consumer(slot)
            identity = (slot, consumer.identity, consumer.token, consumer.pid, consumer.birth)
            if consumer.state in (_ACTIVE, _FAILED) and (consumer.pid <= 0 or identity in dead):
                if consumer.stable & (1 | _CALLBACK_FAILED_FLAG):
                    consumer = consumer._replace(state=_RETAINED, expires=now + int(self.retention * 1000000000))
                else:
                    consumer = consumer._replace(state=0, identity=_EMPTY_ID)
                self._write_consumer(slot, consumer)
                changed = True
            if consumer.state == _RETAINED and now >= consumer.expires:
                consumer = consumer._replace(state=_EXPIRED)
                self._write_consumer(slot, consumer)
                changed = True
            consumers.append((slot, consumer))
        if changed:
            self._bump_consumers()
        return consumers

    def _reclaim(self, lane, consumers):
        """只回收所有相关消费者已确认的记录；没有消费者时保留待取消息。"""
        cursors = []
        for slot, consumer in consumers:
            if consumer.state not in (_ACTIVE, _RETAINED, _FAILED):
                continue
            # stable 的最低位记录稳定身份，其余位记录相关 lane，避免未订阅的日志互相阻塞。
            if (consumer.stable >> 1) & (1 << lane):
                cursors.append(consumer.market_cursor if lane == 0 else consumer.trade_cursor)
        state = self._lane(lane)
        if not cursors:
            # 首位消费者注册前保留积压；此前身份显式释放或到期后才允许回收。
            if not any((consumer.stable >> 1) & (1 << lane) for slot, consumer in consumers):
                return state
            acknowledged = state.next_seq - 1
        else:
            acknowledged = min(cursors)
        head, position = state.head, state.reclaim_pos
        while head < state.next_seq and head <= acknowledged:
            record = self._record(lane, head)
            if record.pos != position:
                raise PushGapError("shared push retained byte range has a gap")
            position = record.pos + record.length
            if record.message_id != _EMPTY_ID:
                self._published_ids.pop(record.message_id, None)
            head += 1
        if head != state.head:
            state = state._replace(head=head, reclaim_pos=position)
            self._write_lane(lane, state)
        return state

    def _signal_name(self, identity):
        """每个订阅者使用独立提示，避免单个自动复位信号吞掉其它消费者唤醒。"""
        return endpoint_name(self.namespace, self.account_id, "push-data-" + identity.hex())

    def _notify(self, consumers, lane):
        """提交后尽力唤醒；失败仅记录，不能把已提交消息伪装为未提交。"""
        # 通知在共享区域锁外执行，独立锁防止并发发布重复打开同名句柄。
        # 临时订阅重连会更换身份，及时关闭离线身份，句柄数才受消费者槽位上限约束。
        active = {consumer.identity for slot, consumer in consumers if consumer.state == _ACTIVE}
        with self._signals_lock:
            if not self._started:
                # 停止可发生在消息提交和锁外通知之间，此时不能重新打开已回收的句柄。
                return
            for identity in list(self._signals):
                if identity not in active:
                    try:
                        self._signals.pop(identity).close()
                    except Exception:
                        self._signal_errors += 1
            for slot, consumer in consumers:
                if consumer.state != _ACTIVE or not ((consumer.stable >> 1) & (1 << lane)):
                    continue
                try:
                    signal = self._signals.get(consumer.identity)
                    if signal is None:
                        signal = NamedSignal(self._signal_name(consumer.identity), directory=self.directory)
                        self._signals[consumer.identity] = signal
                    if signal.set() is False:
                        self._missed_hints += 1
                except Exception:
                    self._signal_errors += 1

    def publish(self, topic, data, message_id=None, timeout_seconds=None):
        """提交消息；timeout_seconds 覆盖总等待预算，0 表示立即尝试。

        返回 True 表示已提交或同 ID 同内容已经提交。ID 幂等只覆盖仍保留的日志。
        exec: 主题进入独立交易日志；容量不足抛可重试背压，绝不覆盖未确认消息。
        """
        if not self._started or self.role != "server" or self._region is None:
            raise SharedMemoryPushError("shared push publisher is not started")
        if self._error is not None:
            raise self._error
        topic = str(topic)
        lane = int(topic.startswith("exec:"))
        payload = self._encode_payload(topic, data)
        if len(payload) > self.capacities[lane]:
            raise SharedMemoryPushError("push payload exceeds lane capacity")
        budget = self.publish_timeout if timeout_seconds is None else _seconds(
            {"timeout": timeout_seconds}, "timeout", self.publish_timeout)
        deadline = time.monotonic() + budget
        identity = _EMPTY_ID if message_id is None else hashlib.sha256(str(message_id).encode("utf-8")).digest()
        force_maintenance = False
        while True:
            try:
                with self._validated_lock(budget, maintain=True,
                                          force_maintenance=force_maintenance, deadline=deadline):
                    consumers = self._subscribers()
                    previous = self._published_ids.get(identity) if identity != _EMPTY_ID else None
                    if previous is not None:
                        previous_lane, seq = previous
                        record = self._record(previous_lane, seq)
                        if previous_lane != lane or self._read_bytes(previous_lane, record) != payload:
                            raise SharedMemoryPushError("message_id reused with different push payload")
                        return True
                    state = self._lane(lane)
                    if state.next_seq - state.head >= self.slot_counts[lane] or state.write_pos - state.reclaim_pos + len(payload) > self.capacities[lane]:
                        # 有空余空间时保留历史即可；真正遇到容量边界才批量读取 ACK 并回收。
                        consumers = self._subscribers(refresh=True)
                        state = self._reclaim(lane, consumers)
                        if (state.next_seq - state.head >= self.slot_counts[lane]
                                or state.write_pos - state.reclaim_pos + len(payload) > self.capacities[lane]) and not force_maintenance:
                            if budget and time.monotonic() >= deadline:
                                raise PushBackpressureError("shared push lane full; unacknowledged messages retained")
                            # 缓存中的消费者可能刚退出；释放元数据锁后沿同一截止强制探活一次。
                            force_maintenance = True
                            continue
                    if state.next_seq - state.head < self.slot_counts[lane] and state.write_pos - state.reclaim_pos + len(payload) <= self.capacities[lane]:
                        seq = state.next_seq
                        offset = self._index_offsets[lane] + (seq % self.slot_counts[lane]) * _RECORD.size
                        record = _Record(seq, state.write_pos, len(payload), zlib.crc32(payload) & 0xffffffff, 0, identity)
                        _RECORD.pack_into(self._region.buffer, offset, *record)
                        start = state.write_pos % self.capacities[lane]
                        count = min(len(payload), self.capacities[lane] - start)
                        base = self._data_offsets[lane]
                        self._region.buffer[base + start:base + start + count] = payload[:count]
                        if count < len(payload):
                            self._region.buffer[base:base + len(payload) - count] = payload[count:]
                        # 提交标记最后写，随后发布可见序号；读者同样在锁内检查两者。
                        _RECORD.pack_into(self._region.buffer, offset, *record._replace(commit=seq))
                        self._write_lane(lane, state._replace(next_seq=seq + 1, write_pos=state.write_pos + len(payload)))
                        if identity != _EMPTY_ID:
                            self._published_ids[identity] = (lane, seq)
                        self._published += 1
                        break
            except TransportTimeout as exc:
                raise PushLockBusyError("shared push lock budget exhausted: %s" % exc)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PushBackpressureError("shared push lane full; unacknowledged messages retained")
            self._space_signal.wait(min(remaining, self.poll_interval))
        self._notify(consumers, lane)
        return True

    def start_subscriber(self, topics, on_msg):
        """有界等待发布者，注册或恢复独立游标；回调成功后才确认对应记录。"""
        if self.role != "client":
            raise SharedMemoryPushError("only client may start subscriber")
        if self._started:
            raise SharedMemoryPushError("shared push subscriber is already started")
        if not callable(on_msg):
            raise ValueError("on_msg must be callable")
        self._topics = set(str(topic) for topic in (topics or []))
        if not self._topics:
            raise ValueError("at least one push topic is required")
        fingerprint = hashlib.sha256(json.dumps(sorted(self._topics)).encode("utf-8")).digest()
        lane_mask = (1 if any(not topic.startswith("exec:") for topic in self._topics) else 0) | (2 if any(topic.startswith("exec:") for topic in self._topics) else 0)
        self._lanes = tuple(lane for lane in (1, 0) if lane_mask & (1 << lane))
        self._token = uuid.uuid4().bytes
        self._error = None
        self._epoch = None
        deadline = time.monotonic() + self.init_timeout
        self._open(max(0.0, deadline - time.monotonic()))
        try:
            while True:
                with self._validated_lock(self.init_timeout, maintain=True, allow_missing=True,
                                          deadline=deadline) as header:
                    if header is not None:
                        self._epoch = header.epoch
                        consumers = self._maintain_consumers()
                        found = next(((slot, consumer) for slot, consumer in consumers if consumer.identity == self._identity and consumer.state), None)
                        if found is not None:
                            slot, consumer = found
                            if consumer.state == _EXPIRED:
                                fresh_quote = (lane_mask == 1
                                               and ((consumer.stable >> 1) & 3) == 1
                                               and self._pending_topic_update == fingerprint
                                               and consumer.token == self._topic_update_owner
                                               and not consumer.stable & _CALLBACK_FAILED_FLAG)
                                if not fresh_quote:
                                    raise PushGapError("subscriber retention expired; explicit new subscription required")
                                # 原持有者正常全部退订后主动新订阅，按新行情基线建立游标。
                                # 交易日志、重建实例和回调失败仍不能丢弃旧游标或掩盖缺口。
                                consumer = consumer._replace(market_cursor=self._lane(0).head - 1)
                            if consumer.state in (_ACTIVE, _FAILED):
                                raise SharedMemoryPushError("subscriber_id is already active")
                            if consumer.topics != fingerprint:
                                permitted = (self._pending_topic_update == fingerprint
                                             and consumer.token == self._topic_update_owner
                                             and not consumer.stable & _CALLBACK_FAILED_FLAG
                                             and ((consumer.stable >> 1) & 3) == lane_mask)
                                if not permitted:
                                    raise SharedMemoryPushError("subscriber_id topics changed; explicit new identity required")
                        else:
                            free = next((slot for slot, consumer in consumers if consumer.state == 0), None)
                            if free is None:
                                raise SharedMemoryPushError("shared push subscriber table is full")
                            slot = free
                            consumer = _Consumer(0, 0, self._identity, fingerprint, 0, 0, 0, 0,
                                                 self._lane(0).head - 1, self._lane(1).head - 1, self._token)
                        consumer = consumer._replace(state=_ACTIVE, stable=int(self.stable) | (lane_mask << 1), topics=fingerprint,
                                                     pid=self._pid, birth=self._birth, seen=int(time.time() * 1000000000), expires=0, token=self._token)
                        self._write_consumer(slot, consumer)
                        self._bump_consumers()
                        self._slot = slot
                        self._pending_topic_update = self._topic_update_owner = None
                        break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise SharedMemoryPushError("shared push publisher initialization timeout")
                self._space_signal.wait(min(remaining, self.poll_interval))
            self._data_signal = NamedSignal(self._signal_name(self._identity), directory=self.directory)
            self._running = self._started = True
            self._thread = threading.Thread(target=self._consume, args=(on_msg,), name="bigqmt-shm-push", daemon=True)
            self._thread.start()
        except Exception as exc:
            self._running = False
            try:
                self._release_consumer()
            finally:
                self._close_handles()
                self._started = False
            if isinstance(exc, TransportTimeout) and time.monotonic() >= deadline:
                # 两段锁共用启动截止，但对调用方仍保留原有的初始化超时错误语义。
                raise SharedMemoryPushError("shared push publisher initialization timeout") from exc
            raise

    def _owned_consumer(self):
        """确认当前实例仍持有消费者槽，避免重建后旧线程更新新游标。"""
        consumer = self._consumer(self._slot)
        if consumer.identity != self._identity or consumer.token != self._token or consumer.state != _ACTIVE:
            raise PushGapError("shared push subscriber ownership was lost")
        return consumer

    def update_topics(self, topics):
        """在相同日志范围内原地更新主题，保留已确认游标和正在执行的回调。

        新主题只接收当前游标之后的消息；跨行情/交易日志迁移需要显式新订阅身份。
        同一实例正常停止后可预设下次主题；恢复时仍验证原槽所有权，不能替代故障恢复。
        """
        selected = set(str(topic) for topic in (topics or []))
        if not selected:
            raise ValueError("at least one push topic is required")
        lanes = tuple(lane for lane in (1, 0) if any(int(topic.startswith("exec:")) == lane for topic in selected))
        if lanes != self._lanes:
            raise SharedMemoryPushError("topic update cannot change lanes; explicit new subscription required")
        if self._error is not None:
            raise self._error
        fingerprint = hashlib.sha256(json.dumps(sorted(selected)).encode("utf-8")).digest()
        if not self._started and self._region is None and self.role == "client" and self._epoch is not None:
            self._pending_topic_update = fingerprint
            self._topic_update_owner = self._token
            self._topics = selected
            return True
        if not self._started or self.role != "client" or self._region is None:
            raise SharedMemoryPushError("shared push subscriber is not started")
        with self._validated_lock(self.lock_timeout):
            consumer = self._owned_consumer()
            self._write_consumer(self._slot, consumer._replace(topics=fingerprint))
            self._topics = selected
        return True

    def _next_message(self, lane):
        """在锁内复制一条待确认消息；业务解码和回调在锁外执行。"""
        with self._validated_lock(self.lock_timeout):
            consumer = self._owned_consumer()
            if not ((consumer.stable >> 1) & (1 << lane)):
                return None
            seq = (consumer.market_cursor if lane == 0 else consumer.trade_cursor) + 1
            state = self._lane(lane)
            if seq < state.head:
                raise PushGapError("shared push cursor fell behind retained history")
            if seq >= state.next_seq:
                return None
            return seq, self._read_bytes(lane, self._record(lane, seq))

    def _ack(self, lane, seq):
        """仅在对应回调成功后推进一次确认，后续订阅实例可据此恢复。"""
        with self._validated_lock(self.lock_timeout):
            consumer = self._owned_consumer()
            cursor = consumer.market_cursor if lane == 0 else consumer.trade_cursor
            if cursor + 1 != seq:
                raise PushGapError("shared push acknowledgement is not contiguous")
            consumer = consumer._replace(market_cursor=seq) if lane == 0 else consumer._replace(trade_cursor=seq)
            self._write_consumer(self._slot, consumer)
            self._acknowledged += 1
        try:
            if self._space_signal.set() is False:
                self._missed_hints += 1
        except Exception:
            self._signal_errors += 1

    def _consume(self, on_msg):
        """公平轮询两个日志，回调失败保持游标并退出，避免无界重复副作用。"""
        try:
            while self._running:
                progress = False
                for lane in self._lanes:
                    if not self._running:
                        break
                    message = self._next_message(lane)
                    if message is None:
                        continue
                    progress = True
                    seq, raw = message
                    decoded = self._decode_payload(raw)
                    if not isinstance(decoded, dict) or "combo_key" not in decoded:
                        raise PushGapError("shared push message envelope is invalid")
                    topic = decoded["combo_key"]
                    if topic in self._topics:
                        try:
                            on_msg(topic, decoded.get("data"))
                        except Exception as exc:
                            raise PushCallbackError("push callback failed; message is unacknowledged: %s" % exc)
                    # 回调内 stop 只设置停止标记，仍须先确认这条成功消息再退出。
                    self._ack(lane, seq)
                if not progress and self._running:
                    self._data_signal.wait(self.poll_interval)
        except Exception as exc:
            self._error = exc if isinstance(exc, SharedMemoryPushError) else SharedMemoryPushError(str(exc))
            try:
                with self._region.locked(self.lock_timeout) as abandoned:
                    if abandoned:
                        self._validate(abandoned=True)
                    consumer = self._consumer(self._slot)
                    if consumer.token == self._token:
                        flags = consumer.stable
                        if isinstance(self._error, PushCallbackError):
                            # 业务失败证据独立于租约状态；普通 stop 或死亡回收不能清除交易门禁。
                            flags |= _CALLBACK_FAILED_FLAG
                        self._write_consumer(self._slot, consumer._replace(state=_FAILED, stable=flags))
                        self._bump_consumers()
            except Exception:
                pass
        finally:
            self._running = False
            # 正常 stop 才释放身份，错误态保留槽和未确认记录供健康检查。
            if self._error is None:
                try:
                    self._release_consumer()
                    self._close_handles()
                    self._started = False
                except Exception as exc:
                    self._error = SharedMemoryPushError("subscriber cleanup failed: %s" % exc)

    def check_health(self):
        """快速检查发布代次与消费者错误；健康时返回状态摘要，异常时明确抛错。"""
        if self._error is not None:
            raise self._error
        if not self._started or self._region is None:
            raise SharedMemoryPushError("shared push channel is not started")
        try:
            with self._validated_lock(min(self.lock_timeout, 0.02)):
                if self.role == "client":
                    self._owned_consumer()
                    if not self._running or self._thread is None or not self._thread.is_alive():
                        raise SharedMemoryPushError("shared push subscriber worker exited")
                else:
                    consumers = [self._consumer(slot) for slot in range(self.max_consumers)]
                    if any(consumer.state == _FAILED or consumer.stable & _CALLBACK_FAILED_FLAG for consumer in consumers):
                        raise PushCallbackError("a shared push subscriber callback failed with unacknowledged messages")
        except TransportTimeout as exc:
            raise PushLockBusyError("shared push health check lock busy: %s" % exc)
        return self.status()

    def status(self):
        """返回不触发重试或回调的本地诊断快照。"""
        return {"backend": "shm", "role": self.role, "push_codec": self.push_codec, "started": self._started,
                "running": self._running, "epoch": self._epoch.hex() if self._epoch else "",
                "subscriber_id": self.subscriber_id if self.role == "client" else "",
                "published": self._published, "acknowledged": self._acknowledged,
                "signal_errors": self._signal_errors,
                "missed_hints": self._missed_hints,
                "callback_failed": isinstance(self._error, PushCallbackError),
                "error": str(self._error) if self._error is not None else ""}

    def _release_consumer(self):
        """停止时保留稳定身份的未确认游标，临时身份则显式释放。"""
        if self._region is None or self._slot is None:
            return
        with self._region.locked(self.lock_timeout) as abandoned:
            if abandoned:
                self._validate(abandoned=True)
            if self._header().epoch == self._epoch:
                consumer = self._consumer(self._slot)
                if consumer.token == self._token:
                    if self.stable or consumer.stable & _CALLBACK_FAILED_FLAG:
                        consumer = consumer._replace(state=_RETAINED, expires=int((time.time() + self.retention) * 1000000000))
                    else:
                        consumer = consumer._replace(state=0, identity=_EMPTY_ID)
                    self._write_consumer(self._slot, consumer)
                    self._bump_consumers()
        # 锁超时保留槽引用，使调用方之后显式 stop 仍可重试释放。
        self._slot = None

    def _close_handles(self):
        """关闭本实例拥有的映射和信号，不删除仍可能被其它进程使用的命名资源。"""
        with self._signals_lock:
            self._started = False
            signals = list(self._signals.values())
            self._signals.clear()
        for signal in signals + [self._data_signal, self._space_signal]:
            if signal is not None:
                try:
                    signal.close()
                except Exception:
                    pass
        self._data_signal = self._space_signal = None
        if self._region is not None:
            self._region.close()
            self._region = None

    def stop(self):
        """有界停止；回调内调用由消费线程完成 ACK 后自行清理。"""
        self._running = False
        thread = self._thread
        if thread is threading.current_thread():
            return
        if self._data_signal is not None:
            try:
                self._data_signal.set()
            except Exception:
                pass
        if thread is not None and thread.is_alive():
            thread.join(max(1.0, self.lock_timeout + self.poll_interval))
            if thread.is_alive():
                raise SharedMemoryPushError("subscriber callback is still running; resources retained")
        if self.role == "server" and self._region is not None and self._started:
            with self._region.locked(self.lock_timeout):
                header = self._header()
                if header.epoch == self._epoch and header.pid == self._pid and header.birth == self._birth:
                    _HEADER.pack_into(self._region.buffer, 0, *header._replace(pid=0))
        self._release_consumer()
        self._close_handles()
        self._thread = None
        self._started = False
