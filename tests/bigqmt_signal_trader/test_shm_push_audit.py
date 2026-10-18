"""共享推送故障注入审计：资源上限、协议损坏与生命周期边界。"""

import contextlib
import threading
import time

import pytest

from bigqmt_signal_trader import shm_push_channel as module
from bigqmt_signal_trader.shm_push_channel import (
    PushBackpressureError, PushCallbackError, PushGapError,
    SharedMemoryPushError, SharedMemoryQuotePushChannel,
)
from bigqmt_signal_trader.transports.base import TransportTimeout
from bigqmt_signal_trader.transports.shm_primitives import cleanup


def _wait(predicate, timeout=3):
    """等待可观察的协议状态，使用有界时间而不假设线程调度顺序。"""
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "shared push state timed out"
        time.sleep(.002)


@pytest.fixture
def factory(tmp_path):
    """提供真实独立命名映射，退出时回收全部实例和测试内核资源。"""
    instances = []
    settings = dict(directory=str(tmp_path), namespace=str(tmp_path),
                    market_push_bytes=4096, trade_push_bytes=4096,
                    market_push_slots=8, trade_push_slots=8,
                    max_push_subscribers=8, init_timeout_seconds=.05,
                    lock_timeout_seconds=.05, push_poll_seconds=.002,
                    publish_timeout_seconds=.02, subscriber_retention_seconds=.1)

    def build(role, subscriber_id=None, **overrides):
        """配置覆盖仅影响当前测试实例，容量与对端保持一致。"""
        config = dict(settings, **overrides)
        if subscriber_id is not None:
            config["subscriber_id"] = subscriber_id
        instance = SharedMemoryQuotePushChannel(role, config, "audit")
        instances.append(instance)
        return instance

    yield build
    names = set()
    for instance in reversed(instances):
        names.update((instance._name, instance._space_name,
                      instance._signal_name(instance._identity)))
        names.update(instance._signal_name(identity) for identity in instance._signals)
        instance.stop()
    for name in names:
        cleanup(name, directory=str(tmp_path))


def _registered_without_worker(client, monkeypatch, topics=("ticks",)):
    """暂停消费循环以便锁内注入协议故障，注册仍走真实共享映射。"""
    with monkeypatch.context() as scoped:
        scoped.setattr(client, "_consume", lambda callback: None)
        client.start_subscriber(topics, lambda topic, data: None)
        client._thread.join(1)
    return client


@pytest.mark.parametrize("config", [
    {"market_push_bytes": 0}, {"trade_push_slots": -1},
    {"init_timeout_seconds": -1}, {"lock_timeout_seconds": float("inf")},
    {"push_poll_seconds": float("nan")}, {"push_codec": "pickle"},
])
def test_invalid_configuration_never_opens_resources(config):
    """非法容量、预算和未协商编码在打开资源前明确拒绝。"""
    with pytest.raises(ValueError):
        SharedMemoryQuotePushChannel(config=config)


def test_wrong_roles_callbacks_topics_and_inactive_channels(factory):
    """角色、空主题和失效通道不能以假成功进入业务循环。"""
    with pytest.raises(ValueError, match="role"):
        SharedMemoryQuotePushChannel("peer")
    server, client = factory("server"), factory("client")
    with pytest.raises(SharedMemoryPushError, match="only server"):
        client.start_publisher()
    with pytest.raises(SharedMemoryPushError, match="only client"):
        server.start_subscriber(["ticks"], lambda topic, data: None)
    with pytest.raises(ValueError, match="callable"):
        client.start_subscriber(["ticks"], None)
    with pytest.raises(ValueError, match="topic"):
        client.start_subscriber([], lambda topic, data: None)
    with pytest.raises(ValueError, match="topic"):
        client.update_topics([])
    client._lanes = (0,)
    with pytest.raises(SharedMemoryPushError, match="not started"):
        client.update_topics(["ticks"])
    with pytest.raises(SharedMemoryPushError, match="not started"):
        client.check_health()
    server.start_publisher()
    server.start_publisher()
    client.start_subscriber(["ticks"], lambda topic, data: None)
    with pytest.raises(SharedMemoryPushError, match="already started"):
        client.start_subscriber(["ticks"], lambda topic, data: None)
    assert server.publisher_endpoint.startswith("shm://")


def test_partial_open_failure_closes_mapping_and_open_is_idempotent(factory, monkeypatch):
    """通知对象创建失败须释放先前映射；后续显式重试可正常启动。"""
    server = factory("server")
    regions = []
    original_region = module.SharedRegion

    def capture_region(*args, **kwargs):
        """记录真实映射以验证构造失败后已关闭。"""
        region = original_region(*args, **kwargs)
        regions.append(region)
        return region

    def fail_signal(*args, **kwargs):
        """模拟命名事件分配额度耗尽。"""
        raise OSError("event quota")

    with monkeypatch.context() as scoped:
        scoped.setattr(module, "SharedRegion", capture_region)
        scoped.setattr(module, "NamedSignal", fail_signal)
        with pytest.raises(OSError, match="quota"):
            server.start_publisher()
    assert server._region is None
    assert regions[0].buffer is None
    server.start_publisher()
    region = server._region
    server._open()
    assert server._region is region


def test_ephemeral_reconnect_bounds_publisher_notification_handles(factory):
    """重复连接更换临时身份时，发布端事件句柄数仍受消费者容量约束。"""
    server = factory("server", max_push_subscribers=1)
    server.start_publisher()
    retired = []
    for index in range(25):
        client = factory("client", max_push_subscribers=1)
        received = []
        client.start_subscriber(["ticks"], lambda topic, data: received.append(data))
        server.publish("ticks", index)
        _wait(lambda: client.status()["acknowledged"] >= 1 and received[-1] == index)
        assert len(server._signals) == 1
        retired.extend(server._signals.values())
        client.stop()
    server.publish("ticks", 26)
    assert not server._signals
    assert all(signal._closed for signal in retired)


def test_concurrent_publish_opens_one_notification_handle(factory, monkeypatch):
    """并发提交同一消费者通知时不会竞态重复分配或泄漏事件句柄。"""
    server = factory("server", market_push_slots=64)
    client = factory("client", market_push_slots=64)
    server.start_publisher()
    received = []
    client.start_subscriber(["ticks"], lambda topic, data: received.append(data))
    original_signal = module.NamedSignal
    opened = []

    def slow_open(name, directory=None):
        """扩大首次打开通知句柄的竞争窗口。"""
        time.sleep(.005)
        signal = original_signal(name, directory=directory)
        opened.append(signal)
        return signal

    monkeypatch.setattr(module, "NamedSignal", slow_open)
    barrier = threading.Barrier(12)
    errors = []

    def publish(value):
        """并行写入可区分数据，保存线程内错误供主线程断言。"""
        try:
            barrier.wait(2)
            server.publish("ticks", value, timeout_seconds=1)
        except Exception as exc:
            errors.append(exc)

    workers = [threading.Thread(target=publish, args=(index,)) for index in range(12)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(3)
    _wait(lambda: client.status()["acknowledged"] == 12)
    assert not errors
    assert sorted(received) == list(range(12))
    assert len(opened) == 1


def test_stop_between_commit_and_notification_does_not_reopen_handles(factory, monkeypatch):
    """提交后停止与通知交错，消息仍已提交但不得重新打开已回收事件。"""
    server, client = factory("server"), factory("client", "stop-race")
    server.start_publisher()
    _registered_without_worker(client, monkeypatch)
    committed, resume = threading.Event(), threading.Event()
    original_notify = server._notify
    results = []

    def delayed_notify(consumers, lane):
        """停在共享提交已结束、通知句柄尚未打开的边界。"""
        committed.set()
        assert resume.wait(2)
        return original_notify(consumers, lane)

    def publish():
        """保存发布结果，确认停止没有把已提交消息改成可重试失败。"""
        results.append(server.publish("ticks", 1))

    monkeypatch.setattr(server, "_notify", delayed_notify)
    worker = threading.Thread(target=publish)
    worker.start()
    try:
        assert committed.wait(1)
        server.stop()
    finally:
        resume.set()
        worker.join(2)
    assert results == [True]
    assert not server._signals
    assert server._region is None


@pytest.mark.parametrize("changed,error", [
    ({"magic": b"BADMAGIC"}, "initialized"),
    ({"market_slots": 999}, "layout mismatch"),
])
def test_corrupt_header_rejects_health_and_submission(factory, changed, error):
    """头部损坏和布局漂移不得继续写入或声称服务健康。"""
    server = factory("server")
    server.start_publisher()
    with server._region.locked(1):
        header = server._header()
        module._HEADER.pack_into(server._region.buffer, 0, *header._replace(**changed))
    with pytest.raises(SharedMemoryPushError, match=error):
        server.check_health()
    if "magic" in changed:
        with pytest.raises(PushGapError, match="mutex owner"):
            server._validate(abandoned=True)
    with server._region.locked(1):
        module._HEADER.pack_into(server._region.buffer, 0, *header)


@pytest.mark.parametrize("lane_change", [
    {"head": 0}, {"next_seq": 100}, {"reclaim_pos": 1}, {"write_pos": 8192},
])
def test_lane_corruption_never_reclaims_or_submits(factory, lane_change):
    """索引顺序、容量或字节游标损坏一律失败关闭。"""
    server = factory("server")
    server.start_publisher()
    with server._region.locked(1):
        state = server._lane(0)
        server._write_lane(0, state._replace(**lane_change))
    with pytest.raises(PushGapError, match="metadata"):
        server.publish("ticks", 1)
    assert server.status()["published"] == 0
    with server._region.locked(1):
        server._write_lane(0, state)


def test_subscriber_table_full_is_explicit_and_does_not_steal_slot(factory):
    """消费者表满时拒绝新身份，原消费者仍能收到并确认数据。"""
    server = factory("server", max_push_subscribers=1)
    first, second = (factory("client", identity, max_push_subscribers=1)
                     for identity in ("first", "second"))
    server.start_publisher()
    first.start_subscriber(["ticks"], lambda topic, data: None)
    with pytest.raises(SharedMemoryPushError, match="table is full"):
        second.start_subscriber(["ticks"], lambda topic, data: None)
    server.publish("ticks", 1)
    _wait(lambda: first.status()["acknowledged"] == 1)
    assert second._slot is None and second._region is None


def test_dead_ephemeral_consumer_does_not_retain_unowned_history(factory, monkeypatch):
    """死亡临时身份无需保留确认游标，回收后新实例可占用同一槽。"""
    server, client = factory("server"), factory("client")
    server.start_publisher()
    _registered_without_worker(client, monkeypatch)
    with server._region.locked(1):
        consumer = server._consumer(client._slot)
        server._write_consumer(client._slot, consumer._replace(pid=2147483647, birth=1))
        server._bump_consumers()
    # 真实发布先在锁外核验死亡身份，再锁内回收，不能由持锁测试直接执行操作系统探活。
    assert server.publish("exec:order", {"order_id": 1})
    with server._region.locked(1):
        consumer = server._consumer(client._slot)
        assert consumer.state == 0
        assert consumer.identity == module._EMPTY_ID


def test_reclaim_ignores_other_lane_and_discards_only_acknowledged_records(factory, monkeypatch):
    """行情确认仅回收行情消息；交易消费者不会阻塞行情日志。"""
    server, client = factory("server"), factory("client", "trade")
    server.start_publisher()
    _registered_without_worker(client, monkeypatch, ("exec:order",))
    server.publish("ticks", 1, message_id="market")
    with server._region.locked(1):
        assert server._reclaim(0, server._maintain_consumers()).head == 1
        state = server._lane(0)
        record = server._record(0, 1)
        server._write_lane(0, state._replace(write_pos=state.write_pos + 1))
        offset = server._index_offsets[0] + module._RECORD.size
        module._RECORD.pack_into(server._region.buffer, offset, *record._replace(pos=1))
        fake = server._consumer(client._slot)._replace(stable=2, market_cursor=1)
        with pytest.raises(PushGapError, match="byte range"):
            server._reclaim(0, [(0, fake)])
        module._RECORD.pack_into(server._region.buffer, offset, *record)
        server._write_lane(0, state)
        assert server._reclaim(0, [(0, fake)]).head == 2
        assert not server._published_ids


def test_signal_failures_after_ack_do_not_repeat_callback(factory, monkeypatch):
    """确认后的提示失败不能撤销 ACK 或导致业务回调被重复执行。"""
    server, client = factory("server"), factory("client", "hints")
    server.start_publisher()
    received = []
    client.start_subscriber(["ticks"], lambda topic, data: received.append(data))
    monkeypatch.setattr(client._space_signal, "set", lambda: False)
    server.publish("ticks", 1)
    _wait(lambda: client.status()["missed_hints"] == 1)

    def fail_hint():
        """模拟提交之后的事件对象写入错误。"""
        raise OSError("hint failure")

    monkeypatch.setattr(client._space_signal, "set", fail_hint)
    monkeypatch.setattr(server._signals[client._identity], "set", lambda: False)
    server.publish("ticks", 2)
    _wait(lambda: client.status()["signal_errors"] == 1)
    assert received == [1, 2]
    assert client.status()["acknowledged"] == 2
    assert server.status()["missed_hints"] == 1


def test_notify_cleanup_error_is_visible_but_does_not_change_commit(factory, monkeypatch):
    """过期通知句柄关闭失败只记录诊断，已经提交的消息仍返回成功。"""
    server, client = factory("server"), factory("client")
    server.start_publisher()
    client.start_subscriber(["ticks"], lambda topic, data: None)
    server.publish("ticks", 1)
    _wait(lambda: client.status()["acknowledged"] == 1)
    signal = server._signals[client._identity]
    original_close = signal.close

    def fail_after_close():
        """模拟内核资源已释放但关闭通知返回错误。"""
        original_close()
        raise OSError("close notification")

    monkeypatch.setattr(signal, "close", fail_after_close)
    client.stop()
    assert server.publish("ticks", 2)
    assert server.status()["signal_errors"] == 1
    assert not server._signals


def test_zero_budget_full_lane_and_retry_after_ack(factory, monkeypatch):
    """满日志非阻塞发布明确未提交；确认释放容量后相同请求可重试。"""
    server = factory("server", market_push_slots=1)
    client = factory("client", "manual", market_push_slots=1)
    server.start_publisher()
    _registered_without_worker(client, monkeypatch)
    server.publish("ticks", 1)
    with pytest.raises(PushBackpressureError, match="lane full"):
        server.publish("ticks", 2, timeout_seconds=0)
    client._ack(0, 1)
    assert server.publish("ticks", 2, timeout_seconds=0)
    assert server.status()["published"] == 2


def test_manual_cursor_guard_rejects_gaps_wrong_ack_and_lost_owner(factory, monkeypatch):
    """过期游标、跳号确认与槽被接管均不能推进 ACK。"""
    server, client = factory("server"), factory("client", "manual")
    server.start_publisher()
    _registered_without_worker(client, monkeypatch)
    assert client._next_message(1) is None
    with pytest.raises(PushGapError, match="not contiguous"):
        client._ack(0, 2)
    with server._region.locked(1):
        state = server._lane(0)
        server._write_lane(0, state._replace(head=2, next_seq=2))
    with pytest.raises(PushGapError, match="fell behind"):
        client._next_message(0)
    with server._region.locked(1):
        server._write_lane(0, state)
        consumer = server._consumer(client._slot)
        server._write_consumer(client._slot, consumer._replace(token=b"x" * 16))
    with pytest.raises(PushGapError, match="ownership"):
        client.check_health()
    client._release_consumer()
    assert client._slot is None


def test_worker_exit_and_sticky_error_are_observable(factory, monkeypatch):
    """消费线程退出和故障锁都必须阻止健康假阳性及隐式重试。"""
    server, client = factory("server"), factory("client", "manual")
    server.start_publisher()
    _registered_without_worker(client, monkeypatch)
    with pytest.raises(SharedMemoryPushError, match="worker exited"):
        client.check_health()
    failure = PushCallbackError("side effect uncertain")
    client._error = failure
    with pytest.raises(PushCallbackError):
        client.update_topics(["ticks"])
    server._error = failure
    with pytest.raises(PushCallbackError):
        server.publish("ticks", 1)


@pytest.mark.parametrize("raw", [b"[]", b'{"data":1}', b"invalid json"])
def test_corrupt_envelope_is_never_delivered_or_acknowledged(factory, monkeypatch, raw):
    """CRC 正确但业务信封非法也必须保持未确认，避免丢失交易信息。"""
    server = factory("server")
    server.start_publisher()
    monkeypatch.setattr(server, "_encode_payload", lambda topic, data: raw)
    server.publish("ticks", 1)
    client = factory("client", "broken")
    received = []
    client.start_subscriber(["ticks"], lambda topic, data: received.append(data))
    _wait(lambda: bool(client.status()["error"]))
    assert received == []
    assert client.status()["acknowledged"] == 0
    with pytest.raises(SharedMemoryPushError):
        client.check_health()


def test_health_lock_contention_is_retryable(factory, monkeypatch):
    """健康探测短时锁竞争保持可重试语义，不误报永久通信损坏。"""
    server = factory("server")
    server.start_publisher()

    @contextlib.contextmanager
    def busy(timeout):
        """模拟锁预算耗尽，确保错误经过健康接口转换。"""
        raise TransportTimeout("busy")
        yield

    with monkeypatch.context() as scoped:
        scoped.setattr(server._region, "locked", busy)
        with pytest.raises(PushBackpressureError, match="lock busy"):
            server.check_health()
    assert server.check_health()["started"]


def test_active_callback_stop_retains_handles_until_callback_finishes(factory):
    """回调超过停止预算时保留映射，防止回调结束后的 ACK 访问已释放内存。"""
    server, client = factory("server"), factory("client", "blocked")
    server.start_publisher()
    entered, release = threading.Event(), threading.Event()

    def callback(topic, data):
        """保持回调未结束以覆盖停止与 ACK 的生命周期交错。"""
        entered.set()
        assert release.wait(3)

    client.start_subscriber(["ticks"], callback)
    server.publish("ticks", 1)
    assert entered.wait(1)
    try:
        with pytest.raises(SharedMemoryPushError, match="still running"):
            client.stop()
        assert client._region is not None
    finally:
        release.set()
    _wait(lambda: not client.status()["started"])
    assert client.status()["acknowledged"] == 1


def test_capacity_wait_observes_ack_and_keeps_original_deadline(factory, monkeypatch):
    """等待空间的发布能被 ACK 唤醒，仍只提交一次且遵守整体预算。"""
    server = factory("server", market_push_slots=1)
    client = factory("client", "waiting", market_push_slots=1)
    server.start_publisher()
    _registered_without_worker(client, monkeypatch)
    server.publish("ticks", 1)
    original_wait = server._space_signal.wait
    waits = []

    def acknowledge_on_wait(timeout):
        """首轮等待时模拟消费者完成业务并提交确认。"""
        waits.append(timeout)
        client._ack(0, 1)
        return original_wait(timeout)

    monkeypatch.setattr(server._space_signal, "wait", acknowledge_on_wait)
    assert server.publish("ticks", 2, timeout_seconds=.5)
    assert len(waits) == 1
    assert 0 < waits[0] <= server.poll_interval
    assert server.status()["published"] == 2


@pytest.mark.parametrize("lost_owner", [False, True])
def test_fault_recording_does_not_overwrite_new_owner_or_abandoned_metadata(factory, monkeypatch, lost_owner):
    """消费失败记账若遇持锁退出或所有权变更，不得改写新持有者槽。"""
    server, client = factory("server"), factory("client", "fault")
    server.start_publisher()
    _registered_without_worker(client, monkeypatch)
    original_locked = client._region.locked
    before = client._consumer(client._slot)
    if lost_owner:
        with server._region.locked(1):
            before = before._replace(token=b"n" * 16)
            server._write_consumer(client._slot, before)

    def fail_read(lane):
        """模拟复制负载阶段检测到不可恢复日志缺口。"""
        raise PushGapError("injected log gap")

    @contextlib.contextmanager
    def abandoned_lock(timeout):
        """模拟 Windows 报告互斥量上一个持有者异常退出。"""
        with original_locked(timeout):
            yield True

    with monkeypatch.context() as scoped:
        scoped.setattr(client, "_next_message", fail_read)
        if not lost_owner:
            scoped.setattr(client._region, "locked", abandoned_lock)
        client._consume(lambda topic, data: None)
    assert isinstance(client._error, PushGapError)
    with server._region.locked(1):
        assert server._consumer(client._slot) == before
        if not lost_owner:
            assert server._header().health == 1


def test_cleanup_fault_is_visible_and_stop_can_retry(factory, monkeypatch):
    """正常消费结束时释放身份失败应保留诊断，显式 stop 仍能再次回收。"""
    server, client = factory("server"), factory("client", "cleanup")
    server.start_publisher()
    _registered_without_worker(client, monkeypatch)
    client._running = False

    def fail_release():
        """模拟停止时锁暂时不可取得。"""
        raise TransportTimeout("cleanup busy")

    with monkeypatch.context() as scoped:
        scoped.setattr(client, "_release_consumer", fail_release)
        client._consume(lambda topic, data: None)
    with pytest.raises(SharedMemoryPushError, match="cleanup failed"):
        client.check_health()
    assert client._slot is not None and client._region is not None
    client.stop()
    assert client._slot is None and client._region is None


def test_release_abandoned_lock_preserves_fault_and_slot_for_retry(factory, monkeypatch):
    """释放身份时遇到弃置锁不得修改半写记录，并保留槽供显式处理。"""
    server, client = factory("server"), factory("client", "abandoned")
    server.start_publisher()
    _registered_without_worker(client, monkeypatch)
    original_locked = client._region.locked

    @contextlib.contextmanager
    def abandoned_lock(timeout):
        """锁仍实际互斥，只替换上一个拥有者退出的内核返回标记。"""
        with original_locked(timeout):
            yield True

    with monkeypatch.context() as scoped:
        scoped.setattr(client._region, "locked", abandoned_lock)
        with pytest.raises(PushGapError, match="mutex owner"):
            client._release_consumer()
    assert client._slot is not None
    assert server._header().health == 1


def test_stop_tolerates_notification_failures_and_closes_remaining_handles(factory, monkeypatch):
    """停止唤醒和单个句柄关闭失败不能跳过其它资源清理。"""
    server, client = factory("server"), factory("client", "stop")
    server.start_publisher()
    _registered_without_worker(client, monkeypatch)
    data_signal = client._data_signal
    space_signal = client._space_signal
    original_close = data_signal.close

    def fail_set():
        """模拟停止提示发生内核事件错误。"""
        raise OSError("event failure")

    def fail_close():
        """关闭已完成后注入异常，核对其它句柄仍被回收。"""
        original_close()
        raise OSError("close failure")

    monkeypatch.setattr(data_signal, "set", fail_set)
    monkeypatch.setattr(data_signal, "close", fail_close)
    client.stop()
    assert client._region is None
    assert data_signal._closed and space_signal._closed


def test_old_publisher_stop_does_not_clear_new_generation(factory):
    """旧发布者退出不得把新代次的持有者标记清零。"""
    server = factory("server")
    server.start_publisher()
    with server._region.locked(1):
        header = server._header()._replace(epoch=b"n" * 16)
        module._HEADER.pack_into(server._region.buffer, 0, *header)
    server.stop()
    observer = factory("client")
    observer._open()
    assert observer._header() == header
    with observer._region.locked(1):
        module._HEADER.pack_into(observer._region.buffer, 0, *header._replace(pid=0))
