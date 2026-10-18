"""共享推送协议的独立进程、确认恢复、背压与损坏检测测试。"""

import multiprocessing
import json
import os
import sys
import subprocess
import threading
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.shm_push_channel import (  # noqa: E402
    PushBackpressureError, PushCallbackError, PushGapError,
    SharedMemoryPushError, SharedMemoryQuotePushChannel,
)
from bigqmt_signal_trader.transports.shm_primitives import cleanup  # noqa: E402
from bigqmt_signal_trader import shm_push_channel as module  # noqa: E402
from bigqmt_signal_trader.quote_subscription_manager import BufferedAcknowledgedPublisher  # noqa: E402


def _wait(predicate, timeout=3):
    """等待共享协议的可观察状态，超时明确失败而不依赖固定睡眠。"""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("condition timed out")
        time.sleep(0.005)


@pytest.fixture
def factory(tmp_path):
    """提供彼此隔离的小容量日志，并在用例结束后删除已关闭的命名资源。"""
    instances = []
    settings = {"directory": str(tmp_path), "namespace": str(tmp_path),
                "market_push_bytes": 131072, "trade_push_bytes": 32768,
                "market_push_slots": 8, "trade_push_slots": 4,
                "max_push_subscribers": 8, "init_timeout_seconds": 0.15,
                "lock_timeout_seconds": 0.2, "push_poll_seconds": 0.005,
                "publish_timeout_seconds": 0.01, "subscriber_retention_seconds": 0.2}

    def build(role, subscriber_id=None, account_id="acct", **options):
        """构造通道，配置覆盖用于容量和保留期边界。"""
        config = dict(settings, **options)
        if subscriber_id is not None:
            config["subscriber_id"] = subscriber_id
        channel = SharedMemoryQuotePushChannel(role, config, account_id)
        instances.append(channel)
        return channel

    yield build
    names = set()
    for channel in reversed(instances):
        names.update([channel._name, channel._space_name, channel._signal_name(channel._identity)])
        names.update(channel._signal_name(identity) for identity in channel._signals)
        channel.stop()
    for name in names:
        cleanup(name, directory=settings["directory"])


def test_multiple_subscribers_topics_and_callback_stop(factory):
    """两个订阅者各自收到完整交易主题，回调停止仍确认成功消息。"""
    server = factory("server")
    server.start_publisher()
    received = [[], []]
    clients = [factory("client", "first"), factory("client", "second")]

    def callback(index):
        """构造可在业务回调内主动停止的独立消费者。"""
        def consume(topic, data):
            """记录事件并请求当前消费者停止。"""
            received[index].append((topic, data))
            clients[index].stop()
        return consume

    for index, client in enumerate(clients):
        client.start_subscriber(["exec:order", "ticks:510300.SH"], callback(index))
    assert server.publish("exec:order", {"order_id": 1}) is True
    _wait(lambda: all(not client.status()["started"] for client in clients))
    assert received == [[("exec:order", {"order_id": 1})]] * 2
    assert all(client.status()["acknowledged"] == 1 for client in clients)


def test_callback_failure_keeps_unacknowledged_for_explicit_stable_recovery(factory):
    """失败回调不推进游标；同稳定身份显式重建后重放，健康错误保持可见。"""
    server, client = factory("server"), factory("client", "stable")
    server.start_publisher()
    calls = []

    def fail(topic, data):
        """模拟已经触发业务代码、但尚未成功结束的回调。"""
        calls.append(data)
        raise RuntimeError("business failure")

    client.start_subscriber(["exec:order"], fail)
    server.publish("exec:order", {"order_id": 7})
    _wait(lambda: client.status()["callback_failed"])
    with pytest.raises(PushCallbackError):
        client.check_health()
    with pytest.raises(PushCallbackError):
        server.check_health()
    assert client.status()["acknowledged"] == 0
    assert calls == [{"order_id": 7}]
    client.stop()
    with pytest.raises(PushCallbackError):
        server.check_health()
    resumed = factory("client", "stable")
    received = []
    resumed.start_subscriber(["exec:order"], lambda topic, data: received.append(data))
    _wait(lambda: resumed.status()["acknowledged"] == 1)
    assert received == [{"order_id": 7}]
    assert server.check_health()["started"]


@pytest.mark.parametrize("stable", [False, True])
def test_callback_failure_remains_visible_after_stop_and_retention_expiry(factory, stable):
    """关闭及保留期过期不清除业务回调失败证据，临时身份也不能绕过门禁。"""
    server = factory("server", subscriber_retention_seconds=0)
    client = factory("client", "stable" if stable else None, subscriber_retention_seconds=0)
    server.start_publisher()

    def fail(topic, data):
        """制造尚未被服务端观察到的业务回调失败。"""
        raise RuntimeError("callback side effect uncertain")

    client.start_subscriber(["exec:order"], fail)
    server.publish("exec:order", 1)
    _wait(lambda: client.status()["callback_failed"])
    client.stop()
    # 尚未做过服务端健康检查就先关闭订阅，重现集成层半秒巡检窗口的竞态。
    with server._region.locked(1):
        server._maintain_consumers()
    with pytest.raises(PushCallbackError):
        server.check_health()
    if stable:
        with pytest.raises(PushGapError, match="retention expired"):
            client.start_subscriber(["exec:order"], lambda topic, data: None)
    # 发布者显式重建代次后才解除不可恢复的过期身份故障，旧消息不声称已确认。
    server.stop()
    server.start_publisher()
    assert server.check_health()["started"]


def test_retained_cursor_does_not_replay_already_acknowledged_messages(factory):
    """正常退出保留 ACK，重新订阅只接收退出后的消息。"""
    server, client = factory("server"), factory("client", "stable")
    server.start_publisher()
    first = []
    client.start_subscriber(["exec:order"], lambda topic, data: first.append(data))
    server.publish("exec:order", 1)
    _wait(lambda: client.status()["acknowledged"] == 1)
    client.stop()
    server.publish("exec:order", 2)
    resumed = factory("client", "stable")
    second = []
    resumed.start_subscriber(["exec:order"], lambda topic, data: second.append(data))
    _wait(lambda: resumed.status()["acknowledged"] == 1)
    assert first == [1]
    assert second == [2]


def test_market_backpressure_preserves_unacked_and_trade_capacity(factory):
    """行情填满只报背压，不覆盖未确认数据，也不占用交易日志容量。"""
    server = factory("server", market_push_slots=2)
    client = factory("client", "slow", market_push_slots=2)
    server.start_publisher()
    gate, entered = threading.Event(), threading.Event()
    received = []

    def consume(topic, data):
        """暂停首条消息，稳定制造未确认积压。"""
        received.append(data)
        entered.set()
        gate.wait(2)

    client.start_subscriber(["ticks"], consume)
    try:
        server.publish("ticks", 1)
        assert entered.wait(1)
        server.publish("ticks", 2)
        begin = time.monotonic()
        with pytest.raises(PushBackpressureError):
            server.publish("ticks", 3, timeout_seconds=0)
        assert time.monotonic() - begin < 0.2
        assert server.publish("exec:order", {"order_id": 9}) is True
    finally:
        gate.set()
    _wait(lambda: client.status()["acknowledged"] == 2)
    server.publish("ticks", 3)
    _wait(lambda: client.status()["acknowledged"] == 3)
    assert received == [1, 2, 3]


def test_message_id_deduplication_and_conflict(factory):
    """同 ID 同内容重试只占一个序号，同 ID 不同内容明确拒绝。"""
    server = factory("server")
    server.start_publisher()
    assert server.publish("exec:order", {"id": 1}, message_id="event-1")
    assert server.publish("exec:order", {"id": 1}, message_id="event-1")
    with pytest.raises(SharedMemoryPushError, match="different"):
        server.publish("exec:order", {"id": 2}, message_id="event-1")
    assert server.status()["published"] == 1


def test_retention_expiration_is_gap_and_frees_capacity(factory):
    """过期稳定身份收到明确缺口，发布者可回收已明确放弃的保留数据。"""
    server = factory("server", subscriber_retention_seconds=0, trade_push_slots=1)
    client = factory("client", "expired", subscriber_retention_seconds=0, trade_push_slots=1)
    server.start_publisher()
    client.start_subscriber(["exec:order"], lambda topic, data: None)
    client.stop()
    server.publish("exec:order", 1)
    server.publish("exec:order", 2)
    with pytest.raises(PushGapError, match="retention expired"):
        client.start_subscriber(["exec:order"], lambda topic, data: None)


def test_explicit_quote_resubscribe_after_expiry_establishes_a_fresh_cursor(factory):
    """正常停止的原行情持有者可以主动新订阅，并沿当前日志建立全新基线。"""
    server = factory("server", subscriber_retention_seconds=0)
    client = factory("client", "quote", subscriber_retention_seconds=0)
    server.start_publisher()
    first = []
    client.start_subscriber(["ticks:first"], lambda topic, data: first.append(data))
    server.publish("ticks:first", 1)
    _wait(lambda: client.status()["acknowledged"] == 1)
    client.stop()
    # 足够多的新消息回收旧行情；重新订阅绝不能继承已经落在日志头之前的游标。
    for value in range(10):
        server.publish("ticks:second", value)
    client.update_topics(["ticks:second"])
    second = []
    client.start_subscriber(["ticks:second"], lambda topic, data: second.append(data))
    server.publish("ticks:second", 10)
    _wait(lambda: bool(second) and second[-1] == 10)
    assert first == [1]
    assert client.check_health()["error"] == ""


def test_expired_quote_identity_without_explicit_owner_intent_still_reports_gap(factory):
    """单纯重试及另一个实例复用行情身份，都不能绕过已过期订阅的缺口检查。"""
    server = factory("server", subscriber_retention_seconds=0)
    client = factory("client", "quote", subscriber_retention_seconds=0)
    server.start_publisher()
    client.start_subscriber(["ticks"], lambda topic, data: None)
    client.stop()
    with server._region.locked(1):
        server._maintain_consumers()
    with pytest.raises(PushGapError, match="retention expired"):
        client.start_subscriber(["ticks"], lambda topic, data: None)
    replacement = factory("client", "quote", subscriber_retention_seconds=0)
    with pytest.raises(PushGapError, match="retention expired"):
        replacement.start_subscriber(["ticks"], lambda topic, data: None)


def test_explicit_topic_update_cannot_reset_expired_execution_cursor(factory):
    """交易订阅即使是原持有者主动更新主题，也必须继续报告过期缺口。"""
    server = factory("server", subscriber_retention_seconds=0)
    client = factory("client", "trades", subscriber_retention_seconds=0)
    server.start_publisher()
    client.start_subscriber(["exec:order"], lambda topic, data: None)
    client.stop()
    client.update_topics(["exec:trade"])
    with pytest.raises(PushGapError, match="retention expired"):
        client.start_subscriber(["exec:trade"], lambda topic, data: None)


def test_epoch_restart_is_reported_to_existing_subscriber(factory):
    """发布者重启后旧订阅不得跨代次假装消息连续。"""
    server, client = factory("server"), factory("client", "stable")
    server.start_publisher()
    client.start_subscriber(["exec:order"], lambda topic, data: None)
    old_epoch = server.status()["epoch"]
    server.stop()
    server.start_publisher()
    assert server.status()["epoch"] != old_epoch
    with pytest.raises(PushGapError):
        client.check_health()


def test_crc_corruption_is_visible_and_not_acknowledged(factory):
    """人为损坏已提交负载后，消费者报告故障而不交给业务或推进 ACK。"""
    server = factory("server")
    server.start_publisher()
    server.publish("exec:order", {"id": 1})
    with server._region.locked(1):
        record = server._record(1, 1)
        offset = server._data_offsets[1] + record.pos % server.capacities[1]
        server._region.buffer[offset:offset + 1] = bytes([server._region.buffer[offset] ^ 255])
    client = factory("client", "stable")
    received = []
    client.start_subscriber(["exec:order"], lambda topic, data: received.append(data))
    _wait(lambda: bool(client.status()["error"]))
    with pytest.raises(PushGapError, match="CRC"):
        client.check_health()
    assert not received
    assert client.status()["acknowledged"] == 0


def test_no_publisher_and_duplicate_owner_fail_clearly(factory):
    """服务尚未启动只能有界等待，同一端点的第二发布者必须拒绝。"""
    client = factory("client", "stable", init_timeout_seconds=0.01)
    with pytest.raises(SharedMemoryPushError, match="initialization timeout"):
        client.start_subscriber(["quotes"], lambda topic, data: None)
    server = factory("server")
    server.start_publisher()
    with pytest.raises(SharedMemoryPushError, match="already owns"):
        factory("server").start_publisher()


def test_same_stable_identity_rejects_concurrent_owner_and_topic_change(factory):
    """稳定身份禁止并发占用，并拒绝通过改主题悄悄改变历史确认范围。"""
    server, client = factory("server"), factory("client", "stable")
    server.start_publisher()
    client.start_subscriber(["exec:order"], lambda topic, data: None)
    other = factory("client", "stable")
    with pytest.raises(SharedMemoryPushError, match="already active"):
        other.start_subscriber(["exec:order"], lambda topic, data: None)
    client.stop()
    with pytest.raises(SharedMemoryPushError, match="topics changed"):
        other.start_subscriber(["quotes"], lambda topic, data: None)


def test_accounts_are_isolated(factory):
    """相同 namespace 的不同账号使用不同映射及发布者所有权。"""
    first, second = factory("server", account_id="a"), factory("server", account_id="b")
    first.start_publisher()
    second.start_publisher()
    received = []
    client = factory("client", "stable", account_id="b")
    client.start_subscriber(["exec:order"], lambda topic, data: received.append(data))
    first.publish("exec:order", "a")
    second.publish("exec:order", "b")
    _wait(lambda: client.status()["acknowledged"] == 1)
    assert received == ["b"]


def _child_consumer(config, ready, values):
    """独立解释器通过命名资源订阅，验证并非同进程内存替身。"""
    channel = SharedMemoryQuotePushChannel("client", config, "acct")
    done = threading.Event()

    def consume(topic, data):
        """把进程收到的业务消息返回给测试父进程。"""
        values.put((topic, data))
        done.set()

    try:
        channel.start_subscriber(["exec:order"], consume)
        ready.set()
        if not done.wait(5):
            raise RuntimeError("child subscriber timed out")
        _wait(lambda: channel.status()["acknowledged"] == 1)
    finally:
        channel.stop()


def test_spawned_process_receives_and_acknowledges(factory):
    """spawn 子进程独立打开映射，完成真实跨进程消息和 ACK。"""
    server = factory("server")
    server.start_publisher()
    naming = factory("client", "child")
    context = multiprocessing.get_context("spawn")
    ready, values = context.Event(), context.Queue()
    child = context.Process(target=_child_consumer, args=(naming.config, ready, values))
    child.start()
    try:
        assert ready.wait(5)
        server.publish("exec:order", {"id": 123})
        assert values.get(timeout=5) == ("exec:order", {"id": 123})
        child.join(5)
        assert child.exitcode == 0
    finally:
        if child.is_alive():
            child.terminate()
            child.join(5)
        values.close()


def _child_blocking_consumer(config, ready, entered):
    """在回调执行期间等待被测试父进程强制终止，制造未 ACK 的真实进程退出。"""
    channel = SharedMemoryQuotePushChannel("client", config, "acct")

    def consume(topic, data):
        """通知回调已进入，此时还没有推进共享确认游标。"""
        entered.set()
        threading.Event().wait(60)

    channel.start_subscriber(["exec:order"], consume)
    ready.set()
    threading.Event().wait(60)


def test_process_crash_preserves_unacknowledged_stable_cursor(factory):
    """实际消费进程在回调中被杀死，同身份的新进程实例仍能收到未确认消息。"""
    server = factory("server")
    server.start_publisher()
    naming = factory("client", "crashed-child")
    context = multiprocessing.get_context("spawn")
    ready, entered = context.Event(), context.Event()
    child = context.Process(target=_child_blocking_consumer, args=(naming.config, ready, entered))
    child.start()
    try:
        assert ready.wait(5)
        server.publish("exec:order", {"id": 42})
        assert entered.wait(5)
    finally:
        child.terminate()
        child.join(5)
    resumed = factory("client", "crashed-child")
    received = []
    resumed.start_subscriber(["exec:order"], lambda topic, data: received.append(data))
    _wait(lambda: resumed.status()["acknowledged"] == 1)
    assert received == [{"id": 42}]


def test_variable_payload_wraparound_preserves_order_and_crc(factory):
    """反复跨数据区末尾写入不同长度消息，回收索引后顺序与内容保持一致。"""
    server = factory("server", market_push_bytes=256, market_push_slots=3)
    client = factory("client", "wrap", market_push_bytes=256, market_push_slots=3)
    server.start_publisher()
    received = []
    client.start_subscriber(["ticks"], lambda topic, data: received.append(data))
    expected = [{"seq": index, "text": "x" * (30 + index % 40)} for index in range(30)]
    for index, payload in enumerate(expected):
        server.publish("ticks", payload)
        _wait(lambda: client.status()["acknowledged"] == index + 1)
    assert received == expected


def test_notification_failure_after_commit_does_not_invite_duplicate_retry(factory, monkeypatch):
    """提交后的唤醒故障仍返回成功，消费者通过超时重查共享状态收到消息。"""
    server, client = factory("server"), factory("client", "signal-failure")
    server.start_publisher()
    received = []
    client.start_subscriber(["exec:order"], lambda topic, data: received.append(data))

    def unavailable_signal(name, directory=None):
        """模拟新开通知句柄失败，既有消费者句柄仍能超时轮询。"""
        raise RuntimeError("signal unavailable")

    monkeypatch.setattr(module, "NamedSignal", unavailable_signal)
    assert server.publish("exec:order", 1, message_id="one") is True
    _wait(lambda: client.status()["acknowledged"] == 1)
    assert received == [1]
    assert server.status()["signal_errors"] == 1
    assert server.status()["published"] == 1


def test_slot_commit_corruption_is_not_acknowledged(factory):
    """索引提交标记损坏后不能仅凭数据 CRC 正确就交付或确认。"""
    server = factory("server")
    server.start_publisher()
    server.publish("exec:order", 1)
    with server._region.locked(1):
        record = server._record(1, 1)
        offset = server._index_offsets[1] + module._RECORD.size
        module._RECORD.pack_into(server._region.buffer, offset, *record._replace(commit=0))
    client = factory("client", "broken-commit")
    client.start_subscriber(["exec:order"], lambda topic, data: None)
    _wait(lambda: bool(client.status()["error"]))
    with pytest.raises(PushGapError, match="uncommitted"):
        client.check_health()
    assert client.status()["acknowledged"] == 0


def test_ephemeral_normal_stop_releases_consumer_slot(factory):
    """无稳定 ID 的订阅正常停止后可重复使用容量，不积累过期身份。"""
    server = factory("server", max_push_subscribers=1)
    server.start_publisher()
    for _ in range(3):
        client = factory("client", max_push_subscribers=1)
        client.start_subscriber(["exec:order"], lambda topic, data: None)
        client.stop()
    assert server.check_health()["started"]


def test_publish_lock_wait_respects_total_budget(factory):
    """其它线程持锁时，非阻塞发布明确报告未提交而不是等待整个锁默认超时。"""
    server = factory("server")
    server.start_publisher()
    entered, release = threading.Event(), threading.Event()

    def hold_lock():
        """在另一个线程持有区域锁，模拟并发访问压力。"""
        with server._region.locked(1):
            entered.set()
            release.wait(2)

    thread = threading.Thread(target=hold_lock)
    thread.start()
    try:
        assert entered.wait(1)
        start = time.monotonic()
        with pytest.raises(PushBackpressureError, match="lock budget") as publish_busy:
            server.publish("exec:order", 1, timeout_seconds=0)
        assert publish_busy.value.lock_busy is True
        assert time.monotonic() - start < 0.1
        assert server.status()["published"] == 0
        with pytest.raises(PushBackpressureError, match="health check lock busy") as health_busy:
            server.check_health()
        assert health_busy.value.lock_busy is True
    finally:
        release.set()
        thread.join(2)


@pytest.mark.parametrize("release_after", [0.01, None])
def test_buffered_publish_waits_within_flush_budget(factory, monkeypatch, release_after):
    """短暂确认锁竞争不误关交易；持续占锁时有界退出并保留同一条待提交回报。"""
    server = factory("server")
    server.start_publisher()
    publisher = BufferedAcknowledgedPublisher(server)
    publisher.publish("exec:order", {"order_id": 17})
    entered, release = threading.Event(), threading.Event()

    def hold_lock():
        """健康检查结束后再占锁，复现发布前出现的真实并发竞争。"""
        with server._region.locked(1):
            entered.set()
            release.wait(2 if release_after is None else release_after)

    thread = threading.Thread(target=hold_lock)
    original_health = server.check_health

    def health_then_contend():
        """保证健康检查已通过且另一线程持锁，再让正式发布逻辑争用共享锁。"""
        result = original_health()
        thread.start()
        assert entered.wait(1)
        return result

    monkeypatch.setattr(server, "check_health", health_then_contend)
    try:
        started = time.monotonic()
        committed = publisher.flush(max_wall_seconds=0.2 if release_after else 0.02)
        elapsed = time.monotonic() - started
        assert elapsed < 0.5
        state = publisher.status()
        if release_after is not None:
            assert committed == 1
            assert state["new_orders_blocked"] is False
            assert state["local_pending"]["exec"] == 0
        else:
            assert committed == 0
            assert state["new_orders_blocked"] is True
            assert state["local_pending"]["exec"] == 1
            assert "lock budget exhausted" in state["last_error"]
    finally:
        release.set()
        thread.join(2)
        monkeypatch.setattr(server, "check_health", original_health)
    if release_after is None:
        assert publisher.flush() == 1
        assert publisher.status()["new_orders_blocked"] is False
    assert server.status()["published"] == 1


def test_abandoned_lock_fault_is_persisted_until_publisher_restart(factory):
    """一次持锁异常退出不能只影响一个读者，协议故障须留在共享头直到重建。"""
    server = factory("server")
    server.start_publisher()
    with server._region.locked(1):
        with pytest.raises(PushGapError, match="mutex owner"):
            server._validate(abandoned=True)
    with pytest.raises(PushGapError, match="unhealthy"):
        server.check_health()
    with pytest.raises(PushGapError):
        server.publish("exec:order", 1)
    server.stop()
    server.start_publisher()
    assert server.check_health()["started"]


def test_subscriber_can_wait_for_later_publisher_start(factory):
    """消费者可先打开空映射，再在限定初始化窗口内发现发布者。"""
    client = factory("client", "early", init_timeout_seconds=1)
    errors = []

    def subscribe():
        """记录启动结果，避免测试线程异常被隐藏。"""
        try:
            client.start_subscriber(["quotes"], lambda topic, data: None)
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=subscribe)
    worker.start()
    _wait(lambda: client._region is not None)
    server = factory("server")
    server.start_publisher()
    worker.join(2)
    assert not worker.is_alive()
    assert not errors
    assert client.check_health()["started"]


def test_oversized_payload_and_closed_channel_fail_before_submission(factory):
    """超大消息和未启动通道都明确失败，不返回假成功。"""
    server = factory("server", trade_push_bytes=64)
    with pytest.raises(SharedMemoryPushError, match="not started"):
        server.publish("exec:order", 1)
    server.start_publisher()
    with pytest.raises(SharedMemoryPushError, match="exceeds"):
        server.publish("exec:order", "x" * 64)
    assert server.status()["published"] == 0


def test_subscription_start_failure_releases_handles_and_can_resume(factory, monkeypatch):
    """身份已登记而线程启动失败时，关闭所有句柄，显式重试同身份仍可恢复。"""
    server, client = factory("server"), factory("client", "thread-failure")
    server.start_publisher()

    def fail_start(thread):
        """模拟线程配额耗尽。"""
        raise RuntimeError("thread quota exhausted")

    with monkeypatch.context() as scoped:
        scoped.setattr(module.threading.Thread, "start", fail_start)
        with pytest.raises(RuntimeError, match="quota"):
            client.start_subscriber(["exec:order"], lambda topic, data: None)
    assert client._region is None
    assert client._slot is None
    assert not client.status()["started"]
    client.start_subscriber(["exec:order"], lambda topic, data: None)
    assert client.check_health()["started"]


def test_live_topic_update_preserves_ack_and_stable_resume_fingerprint(factory):
    """运行中增删同日志主题不关闭消费者，恢复仍按更新后的主题与原 ACK 继续。"""
    server, client = factory("server"), factory("client", "dynamic-topics")
    server.start_publisher()
    received = []
    client.start_subscriber(["quotes:a"], lambda topic, data: received.append((topic, data)))
    server.publish("quotes:a", 1)
    _wait(lambda: client.status()["acknowledged"] == 1)
    original_slot = client._slot
    assert client.update_topics(["quotes:a", "quotes:b"])
    server.publish("quotes:b", 2)
    _wait(lambda: client.status()["acknowledged"] == 2)
    assert client._slot == original_slot
    assert client.update_topics(["quotes:b"])
    server.publish("quotes:a", 3)
    server.publish("quotes:b", 4)
    _wait(lambda: client.status()["acknowledged"] == 4)
    client.stop()
    resumed = factory("client", "dynamic-topics")
    resumed.start_subscriber(["quotes:b"], lambda topic, data: received.append((topic, data)))
    server.publish("quotes:b", 5)
    _wait(lambda: resumed.status()["acknowledged"] == 1)
    assert received == [("quotes:a", 1), ("quotes:b", 2), ("quotes:b", 4), ("quotes:b", 5)]


def test_topic_update_rejects_implicit_lane_migration(factory):
    """不允许通过在线改主题隐式接管另一日志的历史确认范围。"""
    server, client = factory("server"), factory("client", "dynamic-topics")
    server.start_publisher()
    client.start_subscriber(["quotes:a"], lambda topic, data: None)
    with pytest.raises(SharedMemoryPushError, match="cannot change lanes"):
        client.update_topics(["exec:order"])


def test_same_stopped_instance_can_explicitly_replace_topics_without_resetting_ack(factory):
    """全部退订后同实例显式设置新组合，保留原 ACK 而不误报指纹冲突。"""
    server, client = factory("server"), factory("client", "reused-session")
    server.start_publisher()
    received = []
    client.start_subscriber(["quotes:a"], lambda topic, data: received.append((topic, data)))
    server.publish("quotes:a", 1)
    _wait(lambda: client.status()["acknowledged"] == 1)
    client.stop()
    assert client.update_topics(["quotes:b"])
    client.start_subscriber(["quotes:b"], lambda topic, data: received.append((topic, data)))
    server.publish("quotes:b", 2)
    _wait(lambda: client.status()["acknowledged"] == 2)
    assert received == [("quotes:a", 1), ("quotes:b", 2)]


def test_stopped_topic_change_cannot_overwrite_a_new_owners_identity(factory):
    """原实例停止后若同身份已由其它实例接管，旧预设不能改写其恢复范围。"""
    server, first = factory("server"), factory("client", "ownership")
    server.start_publisher()
    first.start_subscriber(["quotes:a"], lambda topic, data: None)
    first.stop()
    first.update_topics(["quotes:b"])
    second = factory("client", "ownership")
    second.start_subscriber(["quotes:a"], lambda topic, data: None)
    second.stop()
    with pytest.raises(SharedMemoryPushError, match="topics changed"):
        first.start_subscriber(["quotes:b"], lambda topic, data: None)


@pytest.mark.parametrize("isolated_role", ["server", "client"])
def test_default_json_works_between_msgpack_environment_and_isolated_process(factory, isolated_role):
    """一端可用 msgpack、另一端 -S 无依赖，默认协议仍可双向独立进程交付。"""
    pytest.importorskip("msgpack")
    parent_role = "client" if isolated_role == "server" else "server"
    parent = factory(parent_role, "codec-parent")
    naming = factory("client", "codec-child")
    config = dict(parent.config, subscriber_id="codec-child")
    ready_path = os.path.join(config["directory"], "codec-ready.json")
    done_path = os.path.join(config["directory"], "codec-done")
    if parent_role == "server":
        parent.start_publisher()
    script = r'''
import importlib.util
import json
import os
import sys
import time
sys.path.insert(0, sys.argv[1])
assert importlib.util.find_spec("msgpack") is None
from bigqmt_signal_trader.shm_push_channel import SharedMemoryQuotePushChannel
config = json.loads(sys.argv[2])
role, ready, done = sys.argv[3:]
channel = SharedMemoryQuotePushChannel(role, config, "acct")
received = []
try:
    if role == "server":
        channel.start_publisher()
    else:
        channel.start_subscriber(["exec:order"], lambda topic, data: received.append(data))
    with open(ready, "w") as stream:
        json.dump({"codec": channel.status()["push_codec"]}, stream)
    if role == "server":
        channel.publish("exec:order", {"id": 42, "text": "codec"})
    deadline = time.monotonic() + 5
    while not (os.path.exists(done) if role == "server" else channel.status()["acknowledged"] == 1):
        if time.monotonic() > deadline:
            raise RuntimeError("isolated codec process timed out")
        time.sleep(.005)
    print(json.dumps({"codec": channel.status()["push_codec"], "received": received}))
finally:
    channel.stop()
'''
    child = subprocess.Popen([sys.executable, "-S", "-c", script, os.path.join(ROOT, "src"),
                              json.dumps(config), isolated_role, ready_path, done_path],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    try:
        _wait(lambda: os.path.exists(ready_path) or child.poll() is not None)
        assert os.path.exists(ready_path), child.communicate(timeout=2)
        if parent_role == "server":
            parent.publish("exec:order", {"id": 42, "text": "codec"})
        else:
            received = []
            parent.start_subscriber(["exec:order"], lambda topic, data: received.append(data))
            _wait(lambda: parent.status()["acknowledged"] == 1)
            assert received == [{"id": 42, "text": "codec"}]
            with open(done_path, "w") as stream:
                stream.write("done")
        output, errors = child.communicate(timeout=5)
        assert child.returncode == 0, errors
        result = json.loads(output)
        assert result["codec"] == "json"
        if parent_role == "server":
            assert result["received"] == [{"id": 42, "text": "codec"}]
    finally:
        if child.poll() is None:
            child.terminate()
            child.communicate(timeout=5)
    assert naming._region is None


@pytest.mark.parametrize("server_codec,client_codec", [("json", "msgpack"), ("msgpack", "json")])
def test_explicit_codec_mismatch_fails_before_subscriber_registration(factory, server_codec, client_codec):
    """明确不同编码的两端在启动阶段拒绝，不能登记后等业务回调解码失败。"""
    pytest.importorskip("msgpack")
    server = factory("server", push_codec=server_codec)
    client = factory("client", "mismatch", push_codec=client_codec)
    server.start_publisher()
    with pytest.raises(SharedMemoryPushError, match="codec mismatch"):
        client.start_subscriber(["exec:order"], lambda topic, data: None)
    assert client._slot is None
    assert client._region is None
    assert not client.status()["started"]


def test_explicit_msgpack_roundtrip(factory):
    """需要性能时可显式选择 msgpack，两端共同声明后完成交付与 ACK。"""
    pytest.importorskip("msgpack")
    server = factory("server", push_codec="msgpack")
    client = factory("client", "msgpack", push_codec="msgpack")
    server.start_publisher()
    received = []
    client.start_subscriber(["exec:order"], lambda topic, data: received.append(data))
    server.publish("exec:order", {"text": "中文", "id": 1})
    _wait(lambda: client.status()["acknowledged"] == 1)
    assert received == [{"text": "中文", "id": 1}]
    assert client.status()["push_codec"] == "msgpack"


def test_explicit_msgpack_without_dependency_fails_before_open(factory, monkeypatch):
    """显式依赖缺失在构造阶段报错，不创建映射也不自动改用 JSON。"""
    import builtins
    original = builtins.__import__

    def import_without_msgpack(name, *args, **kwargs):
        """模拟当前解释器没有安装可选编码依赖。"""
        if name == "msgpack":
            raise ImportError("msgpack unavailable")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_msgpack)
    with pytest.raises(SharedMemoryPushError, match="requires msgpack"):
        factory("client", "missing-codec", push_codec="msgpack")


def test_previous_push_protocol_version_fails_before_subscriber_registration(factory):
    """无编码声明的旧协议不能混入新协议会话。"""
    server = factory("server")
    server.start_publisher()
    with server._region.locked(1):
        header = server._header()._replace(version=1)
        module._HEADER.pack_into(server._region.buffer, 0, *header)
    client = factory("client", "old-version")
    with pytest.raises(SharedMemoryPushError, match="protocol version mismatch"):
        client.start_subscriber(["exec:order"], lambda topic, data: None)
    assert client._slot is None
