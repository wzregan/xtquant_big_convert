"""锁外进程探活的协议竞态：旧探针不得修改新身份，关闭不能释放在途映射。"""

import contextlib
import os
import sys
import threading
import time
import types
import uuid

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader import shm_push_channel as module  # noqa: E402
from bigqmt_signal_trader.transports.base import TransportError, TransportTimeout  # noqa: E402
from bigqmt_signal_trader.transports.shm_primitives import cleanup  # noqa: E402


@pytest.fixture
def pair(tmp_path, monkeypatch):
    """注册真实消费者但暂停自动消费，让用例精确安排复制与确认的并发时序。"""
    config = {
        "directory": str(tmp_path), "namespace": str(tmp_path),
        "market_push_bytes": 8192, "trade_push_bytes": 8192,
        "market_push_slots": 8, "trade_push_slots": 8,
        "max_push_subscribers": 4, "init_timeout_seconds": 0.3,
        "lock_timeout_seconds": 0.2, "publish_timeout_seconds": 0.2,
        "push_poll_seconds": 0.005, "subscriber_retention_seconds": 30,
    }
    server = module.SharedMemoryQuotePushChannel("server", config, "probe-races")
    client = module.SharedMemoryQuotePushChannel("client", dict(config, subscriber_id="stable"), "probe-races")
    release = threading.Event()
    monkeypatch.setattr(client, "_consume", lambda callback: release.wait(5))
    server.start_publisher()
    client.start_subscriber(["exec:order"], lambda topic, payload: None)
    assert server.publish("exec:order", {"order_id": 19})
    region = client._region
    yield server, client
    release.set()
    if client._thread is not None:
        client._thread.join(1)
    # 个别用例故意关闭底层映射；清理不再读取该已关闭映射。
    if region._closed:
        client._region = None
        client._slot = None
    client.stop()
    server.stop()
    for name in {server._name, server._space_name, client._signal_name(client._identity)}:
        cleanup(name, directory=config["directory"])


def _invoke(client, operation):
    """执行待测复制、确认或公开健康操作；确认序号来自真实已发布记录。"""
    if operation == "next":
        return client._next_message(1)
    if operation == "ack":
        return client._ack(1, 1)
    return client.check_health()


@pytest.mark.parametrize("operation", ["next", "ack", "health"])
@pytest.mark.parametrize("changed", ["epoch", "pid", "birth", "health", "stop"])
def test_publisher_identity_must_survive_unlocked_probe(pair, monkeypatch, operation, changed):
    """探活后发布者字段变化必须失败，不能消费或确认旧快照代表的消息。"""
    server, client = pair
    called = []

    def probe(pid, birth):
        """在探活窗口变更共享头，模拟同名发布者重建或正常停止。"""
        called.append((pid, birth))
        if changed == "stop":
            server.stop()
        else:
            with server._region.locked(0.1):
                header = server._header()
                values = {"epoch": uuid.uuid4().bytes, "pid": header.pid + 1,
                          "birth": header.birth + 1, "health": 1}
                module._HEADER.pack_into(server._region.buffer, 0,
                                         *header._replace(**{changed: values[changed]}))
        return True

    monkeypatch.setattr(client, "_peer_alive", probe)
    with pytest.raises(module.PushGapError):
        _invoke(client, operation)
    assert called
    assert client.status()["acknowledged"] == 0
    with client._region.locked(0.1):
        assert client._consumer(client._slot).trade_cursor == 0


@pytest.mark.parametrize("operation", ["next", "ack", "health"])
def test_consumer_takeover_cannot_use_old_instance_probe(pair, monkeypatch, operation):
    """发布者仍存活也不能让旧 token 复制或确认已换主消费者的日志。"""
    server, client = pair
    token = uuid.uuid4().bytes

    def probe(pid, birth):
        """探活期间由新实例取得同一稳定订阅的槽位。"""
        with server._region.locked(0.1):
            consumer = server._consumer(client._slot)
            server._write_consumer(client._slot, consumer._replace(token=token))
            server._bump_consumers()
        return True

    monkeypatch.setattr(client, "_peer_alive", probe)
    with pytest.raises(module.PushGapError, match="ownership"):
        _invoke(client, operation)
    with server._region.locked(0.1):
        consumer = server._consumer(client._slot)
        assert consumer.token == token
        assert consumer.state == module._ACTIVE
        assert consumer.trade_cursor == 0
    assert client.status()["acknowledged"] == 0


def test_old_dead_consumer_probe_does_not_reclaim_new_owner(pair, monkeypatch):
    """死亡探针只适用于被检查的完整身份，不能把同槽新消费者改成保留态。"""
    server, client = pair
    dead_pid, dead_birth = 2147483600, 191
    token = uuid.uuid4().bytes
    with server._region.locked(0.1):
        old = server._consumer(client._slot)
        server._write_consumer(client._slot, old._replace(pid=dead_pid, birth=dead_birth))
        server._bump_consumers()
    original = server._peer_alive
    probed = []

    def probe(pid, birth):
        """旧消费者死亡返回之前先登记新 token，保留其最新确认游标。"""
        if (pid, birth) != (dead_pid, dead_birth):
            return original(pid, birth)
        probed.append((pid, birth))
        with server._region.locked(0.1):
            current = server._consumer(client._slot)
            server._write_consumer(client._slot, current._replace(
                token=token, pid=client._pid, birth=client._birth, trade_cursor=1))
            server._bump_consumers()
        return False

    monkeypatch.setattr(server, "_peer_alive", probe)
    assert server.publish("exec:order", {"order_id": 20})
    assert probed
    with server._region.locked(0.1):
        current = server._consumer(client._slot)
        assert current.state == module._ACTIVE
        assert current.token == token
        assert current.trade_cursor == 1


def test_consumer_ack_while_publisher_probes_is_not_overwritten(pair, monkeypatch):
    """消费者在锁外探活窗口推进的 ACK 必须保留，回收使用重新加锁后的记录。"""
    server, client = pair
    foreign_pid, foreign_birth = 2147483600, 191
    with server._region.locked(0.1):
        consumer = server._consumer(client._slot)
        server._write_consumer(client._slot, consumer._replace(pid=foreign_pid, birth=foreign_birth))
        server._bump_consumers()
    original = server._peer_alive
    probed = []

    def probe(pid, birth):
        """借真实消费者确认一条已成功处理的消息，之后才返回进程存活。"""
        if (pid, birth) != (foreign_pid, foreign_birth):
            return original(pid, birth)
        probed.append((pid, birth))
        client._ack(1, 1)
        return True

    monkeypatch.setattr(server, "_peer_alive", probe)
    assert server.publish("exec:order", {"order_id": 20})
    assert len(probed) == 1
    with server._region.locked(0.1):
        assert server._consumer(client._slot).trade_cursor == 1
    assert client._next_message(1)[0] == 2
    assert client.status()["acknowledged"] == 1


@pytest.mark.parametrize("lock_number", [1, 2])
def test_abandoned_either_phase_persists_fault(pair, monkeypatch, lock_number):
    """无论快照锁还是提交锁发生 abandoned，故障必须写共享头供其他实例观察。"""
    server, client = pair
    original = client._region.locked
    count = []

    @contextlib.contextmanager
    def locked(timeout_seconds):
        """保留真实区域锁，仅替换 Win32 返回的持锁者死亡标志。"""
        count.append(timeout_seconds)
        with original(timeout_seconds):
            yield len(count) == lock_number

    monkeypatch.setattr(client._region, "locked", locked)
    with pytest.raises(module.PushGapError, match="mutex owner"):
        client._next_message(1)
    with server._region.locked(0.1):
        assert server._header().health == 1
        assert server._consumer(client._slot).trade_cursor == 0
    with pytest.raises(module.PushGapError):
        server.check_health()


def test_probe_and_both_lock_phases_share_one_deadline(pair, monkeypatch):
    """第一次锁和探活已用掉的时间必须从第二次锁预算扣除。"""
    _server, client = pair
    clock = [300.0]
    budgets = []
    original = client._region.locked

    @contextlib.contextmanager
    def locked(timeout_seconds):
        """记录真实锁收到的剩余预算，模拟首次操作耗费七十毫秒。"""
        budgets.append(timeout_seconds)
        with original(timeout_seconds) as abandoned:
            yield abandoned
        if len(budgets) == 1:
            clock[0] += 0.07

    def probe(pid, birth):
        """模拟操作系统身份检查耗费八十毫秒，不实际占用 CPU 或休眠。"""
        clock[0] += 0.08
        return True

    monkeypatch.setattr(client._region, "locked", locked)
    monkeypatch.setattr(client, "_peer_alive", probe)
    monkeypatch.setattr(module, "time", types.SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    assert client._next_message(1)[0] == 1
    assert budgets == pytest.approx([0.2, 0.05])


def test_close_waits_for_probe_without_unmapping_active_operation(pair, monkeypatch):
    """关闭与锁外探活并发时先拒绝后续读取，再等在途操作退出后释放 mmap。"""
    _server, client = pair
    region = client._region
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()
    errors, results = [], []

    def probe(pid, birth):
        """把探活停在锁外，让另一线程开始资源关闭。"""
        entered.set()
        assert release.wait(2)
        return True

    def read():
        """收集调用结果，保证后台异常不会只成为 pytest 警告。"""
        try:
            results.append(client._next_message(1))
        except Exception as error:
            errors.append(error)

    def close():
        """只关闭真实底层资源，捕获关闭错误并暴露完成状态。"""
        try:
            region.close()
        except Exception as error:
            errors.append(error)
        finally:
            closed.set()

    monkeypatch.setattr(client, "_peer_alive", probe)
    reader = threading.Thread(target=read)
    closer = threading.Thread(target=close)
    reader.start()
    try:
        assert entered.wait(1)
        closer.start()
        deadline = time.monotonic() + 1
        while not region._closing and time.monotonic() < deadline:
            time.sleep(0.001)
        assert region._closing
        assert not closed.is_set()
        assert region.buffer is not None
    finally:
        release.set()
        reader.join(2)
        if closer.ident is not None:
            closer.join(2)
    assert not reader.is_alive() and not closer.is_alive()
    assert closed.is_set() and region.buffer is None
    assert not results
    assert len(errors) == 1 and isinstance(errors[0], TransportError)
    assert "clos" in str(errors[0])
    assert client.status()["acknowledged"] == 0


@pytest.mark.parametrize("operation", ["next", "ack", "health"])
def test_dead_publisher_with_unchanged_header_still_fails(pair, monkeypatch, operation):
    """共享头未改也不能把明确已退出的发布者误判为健康或确认其消息。"""
    server, client = pair
    monkeypatch.setattr(client, "_peer_alive", lambda pid, birth: False)
    with pytest.raises(module.PushGapError):
        _invoke(client, operation)
    with server._region.locked(0.1):
        assert server._consumer(client._slot).trade_cursor == 0
    assert client.status()["acknowledged"] == 0


@pytest.mark.parametrize("operation", ["ack", "publish"])
def test_probe_exhausting_positive_budget_cannot_commit(pair, monkeypatch, operation):
    """探活耗尽正数总预算后必须停止写操作，不能把剩余零误作一次新的立即尝试。"""
    server, client = pair
    owner = client if operation == "ack" else server
    clock = [500.0]
    previous_published = server.status()["published"]

    def probe(pid, birth):
        """模拟慢系统调用使用超过本次请求预算的时间。"""
        clock[0] += 0.3
        return True

    monkeypatch.setattr(owner, "_peer_alive", probe)
    monkeypatch.setattr(module, "time", types.SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    with pytest.raises((TransportError, TransportTimeout)):
        if operation == "ack":
            client._ack(1, 1)
        else:
            server.publish("exec:order", {"order_id": 20}, timeout_seconds=0.2)
    assert server.status()["published"] == previous_published
    assert client.status()["acknowledged"] == 0
    with server._region.locked(0.1):
        assert server._consumer(client._slot).trade_cursor == 0


@pytest.mark.parametrize("phase", ["first_lock", "probe"])
def test_mapping_replacement_cannot_redirect_inflight_operation(pair, monkeypatch, phase):
    """两阶段间实例切换映射时必须失败，不能把旧锁保护当作新 mmap 的访问权。"""
    server, client = pair
    region = client._region
    original = region.locked

    @contextlib.contextmanager
    def locked(timeout_seconds):
        """模拟旧映射快照被读取前，生命周期管理者切换实例映射。"""
        with original(timeout_seconds) as abandoned:
            client._region = server._region
            yield abandoned

    def probe(pid, birth):
        """模拟两段锁之间发生的映射实例切换。"""
        client._region = server._region
        return True

    if phase == "first_lock":
        monkeypatch.setattr(region, "locked", locked)
    else:
        monkeypatch.setattr(client, "_peer_alive", probe)
    try:
        with pytest.raises(module.PushGapError, match="mapping changed"):
            client._ack(1, 1)
        assert client.status()["acknowledged"] == 0
        with server._region.locked(0.1):
            assert server._consumer(client._slot).trade_cursor == 0
    finally:
        client._region = region
        monkeypatch.setattr(region, "locked", original)


def test_positive_deadline_expiring_during_second_lock_cannot_ack(pair, monkeypatch):
    """第二段成功获取锁时正预算也必须有效，防止调度延迟让过期调用继续写入。"""
    server, client = pair
    clock = [700.0]
    original = client._region.locked
    count = []

    @contextlib.contextmanager
    def locked(timeout_seconds):
        """模拟拿到第二段锁后才被调度，调用预算已经消耗完。"""
        count.append(timeout_seconds)
        with original(timeout_seconds) as abandoned:
            if len(count) == 2:
                clock[0] += 0.3
            yield abandoned

    monkeypatch.setattr(client._region, "locked", locked)
    monkeypatch.setattr(module, "time", types.SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    with pytest.raises(TransportTimeout):
        client._ack(1, 1)
    with server._region.locked(0.1):
        assert server._consumer(client._slot).trade_cursor == 0
    assert client.status()["acknowledged"] == 0


@pytest.mark.parametrize("dead", [False, True])
@pytest.mark.parametrize("timeout", [0, 0.02])
def test_full_lane_rechecks_cached_ephemeral_without_waiting_for_ttl(pair, monkeypatch, dead, timeout):
    """满日志必须重新探活临时消费者；死亡立即回收，活着则仍保留所有未确认消息。"""
    server, client = pair
    foreign_pid, foreign_birth = 2147483600, 193
    alive = [True]
    observations = []
    original = server._peer_alive
    with server._region.locked(0.1):
        consumer = server._consumer(client._slot)
        server._write_consumer(client._slot, consumer._replace(
            pid=foreign_pid, birth=foreign_birth, stable=consumer.stable & ~1))
        server._bump_consumers()
    client.stable = False

    def probe(pid, birth):
        """返回外部消费者当次真实存活状态，发布者身份走原始检查。"""
        if (pid, birth) == (foreign_pid, foreign_birth):
            observations.append(alive[0])
            return alive[0]
        return original(pid, birth)

    monkeypatch.setattr(server, "_peer_alive", probe)
    for value in range(2, 9):
        assert server.publish("exec:order", {"order_id": value})
    assert server._next_maintenance > time.monotonic()
    assert observations == [True]
    alive[0] = not dead
    if dead:
        assert server.publish("exec:order", {"order_id": 9}, timeout_seconds=timeout)
    else:
        with pytest.raises(module.PushBackpressureError):
            server.publish("exec:order", {"order_id": 9}, timeout_seconds=timeout)
    assert len(observations) >= 2
    assert observations[-1] is not dead
    with server._region.locked(0.1):
        consumer = server._consumer(client._slot)
        lane = server._lane(1)
        if dead:
            assert consumer.state == 0
            assert lane.head == 9 and lane.next_seq == 10
        else:
            assert consumer.state == module._ACTIVE
            assert lane.head == 1 and lane.next_seq == 9
            assert server._record(1, 1).seq == 1
    assert server.status()["published"] == (9 if dead else 8)


@pytest.mark.parametrize("boundary", ["force_retry", "space_wait"])
def test_publish_retries_keep_original_positive_deadline(pair, monkeypatch, boundary):
    """空间等待或强制探活重试之后，即使已有新空间也不能延长原始发布截止时间。"""
    server, client = pair
    for value in range(2, 9):
        assert server.publish("exec:order", {"order_id": value})
    clock = [900.0]
    released = []
    original = server._validated_lock

    def acknowledge_then_expire():
        """由真实消费者释放全部空间，再模拟调度延迟使原正预算到期。"""
        if not released:
            for seq in range(1, 9):
                client._ack(1, seq)
            released.append(True)
            clock[0] += 0.3

    @contextlib.contextmanager
    def validated(*args, **kwargs):
        """保持正式锁逻辑，仅在第一次释放锁后模拟另一线程确认与调度暂停。"""
        with original(*args, **kwargs) as value:
            yield value
        acknowledge_then_expire()

    def wait(seconds):
        """模拟空间提示唤醒，但线程恢复运行时已经超过最初截止时间。"""
        acknowledge_then_expire()
        return True

    monkeypatch.setattr(module, "time", types.SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    if boundary == "force_retry":
        monkeypatch.setattr(server, "_validated_lock", validated)
    else:
        monkeypatch.setattr(server._space_signal, "wait", wait)
    with pytest.raises(module.PushBackpressureError):
        server.publish("exec:order", {"order_id": 9}, timeout_seconds=0.2)
    assert released == [True]
    assert server.status()["published"] == 8
    assert client.status()["acknowledged"] == 8
    with server._region.locked(0.1):
        assert server._lane(1).next_seq == 9


def test_late_publisher_does_not_extend_subscriber_initialization_deadline(pair, monkeypatch):
    """发布者在等待后出现但初始化已过期时，消费者不能获得新的完整注册预算。"""
    server, _client = pair
    server.stop()
    late = module.SharedMemoryQuotePushChannel(
        "client", dict(server.config, subscriber_id="late"), "probe-races")
    clock = [1100.0]
    waited = []
    original = late._open

    def wait(seconds):
        """等待时真实启动发布者，再使调用线程迟于原截止时间恢复运行。"""
        server.start_publisher()
        waited.append(seconds)
        clock[0] += 0.4
        return True

    def opened(timeout_seconds=None):
        """保留正式映射打开过程，仅控制初始化期间的唤醒时序。"""
        original(timeout_seconds)
        monkeypatch.setattr(late._space_signal, "wait", wait)

    monkeypatch.setattr(module, "time", types.SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    monkeypatch.setattr(late, "_open", opened)
    try:
        with pytest.raises((TransportError, TransportTimeout)):
            late.start_subscriber(["exec:order"], lambda topic, payload: None)
        assert len(waited) == 1
        assert late._slot is None and late._region is None
        assert late.status()["started"] is False
        with server._region.locked(0.1):
            assert not any(server._consumer(slot).identity == late._identity
                           for slot in range(server.max_consumers))
    finally:
        late.stop()
