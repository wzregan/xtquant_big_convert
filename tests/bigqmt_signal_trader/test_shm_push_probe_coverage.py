"""锁外进程探活的启动、接管和预算边界；只补整体共享推送回归尚未覆盖的真实分支。"""

import contextlib
import time
from types import SimpleNamespace

import pytest

from bigqmt_signal_trader import shm_push_channel as module
from bigqmt_signal_trader.transports.base import TransportTimeout
from bigqmt_signal_trader.transports.shm_primitives import cleanup


@pytest.fixture
def channels(tmp_path):
    """同一隔离命名空间内创建任意角色，统一清理全部映射及订阅信号。"""
    settings = {
        "directory": str(tmp_path), "namespace": str(tmp_path),
        "market_push_bytes": 8192, "trade_push_bytes": 8192,
        "market_push_slots": 4, "trade_push_slots": 4,
        "max_push_subscribers": 2, "push_poll_seconds": 0.002,
    }
    instances = []

    def create(role, **options):
        """构造尚未启动的通道，options 只覆盖该用例明确需要的协议参数。"""
        channel = module.SharedMemoryQuotePushChannel(role, dict(settings, **options), "probe-boundary")
        instances.append(channel)
        return channel

    yield create
    names = set()
    for channel in reversed(instances):
        names.update([channel._name, channel._space_name, channel._signal_name(channel._identity)])
        names.update(channel._signal_name(identity) for identity in channel._signals)
        channel.stop()
    for name in names:
        cleanup(name, str(tmp_path))


def test_closed_mapping_cannot_supply_a_validated_write_lock(channels):
    """未打开或已关闭的实例没有共享区域，不能交出供调用方写入的锁。"""
    channel = channels("server")
    with pytest.raises(module.SharedMemoryPushError, match="not started"):
        with channel._validated_lock(0.02):
            pytest.fail("an unopened channel supplied a write lock")
    assert channel.status()["published"] == 0


def test_subscriber_retries_when_owner_changes_during_initial_probe(channels, monkeypatch):
    """首次注册探活时发布者重建，应重新获取新代次，不能注册进旧代次。"""
    server, client = channels("server"), channels("client")
    server.start_publisher()
    previous_epoch = server._epoch
    original = client._peer_alive
    changed = []

    def restart_once(pid, birth):
        """探活返回前真实停止并重建发布者，第二轮才读取新代次的存活状态。"""
        alive = original(pid, birth)
        if not changed:
            changed.append(True)
            server.stop()
            server.start_publisher()
        return alive

    monkeypatch.setattr(client, "_peer_alive", restart_once)
    client.start_subscriber(["exec:order"], lambda _topic, _data: None)
    assert changed
    assert client._epoch == server._epoch != previous_epoch
    with server._region.locked(0.1):
        active = [server._consumer(slot) for slot in range(server.max_consumers)
                  if server._consumer(slot).state == module._ACTIVE]
    assert len(active) == 1
    assert active[0].token == client._token


def test_publisher_takeover_cannot_overwrite_owner_created_during_probe(channels, monkeypatch):
    """旧身份被判死亡后已有竞争者完成接管，迟到的启动者必须关闭自己的句柄。"""
    owner, challenger = channels("server"), channels("server")
    owner.start_publisher()
    previous_epoch = owner._epoch

    def stale_dead_result(_pid, _birth):
        """返回旧代次的死亡结果，同时真实安装另一个新发布代次。"""
        owner.stop()
        owner.start_publisher()
        return False

    monkeypatch.setattr(challenger, "_peer_alive", stale_dead_result)
    with pytest.raises(module.SharedMemoryPushError, match="ownership changed"):
        challenger.start_publisher()
    assert challenger._region is None
    assert owner._epoch != previous_epoch
    assert owner.check_health()["started"] is True
    assert owner.publish("exec:order", {"id": "new-owner"}) is True


def test_existing_live_owner_rejects_duplicate_without_resetting_log(channels):
    """同名活发布者不能被第二实例覆盖，之前已提交事件与原代次保持可用。"""
    owner, challenger = channels("server"), channels("server")
    owner.start_publisher()
    owner.publish("exec:order", {"id": "original"})
    epoch = owner._epoch
    with pytest.raises(module.SharedMemoryPushError, match="already owns"):
        challenger.start_publisher()
    assert challenger._region is None
    assert owner.check_health()["epoch"] == epoch.hex()
    with owner._region.locked(0.1):
        lane = owner._lane(1)
        assert lane.next_seq == 2
        assert owner._decode_payload(owner._read_bytes(1, owner._record(1, 1)))["data"] == {"id": "original"}


@pytest.mark.parametrize("expired_at", ["after_first_lock", "after_second_lock"])
def test_publisher_initialization_obeys_original_budget_at_both_lock_boundaries(
    channels, monkeypatch, expired_at,
):
    """启动探活或第二次抢锁用尽原预算时，不能把未初始化映射变成活发布者。"""
    channel = channels("server", init_timeout_seconds=0.02)
    channel._open()
    region = channel._region
    original_locked = region.locked
    clock = [100.0]
    calls = []

    @contextlib.contextmanager
    def scheduled_lock(timeout_seconds=5.0):
        """用真实锁承载调度窗口，只控制调用方的单调时钟，不放宽任何预算。"""
        with original_locked(timeout_seconds) as abandoned:
            calls.append(timeout_seconds)
            if expired_at == "after_second_lock" and len(calls) == 2:
                clock[0] += 0.03
            yield abandoned
        if expired_at == "after_first_lock" and len(calls) == 1:
            clock[0] += 0.03

    monkeypatch.setattr(region, "locked", scheduled_lock)
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    with pytest.raises(TransportTimeout, match="initialization budget exhausted"):
        channel.start_publisher()
    assert channel._region is None
    assert channel._epoch is None
    assert channel.status()["started"] is False
    assert region._closed


def test_full_queue_cannot_start_another_probe_after_original_deadline(channels, monkeypatch):
    """真实日志仍满且回收判定结束时原预算已尽，不能再探活或提交第二条记录。"""
    server = channels("server", trade_push_slots=1)
    server.start_publisher()
    assert server.publish("exec:order", {"id": "retained"})
    original_reclaim = server._reclaim
    clock = [100.0]
    reclaimed = []

    def delayed_reclaim(lane, consumers):
        """沿用真实未确认游标回收结果，模拟返回调用方之前发生调度延迟。"""
        result = original_reclaim(lane, consumers)
        reclaimed.append(lane)
        clock[0] += 0.03
        return result

    monkeypatch.setattr(server, "_reclaim", delayed_reclaim)
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    with pytest.raises(module.PushBackpressureError, match="unacknowledged messages retained"):
        server.publish("exec:order", {"id": "must-not-commit"}, timeout_seconds=0.02)
    assert reclaimed == [1]
    assert server.status()["published"] == 1
    with server._region.locked(0.1):
        assert server._lane(1).next_seq == 2
        assert server._decode_payload(server._read_bytes(1, server._record(1, 1)))["data"] == {"id": "retained"}


def test_absent_publisher_wait_cannot_continue_after_initial_deadline(channels, monkeypatch):
    """真实双锁核验确认发布者缺席，返回等待循环时预算过期应结束且不留下注册槽。"""
    client = channels("client", init_timeout_seconds=0.02)
    original_validated_lock = client._validated_lock
    clock = [100.0]
    observations = []

    @contextlib.contextmanager
    def delayed_return(*args, **kwargs):
        """保留真实探活及双锁路径，只在全部锁释放后模拟调用线程被暂停。"""
        with original_validated_lock(*args, **kwargs) as header:
            observations.append(header)
            yield header
        clock[0] += 0.03

    monkeypatch.setattr(client, "_validated_lock", delayed_return)
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    with pytest.raises(module.SharedMemoryPushError, match="initialization timeout"):
        client.start_subscriber(["exec:order"], lambda _topic, _data: None)
    assert observations == [None]
    assert client._region is None
    assert client._slot is None
    inspector = channels("server")
    inspector._open()
    with inspector._region.locked(0.1):
        assert inspector._header().pid == 0
        assert all(inspector._consumer(slot).state == 0 for slot in range(inspector.max_consumers))
