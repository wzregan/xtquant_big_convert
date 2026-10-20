"""验证慢巡检不饿死实际发送、扩容边界以及提交时仍保留故障校验。"""

import contextlib
import json
import time
from unittest.mock import Mock

import pytest

from bigqmt_signal_trader import shm_push_channel as shm
from bigqmt_signal_trader.quote_subscription_manager import BufferedAcknowledgedPublisher
from bigqmt_signal_trader.transports.base import TransportError
from bigqmt_signal_trader.transports.shm_primitives import cleanup


@pytest.fixture
def raw(tmp_path):
    """创建独立的真实共享日志，仅由当前用例发布并在结束时归还命名资源。"""
    config = dict(directory=str(tmp_path), namespace=str(tmp_path),
                  market_push_slots=16384, market_push_bytes=4 * 1024 * 1024,
                  trade_push_slots=16, trade_push_bytes=65536, max_push_subscribers=4)
    channel = shm.SharedMemoryQuotePushChannel("server", config, "budget-regression")
    channel.start_publisher()
    yield channel
    channel.stop()
    for name in (channel._name, channel._space_name):
        cleanup(name, str(tmp_path))


def test_slow_health_does_not_starve_pending_real_shared_memory(raw, monkeypatch):
    """巡检耗时超过发送预算时，实际共享提交仍前进且保留数值和顺序。"""
    health = Mock(side_effect=lambda: time.sleep(0.025))
    monkeypatch.setattr(raw, "check_health", health)
    publisher = BufferedAcknowledgedPublisher(raw, {"push_background_sender": False})
    for sequence in range(300):
        publisher.publish("quote:test", {"seq": sequence, "price": 4.123456789012345})
    while publisher.status()["local_pending"]["quote"]:
        assert publisher.flush(128, 0.02) > 0
    health.assert_not_called()
    with raw._region.locked(0.1):
        payloads = [raw._decode_payload(raw._read_bytes(0, raw._record(0, n)))
                    for n in range(1, 301)]
    assert [item["data"]["seq"] for item in payloads] == list(range(300))
    assert all(item["data"]["price"].hex() == (4.123456789012345).hex() for item in payloads)
    assert publisher.status()["overflow_count"] == 0
    # 空闲巡检仍执行，并且其耗时可单独诊断。
    publisher.flush()
    health.assert_called_once()
    assert publisher.status()["last_health_check_seconds"] >= 0.025


@pytest.mark.parametrize("fault", ["header", "callback"])
def test_submission_checks_faults_without_standalone_health(raw, monkeypatch, fault):
    """省略前置巡检不能绕过共享损坏或消费者回调失败，队首保持未提交。"""
    health = Mock()
    monkeypatch.setattr(raw, "check_health", health)
    publisher = BufferedAcknowledgedPublisher(raw, {"push_background_sender": False})
    publisher.publish("exec:order", {"id": 1})
    with raw._region.locked(0.1):
        if fault == "header":
            header = raw._header()
            shm._HEADER.pack_into(raw._region.buffer, 0, *header._replace(health=1))
        else:
            consumer = raw._consumer(0)
            raw._write_consumer(0, consumer._replace(
                state=shm._FAILED, stable=shm._CALLBACK_FAILED_FLAG | 5,
                pid=raw._pid, birth=raw._birth))
            raw._bump_consumers()
    assert publisher.flush() == 0
    health.assert_not_called()
    state = publisher.status()
    assert state["new_orders_blocked"] and state["sticky_error"]
    assert state["local_pending"]["exec"] == 1
    with raw._region.locked(0.1):
        assert raw._lane(1).next_seq == 1


def test_quote_capacity_10240_and_sticky_overflow_preserve_existing_fifo():
    """允许完整 10240 批突发，第 10241 批明确失败，排空不能自动抹掉缺口。"""
    channel = Mock(publish_checks_health=True)
    publisher = BufferedAcknowledgedPublisher(channel, {"push_background_sender": False})
    for n in range(10240):
        publisher.publish("quote:test", {"seq": n})
    state = publisher.status()
    assert state["local_queue_items"]["quote"] == 10240
    assert state["local_queue_bytes"]["quote"] == 128 * 1024 * 1024
    assert state["oldest_pending_seconds"]["quote"] > 0
    assert state["oldest_pending_seconds"]["exec"] == 0
    with pytest.raises(TransportError, match="queue is full"):
        publisher.publish("quote:test", {"seq": 10240})
    # 行情满不能消耗交易回报的独立容量，交易仍先提交。
    publisher.publish("exec:trade", {"id": 1})
    assert publisher.flush(10241, 5) == 10241
    calls = channel.publish.call_args_list
    assert calls[0].args[0] == "exec:trade"
    assert [call.args[1]["seq"] for call in calls[1:]] == list(range(10240))
    assert publisher.status()["oldest_pending_seconds"]["quote"] == 0
    assert publisher.status()["new_orders_blocked"]


def test_quote_byte_limit_applies_before_item_limit():
    """扩充条数后仍按实际对象大小拒绝超大快照，避免无界内存增长。"""
    publisher = BufferedAcknowledgedPublisher(Mock(), {"local_quote_queue_bytes": 512})
    with pytest.raises(TransportError, match="queue is full"):
        publisher.publish("quote:test", {"payload": "x" * 1024})
    assert publisher.status()["local_pending"]["quote"] == 0


def test_owner_steady_publish_uses_one_metadata_lock(raw, monkeypatch):
    """已确认的发布者在无维护任务时用一次锁完成提交，减少 Windows 内核调用。"""
    assert raw.publish("quote:test", {"n": 0})
    raw._next_maintenance = time.monotonic() + 60
    locked = raw._region.locked
    calls = []

    @contextlib.contextmanager
    def count(timeout):
        """保留真实互斥行为，只统计一次发布所需的元数据加锁次数。"""
        calls.append(timeout)
        with locked(timeout) as abandoned:
            yield abandoned

    monkeypatch.setattr(raw._region, "locked", count)
    assert raw.publish("quote:test", {"n": 1})
    assert len(calls) == 1


def test_owner_fast_path_cannot_commit_after_budget_expires(raw, monkeypatch):
    """即使本进程发布者免探活，持锁期间预算到期也不能迟到提交。"""
    raw.publish("quote:test", {"n": 0})
    raw._next_maintenance = time.monotonic() + 60
    validate = raw._validate

    def delayed_validation(*args, **kwargs):
        """模拟取得共享锁后被调度器暂停，不更改元数据及真实校验结果。"""
        result = validate(*args, **kwargs)
        time.sleep(0.025)
        return result

    monkeypatch.setattr(raw, "_validate", delayed_validation)
    with pytest.raises(shm.PushLockBusyError, match="budget exhausted"):
        raw.publish("quote:test", {"n": 1}, timeout_seconds=0.01)
    assert raw.status()["published"] == 1


def test_background_sender_progresses_after_windows_scheduling_delay(raw, monkeypatch):
    """实际加锁路径停顿超过 20ms 时，后台仍能提交，不因短预算无限重试。"""
    publisher = BufferedAcknowledgedPublisher(raw)
    publisher.start_publisher()
    locked = raw._region.locked

    @contextlib.contextmanager
    def delayed_lock(timeout):
        """模拟已获锁后等待 GIL 的调度停顿，保留真实共享区与互斥行为。"""
        with locked(timeout) as abandoned:
            time.sleep(0.03)
            yield abandoned

    monkeypatch.setattr(raw._region, "locked", delayed_lock)
    try:
        publisher.publish("quote:test", {"price": 4.123456789012345})
        deadline = time.monotonic() + 2
        while publisher.status()["shared_committed"] != 1:
            assert time.monotonic() < deadline
            time.sleep(0.005)
        assert publisher.status()["local_pending"]["quote"] == 0
        assert publisher.status()["overflow_count"] == 0
        with raw._region.locked(1):
            payload = raw._decode_payload(raw._read_bytes(0, raw._record(0, 1)))
        assert payload["data"]["price"].hex() == (4.123456789012345).hex()
    finally:
        monkeypatch.setattr(raw._region, "locked", locked)
        publisher.stop()


def test_idle_health_recovery_does_not_hide_pending_publish_lock():
    """巡检恢复不能清除尚未提交的锁忙；同一队首成功提交后才解除阻断。"""
    channel = Mock(publish_checks_health=True)
    publisher = BufferedAcknowledgedPublisher(channel, {"push_background_sender": False})
    channel.check_health.side_effect = shm.PushLockBusyError("health busy")
    assert publisher.flush() == 0
    publisher.publish("exec:order", {"id": 1})
    channel.publish.side_effect = shm.PushLockBusyError("publish busy")
    assert publisher.flush() == 0
    channel.check_health.side_effect = None
    publisher.check_health()
    assert publisher.status()["health_check_busy"] is False
    assert publisher.status()["publish_lock_busy"] is True
    assert publisher.status()["last_error"].endswith("publish busy")
    assert publisher.status()["new_orders_blocked"] is True
    channel.publish.side_effect = None
    assert publisher.flush() == 1
    assert publisher.status()["new_orders_blocked"] is False


def test_debug_metrics_are_opt_in_and_rate_limited(monkeypatch, capsys):
    """默认静默；开启诊断后输出完整事件名、队列年龄及耗时，每秒最多一次。"""
    channel = Mock(publish_checks_health=True)
    publisher = BufferedAcknowledgedPublisher(channel, {"push_background_sender": False})
    monkeypatch.delenv("BIGQMT_DEBUG", raising=False)
    publisher.publish("quote:test", {"n": 1})
    publisher.flush()
    assert capsys.readouterr().out == ""
    monkeypatch.setenv("BIGQMT_DEBUG", "1")
    publisher._debug_progress()
    publisher._debug_progress()
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    data = json.loads(lines[0].split(" ", 1)[1])
    assert data["event"] == "push_progress"
    assert data["state"]["oldest_pending_seconds"] == {"exec": 0.0, "quote": 0.0}
    assert data["state"]["last_publish_seconds"] >= 0
