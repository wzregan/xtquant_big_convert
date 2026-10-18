"""报单前只读健康锁忙可有界复查，原生委托执行和真实故障不可重试。"""

import time
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest

from bigqmt_signal_trader.quote_subscription_manager import (
    BufferedAcknowledgedPublisher,
)
from bigqmt_signal_trader.sdk_contract import ExecutionRejected
from bigqmt_signal_trader.shm_push_channel import (
    PushBackpressureError,
    PushCallbackError,
    PushLockBusyError,
)
from bigqmt_signal_trader_strategy import _install_acknowledged_push_gate


def guarded_publisher(raw, config=None):
    """仅替代共享介质健康检查，保留生产缓冲队列和正式报单门禁。"""
    handlers = Mock()
    handlers._canonical_method.side_effect = lambda name: name
    original = handlers.handle
    publisher = BufferedAcknowledgedPublisher(
        raw, dict(config or {}, push_background_sender=False)
    )
    _install_acknowledged_push_gate(handlers, publisher)
    return handlers, original, publisher


def test_transient_health_lock_busy_rechecks_then_executes_once():
    """两次健康锁竞争后明确恢复，清除旧 busy 标记但只执行一次原生报单。"""
    raw = Mock()
    raw.check_health.side_effect = [
        PushBackpressureError("lock busy"),
        PushBackpressureError("lock busy"),
        {"healthy": True},
    ]
    handlers, execute, publisher = guarded_publisher(raw)
    publisher._health_busy = True
    publisher._last_error = "old health lock busy"
    handlers.handle("submit_order", {"remark": "unique"})
    assert raw.check_health.call_count == 3
    execute.assert_called_once_with("submit_order", {"remark": "unique"})
    assert not publisher.status()["health_check_busy"]
    assert not publisher.status()["new_orders_blocked"]


def test_persistent_health_lock_busy_has_bounded_deadline_without_execution():
    """持续锁忙在限定时间内返回明确未执行，不能无限阻塞 QMT 策略线程。"""
    raw = Mock()
    raw.check_health.side_effect = PushBackpressureError("lock busy")
    handlers, execute, _publisher = guarded_publisher(raw)
    started = time.monotonic()
    with pytest.raises(ExecutionRejected, match="lock busy"):
        handlers.handle("submit_order", {})
    assert 0.08 <= time.monotonic() - started < 0.4
    assert raw.check_health.call_count > 1
    execute.assert_not_called()


def test_permanent_callback_failure_is_never_retried_or_executed():
    """回调失败属于硬故障，不能因为其他健康竞争被一起放行。"""
    raw = Mock()
    raw.check_health.side_effect = PushCallbackError("callback failed")
    handlers, execute, _publisher = guarded_publisher(raw)
    with pytest.raises(ExecutionRejected, match="callback failed"):
        handlers.handle("submit_order", {})
    raw.check_health.assert_called_once()
    execute.assert_not_called()


@pytest.mark.parametrize("blocked", ["high_water", "backpressure", "sticky"])
def test_successful_health_read_does_not_clear_actual_backlog_or_sticky_error(blocked):
    """底层健康恢复不能解除真实队列高水位、发送背压或粘滞错误。"""
    raw = Mock()
    handlers, execute, publisher = guarded_publisher(
        raw, {"local_exec_queue_items": 2, "local_queue_high_water": 0.5}
    )
    publisher._health_busy = True
    if blocked == "high_water":
        publisher.publish("exec:trade", {"trade_id": "held"})
    elif blocked == "backpressure":
        publisher._backpressure = True
        publisher._last_error = "execution backlog"
    else:
        publisher._sticky_error = "execution overflow"
    with pytest.raises(ExecutionRejected):
        handlers.handle("submit_order", {})
    assert publisher.status()["new_orders_blocked"] is True
    execute.assert_not_called()


def test_exception_after_execution_is_not_replayed():
    """只读门禁通过后，即使原生执行结果异常也必须直接传播，不进入健康复查循环。"""
    raw = Mock()
    handlers, execute, _publisher = guarded_publisher(raw)
    execute.side_effect = RuntimeError("already sent but response lost")
    with pytest.raises(RuntimeError, match="already sent"):
        handlers.handle("submit_order", {})
    execute.assert_called_once()
    raw.check_health.assert_called_once()


def test_publish_lock_busy_waits_for_original_record_commit_before_order():
    """健康读取不能解除发送锁忙，必须等后台真实提交原队首才执行一次新委托。"""
    entered, release = threading.Event(), threading.Event()
    raw = Mock()
    publish_count = []

    def publish(*args, **kwargs):
        """第一轮锁忙，第二轮暂停在真正提交之前，验证门禁没有提前放行。"""
        publish_count.append(kwargs["message_id"])
        if len(publish_count) == 1:
            raise PushLockBusyError("publish lock busy")
        entered.set()
        assert release.wait(2)
        return True

    raw.publish.side_effect = publish
    publisher = BufferedAcknowledgedPublisher(raw)
    handlers = Mock()
    handlers._canonical_method.side_effect = lambda method: method
    execute = handlers.handle
    _install_acknowledged_push_gate(handlers, publisher)
    publisher.start_publisher()
    try:
        publisher.publish("quote:tick", {"lastPrice": 4.101})
        assert entered.wait(1)
        assert publisher.status()["publish_lock_busy"] is True
        assert publisher.status()["capacity_backpressure"] is False
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(handlers.handle, "submit_order", {})
            time.sleep(0.02)
            execute.assert_not_called()
            assert publisher.status()["shared_committed"] == 0
            release.set()
            result.result(1)
        assert publisher.status()["shared_committed"] == 1
        assert not publisher.status()["publish_lock_busy"]
        assert len(publish_count) == 2 and len(set(publish_count)) == 1
        execute.assert_called_once_with("submit_order", {})
    finally:
        release.set()
        publisher.stop()


def test_successful_health_read_cannot_clear_uncommitted_publish_lock_busy():
    """发送者持续无法提交时，只读检查再多次成功也不代表原回报已经可靠发布。"""
    raw = Mock()
    raw.publish.side_effect = PushLockBusyError("publish lock busy")
    handlers, execute, publisher = guarded_publisher(raw)
    publisher.publish("exec:trade", {"trade_id": "uncommitted"})
    assert publisher.flush() == 0
    with pytest.raises(ExecutionRejected, match="publish lock busy"):
        handlers.handle("submit_order", {})
    assert publisher.status()["publish_lock_busy"] is True
    assert publisher.status()["shared_committed"] == 0
    assert publisher.status()["local_pending"]["exec"] == 1
    execute.assert_not_called()
