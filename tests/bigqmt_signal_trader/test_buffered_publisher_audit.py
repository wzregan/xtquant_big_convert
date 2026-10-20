"""核对共享日志后台发送器启停、竞争及失败后的资源所有权。"""

import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest

from bigqmt_signal_trader import quote_subscription_manager as implementation
from bigqmt_signal_trader.quote_subscription_manager import BufferedAcknowledgedPublisher
from bigqmt_signal_trader.transports.base import TransportError


@pytest.fixture
def channel():
    """记录底层生命周期；这里只替代介质，真实 IPC 由跨进程用例覆盖。"""
    return Mock(publisher_endpoint="audit://publisher")


@pytest.mark.parametrize("background", [False, "false", "off", "0"])
def test_manual_sender_configuration_and_repeated_start_are_idempotent(channel, background):
    """布尔及字符串配置均能关闭后台线程，重复启动不重置底层日志。"""
    publisher = BufferedAcknowledgedPublisher(channel, {"push_background_sender": background})
    publisher.start_publisher()
    publisher.start_publisher()
    assert publisher.publisher_endpoint == "audit://publisher"
    assert publisher.status()["background_sender"] is False
    channel.start_publisher.assert_called_once()
    publisher.stop()
    publisher.stop()
    channel.stop.assert_called_once()


def test_start_after_stop_never_reopens_closed_resources(channel):
    """已关闭对象不能先打开底层资源再抛错，避免残留发布者占用。"""
    publisher = BufferedAcknowledgedPublisher(channel)
    publisher.stop()
    with pytest.raises(TransportError, match="closed"):
        publisher.start_publisher()
    channel.start_publisher.assert_not_called()


@pytest.mark.parametrize("failure", ["channel", "thread_constructor", "thread_start"])
def test_start_failure_releases_channel_without_joining_unstarted_thread(channel, monkeypatch, failure):
    """任一启动步骤失败都释放已分配映射；显式 stop 不会 join 未启动线程。"""
    publisher = BufferedAcknowledgedPublisher(channel)
    error = RuntimeError("injected startup failure")
    if failure == "channel":
        channel.start_publisher.side_effect = error
    elif failure == "thread_constructor":
        monkeypatch.setattr(implementation.threading, "Thread", Mock(side_effect=error))
    else:
        monkeypatch.setattr(implementation.threading.Thread, "start", Mock(side_effect=error))
    with pytest.raises(RuntimeError, match="injected startup"):
        publisher.start_publisher()
    publisher.stop()
    assert publisher.status()["healthy"] is False
    assert publisher.status()["sender_alive"] is False
    channel.stop.assert_called_once()


def test_start_cleanup_failure_can_be_retried_explicitly(channel):
    """清理错误不掩盖初始故障，句柄仍交给后续 stop 重试。"""
    publisher = BufferedAcknowledgedPublisher(channel)
    channel.start_publisher.side_effect = ValueError("start failed")
    channel.stop.side_effect = [OSError("close busy"), None]
    with pytest.raises(ValueError, match="start failed"):
        publisher.start_publisher()
    publisher.stop()
    assert channel.stop.call_count == 2


def test_concurrent_start_and_stop_preserve_resource_order(channel):
    """启动分配尚未完成时，关闭必须等待，不能留下关闭之后的新映射。"""
    entered, release = threading.Event(), threading.Event()
    publisher = BufferedAcknowledgedPublisher(channel, {"push_background_sender": False})

    def allocate():
        """用事件控制分配交界，无需用时间猜测线程次序。"""
        entered.set()
        assert release.wait(2.0)

    channel.start_publisher.side_effect = allocate
    with ThreadPoolExecutor(max_workers=2) as executor:
        start = executor.submit(publisher.start_publisher)
        assert entered.wait(1.0)
        stop = executor.submit(publisher.stop)
        assert not stop.done()
        release.set()
        start.result(2.0)
        stop.result(2.0)
    assert channel.mock_calls[0][0] == "start_publisher"
    assert channel.mock_calls[1][0] == "stop"
    with pytest.raises(TransportError, match="closed"):
        publisher.publish("exec:trade", {"id": 1})


def test_sender_unexpected_failure_blocks_orders_and_releases_resources(channel, monkeypatch):
    """发送循环意外退出不能继续报告健康，停止后才释放映射。"""
    publisher = BufferedAcknowledgedPublisher(channel)
    failed = threading.Event()

    def fail_flush(**kwargs):
        """注入非通道异常以验证线程顶层故障传播。"""
        failed.set()
        raise RuntimeError("sender crashed")

    monkeypatch.setattr(publisher, "flush", fail_flush)
    publisher.start_publisher()
    assert failed.wait(1.0)
    publisher._sender.join(1.0)
    assert publisher.status()["new_orders_blocked"] is True
    with pytest.raises(TransportError, match="sender crashed"):
        publisher.check_health()
    publisher.stop()
    channel.stop.assert_called_once()


def test_manual_flush_contention_returns_without_duplicate_submission(channel):
    """主线程与辅助线程同时请求刷新，只允许一个写者提交队首。"""
    publisher = BufferedAcknowledgedPublisher(channel, {"push_background_sender": False})
    publisher.publish("exec:trade", {"id": 1})
    publisher._flush_lock.acquire()
    try:
        assert publisher.flush() == 0
        channel.publish.assert_not_called()
    finally:
        publisher._flush_lock.release()
    assert publisher.flush() == 1
    channel.publish.assert_called_once()
    publisher.stop()


def test_callback_stop_between_health_and_publish_keeps_pending_event(channel):
    """健康检查期间收到停止信号，不能再将待发送事件提交给关闭的介质。"""
    publisher = BufferedAcknowledgedPublisher(channel, {"push_background_sender": False})
    publisher.publish("exec:trade", {"id": 1})

    def close_during_health():
        """模拟另一线程在本轮发送前标记关闭，最终清理由调用方执行。"""
        with publisher._lock:
            publisher._closed = True

    channel.check_health.side_effect = close_during_health
    assert publisher.flush() == 0
    channel.publish.assert_not_called()
    publisher.stop()
    assert publisher.status()["pending_at_stop"] == 1


def test_retained_payload_estimator_handles_cycles_and_repeated_objects():
    """循环容器与共享叶子不会导致无限递归或重复计入预算。"""
    shared = [1, 2]
    value = {"first": shared, "second": shared}
    value["cycle"] = value
    assert implementation._retained_payload_bytes(value) > 0


@pytest.mark.parametrize("settings", [{"local_quote_queue_items": 0}, {"local_queue_high_water": 0},
                                     {"local_queue_high_water": 2}])
def test_invalid_limits_fail_before_any_resource_allocation(channel, settings):
    """非法容量和高水位在创建内核资源之前明确拒绝。"""
    with pytest.raises(ValueError):
        BufferedAcknowledgedPublisher(channel, settings)
    assert not channel.mock_calls


def test_publisher_role_rejects_subscriber_and_propagates_health(channel):
    """角色混用立即失败；正常健康结果保留底层字段。"""
    publisher = BufferedAcknowledgedPublisher(channel)
    with pytest.raises(RuntimeError, match="cannot start a subscriber"):
        publisher.start_subscriber([], Mock())
    channel.check_health.return_value = {"healthy": True, "epoch": "one"}
    assert publisher.check_health() == {"healthy": True, "epoch": "one"}
    publisher.stop()


def test_stop_during_manual_flush_retains_mapping_until_flush_exits(channel):
    """手动刷新超过关闭预算时保留资源，刷新结束后可再次关闭。"""
    entered, release = threading.Event(), threading.Event()
    publisher = BufferedAcknowledgedPublisher(channel, {"push_background_sender": False})
    publisher.publish("exec:trade", {"id": 1})

    def slow_publish(*args, **kwargs):
        """确定性停在底层提交期间，模拟内核调用长时间未返回。"""
        entered.set()
        assert release.wait(4.0)

    channel.publish.side_effect = slow_publish
    with ThreadPoolExecutor(max_workers=1) as executor:
        flush = executor.submit(publisher.flush, 1, 3.0)
        assert entered.wait(1.0)
        try:
            with pytest.raises(TransportError, match="flush is still running"):
                publisher.stop()
            channel.stop.assert_not_called()
        finally:
            release.set()
        assert flush.result(1.0) == 1
    publisher.stop()
    channel.stop.assert_called_once()


def test_deadline_expiring_after_selecting_queue_preserves_head(channel, monkeypatch):
    """抢占发生在取队首之后时，剩余预算为零不得再提交。"""
    channel.publish_checks_health = True
    publisher = BufferedAcknowledgedPublisher(channel, {"push_background_sender": False})
    publisher.publish("exec:trade", {"id": 1})
    with monkeypatch.context() as context:
        context.setattr(implementation.time, "monotonic", Mock(side_effect=[0, 0, 0, 2, 2]))
        assert publisher.flush(max_wall_seconds=1.0) == 0
    channel.publish.assert_not_called()
    assert publisher.flush() == 1
    publisher.stop()
