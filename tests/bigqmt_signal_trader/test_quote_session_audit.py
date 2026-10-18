"""验证共享内存订阅会话的故障恢复与租约管理，不访问终端。"""

import threading
import time
from unittest.mock import Mock

import pytest

from bigqmt_signal_trader import quote_subscription_manager as manager_module
from bigqmt_signal_trader.quote_subscription_manager import (
    ContextInfoQuoteSource, QuoteSourceAdapter, QuoteSubscriptionManager,
)
from bigqmt_signal_trader.transports.base import TransportError
from bigqmt_signal_trader.whole_quote_session import WholeQuoteClientSession


def _session(acknowledged=True, **kwargs):
    """创建可控的介质替身，保留会话本身的全部锁、回调和恢复逻辑。"""
    channel = Mock(supports_acknowledgements=acknowledged)
    rpc = Mock(return_value={})
    return WholeQuoteClientSession(rpc, channel, "audit", **kwargs), channel, rpc


@pytest.mark.parametrize("prior_failures", [0, 3])
def test_replay_failure_does_not_kill_heartbeat(prior_failures):
    """静默恢复和故障恢复中的重放失败都继续重试，后续心跳仍到达服务端。"""
    session, channel, rpc = _session(
        False, heartbeat_interval_seconds=0.001, push_silence_replay_heartbeats=4,
    )
    session.subscribe_whole_quote(["SH"])
    replayed = threading.Event()
    counts = {"keepalive": 0, "replay": 0}

    def unstable_rpc(method, params):
        """第一次重放失败，后续成功，模拟连接恢复与注册恢复并不同步。"""
        if method == "quote_keepalive":
            counts["keepalive"] += 1
            if counts["keepalive"] <= prior_failures:
                raise TransportError("bridge reconnecting")
            if counts["replay"] >= 2:
                replayed.set()
        else:
            counts["replay"] += 1
            if counts["replay"] == 1:
                raise TransportError("subscription queue busy")
        return {}

    rpc.side_effect = unstable_rpc
    session.start()
    session.start()
    try:
        assert replayed.wait(2.0)
        assert session._heartbeat_thread.is_alive()
    finally:
        session.close()


def test_invalid_subscription_unknown_unsubscribe_and_custom_identity():
    """输入错误不发送 RPC；外部分配的稳定编号原样传递。"""
    session, channel, rpc = _session(sub_id_func=lambda: "stable")
    with pytest.raises(ValueError):
        session.subscribe_whole_quote([])
    assert session.unsubscribe_quote("missing") == 0
    assert session._check_push_health() is True
    assert session.subscribe_whole_quote(["SH"]) == "stable"
    session.close()


@pytest.mark.parametrize("factory", [None, Mock(side_effect=OSError("reopen failed"))])
def test_unrecoverable_channel_reports_failed_health(factory):
    """没有恢复工厂或工厂失败时，不能以线程仍运行冒充行情健康。"""
    session, channel, rpc = _session(push_channel_factory=factory)
    session.subscribe_whole_quote(["SH"])
    channel.check_health.side_effect = TransportError("lost mapping")
    assert session._check_push_health() is False
    assert session.push_health()["reconciliation_required"] is True
    assert session.push_health()["last_error"]
    session.close()


def test_legacy_callback_and_stop_errors_do_not_prevent_other_subscriptions():
    """旧无确认通道沿用隔离回调错误的语义，停止错误不阻断新主题启动。"""
    session, channel, rpc = _session(False)
    first = session.subscribe_whole_quote(["SH"], Mock(side_effect=ValueError("user")))
    session._on_push("SH", {"value": 1})
    channel.stop.side_effect = TransportError("already stopped")
    second = session.subscribe_whole_quote(["SZ"])
    assert session.has_subscription(first) and session.has_subscription(second)
    session.close()


def test_acknowledged_channel_without_topic_update_restarts_subscriber():
    """没有原地更新能力的确认通道仍可通过重启订阅切换主题。"""
    session, channel, rpc = _session()
    channel.update_topics = None
    session.subscribe_whole_quote(["SH"])
    session.subscribe_whole_quote(["SZ"])
    assert channel.start_subscriber.call_count == 2
    channel.stop.assert_called_once()
    session.close()


def test_acknowledged_health_retry_and_live_push_reset_silence(monkeypatch):
    """临时故障延后心跳，实际推送恢复后静默计数从零开始。"""
    session, channel, rpc = _session(heartbeat_interval_seconds=0.001)
    session.subscribe_whole_quote(["SH"])
    rounds = []

    def advance(seconds):
        """控制心跳轮次与恢复时机，消除基于真实睡眠的竞态。"""
        rounds.append(seconds)
        if len(rounds) == 1:
            session._on_push("SH", {})
        if len(rounds) == 3:
            session._started = False

    monkeypatch.setattr(session, "_check_push_health", Mock(side_effect=[False, True, True]))
    monkeypatch.setattr(session._heartbeat_stop, "wait", advance)
    session._started = True
    session._heartbeat_loop()
    assert len(rounds) == 3
    session.close()


@pytest.mark.parametrize("acknowledged,subscribed", [(False, False), (False, True), (True, True)])
def test_stop_wakes_long_heartbeat_wait_and_releases_thread(monkeypatch, acknowledged, subscribed):
    """正常续租、空订阅和健康失败退避都能立即唤醒，不遗留三秒休眠线程。"""
    session, channel, rpc = _session(acknowledged, heartbeat_interval_seconds=60)
    if subscribed:
        session.subscribe_whole_quote(["SH"])
    if acknowledged:
        monkeypatch.setattr(session, "_check_push_health", lambda: False)
    waiting = threading.Event()
    original = session._heartbeat_stop.wait

    def observed_wait(seconds):
        """只暴露真实等待入口，不用固定睡眠猜测线程时序。"""
        waiting.set()
        return original(seconds)

    monkeypatch.setattr(session._heartbeat_stop, "wait", observed_wait)
    session.start()
    thread = session._heartbeat_thread
    assert waiting.wait(2)
    start = time.monotonic()
    session.stop()
    assert time.monotonic() - start < 0.5
    assert not thread.is_alive() and session._heartbeat_thread is None
    session.start()
    assert session._heartbeat_thread is not thread
    session.close()


def test_stop_keeps_blocked_thread_owned_and_prevents_duplicate_start():
    """真实 RPC 阻塞时不能谎报停止，也不能清除旧代停止信号启动第二个心跳。"""
    session, channel, rpc = _session(False)
    session.subscribe_whole_quote(["SH"])
    entered, release = threading.Event(), threading.Event()

    def blocked_rpc(method, params):
        """模拟不可中断的外部调用，只有测试明确释放才能返回。"""
        entered.set()
        assert release.wait(5)
        return {}

    rpc.side_effect = blocked_rpc
    session.start()
    thread = session._heartbeat_thread
    try:
        assert entered.wait(2)
        with pytest.raises(RuntimeError, match="停止期限"):
            session.stop()
        assert session._heartbeat_thread is thread and thread.is_alive()
        with pytest.raises(RuntimeError, match="不能重启"):
            session.start()
        assert session._heartbeat_stop.is_set()
    finally:
        release.set()
        session.close()


def test_close_waits_for_push_callback_without_holding_session_lock():
    """通道停止等待中的回调可以获得会话锁，最终关闭不得吞掉可靠通道退出失败。"""
    session, channel, rpc = _session()
    callback = Mock()
    session.subscribe_whole_quote(["SH"], callback)
    observed = []
    threads = []

    def stop_channel():
        """在停止入口才触发迟到回调，确定覆盖持锁等待造成的互相阻塞。"""
        thread = threading.Thread(target=session._on_push, args=("SH", {}))
        threads.append(thread)
        thread.start()
        thread.join(0.5)
        observed.append(thread.is_alive())

    channel.stop.side_effect = stop_channel
    try:
        session.close()
        assert observed == [False]
        callback.assert_not_called()
    finally:
        for thread in threads:
            thread.join(1)


def test_close_reports_acknowledged_channel_stop_failure_and_allows_retry():
    """可靠通道真实退出失败必须向所有者报告，稍后可在同一对象重试清理。"""
    session, channel, rpc = _session()
    session.subscribe_whole_quote(["SH"])
    channel.stop.side_effect = RuntimeError("consumer still alive")
    with pytest.raises(RuntimeError, match="consumer still alive"):
        session.close()
    channel.stop.side_effect = None
    session.close()
    assert channel.stop.call_count == 2


def test_heartbeat_can_request_its_own_stop_without_joining_itself():
    """心跳 RPC 的内部回调可以请求停止，线程引用由外部最终回收确认。"""
    session, channel, rpc = _session(False)
    session.subscribe_whole_quote(["SH"])
    session.subscribe_whole_quote(["SZ"])
    requested = threading.Event()

    def self_stop(method, params):
        """在心跳所属线程中停止，不触发当前线程不能join自身的异常。"""
        session.stop()
        requested.set()
        return {}

    rpc.side_effect = self_stop
    session.start()
    assert requested.wait(2)
    session.close()
    assert session._heartbeat_thread is None


def test_final_close_retries_channel_whose_last_unsubscribe_stop_failed():
    """最后一次退订的旧兼容路径未完成停止时，最终关闭仍负责重试真实回收。"""
    session, channel, rpc = _session()
    handle = session.subscribe_whole_quote(["SH"])
    channel.stop.side_effect = [RuntimeError("transient stop"), None]
    session.unsubscribe_quote(handle)
    assert not session.has_subscription(handle)
    session.close()
    assert channel.stop.call_count == 2


def test_source_contract_and_invalid_native_handles():
    """抽象适配器拒绝直接使用，原生非法订阅号不会当作成功保存。"""
    source = QuoteSourceAdapter()
    with pytest.raises(NotImplementedError):
        source.subscribe([], Mock())
    with pytest.raises(NotImplementedError):
        source.unsubscribe(1)
    context = Mock()
    context.subscribe_whole_quote.return_value = "invalid"
    adapter = ContextInfoQuoteSource(context)
    with pytest.raises(RuntimeError, match="failed"):
        adapter.subscribe(["510300.SH"], Mock())
    with pytest.raises(RuntimeError, match="no quote codes"):
        adapter.subscribe([], Mock())
    context.unsubscribe_quote.side_effect = OSError("already closed")
    adapter.unsubscribe([1, 2])
    assert context.unsubscribe_quote.call_count == 2


def test_partial_native_subscription_failure_cleans_previous_handles():
    """混合标的部分注册失败时，逐个释放先前成功的句柄，清理错误不掩盖注册失败。"""
    context = Mock()
    context.subscribe_whole_quote.return_value = 1
    context.subscribe_quote.side_effect = RuntimeError("option failed")
    context.unsubscribe_quote.side_effect = OSError("close failed")
    with pytest.raises(RuntimeError, match="option failed"):
        ContextInfoQuoteSource(context).subscribe(["510300.SH", "10000001.SHO"], Mock())
    context.unsubscribe_quote.assert_called_once_with(1)


def test_empty_option_update_is_not_published():
    """空的原生期权更新不生成虚构推送。"""
    context, callback = Mock(), Mock()
    context.subscribe_quote.return_value = 1
    ContextInfoQuoteSource(context).subscribe(["10000001.SHO"], callback)
    context.subscribe_quote.call_args[0][-1]({})
    callback.assert_not_called()


def test_late_native_callback_after_unsubscribe_is_ignored():
    """退订之后的迟到原生回调不会发布到已经消失的组合。"""
    source, publisher = Mock(), Mock()
    manager = QuoteSubscriptionManager(source, on_push_publisher=publisher)
    manager.subscribe("client", "one", ["SH"])
    callback = source.subscribe.call_args[0][1]
    source.unsubscribe.side_effect = OSError("already stopped")
    manager.unsubscribe("client", "one")
    callback({"value": 1})
    publisher.assert_not_called()
    assert manager._remove_subscription_locked("missing", "client", "one") is None
    manager._on_push_publisher = None
    callback({"value": 2})


def test_keepalive_waiting_on_unsubscribe_cannot_revive_removed_lease():
    """心跳在锁外排队期间退订，获得锁后必须重新确认该订阅仍存在。"""
    manager = QuoteSubscriptionManager(Mock())
    manager.subscribe("client", "old", ["SH"])
    manager.subscribe("client", "remaining", ["SH"])
    lock = manager._lock
    entering = threading.Event()
    owner = threading.get_ident()

    class ObservedLock:
        """保留真实 RLock 行为，仅暴露等待入口以确定退订与心跳的先后。"""

        def __enter__(self):
            """工作线程准备取锁时通知主线程，不更改锁的互斥语义。"""
            if threading.get_ident() != owner:
                entering.set()
            lock.acquire()

        def __exit__(self, *args):
            """归还实际重入锁。"""
            lock.release()

    manager._lock = ObservedLock()
    with lock:
        thread = threading.Thread(target=manager.keepalive, args=("client", "old"))
        thread.start()
        assert entering.wait(1.0)
        manager.unsubscribe("client", "old")
    thread.join(1.0)
    assert not thread.is_alive()
    assert set(manager._combos["SH"].clients) == {("client", "remaining")}


def test_service_assembly_failure_releases_factory_resource():
    """通道创建成功后组装失败也必须释放资源，关闭失败不掩盖原始配置错误。"""
    channel = Mock()
    channel.stop.side_effect = OSError("cleanup failed")
    backend = Mock(acknowledged_push=True)
    backend.build_push_channel.return_value = channel
    with pytest.raises(ValueError):
        manager_module.build_quote_subscription_service(
            Mock(), backend=backend, rpc_config={"shm": {"local_quote_queue_items": 0}},
        )
    channel.stop.assert_called_once()


def test_normalization_fallback_and_legacy_endpoint(monkeypatch):
    """空代码被忽略，无法标准化的代码保留既有大小写回退行为。"""
    assert manager_module.normalize_subscription_code(" ") == ""
    monkeypatch.setattr(manager_module, "normalize_stock_code", Mock(side_effect=ValueError("invalid")))
    assert manager_module.normalize_subscription_code("bad.invalid") == "BAD.INVALID"
    assert manager_module._default_quote_push_zmq_bind("audit").startswith("tcp://")
