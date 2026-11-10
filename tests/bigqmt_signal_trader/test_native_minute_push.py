"""原生分钟桥接的资源所有权、丢帧检测、批量兼容及 SDK 数据精度。"""

import threading
from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.native_bar_subscription import (
    NativeBarDelivery, NativeBarSubscriptions, bar_topic,
)
from bigqmt_signal_trader.quote_subscription_manager import (
    QuoteSubscriptionManager, build_quote_subscription_service,
)
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, LISTENER_DEFERRED_METHODS
from bigqmt_signal_trader.whole_quote_session import WholeQuoteClientSession
from bigqmt_signal_trader.xtquant_compat import BigQmtXtData


class Context:
    """保存真实调用签名的原生接口替身，可在订阅时同步回调。"""

    def __init__(self):
        """初始化受控原生接口或推送通道状态。"""
        self.callbacks, self.calls, self.closed = {}, [], []
        self.initial = None
        self.handle = 1
        self.error = None

    def subscribe_quote(self, *args):
        """记录代码、周期、复权和 result_type，模拟原生首次推送。"""
        self.calls.append(args[:4])
        if self.error:
            raise self.error
        self.callbacks[self.handle] = args[4]
        if self.initial is not None:
            args[4](self.initial)
        return self.handle

    def unsubscribe_quote(self, handle):
        """记录释放请求并允许测试暂时失败后的重试。"""
        if self.error:
            raise self.error
        self.closed.append(handle)


@pytest.fixture
def registry(monkeypatch):
    """使用可手动排空的真实队列，避免以 sleep 判断异步测试结果。"""
    monkeypatch.setattr(NativeBarSubscriptions, "_send_loop", lambda self: None)
    context, delivered, clock = Context(), [], [100.0]
    manager = NativeBarSubscriptions(context, lambda *item: delivered.append(item), now=lambda: clock[0])
    yield manager, context, delivered, clock
    context.error = None
    manager.close()


def subscribe(registry, code="510300.SH", sub="1"):
    """创建一项订阅并返回原生回调与控制响应。"""
    manager, context, _delivered, _clock = registry
    result = manager.subscribe("client", sub, code)
    return context.callbacks[context.handle], result


@pytest.mark.parametrize("limits", [{"max_items": 0}, {"max_bytes": 0}, {"timeout_seconds": 0}])
def test_limits_must_be_positive(limits):
    """容量和租约时长必须为正数。"""
    with pytest.raises(ValueError):
        NativeBarSubscriptions(Context(), lambda *args: None, **limits)


def test_native_call_and_synchronous_callback(registry):
    """同步首次回调先有所有权，快照与原生对象相互独立。"""
    manager, context, delivered, _clock = registry
    context.initial = {"510300.SH": {"stime": ["20261110103000"], "volume": [2**53 + 1]}}
    callback, result = subscribe(registry)
    context.initial["510300.SH"]["volume"][0] = 0
    assert context.calls == [("510300.SH", "1m", "none", "list")]
    assert result["native_minute_push"] is True
    assert result["topic"] == bar_topic("client", "1")
    assert manager._send_one()
    assert delivered[0][1]["data"]["510300.SH"]["volume"] == [2**53 + 1]
    assert not manager._send_one()
    assert manager.subscribe("client", "1", "510300.SH") == result
    assert len(context.calls) == 1
    with pytest.raises(ValueError, match="cannot change"):
        manager.subscribe("client", "1", "510500.SH")
    manager.unsubscribe("client", "1")
    manager.unsubscribe("client", "missing")
    callback({"ignored": True})
    assert len(context.closed) == 1 and not manager._queue


@pytest.mark.parametrize("client,sub,code,period", [("", "1", "x", "1m"), ("c", "", "x", "1m"),
                                                   ("c", "1", "", "1m"), ("c", "1", "x", "5m")])
def test_invalid_registration(registry, client, sub, code, period):
    """错误订阅参数不能调用原生接口。"""
    with pytest.raises(ValueError):
        registry[0].subscribe(client, sub, code, period)


@pytest.mark.parametrize("handle", [None, -1, 0, True, "1"])
def test_rejected_native_handle_rolls_back(registry, handle):
    """无效原生句柄必须回滚本地登记。"""
    manager, context, _out, _clock = registry
    context.handle = handle
    with pytest.raises(RuntimeError, match="invalid handle"):
        subscribe(registry)
    assert not manager._entries


def test_unavailable_closed_and_native_exception(registry):
    """接口缺失、关闭和原生异常均明确失败。"""
    manager, context, _out, _clock = registry
    context.error = ValueError("quota")
    with pytest.raises(ValueError, match="quota"):
        subscribe(registry)
    assert not manager._entries
    context.error = None
    context.subscribe_quote = None
    assert not manager.supported
    with pytest.raises(RuntimeError, match="unavailable"):
        subscribe(registry)
    context.subscribe_quote = lambda *args: 1
    manager.close()
    with pytest.raises(RuntimeError, match="closed"):
        subscribe(registry)


def test_lease_and_failed_release_are_retried(registry):
    """租约续期有效，退订失败的句柄允许再次清理。"""
    manager, context, _out, clock = registry
    subscribe(registry)
    clock[0] = 120
    manager.keepalive("client", "1")
    assert manager.reap_expired(140) == 0
    context.error = RuntimeError("native busy")
    with pytest.raises(RuntimeError, match="native busy"):
        manager.unsubscribe("client", "1")
    with pytest.raises(RuntimeError, match="expired"):
        manager.keepalive("client", "1")
    context.error = None
    assert manager.reap_expired() == 1
    with pytest.raises(RuntimeError, match="expired"):
        manager.keepalive("client", "1")
    subscribe(registry)
    assert manager.reap_expired(151) == 1


@pytest.mark.parametrize("byte_limit", [False, True])
def test_overflow_notifies_gap_and_stays_bounded(registry, byte_limit):
    """队列受条数和字节数限制，溢出明确交付缺口。"""
    manager, _context, delivered, _clock = registry
    callback, _result = subscribe(registry)
    if byte_limit:
        manager._max_bytes = 1
    else:
        manager._max_items = 1
    callback({"510300.SH": [{"volume": 1}]})
    callback({"510300.SH": [{"volume": 2}]})
    assert len(manager._queue) <= 1
    manager._send_one()
    assert delivered[0][1]["data"] == {"510300.SH": {"__gap": True}}
    manager.unsubscribe("client", "1")
    while manager._send_one():
        pass
    assert manager._bytes == 0


def test_snapshot_and_publish_failure_are_visible(registry):
    """快照及发送失败均生成可恢复的缺口通知。"""
    manager, _context, delivered, _clock = registry
    callback, _result = subscribe(registry)

    class BadCopy:
        """模拟原生回调对象无法建立独立快照。"""
        def __deepcopy__(self, memo):
            """复制失败不能吞掉真实的数据缺口。"""
            raise ValueError("copy failed")

    callback(BadCopy())
    assert manager._send_one()
    assert delivered[-1][1]["data"]["510300.SH"]["__gap"]
    callback({"510300.SH": []})
    publish = manager._publisher
    manager._publisher = lambda *args: (_ for _ in ()).throw(RuntimeError("network"))
    assert manager._send_one() is False
    manager._publisher = publish
    assert manager._send_one()
    assert delivered[-1][1]["data"]["510300.SH"]["__gap"]


def test_background_sender_and_close_do_not_call_native_off_thread():
    """后台线程只转发数据，关闭时完整回收原生句柄。"""
    context, delivered = Context(), threading.Event()
    manager = NativeBarSubscriptions(context, lambda *args: delivered.set())
    manager.subscribe("c", "1", "510300.SH")
    context.callbacks[1]({"510300.SH": []})
    assert delivered.wait(2)
    manager.close()
    assert not manager._thread.is_alive() and context.closed == [1]


def test_delivery_detects_sequence_and_generation_gaps():
    """重复消息被过滤，换代和丢帧先报告缺口。"""
    seen = []
    deliver = NativeBarDelivery("x", seen.append)
    for generation, seq in [("one", 1), ("one", 1), ("one", 3), ("two", 1), ("two", 0), ("two", True)]:
        deliver({"generation": generation, "sequence": seq, "data": {"x": [seq]}})
    assert seen == [{"x": [1]}, {"x": {"__gap": True}}, {"x": [3]},
                    {"x": {"__gap": True}}, {"x": [1]},
                    {"x": {"__gap": True}}, {"x": {"__gap": True}}]
    NativeBarDelivery("x", None)({"generation": "g", "sequence": 2, "data": {}})
    NativeBarDelivery("x", None).gap()


class Channel:
    """共享主题通道替身，提供可同步交付的真实会话入口。"""
    def __init__(self):
        """初始化受控原生接口或推送通道状态。"""
        self.callback, self.topics = None, []
    def start_subscriber(self, topics, callback):
        """登记过滤主题及消费函数。"""
        self.topics, self.callback = topics, callback
    def stop(self):
        """模拟幂等资源释放。"""
        self.topics = []
    def publish(self, topic, data):
        """只向已订阅主题转发，验证先监听后申请顺序。"""
        if topic in self.topics:
            self.callback(topic, data)


def handlers_for(manager):
    """使用正式 RPC 处理器组装受控服务端。"""
    owner = QuoteSubscriptionManager(SimpleNamespace())
    owner.native_bars = manager
    return BigQmtRpcHandlers("acct", SimpleNamespace(), SimpleNamespace(), quote_subscription_manager=owner)


def test_sdk_session_native_end_to_end(registry):
    """完整 SDK 到原生链路保留字段精度并正确释放句柄。"""
    manager, context, _out, _clock = registry
    handlers, channel, seen = handlers_for(manager), Channel(), []
    manager._publisher = channel.publish
    context.initial = {"510300.SH": {"stime": ["20261110103000"], "volume": [2**53 + 1], "close": [4.101001]}}
    session = WholeQuoteClientSession(handlers.handle, channel, "c")
    from xtquant.xtdata import _sdk_quote_callback
    sdk = BigQmtXtData(SimpleNamespace(call=handlers.handle))
    sdk._quote_session_factory = lambda: session
    assert not sdk.native_minute_push
    assert sdk.connect() and sdk.native_minute_push
    sub = sdk.subscribe_quote("510300.SH", "1m", callback=_sdk_quote_callback(seen.append))
    try:
        manager._send_one()
        row = seen[0]["510300.SH"][0]
        assert row["volume"] == 2**53 + 1 and row["close"] == 4.101001
        assert row["time"] == 1794277800000
        assert not sdk._bar_pollers
        session.replay_subscriptions()
        assert len(context.calls) == 1
        manager.unsubscribe("c", sub)
        session.replay_subscriptions()
        assert seen[-1] == {"510300.SH": {"__gap": True}}
        assert sdk.stop_all_subscriptions() == 1
        assert not manager._entries
    finally:
        session.close()


def test_rpc_capability_and_native_thread_affinity(registry):
    """能力声明符合实际服务端状态，原生控制在策略线程运行。"""
    handlers = handlers_for(registry[0])
    assert "native_minute_push_v1" in handlers.handle("ping")["sdk_capabilities"]
    assert {"subscribe_bar", "unsubscribe_bar"} <= LISTENER_DEFERRED_METHODS
    assert "bar_keepalive" not in LISTENER_DEFERRED_METHODS
    with pytest.raises(ValueError, match="exactly one"):
        handlers.handle("subscribe_bar", {"client_id": "c", "sub_id": "s", "codes": ["x", "y"]})
    handlers.quote_subscription_manager.native_bars = None
    assert "native_minute_push_v1" not in handlers.handle("ping")["sdk_capabilities"]
    with pytest.raises(RuntimeError, match="not configured"):
        handlers.handle("bar_keepalive", {"client_id": "c", "sub_id": "s"})


@pytest.fixture
def captured_pollers(monkeypatch):
    """记录旧轮询实际取数参数，不依赖后台线程的调度时序。"""
    pollers = []
    class Poller:
        """只捕获取数入口，生命周期仍由正式 SDK 管理。"""
        def __init__(self, fetch, callback, interval, **kwargs):
            """保存正式 SDK 传入的轮询取数函数。"""
            self.fetch, self.started, self.stopped = fetch, False, False
            pollers.append(self)
        def start(self):
            """记录轮询启动，无需创建后台线程。"""
            self.started = True
        def stop(self):
            """记录轮询停止以验证 SDK 退订所有权。"""
            self.stopped = True
    monkeypatch.setattr("bigqmt_signal_trader.xtquant_compat._BarPoller", Poller)
    return pollers


@pytest.mark.parametrize("window", [{"start_time": "20261110"}, {"end_time": "20261111"},
                                    {"count": 30}, {"count": -1}])
@pytest.mark.parametrize("capable", [False, True])
def test_minute_history_arguments_preserve_legacy_polling(window, capable, captured_pollers):
    """已有分钟历史参数继续原样传给轮询读取，即使服务端支持原生推送。"""
    from unittest.mock import Mock
    sdk = BigQmtXtData(SimpleNamespace())
    sdk.native_minute_push = capable
    sdk.get_market_data_ex = Mock(return_value={})
    handle = sdk.subscribe_quote("x", "1m", **window)
    captured_pollers[0].fetch()
    params = sdk.get_market_data_ex.call_args.kwargs
    assert params["period"] == "1m" and params["fill_data"] is False and params["use_formula"] is False
    assert all(params[name] == value for name, value in window.items())
    assert captured_pollers[0].started and sdk.native_minute_push is capable
    sdk.unsubscribe_quote(handle)
    assert captured_pollers[0].stopped and not sdk._bar_pollers


def test_default_minute_subscription_on_old_bridge_keeps_polling(captured_pollers):
    """旧桥接无能力声明时默认分钟订阅继续工作，且不冒充原生推送。"""
    from unittest.mock import Mock
    client = SimpleNamespace(call=Mock(return_value={"pong": True}))
    sdk = BigQmtXtData(client)
    sdk.get_market_data_ex = Mock(return_value={})
    handle = sdk.subscribe_quote("x", "1m")
    client.call.assert_called_once_with("ping")
    captured_pollers[0].fetch()
    assert sdk.get_market_data_ex.call_args.kwargs["count"] == 2
    assert sdk.native_minute_push is False and sdk._quote_session is None
    sdk.unsubscribe_quote(handle)
    assert captured_pollers[0].stopped


def test_old_bridge_rejects_explicit_native_batch_before_creating_handles(captured_pollers):
    """新增原生批入口明确报告能力缺失，调用方可选旧单只入口。"""
    from unittest.mock import Mock
    client = SimpleNamespace(call=Mock(return_value={"pong": True}))
    sdk = BigQmtXtData(client)
    with pytest.raises(NotImplementedError, match="does not support"):
        sdk.subscribe_quote_many(["x", "y"])
    client.call.assert_called_once_with("ping")
    assert not captured_pollers and sdk.native_minute_push is False and sdk._quote_session is None


@pytest.mark.parametrize("many", [False, True])
def test_native_subscription_error_never_falls_back(registry, captured_pollers, many):
    """握手确认能力后真实原生失败原样传播，不隐藏为兼容轮询。"""
    handlers, context = handlers_for(registry[0]), registry[1]
    context.error = RuntimeError("native quota exhausted")
    sdk = BigQmtXtData(SimpleNamespace(call=handlers.handle))
    session = WholeQuoteClientSession(handlers.handle, Channel(), "c")
    sdk._quote_session_factory = lambda: session
    try:
        with pytest.raises(RuntimeError, match="native quota exhausted"):
            sdk.subscribe_quote_many(["x"]) if many else sdk.subscribe_quote("x", "1m")
        assert sdk.native_minute_push is True and not captured_pollers
        assert not registry[0]._entries and not session._subscriptions
    finally:
        context.error = None
        session.close()


@pytest.mark.parametrize("many", [False, True])
def test_capability_handshake_failure_never_starts_polling(captured_pollers, many):
    """握手故障不是能力缺失，连接错误必须直接传给调用方。"""
    from unittest.mock import Mock
    sdk = BigQmtXtData(SimpleNamespace(call=Mock(side_effect=OSError("disconnected"))))
    with pytest.raises(OSError, match="disconnected"):
        sdk.subscribe_quote_many(["x"]) if many else sdk.subscribe_quote("x", "1m")
    assert not captured_pollers and sdk.native_minute_push is False


def test_session_failed_native_registration_cleans_local_and_remote():
    """注册响应失败时清理本地路由并尽力释放远端句柄。"""
    calls, channel = [], Channel()
    def rpc(method, params):
        """旧服务端不能确认原生能力，客户端必须明确失败。"""
        calls.append(method)
        return {}
    session = WholeQuoteClientSession(rpc, channel, "c")
    with pytest.raises(RuntimeError, match="did not confirm"):
        session.subscribe_bar("x")
    assert calls == ["subscribe_bar", "unsubscribe_bar"]
    assert not session._subscriptions and not channel.topics
    session.close()


def test_batch_193_yields_between_short_control_batches_without_pollers(registry):
    """193 只订阅分 25 个短控制批让出调度，不创建轮询线程。"""
    manager, context, _out, _clock = registry
    handlers, channel, calls = handlers_for(manager), Channel(), []
    def rpc(method, params):
        """记录控制往返次数，数据推送不计入控制请求。"""
        calls.append(method)
        return handlers.handle(method, params)
    session = WholeQuoteClientSession(rpc, channel, "c")
    sdk = BigQmtXtData(SimpleNamespace(call=handlers.handle))
    sdk._quote_session_factory = lambda: session
    codes = ["%06d.SH" % (510000 + index) for index in range(193)]
    try:
        handles = sdk.subscribe_quote_many(codes)
        assert set(handles) == set(codes) and len(set(handles.values())) == 193
        assert calls.count("subscribe_bar_many") == 25
        assert len(context.calls) == 193 and not sdk._bar_pollers
        assert sdk.native_minute_push
        assert sdk.stop_all_subscriptions() == 193
        assert not manager._entries
    finally:
        session.close()


@pytest.mark.parametrize("entries,timeout", [(None, 5), ([], 5), ([{}] * 257, 5), ([{}], 0), ([{}], 31)])
def test_batch_limits(registry, entries, timeout):
    """非法批大小和预算必须在创建原生句柄前拒绝。"""
    with pytest.raises(ValueError):
        registry[0].subscribe_many("c", entries, timeout)


def test_batch_replay_and_partial_failure_preserve_existing(registry):
    """批失败只回滚新创建的句柄，不破坏已存在订阅。"""
    manager, context, _out, _clock = registry
    entries = [{"sub_id": "1", "code": "x"}, {"sub_id": "2", "code": "y"}]
    manager.subscribe_many("c", entries[:1])
    real = context.subscribe_quote
    def failing(code, *args):
        """第三只失败时保留重放前已有句柄，仅回收本批新创建项。"""
        if code == "z":
            raise RuntimeError("quota")
        return real(code, *args)
    context.subscribe_quote = failing
    with pytest.raises(RuntimeError, match="quota"):
        manager.subscribe_many("c", entries + [{"sub_id": "3", "code": "z"}])
    assert set(manager._entries) == {("c", "1")}
    assert len(context.closed) == 1
    with pytest.raises(ValueError, match="duplicate"):
        manager.subscribe_many("c", entries[:1] * 2)


def test_batch_deadline_prevents_next_native_call(registry, monkeypatch):
    """原生调用消耗预算后不再发起下一个调用。"""
    manager, context, _out, _clock = registry
    timer = [0.0]
    real = context.subscribe_quote
    def delayed(*args):
        """模拟不可抢占的首个原生调用消耗预算，无需真实等待。"""
        result = real(*args)
        timer[0] = 6.0
        return result
    monkeypatch.setattr("bigqmt_signal_trader.native_bar_subscription.time.monotonic", lambda: timer[0])
    context.subscribe_quote = delayed
    with pytest.raises(TimeoutError, match="deadline"):
        manager.subscribe_many("c", [{"sub_id": "1", "code": "x"}, {"sub_id": "2", "code": "y"}], timeout_seconds=5.0)
    assert len(context.calls) == 1 and not manager._entries


def test_batch_failed_rollback_remains_owned(registry):
    """回滚失败不能丢失句柄所有权。"""
    manager, context, _out, _clock = registry
    real = context.subscribe_quote
    def failing(code, *args):
        """制造创建和回滚同时失败，句柄仍可被租约清理。"""
        if code == "bad":
            context.error = RuntimeError("native busy")
            raise ValueError("quota")
        return real(code, *args)
    context.subscribe_quote = failing
    with pytest.raises(ValueError, match="quota"):
        manager.subscribe_many("c", [{"sub_id": "1", "code": "ok"}, {"sub_id": "2", "code": "bad"}])
    assert ("c", "1") in manager._entries
    with pytest.raises(RuntimeError, match="cleanup pending"):
        manager.subscribe("c", "1", "ok")
    context.error = None
    assert manager.reap_expired() == 1


def test_close_continues_after_native_release_failure(registry):
    """关闭会释放其他资源，同时报告仍待重试的原生句柄。"""
    manager, context, _out, _clock = registry
    subscribe(registry)
    context.error = RuntimeError("busy")
    with pytest.raises(RuntimeError, match="cleanup failed"):
        manager.close()
    assert manager._entries and not manager._thread.is_alive()
    context.error = None
    manager.close()
    assert not manager._entries


def test_close_reports_unjoined_sender(registry):
    """发送线程未退出时不能报告关闭完成。"""
    manager = registry[0]
    manager._thread = SimpleNamespace(join=lambda timeout: None, is_alive=lambda: True)
    try:
        with pytest.raises(RuntimeError, match="did not stop"):
            manager.close()
    finally:
        manager._thread = None


def test_delivery_fences_previous_generation_after_control_confirmation():
    """控制响应确认换代后拒绝旧代次迟到数据。"""
    seen, delivery = [], NativeBarDelivery("x", None)
    delivery.callback = seen.append
    with pytest.raises(RuntimeError, match="no generation"):
        delivery.confirm(None)
    delivery.confirm("one")
    delivery({"generation": "one", "sequence": 1, "data": {"x": [1]}})
    delivery.confirm("two")
    delivery({"generation": "one", "sequence": 2, "data": {"x": [2]}})
    delivery({"generation": "two", "sequence": 1, "data": {"x": [3]}})
    assert seen == [{"x": [1]}, {"x": {"__gap": True}}, {"x": [3]}]


@pytest.mark.parametrize("codes", [[], ["x"] * 0, [""], [None], [str(i) for i in range(257)]])
def test_client_batch_input_limits(codes):
    """客户端拒绝空证券及超过约定上限的批次。"""
    session = WholeQuoteClientSession(lambda *args: None, Channel(), "c")
    with pytest.raises(ValueError):
        session.subscribe_bars(codes)


def test_client_batch_partial_confirmation_rolls_back():
    """部分批响应不能被当作完整注册成功。"""
    calls, session = [], None
    def rpc(method, params):
        """模拟损坏的批响应，不能遗留本地订阅路由。"""
        calls.append(method)
        return {}
    session = WholeQuoteClientSession(rpc, Channel(), "c")
    with pytest.raises(RuntimeError, match="complete"):
        session.subscribe_bars(["x", "y"])
    assert calls == ["subscribe_bar_many", "unsubscribe_bar_many"]
    assert not session._subscriptions


def test_batch_lease_missing_reports_gap_and_recovers(registry):
    """缺失租约报告缺口，重放和续租均使用批控制接口。"""
    manager, _context, _out, clock = registry
    handlers, seen, calls = handlers_for(manager), [], []
    def rpc(method, params):
        """记录批续租和重放控制路径，拒绝逐证券续租。"""
        calls.append(method)
        return handlers.handle(method, params)
    session = WholeQuoteClientSession(rpc, Channel(), "c")
    session.subscribe_bars(["x", "y"], seen.append)
    entries = list(session._subscriptions.items())
    assert session._keepalive_bars(entries) == 0
    assert calls[-1] == "bar_keepalive_many"
    clock[0] += 31
    assert handlers.quote_subscription_manager.reap_expired() == 2
    assert session._keepalive_bars(entries) == 2
    assert seen == [{"x": {"__gap": True}}, {"y": {"__gap": True}}]
    session.replay_subscriptions()
    assert calls[-1] == "subscribe_bar_many"
    assert session._keepalive_bars(entries) == 0
    session.unsubscribe_bars()
    assert calls[-1] == "unsubscribe_bar_many"
    session.close()


def test_batch_lease_network_and_callback_failure_do_not_kill_heartbeat():
    """连接和消费者故障不能终止唯一心跳线程。"""
    def failure(*args):
        """模拟连接中断及消费者异常。"""
        raise RuntimeError("broken")
    session = WholeQuoteClientSession(failure, Channel(), "c")
    entry = {"callback": NativeBarDelivery("x", failure)}
    assert session._keepalive_bars([(1, entry)]) == 1
    assert session.push_health()["callback_failed"]
    session.close()


def test_heartbeat_uses_one_batch_for_minute_leases(registry):
    """完整心跳流程只进行一次分钟批续租。"""
    handlers, calls = handlers_for(registry[0]), []
    def rpc(method, params):
        """执行一次完整心跳后在等待点停止，避免基于时间的断言。"""
        calls.append(method)
        return handlers.handle(method, params)
    session = WholeQuoteClientSession(rpc, Channel(), "c")
    session.subscribe_bars(["x", "y"])
    session._started = True
    session._heartbeat_stop.wait = lambda timeout: setattr(session, "_started", False)
    session._heartbeat_loop()
    assert calls == ["subscribe_bar_many", "bar_keepalive_many"]
    session.unsubscribe_bars()
    session.close()


@pytest.mark.parametrize("many", [False, True])
def test_push_start_failure_does_not_leak_local_routes(many):
    """启动推送失败不会留下本地假订阅。"""
    channel = Channel()
    channel.start_subscriber = lambda *args: (_ for _ in ()).throw(RuntimeError("push unavailable"))
    session = WholeQuoteClientSession(lambda *args: None, channel, "c")
    with pytest.raises(RuntimeError, match="push unavailable"):
        session.subscribe_bars(["x", "y"]) if many else session.subscribe_bar("x")
    assert not session._subscriptions
    session.close()


@pytest.mark.parametrize("many", [False, True])
def test_failed_cleanup_preserves_original_subscribe_error(many):
    """清理异常不能掩盖最初的注册失败原因。"""
    def rpc(method, params):
        """服务端响应损坏且连接断开时保留注册错误，租约负责最终清理。"""
        if method.startswith("unsubscribe"):
            raise OSError("disconnected")
        return {}
    session = WholeQuoteClientSession(rpc, Channel(), "c")
    with pytest.raises(RuntimeError, match="confirm"):
        session.subscribe_bars(["x"]) if many else session.subscribe_bar("x")
    assert not session._subscriptions
    session.close()


def test_callback_snapshot_finishes_after_unsubscribe(registry):
    """退订过程中迟到的回调快照不会重新入队。"""
    manager, _context, _out, _clock = registry
    callback, _result = subscribe(registry)
    class LateSnapshot:
        """在复制期间完成退订，迟到快照不能重新进入队列。"""
        def __deepcopy__(self, memo):
            """模拟交错执行，不依赖线程调度概率。"""
            manager.unsubscribe("client", "1")
            return {"510300.SH": []}
    callback(LateSnapshot())
    assert not manager._queue


def test_reaper_and_batch_release_keep_failed_handles_owned(registry):
    """批退订和租约清理失败的句柄仍由服务端持有。"""
    manager, context, _out, _clock = registry
    subscribe(registry)
    context.error = OSError("busy")
    with pytest.raises(RuntimeError, match="batch cleanup"):
        manager.unsubscribe_many("client", ["1"])
    with pytest.raises(RuntimeError, match="lease cleanup"):
        manager.reap_expired()
    context.error = None
    assert manager.reap_expired() == 1


def test_rpc_batch_validation_and_unknown_lease(registry):
    """批控制参数需要校验，未知租约明确返回缺失编号。"""
    handlers = handlers_for(registry[0])
    with pytest.raises(ValueError, match="client_id"):
        handlers.handle("subscribe_bar_many", {})
    for method in ("bar_keepalive_many", "unsubscribe_bar_many"):
        with pytest.raises(ValueError, match="1..256"):
            handlers.handle(method, {"client_id": "c", "sub_ids": []})
    assert handlers.handle("bar_keepalive_many", {"client_id": "c", "sub_ids": ["none"]}) == {"missing": ["none"]}


def test_sdk_batch_wrapper_keeps_integer_precision(monkeypatch):
    """标准 SDK 批入口仍保留原生大整数成交量。"""
    from xtquant import xtdata
    seen = []
    def native(codes, period, callback, timeout_seconds):
        """经正式 SDK 包装层交付原生列数组。"""
        callback({"x": {"stime": ["20261110103000"], "volume": [2**53 + 1]}})
        return {"x": 1}
    monkeypatch.setattr("bigqmt_signal_trader.xtquant_compat.xtdata", SimpleNamespace(subscribe_quote_many=native))
    assert xtdata.subscribe_quote_many(["x"], callback=seen.append) == {"x": 1}
    assert seen[0]["x"][0]["volume"] == 2**53 + 1
    with pytest.raises(ValueError, match="valid time"):
        xtdata._sdk_quote_callback(seen.append)({"x": {"stime": ["bad"]}})
    with pytest.raises(ValueError, match="only support"):
        BigQmtXtData(SimpleNamespace()).subscribe_quote_many(["x"], period="5m")


def test_connection_capability_is_cached_and_reset():
    """能力通过握手更新，属性读取无 RPC 副作用。"""
    calls = []
    def rpc(*args):
        """只有握手确认后才暴露原生能力，普通属性读取不执行 RPC。"""
        calls.append(args)
        return {"pong": True, "sdk_capabilities": ["native_minute_push_v1"]}
    sdk = BigQmtXtData(SimpleNamespace(call=rpc))
    assert sdk.native_minute_push is False and not calls
    sdk.connect()
    sdk.connect()
    assert sdk.native_minute_push and len(calls) == 1
    sdk.disconnect()
    assert not sdk.native_minute_push


def test_single_subscription_control_lifecycle(registry):
    """单证券控制接口完成注册、续租和退订生命周期。"""
    handlers = handlers_for(registry[0])
    params = {"client_id": "c", "sub_id": "s", "codes": ["x"]}
    handlers.handle("subscribe_bar", params)
    assert handlers.handle("bar_keepalive", params) == {}
    assert handlers.handle("unsubscribe_bar", params) == {}
    assert not registry[0]._entries


def test_replay_response_does_not_restore_concurrently_removed_route(registry):
    """迟到的重放响应不能恢复已撤销的本地订阅。"""
    handlers, channel = handlers_for(registry[0]), Channel()
    session = WholeQuoteClientSession(handlers.handle, channel, "c")
    handles = session.subscribe_bars(["x"])
    def rpc(method, params):
        """控制回复返回前完成退订，重放响应不能恢复已删除路由。"""
        result = handlers.handle(method, params)
        if method == "subscribe_bar_many":
            session._subscriptions.pop(handles["x"])
        return result
    session._rpc = rpc
    session.replay_subscriptions()
    assert not session._subscriptions
    session.close()


@pytest.mark.parametrize("batch_size", [0, 33])
def test_client_control_batch_size_is_bounded(batch_size):
    """控制批大小不能超出避免长时间独占 QMT 的边界。"""
    with pytest.raises(ValueError, match="1..32"):
        WholeQuoteClientSession(lambda *args: None, Channel(), "c", bar_subscribe_batch_size=batch_size)


@pytest.mark.parametrize("fail_after", [0, 1])
def test_client_batches_share_one_deadline_and_rollback(registry, monkeypatch, fail_after):
    """全部控制批共享同一预算，超时回滚所有已建句柄。"""
    manager, context, _out, _clock = registry
    handlers, clock, calls = handlers_for(manager), [100.0], []
    def rpc(method, params):
        """每批消耗同一假时钟预算，后续批不能重新获得完整超时时间。"""
        calls.append((method, params))
        result = handlers.handle(method, params)
        if method == "subscribe_bar_many":
            clock[0] += 2
        return result
    monkeypatch.setattr("bigqmt_signal_trader.whole_quote_session.time.monotonic", lambda: clock[0])
    session = WholeQuoteClientSession(rpc, Channel(), "c", bar_subscribe_batch_size=1)
    with pytest.raises(TimeoutError, match="deadline"):
        session.subscribe_bars(["x", "y"], timeout_seconds=float(fail_after))
    assert not session._subscriptions and not manager._entries
    assert len(context.calls) == fail_after
    if fail_after:
        assert calls[0][1]["timeout_seconds"] == 1.0
    assert calls[-1][0] == "unsubscribe_bar_many"
    session.close()


def test_second_control_batch_failure_releases_first_batch(registry):
    """后一批创建失败时前一批成功资源也会释放。"""
    manager, context, _out, _clock = registry
    real = context.subscribe_quote
    def native(code, *args):
        """第二批的原生失败必须回收已成功的第一批句柄。"""
        if code == "bad":
            raise RuntimeError("quota")
        return real(code, *args)
    context.subscribe_quote = native
    session = WholeQuoteClientSession(handlers_for(manager).handle, Channel(), "c", bar_subscribe_batch_size=1)
    with pytest.raises(RuntimeError, match="quota"):
        session.subscribe_bars(["ok", "bad"])
    assert not manager._entries and not session._subscriptions
    assert len(context.closed) == 1
    session.close()


def test_sdk_selected_batch_unsubscribe_preserves_other_routes(registry, monkeypatch):
    """仅退订指定分钟句柄，未选分钟保留，Tick 仍通过原有单项路径释放。"""
    from xtquant import xtdata
    manager, _context, _out, _clock = registry
    handlers, calls = handlers_for(manager), []
    def rpc(method, params):
        """记录退订请求，证明部分集合只需一次批控制。"""
        calls.append(method)
        return handlers.handle(method, params)
    session = WholeQuoteClientSession(rpc, Channel(), "c")
    sdk = BigQmtXtData(SimpleNamespace())
    sdk._quote_session = session
    handles = session.subscribe_bars(["x", "y", "z"])
    monkeypatch.setattr("bigqmt_signal_trader.xtquant_compat.xtdata", sdk)
    other = []
    sdk.unsubscribe_quote = other.append
    assert xtdata.unsubscribe_quote_many([handles["x"], handles["y"], handles["x"], 999]) == 0
    assert calls[-1] == "unsubscribe_bar_many" and other == [999]
    assert set(session._subscriptions) == {handles["z"]}
    session.unsubscribe_bars()
    session.close()
    sdk._quote_session = None
    sdk.unsubscribe_quote_many([42])
    assert other[-1] == 42


def test_heartbeat_does_not_replay_unconfirmed_initial_subscriptions(registry):
    """首批尚未完成时，心跳和重放不能并发申请仍在创建中的句柄。"""
    handlers, calls, nested = handlers_for(registry[0]), [], [False]
    session = None
    def rpc(method, params):
        """在控制响应之前执行一次心跳，模拟较慢的原生首次订阅。"""
        calls.append(method)
        if method == "subscribe_bar_many" and not nested[0]:
            nested[0] = True
            session.replay_subscriptions()
            session._started = True
            session._heartbeat_stop.wait = lambda timeout: setattr(session, "_started", False)
            session._heartbeat_loop()
        return handlers.handle(method, params)
    session = WholeQuoteClientSession(rpc, Channel(), "c")
    session.subscribe_bars(["x", "y"])
    assert calls == ["subscribe_bar_many"]
    session.unsubscribe_bars()
    session.close()


def test_generation_confirmation_serializes_with_inflight_delivery():
    """换代不能越过尚未交付完的旧回调，避免缺口之后再次混入旧代次数据。"""
    entered, release, attempting, confirmed = (threading.Event() for _ in range(4))
    seen = []
    def callback(data):
        """阻塞旧消息交付以明确控制换代交错顺序。"""
        if data == {"x": [1]}:
            entered.set()
            assert release.wait(2)
        seen.append(data)
    delivery = NativeBarDelivery("x", callback)
    delivery.confirm("old")
    old = threading.Thread(target=lambda: delivery({"generation": "old", "sequence": 1, "data": {"x": [1]}}))
    def confirm():
        """只有旧回调结束后才能确认新代次并发出缺口。"""
        attempting.set()
        delivery.confirm("new")
        confirmed.set()
    changer = threading.Thread(target=confirm)
    old.start()
    assert entered.wait(2)
    changer.start()
    try:
        assert attempting.wait(2)
        assert not confirmed.wait(0.02)
    finally:
        release.set()
        old.join(2)
        changer.join(2)
    assert seen == [{"x": [1]}, {"x": {"__gap": True}}]


def test_sdk_session_forwards_remaining_budget_only_for_native_creation(monkeypatch):
    """正式客户端会话把剩余秒数传给传输等待，其余控制方法不改变默认超时。"""
    from unittest.mock import Mock
    from bigqmt_signal_trader import xtquant_compat
    client = SimpleNamespace(call=Mock(return_value={}), timeout_seconds=17)
    sdk = BigQmtXtData(client)
    sdk._build_quote_push_channel = Channel
    monkeypatch.setattr(xtquant_compat, "_client_backend", lambda _client: SimpleNamespace(acknowledged_push=False))
    session = sdk._build_quote_session()
    session._rpc("subscribe_bar_many", {"timeout_seconds": 2.5})
    client.call.assert_called_with("subscribe_bar_many", {"timeout_seconds": 2.5}, timeout_seconds=2.5)
    session._rpc("bar_keepalive_many", {"sub_ids": [1]})
    client.call.assert_called_with("bar_keepalive_many", {"sub_ids": [1]})
    assert session._bar_subscribe_timeout == 17
    session.close()


def test_runtime_reset_accepts_older_manager_without_native_bars(monkeypatch):
    """旧 Tick 管理器没有分钟组件时，重置仍会释放推送通道。"""
    from unittest.mock import Mock
    import bigqmt_signal_trader_strategy as runtime
    manager, channel = SimpleNamespace(_combos={}, _sub_index={}), SimpleNamespace(stop=Mock())
    monkeypatch.setattr(runtime, "_rpc_service", None)
    monkeypatch.setattr(runtime, "_quote_subscription_service", (manager, channel))
    runtime.reset_app()
    channel.stop.assert_called_once()


def test_expired_server_batch_does_not_start_first_native_call(registry, monkeypatch):
    """执行前预算耗尽时连第一只也不调用，避免已过期工作占用原生线程。"""
    ticks = iter([100.0, 106.0])
    monkeypatch.setattr("bigqmt_signal_trader.native_bar_subscription.time.monotonic", lambda: next(ticks, 106.0))
    with pytest.raises(TimeoutError, match="deadline"):
        registry[0].subscribe_many("c", [{"sub_id": 1, "code": "x"}], timeout_seconds=5)
    assert not registry[1].calls


def test_heartbeat_ignores_concurrently_removed_subscription():
    """取到订阅编号后发生退订，迟到心跳不能复活本地路由。"""
    calls = []
    session = WholeQuoteClientSession(lambda *args: calls.append(args), Channel(), "c")
    session._subscriptions[1] = {"codes": ["x"]}
    def check_stop():
        """控制编号快照与路由查找之间的退订交错。"""
        session._subscriptions.pop(1, None)
        return False
    session._heartbeat_stop.is_set = check_stop
    session._heartbeat_stop.wait = lambda _timeout: setattr(session, "_started", False)
    session._started = True
    session._heartbeat_loop()
    assert not calls and not session._subscriptions
    session.close()
