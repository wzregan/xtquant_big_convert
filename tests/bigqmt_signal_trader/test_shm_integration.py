"""验证确认型共享推送接入真实回调、交易门禁、消费屏障及故障恢复。"""

import importlib
import multiprocessing
import os
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader_strategy as strategy
from bigqmt_signal_trader import xtquant_compat
from bigqmt_signal_trader.quote_push_channel import QuotePushChannel
from bigqmt_signal_trader.quote_subscription_manager import BufferedAcknowledgedPublisher
from bigqmt_signal_trader.sdk_contract import ExecutionRejected
from bigqmt_signal_trader.transports import CommunicationBackend, TransportError, register_backend
from bigqmt_signal_trader.transports import backend as registry
from bigqmt_signal_trader.whole_quote_session import WholeQuoteClientSession
from bigqmt_signal_trader.xtquant_compat import (
    BigQmtRpcClient, BigQmtXtTrader, RpcServerRepliedError, _build_client_push_channel,
)


class Backpressure(TransportError):
    """尚未共享提交的暂时背压，可以重试同一消息编号。"""

    retryable = True


class CallbackFailure(TransportError):
    """用户回调失败不能自动无限重放副作用。"""

    callback_failed = True


class RecordingChannel(QuotePushChannel):
    """记录共享提交与订阅生命周期，替代 IPC 介质而保留接入层业务路径。"""

    supports_acknowledgements = True

    def __init__(self):
        """保存消息、尝试编号和可控故障。"""
        self.committed = []
        self.attempts = []
        self.error = None
        self.health_error = None
        self.stopped = 0
        self.started = 0
        self.topics = []

    @property
    def publisher_endpoint(self):
        """返回测试用非网络端点。"""
        return "recording://push"

    def start_publisher(self):
        """记录正式发布入口调用。"""
        self.started += 1

    def start_subscriber(self, topics, on_msg):
        """保存订阅及回调，便于测试真实分发行为。"""
        self.started += 1
        self.topics = list(topics)
        self.callback = on_msg

    def publish(self, topic, data, message_id=None, timeout_seconds=None):
        """仅在无故障时记录共享提交，始终保存尝试的稳定编号。"""
        self.attempts.append((message_id, timeout_seconds))
        if self.error is not None:
            raise self.error
        self.committed.append((topic, data))
        return True

    def check_health(self):
        """模拟接收线程或共享日志出现可见故障。"""
        if self.health_error is not None:
            raise self.health_error
        return {"healthy": True}

    def stop(self):
        """统计回收次数。"""
        self.stopped += 1


@pytest.fixture(autouse=True)
def isolate_configuration(monkeypatch):
    """隔离后端及客户端配置，不接触外部 Redis 或真实账户。"""
    monkeypatch.setattr(xtquant_compat, "load_client_config", lambda: {})
    with patch.dict(registry._BACKENDS):
        yield


@pytest.fixture
def acknowledged_backend():
    """注册确认型后端定义，工厂只返回测试通道。"""
    captured = []

    def push_factory(role, config, account_id, print_prefix):
        """保存两种用途的客户端配置，验证订阅身份稳定且互不争用。"""
        captured.append(config)
        return RecordingChannel()

    backend = CommunicationBackend("ack_integration_test", Mock(), push_factory,
                                   execution_events_via_push=True, acknowledged_push=True)
    register_backend(backend)
    return captured


def test_queue_snapshots_native_payload_and_prioritizes_execution():
    """原生回调立即完成，后续数据复用不污染快照，交易先于行情共享提交。"""
    channel = RecordingChannel()
    publisher = BufferedAcknowledgedPublisher(channel)
    quote = {"510300.SH": {"lastPrice": 4.1, "bid": [4.0]}}
    publisher.publish("510300.SH", quote)
    publisher.publish("exec:trade", {"volume": 100})
    quote["510300.SH"]["bid"].append(99.0)
    assert channel.committed == []
    assert publisher.status()["local_queued"] == 2
    assert publisher.status()["shared_committed"] == 0
    assert publisher.flush() == 2
    assert channel.committed == [
        ("exec:trade", {"volume": 100}),
        ("510300.SH", {"510300.SH": {"lastPrice": 4.1, "bid": [4.0]}}),
    ]
    assert all(0.0 < timeout <= 0.01 for _, timeout in channel.attempts)
    assert channel.attempts[1][1] <= channel.attempts[0][1]


def test_backpressure_keeps_fifo_and_same_message_id_until_commit():
    """背压后队首保留，恢复时沿用同一编号，成功提交才释放本地预算。"""
    channel = RecordingChannel()
    publisher = BufferedAcknowledgedPublisher(channel)
    publisher.publish("exec:trade", {"id": 1})
    channel.error = Backpressure("full")
    assert publisher.flush() == 0
    assert publisher.status()["new_orders_blocked"] is True
    assert publisher.status()["local_pending"]["exec"] == 1
    channel.error = None
    assert publisher.flush() == 1
    assert channel.attempts[0][0] == channel.attempts[1][0]
    assert publisher.status()["new_orders_blocked"] is False
    assert publisher.status()["shared_committed"] == 1


def test_queue_overflow_is_sticky_without_overwriting_older_events():
    """有界容量满时保留旧回报，故障不会因队列排空而自动宣称已对账。"""
    channel = RecordingChannel()
    publisher = BufferedAcknowledgedPublisher(channel, {"local_exec_queue_items": 2})
    publisher.publish("exec:trade", {"id": 1})
    assert publisher.status()["new_orders_blocked"] is False
    publisher.publish("exec:trade", {"id": 2})
    with pytest.raises(TransportError, match="queue is full"):
        publisher.publish("exec:trade", {"id": 3})
    assert publisher.flush() == 2
    assert [data["id"] for _, data in channel.committed] == [1, 2]
    assert publisher.status()["overflow_count"] == 1
    assert publisher.status()["new_orders_blocked"] is True


def test_execution_and_quote_queue_budgets_are_independent():
    """行情拥塞不能吃掉交易的本地保留容量。"""
    publisher = BufferedAcknowledgedPublisher(RecordingChannel(), {"local_quote_queue_items": 1})
    publisher.publish("510300.SH", {"price": 4.1})
    with pytest.raises(TransportError):
        publisher.publish("510300.SH", {"price": 4.2})
    assert publisher.publish("exec:trade", {"volume": 100}) is True
    assert publisher.status()["local_pending"] == {"exec": 1, "quote": 1}


def test_stop_reports_locally_accepted_but_not_shared_committed_messages():
    """停止不无限发送，不把尚未共享提交的回报计成已经交付。"""
    channel = RecordingChannel()
    publisher = BufferedAcknowledgedPublisher(channel)
    publisher.publish("exec:trade", {"id": 1})
    publisher.stop()
    assert publisher.status()["pending_at_stop"] == 1
    assert publisher.status()["shared_committed"] == 0
    assert publisher.status()["local_pending"]["exec"] == 1
    assert publisher.flush() == 0
    assert channel.stopped == 1


def test_bad_channel_health_blocks_new_orders_even_without_pending_messages():
    """共享资源损坏不能因当前队列为空而漏过交易门禁。"""
    channel = RecordingChannel()
    channel.health_error = TransportError("corrupt header")
    publisher = BufferedAcknowledgedPublisher(channel)
    assert publisher.flush() == 0
    assert publisher.status()["new_orders_blocked"] is True
    assert "corrupt header" in publisher.status()["sticky_error"]


def test_gate_keeps_original_queries_and_cancels_available_during_backpressure():
    """新报单被阻断，但原业务处理器的查询、撤单、权限信息仍保持可用。"""
    channel = RecordingChannel()
    publisher = BufferedAcknowledgedPublisher(channel)
    original = Mock(side_effect=lambda method, params=None: {"allow_order_methods": True, "method": method})
    handlers = SimpleNamespace(handle=original, _canonical_method=lambda method: method)
    strategy._install_acknowledged_push_gate(handlers, publisher)
    publisher.publish("exec:trade", {"id": 1})
    channel.error = Backpressure("full")
    publisher.flush()
    for method in ("submit_order", "submit_orders_batch", "passorder"):
        with pytest.raises(ExecutionRejected, match="new orders blocked") as rejected:
            handlers.handle(method)
        assert rejected.value.error_code == "EXECUTION_NOT_STARTED"
    assert handlers.handle("cancel_order")["method"] == "cancel_order"
    assert handlers.handle("query_stock_orders")["method"] == "query_stock_orders"
    pong = handlers.handle("ping")
    assert pong["allow_order_methods"] is True
    assert pong["communication_health"]["new_orders_blocked"] is True
    assert original.call_count == 3


def test_real_native_trade_callback_is_queued_then_flushed_on_strategy_thread(monkeypatch):
    """真实成交归一化入口不等待共享日志，策略回调负责有界提交。"""
    channel = RecordingChannel()
    publisher = BufferedAcknowledgedPublisher(channel)
    config = {"account_id": "acct", "rpc": {"redis_enabled": False}}
    monkeypatch.setattr(strategy, "_build_config", lambda: config)
    monkeypatch.setattr(strategy, "_quote_subscription_service", (object(), publisher))
    monkeypatch.setattr(strategy, "_rpc_service", None)
    strategy._publish_exec_event("trade", {
        "m_strAccountID": "acct", "m_strInstrumentID": "510300", "m_strExchangeID": "SH",
        "m_nVolume": 100, "m_dPrice": 4.1, "m_strTradeID": "t1", "m_strOrderSysID": "s1",
    })
    assert publisher.status()["local_pending"]["exec"] == 1
    assert channel.committed == []
    assert strategy._flush_acknowledged_push(config) == 1
    topic, event = channel.committed[0]
    assert topic == "exec:trade"
    assert event["trade_id"] == "t1"
    assert event["volume"] == 100


def test_stable_subscriber_identity_separates_quotes_from_trades(acknowledged_backend):
    """同一客户端重建保持身份，行情与交易的游标身份必须不同。"""
    trader = BigQmtXtTrader(account_id="acct", redis_config={"transport": "ack_integration_test"})
    try:
        _build_client_push_channel(trader.client, "exec")
        _build_client_push_channel(trader.client, "exec")
        _build_client_push_channel(trader.client, "quote")
        ids = [config["shm"]["subscriber_id"] for config in acknowledged_backend]
        assert ids[0] == ids[1]
        assert ids[0] != ids[2]
    finally:
        trader.stop()


def test_callback_error_is_propagated_to_acknowledged_push(acknowledged_backend):
    """用户成交回调失败必须传回日志消费端，不能被兼容 SDK 吞掉后提前确认。"""
    trader = BigQmtXtTrader(account_id="acct", redis_config={"transport": "ack_integration_test"})
    trader.register_callback(SimpleNamespace(on_stock_trade=Mock(side_effect=RuntimeError("user failed"))))
    try:
        with pytest.raises(RuntimeError, match="user failed"):
            trader._on_push_exec_event("exec:trade", {"account_id": "acct", "event_type": "trade"})
    finally:
        trader.stop()


def test_async_order_barrier_delays_ack_until_user_callback_completed(acknowledged_backend):
    """异步响应先于成交回调，屏障暂存本身不能成为共享日志确认。"""
    trader = BigQmtXtTrader(account_id="acct", redis_config={"transport": "ack_integration_test"})
    trace = []
    trader.register_callback(SimpleNamespace(on_stock_trade=lambda trade: trace.append("trade")))
    trader._event_running = True
    trader._arm_order_barrier("remark", 1)
    complete = threading.Event()

    def receive():
        """模拟共享日志消费线程，返回才表示该回报可确认。"""
        trader._on_push_exec_event("exec:trade", {
            "account_id": "acct", "event_type": "trade", "remark": "remark",
        })
        complete.set()

    thread = threading.Thread(target=receive, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 1.0
        while not trader._async_barrier["remark"]["events"] and time.monotonic() < deadline:
            time.sleep(0.001)
        assert not complete.is_set()
        assert trace == []
        trace.append("response")
        trader._release_order_barrier("remark", 1)
        assert complete.wait(1.0)
        assert trace == ["response", "trade"]
    finally:
        trader.stop()
        thread.join(1.0)


def test_execution_callback_failure_notifies_disconnect_and_stops_auto_replay(acknowledged_backend, monkeypatch):
    """回调副作用失败后明确断连，后台不能不停重复同一未确认回报。"""
    trader = BigQmtXtTrader(account_id="acct", redis_config={"transport": "ack_integration_test"})
    callback = Mock()
    trader.register_callback(callback)
    channel = RecordingChannel()
    channel.health_error = CallbackFailure("callback failed")
    trader._build_quote_push_channel = Mock(return_value=channel)
    trader._event_running = True
    monkeypatch.setattr(xtquant_compat.time, "sleep", lambda seconds: None)
    try:
        trader._event_loop_push_channel()
        assert trader._event_running is False
        assert trader._push_callback_failed is True
        assert trader._push_listener_error
        callback.on_disconnected.assert_called_once()
        assert channel.stopped == 1
    finally:
        trader.stop()


def test_quote_transport_recovery_replays_same_remote_subscription_ids():
    """传输损坏时重建实际通道，远端订阅编号保持不变且健康显式要求对账。"""
    original = RecordingChannel()
    replacement = RecordingChannel()
    calls = []

    def rpc(method, params):
        """记录订阅与重放使用的原始编号。"""
        calls.append((method, dict(params)))
        return {"topic": "510300.SH"}

    session = WholeQuoteClientSession(rpc, original, "quote-client",
                                      push_channel_factory=lambda: replacement)
    sub_id = session.subscribe_whole_quote(["510300.SH"], lambda data: None)
    original.health_error = TransportError("publisher epoch changed")
    try:
        assert session._check_push_health() is True
        assert original.stopped == 1
        assert replacement.started == 1
        assert session.has_subscription(sub_id)
        assert [params["sub_id"] for method, params in calls if method == "subscribe_whole_quote"] == [sub_id, sub_id]
        assert session.push_health()["reconciliation_required"] is True
        assert session.push_health()["healthy"] is False
    finally:
        session.close()


def test_quote_callback_failure_does_not_recreate_and_repeat_side_effects():
    """行情回调失败保留未确认游标，自动健康循环不能重建后无穷重复。"""
    channel = RecordingChannel()
    factory = Mock(return_value=RecordingChannel())
    callback = Mock(side_effect=RuntimeError("quote callback failed"))
    session = WholeQuoteClientSession(lambda method, params: {"topic": "510300.SH"}, channel,
                                      "quote-client", push_channel_factory=factory)
    session.subscribe_whole_quote(["510300.SH"], callback)
    try:
        with pytest.raises(RuntimeError, match="quote callback failed"):
            channel.callback("510300.SH", {"price": 4.1})
        channel.health_error = CallbackFailure("callback failed")
        assert session._check_push_health() is False
        assert session._check_push_health() is False
        factory.assert_not_called()
        callback.assert_called_once()
        assert session.push_health()["callback_failed"] is True
    finally:
        session.close()


def test_temporary_health_lock_busy_does_not_poison_or_drop_pending_events():
    """健康元数据锁繁忙只延后提交，恢复后沿用现有待发布快照。"""
    channel = RecordingChannel()
    publisher = BufferedAcknowledgedPublisher(channel)
    publisher.publish("exec:trade", {"id": 1})
    channel.health_error = Backpressure("health lock busy")
    assert publisher.flush() == 0
    assert publisher.status()["new_orders_blocked"] is True
    assert publisher.status()["sticky_error"] == ""
    assert publisher.status()["local_pending"]["exec"] == 1
    channel.health_error = None
    assert publisher.flush() == 1
    assert publisher.status()["new_orders_blocked"] is False


def test_permanent_publish_error_requires_explicit_reconciliation_instead_of_retry():
    """永久错误仍保留队首，但不能自行再次提交可能不确定的消息。"""
    channel = RecordingChannel()
    publisher = BufferedAcknowledgedPublisher(channel)
    publisher.publish("exec:trade", {"id": 1})
    channel.error = TransportError("permanent failure")
    assert publisher.flush() == 0
    channel.error = None
    assert publisher.flush() == 0
    assert len(channel.attempts) == 1
    assert publisher.status()["local_pending"]["exec"] == 1
    assert publisher.status()["new_orders_blocked"] is True


def test_queue_copies_mutable_leaf_objects_instead_of_preserving_config_ownership():
    """事件叶子也归包装器独立所有，不能复用配置复制的注入对象语义。"""
    channel = RecordingChannel()
    publisher = BufferedAcknowledgedPublisher(channel)
    payload = {"prices": bytearray(b"123")}
    publisher.publish("exec:trade", payload)
    payload["prices"][0] = ord("9")
    assert publisher.flush() == 1
    assert channel.committed[0][1]["prices"] == bytearray(b"123")


def test_snapshot_failure_is_visible_to_submit_gate():
    """原生数据无法形成独立快照时不能只打印错误后继续接受交易。"""
    channel = RecordingChannel()
    publisher = BufferedAcknowledgedPublisher(channel)
    with pytest.raises(TransportError, match="snapshot failed"):
        publisher.publish("exec:trade", {"unserializable": threading.Lock()})
    assert publisher.status()["new_orders_blocked"] is True
    assert publisher.status()["local_queued"] == 0


def test_quote_health_lock_busy_preserves_channel_and_clears_after_recovery():
    """临时锁忙不能生成错误的新代次或重复行情回调。"""
    channel = RecordingChannel()
    factory = Mock(return_value=RecordingChannel())
    session = WholeQuoteClientSession(lambda method, params: {"topic": "510300.SH"}, channel,
                                      "quote-client", push_channel_factory=factory)
    session.subscribe_whole_quote(["510300.SH"], lambda data: None)
    channel.health_error = Backpressure("health lock busy")
    try:
        assert session._check_push_health() is False
        assert session.push_health()["reconciliation_required"] is False
        assert session.push_health()["health_check_busy"] is True
        channel.health_error = None
        assert session._check_push_health() is True
        assert session.push_health()["healthy"] is True
        factory.assert_not_called()
        assert channel.stopped == 0
    finally:
        session.close()


def test_execution_health_lock_busy_does_not_disconnect_or_rebuild(acknowledged_backend, monkeypatch):
    """交易订阅锁忙一轮后恢复，原共享游标及实际回报线程保持不变。"""
    trader = BigQmtXtTrader(account_id="acct", redis_config={"transport": "ack_integration_test"})
    callback = Mock()
    trader.register_callback(callback)
    channel = RecordingChannel()
    trader._build_quote_push_channel = Mock(return_value=channel)
    trader._event_running = True
    channel.check_health = Mock(side_effect=[Backpressure("health lock busy"), {"healthy": True}])
    sleeps = []

    def advance(seconds):
        """控制只经历一次锁忙和一次恢复，不引入真实延迟。"""
        sleeps.append(seconds)
        if len(sleeps) == 2:
            trader._event_running = False

    monkeypatch.setattr(xtquant_compat.time, "sleep", advance)
    try:
        trader._event_loop_push_channel()
        callback.on_disconnected.assert_not_called()
        trader._build_quote_push_channel.assert_called_once()
        assert channel.check_health.call_count == 2
        assert trader._push_callback_failed is False
    finally:
        trader.stop()


def _spawn_quote_sdk_client(settings, connection):
    """独立进程通过真实 SDK 和后端工厂查询与订阅，不使用继承的映射句柄。"""
    xtquant_compat.load_client_config = lambda: {}
    client = BigQmtRpcClient(account_id="integration-acct", transport="shm", timeout_seconds=2.0,
                             redis_config={"redis_enabled": False, "shm": settings})
    client._redis = Mock(side_effect=AssertionError("unexpected Redis"))
    received = []
    arrived = threading.Event()
    channel = _build_client_push_channel(client, "quote")
    session = WholeQuoteClientSession(lambda method, params: client.call(method, params, use_formula=False),
                                      channel, "spawn-sdk-client")

    def consume(data):
        """记录跨进程推送，返回后才允许共享日志确认。"""
        received.append(data)
        arrived.set()

    try:
        pong = client.call("ping", use_formula=False)
        sub_id = session.subscribe_whole_quote(["510300.SH"], consume)
        connection.send(("ready", channel._signal_name(channel._identity)))
        if not arrived.wait(3.0):
            raise AssertionError("cross-process quote did not arrive")
        deadline = time.monotonic() + 1.0
        while channel.status()["acknowledged"] != 1 and time.monotonic() < deadline:
            time.sleep(0.002)
        connection.send(("result", {"pong": pong, "sub_id": sub_id, "received": received,
                                    "acknowledged": channel.status()["acknowledged"],
                                    "redis_calls": client._redis.call_count}))
    except Exception as exc:
        connection.send(("error", "%s: %s" % (type(exc).__name__, exc)))
        raise
    finally:
        session.close()
        if client._transport_instance is not None:
            client._transport_instance.stop()
        connection.close()


def test_real_strategy_shm_factory_and_spawn_sdk_rpc_quote_roundtrip(tmp_path, monkeypatch):
    """真实策略组装、独立进程 SDK RPC 和行情推送共同使用 SHM，且无 Redis 旁路。"""
    from bigqmt_signal_trader.adapters import redis_common
    from bigqmt_signal_trader.transports.shm_primitives import cleanup

    redis_factory = Mock(side_effect=AssertionError("unexpected Redis"))
    monkeypatch.setattr(redis_common, "build_redis_client", redis_factory)
    # 本用例验证 IPC 组装；禁止策略编辑器的模块重载改变其它测试已导入的类身份。
    monkeypatch.setattr(importlib, "reload", lambda module: module)
    monkeypatch.setattr(strategy, "_rpc_service", None)
    monkeypatch.setattr(strategy, "_quote_subscription_service", None)
    settings = {
        "namespace": str(tmp_path), "directory": str(tmp_path),
        "rpc_slots": 4, "rpc_slot_bytes": 4096,
        "market_push_bytes": 131072, "trade_push_bytes": 32768,
        "market_push_slots": 16, "trade_push_slots": 8, "max_push_subscribers": 4,
        "wait_poll_seconds": 0.005, "push_poll_seconds": 0.005,
        "subscriber_id": "spawn-sdk-stable", "local_flush_wall_seconds": 0.05,
    }
    callbacks = []

    def subscribe(codes, callback):
        """替代本机 QMT 行情源，保留实际服务端订阅与回调路径。"""
        callbacks.append(callback)
        return 1

    context = SimpleNamespace(subscribe_whole_quote=subscribe, unsubscribe_quote=Mock())
    config = {"account_id": "integration-acct", "rpc": {
        "enabled": True, "transport": "shm", "redis_enabled": False,
        "allow_order_methods": False, "shm": settings,
    }}
    service = strategy._start_rpc_service(context, SimpleNamespace(), config)
    manager, wrapper = strategy._quote_subscription_service
    transport, raw_push = service._transport, wrapper._channel
    assert service.communication_backend.name == "shm"
    assert service.background_threads is False
    assert wrapper.supports_acknowledgements is True
    assert service.listen_redis is None
    assert service.handlers.order_identity_redis_client is None
    spawn = multiprocessing.get_context("spawn")
    parent, child = spawn.Pipe()
    process = spawn.Process(target=_spawn_quote_sdk_client, args=(settings, child))
    names = {transport._name, transport._name + "_request", transport._name + "_space",
             raw_push._name, raw_push._space_name}
    names.update(transport._name + "_reply_%d" % index for index in range(settings["rpc_slots"]))
    process.start()
    child.close()
    result = None
    try:
        deadline = time.monotonic() + 7.0
        while result is None and time.monotonic() < deadline:
            strategy._drain_rpc_service(config)
            if parent.poll(0.002):
                kind, payload = parent.recv()
                if kind == "error":
                    pytest.fail(payload)
                if kind == "ready":
                    names.add(payload)
                    assert len(callbacks) == 1
                    data = {"510300.SH": {"lastPrice": 4.1, "bidPrice": [4.0]}}
                    callbacks[0](data)
                    data["510300.SH"]["bidPrice"].append(99.0)
                    assert wrapper.status()["local_queued"] == 1
                if kind == "result":
                    result = payload
        assert result is not None, "spawn SDK did not finish within bounded deadline"
        process.join(2.0)
        assert process.exitcode == 0
        assert result["pong"]["pong"] is True
        assert result["pong"]["account_id"] == "integration-acct"
        assert result["pong"]["communication_health"]["healthy"] is True
        assert result["received"] == [{"510300.SH": {"lastPrice": 4.1, "bidPrice": [4.0]}}]
        assert result["acknowledged"] == 1
        assert result["redis_calls"] == 0
        assert wrapper.status()["shared_committed"] == 1
        assert manager._combos
        redis_factory.assert_not_called()
    finally:
        if process.is_alive():
            process.terminate()
        process.join(2.0)
        names.update(raw_push._signal_name(identity) for identity in raw_push._signals)
        wrapper.stop()
        service.stop()
        parent.close()
        for name in names:
            cleanup(name, str(tmp_path))


def test_sdk_callback_failure_remains_visible_after_subscriber_stops(tmp_path):
    """服务端晚检查也必须发现用户回调失败，订阅端关闭不能清除交易门禁证据。"""
    from bigqmt_signal_trader.transports.shm_primitives import cleanup

    settings = {"namespace": str(tmp_path), "directory": str(tmp_path),
                "market_push_bytes": 65536, "trade_push_bytes": 32768,
                "market_push_slots": 8, "trade_push_slots": 4, "max_push_subscribers": 4,
                "subscriber_id": "callback-failure-stable", "push_poll_seconds": 0.002}
    backend = registry.get_backend("shm")
    raw = backend.build_push_channel("server", {"shm": settings}, account_id="acct")
    wrapper = BufferedAcknowledgedPublisher(raw)
    trader = BigQmtXtTrader(account_id="acct", redis_config={"transport": "shm", "shm": settings,
                                                            "redis_enabled": False})
    trader.register_callback(SimpleNamespace(on_stock_trade=Mock(side_effect=RuntimeError("user failed"))))
    client = trader._build_quote_push_channel()
    names = {raw._name, raw._space_name, client._signal_name(client._identity)}
    try:
        wrapper.start_publisher()
        client.start_subscriber(["exec:trade"], trader._on_push_exec_event)
        wrapper.publish("exec:trade", {"account_id": "acct", "event_type": "trade", "trade_id": "t1"})
        assert wrapper.flush(max_wall_seconds=0.05) == 1
        deadline = time.monotonic() + 1.0
        while not client.status()["callback_failed"] and time.monotonic() < deadline:
            time.sleep(0.002)
        assert client.status()["callback_failed"] is True
        assert client.status()["acknowledged"] == 0
        client.stop()
        assert wrapper.flush(max_wall_seconds=0.05) == 0
        assert wrapper.status()["new_orders_blocked"] is True
        assert "callback" in wrapper.status()["sticky_error"]
        trader.callback.on_stock_trade.assert_called_once()
    finally:
        names.update(raw._signal_name(identity) for identity in raw._signals)
        client.stop()
        wrapper.stop()
        trader.stop()
        for name in names:
            cleanup(name, str(tmp_path))


def test_whole_quote_shm_supports_multiple_topics_and_explicit_resubscribe(tmp_path):
    """真实稳定游标支持正常增删组合；全部退订后也能由用户显式订阅新组合。"""
    from bigqmt_signal_trader.transports.shm_primitives import cleanup

    settings = {"namespace": str(tmp_path), "directory": str(tmp_path),
                "market_push_bytes": 65536, "trade_push_bytes": 32768,
                "market_push_slots": 16, "trade_push_slots": 8, "max_push_subscribers": 4,
                "subscriber_id": "multi-quote-stable", "push_poll_seconds": 0.002}
    backend = registry.get_backend("shm")
    server = backend.build_push_channel("server", {"shm": settings}, account_id="acct")
    client = backend.build_push_channel("client", {"shm": settings}, account_id="acct")
    received = [[], [], []]

    def rpc(method, params):
        """替代业务注册持久化，保留实际 SDK 的组合主题维护与共享接收流程。"""
        if method == "subscribe_whole_quote":
            return {"topic": ",".join(params["codes"])}
        return {}

    def wait_count(index, count):
        """等待真实接收线程处理有限条消息，失败不得无限挂住测试。"""
        deadline = time.monotonic() + 1.0
        while len(received[index]) < count and time.monotonic() < deadline:
            time.sleep(0.002)
        assert len(received[index]) == count

    session = WholeQuoteClientSession(rpc, client, "multi-quote-client")
    names = {server._name, server._space_name, client._signal_name(client._identity)}
    try:
        server.start_publisher()
        first = session.subscribe_whole_quote(["510300.SH"], received[0].append)
        second = session.subscribe_whole_quote(["159915.SZ"], received[1].append)
        server.publish("510300.SH", {"price": 4.1})
        server.publish("159915.SZ", {"price": 2.1})
        wait_count(0, 1)
        wait_count(1, 1)
        session.unsubscribe_quote(first)
        server.publish("510300.SH", {"price": 4.2})
        server.publish("159915.SZ", {"price": 2.2})
        wait_count(1, 2)
        assert len(received[0]) == 1
        session.unsubscribe_quote(second)
        third = session.subscribe_whole_quote(["512000.SH"], received[2].append)
        server.publish("512000.SH", {"price": 1.1})
        wait_count(2, 1)
        assert session.has_subscription(third)
        assert session._check_push_health() is True
    finally:
        names.update(server._signal_name(identity) for identity in server._signals)
        session.close()
        client.stop()
        server.stop()
        for name in names:
            cleanup(name, str(tmp_path))


@pytest.mark.parametrize("unknown", [False, True, None])
def test_sdk_preserves_explicit_response_execution_status_and_original_request_id(unknown):
    """错误包的结果确定性与原请求编号原样到 SDK，缺字段不推断为未执行。"""
    client = BigQmtRpcClient(account_id="acct", transport="shm")
    response = {"ok": False, "error": "response encoding failed", "error_code": "SHM_RESPONSE_ENCODING",
                "request_id": "原始请求:00/A"}
    if unknown is not None:
        response["outcome_unknown"] = unknown
    client._transport_instance = SimpleNamespace(send_request=Mock(return_value=response))
    with pytest.raises(RpcServerRepliedError) as captured:
        client.call("synthetic_write", use_formula=False)
    assert captured.value.outcome_unknown is unknown
    assert captured.value.request_id == "原始请求:00/A"
    assert captured.value.error_code == "SHM_RESPONSE_ENCODING"


def test_sdk_general_server_error_does_not_claim_request_never_executed():
    """普通业务诊断可能出现在已提交之后，默认结果语义继续是未知。"""
    client = BigQmtRpcClient(account_id="acct", transport="shm")
    client._transport_instance = SimpleNamespace(send_request=Mock(return_value={
        "ok": True, "data": None, "server_error": "order identity not found", "request_id": "original",
    }))
    with pytest.raises(RpcServerRepliedError) as captured:
        client.call("synthetic_write", use_formula=False)
    assert captured.value.outcome_unknown is None
    assert captured.value.request_id == "original"
    assert captured.value.error_code == ""


@pytest.mark.parametrize("unknown", [False, True])
def test_sdk_server_diagnostic_preserves_supplied_execution_fields(unknown):
    """成功包内的服务诊断同样保留显式执行状态、错误码及原请求编号。"""
    client = BigQmtRpcClient(account_id="acct", transport="shm")
    client._transport_instance = SimpleNamespace(send_request=Mock(return_value={
        "ok": True, "data": [1], "server_error": "native submission diagnostic",
        "request_id": "diagnostic-original", "outcome_unknown": unknown, "error_code": "NATIVE_DIAGNOSTIC",
    }))
    with pytest.raises(RpcServerRepliedError) as captured:
        client.call("synthetic_write", use_formula=False)
    assert captured.value.outcome_unknown is unknown
    assert captured.value.request_id == "diagnostic-original"
    assert captured.value.error_code == "NATIVE_DIAGNOSTIC"


@pytest.mark.parametrize("unknown", [False, True])
def test_sdk_keeps_original_shared_memory_timeout_instance(unknown):
    """传输层超时对象及编号不被 SDK 重新包装，从而保留明确执行门禁结果。"""
    from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTimeout

    client = BigQmtRpcClient(account_id="acct", transport="shm")
    original = SharedMemoryTimeout("original timeout", request_id="original-timeout-id", outcome_unknown=unknown)
    client._transport_instance = SimpleNamespace(send_request=Mock(side_effect=original))
    with pytest.raises(SharedMemoryTimeout) as captured:
        client.call("synthetic_write", use_formula=False)
    assert captured.value is original
    assert captured.value.outcome_unknown is unknown
    assert captured.value.request_id == "original-timeout-id"


def _synthetic_async_jobs():
    """构造同账户三笔队列任务，不通过公开接口启动后台下单线程。"""
    return [(index + 1, ("acct",), {"stock_code": "510300.SH", "order_type": 23,
                                    "order_volume": 100, "price_type": 11, "price": 4.1,
                                    "order_remark": "synthetic-%d" % index}) for index in range(3)]


@pytest.mark.parametrize("unknown", [False, None])
def test_batch_explicit_not_started_and_legacy_error_keep_single_fallback(unknown):
    """确定尚未执行及旧服务明确拒绝维持原逐单回退，不扩大兼容行为变化。"""
    trader = BigQmtXtTrader(account_id="acct", redis_config={"transport": "shm"})
    trader.order_stock_batch = Mock(side_effect=RpcServerRepliedError("batch refused", outcome_unknown=unknown))
    trader.order_stock_result = Mock(return_value={"order_sys_id": "synthetic-id", "user_order_id": "synthetic"})
    try:
        trader._submit_async_jobs(_synthetic_async_jobs())
        trader.order_stock_batch.assert_called_once()
        assert trader.order_stock_result.call_count == 3
        assert trader._async_callback_queue.qsize() == 3
        while not trader._async_callback_queue.empty():
            assert trader._async_callback_queue.get_nowait()["kind"] == "response"
            trader._async_callback_queue.task_done()
    finally:
        trader.stop()


@pytest.mark.parametrize("failure_stage", ["response_encoding", "partial_handler", "result_conversion", "server_error"])
def test_actual_shm_batch_executed_then_error_never_resubmits(tmp_path, monkeypatch, failure_stage):
    """业务异常、转换/编码错误及成功包内原生诊断都不能重报，逐单明确 -4 待核对。"""
    from bigqmt_signal_trader import redis_rpc
    from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService
    from bigqmt_signal_trader.transports.shm_primitives import cleanup

    settings = {"namespace": str(tmp_path), "directory": str(tmp_path), "rpc_slots": 4,
                "rpc_slot_bytes": 1024, "max_payload_bytes": 8192, "wait_poll_seconds": 0.005}
    backend = registry.get_backend("shm")
    server = backend.build_rpc_transport({"shm": settings}, account_id="acct")
    trader = BigQmtXtTrader(account_id="acct", redis_config={"transport": "shm", "shm": settings,
                                                            "redis_enabled": False})
    trader.client._formula_router = lambda: None
    trader.order_stock_result = Mock(side_effect=AssertionError("duplicate single-order fallback"))
    callback = SimpleNamespace(on_order_error=Mock())
    trader.register_callback(callback)
    executed = []
    reply_ids = []
    reply_snapshots = []
    original_reply = server.send_response
    sentinel = object()
    original_conversion = redis_rpc.to_jsonable

    def convert(value, dataframe_format="records"):
        """仅破坏业务结果转换，保留其它实际服务编解码流程。"""
        if value is sentinel:
            raise RuntimeError("injected result conversion failure")
        return original_conversion(value, dataframe_format)

    def handle(method, params):
        """代替原生网关执行合成记录，明确在交易可能生效之后注入不同异常。"""
        assert method == "order_stock_batch"
        if failure_stage == "partial_handler":
            executed.append(dict(params["orders"][0]))
            raise RuntimeError("injected handler failure after first item")
        executed.extend(dict(item) for item in params["orders"])
        if failure_stage == "result_conversion":
            return sentinel
        if failure_stage == "server_error":
            handlers._last_server_error = "native orders submitted; identity confirmation failed"
            return [{"success": True, "order_sys_id": "synthetic-%d" % index} for index in range(3)]
        return "x" * 16384

    def send_response(request, response):
        """记录实际批量请求编号，保留真实编码及共享回包路径。"""
        reply_ids.append(request["request_id"])
        reply_snapshots.append(dict(response))
        return original_reply(request, response)

    monkeypatch.setattr(server, "send_response", send_response)
    monkeypatch.setattr(redis_rpc, "to_jsonable", convert)
    handlers = SimpleNamespace(handle=handle)
    service = RedisPubSubRpcService(None, handlers, account_id="acct", transport=server,
                                   process_in_listener=True, listener_methods=("order_stock_batch",),
                                   background_threads=True)
    names = {server._name, server._name + "_request", server._name + "_space"}
    names.update(server._name + "_reply_%d" % index for index in range(settings["rpc_slots"]))
    try:
        service.start()
        trader._submit_async_jobs(_synthetic_async_jobs())
        assert len(executed) == (1 if failure_stage == "partial_handler" else 3)
        assert len(reply_ids) == 1
        if failure_stage == "server_error":
            assert reply_snapshots[0]["ok"] is True
            assert reply_snapshots[0]["outcome_unknown"] is True
            assert reply_snapshots[0]["server_error"]
            assert len(reply_snapshots[0]["data"]) == 3
        assert server.status()["executed"] == 1
        assert trader.client._transport_instance.status()["published"] == 1
        trader.order_stock_result.assert_not_called()
        assert trader._async_callback_queue.qsize() == 3
        for index in range(3):
            unit = trader._async_callback_queue.get_nowait()
            assert unit["kind"] == "error"
            assert unit["seq"] == index + 1
            assert unit["error_id"] == -4
            assert unit["outcome_unknown"] is True
            assert unit["request_id"] == reply_ids[0]
            expected_code = "SHM_RESPONSE_ENCODING" if failure_stage == "response_encoding" else ""
            assert unit["error_code"] == expected_code
            assert reply_ids[0] in unit["error_msg"]
            trader._fire_async_outcome(unit)
            trader._async_callback_queue.task_done()
        assert callback.on_order_error.call_count == 3
        for call in callback.on_order_error.call_args_list:
            error = call.args[0]
            assert error.error_id == -4
            assert error.outcome_unknown is True
            assert error.request_id == reply_ids[0]
            assert error.error_code == expected_code
    finally:
        trader.stop()
        if trader.client._transport_instance is not None:
            trader.client._transport_instance.stop()
        service.stop()
        for name in names:
            cleanup(name, str(tmp_path))


def test_actual_shm_account_rejection_marks_business_not_attempted(tmp_path, monkeypatch):
    """真实 SHM 服务前置账户拒绝明确尚未尝试业务，SDK保留原拒绝请求编号。"""
    from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService
    from bigqmt_signal_trader.transports.shm_primitives import cleanup

    settings = {"namespace": str(tmp_path), "directory": str(tmp_path), "rpc_slots": 4,
                "rpc_slot_bytes": 4096, "wait_poll_seconds": 0.005}
    server = registry.get_backend("shm").build_rpc_transport({"shm": settings}, account_id="acct")
    client = BigQmtRpcClient(account_id="acct", transport="shm", redis_config={"shm": settings,
                                                                             "redis_enabled": False})
    handle = Mock(side_effect=AssertionError("account rejection must happen before handler"))
    original_reply = server.send_response
    replies = []

    def send_response(request, response):
        """记录服务原响应，SDK错误编号必须与实际发布的拒绝包一致。"""
        replies.append(dict(response))
        return original_reply(request, response)

    monkeypatch.setattr(server, "send_response", send_response)
    service = RedisPubSubRpcService(None, SimpleNamespace(handle=handle), account_id="acct", transport=server,
                                   process_in_listener=True, listener_methods=("synthetic_write",),
                                   background_threads=True)
    names = {server._name, server._name + "_request", server._name + "_space"}
    names.update(server._name + "_reply_%d" % index for index in range(settings["rpc_slots"]))
    try:
        service.start()
        with pytest.raises(RpcServerRepliedError, match="account_id mismatch") as captured:
            client.call("synthetic_write", account_id="different-acct", use_formula=False)
        handle.assert_not_called()
        assert captured.value.outcome_unknown is False
        assert captured.value.request_id == replies[0]["request_id"]
        assert replies[0]["outcome_unknown"] is False
        assert replies[0]["ok"] is False
        assert client._transport_instance.status()["published"] == 1
    finally:
        if client._transport_instance is not None:
            client._transport_instance.stop()
        service.stop()
        for name in names:
            cleanup(name, str(tmp_path))


def test_legacy_service_handler_failure_does_not_add_execution_status():
    """未声明执行门禁的旧后端保持原错误包，不能被新 SHM 标记规则影响。"""
    from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService

    handler = Mock(side_effect=RuntimeError("legacy handler failure"))
    service = RedisPubSubRpcService(None, SimpleNamespace(handle=handler), account_id="acct",
                                   transport=SimpleNamespace(send_response=Mock()))
    response = service.process_request({"request_id": "legacy-request", "account_id": "acct",
                                        "method": "synthetic_write", "params": {}})
    assert response["ok"] is False
    assert "outcome_unknown" not in response


def test_legacy_service_success_with_diagnostic_keeps_old_response_semantics():
    """未声明执行门禁的成功诊断包不增加结果状态，保留旧 SDK 的未知默认。"""
    from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService

    handlers = SimpleNamespace(handle=Mock(return_value=[1]), _last_server_error="legacy diagnostic")
    service = RedisPubSubRpcService(None, handlers, account_id="acct",
                                   transport=SimpleNamespace(send_response=Mock()))
    response = service.process_request({"request_id": "legacy-request", "account_id": "acct",
                                        "method": "synthetic_write", "params": {}})
    assert response["ok"] is True
    assert response["server_error"] == "legacy diagnostic"
    assert "outcome_unknown" not in response
