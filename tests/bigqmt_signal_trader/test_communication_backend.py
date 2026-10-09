"""验证后端切换同时覆盖 RPC、行情推送、交易回报和 QMT 调度。"""

import os
import sys
import threading
from unittest.mock import Mock, patch

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader_redis_rpc_runtime as runtime
from bigqmt_signal_trader import exec_events
from bigqmt_signal_trader.quote_push_channel import QuotePushChannel
from bigqmt_signal_trader.quote_subscription_manager import build_quote_subscription_service
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, RedisPubSubRpcService
from bigqmt_signal_trader.transports import (
    CommunicationBackend, RpcTransport, TransportError, build_transport,
    get_backend, register_backend,
)
from bigqmt_signal_trader.transports import backend as registry
from bigqmt_signal_trader.transports.backend import zmq_push_address
from bigqmt_signal_trader.whole_quote_session import WholeQuoteClientSession
from bigqmt_signal_trader.xtquant_compat import (
    BigQmtRpcClient, BigQmtXtTrader, RpcServerRepliedError,
    XtQuantTraderCallback, _build_client_push_channel,
)
from bigqmt_signal_trader_strategy import _resolve_background_threads


class MemoryBus(object):
    """测试用进程内通信介质，完全不依赖 Redis 或套接字。"""

    def __init__(self):
        """保存服务器、请求响应及订阅对象。"""
        self.server = None
        self.pump = None
        self.responses = {}
        self.requests = []
        self.subscribers = []


class MemoryRpc(RpcTransport):
    """只实现传输契约，实际请求仍由正式 RPC 服务和处理器执行。"""

    def __init__(self, config, account_id, print_prefix):
        """从后端参数取得测试介质，不解释业务方法。"""
        super(MemoryRpc, self).__init__(account_id, print_prefix)
        self.bus = config["bus"]

    def start_receiving(self, on_request, background_threads=True):
        """安装正式服务回调，不启动测试线程。"""
        super(MemoryRpc, self).start_receiving(on_request)
        self.bus.server = self

    def send_request(self, request, timeout_seconds):
        """模拟接收与策略回调，返回匹配本次请求编号的结果。"""
        self.bus.requests.append(request)
        self.bus.pump()
        return self.bus.responses.pop(request["request_id"])

    def drain_request_queue(self, max_items=20):
        """由模拟策略回调非阻塞收取一批请求，保持服务端线程语义。"""
        count = min(max_items, len(self.bus.requests))
        for _ in range(count):
            self.deliver(self.bus.requests.pop(0))
        return count

    def send_response(self, request, response):
        """按请求编号保存响应，验证正式服务的回复路由。"""
        self.bus.responses[request["request_id"]] = response


class MemoryPush(QuotePushChannel):
    """以同一测试介质验证行情与交易回报的推送契约。"""

    def __init__(self, role, config, account_id, print_prefix):
        """创建未启动通道，介质由测试夹具持有。"""
        self.bus = config["bus"]
        self.topics = []
        self.callback = None

    @property
    def publisher_endpoint(self):
        """提供与 TCP 无关的诊断端点。"""
        return "memory-test://events"

    def start_publisher(self):
        """测试介质无需额外发布资源。"""

    def start_subscriber(self, topics, on_msg):
        """登记主题和回调，停止时解除登记。"""
        self.topics = list(topics)
        self.callback = on_msg
        if self not in self.bus.subscribers:
            self.bus.subscribers.append(self)

    def publish(self, topic, data):
        """只向订阅此主题的对象投递原业务字典。"""
        for subscriber in list(self.bus.subscribers):
            if topic in subscriber.topics:
                subscriber.callback(topic, data)

    def stop(self):
        """解除本通道的登记。"""
        if self in self.bus.subscribers:
            self.bus.subscribers.remove(self)


class Context(object):
    """代替原生 QMT 行情源，保存正式订阅适配器安装的回调。"""

    def __init__(self):
        """初始化原生订阅列表。"""
        self.callbacks = {}

    def subscribe_whole_quote(self, code_list, callback=None):
        """返回正整数订阅号，回调由测试主动触发。"""
        handle = len(self.callbacks) + 1
        self.callbacks[handle] = callback
        return handle

    def unsubscribe_quote(self, handle):
        """删除对应原生订阅。"""
        self.callbacks.pop(handle, None)


class Recorder(XtQuantTraderCallback):
    """接收通过正式交易 SDK 分发的成交对象。"""

    def __init__(self):
        """保存收到的成交。"""
        self.trades = []

    def on_stock_trade(self, trade):
        """记录成交以验证字段转换与账号过滤。"""
        self.trades.append(trade)


@pytest.fixture
def memory_backend():
    """注册独立后端并在测试结束后恢复全局注册表。"""
    bus = MemoryBus()
    config = {"transport": "memory_test", "redis_enabled": False,
              "backend_config": {"bus": bus}}
    with patch.dict(registry._BACKENDS):
        register_backend(CommunicationBackend(
            "memory_test", MemoryRpc, MemoryPush,
            supports_scheduler_polling=True, execution_events_via_push=True,
        ))
        yield bus, config


def make_service(bus, config):
    """通过正式工厂组装请求和行情服务，并绑定模拟策略回调。"""
    context = Context()
    manager, publisher = build_quote_subscription_service(
        context, "memory_test", "acct", rpc_config=config,
    )
    handlers = BigQmtRpcHandlers(
        "acct", market_data=object(), position_provider=object(),
        quote_subscription_manager=manager,
    )
    server = RedisPubSubRpcService(
        None, handlers, "acct", transport=build_transport("memory_test", config),
        debug_log_limit=0,
    )
    server.start()
    publisher.start_publisher()
    bus.pump = lambda: (server.drain_request_queue(), server.drain_pending())
    return context, manager, publisher, server


def test_registered_backend_carries_rpc_quotes_and_trades(memory_backend):
    """改后端配置后，真实 SDK、RPC 服务和行情管理器仍能收发三类业务。"""
    bus, config = memory_backend
    context, manager, publisher, server = make_service(bus, config)
    client = BigQmtRpcClient(account_id="acct", redis_config=config)
    assert client.call("ping", use_formula=False)["allow_order_methods"] is False
    quote_seen = threading.Event()
    received = []

    def on_quote(data):
        """记录正式订阅会话分发的行情。"""
        received.append(data)
        quote_seen.set()

    session = WholeQuoteClientSession(
        lambda method, params: client.call(method, params, use_formula=False),
        _build_client_push_channel(client), "quote-client",
    )
    trader = BigQmtXtTrader(account_id="acct", redis_config=config)
    recorder = Recorder()
    trader.register_callback(recorder)
    trade_channel = trader._build_quote_push_channel()
    try:
        session.start()
        session.subscribe_whole_quote(["510300.SH"], on_quote)
        sample = {"510300.SH": {"lastPrice": 4.1, "volume": 100}}
        next(iter(context.callbacks.values()))(sample)
        assert quote_seen.wait(1.0)
        assert received == [sample]
        assert manager._push_endpoint == "memory-test://events"
        assert trader._exec_transport_uses_push()
        trade_channel.start_subscriber(
            list(exec_events.EXEC_TOPICS.values()), trader._on_push_exec_event,
        )
        exec_events.publish_exec_event(publisher, "acct", {
            "event_type": "trade", "account_id": "acct", "stock_code": "510300.SH",
            "action": "BUY", "trade_id": "t1", "order_sys_id": "sys1",
            "volume": 100, "price": 4.1,
        })
        assert len(recorder.trades) == 1
        assert recorder.trades[0].stock_code == "510300.SH"
    finally:
        session.close()
        trade_channel.stop()
        trader.stop()
        server.stop()
        publisher.stop()
    assert not bus.subscribers


def test_switch_keeps_account_and_order_method_guards(memory_backend):
    """后端替换不能绕过正式服务的账号校验与默认禁止交易规则。"""
    bus, config = memory_backend
    context, manager, publisher, server = make_service(bus, config)
    client = BigQmtRpcClient(account_id="acct", redis_config=config)
    try:
        with pytest.raises(RpcServerRepliedError, match="account_id mismatch"):
            client.call("ping", account_id="other", use_formula=False)
        with pytest.raises(RpcServerRepliedError, match="not allowed"):
            client.call("submit_order", use_formula=False)
    finally:
        server.stop()
        publisher.stop()


def test_disabled_redis_never_opens_a_side_channel(memory_backend):
    """禁用 Redis 后，自定义后端的回报监听不能另建 Redis 连接。"""
    bus, config = memory_backend
    trader = BigQmtXtTrader(account_id="acct", redis_config=config)
    trader.client._redis = Mock(side_effect=AssertionError("unexpected Redis"))
    assert trader._exec_events_redis_or_none() is None
    channel = trader._build_quote_push_channel()
    trader.client._redis.assert_not_called()
    channel.stop()
    trader.stop()


def test_backend_capabilities_control_qmt_scheduler(memory_backend):
    """新后端按能力进入策略回调轮询，无需新增名称分支。"""
    assert _resolve_background_threads("memory_test", False) is False
    assert _resolve_background_threads("memory_test", None) is True
    assert _resolve_background_threads("shm", False) is True


def test_request_only_backend_rejects_push_without_fallback():
    """缺少推送实现时明确失败，不悄悄使用 Redis。"""
    backend = CommunicationBackend("rpc_only_test", lambda **kwargs: None)
    with pytest.raises(TransportError, match="does not support event push"):
        backend.build_push_channel("client")


def test_registration_cannot_silently_replace_existing_backend(memory_backend):
    """重复名称必须显式替换，避免 RPC 与推送意外换代。"""
    backend = get_backend("memory_test")
    with pytest.raises(ValueError, match="already registered"):
        register_backend(backend)
    register_backend(backend, replace=True)
    assert get_backend("MEMORY_TEST") is backend
    with pytest.raises(ValueError, match="unknown rpc transport"):
        get_backend("not_registered")


@pytest.mark.parametrize("role,key", [("client", "connect_address"), ("server", "bind_address")])
def test_push_endpoint_follows_explicit_rpc_endpoint(role, key):
    """显式 RPC 地址必须同时决定推送的主机和端口。"""
    config = {"zmq": {key: "tcp://192.0.2.10:18000", "host": "127.0.0.1", "port": 17000}}
    assert zmq_push_address(role, config, "acct") == "tcp://192.0.2.10:18001"
    config["zmq"]["quote_push_" + key] = "tcp://192.0.2.11:19000"
    assert zmq_push_address(role, config, "acct") == "tcp://192.0.2.11:19000"


def test_non_tcp_rpc_cannot_invent_a_tcp_push_endpoint():
    """非 TCP 端点需显式指定推送端点，避免更换介质时残留 TCP。"""
    with pytest.raises(ValueError, match="non-TCP RPC"):
        zmq_push_address("client", {"zmq": {"connect_address": "inproc://test"}})
    with pytest.raises(ValueError, match="leave room"):
        zmq_push_address("server", {"zmq": {"bind_address": "tcp://127.0.0.1:65535"}})


def test_runtime_forwards_backend_config_without_changing_permissions(memory_backend):
    """动态配置与初始化透传后端参数，交易权限仍由独立配置控制。"""
    bus, config = memory_backend
    captured = {}
    with patch.dict(runtime.__dict__):
        runtime.configure = lambda **values: captured.update(values)
        runtime.set_account_id = lambda account: None
        runtime.configure_runtime_redis(dict(config, rpc_allow_order_methods=False))
        runtime._apply_config("acct")
        assert captured["rpc"]["backend_config"]["bus"] is bus
        assert captured["rpc"]["allow_order_methods"] is False
        assert captured["rpc"]["transport"] == "memory_test"


@pytest.mark.parametrize("generic", [False, True])
def test_runtime_forwards_shared_memory_configuration(generic):
    """外层及通用参数均可把共享端点传到策略组装层，且不改变交易权限。"""
    captured = {}
    shm = {"namespace": "Local\\acceptance", "rpc_slots": 17, "trade_push_bytes": 32768}
    config = {"transport": "shm", "redis_enabled": False, "rpc_allow_order_methods": False}
    config["backend_config" if generic else "shm"] = {"shm": shm} if generic else shm
    with patch.dict(runtime.__dict__):
        runtime.RPC_SHM_CONFIG = {}
        runtime.RPC_BACKEND_CONFIG = {}
        runtime.configure = lambda **values: captured.update(values)
        runtime.set_account_id = lambda account: None
        runtime.configure_runtime_redis(config)
        runtime._apply_config("acct")
        assert captured["rpc"]["shm"] == shm
        assert captured["rpc"]["allow_order_methods"] is False
        assert captured["rpc"]["transport"] == "shm"
