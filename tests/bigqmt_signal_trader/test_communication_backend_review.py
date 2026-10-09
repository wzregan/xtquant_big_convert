"""独立验证后端定义一致性、并发初始化、配置隔离和失败清理。"""

import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader_strategy as strategy
from bigqmt_signal_trader import xtquant_compat
from bigqmt_signal_trader.quote_push_channel import QuotePushChannel
from bigqmt_signal_trader.quote_subscription_manager import build_quote_subscription_service
from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService
from bigqmt_signal_trader.transports import CommunicationBackend, RpcTransport, TransportError, register_backend
from bigqmt_signal_trader.transports import backend as registry
from bigqmt_signal_trader.transports.factory import build_transport
from bigqmt_signal_trader.xtquant_compat import BigQmtRpcClient, BigQmtXtTrader, _build_client_push_channel


class TaggedRpc(RpcTransport):
    """记录工厂代次，不依赖任何外部连接。"""

    def __init__(self, tag, config, account_id="", print_prefix=""):
        """保存代次标记、工厂参数和关闭次数。"""
        super(TaggedRpc, self).__init__(account_id, print_prefix)
        self.tag = tag
        self.config = config
        self.stop_count = 0

    def send_request(self, request, timeout_seconds):
        """按请求编号返回成功响应，使 SDK 走完整收发入口。"""
        return {"ok": True, "request_id": request["request_id"], "data": self.tag}

    def send_response(self, request, response):
        """测试客户端无需服务端发送。"""

    def stop(self):
        """统计本实例回收次数。"""
        self.stop_count += 1
        super(TaggedRpc, self).stop()


class TaggedPush(QuotePushChannel):
    """记录推送所属代次与配置，不创建线程。"""

    def __init__(self, tag, config):
        """保存推送后端身份。"""
        self.tag = tag
        self.config = config
        self.stop_count = 0

    def start_publisher(self):
        """内存测试无需发布资源。"""

    def start_subscriber(self, topics, on_msg):
        """保存订阅入口，测试不投递事件。"""
        self.callback = on_msg

    def publish(self, topic, data):
        """内存测试无需传输资源。"""

    def stop(self):
        """统计本实例回收次数。"""
        self.stop_count += 1


class PollingTaggedRpc(TaggedRpc):
    """满足 ZMQ 轮询能力声明的连接替身，用于检查连接配置。"""

    def drain_request_queue(self, max_items=20):
        """测试不投递请求，但提供真实覆盖的方法以满足能力契约。"""
        return 0


def tagged_backend(name="review_test", tag="old", execution_events_via_push=True):
    """创建带身份标记的一组成套工厂，便于发现混用不同代次。"""

    def rpc_factory(config, account_id, print_prefix):
        """创建当前代次的 RPC 实例。"""
        return TaggedRpc(tag, config, account_id, print_prefix)

    def push_factory(role, config, account_id, print_prefix):
        """创建当前代次的推送实例。"""
        return TaggedPush(tag, config)

    return CommunicationBackend(name, rpc_factory, push_factory,
                                execution_events_via_push=execution_events_via_push)


@pytest.fixture(autouse=True)
def isolated_configuration(monkeypatch):
    """恢复后端注册表并禁止加载机器上的实际客户端配置。"""
    monkeypatch.setattr(xtquant_compat, "load_client_config", lambda: {})
    with patch.dict(registry._BACKENDS):
        yield


@pytest.mark.parametrize("warm_rpc", [False, True])
def test_existing_client_keeps_matching_backend_after_registration_replace(warm_rpc):
    """替换注册表只影响新客户端，旧客户端的 RPC、推送必须属于同一代。"""
    register_backend(tagged_backend())
    client = BigQmtRpcClient(account_id="acct", transport="review_test")
    if warm_rpc:
        assert client.call("ping", use_formula=False) == "old"
    register_backend(tagged_backend(tag="new"), replace=True)
    push = _build_client_push_channel(client)
    try:
        assert client.call("ping", use_formula=False) == "old"
        assert push.tag == "old"
        fresh = BigQmtRpcClient(account_id="acct", transport="review_test")
        assert fresh.call("ping", use_formula=False) == "new"
        assert _build_client_push_channel(fresh).tag == "new"
    finally:
        push.stop()


def test_existing_trader_keeps_backend_event_capability_after_replace():
    """已创建交易客户端不能因为全局替换而突然改变回报监听方式。"""
    register_backend(tagged_backend())
    trader = BigQmtXtTrader(account_id="acct", redis_config={"transport": "review_test"})
    register_backend(tagged_backend(tag="new", execution_events_via_push=False), replace=True)
    try:
        assert trader._exec_transport_uses_push() is True
    finally:
        trader.stop()


@pytest.mark.parametrize("name", ["redis", "default"])
def test_redis_alias_uses_registered_rpc_and_push_together(name, monkeypatch):
    """Redis 及其默认别名也必须使用统一注册工厂，避免 RPC 绕开抽象。"""
    register_backend(tagged_backend("redis", "registered"), replace=True)
    monkeypatch.setattr(xtquant_compat, "call_redis_rpc", Mock(side_effect=AssertionError("legacy RPC bypass")))
    client = BigQmtRpcClient(account_id="acct", transport=name)
    client._redis = Mock(side_effect=AssertionError("unexpected Redis connection"))
    assert client.call("ping", use_formula=False) == "registered"
    assert _build_client_push_channel(client).tag == "registered"
    client._redis.assert_not_called()


def test_custom_push_backend_does_not_probe_unconfigured_redis():
    """独立推送后端缺省不探测 Redis，避免连上无关实例而漏收正式回报。"""
    register_backend(tagged_backend())
    trader = BigQmtXtTrader(account_id="acct", redis_config={"transport": "review_test"})
    trader.client._redis = Mock(side_effect=AssertionError("unexpected Redis probe"))
    try:
        assert trader._exec_events_redis_or_none() is None
        trader.client._redis.assert_not_called()
    finally:
        trader.stop()


@pytest.mark.parametrize("redis_enabled", [False, "false", 0])
def test_disabled_redis_is_not_probed_by_execution_listener(redis_enabled):
    """配置接受的布尔格式必须保持一致，禁用不能只识别 False 单例。"""
    register_backend(tagged_backend())
    trader = BigQmtXtTrader(account_id="acct", redis_config={
        "transport": "review_test", "redis_enabled": redis_enabled,
    })
    trader.client._redis = Mock(side_effect=AssertionError("unexpected Redis probe"))
    try:
        assert trader._exec_events_redis_or_none() is None
        trader.client._redis.assert_not_called()
    finally:
        trader.stop()


def test_disabled_redis_discovery_never_builds_or_calls_redis(monkeypatch):
    """显式禁用 Redis 优先于残留发现参数及 Redis 连接配置。"""
    from bigqmt_signal_trader.adapters import redis_common
    from bigqmt_signal_trader.transports.zmq_transport import ZmqTransport

    create_redis = Mock(side_effect=AssertionError("unexpected Redis discovery"))
    factory_redis = Mock(side_effect=AssertionError("unexpected client Redis discovery"))
    monkeypatch.setattr(redis_common, "build_redis_client", create_redis)
    monkeypatch.setattr(ZmqTransport, "from_config", Mock(side_effect=lambda config, **kw: PollingTaggedRpc("zmq", config)))
    transport = build_transport("zmq", {
        "redis_enabled": False,
        "redis": {"host": "unused.invalid"},
        "redis_client_factory": factory_redis,
        "zmq": {"redis_discovery_enabled": True},
    }, account_id="acct")
    assert transport.config.get("discovery_redis_client") is None
    create_redis.assert_not_called()
    factory_redis.assert_not_called()


def test_server_push_factory_preserves_explicit_config_dependency():
    """省略旧参数时不能用 None 覆盖通用配置里已经注入的 Redis 对象。"""
    register_backend(tagged_backend())
    dependency = object()
    manager, channel = build_quote_subscription_service(
        object(), "review_test", "acct", rpc_config={"redis_client": dependency},
    )
    try:
        assert channel.config["redis_client"] is dependency
    finally:
        channel.stop()


def test_factory_configuration_isolated_without_copying_dependency_objects():
    """工厂可调整自己的参数副本，但不能污染调用方或另一类通道的参数。"""
    dependency = object()
    original = {"backend_config": {"options": {"values": [1]}, "dependency": dependency}}

    def rpc_factory(config, account_id, print_prefix):
        """模拟工厂解析参数时添加默认值。"""
        config["options"]["values"].append(2)
        return TaggedRpc("rpc", config)

    def push_factory(role, config, account_id, print_prefix):
        """保存推送工厂看到的独立参数。"""
        return TaggedPush("push", config)

    backend = CommunicationBackend("config_isolation_test", rpc_factory, push_factory)
    rpc = backend.build_rpc_transport(original)
    push = backend.build_push_channel("client", original)
    assert original["backend_config"]["options"]["values"] == [1]
    assert rpc.config["options"]["values"] == [1, 2]
    assert push.config["options"]["values"] == [1]
    assert rpc.config["dependency"] is dependency
    assert push.config["dependency"] is dependency


def test_client_keeps_one_configuration_snapshot_for_rpc_and_push(monkeypatch):
    """首次惰性 RPC 不能重新加载配置，导致它与推送采用不同参数版本。"""
    register_backend(tagged_backend())
    loader = Mock(return_value={"redis_config": {"transport": "review_test"}})
    monkeypatch.setattr(xtquant_compat, "load_client_config", loader)
    client = BigQmtRpcClient(account_id="acct")
    loader.return_value = {"redis_config": {
        "transport": "review_test", "new_parameter": "changed-after-construction",
    }}
    rpc = client._transport()
    push = _build_client_push_channel(client)
    assert "new_parameter" not in rpc.config
    assert "new_parameter" not in push.config
    assert loader.call_count == 1


def test_client_configuration_copies_containers_and_preserves_injected_objects():
    """调用方后续调整配置容器不能改变现有客户端，注入依赖仍由原对象承担。"""
    register_backend(tagged_backend())
    dependency = object()
    config = {"transport": "review_test", "backend_config": {
        "options": {"values": [1]}, "dependency": dependency,
    }}
    client = BigQmtRpcClient(account_id="acct", redis_config=config)
    config["backend_config"]["options"]["values"].append(2)
    rpc = client._transport()
    push = _build_client_push_channel(client)
    assert rpc.config["options"]["values"] == [1]
    assert push.config["options"]["values"] == [1]
    assert rpc.config["dependency"] is dependency
    assert push.config["dependency"] is dependency


def test_concurrent_first_rpc_initialization_creates_one_owned_transport():
    """首次并发请求只能创建一个可回收的 RPC 实例，避免连接泄漏。"""
    entered = threading.Event()
    release = threading.Event()
    start = threading.Barrier(3)
    created = []

    def rpc_factory(config, account_id, print_prefix):
        """暂缓连接创建，让两个调用同时进入首次初始化。"""
        instance = TaggedRpc("concurrent", config)
        created.append(instance)
        entered.set()
        assert release.wait(2.0)
        return instance

    register_backend(CommunicationBackend("concurrent_test", rpc_factory))
    client = BigQmtRpcClient(account_id="acct", transport="concurrent_test")

    def initialize():
        """同步发起惰性初始化，不模拟网络延迟。"""
        start.wait(timeout=2.0)
        return client._transport()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(initialize) for _ in range(2)]
        start.wait(timeout=2.0)
        try:
            assert entered.wait(2.0)
            # 短暂保持首次创建未结束；旧代码第二线程会重复创建同一连接。
            release.wait(0.1)
        finally:
            release.set()
        first, second = [future.result(timeout=2.0) for future in futures]
    assert len(created) == 1
    assert first is second
    assert client._transport() is first


def test_failed_rpc_factory_can_retry_without_caching_a_broken_instance():
    """创建失败保留原错误，后续重试仍可建立正常实例。"""
    expected = TaggedRpc("recovered", {})
    factory = Mock(side_effect=[RuntimeError("factory failed"), expected])
    register_backend(CommunicationBackend("retry_test", factory))
    client = BigQmtRpcClient(account_id="acct", transport="retry_test")
    with pytest.raises(RuntimeError, match="factory failed"):
        client._transport()
    assert client._transport() is expected
    assert factory.call_count == 2


def test_execution_subscriber_start_failure_closes_partially_started_channel(monkeypatch):
    """订阅启动过程中异常仍需回收通道资源，不等待用户显式停止。"""
    register_backend(tagged_backend())
    trader = BigQmtXtTrader(account_id="acct", redis_config={"transport": "review_test"})
    channel = TaggedPush("failed", {})
    channel.start_subscriber = Mock(side_effect=RuntimeError("subscribe failed"))
    trader._build_quote_push_channel = Mock(return_value=channel)
    monkeypatch.setattr(xtquant_compat.time, "sleep", lambda seconds: None)
    try:
        trader._event_loop_push_channel()
        assert channel.stop_count == 1
    finally:
        trader.stop()


def test_base_rpc_receiving_signature_matches_formal_service_contract():
    """基类默认接收入口必须接受正式服务传入的线程模式参数。"""
    transport = TaggedRpc("base_contract", {}, account_id="acct")
    service = RedisPubSubRpcService(None, Mock(), "acct", transport=transport)
    try:
        service.start()
        assert transport._running is True
        assert transport._on_request == service.enqueue_payload
    finally:
        service.stop()
    assert transport.stop_count == 1


def test_polling_capability_requires_an_actual_drain_implementation():
    """轮询能力声明必须能执行，不能落到无关 Redis 路径或等到盘中才失败。"""
    transport = TaggedRpc("missing_drain", {})
    backend = CommunicationBackend("missing_drain_test", Mock(return_value=transport),
                                   supports_scheduler_polling=True)
    with pytest.raises(TransportError, match="poll|drain"):
        backend.build_rpc_transport()
    assert transport.stop_count == 1


@pytest.mark.parametrize("failure_phase", ["build", "rpc", "push"])
def test_strategy_start_failure_releases_both_channels_and_allows_retry(monkeypatch, failure_phase):
    """成套启动失败不能保留半运行状态，释放两个通道后下一次可重新建立。"""
    created = []
    monkeypatch.setattr(strategy, "_rpc_service", None)
    monkeypatch.setattr(strategy, "_quote_subscription_service", None)
    monkeypatch.setattr(strategy, "_log_err", Mock())

    def build_service(context_info, app, config):
        """每次建立独立通道，只在首次启动注入指定阶段错误。"""
        service = Mock()
        channel = Mock()
        manager = Mock(_combos={}, _sub_index={})
        if not created and failure_phase != "build":
            target = service.start if failure_phase == "rpc" else channel.start_publisher
            target.side_effect = RuntimeError("%s start failed" % failure_phase)
        created.append((service, manager, channel))
        strategy._quote_subscription_service = (manager, channel)
        if len(created) == 1 and failure_phase == "build":
            # 推送工厂已成功、RPC 组装随后失败时，入口也要清理已取得的推送资源。
            raise RuntimeError("build start failed")
        return service

    monkeypatch.setattr(strategy, "_build_rpc_service", build_service)
    config = {"rpc": {"transport": "zmq"}}
    with pytest.raises(RuntimeError, match="start failed"):
        strategy._start_rpc_service(object(), object(), config)
    service, manager, channel = created[0]
    if failure_phase != "build":
        service.stop.assert_called_once()
    else:
        service.start.assert_not_called()
    channel.stop.assert_called_once()
    assert strategy._rpc_service is None
    assert strategy._quote_subscription_service is None
    try:
        retried = strategy._start_rpc_service(object(), object(), config)
        assert len(created) == 2
        assert retried is created[1][0]
        assert strategy._rpc_service is retried
        created[1][0].start.assert_called_once()
        created[1][2].start_publisher.assert_called_once()
    finally:
        for owned_service, _, owned_channel in created[1:]:
            owned_service.stop()
            owned_channel.stop()


@pytest.mark.parametrize("kind", ["rpc", "push"])
def test_backend_factory_none_is_rejected_without_a_transport_fallback(kind):
    """工厂错误返回 None 必须在边界报错，不能让 SDK 悄悄回到 Redis。"""
    backend = CommunicationBackend("none_factory_test", lambda config, **kw: None,
                                   lambda **kw: None)
    with pytest.raises(TransportError, match="factory|None|transport|channel"):
        if kind == "rpc":
            backend.build_rpc_transport()
        else:
            backend.build_push_channel("client")


@pytest.mark.parametrize("name,stale_redis", [
    ("review_server", False), ("redis", False),
    ("review_server", True), ("redis", True),
])
def test_formal_server_assembly_uses_one_fixed_backend_without_hidden_redis(monkeypatch, name, stale_redis):
    """正式 QMT 组装入口固定一组工厂，Redis 名称替换也不能绕开或另建 Redis。"""
    import importlib
    from bigqmt_signal_trader.adapters import redis_common
    from bigqmt_signal_trader import multi_account

    monkeypatch.setattr(importlib, "reload", lambda module: module)
    monkeypatch.setattr(strategy, "_load_bridge_module", None)
    monkeypatch.setattr(strategy, "_rpc_service", None)
    monkeypatch.setattr(strategy, "_quote_subscription_service", None)
    monkeypatch.setattr(strategy, "_exec_event_redis_client", None)
    monkeypatch.setattr(multi_account, "build_multi_account_rpc_service",
                        lambda context, app, config, builder: builder(context, app, config))
    create_redis = Mock(side_effect=AssertionError("unexpected server Redis"))
    monkeypatch.setattr(redis_common, "build_redis_client", create_redis)

    def rpc_factory(config, account_id, print_prefix):
        """组装中替换注册表，检验后续推送与能力仍使用入口固定的定义。"""
        register_backend(tagged_backend(name, "new"), replace=True)
        return TaggedRpc("old", config, account_id, print_prefix)

    def push_factory(role, config, account_id, print_prefix):
        """仅创建本次组装选中的旧代推送对象。"""
        return TaggedPush("old", config)

    selected = CommunicationBackend(name, rpc_factory, push_factory,
                                    execution_events_via_push=True)
    register_backend(selected, replace=name == "redis")
    config = {"enable_rpc": True, "account_id": "acct", "rpc": {"transport": name}}
    if stale_redis:
        config["redis"] = {"host": "unused.invalid"}
        config["rpc"]["redis_enabled"] = False
    service = strategy._start_rpc_service(object(), SimpleNamespace(), config)
    try:
        assert service.communication_backend is selected
        assert service._transport.tag == "old"
        assert strategy._quote_subscription_service[1].tag == "old"
        assert service.handlers.download_job_redis_client is None
        assert service.handlers.order_identity_redis_client is None
        create_redis.assert_not_called()
        response = service.process_request({
            "account_id": "acct", "request_id": "review-ping", "method": "ping",
        })
        assert response["ok"] is True
        assert response["data"]["allow_order_methods"] is False
    finally:
        service.stop()
        if strategy._quote_subscription_service is not None:
            strategy._quote_subscription_service[1].stop()


def test_formal_server_assembly_failure_releases_factory_created_transport(monkeypatch):
    """RPC 实例已创建但处理器组装失败时，正式入口回收它并支持重新组装。"""
    import importlib
    from bigqmt_signal_trader import multi_account, redis_rpc

    monkeypatch.setattr(importlib, "reload", lambda module: module)
    monkeypatch.setattr(strategy, "_load_bridge_module", None)
    monkeypatch.setattr(strategy, "_rpc_service", None)
    monkeypatch.setattr(strategy, "_quote_subscription_service", None)
    monkeypatch.setattr(strategy, "_exec_event_redis_client", None)
    monkeypatch.setattr(multi_account, "build_multi_account_rpc_service",
                        lambda context, app, config, builder: builder(context, app, config))
    rpc_instances = []
    push_instances = []
    original_handlers = redis_rpc.BigQmtRpcHandlers
    calls = []

    def rpc_factory(config, account_id, print_prefix):
        """记录入口在构造处理器之前取得的 RPC 资源。"""
        transport = TaggedRpc("partial", config, account_id, print_prefix)
        rpc_instances.append(transport)
        return transport

    def push_factory(role, config, account_id, print_prefix):
        """记录已取得但尚未启动的推送资源。"""
        channel = TaggedPush("partial", config)
        push_instances.append(channel)
        return channel

    def handlers_factory(*args, **kwargs):
        """第一次模拟处理器组装异常，第二次沿用真实业务处理器。"""
        if not calls:
            calls.append("failed")
            raise RuntimeError("handler assembly failed")
        return original_handlers(*args, **kwargs)

    monkeypatch.setattr(redis_rpc, "BigQmtRpcHandlers", handlers_factory)
    register_backend(CommunicationBackend("server_partial_test", rpc_factory, push_factory))
    config = {"enable_rpc": True, "account_id": "acct", "rpc": {"transport": "server_partial_test"}}
    with pytest.raises(RuntimeError, match="handler assembly failed"):
        strategy._start_rpc_service(object(), SimpleNamespace(), config)
    assert rpc_instances[0].stop_count == 1
    assert push_instances[0].stop_count == 1
    assert strategy._rpc_service is None
    assert strategy._quote_subscription_service is None
    try:
        retried = strategy._start_rpc_service(object(), SimpleNamespace(), config)
        assert len(rpc_instances) == 2
        assert retried._transport is rpc_instances[1]
        assert strategy._quote_subscription_service[1] is push_instances[1]
    finally:
        if strategy._rpc_service is not None:
            strategy._rpc_service.stop()
        if strategy._quote_subscription_service is not None:
            strategy._quote_subscription_service[1].stop()


def test_mysql_event_dependency_does_not_enable_stale_position_fallback():
    """MySQL 沿用 Redis 推送不能顺带开启旧资产缓存，查询失败仍向调用方报错。"""
    trader = BigQmtXtTrader(account_id="acct", redis_config={"transport": "mysql"})
    trader.client.call = Mock(side_effect=RuntimeError("backend offline"))
    redis_client = Mock()
    redis_client.get.return_value = (
        b'{"positions":{"510300.SH":{"stock_code":"510300.SH","volume":100}}}'
    )
    trader.client._redis = Mock(return_value=redis_client)
    try:
        with pytest.raises(RuntimeError, match="backend offline"):
            trader.query_stock_positions(SimpleNamespace(account_id="acct"))
        trader.client._redis.assert_not_called()
    finally:
        trader.stop()
