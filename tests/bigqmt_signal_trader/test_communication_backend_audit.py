"""审计公共通信后端的配置隔离、工厂失败和共享内存资源所有权。"""

import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from bigqmt_signal_trader.transports import backend as b
from bigqmt_signal_trader.transports.base import RpcTransport, TransportError
from bigqmt_signal_trader.transports.shm_primitives import cleanup


@pytest.mark.parametrize("name,options,error,message", [
    ("", {}, ValueError, "non-default"),
    (" DEFAULT ", {}, ValueError, "non-default"),
    ("bad", {"rpc_factory": None}, TypeError, "rpc_factory"),
    ("bad", {"push_factory": 42}, TypeError, "push_factory"),
    ("bad", {"execution_events_via_push": True}, ValueError, "event push"),
    ("bad", {"acknowledged_push": True}, ValueError, "acknowledged push"),
    ("bad", {"default_background_threads": False}, ValueError, "scheduler polling"),
])
def test_backend_capabilities_reject_incoherent_definitions(name, options, error, message):
    """能力与工厂定义不一致应在注册前失败，不能延迟至交易期间。"""
    settings = {"rpc_factory": Mock()}
    settings.update(options)
    with pytest.raises(error, match=message):
        b.CommunicationBackend(name, **settings)


@pytest.mark.parametrize("role", ["", "CLIENT", "publisher", None])
def test_invalid_push_role_never_calls_factory(role):
    """角色拼写错误必须先拒绝，避免错误工厂偷偷创建连接。"""
    factory = Mock()
    backend = b.CommunicationBackend("audit-role", Mock(), factory)
    with pytest.raises(ValueError, match="push role"):
        backend.build_push_channel(role)
    factory.assert_not_called()
    with pytest.raises(ValueError, match="push role"):
        b.zmq_push_address(role)


@pytest.mark.parametrize("broken_stop", [False, True])
def test_failed_capability_check_releases_factory_product_and_preserves_error(broken_stop):
    """工厂已分配资源后发现轮询能力不实，清理失败也不能覆盖原能力错误。"""
    stop = Mock(side_effect=RuntimeError("cleanup failed") if broken_stop else None)
    transport = SimpleNamespace(drain_request_queue=None, stop=stop)
    backend = b.CommunicationBackend("bad-poll", Mock(return_value=transport),
                                    supports_scheduler_polling=True)
    with pytest.raises(TransportError, match="does not implement drain_request_queue"):
        backend.build_rpc_transport()
    stop.assert_called_once_with()


def test_factory_failure_has_no_cached_product_or_cross_channel_cleanup():
    """RPC 工厂抛错后可重试；后端不擅自关闭调用方持有的另一推送实例。"""
    rpc = RpcTransport("audit")
    push = SimpleNamespace(stop=Mock())
    rpc_factory = Mock(side_effect=[RuntimeError("RPC factory failed"), rpc])
    push_factory = Mock(return_value=push)
    backend = b.CommunicationBackend("independent", rpc_factory, push_factory)
    owned_push = backend.build_push_channel("server")
    with pytest.raises(RuntimeError, match="RPC factory failed"):
        backend.build_rpc_transport()
    assert backend.build_rpc_transport() is rpc
    assert owned_push is push
    push.stop.assert_not_called()
    rpc.stop()
    push.stop()


def test_container_copy_preserves_aliases_cycles_and_injected_resource_identity():
    """配置容器复制必须保留别名关系；连接和锁保持原身份，不能复制所有权。"""
    lock = threading.Lock()
    shared = [1]
    original = {"left": shared, "right": shared, "nested": (shared, lock)}
    original["self"] = original
    shared.append(shared)
    copied = b.copy_backend_config(original)
    assert copied is not original
    assert copied["self"] is copied
    assert copied["left"] is copied["right"] is copied["nested"][0]
    assert copied["left"][1] is copied["left"]
    assert copied["nested"][1] is lock
    copied["left"].append(2)
    assert len(original["left"]) == 2
    memo = {id(lock): lock}
    assert b.copy_backend_config(lock, memo) is lock


def test_factory_config_expansion_preserves_outer_override_and_independent_snapshots():
    """外层配置覆盖嵌套默认值，RPC 与推送工厂拥有各自容器副本。"""
    dependency = threading.Lock()
    original = {"backend_config": {"mode": "default", "items": [1]},
                "mode": "explicit", "dependency": dependency}
    captured = []

    def factory(config, **_kwargs):
        """保存且修改当前工厂配置，验证不影响下一次工厂调用。"""
        captured.append(config)
        config["items"].append(len(captured))
        return SimpleNamespace(stop=Mock())

    backend = b.CommunicationBackend("copy", factory, factory)
    backend.build_rpc_transport(original)
    backend.build_push_channel("client", original)
    assert original["backend_config"]["items"] == [1]
    assert captured[0]["items"] == [1, 1]
    assert captured[1]["items"] == [1, 2]
    assert all(config["mode"] == "explicit" and config["dependency"] is dependency for config in captured)


def test_registry_validation_does_not_replace_existing_backend(monkeypatch):
    """注册失败不改变原绑定；只有明确 replace 才替换未来创建的后端定义。"""
    monkeypatch.setattr(b, "_BACKENDS", {})
    first = b.CommunicationBackend("redis", Mock())
    second = b.CommunicationBackend("redis", Mock())
    with pytest.raises(TypeError, match="CommunicationBackend"):
        b.register_backend(object())
    b.register_backend(first)
    with pytest.raises(ValueError, match="already registered"):
        b.register_backend(second)
    assert b.get_backend("default") is first
    assert b.get_backend("") is first
    b.register_backend(second, replace=True)
    assert b.get_backend(" REDIS ") is second
    with pytest.raises(ValueError, match="unknown rpc transport"):
        b.get_backend("missing")


@pytest.mark.parametrize("role", ["client", "server"])
def test_zmq_push_default_and_explicit_endpoints_are_lazy(role):
    """旧 ZMQ 后端组装仅计算地址，不建立套接字或偷用其他后端。"""
    suffix = "connect_address" if role == "client" else "bind_address"
    channel = b._build_zmq_push(role, {"zmq": {suffix: "tcp://127.0.0.1:19000"}}, "audit", "[audit]")
    assert getattr(channel, suffix) == "tcp://127.0.0.1:19001"
    assert channel._context is None
    assert b.zmq_push_address(role, {"zmq": {"host": "127.0.0.2", "port": 19000}}) == "tcp://127.0.0.2:19001"
    assert b.zmq_push_address(role).startswith("tcp://")
    assert b.zmq_push_address(role, {"zmq": {"quote_push_" + suffix: "ipc://quote"}}) == "ipc://quote"
    channel.stop()


@pytest.mark.parametrize("mode", ["injected", "factory", "default", "empty-factory"])
def test_redis_push_respects_injected_dependency_without_duplicate_factory(monkeypatch, mode):
    """Redis 兼容工厂只在缺少注入资源时创建依赖，不重复拥有或关闭外部连接。"""
    from bigqmt_signal_trader import quote_push_channel
    from bigqmt_signal_trader.adapters import redis_common

    dependency = object()
    build_client = Mock(return_value=dependency)
    factory = Mock(return_value=dependency if mode == "factory" else None)
    channel_factory = Mock(return_value=object())
    monkeypatch.setattr(redis_common, "build_redis_client", build_client)
    monkeypatch.setattr(quote_push_channel, "RedisQuotePushChannel", channel_factory)
    config = {"redis": {"host": "offline.invalid"}}
    if mode == "injected":
        config.update(redis_client=dependency, redis_client_factory=factory)
    elif mode in ("factory", "empty-factory"):
        config["redis_client_factory"] = factory
    channel = b._build_redis_push("client", config, "audit", "[audit]")
    assert channel is channel_factory.return_value
    channel_factory.assert_called_once_with(dependency, account_id="audit", print_prefix="[audit]")
    assert factory.call_count == (1 if mode in ("factory", "empty-factory") else 0)
    assert build_client.call_count == (1 if mode in ("default", "empty-factory") else 0)


def test_shm_factory_pairs_are_lazy_isolated_and_owned_by_callers(tmp_path):
    """共享后端成套参数一致，关闭一个客户端不关闭发布者或另一个客户端。"""
    config = {"shm": {"namespace": str(tmp_path), "directory": str(tmp_path),
                      "rpc_slots": 2, "rpc_slot_bytes": 1024,
                      "market_push_bytes": 8192, "trade_push_bytes": 8192,
                      "market_push_slots": 8, "trade_push_slots": 8,
                      "max_push_subscribers": 4, "push_poll_seconds": 0.005}}
    backend = b.get_backend("shm")
    rpc = backend.build_rpc_transport(config, "audit")
    publisher = backend.build_push_channel("server", config, "audit")
    first = backend.build_push_channel("client", config, "audit")
    second = backend.build_push_channel("client", config, "audit")
    channels = [publisher, first, second]
    try:
        assert backend.acknowledged_push and backend.execution_events_via_push
        assert not backend.default_background_threads
        assert rpc.namespace == publisher.namespace == first.namespace == second.namespace
        assert rpc._region is None and all(channel._region is None for channel in channels)
        assert not list(tmp_path.iterdir())
        config["shm"]["market_push_bytes"] = 16384
        assert publisher.capacities[0] == 8192
        publisher.start_publisher()
        first_received = threading.Event()
        second_received = threading.Event()
        first.start_subscriber(["exec:audit"], lambda *_args: first_received.set())
        second.start_subscriber(["exec:audit"], lambda *_args: second_received.set())
        first.stop()
        assert publisher.publish("exec:audit", {"id": 1})
        assert second_received.wait(1)
        assert not first_received.is_set()
        assert publisher.check_health()["started"]
        assert publisher.status()["error"] == ""
    finally:
        names = set()
        for channel in reversed(channels):
            names.update([channel._name, channel._space_name, channel._signal_name(channel._identity)])
            names.update(channel._signal_name(identity) for identity in channel._signals)
            channel.stop()
        rpc.stop()
        for name in names:
            cleanup(name, str(tmp_path))


def test_tuple_list_cycle_keeps_the_same_copied_tuple_identity():
    """元组经列表回指自身时，复制后仍是同一个元组，不能拆成两个别名。"""
    values = []
    original = (values,)
    values.append(original)
    copied = b.copy_backend_config(original)
    assert copied is not original
    assert copied[0] is not values
    assert copied[0][0] is copied
    assert original[0][0] is original


@pytest.mark.parametrize("address", ["tcp://localhost", "tcp://:1234", "tcp://localhost:"])
def test_zmq_rpc_address_requires_real_host_and_port(address):
    """协议前缀中的冒号不能代替主机端口分隔符，空主机或空端口也必须拒绝。"""
    with pytest.raises(ValueError, match="invalid RPC endpoint"):
        b.zmq_push_address("client", {"zmq": {"connect_address": address}})
