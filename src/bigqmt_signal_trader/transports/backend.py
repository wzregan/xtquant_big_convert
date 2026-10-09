"""统一创建 RPC 和事件推送通道，业务调用与具体通信方式解耦。"""

from .base import RpcTransport, TransportError
from .factory import _build_mysql, _build_redis, _build_shm, _build_zmq


class CommunicationBackend(object):
    """一个后端的通道工厂及调度能力，本身不持有连接。"""

    def __init__(self, name, rpc_factory, push_factory=None,
                 supports_scheduler_polling=False, default_background_threads=True,
                 execution_events_via_push=False, redis_auxiliary_default=False,
                 push_depends_on_redis=False):
        """定义后端；创建出的通道由调用方启动和关闭。

        rpc_factory 接收 config、account_id、print_prefix，返回 RpcTransport。
        push_factory 额外接收 role（client 或 server），返回 QuotePushChannel。
        supports_scheduler_polling 表示支持从 QMT 回调非阻塞收取 RPC 请求。
        execution_events_via_push 表示可通过推送通道接收交易回报。
        redis_auxiliary_default 保留原有 Redis 日志和缓存的缺省启用行为。
        push_depends_on_redis 表示推送也依赖 Redis，故障时不能作为独立回退通道。
        """
        self.name = str(name).strip().lower()
        if not self.name or self.name == "default":
            raise ValueError("communication backend requires a non-default name")
        if not callable(rpc_factory):
            raise TypeError("rpc_factory must be callable")
        if push_factory is not None and not callable(push_factory):
            raise TypeError("push_factory must be callable")
        if execution_events_via_push and push_factory is None:
            raise ValueError("event push requires a push_factory")
        if not default_background_threads and not supports_scheduler_polling:
            raise ValueError("backend without scheduler polling requires background threads")
        self._rpc_factory = rpc_factory
        self._push_factory = push_factory
        self.supports_scheduler_polling = bool(supports_scheduler_polling)
        self.default_background_threads = bool(default_background_threads)
        self.execution_events_via_push = bool(execution_events_via_push)
        self.redis_auxiliary_default = bool(redis_auxiliary_default)
        self.push_depends_on_redis = bool(push_depends_on_redis)

    def build_rpc_transport(self, config=None, account_id="", print_prefix="[bigqmt_rpc]"):
        """创建 RPC 通道；config 为后端参数及依赖对象，调用方负责生命周期。"""
        transport = self._rpc_factory(_backend_config(config), account_id=account_id,
                                      print_prefix=print_prefix)
        if transport is None:
            raise TransportError("backend %r RPC factory returned None" % self.name)
        try:
            if self.supports_scheduler_polling:
                drain = getattr(transport, "drain_request_queue", None)
                if not callable(drain) or getattr(drain, "__func__", None) is RpcTransport.drain_request_queue:
                    raise TransportError("backend %r declares scheduler polling but does not implement drain_request_queue"
                                         % self.name)
        except Exception:
            # 工厂可能已经分配资源，能力校验失败也必须交还给该实例清理。
            try:
                transport.stop()
            except Exception:
                pass
            raise
        return transport

    def build_push_channel(self, role, config=None, account_id="", print_prefix="[bigqmt_quote_push]"):
        """创建推送通道；role 为 client 或 server，调用方负责启动和关闭。"""
        if role not in ("client", "server"):
            raise ValueError("push role must be client or server")
        if self._push_factory is None:
            # 缺少推送实现必须报错，不能悄悄切换到另一种通信方式。
            raise TransportError("communication backend %r does not support event push" % self.name)
        channel = self._push_factory(role=role, config=_backend_config(config),
                                     account_id=account_id, print_prefix=print_prefix)
        if channel is None:
            raise TransportError("backend %r push factory returned None" % self.name)
        return channel


def copy_backend_config(config, memo=None):
    """递归复制配置容器，连接、锁及其它注入对象保持原身份和所有权。"""
    if memo is None:
        memo = {}
    identity = id(config)
    if identity in memo:
        return memo[identity]
    if isinstance(config, dict):
        result = {}
        memo[identity] = result
        result.update((key, copy_backend_config(value, memo)) for key, value in config.items())
        return result
    if isinstance(config, list):
        result = []
        memo[identity] = result
        result.extend(copy_backend_config(value, memo) for value in config)
        return result
    if isinstance(config, tuple):
        result = tuple(copy_backend_config(value, memo) for value in config)
        memo[identity] = result
        return result
    return config


def config_bool(value, default=False):
    """统一解析开关；支持配置文件常用的布尔值、数字和字符串。"""
    if value is None:
        return bool(default)
    if isinstance(value, str):
        return value.strip().lower() not in ("", "0", "false", "no", "off", "none")
    return bool(value)


def _backend_config(config):
    """隔离并展开通用后端参数；外层已有配置优先，避免改变历史语义。"""
    outer = copy_backend_config(dict(config or {}))
    result = dict(outer.pop("backend_config", None) or {})
    result.update(outer)
    return result


_BACKENDS = {}


def register_backend(backend, replace=False):
    """在启动前注册后端；replace 显式允许替换定义，不迁移运行中的连接。"""
    if not isinstance(backend, CommunicationBackend):
        raise TypeError("backend must be a CommunicationBackend")
    if backend.name in _BACKENDS and not replace:
        raise ValueError("communication backend %r is already registered" % backend.name)
    _BACKENDS[backend.name] = backend


def get_backend(name="redis"):
    """按配置名称获取后端，空名称和 default 沿用 Redis。"""
    normalized = str(name or "redis").strip().lower()
    if normalized == "default":
        normalized = "redis"
    try:
        return _BACKENDS[normalized]
    except KeyError:
        raise ValueError("unknown rpc transport %r (known: %s)"
                         % (normalized, ", ".join(sorted(_BACKENDS))))


def zmq_push_address(role, config=None, account_id=""):
    """推送地址优先显式配置，否则从实际 RPC 主机及端口加一派生。"""
    from .zmq_transport import DEFAULT_ZMQ_HOST, _default_zmq_port

    if role not in ("client", "server"):
        raise ValueError("push role must be client or server")
    settings = dict(_backend_config(config).get("zmq") or {})
    suffix = "bind_address" if role == "server" else "connect_address"
    explicit = settings.get("quote_push_" + suffix)
    if explicit:
        return str(explicit)
    rpc_address = settings.get(suffix)
    if rpc_address:
        # 不能只看 host/port，否则显式 RPC 地址会与推送地址指向不同主机。
        address = str(rpc_address)
        if not address.startswith("tcp://"):
            raise ValueError("non-TCP RPC requires an explicit quote_push_" + suffix)
        host_prefix, separator, port = address.rpartition(":")
        if not separator:
            raise ValueError("invalid RPC endpoint: %s" % address)
        base_port = int(port)
    else:
        host = settings.get("host") or ("0.0.0.0" if role == "server" else DEFAULT_ZMQ_HOST)
        base_port = int(settings.get("port") or _default_zmq_port(account_id))
        host_prefix = "tcp://%s" % host
    if not 0 < base_port < 65535:
        raise ValueError("RPC port must leave room for the push port")
    return "%s:%d" % (host_prefix, base_port + 1)


def _build_zmq_push(role, config, account_id, print_prefix):
    """创建 ZMQ 通道并保存端点，此时不创建套接字。"""
    from ..quote_push_channel import ZmqQuotePushChannel

    field = "bind_address" if role == "server" else "connect_address"
    return ZmqQuotePushChannel(print_prefix=print_prefix,
                              **{field: zmq_push_address(role, config, account_id)})


def _build_redis_push(role, config, account_id, print_prefix):
    """沿用 Redis 推送，只在真正选择此通道时调用客户端连接工厂。"""
    from ..quote_push_channel import RedisQuotePushChannel

    client = config.get("redis_client")
    if client is None and config.get("redis_client_factory") is not None:
        client = config["redis_client_factory"]()
    if client is None:
        from ..adapters.redis_common import build_redis_client

        # 与 Redis RPC 工厂使用同一默认配置，不能生成内部 client 为 None 的空通道。
        client = build_redis_client(dict(config.get("redis") or {}))
    return RedisQuotePushChannel(client, account_id=account_id, print_prefix=print_prefix)




# 注册时不建立连接；同一后端成套提供 RPC、推送及策略线程调度能力。
register_backend(CommunicationBackend("redis", _build_redis, _build_redis_push,
                                     supports_scheduler_polling=True,
                                     default_background_threads=False,
                                     execution_events_via_push=True,
                                     redis_auxiliary_default=True,
                                     push_depends_on_redis=True))
register_backend(CommunicationBackend("zmq", _build_zmq, _build_zmq_push,
                                     supports_scheduler_polling=True,
                                     execution_events_via_push=True))
# 保持历史 MySQL RPC 配合 Redis 推送的组合。
register_backend(CommunicationBackend("mysql", _build_mysql, _build_redis_push,
                                     supports_scheduler_polling=True,
                                     redis_auxiliary_default=True,
                                     push_depends_on_redis=True))
register_backend(CommunicationBackend("shm", _build_shm))
