"""Transport factory: pick a transport backend by name.

``build_transport(name, config, ...)`` returns a ready transport instance.
``name`` is the ``rpc.transport`` config value (default ``"redis"``). Unknown
names raise :class:`ValueError`. Optional dependencies (``zmq``, a mysql
driver) are imported lazily; a missing dependency surfaces as a clear
``ImportError`` only when that transport is actually selected.
"""

from .redis_transport import RedisTransport


KNOWN_TRANSPORTS = ("redis", "zmq", "mysql", "shm")


def build_transport(name, config=None, account_id="", print_prefix="[bigqmt_rpc]"):
    """按通信后端创建 RPC 通道；config 保持现有参数及依赖注入语义。"""
    from .backend import get_backend

    return get_backend(name).build_rpc_transport(
        config, account_id=account_id, print_prefix=print_prefix,
    )


def _build_redis(config, account_id, print_prefix):
    """创建 Redis RPC，保留客户端惰性注入和服务端有限时长的接收连接。"""
    from ..adapters.redis_common import build_redis_client

    server_side = bool(config.get("server_side", False))
    redis_config = dict(config.get("redis") or {})
    redis_client = config.get("redis_client")
    if redis_client is None:
        client_factory = config.get("redis_client_factory")
        if client_factory is not None:
            redis_client = client_factory()
        else:
            listen_config = dict(redis_config)
            # 策略回调也会使用此连接；服务端不允许无界 socket 等待阻塞 QMT。
            if server_side and listen_config.get("socket_timeout") in (None, ""):
                listen_config["socket_timeout"] = 10
            redis_client = build_redis_client(listen_config)
    response_redis_client = config.get("response_redis_client")
    if response_redis_client is None:
        response_redis_client = build_redis_client(redis_config) if server_side else redis_client
    return RedisTransport(
        redis_client,
        account_id=account_id,
        response_redis_client=response_redis_client,
        request_channel_template=config.get(
            "request_channel_template", "bigqmt:rpc:req:{account_id}"
        ),
        request_queue_template=config.get(
            "request_queue_template", "bigqmt:rpc:queue:{account_id}"
        ),
        response_channel_template=config.get(
            "response_channel_template", "bigqmt:rpc:resp:{account_id}:{request_id}"
        ),
        response_list_template=config.get(
            "response_list_template", "bigqmt:rpc:respq:{account_id}:{request_id}"
        ),
        response_key_template=config.get(
            "response_key_template", "bigqmt:rpc:resp:{account_id}:{request_id}"
        ),
        response_ttl_seconds=int(config.get("response_ttl_seconds", 60)),
        queue_poll_interval_seconds=float(config.get("queue_poll_interval_seconds", 0.02)),
        debug_log_limit=int(config.get("debug_log_limit", 0)),
        print_prefix=print_prefix,
    )


def _build_zmq(config, account_id, print_prefix):
    """创建 ZMQ RPC；仅按显式发现配置调用客户端 Redis 工厂。"""
    from .backend import config_bool
    from .zmq_transport import ZmqTransport

    zmq_config = dict(config.get("zmq") or {})
    if not config_bool(config.get("redis_enabled"), True):
        # 显式禁用优先于旧配置遗留的发现对象、开关和 Redis 地址。
        zmq_config["redis_discovery_enabled"] = False
        zmq_config.pop("discovery_redis_client", None)
        zmq_config.pop("redis", None)
        return ZmqTransport.from_config(zmq_config, account_id=account_id, print_prefix=print_prefix)
    # Wire up service discovery: if the caller injected a redis_client (server
    # side) or provided redis connection settings, the ZMQ transport can
    # publish/look up the actual bound port when the default port is taken.
    discovery_client = zmq_config.get("discovery_redis_client")
    client_factory = config.get("redis_client_factory")
    if (
        discovery_client is None and client_factory is not None
        and not zmq_config.get("connect_address")
        and bool(zmq_config.get("redis_discovery_enabled", False))
    ):
        discovery_client = client_factory()
        zmq_config["discovery_redis_client"] = discovery_client
    if discovery_client is None and config.get("redis_client") is not None:
        discovery_client = config.get("redis_client")
        zmq_config["discovery_redis_client"] = discovery_client
    if discovery_client is None and config.get("redis"):
        # Build a small client just for discovery from the redis config block.
        try:
            from ..adapters.redis_common import build_redis_client

            discovery_client = build_redis_client(dict(config.get("redis") or {}))
            zmq_config["discovery_redis_client"] = discovery_client
        except Exception:
            pass
    return ZmqTransport.from_config(
        zmq_config,
        account_id=account_id,
        print_prefix=print_prefix,
    )


def _build_mysql(config, account_id, print_prefix):
    from .mysql_transport import MysqlTransport

    return MysqlTransport.from_config(
        config.get("mysql") or {},
        account_id=account_id,
        print_prefix=print_prefix,
    )


def _build_shm(config, account_id, print_prefix):
    """创建共享内存 RPC；映射和同步对象在首次使用时才建立。"""
    from .shm_transport import SharedMemoryTransport

    return SharedMemoryTransport(
        account_id=account_id,
        print_prefix=print_prefix,
        **dict(config.get("shm") or {})
    )
