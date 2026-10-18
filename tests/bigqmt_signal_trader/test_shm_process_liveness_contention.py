"""真实跨进程共享推送中，慢 OS 存活查询不能占住发布和健康门禁的元数据锁。"""

import contextlib
import multiprocessing
import os
import threading
import time
import uuid
from types import SimpleNamespace

import pytest

from bigqmt_signal_trader import shm_push_channel, xtquant_compat
from bigqmt_signal_trader.quote_subscription_manager import BufferedAcknowledgedPublisher
from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService
from bigqmt_signal_trader.shm_push_channel import SharedMemoryQuotePushChannel
from bigqmt_signal_trader.transports.shm_primitives import cleanup
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport
from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader, RpcServerRepliedError
from bigqmt_signal_trader_strategy import _install_acknowledged_push_gate


_ACCOUNT = "isolated-liveness-regression"
_TOPIC = "exec:liveness-regression"


def _serve_real_channels(settings, ready, stop):
    """子进程运行真实推送、SHM RPC 与正式门禁，仅原生业务结果使用隔离计数器。"""
    raw = SharedMemoryQuotePushChannel("server", settings, _ACCOUNT)
    publisher = BufferedAcknowledgedPublisher(raw, {"push_background_sender": False})
    transport = SharedMemoryTransport(account_id=_ACCOUNT, **settings)
    executed = []

    def handle(method, params=None):
        """只在门禁通过后记录隔离委托；发布操作必须实际进入共享日志。"""
        if method == "ping":
            return {"pong": True}
        if method == "probe_and_publish":
            raw.check_health()
            publisher.publish(_TOPIC, params)
            committed = publisher.flush()
            return {"committed": committed, "publisher": publisher.status()}
        if method == "submit_order":
            executed.append(dict(params or {}))
            return {"order_id": len(executed)}
        if method == "snapshot":
            return {"publisher": publisher.status(), "executed": executed}
        raise ValueError("unknown isolated operation: " + method)

    handlers = SimpleNamespace(handle=handle, _canonical_method=lambda method: method)
    _install_acknowledged_push_gate(handlers, publisher)
    service = RedisPubSubRpcService(
        None, handlers, account_id=_ACCOUNT, transport=transport,
        process_in_listener=True, background_threads=True,
    )
    try:
        publisher.start_publisher()
        service.start()
        ready.send({"pid": os.getpid()})
        # 交易与测试控制方法仍走正式待处理队列，按策略主线程模式主动消费。
        while not stop.wait(0.001):
            service.drain_pending()
    finally:
        names = {raw._name, raw._space_name}
        names.update(raw._signal_name(identity) for identity in raw._signals)
        service.stop()
        publisher.stop()
        names.update([transport._name, transport._name + "_request", transport._name + "_space"])
        names.update(transport._name + "_reply_%d" % index for index in range(settings["rpc_slots"]))
        for name in names:
            cleanup(name, settings["directory"])
        ready.close()


@pytest.fixture
def real_liveness_chain(tmp_path, monkeypatch):
    """使用独立原生进程，避免同进程身份的存活快捷校验掩盖真正跨 PID 路径。"""
    monkeypatch.setattr(xtquant_compat, "load_client_config", lambda: {})
    settings = dict(
        directory=str(tmp_path), namespace=uuid.uuid4().hex,
        rpc_slots=4, rpc_slot_bytes=8192, wait_poll_seconds=0.002,
        market_push_bytes=65536, trade_push_bytes=65536,
        market_push_slots=16, trade_push_slots=16, max_push_subscribers=2,
        push_poll_seconds=0.002,
    )
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    stop = context.Event()
    process = context.Process(target=_serve_real_channels, args=(settings, send, stop))
    process.start()
    send.close()
    assert receive.poll(5), "isolated publisher failed to start"
    assert receive.recv()["pid"] != os.getpid()
    trader = BigQmtXtTrader(
        account_id=_ACCOUNT, redis_config={"transport": "shm", "shm": settings},
    )
    client = SharedMemoryQuotePushChannel("client", settings, _ACCOUNT)
    try:
        yield trader, client
    finally:
        client.stop()
        trader.stop()
        if trader.client._transport_instance is not None:
            trader.client._transport_instance.stop()
        stop.set()
        process.join(5)
        if process.is_alive():
            process.terminate()
            process.join(2)
        receive.close()
        cleanup(client._signal_name(client._identity), settings["directory"])
        assert process.exitcode == 0


@contextlib.contextmanager
def _slow_consumer_peer_check(monkeypatch):
    """只将消费线程的一次真实跨进程 OS 探活延迟 150ms，不改锁或健康超时。"""
    original = shm_push_channel.process_alive
    entered, release = threading.Event(), threading.Event()
    calls = []

    def delayed(pid, birth):
        """延迟仅注入原 OS 调用边界，其前后仍运行正式共享头与所有权校验。"""
        selected = (
            threading.current_thread().name == "bigqmt-shm-push"
            and pid != os.getpid() and not entered.is_set()
        )
        if selected:
            entered.set()
            started = time.monotonic()
            assert release.wait(3), "controlled OS check was not released"
            calls.append(time.monotonic() - started)
        return original(pid, birth)

    monkeypatch.setattr(shm_push_channel, "process_alive", delayed)
    assert entered.wait(2), "consumer never checked the independent publisher"
    timer = threading.Timer(0.15, release.set)
    timer.start()
    try:
        yield release
    finally:
        # 保留完整 150ms 注入区间；不能在断言返回后立即释放而让测试退化成无争锁。
        timer.join(1)
        release.set()
        assert timer.is_alive() is False
        deadline = time.monotonic() + 1
        while not calls and time.monotonic() < deadline:
            time.sleep(0.001)
        assert calls and calls[0] >= 0.15


def _wait(predicate):
    """等待真实 ACK 或回调故障的可见状态；超时只失败，不自动重放业务。"""
    deadline = time.monotonic() + 2
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.002)


def test_slow_peer_probe_releases_real_metadata_lock_for_health_and_publish(
    real_liveness_chain, monkeypatch,
):
    """消费者探活暂停时，另一个进程能真实检查健康并提交完整事件，随后精确消费。"""
    trader, client = real_liveness_chain
    received = []
    client.start_subscriber([_TOPIC], lambda topic, data: received.append((topic, data)))
    payload = {"order_id": "exact-1", "price": 4.123456789012345, "quantity": 100}
    with _slow_consumer_peer_check(monkeypatch) as released:
        result = trader.client.call("probe_and_publish", payload)
        assert result["committed"] == 1
        assert result["publisher"]["shared_committed"] == 1
        assert result["publisher"]["healthy"] is True
        assert not released.is_set(), "publish only completed after the slow probe ended"
        assert received == []
    _wait(lambda: client.status()["acknowledged"] == 1)
    assert received == [(_TOPIC, payload)]


def test_actual_sdk_ping_does_not_cache_false_health_during_slow_peer_probe(
    real_liveness_chain, monkeypatch,
):
    """保留正式 100ms 健康核验及 SDK 1 秒 RPC；150ms 探活不得误关 SDK 交易门禁。"""
    trader, client = real_liveness_chain
    client.start_subscriber([_TOPIC], lambda topic, data: None)
    with _slow_consumer_peer_check(monkeypatch) as released:
        trader._probe_communication_health()
        health = trader.communication_health
        assert health["healthy"] is True, health
        assert health["new_orders_blocked"] is False, health
        assert not released.is_set(), "health only recovered after the slow probe ended"
        # 继续穿过同一条真实 SHM RPC 和正式新委托门禁，计数器仅代表隔离原生业务。
        assert trader.client.call("submit_order", {"remark": "one"}) == {"order_id": 1}
    assert trader.client.call("snapshot")["executed"] == [{"remark": "one"}]


def test_real_callback_failure_still_blocks_sdk_and_order_execution(real_liveness_chain):
    """缩短锁范围不吞掉真实回调失败；失败记录未确认且 SDK 与原生门禁均拒绝。"""
    trader, client = real_liveness_chain

    def fail(_topic, _data):
        """真实消费回调报错，触发生产路径写入持久失败标记。"""
        raise RuntimeError("isolated callback side effect uncertain")

    client.start_subscriber([_TOPIC], fail)
    assert trader.client.call("probe_and_publish", {"order_id": "failed"})["committed"] == 1
    _wait(lambda: client.status()["callback_failed"])
    trader._probe_communication_health()
    state = trader.communication_health
    assert state["healthy"] is False
    assert state["new_orders_blocked"] is True
    with pytest.raises(RpcServerRepliedError, match="callback failed"):
        trader.client.call("submit_order", {"remark": "must-not-execute"})
    assert trader.client.call("snapshot")["executed"] == []
    assert client.status()["acknowledged"] == 0
