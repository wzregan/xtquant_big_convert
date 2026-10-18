"""验证正式共享内存 ping 到 SDK 缓存的健康语义，不把采样锁忙缓存为业务故障。"""

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from bigqmt_signal_trader import xtquant_compat
from bigqmt_signal_trader.quote_subscription_manager import BufferedAcknowledgedPublisher
from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService
from bigqmt_signal_trader.shm_push_channel import PushCallbackError, PushLockBusyError
from bigqmt_signal_trader.transports.shm_primitives import cleanup
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport
from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader
from bigqmt_signal_trader_strategy import _install_acknowledged_push_gate


@pytest.fixture
def health_chain(tmp_path, monkeypatch):
    """只替代原生事实及推送介质，保留正式服务、SHM RPC、SDK 探测与缓存。"""
    monkeypatch.setattr(xtquant_compat, "load_client_config", lambda: {})
    settings = dict(namespace=uuid.uuid4().hex, directory=str(tmp_path),
                    rpc_slots=4, rpc_slot_bytes=4096, wait_poll_seconds=0.005)
    server = SharedMemoryTransport(account_id="health-audit", **settings)
    raw = Mock()
    raw.check_health.return_value = {"healthy": True}
    execute = Mock(return_value={"pong": True})
    handlers = SimpleNamespace(handle=execute, _canonical_method=lambda method: method)
    publisher = BufferedAcknowledgedPublisher(raw, {
        "local_exec_queue_items": 2, "local_queue_high_water": 0.5,
    })
    _install_acknowledged_push_gate(handlers, publisher)
    service = RedisPubSubRpcService(None, handlers, account_id="health-audit", transport=server,
                                   process_in_listener=True, background_threads=True)
    trader = BigQmtXtTrader(account_id="health-audit", redis_config={"transport": "shm", "shm": settings})
    service.start()
    try:
        yield trader, publisher, raw, execute, server
    finally:
        trader.stop()
        if trader.client._transport_instance is not None:
            trader.client._transport_instance.stop()
        publisher.stop()
        service.stop()
        names = [server._name, server._name + "_request", server._name + "_space"]
        names.extend(server._name + "_reply_%d" % index for index in range(4))
        for name in names:
            cleanup(name, str(tmp_path))


def assert_cached_gate(trader, allowed):
    """按服务端相同条件读取 SDK 本地缓存；测试不能主动刷新或改写缓存。"""
    state = trader.communication_health
    assert bool(state.get("healthy")) is allowed
    assert bool(state.get("new_orders_blocked")) is not allowed
    return state


def test_ping_rechecks_temporary_health_busy_before_caching(health_chain):
    """后台采样锁忙随后真实恢复，ping 不得把瞬时旧快照缓存三秒阻断新委托。"""
    trader, publisher, raw, execute, server = health_chain
    raw.check_health.side_effect = [PushLockBusyError("sample busy"),
                                    PushLockBusyError("sample busy"), {"healthy": True}]
    assert publisher.flush() == 0
    assert publisher.status()["health_check_busy"] is True
    trader._probe_communication_health()
    assert_cached_gate(trader, True)
    assert raw.check_health.call_count == 3
    assert publisher.status()["health_check_busy"] is False
    execute.assert_called_once_with("ping", {})
    assert server.status()["executed"] == server.status()["responses"] == 1


@pytest.mark.parametrize("fault", ["busy", "publish_busy", "callback", "high_water", "capacity", "sticky", "closed"])
def test_ping_does_not_clear_persistent_or_real_failure(health_chain, fault):
    """持续争锁有界拒绝；回调故障、队列容量与高水位均即时保持缓存门禁关闭。"""
    trader, publisher, raw, execute, _server = health_chain
    if fault == "busy":
        raw.check_health.side_effect = PushLockBusyError("sample stays busy")
    elif fault == "publish_busy":
        raw.publish.side_effect = PushLockBusyError("publish stays busy")
        publisher.publish("quote:tick", {"lastPrice": 4.101})
        assert publisher.flush() == 0
    elif fault == "callback":
        raw.check_health.side_effect = PushCallbackError("callback failed")
    elif fault == "high_water":
        publisher.publish("exec:trade", {"id": "held"})
    elif fault == "capacity":
        publisher._backpressure = True
    elif fault == "sticky":
        publisher._sticky_error = "execution overflow"
    else:
        publisher._closed = True
    started = time.monotonic()
    trader._probe_communication_health()
    elapsed = time.monotonic() - started
    state = assert_cached_gate(trader, False)
    if fault in ("busy", "publish_busy"):
        assert 0.08 <= elapsed < 0.4
        assert "busy" in state["last_error"]
        assert raw.check_health.call_count > 1
        if fault == "publish_busy":
            assert publisher.status()["local_pending"]["quote"] == 1
            assert publisher.status()["shared_committed"] == 0
    else:
        assert elapsed < 0.08
        assert raw.check_health.call_count <= 1
    execute.assert_called_once_with("ping", {})


def test_ping_publish_busy_waits_for_same_record_actual_commit(health_chain):
    """发布锁忙时，ping 必须等待后台原队首真实提交，健康读取不能替代该提交。"""
    trader, publisher, raw, execute, server = health_chain
    entered, release = threading.Event(), threading.Event()
    identities = []

    def publish(*args, **kwargs):
        """第一轮实际拒绝提交，第二轮由测试释放，消息编号始终保持不变。"""
        identities.append(kwargs["message_id"])
        if len(identities) == 1:
            raise PushLockBusyError("publish busy")
        entered.set()
        assert release.wait(1)
        return True

    raw.publish.side_effect = publish
    publisher.start_publisher()
    try:
        publisher.publish("quote:tick", {"lastPrice": 4.101})
        assert entered.wait(1)
        with ThreadPoolExecutor(max_workers=1) as pool:
            probe = pool.submit(trader._probe_communication_health)
            time.sleep(0.02)
            assert not probe.done()
            assert publisher.status()["shared_committed"] == 0
            assert_cached_gate(trader, False)
            release.set()
            probe.result(1)
        assert publisher.status()["shared_committed"] == 1
        assert_cached_gate(trader, True)
        assert len(identities) == 2 and len(set(identities)) == 1
        execute.assert_called_once_with("ping", {})
        assert server.status()["executed"] == server.status()["responses"] == 1
    finally:
        release.set()
