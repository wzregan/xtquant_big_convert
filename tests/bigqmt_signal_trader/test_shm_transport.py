"""通过独立进程及故障注入检验共享内存 RPC 的执行边界。"""

import concurrent.futures
import multiprocessing
import time
import uuid
from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.transports import TransportError, build_transport, get_backend
from bigqmt_signal_trader.transports.shm_transport import (
    SharedMemoryPeerRestarted, SharedMemoryTimeout, SharedMemoryTransport,
)
from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService


def request(value=1):
    """构造测试信封，消息编号与业务数据分开以检测响应串线。"""
    return {"schema_version": 1, "request_id": uuid.uuid4().hex, "account_id": "test",
            "method": "echo", "params": {"value": value}}


def _server_entry(config, pipe):
    """独立解释器重新打开映射；控制管道仅传启动、退出及故障注入信号。"""
    server = SharedMemoryTransport(account_id="test", **config)

    def handle(req):
        """仅回显合成数据，不访问 QMT 或执行交易。"""
        if req["params"].get("pause"):
            pipe.send("executing")
            time.sleep(10)
        return {"ok": True, "data": req["params"], "request_id": req["request_id"]}

    server.start_receiving(handle)
    pipe.send("ready")
    try:
        pipe.recv()
    finally:
        server.stop()
        pipe.close()


@pytest.fixture
def config(tmp_path):
    """每个测试使用独立命名空间，避免影响本机其它端点。"""
    return {"namespace": "test-rpc-" + uuid.uuid4().hex, "directory": str(tmp_path),
            "rpc_slots": 8, "rpc_slot_bytes": 4096, "wait_poll_seconds": 0.01}


@pytest.fixture
def pair(config):
    """关闭所有持有者，POSIX测试资源由临时目录回收。"""
    server = SharedMemoryTransport(account_id="test", **config)
    client = SharedMemoryTransport(account_id="test", **config)
    yield server, client
    client.stop()
    server.stop()


def wait_until(predicate, timeout=2):
    """有限等待测试观察点，超时保留原断言语义。"""
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition was not reached"
        time.sleep(0.001)


def launch(config):
    """用 spawn 启动真正独立进程，避免继承 mmap 或锁制造假阳性。"""
    ctx = multiprocessing.get_context("spawn")
    parent, child = ctx.Pipe()
    process = ctx.Process(target=_server_entry, args=(config, child))
    process.start()
    child.close()
    assert parent.poll(10)
    assert parent.recv() == "ready"
    return process, parent


def finish(process, pipe):
    """测试失败也确保子进程退出，不留下持有锁的后台进程。"""
    if process.is_alive():
        pipe.send("stop")
    process.join(2)
    if process.is_alive():
        process.kill()
        process.join(2)
    pipe.close()


def test_independent_process_20_concurrent_requests_and_large_payload(config):
    """20线程经过真正跨进程映射；大请求和大响应走独立映射并保持内容完整。"""
    process, pipe = launch(config)
    client = SharedMemoryTransport(account_id="test", **config)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            values = list(range(200))
            actual = list(pool.map(lambda x: client.send_request(request(x), 5)["data"]["value"], values))
            assert actual == values
        large = "共享内存" * 300000
        assert client.send_request(request(large), 5)["data"]["value"] == large
        assert client.status()["published"] == 201
    finally:
        client.stop()
        finish(process, pipe)


def test_timeout_before_server_start_has_no_side_effect(pair):
    """无服务端时总等待有界，并明确尚未发布。"""
    _, client = pair
    started = time.monotonic()
    with pytest.raises(SharedMemoryTimeout) as error:
        client.send_request(request(), 0.03)
    assert not error.value.outcome_unknown
    assert time.monotonic() - started < 0.3
    assert client.status()["published"] == 0


def test_expired_request_never_dispatches(pair):
    """请求未被策略轮询时到期，后来 drain 不会执行它。"""
    server, client = pair
    calls = []
    server.start_receiving(lambda req: calls.append(req), background_threads=False)
    with pytest.raises(SharedMemoryTimeout) as error:
        client.send_request(request(), 0.03)
    assert not error.value.outcome_unknown
    assert server.drain_request_queue() == 0
    assert calls == []


def test_deferred_service_deadline_checked_at_actual_execution(pair):
    """接收线程已入本地队列也不意味着可忽略原调用的截止时间。"""
    server, client = pair
    calls = []
    handlers = SimpleNamespace(handle=lambda method, params: calls.append(params) or params)
    service = RedisPubSubRpcService(None, handlers, account_id="test", transport=server,
                                   process_in_listener=False, background_threads=True)
    service.start()
    try:
        with pytest.raises(SharedMemoryTimeout) as error:
            client.send_request(request(), 0.04)
        assert not error.value.outcome_unknown
        assert service.pending.qsize() == 1
        service.drain_pending()
        assert calls == []
    finally:
        service.stop()


def test_late_reply_cannot_overwrite_reused_slot(config):
    """同槽重新分配后，旧回调的令牌会被拒绝。"""
    config = dict(config, rpc_slots=1)
    server = SharedMemoryTransport(account_id="test", **config)
    client = SharedMemoryTransport(account_id="test", **config)
    received = []
    server.start_receiving(lambda req: received.append(req), background_threads=False)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(client.send_request, request("first"), 0.05)
            wait_until(lambda: client.status()["published"] == 1)
            assert server.drain_request_queue() == 1
            with pytest.raises(SharedMemoryTimeout) as error:
                first.result(1)
            assert error.value.outcome_unknown
            second = pool.submit(client.send_request, request("second"), 1)
            wait_until(lambda: client.status()["published"] == 2)
            assert not server.send_response(received[0], {"ok": True, "data": "wrong"})
            assert server.drain_request_queue() == 1
            assert server.send_response(received[1], {"ok": True, "data": "right"})
            assert second.result(1)["data"] == "right"
    finally:
        client.stop()
        server.stop()


def test_server_crash_after_execution_reports_unknown_then_reconnects(config):
    """服务执行中被杀后不自动重发；同一客户端下一次新调用可连接新代次。"""
    process, pipe = launch(config)
    client = SharedMemoryTransport(account_id="test", **config)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            req = request()
            req["params"]["pause"] = True
            future = pool.submit(client.send_request, req, 3)
            assert pipe.poll(3) and pipe.recv() == "executing"
            process.kill()
            process.join(3)
            with pytest.raises(SharedMemoryPeerRestarted) as error:
                future.result(1)
            assert error.value.outcome_unknown
        pipe.close()
        process, pipe = launch(config)
        assert client.send_request(request("fresh"), 2)["data"]["value"] == "fresh"
        assert client.status()["published"] == 2
    finally:
        client.stop()
        finish(process, pipe)


def test_exclusive_server_and_configuration_mismatch(pair, config):
    """活服务端不能被覆盖，ABI容量变化也不能误附着到旧映射。"""
    server, client = pair
    server.start_receiving(lambda req: {"ok": True})
    with pytest.raises(TransportError, match="live server"):
        client.start_receiving(lambda req: None)
    other = SharedMemoryTransport(account_id="test", **dict(config, rpc_slots=9))
    try:
        with pytest.raises(TransportError):
            other.send_request(request(), 0.1)
    finally:
        other.stop()


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_invalid_budget_never_opens_resources(config, timeout):
    """非法预算在建立映射及发布请求前拒绝。"""
    client = SharedMemoryTransport(account_id="test", **config)
    with pytest.raises(ValueError):
        client.send_request(request(), timeout)
    assert client._region is None
    client.stop()


def test_corrupt_frame_is_not_executed(pair):
    """损坏的已提交请求必须显式故障，不能把半包交给交易处理器。"""
    server, client = pair
    calls = []
    server.start_receiving(lambda req: calls.append(req), background_threads=False)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(client.send_request, request(), 0.08)
        wait_until(lambda: client.status()["published"] == 1)
        with server._locked(1):
            from bigqmt_signal_trader.transports.shm_transport import _SLOT_SIZE
            offset = server._offset(0) + _SLOT_SIZE
            server._region.buffer[offset:offset + 1] = b"!"
        assert server.drain_request_queue() == 1
        response = future.result(1)
        assert not response["ok"] and not response["outcome_unknown"]
        assert response["error_code"] == "SHM_REQUEST_CORRUPT"
    assert calls == []


def test_factory_pins_matched_rpc_push_and_polling(config):
    """正式工厂成套提供两个功能，并允许原生QMT在策略线程轮询。"""
    backend = get_backend("shm")
    assert backend.supports_scheduler_polling and backend.acknowledged_push
    assert not backend.default_background_threads
    transport = build_transport("shm", {"shm": config}, account_id="test")
    push = backend.build_push_channel("client", {"shm": config}, account_id="test")
    assert transport._region is None
    assert push.supports_acknowledgements
    transport.stop()
    push.stop()


def test_stop_during_pending_wait_releases_resources(pair):
    """并发stop唤醒等候者，操作退出前不关闭其仍在使用的映射。"""
    server, client = pair
    server.start_receiving(lambda req: None, background_threads=False)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(client.send_request, request(), 3)
        wait_until(lambda: client.status()["published"] == 1)
        client.stop()
        with pytest.raises(SharedMemoryTimeout):
            future.result(0.5)
    assert client._resources_closed


def test_oversized_response_returns_explicit_error(pair):
    """业务完成后回包超限不能假装未执行或只让调用方一直等待。"""
    server, client = pair
    server.start_receiving(lambda req: {"ok": True, "data": "x" * (server.max_payload_bytes + 1)})
    result = client.send_request(request(), 3)
    assert not result["ok"] and result["outcome_unknown"]
    assert result["error_code"] == "SHM_RESPONSE_ENCODING"
