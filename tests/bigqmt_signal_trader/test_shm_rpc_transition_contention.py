"""实际跨映射锁竞争下的执行门禁及响应提交恢复，不允许重执原生业务。"""

import concurrent.futures
import threading
import time
import uuid
from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService
from bigqmt_signal_trader.transports.shm_primitives import cleanup
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTimeout, SharedMemoryTransport


@pytest.mark.parametrize("phase", ["begin", "response"])
@pytest.mark.parametrize("case", ["temporary", "request_expired", "persistent"])
def test_transient_transition_lock_retains_same_request_and_response(tmp_path, monkeypatch, phase, case):
    """短暂争锁恢复同一请求；过期或持续争锁不得迟到执行、隐式重报或污染下一请求。"""
    hold_seconds = {"temporary": 0.03, "request_expired": 0.06, "persistent": 0.15}[case]
    request_seconds = 0.02 if case == "request_expired" else 0.4
    settings = dict(account_id="transition-audit", namespace=uuid.uuid4().hex,
                    directory=str(tmp_path), rpc_slots=4, rpc_slot_bytes=4096,
                    wait_poll_seconds=0.005)
    server, client, holder = [SharedMemoryTransport(**settings) for _ in range(3)]
    holder._open()
    acquired, released = threading.Event(), threading.Event()
    native_calls = []
    holder_thread = None
    injected = False
    method = "_begin_request" if phase == "begin" else "_commit_response"
    original = getattr(server, method)

    def hold():
        """真正持有共享互斥，不以模拟异常代替进程间抢锁行为。"""
        with holder._locked(1.0):
            acquired.set()
            time.sleep(hold_seconds)
        released.set()

    def contend(*args):
        """仅首个状态转换制造争锁，之后仍执行原始门禁或响应提交逻辑。"""
        nonlocal injected, holder_thread
        if not injected:
            injected = True
            holder_thread = threading.Thread(target=hold)
            holder_thread.start()
            assert acquired.wait(1)
        return original(*args)

    def handle(method_name, params):
        """只记录一次原生副作用，响应携带原样精度值以识别重复或错配。"""
        native_calls.append(dict(params))
        return params

    monkeypatch.setattr(server, method, contend)
    service = RedisPubSubRpcService(
        None, SimpleNamespace(handle=handle), account_id=settings["account_id"],
        transport=server, background_threads=True,
    )
    request = dict(request_id=uuid.uuid4().hex, account_id=settings["account_id"],
                   method="synthetic_side_effect", params={"quantity": 2**53 + 1})
    service.start()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(client.send_request, request, request_seconds)
            deadline = time.monotonic() + 1
            while service.pending.qsize() != 1:
                assert time.monotonic() < deadline
                time.sleep(0.001)
            service.drain_pending()
            if case == "temporary":
                response = result.result(1)
                assert response["ok"] is True
                assert response["request_id"] == request["request_id"]
                assert response["data"] == request["params"]
            else:
                with pytest.raises(SharedMemoryTimeout):
                    result.result(1)
        assert released.wait(1)
        executed = case == "temporary" or phase == "response"
        assert native_calls == ([request["params"]] if executed else [])
        assert client.status()["published"] == 1
        assert server.status()["executed"] == int(executed)
        assert server.status()["responses"] == int(case == "temporary")
        assert server.status()["execution_lock_retries" if phase == "begin" else "response_lock_retries"] >= 1
        if case != "temporary":
            fresh = dict(request, request_id=uuid.uuid4().hex, params={"quantity": 2**53 + 3})
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(client.send_request, fresh, 0.4)
                deadline = time.monotonic() + 1
                while service.pending.qsize() != 1:
                    assert time.monotonic() < deadline
                    time.sleep(0.001)
                service.drain_pending()
                assert result.result(1)["data"] == fresh["params"]
            assert native_calls == ([request["params"]] if executed else []) + [fresh["params"]]
    finally:
        if holder_thread is not None:
            holder_thread.join(1)
            assert not holder_thread.is_alive()
        service.stop()
        for transport in (client, holder, server):
            transport.stop()
        names = [server._name, server._name + "_request", server._name + "_space"]
        names.extend(server._name + "_reply_%d" % index for index in range(4))
        for name in names:
            cleanup(name, str(tmp_path))
