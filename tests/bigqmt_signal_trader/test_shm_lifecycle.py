"""独立检验共享内存 RPC 停止交接、实际业务生命周期及唤醒降级。"""

import concurrent.futures
import os
import sys
import threading
import time
import uuid
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService
from bigqmt_signal_trader.transports import TransportError
from bigqmt_signal_trader.transports import shm_transport as implementation
from bigqmt_signal_trader.transports.shm_primitives import cleanup
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryPeerRestarted, SharedMemoryTimeout


def _request(value="first"):
    """生成唯一请求编号，所有业务仅回显合成数据。"""
    return {"request_id": uuid.uuid4().hex, "account_id": "lifecycle-acct", "method": "echo",
            "params": {"value": value}}


def _wait(predicate, timeout_seconds=2.0):
    """等待确定的生命周期观察点，秒数超限时明确失败。"""
    deadline = time.monotonic() + timeout_seconds
    while not predicate():
        assert time.monotonic() < deadline, "lifecycle condition did not occur"
        time.sleep(0.001)


@pytest.fixture
def transports(tmp_path):
    """每个测试独立命名空间，关闭全部持有者后显式清理同步对象。"""
    settings = {"namespace": "lifecycle-" + uuid.uuid4().hex, "directory": str(tmp_path),
                "rpc_slots": 4, "rpc_slot_bytes": 4096, "wait_poll_seconds": 0.01,
                "connect_timeout_seconds": 0.5}
    instances = []

    def create():
        """同一逻辑端点创建独立映射实例，避免仅验证同对象重入锁。"""
        transport = implementation.SharedMemoryTransport(account_id="lifecycle-acct", **settings)
        instances.append(transport)
        return transport

    yield create
    names = set()
    for transport in reversed(instances):
        names.update([transport._name, transport._name + "_request", transport._name + "_space"])
        names.update(transport._name + "_reply_%d" % index for index in range(settings["rpc_slots"]))
        transport.stop()
    for name in names:
        cleanup(name, settings["directory"])


def _echo(request):
    """返回可匹配的普通业务响应，不访问账户或真实交易。"""
    return {"request_id": request["request_id"], "ok": True, "data": request["params"]}


def test_stop_waits_for_lock_longer_than_poll_and_replacement_starts(transports):
    """停止不能吞掉超出轮询间隔的争锁失败，留下本进程旧身份永久占用。"""
    old, holder, replacement = transports(), transports(), transports()
    old.start_receiving(_echo, background_threads=False)
    holder._open()
    acquired = threading.Event()

    def hold():
        """另一映射实例持锁一百毫秒，明显超过十毫秒轮询间隔。"""
        with holder._locked(1.0):
            acquired.set()
            time.sleep(0.1)

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    assert acquired.wait(1.0)
    try:
        old.stop()
        assert old._resources_closed is True
        replacement.start_receiving(_echo, background_threads=False)
        assert replacement.status()["running"] is True
        assert replacement._epoch != old._epoch
    finally:
        thread.join(1.0)
        assert not thread.is_alive()


def test_slow_generic_callback_keeps_ownership_until_actual_exit(transports):
    """普通回调超过 stop 的线程等待预算时，新服务必须等待实际回调结束。"""
    old, client, replacement = transports(), transports(), transports()
    entered, release = threading.Event(), threading.Event()

    def slow(request):
        """保持回调未退出，测试线程负责明确释放而非依赖睡眠竞争。"""
        entered.set()
        assert release.wait(5.0)
        return _echo(request)

    old.start_receiving(slow)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        response = executor.submit(client.send_request, _request(), 4.0)
        try:
            assert entered.wait(1.0)
            old.stop()
            assert old._resources_closed is False
            with pytest.raises(TransportError, match="live server"):
                replacement.start_receiving(_echo, background_threads=False)
            assert not release.is_set()
            release.set()
            _wait(lambda: old._resources_closed)
            with pytest.raises(SharedMemoryPeerRestarted) as error:
                response.result(1.0)
            assert error.value.outcome_unknown is True
            replacement.start_receiving(_echo, background_threads=False)
            assert replacement.status()["running"] is True
        finally:
            release.set()


def test_real_service_pending_handler_keeps_ownership_outside_receiver_thread(transports):
    """后台只负责入队时，手动 drain 的慢业务也必须占有旧服务生命周期。"""
    old, client, replacement = transports(), transports(), transports()
    entered, release = threading.Event(), threading.Event()
    calls = []

    def handle(method, params):
        """真实服务处理器在独立策略线程执行，退出前不允许替换服务。"""
        calls.append((method, params))
        entered.set()
        assert release.wait(5.0)
        return params

    service = RedisPubSubRpcService(None, SimpleNamespace(handle=handle), account_id="lifecycle-acct",
                                   transport=old, process_in_listener=False, background_threads=True)
    service.start()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        response = executor.submit(client.send_request, _request(), 4.0)
        drain = None
        try:
            _wait(lambda: service.pending.qsize() == 1)
            assert calls == []
            drain = executor.submit(service.drain_pending, 1)
            assert entered.wait(1.0)
            service.stop()
            assert old._resources_closed is False
            with pytest.raises(TransportError, match="live server"):
                replacement.start_receiving(_echo, background_threads=False)
            release.set()
            assert drain.result(1.0) == 1
            _wait(lambda: old._resources_closed)
            with pytest.raises(SharedMemoryPeerRestarted) as error:
                response.result(1.0)
            assert error.value.outcome_unknown is True
            assert len(calls) == 1
            replacement.start_receiving(_echo, background_threads=False)
            assert replacement.status()["running"] is True
        finally:
            release.set()
            service.stop()


def test_client_reply_signal_constructor_failure_still_reads_committed_response(transports, monkeypatch):
    """响应提示构造失败只降级轮询，共享请求不得被误判失败或重新发布。"""
    server, client = transports(), transports()
    calls = []
    original_signal = implementation.NamedSignal
    caller_thread = threading.get_ident()

    def build_signal(name, directory):
        """仅破坏客户端回复提示构造，其它同步对象保留真实实现。"""
        if "_reply_" in name and threading.get_ident() == caller_thread:
            raise TransportError("injected reply signal constructor failure")
        return original_signal(name, directory)

    def handle(request):
        """记录实际执行次数，证明没有因提示失败自动重发。"""
        calls.append(request)
        return _echo(request)

    monkeypatch.setattr(implementation, "NamedSignal", build_signal)
    server.start_receiving(handle)
    result = client.send_request(_request("poll-success"), 1.0)
    assert result["ok"] is True
    assert result["data"]["value"] == "poll-success"
    assert client.status()["published"] == 1
    assert client.status()["signal_errors"] >= 1
    assert server.status()["executed"] == 1
    assert len(calls) == 1
    assert server.status()["error"] == ""


def test_begin_request_transient_lock_timeout_rejects_only_this_request(transports, monkeypatch):
    """领取后短暂争锁不能执行业务，也不能杀掉接收线程影响后续请求。"""
    server, holder, client = transports(), transports(), transports()
    holder._open()
    locked = threading.Event()
    release = threading.Event()
    released = threading.Event()
    original_begin = server.begin_request
    holder_thread = None
    calls = []
    first = True

    def hold():
        """令真正执行门禁争锁失败，随后自动释放供下一条请求使用。"""
        try:
            with holder._locked(1.0):
                locked.set()
                # 持锁直到本次执行门禁完成，避免线程调度变慢后测试意外放行。
                assert release.wait(2.0)
        finally:
            released.set()

    def begin(request):
        """在首条请求已经领取后制造真实锁竞争，不替换门禁结果。"""
        nonlocal first, holder_thread
        if first:
            first = False
            holder_thread = threading.Thread(target=hold, daemon=True)
            holder_thread.start()
            assert locked.wait(1.0)
        try:
            return original_begin(request)
        finally:
            release.set()

    def handle(request):
        """仅应收到第二条允许执行的业务请求。"""
        calls.append(request["params"]["value"])
        return _echo(request)

    monkeypatch.setattr(server, "begin_request", begin)
    server.start_receiving(handle)
    try:
        with pytest.raises(SharedMemoryTimeout) as error:
            client.send_request(_request("must-not-execute"), 0.25)
        # Windows 调度可能让服务端先回收过期槽；客户端已无执行凭据时应保守要求
        # 对账，不能仅凭测试知道业务未执行就强迫生产接口返回确定结果。
        if error.value.outcome_unknown:
            assert "request slot expired" in str(error.value)
        assert released.wait(1.0)
        assert calls == []
        assert server.status()["executed"] == 0
        assert server.status()["running"] is True
        assert server.status()["error"] == ""
        assert server._thread.is_alive()
        result = client.send_request(_request("fresh"), 1.0)
        assert result["ok"] is True
        assert calls == ["fresh"]
        assert server.status()["executed"] == 1
    finally:
        release.set()
        if holder_thread is not None:
            holder_thread.join(1.0)
            assert not holder_thread.is_alive()


def test_consumed_request_hint_with_lock_contention_uses_short_retry_budget(transports, monkeypatch):
    """已消费请求提示后领取争锁，后台短等重查，不能再次按空队列睡五十毫秒。"""
    server, holder, client = transports(), transports(), transports()
    server.wait_poll_seconds = 0.05
    holder._open()
    waiting = threading.Event()
    held = threading.Event()
    release = threading.Event()
    contended_wait = threading.Event()
    original_wait = server._wait_hint
    holder_thread = None
    budgets = []
    calls = []

    def hold():
        """独立映射在请求提示已经消费后持锁，直到测试观察到下一次等待预算。"""
        with holder._locked(1.0):
            held.set()
            assert release.wait(1.0)

    def wait_hint(signal, seconds):
        """保留真实提示消费，用同步观察点稳定安排下一次 drain 的锁竞争。"""
        nonlocal holder_thread
        if signal is server._request_signal:
            budgets.append((seconds, server._drain_contended))
            waiting.set()
            if server._drain_contended:
                contended_wait.set()
                release.set()
        result = original_wait(signal, seconds)
        if signal is server._request_signal and client.status()["published"] == 1 and holder_thread is None:
            # 真实请求提示已经由上面的原实现消费，锁竞争发生在下一次领取之前。
            holder_thread = threading.Thread(target=hold, daemon=True)
            holder_thread.start()
            assert held.wait(1.0)
        return result

    def handle(request):
        """竞争只延后同一请求，业务执行不能重复。"""
        calls.append(request["request_id"])
        return _echo(request)

    monkeypatch.setattr(server, "_wait_hint", wait_hint)
    server.start_receiving(handle)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        try:
            assert waiting.wait(1.0)
            future = executor.submit(client.send_request, _request("once"), 1.0)
            assert contended_wait.wait(1.0)
            result = future.result(1.0)
            assert result["ok"] is True
            assert result["data"]["value"] == "once"
            assert len(calls) == 1
            assert client.status()["published"] == 1
            assert server.status()["executed"] == 1
            assert server.status()["lock_contentions"] >= 1
            assert server.status()["error"] == ""
            short_budgets = [seconds for seconds, contended in budgets if contended]
            assert short_budgets
            assert all(0 < seconds <= 0.001 for seconds in short_budgets)
            assert (0.05, False) in budgets
        finally:
            release.set()
            if holder_thread is not None:
                holder_thread.join(1.0)
                assert not holder_thread.is_alive()
