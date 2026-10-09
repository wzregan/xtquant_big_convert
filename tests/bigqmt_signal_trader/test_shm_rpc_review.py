"""独立审查发现的共享内存 RPC 大报文截止时间及崩溃清理回归。"""

import hashlib
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

import pytest


SOURCE = Path(__file__).resolve().parents[2] / "src"
sys.path.insert(0, str(SOURCE))

from bigqmt_signal_trader.transports.shm_primitives import cleanup  # noqa: E402
from bigqmt_signal_trader.transports import shm_transport as shm_module  # noqa: E402
from bigqmt_signal_trader.transports.shm_transport import (  # noqa: E402
    SharedMemoryTimeout, SharedMemoryTransport,
)


@pytest.fixture
def endpoints(tmp_path):
    """使用独立命名域和目录，最后关闭全部对象再清理主端点。"""
    namespace = "rpc-review-" + uuid.uuid4().hex
    values = []

    def create():
        """登记仅用于合成业务的 RPC 对象，内联小容量强制经过动态 blob。"""
        value = SharedMemoryTransport(account_id="synthetic-review", namespace=namespace,
                                      directory=str(tmp_path), rpc_slots=2, rpc_slot_bytes=1024)
        values.append(value)
        return value

    try:
        yield create, tmp_path
    finally:
        for value in reversed(values):
            value.stop()
        if values:
            for suffix in ("", "_request", "_space", "_reply_0", "_reply_1"):
                cleanup(values[0]._name + suffix, str(tmp_path))


def test_expired_external_request_does_not_stop_server_when_blob_is_released(endpoints):
    """客户超时释放大请求后，服务器不能将尚未打开的消失 blob 当作全端点损坏。"""
    create, _directory = endpoints
    server, client = create(), create()
    entered, release = threading.Event(), threading.Event()
    errors, calls = [], []
    original = server._payload

    def paused(*args, **kwargs):
        """在服务端持主区域锁但尚未打开 blob 时暂停，确定性制造实际竞态。"""
        if args[2] == "request" and not entered.is_set():
            entered.set()
            assert release.wait(2.0)
        return original(*args, **kwargs)

    def handler(request):
        """记录业务执行，超时的第一个请求必须未执行且不影响后续请求。"""
        calls.append(request["request_id"])
        return {"ok": True, "request_id": request["request_id"], "data": "OK"}

    def call():
        """提交一次外部 blob 请求，保存原始超时性质供主线程核对。"""
        try:
            client.send_request({"request_id": "expired-blob", "method": "synthetic",
                                 "params": {"padding": "x" * 3000}}, 0.05)
        except Exception as exc:
            errors.append(exc)

    server._payload = paused
    server.start_receiving(handler)
    caller = threading.Thread(target=call)
    caller.start()
    try:
        assert entered.wait(1.0)
        caller.join(1.0)
        assert not caller.is_alive()
        assert len(errors) == 1 and isinstance(errors[0], SharedMemoryTimeout)
        release.set()
        response = client.send_request({"request_id": "following-ping", "method": "synthetic", "params": {}}, 1.0)
        assert response["ok"] and response["request_id"] == "following-ping"
        server.check_health()
        assert calls == ["following-ping"]
    finally:
        release.set()
        caller.join(2.0)


def test_external_response_open_uses_remaining_rpc_deadline(endpoints, monkeypatch):
    """响应 blob 的独立互斥等待不能获得新的 50ms 预算后迟到成功。"""
    create, _directory = endpoints
    server, client = create(), create()
    original = server._create_blob
    owners = []
    opening_budgets = []
    caller_thread = threading.get_ident()
    original_region = shm_module.SharedRegion

    def observed_region(name, *args, **kwargs):
        """记录调用线程交给真正 blob 打开的预算，并保留全部实际 OS 等待。"""
        if "_response_" in name and threading.get_ident() == caller_thread:
            opening_budgets.append(kwargs.get("timeout_seconds"))
        return original_region(name, *args, **kwargs)

    monkeypatch.setattr(shm_module, "SharedRegion", observed_region)

    def held_blob(*args, **kwargs):
        """创建完成后由另一个线程实际持住 blob 锁，模拟真实打开等待。"""
        region = original(*args, **kwargs)
        if args[1] == "response":
            acquired = threading.Event()

            def owner():
                """临时占用 blob 进程锁，远长于客户端剩余预算。"""
                with region.locked(1.0):
                    acquired.set()
                    time.sleep(0.12)

            thread = threading.Thread(target=owner)
            owners.append(thread)
            thread.start()
            assert acquired.wait(1.0)
        return region

    server._create_blob = held_blob
    server.start_receiving(lambda request: {"ok": True, "request_id": request["request_id"], "data": "x" * 3000})
    before = time.monotonic()
    try:
        with pytest.raises(SharedMemoryTimeout) as captured:
            client.send_request({"request_id": "response-budget", "method": "synthetic", "params": {}}, 0.03)
        assert captured.value.outcome_unknown is True
        assert time.monotonic() - before < 0.10
        assert opening_budgets and all(0 <= value <= 0.03 for value in opening_budgets)
    finally:
        for owner in owners:
            owner.join(1.0)
    server.check_health()


@pytest.mark.skipif(os.name == "nt", reason="POSIX 崩溃后命名 blob 文件回收")
def test_restart_reclaims_external_request_blob_of_killed_client(endpoints):
    """提交后客户被 kill，重启按旧槽令牌回收 orphan，不能积累命名内存文件。"""
    create, directory = endpoints
    server = create()
    server.start_receiving(lambda request: {"ok": True, "request_id": request["request_id"]},
                           background_threads=False)
    code = """
import sys
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport
client=SharedMemoryTransport(account_id='synthetic-review',namespace=sys.argv[1],directory=sys.argv[2],rpc_slots=2,rpc_slot_bytes=1024)
client.send_request({'request_id':'orphan-review','method':'synthetic','params':{'padding':'x'*3000}},5.0)
"""
    environment = dict(os.environ, PYTHONPATH=str(SOURCE))
    child = subprocess.Popen([sys.executable, "-c", code, server.namespace, str(directory)],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             universal_newlines=True, env=environment)
    value = None
    try:
        deadline = time.monotonic() + 2.0
        while value is None and time.monotonic() < deadline:
            with server._locked(0.1):
                for index in range(server.rpc_slots):
                    candidate = server._read_slot(index)
                    if candidate is not None and candidate[0] == shm_module._REQUEST:
                        value = candidate
                        break
            if value is None:
                time.sleep(0.002)
        assert value is not None
        blob_name = server._blob_name(value[1], "request")
        key = "bigqmt_" + hashlib.sha256(blob_name.encode("utf-8")).hexdigest()
        region_file = directory / (key + ".region")
        assert region_file.exists()
        child.kill()
        child.communicate(timeout=5.0)

        server.stop()
        restarted = create()
        restarted.start_receiving(lambda request: {"ok": True, "request_id": request["request_id"]},
                                  background_threads=False)
        assert not region_file.exists(), "old committed slot left an orphan request mapping after restart"
        assert not (directory / (key + ".lock")).exists()
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5.0)


@pytest.mark.skipif(os.name == "nt", reason="POSIX PREPARING 阶段的 orphan 文件回收")
def test_killed_client_during_preparing_is_not_executed_and_blob_is_reclaimed(endpoints):
    """大请求尚未发布时被 kill，预留槽令牌足以立即回收 blob 且不执行业务。"""
    create, directory = endpoints
    server = create()
    calls = []

    def handler(request):
        """任何业务执行都说明 PREPARING 被错误当作已提交请求。"""
        calls.append(request["request_id"])
        return {"ok": True, "request_id": request["request_id"]}

    server.start_receiving(handler, background_threads=False)
    code = """
import sys,time
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport
client=SharedMemoryTransport(account_id='synthetic-review',namespace=sys.argv[1],directory=sys.argv[2],rpc_slots=2,rpc_slot_bytes=1024)
original=client._create_blob
def hold(*args,**kwargs):
    '''实际分配后暂停，供父进程强杀尚未提交的客户端。'''
    # 真实分配大映射后暂停，仍未把 PREPARING 提交为 REQUEST。
    blob=original(*args,**kwargs)
    time.sleep(60)
    return blob
client._create_blob=hold
client.send_request({'request_id':'preparing-review','method':'synthetic','params':{'padding':'x'*3000}},5.0)
"""
    child = subprocess.Popen([sys.executable, "-c", code, server.namespace, str(directory)],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             universal_newlines=True, env=dict(os.environ, PYTHONPATH=str(SOURCE)))
    index, value, region_file = None, None, None
    try:
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            with server._locked(0.1):
                for candidate_index in range(server.rpc_slots):
                    candidate = server._read_slot(candidate_index)
                    if candidate is not None:
                        index, value = candidate_index, candidate
                        break
            if value is not None:
                assert value[0] == shm_module._PREPARING
                blob_name = server._blob_name(value[1], "request")
                key = "bigqmt_" + hashlib.sha256(blob_name.encode("utf-8")).hexdigest()
                region_file = directory / (key + ".region")
                if region_file.exists():
                    break
            time.sleep(0.002)
        assert region_file is not None and region_file.exists()
        child.kill()
        child.communicate(timeout=5.0)
        assert server.drain_request_queue(2) == 0
        assert calls == []
        assert not region_file.exists()
        with server._locked(0.1):
            assert server._read_slot(index) is None
        server.check_health()
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5.0)
