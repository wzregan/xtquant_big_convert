"""共享内存 RPC 的异常隔离、资源生命周期与协议完整性审计。"""

import concurrent.futures
import contextlib
import threading
import time
import uuid

import pytest

from bigqmt_signal_trader.transports import shm_transport as impl
from bigqmt_signal_trader.transports.base import TransportError, TransportTimeout


@pytest.fixture
def endpoints(tmp_path):
    """提供独立命名域的真实映射；允许按故障用例调整容量。"""
    values = []
    namespace = "rpc-audit-" + uuid.uuid4().hex

    def create(**options):
        """创建同端点对象，登记句柄用于失败后的统一释放。"""
        settings = dict(account_id="synthetic", namespace=namespace, directory=str(tmp_path),
                        rpc_slots=2, rpc_slot_bytes=1024, max_payload_bytes=65536,
                        wait_poll_seconds=0.005, connect_timeout_seconds=0.1)
        settings.update(options)
        result = impl.SharedMemoryTransport(**settings)
        values.append(result)
        return result

    yield create
    for value in reversed(values):
        value.stop()
    if values:
        for suffix in ("", "_request", "_space", "_reply_0", "_reply_1"):
            impl.cleanup(values[0]._name + suffix, str(tmp_path))


def request(value="payload"):
    """生成与真实业务封装相同的请求，业务仅在测试中回显。"""
    return {"request_id": uuid.uuid4().hex, "method": "echo", "params": {"value": value}}


def echo(value):
    """返回匹配请求号的合成响应。"""
    return {"ok": True, "request_id": value["request_id"], "data": value["params"]}


def wait_for(predicate):
    """等待明确观察点，失败时不留下无限等待。"""
    deadline = time.monotonic() + 2
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.001)


def reserve(server, state=impl._CLAIMED, payload=b"{}", external=False, expired=False, index=0):
    """构造可信提交槽供协议损坏测试，保留真实 mmap 和互斥操作。"""
    token = uuid.uuid4().bytes
    deadline = time.monotonic() + (-1 if expired else 5)
    value = (state, token, server._pid, server._birth, deadline, 1,
             len(payload) | (impl._EXTERNAL if external else 0), impl._crc(payload))
    with server._locked(0.1):
        server._write_slot(index, value, None if external else payload)
    route = {"index": index, "token": token.hex(), "epoch": server._epoch.hex()}
    return {"request_id": "reserved", "_shm_route": route}, value


def test_duplicate_large_response_keeps_first_committed_payload(endpoints):
    """重复回调不能覆盖或清除尚未由客户确认的首次大响应。"""
    server = endpoints()
    server.start_receiving(echo, background_threads=False)
    original, _ = reserve(server)
    first = {"ok": True, "data": "A" * 3000}
    assert server.send_response(original, first)
    assert not server.send_response(original, {"ok": True, "data": "B" * 3000})
    with server._locked(0.1):
        value = server._read_slot(0)
        raw = server._payload(0, value, "response")
    assert b"A" * 3000 in raw
    assert server.status()["responses"] == 1


def test_background_start_failure_preserves_error_and_releases_owner(endpoints, monkeypatch):
    """操作系统拒绝线程启动时必须释放服务所有权，保留原始异常。"""
    server = endpoints()

    def fail_start(_self):
        """注入线程创建资源不足，线程对象从未进入运行状态。"""
        raise OSError("thread resources exhausted")

    with monkeypatch.context() as patch:
        patch.setattr(threading.Thread, "start", fail_start)
        with pytest.raises(OSError, match="resources exhausted"):
            server.start_receiving(echo)
    assert server._resources_closed
    replacement = endpoints()
    replacement.start_receiving(echo, background_threads=False)
    assert replacement.status()["running"]


@pytest.mark.parametrize("options", [
    {"rpc_slots": 0}, {"rpc_slots": 1025}, {"rpc_slot_bytes": 1000},
    {"max_payload_bytes": 1000}, {"rpc_slots": 1024, "rpc_slot_bytes": 1024 * 1024},
])
def test_invalid_mapping_configuration_is_rejected_before_allocation(endpoints, options):
    """非法容量不得打开映射或造成超大匿名内存分配。"""
    with pytest.raises(ValueError):
        endpoints(**options)


def test_closed_transport_rejects_new_operations_and_reports_health(endpoints):
    """关闭后禁止重新打开端点，重复关闭和空 drain 都是幂等操作。"""
    server = endpoints()
    server.stop()
    server.stop()
    with pytest.raises(TransportError, match="closed"):
        server.send_request(request(), 1)
    with pytest.raises(TransportError, match="closed"):
        server.check_health()
    assert server.drain_request_queue() == 0
    assert server.begin_request({}) is False
    with server.execution_scope({}) as allowed:
        assert not allowed


def test_initialization_lock_uses_original_deadline(endpoints):
    """初始化互斥等待也受调用总预算限制，不能悄悄重开连接预算。"""
    client = endpoints()
    acquired, release = threading.Event(), threading.Event()

    def hold():
        """从另一个线程占用初始化锁，保持真实线程锁等待。"""
        with client._init_lock:
            acquired.set()
            assert release.wait(1)

    thread = threading.Thread(target=hold)
    thread.start()
    try:
        assert acquired.wait(1)
        with pytest.raises(impl.SharedMemoryTimeout, match="initialization"):
            client.send_request(request(), 0.01)
        assert client._region is None
    finally:
        release.set()
        thread.join(1)


@pytest.mark.parametrize("fail_number", [1, 2])
def test_signal_initialization_failure_closes_partial_resources(endpoints, monkeypatch, fail_number):
    """创建首个或第二个唤醒对象失败，都释放之前已打开的句柄并允许重试。"""
    client = endpoints()
    original = impl.NamedSignal
    calls, opened = [], []

    def fail_signal(*args, **kwargs):
        """保留真实首个信号，仅在指定创建位置注入系统失败。"""
        calls.append(args[0])
        if len(calls) == fail_number:
            raise OSError("signal allocation failed")
        value = original(*args, **kwargs)
        opened.append(value)
        return value

    with monkeypatch.context() as patch:
        patch.setattr(impl, "NamedSignal", fail_signal)
        with pytest.raises(OSError, match="signal allocation"):
            client._open()
    assert client._region is None
    for signal in opened:
        signal.close()
    client._open()
    assert client._region is not None


@pytest.mark.parametrize("fault", ["header_crc", "header_magic", "abi", "writing", "state", "slot_crc", "external_length", "inline_length"])
def test_protocol_corruption_is_rejected_before_business(endpoints, fault):
    """逐层破坏提交元数据；任何损坏都必须在业务代码运行前失败。"""
    server = endpoints()
    calls = []
    server.start_receiving(lambda value: calls.append(value), background_threads=False)
    original, value = reserve(server, state=impl._REQUEST)
    saved = bytes(server._region.buffer)
    try:
        if fault.startswith("header") or fault == "abi":
            raw = bytearray(server._region.buffer[:impl._HEADER.size])
            if fault == "header_magic":
                raw[0] ^= 1
            elif fault == "abi":
                raw[8] ^= 1
            server._region.buffer[:impl._HEADER.size] = raw
            checksum = impl._crc(raw) ^ (fault == "header_crc")
            server._region.buffer[impl._HEADER.size:impl._HEADER.size + 4] = impl._U32.pack(checksum)
        else:
            offset = server._offset(0)
            if fault in ("writing", "state"):
                server._region.buffer[offset:offset + 4] = impl._U32.pack(impl._WRITING if fault == "writing" else 99)
            elif fault == "slot_crc":
                server._region.buffer[offset + impl._SLOT.size:offset + impl._SLOT.size + 4] = b"bad!"
            else:
                size = (server.max_payload_bytes + 1) | impl._EXTERNAL if fault == "external_length" else server.rpc_slot_bytes + 1
                server._write_slot(0, value[:6] + (size, value[7]))
        with pytest.raises(TransportError):
            server.drain_request_queue()
        assert calls == []
    finally:
        server._region.buffer[:] = saved


def test_abandoned_lock_validates_every_slot_before_recovery(endpoints, monkeypatch):
    """模拟 Windows 遗弃锁通知时仍走真实映射，必须发现未提交槽。"""
    server = endpoints()
    server.start_receiving(echo, background_threads=False)
    original = server._region.locked

    @contextlib.contextmanager
    def abandoned(**options):
        """真实互斥加锁成功后，注入内核遗弃标志供协议层验证。"""
        with original(**options):
            yield True

    monkeypatch.setattr(server._region, "locked", abandoned)
    with server._locked(0.1):
        pass
    offset = server._offset(1)
    server._region.buffer[offset:offset + 4] = impl._U32.pack(impl._WRITING)
    try:
        with pytest.raises(impl.SharedMemoryCorruption, match="uncommitted"):
            with server._locked(0.1):
                pytest.fail("corrupt slot was accepted")
    finally:
        server._free_slot(1)


def test_signal_set_and_wait_failure_fall_back_without_resending(endpoints, monkeypatch):
    """通知仅是性能提示，通知系统失效不能变成重复业务调用。"""
    server, client = endpoints(), endpoints()
    calls = []
    server.start_receiving(lambda value: calls.append(value) or echo(value))
    client._open()

    def unavailable(*_args):
        """模拟独立信号对象运行期失效。"""
        raise OSError("signal unavailable")

    monkeypatch.setattr(client._request_signal, "set", unavailable)
    monkeypatch.setattr(client._reply_signal(0), "wait", unavailable)
    assert client.send_request(request(), 1)["ok"]
    assert len(calls) == 1
    assert client.status()["published"] == 1
    assert client.status()["signal_errors"] >= 1


def test_serialization_rejects_bad_requests_before_publication(endpoints):
    """无效 JSON 和超限报文不占槽，不给业务端产生任何请求。"""
    client = endpoints()
    for value, message in [(object(), "serialization"), ("x" * 65536, "exceeds")]:
        with pytest.raises(TransportError, match=message):
            client.send_request(request(value), 1)
    assert client._region is None
    assert client.status()["published"] == 0


def test_duplicate_business_ids_have_isolated_transport_tokens(endpoints):
    """同业务编号仍用独立槽令牌路由，通信层不擅自去重或混淆返回数据。"""
    server, client = endpoints(), endpoints()
    server.start_receiving(echo)
    requests = [dict(request(index), request_id="same-business-id") for index in range(20)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        replies = list(pool.map(lambda item: client.send_request(item, 3), requests))
    assert [item["data"]["value"] for item in replies] == list(range(20))
    assert server.status()["executed"] == 20


def test_repeat_start_keeps_original_handler_and_epoch(endpoints):
    """同实例重复启动不创建第二个接收线程、不更换运行代次。"""
    server, client = endpoints(), endpoints()
    server.start_receiving(echo)
    thread, epoch = server._thread, server._epoch
    server.start_receiving(lambda _request: pytest.fail("handler replaced"))
    assert server._thread is thread and server._epoch == epoch
    assert client.send_request(request(), 1)["ok"]


@pytest.mark.parametrize("payload", [b"[]", b"{", b"\xff"])
def test_bad_json_body_returns_not_started_and_server_recovers(endpoints, payload):
    """CRC 合法但 JSON/UTF-8 非法的单请求只被拒绝，不能停掉整个接收端。"""
    server, client = endpoints(), endpoints()
    calls = []
    server.start_receiving(lambda value: calls.append(value) or echo(value), background_threads=False)
    _, value = reserve(server, state=impl._REQUEST, payload=payload)
    assert server.drain_request_queue(1) == 1
    with server._locked(0.1):
        response = server._payload(0, server._read_slot(0), "response")
        server._free_slot(0)
    assert b'SHM_REQUEST_CORRUPT' in response and b'"outcome_unknown":false' in response
    assert not calls
    server.start_receiving(echo, background_threads=False)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(client.send_request, request(), 1)
        wait_for(lambda: client.status()["published"] == 1)
        assert server.drain_request_queue(1) == 1
        assert result.result()["ok"]


@pytest.mark.parametrize("route_fault", ["epoch", "index", "token", "expired", "ready", "empty"])
def test_execution_gate_rechecks_ownership_and_deadline(endpoints, route_fault):
    """领取后执行前重新校验代次、令牌、槽和截止时间，拒绝迟到执行。"""
    server = endpoints()
    server.start_receiving(echo, background_threads=False)
    claimed, value = reserve(server, expired=route_fault == "expired")
    if route_fault == "epoch":
        claimed["_shm_route"]["epoch"] = "another-epoch"
    elif route_fault == "index":
        claimed["_shm_route"]["index"] = -1
    elif route_fault == "token":
        claimed["_shm_route"]["token"] = uuid.uuid4().hex
    elif route_fault == "ready":
        server._write_header(server._region.buffer, 0, server._pid, server._birth, server._epoch)
    elif route_fault == "empty":
        server._free_slot(0)
    assert not server.begin_request(claimed)
    assert server.status()["executed"] == 0


def test_out_of_line_response_timeout_preserves_original_request_id(endpoints, monkeypatch):
    """大响应读取超过预算时，仍须保留可用于核对的原请求编号。"""
    server, client = endpoints(), endpoints()
    server.start_receiving(lambda value: dict(echo(value), data="x" * 3000))
    original = client._payload

    def expired_read(*args, **kwargs):
        """仅延迟响应复制至原调用截止时间之后。"""
        time.sleep(max(0, args[1][4] - time.monotonic()) + 0.001)
        return original(*args, **kwargs)

    monkeypatch.setattr(client, "_payload", expired_read)
    value = request()
    with pytest.raises(impl.SharedMemoryTimeout) as captured:
        client.send_request(value, 0.03)
    assert captured.value.request_id == value["request_id"]
    assert captured.value.outcome_unknown is True


def test_total_inline_mapping_limit_is_checked_before_allocation(endpoints):
    """每槽容量合法也不能让总内联映射超过进程内存保护上限。"""
    with pytest.raises(ValueError, match="512 MiB"):
        endpoints(rpc_slots=1024, rpc_slot_bytes=1024 * 1024, max_payload_bytes=2 * 1024 * 1024)


def test_blob_header_write_failure_cleans_allocated_mapping(endpoints, monkeypatch, tmp_path):
    """大报文头提交失败必须释放刚分配的映射和文件，不能积累孤儿对象。"""
    server = endpoints()
    token, payload = uuid.uuid4().bytes, b"x" * 3000
    original = impl._BLOB

    class BrokenHeader:
        """保持报文尺寸，只让头编码抛出异常来模拟提交中断。"""
        size = original.size

        def pack(self, *_args):
            """在真实内存完成正文写入后拒绝头提交。"""
            raise RuntimeError("injected header commit failure")

    monkeypatch.setattr(impl, "_BLOB", BrokenHeader())
    with pytest.raises(RuntimeError, match="header commit"):
        server._create_blob(token, "request", payload, impl._crc(payload), time.monotonic() + 1)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("fault", ["header", "slow_open", "slow_copy"])
def test_external_payload_validation_rejects_corruption_and_late_success(endpoints, monkeypatch, fault):
    """映射打开成功不等于报文有效，也不能在超出原预算后返回成功。"""
    server = endpoints()
    server.start_receiving(echo, background_threads=False)
    _, value = reserve(server, payload=b"x" * 3000, external=True)
    if fault != "header":
        value = value[:4] + (time.monotonic() + 0.02,) + value[5:]
    blob = server._create_blob(value[1], "response", b"x" * 3000, value[7], time.monotonic() + 1)
    original_region, original_crc = impl.SharedRegion, impl._crc

    def slow_open(*args, **kwargs):
        """在真实映射打开后拖延，让剩余预算自然耗尽。"""
        region = original_region(*args, **kwargs)
        time.sleep(0.03)
        return region

    def slow_crc(raw):
        """模拟大报文复制及校验耗时，保持真实校验结果。"""
        result = original_crc(raw)
        time.sleep(0.03)
        return result

    try:
        if fault == "header":
            blob.buffer[:1] = b"!"
        elif fault == "slow_open":
            monkeypatch.setattr(impl, "SharedRegion", slow_open)
        else:
            monkeypatch.setattr(impl, "_crc", slow_crc)
        expected = impl.SharedMemoryCorruption if fault == "header" else impl.SharedMemoryTimeout
        with pytest.raises(expected):
            server._payload(0, value, "response")
    finally:
        monkeypatch.setattr(impl, "_crc", original_crc)
        server._drop_blob(blob, value[1], "response")


def test_client_reclaims_expired_slot_before_publishing_new_request(endpoints):
    """没有策略轮询时，客户端仍能回收已过期槽，避免容量被僵尸请求占满。"""
    server, client = endpoints(rpc_slots=1), endpoints(rpc_slots=1)
    server.start_receiving(echo, background_threads=False)
    reserve(server, state=impl._REQUEST, expired=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(client.send_request, request(), 1)
        wait_for(lambda: client.status()["published"] == 1)
        assert server.drain_request_queue(1) == 1
        assert future.result()["ok"]


@pytest.mark.parametrize("fault", ["restart", "expired"])
def test_big_request_preparation_rechecks_epoch_and_budget(endpoints, monkeypatch, fault):
    """分配大报文期间换代或超时，业务请求绝不能被迟到发布。"""
    server, client = endpoints(), endpoints()
    server.start_receiving(echo, background_threads=False)
    original = client._create_blob

    def disturbed(*args, **kwargs):
        """在真实报文分配后扰动其提交前的端点或剩余预算。"""
        blob = original(*args, **kwargs)
        if fault == "restart":
            server._write_header(server._region.buffer, 1, server._pid, server._birth, uuid.uuid4().bytes)
        else:
            time.sleep(max(0, args[4] - time.monotonic()) + 0.002)
        return blob

    monkeypatch.setattr(client, "_create_blob", disturbed)
    expected = impl.SharedMemoryPeerRestarted if fault == "restart" else impl.SharedMemoryTimeout
    try:
        with pytest.raises(expected) as captured:
            client.send_request(request("x" * 3000), 0.03)
        assert not captured.value.outcome_unknown
        assert client.status()["published"] == 0
    finally:
        server._write_header(server._region.buffer, 1, server._pid, server._birth, server._epoch)


def test_reused_slot_cannot_be_read_as_another_requests_response(endpoints, monkeypatch):
    """槽在客户端等候前被清理时，必须报告待核对而非返回其他请求数据。"""
    server, client = endpoints(), endpoints()
    server.start_receiving(echo, background_threads=False)
    original = client._wait_response

    def removed(index, *args):
        """模拟外部清理完成后客户端才开始读响应的竞态。"""
        with server._locked(0.1):
            server._free_slot(index)
        return original(index, *args)

    monkeypatch.setattr(client, "_wait_response", removed)
    with pytest.raises(impl.SharedMemoryTimeout) as captured:
        client.send_request(request(), 1)
    assert captured.value.outcome_unknown
    assert client._cancel(0, uuid.uuid4().bytes)


def test_wrong_response_business_id_is_rejected(endpoints):
    """即使令牌和代次匹配，错误业务编号的响应仍必须拒绝。"""
    server, client = endpoints(), endpoints()
    server.start_receiving(lambda _request: {"ok": True, "request_id": "wrong-id"})
    with pytest.raises(impl.SharedMemoryCorruption, match="request_id"):
        client.send_request(request(), 1)


def test_response_decode_cannot_extend_request_budget(endpoints, monkeypatch):
    """序列化后解码耗时同样属于一次请求预算，不能迟到报告成功。"""
    from bigqmt_signal_trader import redis_rpc

    server, client = endpoints(), endpoints()
    server.start_receiving(echo)
    original = redis_rpc.loads_rpc_response

    def slow_decode(raw):
        """保留真实协议解码，模拟 CPU 压力造成的处理延迟。"""
        result = original(raw)
        time.sleep(0.03)
        return result

    monkeypatch.setattr(redis_rpc, "loads_rpc_response", slow_decode)
    with pytest.raises(impl.SharedMemoryTimeout, match="decode") as captured:
        client.send_request(request(), 0.02)
    assert captured.value.outcome_unknown


@pytest.mark.parametrize("closed", [False, True])
def test_receive_loop_failure_is_visible_unless_shutdown_requested(endpoints, monkeypatch, closed):
    """后台故障进入健康状态；已经开始的显式关闭不被误记为新的运行故障。"""
    server = endpoints()
    server.start_receiving(echo, background_threads=False)
    error = impl.SharedMemoryCorruption("injected receive failure")

    def failed_drain(_max_items):
        """模拟协议层无法继续读取的错误，并可同时模拟关闭竞争。"""
        if closed:
            server.stop()
        raise error

    monkeypatch.setattr(server, "drain_request_queue", failed_drain)
    server._receive_loop()
    if closed:
        assert server._error is None
    else:
        assert not server.status()["running"]
        with pytest.raises(impl.SharedMemoryCorruption) as captured:
            server.check_health()
        assert captured.value is error


def test_drain_refuses_changed_server_ownership_and_preparing_slot(endpoints):
    """后台执行只处理已提交请求；另一代服务拥有端点时必须停止领取。"""
    server = endpoints()
    server.start_receiving(echo, background_threads=False)
    reserve(server, state=impl._PREPARING)
    assert server.drain_request_queue() == 0
    server._write_header(server._region.buffer, 1, server._pid, server._birth, uuid.uuid4().bytes)
    with pytest.raises(impl.SharedMemoryPeerRestarted, match="ownership"):
        server.drain_request_queue()
    server.stop()
    assert server._resources_closed


def test_response_route_and_publication_lock_failure_do_not_mutate_slot(endpoints):
    """无效路由与响应互斥争用不触碰已领取槽，调用方可核对原结果。"""
    server = endpoints()
    server.start_receiving(echo, background_threads=False)
    value, slot = reserve(server)
    bad = dict(value, _shm_route=dict(value["_shm_route"], index=-1))
    assert not server.send_response(bad, {"ok": True})
    with server._response_locks[0]:
        with pytest.raises(TransportTimeout, match="publication lock"):
            server.send_response(value, {"ok": True})
    assert server._read_slot(0) == slot


def test_expiry_during_large_response_allocation_releases_unpublished_blob(endpoints, monkeypatch, tmp_path):
    """大响应分配后请求已被取消，二次核对必须释放未发布报文。"""
    server = endpoints()
    server.start_receiving(echo, background_threads=False)
    value, _ = reserve(server)
    original = server._create_blob

    def canceled(*args, **kwargs):
        """客户端在大报文创建期间取消，保持真实 blob 创建和释放。"""
        blob = original(*args, **kwargs)
        server._free_slot(0)
        return blob

    monkeypatch.setattr(server, "_create_blob", canceled)
    before = set(tmp_path.iterdir())
    assert not server.send_response(value, {"ok": True, "data": "x" * 3000})
    assert not server._response_blobs
    assert set(tmp_path.iterdir()) == before


def test_response_commit_survives_reply_hint_creation_failure(endpoints, monkeypatch):
    """服务端回复提示创建失败不能丢弃已经写入共享区的响应。"""
    server, client = endpoints(), endpoints()
    server.start_receiving(echo)
    monkeypatch.setattr(server, "_reply_hint", lambda _index: None)
    assert client.send_request(request(), 1)["ok"]
    assert server.status()["responses"] == 1


def test_slow_large_response_does_not_block_another_slots_response(endpoints, monkeypatch):
    """一个槽大报文写入被暂停时，另一槽仍可在自身预算内提交完整响应。"""
    server = endpoints()
    server.start_receiving(echo, background_threads=False)
    first, first_slot = reserve(server, index=0)
    second, _ = reserve(server, index=1)
    entered, release = threading.Event(), threading.Event()
    original = server._create_blob

    def delayed(token, *args, **kwargs):
        """只暂停首槽的真实报文分配，制造跨槽并发完成顺序不同的场景。"""
        if token == first_slot[1]:
            entered.set()
            assert release.wait(2)
        return original(token, *args, **kwargs)

    monkeypatch.setattr(server, "_create_blob", delayed)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(server.send_response, first, {"ok": True, "data": "A" * 3000})
        try:
            assert entered.wait(1)
            assert server.send_response(second, {"ok": True, "data": "B" * 3000})
            with server._locked(0.1):
                assert b"B" * 3000 in server._payload(1, server._read_slot(1), "response")
        finally:
            release.set()
        assert pending.result(1)
    with server._locked(0.1):
        assert b"A" * 3000 in server._payload(0, server._read_slot(0), "response")
    assert server.status()["responses"] == 2
