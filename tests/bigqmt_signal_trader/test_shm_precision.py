"""在独立解释器间逐位核对共享内存数值，区分传输损失与源数值表示。"""

import contextlib
import math
import multiprocessing
import queue
import random
import struct
import time
import uuid
from decimal import Decimal

import pytest

from bigqmt_signal_trader.shm_push_channel import SharedMemoryQuotePushChannel
from bigqmt_signal_trader.transports import TransportError
from bigqmt_signal_trader.transports.shm_primitives import cleanup
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport


def _assert_exact(expected, actual, path="data"):
    """递归核对类型、整数及 binary64 位模式，不用容差隐藏舍入或负零丢失。"""
    assert type(actual) is type(expected), (path, type(expected), type(actual))
    if isinstance(expected, float):
        assert struct.pack(">d", actual) == struct.pack(">d", expected), path
    elif isinstance(expected, dict):
        assert actual.keys() == expected.keys(), path
        for key in expected:
            _assert_exact(expected[key], actual[key], "%s.%s" % (path, key))
    elif isinstance(expected, list):
        assert len(actual) == len(expected), path
        for index, (left, right) in enumerate(zip(expected, actual)):
            _assert_exact(left, right, "%s[%d]" % (path, index))
    else:
        assert actual == expected, path


def _finite_binary64_values():
    """固定种子覆盖全指数范围，并显式加入正负零、次正规数及金融小数。"""
    bits = [
        0, 1, 2, 0x000FFFFFFFFFFFFF, 0x0010000000000000,
        0x0010000000000001, 0x3FEFFFFFFFFFFFFF, 0x3FF0000000000000,
        0x3FF0000000000001, 0x7FEFFFFFFFFFFFFF,
    ]
    values = [struct.unpack(">d", struct.pack(">Q", item | sign))[0]
              for item in bits for sign in (0, 1 << 63)]
    values.extend([0.1, 0.001, 0.000001, 2.321, 729.85, 1_000_000_000.000001])
    rng = random.Random(20261016)
    while len(values) < 4128:
        value = struct.unpack(">d", struct.pack(">Q", rng.getrandbits(64)))[0]
        if math.isfinite(value):
            values.append(value)
    return values


def _payloads():
    """构造金额微元、整数数量、纳秒时间戳与独立浮点向量的确定性报文。"""
    floats = _finite_binary64_values()
    integers = [
        -(2 ** 63), -(2 ** 53 + 1), -1, 0, 1, 100, 1000,
        729_850_000, 2_270_150_000, 2 ** 53 - 1, 2 ** 53, 2 ** 53 + 1,
        2 ** 63 - 1, 2 ** 64 - 1,
    ]
    for index in range(0, len(floats), 32):
        sequence = index // 32
        yield {
            "sequence": sequence,
            "price_binary64": floats[index:index + 32],
            "money_micro_units_and_quantity": integers,
            "timestamp_ms": 1_792_124_000_001 + sequence,
            "timestamp_ns": 1_792_124_000_000_000_001 + sequence,
            "decimal_text": ["0.1000000000000000000001", "729.850000", "-0.000000"],
            "中文键": "价格、成交回报、共享内存\x00结束😀",
            "nested": [True, False, None, {"integer": 1, "float": 1.0}],
            # 周期性超过内联槽容量，要求大映射与普通槽具有相同精度契约。
            "padding": "界" * 24000 if sequence % 16 == 0 else "",
        }


def _resource_names(rpc, push):
    """记录本端实际打开的名称，待所有进程退出后统一清理测试资源。"""
    names = {rpc._name, rpc._name + "_request", rpc._name + "_space",
             push._name, push._space_name, push._signal_name(push._identity)}
    names.update(signal.name for signal in rpc._reply_signals.values())
    names.update(push._signal_name(identity) for identity in push._signals)
    return names


def _peer_entry(config, pipe):
    """独立解释器接收 RPC 并从两条推送日志返回原值，管道只用于生命周期。"""
    rpc = SharedMemoryTransport(account_id="precision-only", **config)
    push = SharedMemoryQuotePushChannel("server", config, "precision-only")

    def handle(request):
        """用确定性回显代替 QMT，不访问账户或网络。"""
        method = request["method"]
        data = request.get("params")
        if method == "decimal_response":
            return {"ok": True, "data": Decimal("729.850000")}
        if method == "decimal_push":
            data = Decimal("729.850000")
        topic = "exec:precision" if request.get("trade_lane") else "ticks:precision"
        try:
            push.publish(topic, data, message_id=request["request_id"], timeout_seconds=3)
        except (TypeError, OverflowError) as exc:
            return {"ok": False, "error_type": type(exc).__name__}
        return {"ok": True, "data": data}

    try:
        push.start_publisher()
        rpc.start_receiving(handle)
        pipe.send("ready")
        assert pipe.recv() == "stop"
    finally:
        names = _resource_names(rpc, push)
        rpc.stop()
        push.stop()
        pipe.send(sorted(names))
        pipe.close()


@contextlib.contextmanager
def _running_peer(tmp_path, codec):
    """创建隔离命名空间；yield 客户端及收件队列，退出时关闭全部持有者。"""
    config = {
        "namespace": "precision-" + uuid.uuid4().hex,
        "directory": str(tmp_path), "push_codec": codec,
        "rpc_slots": 4, "rpc_slot_bytes": 4096,
        "wait_poll_seconds": 0.01, "connect_timeout_seconds": 5,
        "market_push_bytes": 262144, "trade_push_bytes": 262144,
        "market_push_slots": 8, "trade_push_slots": 8,
        "max_push_subscribers": 2, "push_poll_seconds": 0.005,
        "lock_timeout_seconds": 1, "subscriber_retention_seconds": 5,
    }
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=_peer_entry, args=(config, child))
    rpc = SharedMemoryTransport(account_id="precision-only", **config)
    push = SharedMemoryQuotePushChannel("client", config, "precision-only")
    received = queue.Queue()
    names = set()
    process.start()
    child.close()
    try:
        assert parent.poll(10), "精度模拟端未就绪"
        assert parent.recv() == "ready"
        push.start_subscriber(
            ["ticks:precision", "exec:precision"],
            lambda topic, data: received.put((topic, data)),
        )
        yield rpc, push, received
    finally:
        names.update(_resource_names(rpc, push))
        push.stop()
        rpc.stop()
        if process.is_alive():
            parent.send("stop")
            if parent.poll(5):
                names.update(parent.recv())
        process.join(5)
        if process.is_alive():
            process.kill()
            process.join(2)
        parent.close()
        for name in names:
            cleanup(name, directory=str(tmp_path))
        assert process.exitcode == 0, "精度模拟端未正常退出"
        assert not list(tmp_path.iterdir()), "精度测试留下了共享内存资源"


def _request(data, method="echo", trade_lane=False):
    """构造请求编号与业务值分离的协议报文。"""
    return {"schema_version": 1, "request_id": uuid.uuid4().hex,
            "method": method, "params": data, "trade_lane": trade_lane}


@pytest.mark.parametrize("codec", ["json", "msgpack"])
def test_spawn_rpc_and_both_push_lanes_preserve_numeric_bits(tmp_path, codec):
    """真正跨进程的 RPC、大映射、行情与交易推送必须逐类型逐位相同。"""
    if codec == "msgpack":
        pytest.importorskip("msgpack")
    with _running_peer(tmp_path, codec) as (rpc, push, received):
        messages = list(_payloads())
        for data in messages:
            trade_lane = bool(data["sequence"] % 2)
            response = rpc.send_request(_request(data, trade_lane=trade_lane), 5)
            assert response["ok"] is True
            _assert_exact(data, response["data"], "rpc")
            topic, delivered = received.get(timeout=5)
            assert topic == ("exec:precision" if trade_lane else "ticks:precision")
            _assert_exact(data, delivered, "push")
        deadline = time.monotonic() + 3
        while push.status()["acknowledged"] != len(messages):
            assert time.monotonic() < deadline, "精度消息未全部确认"
            time.sleep(0.005)
        assert received.empty()
        assert rpc.status()["published"] == len(messages)
        push.check_health()


@pytest.mark.parametrize("value", [Decimal("0.100000000000000000001"), object()])
def test_rpc_rejects_unsupported_request_types_before_publication(tmp_path, value):
    """RPC 请求不把未声明支持的对象偷偷转换为 float 或字符串。"""
    rpc = SharedMemoryTransport(namespace="precision-reject", directory=str(tmp_path))
    try:
        with pytest.raises(TransportError, match="request serialization failed"):
            rpc.send_request(_request(value), 0.1)
        assert rpc.status()["published"] == 0
        assert rpc._region is None
    finally:
        rpc.stop()


@pytest.mark.parametrize("codec", ["json", "msgpack"])
def test_decimal_is_not_a_supported_lossless_numeric_type(tmp_path, codec):
    """记录现有边界：JSON 响应和推送保留 Decimal 文本，msgpack 推送拒绝。"""
    if codec == "msgpack":
        pytest.importorskip("msgpack")
    with _running_peer(tmp_path, codec) as (rpc, push, received):
        response = rpc.send_request(_request(None, method="decimal_response"), 3)
        _assert_exact("729.850000", response["data"])
        response = rpc.send_request(_request(None, method="decimal_push"), 3)
        if codec == "json":
            assert response["ok"] is True
            _assert_exact("729.850000", received.get(timeout=3)[1])
        else:
            assert response["ok"] is False
            assert response["error_type"] == "TypeError"
            assert received.empty()
        # 非协议类型失败后，合法数值仍可发送，不污染下一条槽位或推送。
        accepted = {"price": 2.321, "money_micro_units": 729_850_000}
        assert rpc.send_request(_request(accepted), 3)["ok"] is True
        _assert_exact(accepted, received.get(timeout=3)[1])
        push.check_health()


@pytest.mark.parametrize("codec", ["json", "msgpack"])
def test_integer_larger_than_uint64_is_never_rounded_to_float(tmp_path, codec):
    """JSON 保留任意长整数；msgpack 超范围须明确拒绝，不经 binary64 中转。"""
    if codec == "msgpack":
        pytest.importorskip("msgpack")
    with _running_peer(tmp_path, codec) as (rpc, push, received):
        value = {"above_uint64": 2 ** 64 + 1, "below_int64": -(2 ** 63) - 1}
        response = rpc.send_request(_request(value), 3)
        if codec == "json":
            assert response["ok"] is True
            _assert_exact(value, response["data"])
            _assert_exact(value, received.get(timeout=3)[1])
        else:
            assert response["ok"] is False
            assert response["error_type"] == "OverflowError"
            assert received.empty()
        push.check_health()


@pytest.mark.parametrize("codec", ["json", "msgpack"])
def test_nonfinite_values_are_transport_values_not_valid_financial_prices(tmp_path, codec):
    """协议当前接受非有限数；这不代表上层可将其当价格或承诺 NaN 位模式。"""
    if codec == "msgpack":
        pytest.importorskip("msgpack")
    with _running_peer(tmp_path, codec) as (rpc, push, received):
        value = {"positive_infinity": float("inf"), "negative_infinity": -float("inf"),
                 "nan": struct.unpack(">d", bytes.fromhex("fff8000000000042"))[0]}
        response = rpc.send_request(_request(value), 3)
        assert response["ok"] is True
        for actual in (response["data"], received.get(timeout=3)[1]):
            _assert_exact(value["positive_infinity"], actual["positive_infinity"])
            _assert_exact(value["negative_infinity"], actual["negative_infinity"])
            assert type(actual["nan"]) is float and math.isnan(actual["nan"])
        push.check_health()


def test_decimal_source_rounding_is_distinct_from_transfer_precision():
    """说明 binary64 的 0.1 在发送前已非十进制精确值，传输不能恢复源信息。"""
    assert Decimal.from_float(0.1) != Decimal("0.1")
    assert int(float(2 ** 53 + 1)) == 2 ** 53
    assert 2 ** 53 + 1 != 2 ** 53


def test_numpy_scalar_request_requires_explicit_protocol_conversion(tmp_path):
    """第三方数值标量须由上层显式转换，不能把默认字符串化当数值兼容。"""
    numpy = pytest.importorskip("numpy")
    rpc = SharedMemoryTransport(namespace="precision-numpy", directory=str(tmp_path))
    try:
        for value in (numpy.int64(2 ** 53 + 1), numpy.float32(2.321)):
            with pytest.raises(TransportError, match="request serialization failed"):
                rpc.send_request(_request(value), 0.1)
        assert rpc.status()["published"] == 0
        assert rpc._region is None
    finally:
        rpc.stop()
