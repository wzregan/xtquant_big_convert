"""性能优化与 MiniQMT 契约的离线回归，不连接柜台或提交真实委托。"""

import json
import os
import sys
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.app import SignalTradingApp
from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
from bigqmt_signal_trader.formula_server import _market_data_params
from bigqmt_signal_trader.local_cache import LocalMarketCache
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, RedisPubSubRpcService, to_jsonable
from bigqmt_signal_trader.xtquant_compat import BigQmtRpcClient, BigQmtXtData, BigQmtXtTrader, _restore_jsonable


@pytest.fixture
def trader():
    """提供纯内存 RPC 客户端，测试结束时回收惰性工作线程。"""
    value = BigQmtXtTrader(account_id="acct")
    value.client = SimpleNamespace(account_id="acct", call=Mock(return_value=[]))
    yield value
    value.stop()


@pytest.mark.parametrize("wire_format", ["records", "columns_v1"])
@pytest.mark.parametrize("case", ["stocks", "datetime", "range", "empty", "multi", "collision", "numeric", "nanosecond", "duplicate"])
def test_dataframe_wire_preserves_index_columns_and_types(wire_format, case):
    """跨 JSON 往返后保留证券索引、字段类型和大整数精度。"""
    cases = {
        "stocks": pd.DataFrame([[1.5, 2.5]], index=pd.Index(["600000.SH"], name="stock"), columns=["20260901", "20260902"]),
        "datetime": pd.DataFrame({"close": [1.0, 2.0]}, index=pd.date_range("2026-09-01", periods=2, tz="Asia/Shanghai", name="date")),
        "range": pd.DataFrame({"volume": [1, 2]}, index=pd.RangeIndex(2, 6, 2, name="row")),
        "empty": pd.DataFrame({"volume": pd.Series([], dtype="int64")}),
        "multi": pd.DataFrame([[1, 2], [3, 4]], index=pd.MultiIndex.from_tuples([("SH", 1), ("SZ", 2)], names=["market", "code"]), columns=pd.MultiIndex.from_tuples([("close", "raw"), ("close", "front")])),
        "collision": pd.DataFrame({"time": [1, 2]}, index=pd.Index([3, 4], name="time")),
        "numeric": pd.DataFrame({"id": [2 ** 60 + 1, 2 ** 60 + 3], "price": [1.5, float("nan")], "volume": pd.Series([1, None], dtype="Int64")}),
        "nanosecond": pd.DataFrame({"at": pd.date_range("2026-09-01", periods=2, freq="ns", tz="UTC")}, index=pd.date_range("2026-09-01", periods=2, freq="ns")),
        "duplicate": pd.DataFrame([[1, 2]], index=["600000.SH"], columns=["close", "close"]),
    }
    frame = cases[case]
    encoded = to_jsonable(frame, dataframe_format=wire_format)
    restored = _restore_jsonable(json.loads(json.dumps(encoded)))
    pd.testing.assert_frame_equal(restored, frame)
    if wire_format == "records":
        assert "records" in encoded and "columns" in encoded


def test_old_dataframe_response_still_decodes():
    """旧服务端未提供索引元数据时，保持原有解析方式。"""
    encoded = {"__bigqmt_type__": "DataFrame", "columns": ["stime", "close"], "records": [["20260901", 1.0]]}
    pd.testing.assert_frame_equal(_restore_jsonable(encoded), pd.DataFrame({"stime": ["20260901"], "close": [1.0]}))


@pytest.mark.parametrize("wire_format", [None, "records", "columns_v1", "unknown"])
def test_server_only_uses_compact_format_when_requested(wire_format):
    """新服务端为旧客户端保留 records，仅在显式协商时切换格式。"""
    frame = pd.DataFrame({"close": [1.0]}, index=["600000.SH"])
    handlers = SimpleNamespace(handle=Mock(return_value={"close": frame}))
    service = RedisPubSubRpcService(None, handlers, account_id="acct", transport=SimpleNamespace(send_response=Mock()))
    request = {"account_id": "acct", "method": "get_market_data"}
    if wire_format is not None:
        request["dataframe_format"] = wire_format
    response = service.process_request(request)
    assert response["ok"]
    payload = response["data"]["close"]
    assert (payload.get("orient") == "columns_v1") == (wire_format == "columns_v1")
    pd.testing.assert_frame_equal(_restore_jsonable(payload), frame)


@pytest.mark.parametrize("has_pandas", [True, False])
@pytest.mark.parametrize("transport_name", ["redis", "zmq"])
def test_client_negotiates_only_decodable_format_and_accepts_old_server(has_pandas, transport_name):
    """轻量客户端不协商按列格式；新客户端可继续消费旧服务端响应。"""
    client = BigQmtRpcClient(account_id="acct", transport=transport_name, redis_client=object())
    client._formula_router = lambda: SimpleNamespace(supports=lambda _: False)
    old_response = {"ok": True, "data": {"__bigqmt_type__": "DataFrame", "columns": ["close"], "records": [[1.0]]}}
    # 两种后端都经过真实工厂，只替换网络收发，验证格式协商与旧响应兼容。
    transport = client._transport()
    try:
        with patch("bigqmt_signal_trader.xtquant_compat.importlib.util.find_spec", return_value=object() if has_pandas else None), patch.object(transport, "send_request", return_value=old_response) as send:
            frame = client.call("get_market_data")
        request = send.call_args[0][0]
    finally:
        transport.stop()
    assert request["dataframe_format"] == ("columns_v1" if has_pandas else "records")
    assert frame["close"].tolist() == [1.0]


@pytest.mark.parametrize("fmt", ["pkl", "parquet"])
@pytest.mark.parametrize("on_index", [False, True])
def test_cache_partial_fields_preserve_history_and_explicit_null(tmp_path, fmt, on_index):
    """只拉 close 时保留旧 open；明确提供 NaN 则必须覆盖旧 close。"""
    if fmt == "parquet":
        pytest.importorskip("pyarrow")
    cache = LocalMarketCache(str(tmp_path), fmt=fmt)
    first = pd.DataFrame({"stime": ["20260901", "20260902"], "open": [10.0, 11.0], "close": [11.0, 12.0]})
    second = pd.DataFrame({"stime": ["20260902", "20260903"], "close": [float("nan"), 13.0]})
    if on_index:
        first, second = first.set_index("stime"), second.set_index("stime")
    cache.write("X", "1d", first)
    cache.write("X", "1d", second)
    result = cache.read("X", "1d")
    assert result["open"].tolist()[:2] == [10.0, 11.0]
    assert pd.isna(result["close"].iloc[1])
    assert result["close"].iloc[2] == 13.0
    assert list(first["close"]) == [11.0, 12.0]


def test_cache_reuses_disk_read_and_invalidates_external_write(tmp_path):
    """热读不反复解码，返回对象可修改，其他实例写入后立即失效。"""
    frame = pd.DataFrame({"close": [1.0, 2.0]}, index=["20260901", "20260902"])
    writer = LocalMarketCache(str(tmp_path), fmt="pkl")
    writer.write("X", "1d", frame)
    reader = LocalMarketCache(str(tmp_path), fmt="pkl")
    with patch.object(reader, "_read_file", wraps=reader._read_file) as read_file:
        result = reader.read("X", "1d", count=1)
        result.iloc[0, 0] = 99.0
        assert reader.read("X", "1d", count=1).iloc[0, 0] == 2.0
        assert read_file.call_count == 1
        writer.write("X", "1d", pd.DataFrame({"close": [3.0]}, index=["20260902"]))
        assert reader.read("X", "1d", count=1).iloc[0, 0] == 3.0
        assert read_file.call_count == 2


def test_cache_skips_unchanged_write_and_bounds_memory(tmp_path):
    """重复数据不重写历史文件，内存副本不超过容量及条目上限。"""
    frame = pd.DataFrame({"close": [1.0]}, index=["20260901"])
    cache = LocalMarketCache(str(tmp_path), fmt="pkl", memory_max_entries=1, memory_max_bytes=1024)
    cache.write("X", "1d", frame)
    with patch.object(cache, "_write_file", wraps=cache._write_file) as write_file:
        cache.write("X", "1d", frame)
        assert write_file.call_count == 0
    cache.write("Y", "1d", frame)
    assert len(cache._frames) == 1
    assert cache._memory_bytes <= 1024
    cache = LocalMarketCache(str(tmp_path), fmt="pkl", memory_max_bytes=1)
    cache.read("X", "1d")
    assert not cache._frames


def test_native_query_callback_order_is_nonblocking(trader):
    """通过事件阻塞后台 RPC，验证调用线程先返回且回调不在调用线程执行。"""
    gate = threading.Event()
    entered = threading.Event()
    seen = []
    caller_thread = threading.get_ident()

    def query(account, cancelable_only=False):
        """模拟一个尚未返回的委托查询。"""
        entered.set()
        assert gate.wait(5)
        return (account, cancelable_only)

    trader.query_stock_orders = query
    try:
        result = trader.query_stock_orders_async("acct", lambda data: seen.append((data, threading.get_ident())), True)
        assert result is None
        assert entered.wait(1)
        assert not seen
    finally:
        gate.set()
    assert trader.wait_async_requests(2)
    assert seen[0][0] == ("acct", True)
    assert seen[0][1] != caller_thread


def test_legacy_query_parameter_order_and_missing_callback_still_execute(trader):
    """旧桥接位置参数可继续使用，无回调扩展也必须真正发送查询。"""
    trader.query_stock_orders = Mock(return_value=["order"])
    callback = Mock()
    assert trader.query_stock_orders_async("acct", True, callback) is None
    assert trader.query_stock_orders_async("acct", cancelable_only=True) > 0
    assert trader.wait_async_requests(2)
    callback.assert_called_once_with(["order"])
    assert trader.query_stock_orders.call_count == 2


@pytest.mark.parametrize("method", ["query_account_infos", "query_account_status", "query_ipo_data"])
def test_accountless_native_callback_position(trader, method):
    """原生单个 callback 参数不得被误当成账户。"""
    query = Mock(return_value=["result"])
    setattr(trader, method, query)
    callback = Mock()
    assert getattr(trader, method + "_async")(callback) is None
    assert trader.wait_async_requests(2)
    query.assert_called_once_with(None)
    callback.assert_called_once_with(["result"])


def test_callback_exception_does_not_kill_following_query(trader):
    """单个用户回调异常不会丢弃队列中后续请求。"""
    trader.query_stock_positions = Mock(return_value=["position"])
    succeeding = Mock()
    trader.query_stock_positions_async("acct", Mock(side_effect=RuntimeError("callback error")))
    trader.query_stock_positions_async("acct", succeeding)
    assert trader.wait_async_requests(2)
    succeeding.assert_called_once_with(["position"])


def test_async_cancel_returns_before_rpc_and_uses_zero_success(trader):
    """撤单入队即返回，后台成功码 0 不会被布尔转换为失败。"""
    gate = threading.Event()
    entered = threading.Event()
    trader.callback = Mock()

    def cancel(account, order_id):
        """模拟柜台撤单等待。"""
        entered.set()
        assert gate.wait(5)
        return 0

    trader.cancel_order_stock = cancel
    try:
        seq = trader.cancel_order_stock_async("another-account", "sys-1")
        assert seq > 0 and entered.wait(1)
        trader.callback.on_cancel_order_stock_async_response.assert_not_called()
    finally:
        gate.set()
    assert trader.wait_async_requests(2)
    response = trader.callback.on_cancel_order_stock_async_response.call_args[0][0]
    assert response.seq == seq and response.cancel_result == 0 and response.success
    assert response.account_id == "another-account"


@pytest.mark.parametrize("response, expected", [({"success": True}, 0), ({"success": False}, -1), ({}, -1), (0, 0), (-1, -1), (True, 0), (False, -1)])
def test_cancel_native_return_code(trader, response, expected):
    """覆盖两种柜台返回形式，并确保返回整数状态码。"""
    trader.client.call.return_value = response
    result = trader.cancel_order_stock_sysid("acct", "SH", "sys-1")
    assert type(result) is int and result == expected


def test_stop_drains_accepted_async_jobs_and_rejects_new_ones(trader):
    """关闭时已接受的任务全部完成，关闭后不会创建新的后台交易线程。"""
    query = Mock(return_value=[])
    trader.query_stock_positions = query
    for _ in range(5):
        trader.query_stock_positions_async("acct")
    trader.stop()
    assert query.call_count == 5
    assert not trader._request_worker.thread.is_alive()
    with pytest.raises(RuntimeError, match="stopped"):
        trader.query_stock_positions_async("acct")


def test_default_order_parameters_and_purchase_limit_rpc(trader):
    """原生六参数下单可调用，申购额度来自服务端而非固定空字典。"""
    trader.client.call.return_value = {"order_sys_id": "sys-1"}
    order_id = trader.order_stock("acct", "600000.SH", 23, 100, 11, 10.0)
    assert isinstance(order_id, int)
    assert str(order_id) == "sys-1"
    assert trader.client.call.call_args[0][1]["strategy_name"] == ""
    trader.client.call.return_value = {"SH": 1000}
    assert trader.query_new_purchase_limit("acct") == {"SH": 1000}
    assert trader.client.call.call_args[0][0] == "get_new_purchase_limit"


def test_market_timeout_is_transport_control():
    """行情超时交给实际 RPC 调用，不混入服务端业务参数。"""
    client = SimpleNamespace(call=Mock(return_value={}), local_cache_config={"enabled": False})
    data = BigQmtXtData(client)
    data.get_market_data_ex(["close"], ["600000.SH"], timeout_seconds=99)
    assert client.call.call_args[1]["timeout_seconds"] == 99
    assert "timeout_seconds" not in client.call.call_args[0][1]


def test_fill_data_false_survives_bigqmt_parameter_translation():
    """缺失数据填充开关不能被首选大 QMT 调用形式丢弃。"""
    context = SimpleNamespace(get_market_data_ex=Mock(return_value={}))
    provider = BigQmtMarketDataProvider(context)
    provider.get_market_data_ex(field_list=["close"], stock_list=["X"], fill_data=False)
    assert context.get_market_data_ex.call_args[1]["fill_data"] is False
    with pytest.raises(ValueError, match="fill_data=False"):
        _market_data_params({"field_list": ["close"], "stock_list": ["X"], "fill_data": False})


def test_fill_data_false_falls_back_from_legacy_raw_interface():
    """旧原始接口没有填充参数时转用标准接口，保留调用方的显式 False。"""
    def raw(fields, stock_code, period="1d", start_time="", end_time="", count=-1, dividend_type="none"):
        """模拟只支持默认填充的旧原始接口。"""
        raise AssertionError("显式 fill_data=False 不能进入旧默认路径")

    context = SimpleNamespace(get_market_data_ex_ori=raw, get_market_data_ex=Mock(return_value={}))
    BigQmtMarketDataProvider(context).get_market_data_ex(field_list=["close"], stock_list=["X"], fill_data=False)
    assert context.get_market_data_ex.call_args[1]["fill_data"] is False


def test_purchase_limit_query_runs_in_strategy_drain():
    """申购额度属于账户查询，默认监听白名单不能让它在后台直接执行。"""
    handlers = SimpleNamespace(handle=Mock(return_value={"SH": 1000}))
    service = RedisPubSubRpcService(None, handlers, account_id="acct", transport=SimpleNamespace(send_response=Mock()), process_in_listener=True, listener_methods=("*",))
    service.enqueue_payload({"account_id": "acct", "method": "get_new_purchase_limit"})
    handlers.handle.assert_not_called()
    service.drain_pending()
    handlers.handle.assert_called_once()


def test_idle_account_queries_are_throttled_but_trade_event_is_immediate():
    """空闲二十轮只发布一次快照，成交事件及到期刷新仍读取最新账户状态。"""
    positions = SimpleNamespace(get_positions=Mock(return_value={}), get_asset=Mock(return_value={}))
    sink = SimpleNamespace(publish=Mock())
    app = SignalTradingApp("acct", SimpleNamespace(fetch=Mock(return_value=[])), None, positions, None, sink, None, position_sync_interval_seconds=3)
    with patch("bigqmt_signal_trader.app.time.monotonic", return_value=100.0) as clock:
        for _ in range(20):
            app.tick()
        assert positions.get_positions.call_count == positions.get_asset.call_count == 1
        app.on_trade_event({})
        assert positions.get_positions.call_count == 2
        clock.return_value = 103.0
        app.tick()
        assert positions.get_positions.call_count == 3
    app.position_sync_enabled = False
    app.tick()
    assert sink.publish.call_count == 3


def test_position_read_failure_keeps_signal_retryable():
    """风控持仓读取失败时不认领、不确认信号，避免优化改变故障重试语义。"""
    source = SimpleNamespace(fetch=Mock(return_value=[object()]), ack=Mock())
    provider = SimpleNamespace(get_positions=Mock(side_effect=RuntimeError("account unavailable")))
    state = Mock()
    app = SignalTradingApp("acct", source, None, provider, None, None, state)
    with pytest.raises(RuntimeError, match="account unavailable"):
        app.tick()
    state.claim.assert_not_called()
    state.mark_finished.assert_not_called()
    source.ack.assert_not_called()


def test_settlement_batch_queries_once_and_retries_next_tick():
    """二十笔待确认委托每轮只查一次；本轮未出现的委托下一轮仍可关联。"""
    gateway = SimpleNamespace(query_orders=Mock(return_value=[]))
    handlers = BigQmtRpcHandlers(account_id="acct", market_data=None, position_provider=None, order_gateway=gateway)
    transport = SimpleNamespace(send_response=Mock())
    service = RedisPubSubRpcService(None, handlers, account_id="acct", transport=transport)
    settlements = []
    for i in range(20):
        item = SimpleNamespace(order_request=SimpleNamespace(account_id="acct", remark="tag-%s" % i), attempts=0, deadline=float("inf"), result=SimpleNamespace(order_sys_id=""), server_error=None, request={}, response={})
        settlements.append(item)
        service._pending_settlements.put(item)
    service.drain_pending()
    assert gateway.query_orders.call_count == 1
    assert service.pending_settlement_count() == 20
    gateway.query_orders.return_value = [SimpleNamespace(user_order_id="tag-%s" % i, order_sys_id="sys-%s" % i) for i in range(20)]
    service.drain_pending()
    assert gateway.query_orders.call_count == 2
    assert service.pending_settlement_count() == 0
    assert transport.send_response.call_count == 20
    assert [item.result.order_sys_id for item in settlements] == ["sys-%s" % i for i in range(20)]
