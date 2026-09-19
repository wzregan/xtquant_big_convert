"""验证 qmt-grpc 新历史接口需要的毫秒精度、只读查询和强制刷新契约。"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from bigqmt_signal_trader import xtquant_compat as compat
from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
from xtquant import xtdata

CODE = "510300.SH"
POINT = datetime(2026, 9, 17, 13, 7, tzinfo=timezone(timedelta(hours=8)))
MILLIS = int(POINT.timestamp() * 1000)


@pytest.fixture
def bridge(monkeypatch):
    """绑定标准 SDK 到可观察的 RPC 桩，禁用磁盘缓存和外部连接。"""
    client = SimpleNamespace(
        call=Mock(), local_cache_config={"enabled": False}, full_tick_cache_config={},
    )
    data = compat.BigQmtXtData(client)
    monkeypatch.setattr(compat, "xtdata", data)
    return client, data


def test_tick_native_time_wins_over_display_stime(bridge):
    """同秒展示标签不能覆盖原生毫秒，也不能把两条记录压成一个时间点。"""
    client, _ = bridge
    frame = pd.DataFrame({
        "stime": ["20260917 13:07:00", "20260917 13:07:00"],
        "time": [MILLIS + 123, MILLIS + 987], "lastPrice": [4.1, 4.2],
    })
    client.call.return_value = {CODE: frame}
    result = xtdata.get_market_data_ex(["time", "lastPrice"], [CODE], period="tick")[CODE]
    assert result["time"].tolist() == [MILLIS + 123, MILLIS + 987]
    assert frame["stime"].tolist() == ["20260917 13:07:00"] * 2
    assert client.call.call_count == 1
    assert client.call.call_args.kwargs["use_formula"] is False
    assert client.call.call_args.args[1]["subscribe"] is False


@pytest.mark.parametrize("source", [
    "20260917 13:07:00.123", "20260917130700123", MILLIS + 123,
    POINT + timedelta(milliseconds=123),
])
def test_tick_display_only_time_keeps_milliseconds(bridge, source):
    """没有原生 time 时才解析明确的展示时间，保留其已有毫秒精度。"""
    client, _ = bridge
    client.call.return_value = {CODE: pd.DataFrame({"stime": [source], "lastPrice": [4.1]})}
    result = xtdata.get_market_data_ex(["time", "lastPrice"], [CODE], period="tick")[CODE]
    assert result["time"].tolist() == [MILLIS + 123]


def test_tick_invalid_and_unsorted_rows_are_not_hidden(bridge):
    """非法源数据必须留给 gRPC 逐项报 INVALID，不能通过过滤和排序冒充成功。"""
    client, _ = bridge
    client.call.return_value = {CODE: pd.DataFrame({
        "stime": ["20260917 13:07:00"] * 3,
        "time": [MILLIS + 987, MILLIS + 123, float("nan")],
        "open": [0, 4.1, 4.1], "lastPrice": [0, 4.2, 4.2],
    })}
    result = xtdata.get_market_data_ex([], [CODE], period="tick")[CODE]
    assert len(result) == 3
    assert result["time"].iloc[:2].tolist() == [MILLIS + 987, MILLIS + 123]
    assert pd.isna(result["time"].iloc[2])


def test_row_number_is_not_a_tick_time(bridge):
    """只有位置索引的 Tick 不能被补造成 Unix 纪元附近的合法时间。"""
    client, _ = bridge
    client.call.return_value = {CODE: pd.DataFrame({"lastPrice": [4.1]})}
    result = xtdata.get_market_data_ex([], [CODE], period="tick")[CODE]
    assert result["time"].iloc[0] is None


@pytest.mark.parametrize("source", ["invalid20260917130700", "-20260917130700",
                                    "2026-09-17T05:07:00Z"])
def test_unknown_tick_time_format_is_not_guessed(bridge, source):
    """无法确认的字符串不能靠去掉符号变成一个错误但看似合法的上海时间。"""
    client, _ = bridge
    client.call.return_value = {CODE: pd.DataFrame({"stime": [source], "lastPrice": [4.1]})}
    result = xtdata.get_market_data_ex([], [CODE], period="tick")[CODE]
    assert result["time"].iloc[0] is None


def test_standard_history_read_never_downloads_adjusted_missing_bars(bridge):
    """本地复权行情全零时只返回缺失，不运行下载自愈或公式回退。"""
    client, _ = bridge
    client.call.return_value = {CODE: pd.DataFrame({
        "stime": ["20260917 13:07:00"], "close": [0.0], "open": [0.0],
    })}
    result = xtdata.get_market_data_ex(
        ["close", "open"], [CODE], period="1m", dividend_type="front", fill_data=False,
    )
    assert result[CODE].empty
    assert client.call.call_count == 1


@pytest.mark.parametrize("result", [None, True])
def test_standard_download_preserves_flag_and_result(bridge, result):
    """单证券下载保留原包装及增量标志，不增加额外历史读取。"""
    client, _ = bridge
    client.call.return_value = result
    assert xtdata.download_history_data(
        CODE, "1m", "20260917130100", "20260917130400", incrementally=False,
    ) == {"finished": 1, "total": 1}
    client.call.assert_called_once_with("download_history_data2", {
        "stock_list": [CODE], "period": "1m", "start_time": "20260917130100",
        "end_time": "20260917130400", "incrementally": False,
    }, timeout_seconds=180.0)


def test_standard_download_false_is_not_reported_as_success(bridge):
    """终端显式失败必须让 qmt-grpc 停止补数，不能报告已完成。"""
    client, _ = bridge
    client.call.return_value = False
    with pytest.raises(RuntimeError, match="历史下载失败"):
        xtdata.download_history_data(CODE, "1m", "20260917130100", incrementally=False)


def test_forced_legacy_download_cannot_use_old_client_cache(bridge, monkeypatch):
    """直接桥接调用也须保留 False 标志，失败后不能读旧缓存来报告成功。"""
    client, data = bridge
    monkeypatch.setattr(data, "_local_cache", lambda: object())
    client.call.side_effect = RuntimeError("download failed")
    with pytest.raises(RuntimeError, match="download failed"):
        data.download_history_data(CODE, "1m", "20260917130100", incrementally=False)
    assert client.call.call_count == 1
    assert client.call.call_args.args[1]["incrementally"] is False


def test_context_query_receives_no_subscription_and_no_fill():
    """经过服务端签名适配后，本地查询的两个约束仍然存在。"""
    context = SimpleNamespace(get_market_data_ex=Mock(return_value={}))
    provider = BigQmtMarketDataProvider(context)
    provider.get_market_data_ex(stock_list=[CODE], period="tick", subscribe=False, fill_data=False)
    assert context.get_market_data_ex.call_args.kwargs["subscribe"] is False
    assert context.get_market_data_ex.call_args.kwargs["fill_data"] is False


def test_old_context_cannot_silently_drop_no_subscription():
    """终端不支持只读标志时明确失败，不能改用会自动订阅的旧 gmd。"""
    context = SimpleNamespace(
        get_market_data_ex=Mock(side_effect=TypeError("unsupported subscribe")),
        get_market_data=Mock(return_value={}),
    )
    with pytest.raises(TypeError, match="subscribe"):
        BigQmtMarketDataProvider(context).get_market_data_ex(stock_list=[CODE], subscribe=False)
    context.get_market_data.assert_not_called()
    assert all(call.kwargs.get("subscribe") is False
               for call in context.get_market_data_ex.call_args_list)


def test_rpc_download_does_not_hide_first_security_failure():
    """逐证券下载第一笔失败后整批失败，不能被第二笔成功覆盖。"""
    handler = BigQmtRpcHandlers.__new__(BigQmtRpcHandlers)
    single = Mock(side_effect=[False, True])
    handler.qmt_api = {"down_history_data": single}
    result = handler._handle_download_history_data2({
        "stock_list": [CODE, "159915.SZ"], "period": "1m",
        "start_time": "20260917130100", "incrementally": False,
    })
    assert result is False
    assert single.call_count == 1


def test_global_forced_refresh_requires_explicit_start():
    """全局函数只按日期判断增量模式，缺少刷新起点时拒绝执行。"""
    handler = BigQmtRpcHandlers.__new__(BigQmtRpcHandlers)
    download = Mock()
    handler.qmt_api = {"down_history_data": download}
    with pytest.raises(RuntimeError, match="start_time"):
        handler._handle_download_history_data({"stock_code": CODE, "incrementally": False})
    download.assert_not_called()
