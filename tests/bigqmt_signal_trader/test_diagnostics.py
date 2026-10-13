"""验证面板诊断默认静默、字段有界和异常不影响桥接请求。"""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from bigqmt_signal_trader import diagnostics
from bigqmt_signal_trader.adapters.position_bigqmt import BigQmtPositionProvider
from bigqmt_signal_trader.exec_events import normalize_trade_event
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, RedisPubSubRpcService


def _records(capsys):
    """读取新增诊断行，忽略已有运行状态提示。"""
    prefix = "[bigqmt_debug] "
    return [json.loads(line[len(prefix):]) for line in capsys.readouterr().out.splitlines()
            if line.startswith(prefix)]


@pytest.fixture
def asset_provider():
    """构造金额不自洽的原生缓存行，证明日志不自动校正金额。"""
    row = SimpleNamespace(m_dAvailable=109.0, m_dBalance=120.0,
                          m_dFrozenCash=0.0, m_dInstrumentValue=20.0)
    return BigQmtPositionProvider(lambda *args: [row])


def test_disabled_debug_does_not_collect_fields_or_print(monkeypatch, capsys, asset_provider):
    """关闭时连原生诊断字段采集都不得执行，且业务金额保持原样。"""
    monkeypatch.delenv("BIGQMT_DEBUG", raising=False)
    monkeypatch.setattr(diagnostics, "native_fields", Mock(side_effect=AssertionError("forbidden")))
    diagnostics.trace("hidden", token="secret")
    assert asset_provider.get_asset("private-account").cash == 109.0
    normalize_trade_event(SimpleNamespace(m_strTradeID="t1", m_dComssion=0))
    diagnostics.native_fields.assert_not_called()
    assert _records(capsys) == []


def test_enabled_trace_is_bounded_redacted_and_uses_no_file_or_socket(monkeypatch, capsys):
    """诊断独立于文件日志；即使文件和套接字禁止，也能安全写面板。"""
    monkeypatch.setenv("BIGQMT_DEBUG", "1")
    monkeypatch.setenv("BIGQMT_LOG_ENABLED", "0")
    with monkeypatch.context() as patch:
        patch.setattr("builtins.open", Mock(side_effect=AssertionError("FileIO forbidden")))
        patch.setattr("socket.socket", Mock(side_effect=AssertionError("TCP forbidden")))
        diagnostics.trace("bounded", account_id="private-account", token="secret",
                          values=list(range(10000)), text="x" * 10000)
    record, = _records(capsys)
    assert record["account_id"] != "private-account"
    assert record["token"] == "[REDACTED]"
    assert record["values"] == list(range(8))
    assert len(record["text"]) == 160


def test_native_mapping_keeps_zero_missing_and_cache_inconsistency(monkeypatch, capsys, asset_provider):
    """记录原生空值与零值的区别，资金差额和两个佣金别名都可定位。"""
    monkeypatch.setenv("BIGQMT_DEBUG", "1")
    snapshot = asset_provider.get_asset("private-account")
    event = normalize_trade_event(SimpleNamespace(m_strTradeID="t1", m_dComssion=0,
                                                 m_dCommission=17.23))
    asset, trade = _records(capsys)
    assert asset["raw"]["m_dAvailable"] == 109.0
    assert asset["raw"]["m_dAvailableCash"] is None
    assert snapshot.cash + snapshot.frozen_cash + snapshot.market_value != snapshot.total_asset
    assert trade["raw"]["m_dComssion"] == 0
    assert trade["raw"]["m_dCommission"] == 17.23
    assert trade["commission"] == event["commission"] == 0


def test_failed_panel_output_never_changes_business_result(monkeypatch, asset_provider):
    """面板不可写时，诊断错误不得覆盖原资金查询结果。"""
    monkeypatch.setenv("BIGQMT_DEBUG", "1")
    monkeypatch.setattr("builtins.print", Mock(side_effect=OSError("panel closed")))
    assert asset_provider.get_asset("private-account").cash == 109.0


def test_request_correlation_and_gate_failure_never_leak_next_request(
    monkeypatch, capsys, asset_provider,
):
    """同一请求串起权限、原生映射和响应；下次请求不继承旧标识或完整参数。"""
    monkeypatch.setenv("BIGQMT_DEBUG", "1")
    handlers = BigQmtRpcHandlers("private-account", None, asset_provider,
                                 allowed_methods={"get_asset"})
    service = RedisPubSubRpcService(Mock(), handlers, account_id="private-account",
                                    background_threads=False)
    monkeypatch.setattr(service, "_publish_response", lambda *args: None)
    first = service.process_request({"request_id": "r1", "method": "get_asset"})
    denied = service.process_request({"request_id": "r2", "method": "download_history_data2",
                                      "params": {"token": "must-not-be-logged"}})
    assert first["ok"] is True and denied["ok"] is False
    asset_provider.get_asset("private-account")
    records = _records(capsys)
    assert next(r for r in records if r["event"] == "asset_native_mapping")["request_id"] == "r1"
    gate = next(r for r in records if r["event"] == "rpc_method_gate" and r["request_id"] == "r2")
    assert gate["allowed"] is False
    assert "request_id" not in records[-1]
    assert "must-not-be-logged" not in json.dumps(records)


@pytest.mark.parametrize("result", [None, False, True])
def test_history_route_and_native_result_preserve_contract(monkeypatch, capsys, result):
    """逐证券回退的原始返回值可见；None 成功、False 失败契约保持不变。"""
    monkeypatch.setenv("BIGQMT_DEBUG", "1")
    native = Mock(return_value=result)
    handlers = BigQmtRpcHandlers("a", None, None, qmt_api={"down_history_data": native})
    assert handlers.handle("download_history_data2", {
        "stock_list": ["510300.SH"], "period": "tick", "start_time": "20261013100000",
    }) is (result is not False)
    records = _records(capsys)
    route = next(r for r in records if r["event"] == "history_download_route")
    assert route["route"] == "native_single_fallback" and route["stock_count"] == 1
    observed = next(r for r in records if r["event"] == "history_download_native_result")
    assert observed["result"] is result
    assert observed["interpreted_success"] is (result is not False)
