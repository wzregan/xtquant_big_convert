"""批量读取只对明确签名不匹配做兼容探测，内部错误不能放大原生调用。"""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from bigqmt_signal_trader.adapters import market_bigqmt


@pytest.mark.parametrize("raw", [True, False])
def test_internal_type_error_runs_once_without_fallback(raw):
    """可绑定的函数体 TypeError 必须原样抛出，不能重复取数或切换数据接口。"""
    failing = Mock(side_effect=TypeError("internal decoder failed"))
    legacy = Mock(return_value={})
    context = SimpleNamespace(get_market_data_ex=legacy, get_market_data=legacy)
    if raw:
        context.get_market_data_ex_ori = failing
    else:
        context.get_market_data_ex = failing
    with pytest.raises(TypeError, match="internal decoder"):
        market_bigqmt.BigQmtMarketDataProvider(context).get_market_data_ex(
            stock_list=["x"], subscribe=False, fill_data=False)
    assert failing.call_count == 1 and legacy.call_count == 0


def test_python_signature_binding_skips_wrong_shapes_without_calling():
    """Python 参数预绑定会跳过字段别名错误，匹配后只执行一次。"""
    calls = []
    def native(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, subscribe):
        """仅支持 SDK 参数命名的严格签名。"""
        calls.append((stock_list, subscribe, fill_data))
        return {"x": []}
    provider = market_bigqmt.BigQmtMarketDataProvider(SimpleNamespace(get_market_data_ex=native))
    assert provider.get_market_data_ex(stock_list=["x"], subscribe=False, fill_data=False) == {"x": []}
    assert calls == [(["x"], False, False)]


@pytest.mark.parametrize("message,expected_calls", [("got an unexpected keyword argument 'fields'", 2),
                                                    ("internal decoder failed", 1)])
def test_opaque_signature_only_retries_binding_errors(monkeypatch, message, expected_calls):
    """无可反射签名的扩展仅在明确参数错误后尝试下个形式。"""
    calls = []
    def native(*args, **kwargs):
        """模拟不可反射的 C 扩展入口。"""
        calls.append((args, kwargs))
        if len(calls) == 1:
            raise TypeError(message)
        return {"x": []}
    monkeypatch.setattr(market_bigqmt.inspect, "signature", Mock(side_effect=ValueError("opaque")))
    provider = market_bigqmt.BigQmtMarketDataProvider(SimpleNamespace(get_market_data_ex=native))
    if expected_calls == 1:
        with pytest.raises(TypeError, match="internal"):
            provider.get_market_data_ex(stock_list=["x"], subscribe=False, fill_data=False)
    else:
        assert provider.get_market_data_ex(stock_list=["x"], subscribe=False, fill_data=False) == {"x": []}
    assert len(calls) == expected_calls


def test_raw_argument_mismatch_can_fallback_with_read_only_semantics():
    """旧 ori 签名不支持两个明确约束时，回退仍完整保留它们。"""
    raw = Mock()
    def old_raw(fields, stock_code):
        """不接受 fill_data 和 subscribe 的旧接口不会实际执行。"""
        raw()
    public = Mock(return_value={"x": []})
    provider = market_bigqmt.BigQmtMarketDataProvider(SimpleNamespace(get_market_data_ex_ori=old_raw, get_market_data_ex=public))
    provider.get_market_data_ex(stock_list=["x"], subscribe=False, fill_data=False)
    raw.assert_not_called()
    assert public.call_count == 1
    assert public.call_args.kwargs["subscribe"] is False and public.call_args.kwargs["fill_data"] is False


def test_no_native_market_method_is_explicitly_unsupported():
    """缺失原生方法时不能返回伪造的空数据成功。"""
    with pytest.raises(NotImplementedError, match="none"):
        market_bigqmt.BigQmtMarketDataProvider(SimpleNamespace()).get_market_data_ex(subscribe=False)


def test_raw_default_signature_mismatch_is_not_hidden():
    """默认语义下旧原始接口签名错误保持显式失败。"""
    def impossible(required_only):
        """不匹配任何受支持行情参数的原生接口。"""
        raise AssertionError("must not be invoked")
    with pytest.raises(TypeError):
        market_bigqmt.BigQmtMarketDataProvider(SimpleNamespace(get_market_data_ex_ori=impossible)).get_market_data_ex()


def test_opaque_all_argument_shapes_fail_cleanly(monkeypatch):
    """扩展的所有参数形式均不匹配时保留最后的签名错误。"""
    monkeypatch.setattr(market_bigqmt.inspect, "signature", Mock(side_effect=TypeError("opaque")))
    provider = market_bigqmt.BigQmtMarketDataProvider(SimpleNamespace(
        get_market_data_ex=Mock(side_effect=TypeError("incompatible function arguments"))))
    with pytest.raises(TypeError, match="incompatible"):
        provider.get_market_data_ex(subscribe=False, fill_data=False)
