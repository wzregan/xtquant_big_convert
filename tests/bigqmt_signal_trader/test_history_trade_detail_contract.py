"""历史交易查询的五参数、日期分组及失败语义回归。"""

from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers


def make_handler(function):
    """构造只读处理器，使用显式股票账户类型，不创建任何通信或交易实例。"""
    return BigQmtRpcHandlers(
        account_id="10000001", market_data=None, position_provider=None,
        order_gateway=SimpleNamespace(account_type="STOCK"),
        qmt_api={"get_history_trade_detail_data": function},
    )


def params(**changes):
    """返回固定历史日期，调用方只覆盖当前测试涉及的字段。"""
    return {"detail_type": "ORDER", "start_date": "20261102",
            "end_date": "20261103", **changes}


@pytest.mark.parametrize("detail_type", ["POSITION", "ORDER", "DEAL", "order"])
def test_history_forwards_five_arguments_and_preserves_groups(detail_type):
    """原生参数包含账户类型，日期分组和对象字段经过JSON化后仍完整。"""
    calls = []

    def native(*args):
        """用原生明细对象模拟日期分组，记录完整调用参数。"""
        calls.append(args)
        return [("20261102", [SimpleNamespace(m_strOrderSysID="22", m_nOrderStatus=54)]),
                ("20261103", [])]

    result = make_handler(native)._handle_get_history_trade_detail_data(
        params(detail_type=detail_type))
    assert calls == [("10000001", "STOCK", detail_type.upper(), "20261102", "20261103")]
    assert result == [["20261102", [{"m_strOrderSysID": "22", "m_nOrderStatus": 54}]],
                      ["20261103", []]]


def test_history_error_is_not_an_empty_success():
    """保留原生失败，避免把缺失数据当成零成交证明。"""
    def native(*args):
        """模拟柜台历史读取失败。"""
        raise RuntimeError("history unavailable")
    with pytest.raises(RuntimeError, match="history unavailable"):
        make_handler(native)._handle_get_history_trade_detail_data(params())


def test_history_missing_binding_is_explicit():
    """未注入接口必须报不可用，不能返回空列表。"""
    with pytest.raises(NotImplementedError):
        make_handler(None)._handle_get_history_trade_detail_data(params())


@pytest.mark.parametrize("changes", [
    {"start_date": ""}, {"end_date": None}, {"start_date": 20261102},
    {"start_date": "2026-11-02"}, {"start_date": "20260230"},
    {"start_date": "20261104"}, {"detail_type": "ACCOUNT"},
])
def test_history_rejects_invalid_query(changes):
    """不向原生接口发送无效日期、反向区间或错误明细类型。"""
    def native(*args):
        """非法参数不能到达原生调用。"""
        raise AssertionError("unexpected native call")
    with pytest.raises(ValueError):
        make_handler(native)._handle_get_history_trade_detail_data(params(**changes))


@pytest.mark.parametrize("value", [None, {}, ["row"], [["day", {}]],
                                   [[True, []]], [[float("nan"), []]]])
def test_history_rejects_malformed_native_response(value):
    """结构不明的返回值不能被包装为成功的历史查询。"""
    with pytest.raises(ValueError):
        make_handler(lambda *args: value)._handle_get_history_trade_detail_data(params())


def test_history_accepts_genuinely_empty_native_result():
    """仅原生成功返回空序列时，允许结果为空。"""
    assert make_handler(lambda *args: [])._handle_get_history_trade_detail_data(params()) == []


def test_history_requires_canonical_date_names():
    """旧日期别名不能绕过正式字段校验，迁移调用方必须显式提供闭区间日期。"""
    calls = []
    handler = make_handler(lambda *args: calls.append(args))
    with pytest.raises(ValueError, match="start_date"):
        handler._handle_get_history_trade_detail_data({
            "detail_type": "ORDER", "start_time": "20261102", "end_time": "20261103",
        })
    assert calls == []


def test_history_uses_default_deal_without_datatype_alias():
    """省略正式明细类型时采用 DEAL，旧 datatype 字段不会改变原生查询类型。"""
    calls = []

    def native(*args):
        """记录默认类型经过账户类型补齐后的原生实参。"""
        calls.append(args)
        return []

    query = params(datatype="ORDER")
    query.pop("detail_type")
    assert make_handler(native)._handle_get_history_trade_detail_data(query) == []
    assert calls == [("10000001", "STOCK", "DEAL", "20261102", "20261103")]
