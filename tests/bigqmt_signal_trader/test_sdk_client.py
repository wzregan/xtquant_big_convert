"""验证标准 xtquant 入口的账户、编号、生命周期和远端执行契约。"""

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "src")
)

from bigqmt_signal_trader.adapters.order_bigqmt import BigQmtOrderGateway
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, RedisPubSubRpcService
from bigqmt_signal_trader.sdk_contract import ExecutionRejected
from bigqmt_signal_trader.sdk_order_ids import OrderIdRegistry
from bigqmt_signal_trader.sdk_trader import XtQuantTrader
from bigqmt_signal_trader.transports.base import TransportTimeout
from bigqmt_signal_trader.xtquant_compat import (
    BigQmtRpcClient,
    BigQmtXtData,
    RpcServerRepliedError,
)
from xtquant.xttype import StockAccount


@pytest.fixture
def rpc():
    """保留真实 RPC 编解码与原生网关，仅用内存传输及原生 API 替身。"""
    raw = {"m_strAccountID": "A100", "m_nBrokerType": 2, "m_strStatus": "正常"}
    query = Mock(
        side_effect=lambda account, kind, detail, strategy="": (
            [raw] if detail == "ACCOUNT" else []
        )
    )
    gateway = BigQmtOrderGateway(
        SimpleNamespace(),
        "A100",
        passorder_func=Mock(),
        get_trade_detail_data_func=query,
    )
    handlers = BigQmtRpcHandlers(
        "A100", None, None, order_gateway=gateway, allow_order_methods=True
    )
    service = RedisPubSubRpcService(
        None,
        handlers,
        account_id="A100",
        transport=SimpleNamespace(send_response=Mock()),
    )
    client = BigQmtRpcClient(account_id="A100")
    client._formula_router = lambda: None
    client._transport_instance = SimpleNamespace(
        send_request=lambda request, timeout: service.process_request(request)
    )
    return SimpleNamespace(
        client=client,
        handlers=handlers,
        gateway=gateway,
        raw=raw,
        query=query,
        service=service,
    )


@pytest.fixture
def trader(rpc, tmp_path, monkeypatch):
    """关闭后台线程调度，单独驱动生命周期，使断连测试不依赖真实等待。"""
    monkeypatch.setenv("BIGQMT_ORDER_ID_DB", str(tmp_path / "ids.sqlite3"))
    value = XtQuantTrader(account_id="A100")
    value.client = rpc.client
    value._start_health_monitor = Mock()
    value._start_event_listener = Mock()
    value.callback = Mock()
    yield value
    value.stop()


def test_sdk_import_uses_standard_contract():
    """标准 SDK 入口明确选择兼容契约，直接桥接入口仍可独立使用。"""
    from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader
    from xtquant.xttrader import XtQuantTrader as imported

    assert imported is XtQuantTrader
    assert issubclass(imported, BigQmtXtTrader)


def test_market_connection_is_real_and_idempotent():
    """重复连接不增加逐查询探测；断开时回收订阅并允许重新连接。"""
    client = SimpleNamespace(call=Mock(return_value={"pong": True}))
    data = BigQmtXtData(client)
    for _ in range(10):
        assert data.connect() is True
    assert client.call.call_count == 1
    session = Mock()
    data._quote_session = session
    data.disconnect()
    session.close.assert_called_once()
    assert data.connect() is True
    assert client.call.call_count == 2
    with pytest.raises(NotImplementedError):
        data.connect(port=58621)
    data.disconnect()
    client.call.return_value = {"pong": False}
    with pytest.raises(ConnectionError):
        data.connect()


def test_batch_details_preserve_failures_and_read_fresh_status(rpc):
    """一批只发一个 RPC，重复项去重，失败项留空，下一批立即反映复牌。"""
    state = {"600000.SH": 3, "600001.SH": -1}

    def detail(code):
        """未知代码模拟原生详情失败，其它代码读取可变状态。"""
        return {"InstrumentStatus": state[code]}

    rpc.handlers.market_data = SimpleNamespace(get_instrument=Mock(side_effect=detail))
    send = rpc.client._transport_instance.send_request
    rpc.client._transport_instance.send_request = Mock(side_effect=send)
    data = BigQmtXtData(rpc.client)
    codes = ["600000.SH", "600001.SH", "600002.SH", "600000.SH"]
    assert data.get_instrument_details(codes) == {
        "600000.SH": {"InstrumentStatus": 3},
        "600001.SH": {"InstrumentStatus": -1}, "600002.SH": None,
    }
    assert rpc.client._transport_instance.send_request.call_count == 1
    assert rpc.handlers.market_data.get_instrument.call_count == 3
    state["600000.SH"] = -1
    assert data.get_instrument_details(codes)["600000.SH"]["InstrumentStatus"] == -1


def test_batch_details_fall_back_only_for_old_server(rpc):
    """旧服务缺少批量方法时兼容回退；传输失败不能触发逐项重试风暴。"""
    rpc.handlers.allowed_methods.discard("get_instruments")
    rpc.handlers.market_data = SimpleNamespace(get_instrument=Mock(return_value={"InstrumentStatus": 3}))
    data = BigQmtXtData(rpc.client)
    assert data.get_instrument_details(["600000.SH"]) == {"600000.SH": {"InstrumentStatus": 3}}
    rpc.client._transport_instance.send_request = Mock(side_effect=TimeoutError("offline"))
    with pytest.raises(TimeoutError):
        data.get_instrument_details(["600000.SH"])
    assert rpc.client._transport_instance.send_request.call_count == 1


@pytest.mark.parametrize("codes", ["600000.SH", [None], [""], ["600000.SH"] * 501])
def test_batch_details_validate_before_native_calls(rpc, codes):
    """服务端先校验类型与批量上限，非法请求不能进入原生调用。"""
    rpc.handlers.market_data = SimpleNamespace(get_instrument=Mock())
    with pytest.raises(ValueError):
        rpc.handlers.handle("get_instruments", {"codes": codes})
    rpc.handlers.market_data.get_instrument.assert_not_called()


def test_successful_rpc_renews_connection_without_extra_ping(rpc, monkeypatch):
    """近期真实 RPC 续期一秒连接检查；过期、失败和显式断开仍要求重新探测。"""
    now = [100.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    data = BigQmtXtData(rpc.client)
    rpc.handlers.market_data = SimpleNamespace(get_instrument=Mock(return_value={"InstrumentStatus": 0}))
    assert data.connect()
    now[0] += 1.1
    data.get_instrument_detail("600000.SH")
    send = rpc.client._transport_instance.send_request
    rpc.client._transport_instance.send_request = Mock(side_effect=send)
    assert data.connect()
    rpc.client._transport_instance.send_request.assert_not_called()
    now[0] += 1.1
    assert data.connect()
    assert rpc.client._transport_instance.send_request.call_count == 1
    now[0] += 1.1
    with pytest.raises(RpcServerRepliedError):
        data._call("missing_method")
    assert rpc.client._last_rpc_success_monotonic == 0
    assert data.connect()
    assert rpc.client._transport_instance.send_request.call_count == 3


def test_invalid_ping_never_becomes_successful_connection(rpc):
    """RPC 信封成功但 pong 非真时，每次连接都必须失败，不能续期为健康。"""
    rpc.handlers._handle_ping = Mock(return_value={"pong": False})
    data = BigQmtXtData(rpc.client)
    for _ in range(2):
        with pytest.raises(ConnectionError):
            data.connect()
    assert rpc.handlers._handle_ping.call_count == 2


@pytest.mark.parametrize("method", ["subscribe_quote", "subscribe_quote2"])
def test_standard_bar_subscription_emits_records_with_milliseconds(monkeypatch, method):
    """标准入口把桥接 DataFrame 转成原生 SDK 回调形状，保留价格与时间。"""
    import pandas as pd

    from bigqmt_signal_trader import xtquant_compat
    from xtquant import xtdata

    frame = pd.DataFrame({"close": [10.5], "volume": [100]}, index=["20260904150000"])
    received = []

    def subscribe(*args):
        """模拟桥接轮询交付一帧，调用方通过最后一个参数接收回调。"""
        args[-1]({"000001.SZ": frame})
        return 1

    monkeypatch.setattr(xtquant_compat.xtdata, method, subscribe)
    assert (
        getattr(xtdata, method)("000001.SZ", period="1m", callback=received.append) == 1
    )
    row = received[0]["000001.SZ"][0]
    assert row["close"] == 10.5
    assert row["time"] == 1788505200000
    assert list(frame.columns) == ["close", "volume"]


def test_bar_subscription_detects_updates_within_same_minute():
    """同一时间标签的价格变动产生新签名；不变数据及 NaN 不重复触发。"""
    import pandas as pd

    from bigqmt_signal_trader.xtquant_compat import _BarPoller

    frame = pd.DataFrame(
        {"close": [10.0], "volume": [1], "empty": [float("nan")]},
        index=["20260904150000"],
    )
    first = _BarPoller._signature({"000001.SZ": frame})
    assert first == _BarPoller._signature({"000001.SZ": frame.copy()})
    frame.loc["20260904150000", "close"] = 10.2
    assert first != _BarPoller._signature({"000001.SZ": frame})


def test_bar_subscription_does_not_deliver_query_completed_after_stop():
    """在查询完成之前退订，返回的迟到数据不能再次进入业务回调。"""
    from bigqmt_signal_trader.xtquant_compat import _BarPoller

    received = []

    def fetch():
        """模拟原生查询尚未返回时由另一个线程退订。"""
        poller.stop()
        return {"000001.SZ": [{"time": 1, "close": 10}]}

    poller = _BarPoller(fetch, received.append, 0.2)
    poller._loop()
    assert received == []


@pytest.mark.parametrize(
    "raw_status,expected",
    [
        ("正常", 0),
        ("登录成功", 0),
        ("准备登录", 1),
        ("登录失败", 3),
        ("收盘后", 6),
        ("未识别文本", -1),
        ("", -1),
    ],
)
def test_account_state_comes_from_account_row(rpc, trader, raw_status, expected):
    """状态来自 ACCOUNT 字段，覆盖东北证券实机的登录成功文本并保留未知状态。"""
    rpc.raw["m_strStatus"] = raw_status
    assert trader.connect() == 0
    status = trader.query_account_status()[0]
    assert (status.account_id, status.account_type, status.status) == (
        "A100",
        2,
        expected,
    )
    assert trader.callback.on_account_status.call_args[0][0].status == expected
    assert all(call.args[2] == "ACCOUNT" for call in rpc.query.call_args_list)


def test_account_query_failure_does_not_become_empty(rpc, trader):
    """底层查询失败向上传播；连接失败不会发出正常账户状态。"""
    rpc.query.side_effect = RuntimeError("offline")
    with pytest.raises(RpcServerRepliedError, match="offline"):
        trader.query_account_status()
    assert trader.connect() == -1
    trader.callback.on_account_status.assert_not_called()


@pytest.mark.parametrize("timeout,expected", [(8.0, 5.0), (0.5, 0.5)])
def test_account_query_budget_allows_native_queue_and_respects_short_config(
    rpc, trader, timeout, expected
):
    """实机历史读取约占用 3.7 秒；状态查询应容纳排队，同时尊重更短的客户端预算。"""
    trader.client.timeout_seconds = timeout
    original = rpc.client._transport_instance.send_request

    def send(request, budget):
        """模拟同一 QMT 策略线程上原生历史查询先占用队列。"""
        if request["method"] == "query_account_status":
            assert budget == expected
            if budget < 3.7:
                raise TransportTimeout("native history query still running")
        return original(request, budget)

    rpc.client._transport_instance.send_request = send
    if timeout < 3.7:
        with pytest.raises(TransportTimeout):
            trader.query_account_status()
    else:
        assert trader.connect() == 0
        assert trader.query_account_status()[0].status == 0


def test_stop_waits_for_inflight_status_without_late_callbacks():
    """停止期间等待已在途状态查询归还线程，禁止提前关闭它的 socket 或发迟到回调。"""
    callback = Mock()
    value = XtQuantTrader(account_id="A100", callback=callback, timeout_seconds=8)
    entered = threading.Event()
    returned = threading.Event()

    def query(*args, **kwargs):
        """模拟超过旧版 2.5 秒停止等待上限、但仍在健康查询预算内的原生排队。"""
        entered.set()
        time.sleep(3.1)
        returned.set()
        return [
            {
                "account_id": "A100",
                "account_type": 2,
                "status": 0,
                "observed_at": time.time(),
            }
        ]

    def close_transport():
        """仅查询所有者退出后，才允许其他线程关闭传输中的 socket。"""
        assert returned.is_set()

    value.client.call = query
    value.client._transport_instance = SimpleNamespace(stop=close_transport)
    value._start_health_monitor()
    try:
        assert entered.wait(2)
        value.stop()
        assert not value._health_thread.is_alive()
        callback.on_account_status.assert_not_called()
        callback.on_disconnected.assert_not_called()
    finally:
        value._health_stop.set()
        value._health_thread.join(4)


@pytest.mark.parametrize(
    "field,value",
    [
        ("account_id", "A200"),
        ("cash", "bad"),
        ("cash", True),
        ("total_asset", float("inf")),
        ("frozen_cash", None),
        ("market_value", None),
    ],
)
def test_asset_rejects_wrong_identity_or_unknown_money(trader, field, value):
    """错误账户和不完整金额必须报错，不能被默认零值转换成可用资产。"""
    row = dict(
        account_id="A100", cash=100, frozen_cash=0, market_value=0, total_asset=100
    )
    row[field] = value
    trader.client.call = Mock(return_value=row)
    with pytest.raises(ValueError):
        trader.query_stock_asset(StockAccount("A100"))


def test_asset_preserves_explicit_zero_and_reports_rpc_failure(trader):
    """真实零资产仍可查询，后续传输失败不得复用上次结果。"""
    row = dict(account_id="A100", cash=0, frozen_cash=0, market_value=0, total_asset=0)
    trader.client.call = Mock(return_value=row)
    assert trader.query_stock_asset(StockAccount("A100")).total_asset == 0
    trader.client.call.side_effect = TimeoutError("asset query timed out")
    with pytest.raises(TimeoutError):
        trader.query_stock_asset(StockAccount("A100"))


@pytest.mark.parametrize(
    "method", ["query_stock_positions", "query_stock_orders", "query_stock_trades"]
)
def test_snapshot_query_failure_is_not_empty(rpc, trader, method):
    """RPC 快照不能把原生查询异常变成无持仓、无委托或无成交。"""
    from bigqmt_signal_trader.adapters.position_bigqmt import BigQmtPositionProvider

    rpc.handlers.position_provider = BigQmtPositionProvider(rpc.query)
    rpc.query.side_effect = RuntimeError("native query unavailable")
    with pytest.raises(RpcServerRepliedError, match="native query unavailable"):
        getattr(trader, method)(StockAccount("A100"))


def test_generation_change_and_poll_failure_disconnect(rpc, trader):
    """热重载或状态查询失败会触发断连，让调用方执行新的完整对账。"""
    assert trader.connect() == 0
    rpc.handlers._sdk_generation = "new-generation"
    trader._health_stop = SimpleNamespace(
        wait=lambda seconds: False, is_set=lambda: False, set=lambda: None
    )
    trader._health_loop()
    trader.callback.on_disconnected.assert_called_once()
    assert not trader._sdk_connected


def test_subscribe_rejects_other_account_or_type(trader):
    """同一连接不能静默换账户或把信用账户当普通账户订阅。"""
    assert trader.connect() == 0
    assert trader.subscribe(StockAccount("A100")) == 0
    assert trader.subscribe(StockAccount("A200")) == -1
    assert trader.subscribe(StockAccount("A100", "CREDIT")) == -1


def test_registry_preserves_strings_across_process_connections(tmp_path):
    """多连接并发、重建和账户隔离均使用数据库中的同一无损映射。"""
    path = str(tmp_path / "ids.sqlite3")

    def remember(_):
        """模拟多个客户端分别打开同一数据库。"""
        return OrderIdRegistry(path).remember("A100", "20260906", "001A23")

    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(remember, range(32)))
    assert len(set(ids)) == 1
    registry = OrderIdRegistry(path)
    assert registry.resolve("A100", "20260906", ids[0]) == "001A23"
    assert registry.remember("A100", "20260906", "123") != registry.remember(
        "A100", "20260906", "00123"
    )
    for account, day in [("A200", "20260906"), ("A100", "20260907")]:
        with pytest.raises(ValueError):
            registry.resolve(account, day, ids[0])


def test_order_query_trade_callback_and_cancel_use_same_integer(rpc, trader):
    """编号经过字符串格式化和重新构建客户端后，撤单仍发原始合同号。"""
    sysid = "001A23"
    order = trader._order_from_dict(
        "A100", {"order_sys_id": sysid, "order_time": time.time()}
    )
    trade = trader._trade_from_dict(
        "A100", {"order_sys_id": sysid, "traded_time": time.time()}
    )
    assert type(order.order_id) is int
    assert order.order_id == trade.order_id == trader._order_id(sysid)
    persisted = int("%s" % order.order_id)
    trader._registry_instance = None
    assert trader._resolve_order_sys_id(persisted) == sysid
    assert (
        trader._order_from_dict("A100", {"user_order_id": "remark-only"}).order_id == 0
    )


def test_missing_submission_id_is_unknown(trader):
    """passorder 没有同步合同号时不能误报失败并诱发重试。"""
    with pytest.raises(RuntimeError, match="outcome unknown"):
        trader._order_id(None)


@pytest.mark.parametrize("change", ["expired", "account", "gate"])
def test_native_boundary_rechecks_after_request_was_accepted(rpc, change):
    """模拟构造请求之后状态变化，实际 passorder 调用仍必须被拦截。"""
    native_submit = rpc.gateway.submit
    context = {"version": 1, "execute_before": time.time() + 60}

    def delayed_submit(request):
        """在服务端第一轮检查后注入排队期的变化。"""
        if change == "expired":
            context["execute_before"] = time.time() - 1
        elif change == "account":
            rpc.raw["m_strStatus"] = "登录失败"
        else:
            rpc.handlers.allow_order_methods = False
        return native_submit(request)

    rpc.gateway.submit = delayed_submit
    with pytest.raises(ExecutionRejected):
        rpc.handlers.handle(
            "order_stock",
            {
                "stock_code": "600000.SH",
                "order_type": 23,
                "order_volume": 100,
                "price": 10,
                "execution_context": context,
            },
        )
    rpc.gateway.passorder.assert_not_called()


def test_queued_expired_request_has_structured_not_started_result(rpc):
    """从 RPC 队列取出已过期请求时，返回可证明未执行的结果。"""
    request = {
        "account_id": "A100",
        "method": "order_stock",
        "params": {
            "stock_code": "600000.SH",
            "order_type": 23,
            "order_volume": 100,
            "price": 10,
            "execution_context": {"version": 1, "execute_before": time.time() - 1},
        },
    }
    response = rpc.service.process_request(request)
    assert not response["ok"]
    assert response["error_code"] == "EXECUTION_NOT_STARTED"
    rpc.gateway.passorder.assert_not_called()


def test_sdk_does_not_retry_transport_timeout(trader):
    """服务端是否执行未知时只调用一次，交由调用方对账。"""
    assert trader.connect() == 0
    trader.client.call = Mock(side_effect=TimeoutError("no reply"))
    with pytest.raises(TimeoutError):
        trader.order_stock(StockAccount("A100"), "600000.SH", 23, 100, 11, 10)
    trader.client.call.assert_called_once()


def test_user_callback_error_does_not_report_transport_failure(trader):
    """业务回调抛异常不等于桥接断连，不能触发错误的重连循环。"""
    trader.callback.on_account_status.side_effect = ValueError("callback error")
    assert trader.connect() == 0
    assert trader._sdk_connected


@pytest.mark.parametrize("kind", ["order_error", "cancel_error"])
def test_error_callback_uses_persistent_integer(trader, kind):
    """错误回调和正常委托使用相同的整数身份。"""
    order_id = trader._order_id("001A23")
    trader._dispatch_event(
        {
            "event_type": kind,
            "account_id": "A100",
            "order_sys_id": "001A23",
            "error_id": 7,
            "remark": "r",
        }
    )
    callback = (
        trader.callback.on_order_error
        if kind == "order_error"
        else trader.callback.on_cancel_error
    )
    value = callback.call_args[0][0]
    assert type(value.order_id) is int
    assert value.order_id == order_id
    assert value.order_sysid == "001A23"


def test_async_single_and_batch_share_execution_guard(rpc, trader):
    """停止后的单笔和批量队列都不能继续进入原生报单。"""
    assert trader.connect() == 0
    trader._sdk_connected = False
    with pytest.raises(RpcServerRepliedError) as captured:
        trader.order_stock_result(
            StockAccount("A100"), "600000.SH", 23, 100, 11, 10, wait_settlement=False
        )
    assert captured.value.error_code == "EXECUTION_NOT_STARTED"
    results = trader.order_stock_batch(
        StockAccount("A100"),
        [
            {
                "stock_code": "600000.SH",
                "order_type": 23,
                "order_volume": 100,
                "price": 10,
                "order_remark": "guard-test",
                "wait_settlement": False,
            }
        ],
    )
    assert not results[0]["success"]
    rpc.gateway.passorder.assert_not_called()


def test_async_response_does_not_use_remark_as_contract(trader):
    """未分配合同号的异步响应保持零编号，后续真实回调才建立映射。"""
    trader._fire_async_outcome(
        {
            "kind": "response",
            "seq": 1,
            "remark": "r",
            "order_sys_id": "",
            "user_order_id": "r",
            "stock_code": "X",
            "strategy_name": "s",
            "order_remark": "r",
            "wait_for_sysid": False,
        }
    )
    value = trader.callback.on_order_stock_async_response.call_args[0][0]
    assert value.order_id == 0
    assert value.error_msg


@pytest.mark.parametrize(
    "fees,expected",
    [
        ({}, None),
        ({"m_dCommission": 0.0}, 0.0),
        ({"m_dComssion": 0.0, "m_dCommission": 1.2}, 1.2),
    ],
)
def test_query_preserves_missing_and_zero_native_fees(rpc, trader, fees, expected):
    """从原生成交行经过服务端与 SDK 后，仍区分缺失、零费用和非零费用。"""
    row = SimpleNamespace(
        m_strAccountID="A100",
        m_strInstrumentID="600000",
        m_strExchangeID="SH",
        m_nOffsetFlag=48,
        m_nVolume=10,
        m_dPrice=10,
        m_strTradeID="T1",
        m_strOrderSysID="001A23",
        **fees,
    )
    rpc.query.side_effect = lambda account, kind, detail, strategy="": (
        [row] if detail == "DEAL" else [rpc.raw]
    )
    result = trader.query_stock_trades(StockAccount("A100"))
    assert len(result) == 1
    assert result[0].commission == expected


def test_quote_close_releases_subscriber():
    """行情断开会释放推送订阅，避免只停心跳却留下回调线程。"""
    from bigqmt_signal_trader.whole_quote_session import WholeQuoteClientSession

    channel = Mock()
    session = WholeQuoteClientSession(Mock(return_value={}), channel, "test-client")
    sub_id = session.subscribe_whole_quote(["600000.SH"])
    session.close()
    channel.stop.assert_called()
    assert not session.has_subscription(sub_id)


@pytest.mark.skipif(
    not os.environ.get("QMT_GRPC_SOURCE"),
    reason="设置 QMT_GRPC_SOURCE 后运行跨仓库验收",
)
def test_real_qmt_grpc_gateway_uses_sdk_without_bigqmt_backend(
    rpc, trader, tmp_path, monkeypatch
):
    """实际 qmt-grpc 网关经过兼容 SDK 和 RPC 服务，验证报单、查询、回调及撤单。"""
    sys.path.insert(0, os.environ["QMT_GRPC_SOURCE"])
    from datetime import datetime, timedelta, timezone

    from qmt_grpc.infrastructure.xtquant.trading_adapter import (
        XtQuantBindings,
        XtQuantTradingGateway,
    )
    from qmt_grpc.market_worker.runtime import _AttachedMiniQmtLifecycle
    from qmt_grpc.modules.trading.ports import (
        CancelDisposition,
        GatewaySubmitRequest,
        SubmitDisposition,
    )
    from qmt_grpc.shared.domain.trading import OrderSide

    from bigqmt_signal_trader import xtquant_compat
    from bigqmt_signal_trader.models import (
        AssetSnapshot,
        CancelResult,
        OrderSnapshot,
        TradeSnapshot,
    )
    from xtquant import xtconstant

    remark = "qg1234567890123456789012"
    orders = []
    deals = []
    rpc.handlers.position_provider = SimpleNamespace(
        get_asset=lambda account: AssetSnapshot(
            account_id=account,
            cash=1000,
            total_asset=1000,
            frozen_cash=0,
            market_value=0,
        ),
        get_positions=lambda account: {},
    )
    rpc.gateway.query_orders = lambda account, strategy: list(orders)
    rpc.gateway.query_orders_strict = rpc.gateway.query_orders
    rpc.gateway.query_trades = lambda account, strategy: list(deals)
    rpc.gateway.query_trades_strict = rpc.gateway.query_trades
    rpc.handlers.settle_orders_inline = True

    def passorder(*args):
        """模拟原生柜台分配合同号，保留真实服务端提交和委托查找逻辑。"""
        assert args[0] == 23
        orders.append(
            OrderSnapshot(
                "001A23",
                args[9],
                args[3],
                "BUY",
                args[6],
                0,
                50,
                price=args[5],
                strategy_name=args[7],
                remark=args[9],
                order_time=int(time.time()),
                price_type=11,
                account_type=2,
            )
        )

    rpc.gateway.passorder.side_effect = passorder

    def cancel(ref, account_id=None):
        """撤单收到的必须是最初的字符串合同号，并提供后续查询事实。"""
        assert ref.order_sys_id == "001A23"
        assert account_id == "A100"
        orders[0].status = 54
        return CancelResult(True)

    rpc.gateway.cancel = cancel
    events = []
    gateway = XtQuantTradingGateway(
        tmp_path,
        19001,
        "A100",
        connect_attempts=1,
        bindings_loader=lambda: XtQuantBindings(
            lambda path, session: trader,
            xtquant_compat.XtQuantTraderCallback,
            StockAccount,
            xtconstant,
        ),
    )
    gateway.set_event_handler(events.append)
    market = _AttachedMiniQmtLifecycle()
    monkeypatch.setattr(xtquant_compat, "xtdata", BigQmtXtData(rpc.client))
    assert market.start() is not None
    gateway.start()
    try:
        assert gateway.query_asset().cash_micros == 1_000_000_000
        assert gateway.query_positions() == ()
        assert gateway.query_account_statuses()[0].state == "OK"
        result = gateway.submit_order(
            GatewaySubmitRequest(
                remark,
                "600000.SH",
                OrderSide.BUY,
                100,
                10_000_000,
                execute_before=datetime.now(timezone.utc) + timedelta(seconds=10),
            )
        )
        assert result.disposition is SubmitDisposition.ACCEPTED
        assert gateway.query_orders()[0].gateway_order_id == result.gateway_order_id
        assert (
            gateway.find_order_by_remark(remark).gateway_order_id
            == result.gateway_order_id
        )
        deals.append(
            TradeSnapshot(
                "T1",
                "001A23",
                "600000.SH",
                "BUY",
                10,
                10,
                user_order_id=remark,
                amount=100,
                traded_time=int(time.time()),
                account_type=2,
                commission=None,
            )
        )
        fact = gateway.query_trades()[0]
        assert fact.gateway_order_id == result.gateway_order_id
        assert fact.commission_micros is None
        trader._dispatch_event(
            dict(deals[0].__dict__, event_type="trade", account_id="A100")
        )
        deadline = time.monotonic() + 2
        while not events and time.monotonic() < deadline:
            time.sleep(0.01)
        assert events[0].gateway_order_id == result.gateway_order_id
        assert (
            gateway.cancel_order(result.gateway_order_id).disposition
            is CancelDisposition.REQUESTED
        )
        assert rpc.gateway.passorder.call_count == 1
    finally:
        gateway.stop()
        market.stop()
