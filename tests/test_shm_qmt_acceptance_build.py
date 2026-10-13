"""验证交付 TEST4 的编码、配置和正式原生调度/SHM 桥接路径。"""

import ast
import base64
import hashlib
import importlib.util
import json
import os
import subprocess
import sys

import pytest


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS = os.path.join(ROOT, "tools")
sys.path.insert(0, TOOLS)
SPEC = importlib.util.spec_from_file_location(
    "build_shm_qmt_acceptance", os.path.join(TOOLS, "build_shm_qmt_acceptance.py"))
BUILDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILDER)


@pytest.fixture
def built_test4(tmp_path):
    """以临时私有映射目录构建完整策略；不接触 Windows 或真实 QMT。"""
    path = tmp_path / "TEST4.py"
    manifest = BUILDER.build_acceptance(str(path), "offline-acceptance", directory=str(tmp_path))
    return path, manifest


@pytest.fixture
def built_trading_test4(tmp_path):
    """生成开放正式交易 RPC 的独立产物；测试仅注入假的原生接口。"""
    path = tmp_path / "TEST4.py"
    manifest = BUILDER.build_acceptance(str(path), "offline-acceptance", directory=str(tmp_path), mode="trading")
    return path, manifest


def test_artifact_is_ascii_gbk_and_embeds_exact_current_shm_sources(built_test4):
    """外壳同时可被 GBK/UTF-8 读取，协议源码不因编码转换发生字节变化。"""
    path, manifest = built_test4
    data = path.read_bytes()
    assert data.startswith(b"#coding:gbk\n")
    assert data.decode("ascii") == data.decode("gbk") == data.decode("utf-8")
    compile(data, str(path), "exec")
    tree = ast.parse(data.decode("ascii"), feature_version=(3, 6))
    embedded = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                    and any(isinstance(target, ast.Name) and target.id == "_EMBEDDED_SOURCES"
                            for target in node.targets))
    for relative in ("bigqmt_signal_trader/transports/backend.py", "bigqmt_signal_trader/transports/factory.py",
                     "bigqmt_signal_trader/transports/shm_primitives.py",
                     "bigqmt_signal_trader/transports/shm_transport.py",
                     "bigqmt_signal_trader/shm_push_channel.py", "bigqmt_signal_trader_strategy.py"):
        with open(os.path.join(ROOT, "src", relative), "rb") as stream:
            original = stream.read()
        assert base64.b64decode(embedded[relative]) == original
        assert manifest["embedded_sha256"][relative] == hashlib.sha256(original).hexdigest()
    assert manifest["sha256"] == hashlib.sha256(data).hexdigest()
    client = json.loads(path.with_name("TEST4.py.client.json").read_text())
    assert client["shm"]["namespace"] == "Local\\bigqmt-test4-shm-v1"
    assert client["formula_server"] == {"enabled": False}
    assert client["redis_enabled"] is False
    assert manifest["allow_history_download"] is False
    assert manifest["debug"] is False
    assert b"BIGQMT_DEBUG = False" in data


WORKER = r'''
import json
import socket
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

path, mode, sdk_root = sys.argv[1:]
sys.path.insert(0, sdk_root)
namespace = {"__name__": "offline_test4", "__file__": path,
             "passorder": Mock(side_effect=AssertionError("native order forbidden")),
             "cancel": Mock(side_effect=AssertionError("native cancel forbidden")),
             "down_history_data": Mock(return_value=True)}
with open(path, "rb") as stream:
    exec(compile(stream.read(), path, "exec"), namespace)
socket.socket = Mock(side_effect=AssertionError("network socket forbidden"))
load = namespace["_load_local_module"]
strategy = namespace["_strategy"]
redis_common = load("bigqmt_signal_trader.adapters.redis_common")
redis_common.build_redis_client = Mock(side_effect=AssertionError("Redis forbidden"))
native_calls = []
subscriptions = []
schedules = []
accounts = []
main_thread = threading.get_ident()

def get_full_tick(codes):
    """替代行情源，记录原生读取是否发生在策略主线程。"""
    native_calls.append(threading.get_ident())
    return {"510300.SH": {"lastPrice": 4.1}}

def subscribe(codes, callback):
    """保留正式订阅管理器接入回调，稍后由主线程送入合成行情。"""
    subscriptions.append(callback)
    return 1

context = SimpleNamespace(set_account=accounts.append,
                          run_time=lambda *args: schedules.append(args),
                          is_last_bar=lambda: False,
                          get_full_tick=get_full_tick,
                          subscribe_whole_quote=subscribe, unsubscribe_quote=Mock())
if mode == "missing_scheduler":
    del context.run_time
    try:
        namespace["init"](context)
    except RuntimeError as exc:
        assert "run_time" in str(exc)
    else:
        raise AssertionError("missing scheduler must not report INIT_OK")
    assert strategy._rpc_service is None
    assert strategy._quote_subscription_service is None
    print("OFFLINE_ACCEPTANCE " + json.dumps({"missing_scheduler_rejected": True}))
    sys.exit(0)

client = channel = None
worker = None
done = threading.Event()
ready = threading.Event()
received = threading.Event()
result = {}
errors = []
try:
    app = namespace["init"](context)
    service = strategy._rpc_service
    assert app is not None
    assert service.communication_backend.name == "shm"
    assert service.background_threads is False
    assert service.redis is service.listen_redis is None
    assert strategy._config["rpc"]["warm_context_data"] is False
    assert strategy._config["grpc_companion"] == {"enabled": False, "auto_start": False}
    assert strategy._config["formula_server"] == {"enabled": False}
    assert strategy._config["position_sync_enabled"] is False
    assert not any(item.name == "bigqmt-context-warmup" for item in threading.enumerate())
    assert accounts == ["offline-acceptance"]
    assert schedules[0][:2] == ("adjust", "100nMilliSecond")
    forbidden = {"submit_order", "submit_orders_batch", "cancel_order", "passorder",
                 "download_history_data", "reload_deployment", "smt_appointment",
                 "create_sector", "create_sector_folder", "add_stock_to_sector",
                 "remove_stock_from_sector", "reset_sector_stock_list", "add_sector", "remove_sector",
                 "download_financial_data", "submit_download_history_data2"}
    if mode == "history_download":
        forbidden.remove("download_history_data")
    assert not (forbidden & service.handlers.allowed_methods)
    sdk = load("bigqmt_signal_trader.xtquant_compat")
    sdk.load_client_config = lambda: {}
    config = dict(namespace["BIGQMT_REDIS_CONFIG"])
    client = sdk.BigQmtRpcClient(account_id="offline-acceptance", transport="shm",
                                 redis_config=config, timeout_seconds=3.0)
    assert client.formula_server_config["enabled"] is False
    assert not client._formula_router().supports("get_market_data_ex")
    channel = sdk._build_client_push_channel(client, "quote")
    handler_threads = []
    original_handle = service.handlers.handle

    def handle(method, params):
        """记录正式服务业务分发线程，不能由调用 RPC 的线程直接执行。"""
        handler_threads.append(threading.get_ident())
        return original_handle(method, params)

    service.handlers.handle = handle

    def consume(topic, data):
        """记录真实共享日志收到的行情；返回后再由通道确认。"""
        result["quote"] = data
        received.set()

    def invoke():
        """经正式 SDK 查询、订阅，再验证禁止的写入口从未到达原生 API。"""
        try:
            result["ping"] = client.call("ping", use_formula=False)
            result["ticks"] = client.call("get_ticks", {"codes": ["510300.SH"]}, use_formula=False)
            if mode == "history_download":
                result["download"] = client.call("download_history_data2", {
                    "stock_list": ["510300.SH"], "period": "tick",
                    "start_time": "20261013100000", "end_time": "20261013100100"}, use_formula=False)
            sub = client.call("subscribe_whole_quote", {"codes": ["510300.SH"],
                              "client_id": "offline-quote", "sub_id": "offline-sub"}, use_formula=False)
            channel.start_subscriber([sub["topic"]], consume)
            ready.set()
            if not received.wait(3.0):
                raise AssertionError("formal push did not arrive")
            deadline = time.monotonic() + 1.0
            while channel.status()["acknowledged"] != 1 and time.monotonic() < deadline:
                time.sleep(0.001)
            assert channel.status()["acknowledged"] == 1
            try:
                client.call("submit_orders_batch", {"orders": []}, use_formula=False)
            except sdk.RpcServerRepliedError as exc:
                result["write_rejected"] = "not allowed" in str(exc)
            else:
                raise AssertionError("READ_ONLY write was accepted")
        except Exception as exc:
            errors.append(exc)
        finally:
            done.set()

    worker = threading.Thread(target=invoke)
    worker.start()
    pushed = False
    deadline = time.monotonic() + 8.0
    while not done.is_set() and time.monotonic() < deadline:
        namespace["adjust"](context)
        if ready.is_set() and not pushed:
            assert len(subscriptions) == 1
            subscriptions[0]({"510300.SH": {"lastPrice": 4.2}})
            pushed = True
        time.sleep(0.001)
    assert done.is_set()
    worker.join(1.0)
    assert not errors, repr(errors)
    assert result["ping"]["pong"] is True
    assert result["ping"]["allow_order_methods"] is False
    assert result["ping"]["communication_health"]["healthy"] is True
    assert result["quote"] == {"510300.SH": {"lastPrice": 4.2}}
    assert result["write_rejected"] is True
    assert all(item == main_thread for item in handler_threads + native_calls)
    expected_calls = 5 if mode == "history_download" else 4
    assert service._transport.status()["executed"] == expected_calls
    if mode == "history_download":
        assert result["download"] is True
        namespace["down_history_data"].assert_called_once_with(
            "510300.SH", "tick", "20261013100000", "20261013100100")
    else:
        namespace["down_history_data"].assert_not_called()
    namespace["_shm_acceptance_state"]["last_log"] -= 11.0
    namespace["adjust"](context)
    namespace["handlebar"](context)
    namespace["passorder"].assert_not_called()
    namespace["cancel"].assert_not_called()
    redis_common.build_redis_client.assert_not_called()
    socket.socket.assert_not_called()
    print("OFFLINE_ACCEPTANCE " + json.dumps({"rpc_executed": expected_calls, "push_acknowledged": 1,
          "native_main_thread": True, "write_rejected": True, "redis_calls": 0, "socket_calls": 0}))
finally:
    if channel is not None:
        channel.stop()
    if client is not None and client._transport_instance is not None:
        client._transport_instance.stop()
    namespace["stop"](context)
'''


@pytest.mark.parametrize("mode", ["formal_bridge", "missing_scheduler", "history_download"])
@pytest.mark.parametrize("debug", [False, True])
def test_built_test4_executes_real_native_init_adjust_rpc_and_ack_push(built_test4, mode, debug):
    """子进程隔离嵌入加载器，真正执行完整 init/adjust/服务/SDK/推送，而非原语探针。"""
    path, _ = built_test4
    if mode == "history_download" or debug:
        manifest = BUILDER.build_acceptance(str(path), "offline-acceptance", directory=str(path.parent),
                                           allow_history_download=mode == "history_download", debug=debug)
        assert manifest["allow_history_download"] is (mode == "history_download")
        assert manifest["debug"] is debug
    completed = subprocess.run([sys.executable, "-c", WORKER, str(path), mode, os.path.join(ROOT, "src")],
                               cwd=str(path.parent),
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20)
    output = completed.stdout.decode("utf-8", "replace")
    assert completed.returncode == 0, output
    summary = next(json.loads(line[len("OFFLINE_ACCEPTANCE "):]) for line in output.splitlines()
                   if line.startswith("OFFLINE_ACCEPTANCE "))
    if mode in ("formal_bridge", "history_download"):
        assert ("[bigqmt_debug]" in output) is debug
        if debug:
            assert '"event": "rpc_execute_begin"' in output
            assert '"event": "rpc_method_gate"' in output
        expected_calls = 5 if mode == "history_download" else 4
        assert summary["rpc_executed"] == expected_calls
        assert summary["push_acknowledged"] == 1
        assert "INIT_OK READ_ONLY rpc=shm push=shm background_threads=False" in output
        assert "HEARTBEAT READ_ONLY" in output
        assert "rpc_executed=%d" % expected_calls in output
        assert "STOP_OK" in output
    else:
        assert summary["missing_scheduler_rejected"] is True
        assert "INIT_FAILED" in output
        assert "INIT_OK" not in output


def test_builder_rejects_empty_identity_before_deployment(tmp_path):
    """缺少账号或命名空间必须提前拒绝，不能交付不确定连接目标。"""
    with pytest.raises(ValueError):
        BUILDER.build_acceptance(str(tmp_path / "TEST4.py"), "")
    with pytest.raises(ValueError):
        BUILDER.build_acceptance(str(tmp_path / "TEST4.py"), "offline", namespace="")
    with pytest.raises(ValueError):
        BUILDER.build_acceptance(str(tmp_path / "TEST4.py"), "offline", mode="implicit-live")


TRADING_WORKER = r'''
"""离线验证正式交易桥接；所有原生下单/撤单仅写进内存，绝不调用真实 QMT。"""
import json
import socket
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

path, mode, sdk_root = sys.argv[1:]
sys.path.insert(0, sdk_root)
namespace = {"__name__": "offline_trading_test4", "__file__": path}
main_thread = threading.get_ident()
native_orders = []
native_cancels = []
native_threads = []
rows = {}
deals = []
context = None

def fake_passorder(*args):
    """替代 QMT 原生报单，只构造内存委托及模拟回调，不接触账户或网络。"""
    native_threads.append(threading.get_ident())
    assert args[-1] is context
    assert args[2] == namespace["BIGQMT_ACCOUNT_ID"]
    native_orders.append(args[:-1])
    order_id = "offline-%d" % len(native_orders)
    code, exchange = str(args[3]).split(".")
    row = SimpleNamespace(m_strAccountID=args[2], m_strOrderSysID=order_id,
                          m_strInstrumentID=code, m_strExchangeID=exchange,
                          m_nOffsetFlag=48 if args[0] == 23 else 49,
                          m_nOrderStatus=50, m_nVolumeTotalOriginal=args[6], m_nVolumeTraded=0,
                          m_dLimitPrice=args[5], m_strRemark=args[9], m_strSource=args[7],
                          m_nOrderPriceType=args[4], m_strInsertDate="20261009", m_strInsertTime="10:00:00")
    rows[order_id] = row
    namespace["order_callback"](context, row)

def fake_cancel(order_id, account_id, account_type, raw_context):
    """替代 QMT 原生撤单，更新内存状态并模拟已撤回调。"""
    native_threads.append(threading.get_ident())
    assert raw_context is context
    assert account_id == namespace["BIGQMT_ACCOUNT_ID"]
    native_cancels.append((order_id, account_id, account_type))
    rows[order_id].m_nOrderStatus = 54
    namespace["order_callback"](context, rows[order_id])
    return True

def fake_detail(account_id, account_type, detail_type, strategy_name=""):
    """仿真柜台查询仅返回本测试内存记录，核验正式撤单及委托查询路径。"""
    native_threads.append(threading.get_ident())
    if detail_type == "ORDER":
        return [row for row in rows.values() if not strategy_name or row.m_strSource == strategy_name]
    if detail_type in ("DEAL", "TRADE"):
        return list(deals)
    return []

namespace.update(passorder=fake_passorder, cancel=fake_cancel, get_trade_detail_data=fake_detail)
missing_api = {"missing_passorder": "passorder", "missing_cancel": "cancel",
               "missing_query": "get_trade_detail_data"}.get(mode)
if missing_api:
    del namespace[missing_api]
with open(path, "rb") as stream:
    exec(compile(stream.read(), path, "exec"), namespace)
socket.socket = Mock(side_effect=AssertionError("network socket forbidden"))
load = namespace["_load_local_module"]
strategy = namespace["_strategy"]
redis_common = load("bigqmt_signal_trader.adapters.redis_common")
redis_common.build_redis_client = Mock(side_effect=AssertionError("Redis forbidden"))
schedules = []
context = SimpleNamespace(set_account=Mock(), run_time=lambda *args: schedules.append(args),
                          is_last_bar=lambda: True, get_full_tick=lambda codes: {},
                          subscribe_whole_quote=lambda *args, **kwargs: 1, unsubscribe_quote=Mock())
if mode == "dryrun_gateway":
    dryrun = load("bigqmt_signal_trader.adapters.order_dryrun").DryRunOrderGateway
    strategy.configure(order_gateway=dryrun())
if mode == "missing_account_binding":
    del context.set_account
if mode == "failed_account_binding":
    context.set_account = Mock(side_effect=RuntimeError("injected set_account failure"))
invalid_modes = {"missing_passorder", "missing_cancel", "missing_query", "dryrun_gateway",
                 "missing_account_binding", "failed_account_binding"}
if mode in invalid_modes:
    try:
        namespace["init"](context)
    except RuntimeError as exc:
        expected = ("native API binding" if missing_api else "not a dry-run gateway"
                    if mode == "dryrun_gateway" else "set_account")
        assert expected in str(exc), str(exc)
    else:
        raise AssertionError("TRADING_TEST must not report ready without a real native gateway")
    assert strategy._rpc_service is None
    assert strategy._quote_subscription_service is None
    assert native_orders == native_cancels == []
    print("OFFLINE_TRADING_ACCEPTANCE " + json.dumps({"invalid_gateway_rejected": True}))
    sys.exit(0)

trader = None
worker = None
done = threading.Event()
orders_placed = threading.Event()
deal_published = threading.Event()
fault_requested = threading.Event()
fault_applied = threading.Event()
result = {}
errors = []
channels = []
try:
    app = namespace["init"](context)
    service = strategy._rpc_service
    gateway_type = load("bigqmt_signal_trader.adapters.order_bigqmt").BigQmtOrderGateway
    assert app is not None and isinstance(app.order_gateway, gateway_type)
    assert service.handlers.order_gateway is app.order_gateway
    assert service.handlers.allow_order_methods is True
    methods = load("bigqmt_signal_trader.redis_rpc").ORDER_METHODS
    assert methods.issubset(service.handlers.allowed_methods)
    assert schedules[0][:2] == ("adjust", "100nMilliSecond")
    assert namespace["BIGQMT_REDIS_CONFIG"]["rpc_allow_order_methods"] is True
    assert strategy._config["exec_events"]["enabled"] is True
    for unused in range(5):
        namespace["adjust"](context)
        namespace["handlebar"](context)
    assert native_orders == native_cancels == [], "strategy startup/scheduler submitted an unsolicited order"
    sdk = load("bigqmt_signal_trader.xtquant_compat")
    sdk.load_client_config = lambda: {}
    trader = sdk.BigQmtXtTrader(account_id=namespace["BIGQMT_ACCOUNT_ID"],
                                redis_config=dict(namespace["BIGQMT_REDIS_CONFIG"]), timeout_seconds=3.0)
    callbacks = SimpleNamespace(on_stock_order=Mock(), on_stock_trade=Mock(), on_order_error=Mock(),
                                on_cancel_error=Mock(), on_disconnected=Mock(), on_account_status=Mock())
    trader.register_callback(callbacks)
    original_factory = trader._build_quote_push_channel

    def capture_channel():
        """保存正式 SDK 启动的交易订阅通道，观察实际 ACK 而不代替消费逻辑。"""
        channel = original_factory()
        channels.append(channel)
        return channel

    trader._build_quote_push_channel = capture_channel
    trader.start()
    deadline = time.monotonic() + 3.0
    while (not channels or not channels[0].status()["running"]) and time.monotonic() < deadline:
        namespace["adjust"](context)
        time.sleep(0.001)
    assert channels and channels[0].status()["running"]
    order_account = namespace["BIGQMT_ACCOUNT_ID"]

    def submit_after_transient_pressure(operation):
        """仅对明确未进入原生报单的短暂锁背压限时重试；未知结果和持久故障立即失败。"""
        deadline = time.monotonic() + 2.0
        while True:
            try:
                return operation()
            except sdk.RpcServerRepliedError as exc:
                if (exc.error_code != "EXECUTION_NOT_STARTED" or exc.outcome_unknown is not False
                        or "PushBackpressureError" not in str(exc) or time.monotonic() >= deadline):
                    raise
                # 正式发布器使用非阻塞锁；允许主线程下一次调度提交原队首，不能清除门禁。
                time.sleep(0.005)

    def invoke():
        """经正式 SDK 发送买卖、二十单批量、直通报单和撤单，全部落在假原生函数。"""
        try:
            result["ping"] = trader.client.call("ping", use_formula=False)
            result["buy"] = submit_after_transient_pressure(lambda: trader.order_stock_result(
                order_account, "510300.SH", 23, 100, 11, 4.1, "offline", "offline-buy", wait_settlement=False))
            result["sell"] = submit_after_transient_pressure(lambda: trader.order_stock_result(
                order_account, "510300.SH", 24, 100, 11, 4.2, "offline", "offline-sell", wait_settlement=False))
            batch = [{"stock_code": "510300.SH", "order_type": 23 if index % 2 == 0 else 24,
                      "order_volume": 100, "price_type": 11, "price": 4.1,
                      "strategy_name": "offline", "order_remark": "offline-batch-%d" % index}
                     for index in range(20)]
            result["batch"] = submit_after_transient_pressure(
                lambda: trader.order_stock_batch(order_account, batch, batch_id="offline-batch"))
            result["batch_repeated"] = submit_after_transient_pressure(
                lambda: trader.order_stock_batch(order_account, batch, batch_id="offline-batch"))
            result["raw"] = submit_after_transient_pressure(lambda: trader.passorder(
                23, 1101, order_account, "510300.SH", 11, 4.1, 100,
                strategy_name="offline", user_order_id="offline-raw"))
            result["cancel"] = trader.cancel_order_stock(order_account, "offline-1")
            result["orders"] = trader.query_stock_orders(order_account)
            orders_placed.set()
            if not deal_published.wait(3.0):
                raise AssertionError("fake native deal not published")
            deadline = time.monotonic() + 3.0
            while (callbacks.on_stock_order.call_count < 24 or callbacks.on_stock_trade.call_count < 1
                   or channels[0].status()["acknowledged"] < 25) and time.monotonic() < deadline:
                time.sleep(0.001)
            assert callbacks.on_stock_order.call_count == 24
            assert callbacks.on_stock_trade.call_count == 1
            assert channels[0].status()["acknowledged"] == 25
            if mode == "health_gate":
                fault_requested.set()
                if not fault_applied.wait(3.0):
                    raise AssertionError("health fault not applied")
                try:
                    trader.order_stock_result(order_account, "510300.SH", 23, 100, 11, 4.1,
                                               "offline", "offline-must-block", wait_settlement=False)
                except sdk.RpcServerRepliedError as exc:
                    assert "new orders blocked" in str(exc), str(exc)
                    assert exc.error_code == "EXECUTION_NOT_STARTED"
                    assert exc.outcome_unknown is False
                    result["health_blocked"] = True
                else:
                    raise AssertionError("push health fault bypassed the new-order gate")
                result["query_during_fault"] = trader.query_stock_orders(order_account)
                result["cancel_during_fault"] = trader.cancel_order_stock(order_account, "offline-2")
        except Exception as exc:
            errors.append(exc)
        finally:
            done.set()

    worker = threading.Thread(target=invoke)
    worker.start()
    deadline = time.monotonic() + 10.0
    trade_sent = False
    while not done.is_set() and time.monotonic() < deadline:
        namespace["adjust"](context)
        if orders_placed.is_set() and not trade_sent:
            trade_sent = True
            deal = SimpleNamespace(m_strAccountID=order_account, m_strOrderSysID="offline-2",
                                   m_strTradeID="offline-deal", m_strInstrumentID="510300", m_strExchangeID="SH",
                                   m_nOffsetFlag=49, m_nVolume=100, m_dPrice=4.2, m_dTradeAmount=420.0,
                                   m_strRemark="offline-sell", m_strSource="offline",
                                   m_strTradeDate="20261009", m_strTradeTime="10:00:01")
            deals.append(deal)
            namespace["deal_callback"](context, deal)
            deal_published.set()
        if fault_requested.is_set() and not fault_applied.is_set():
            publisher = strategy._quote_subscription_service[1]
            error_type = load("bigqmt_signal_trader.transports.base").TransportError
            publisher._channel.check_health = Mock(side_effect=error_type("injected offline channel fault"))
            namespace["adjust"](context)
            assert publisher.status()["new_orders_blocked"] is True
            fault_applied.set()
        time.sleep(0.001)
    assert done.is_set(), "formal SDK trading workflow did not finish within deadline"
    worker.join(1.0)
    assert not errors, repr(errors)
    assert result["ping"]["allow_order_methods"] is True
    assert result["buy"]["status"] == result["sell"]["status"] == "SUBMITTED"
    assert len(result["batch"]) == 20 and all(item["accepted"] for item in result["batch"])
    assert all(item["idempotent"] for item in result["batch_repeated"])
    assert result["raw"]["dry_run"] is False and result["raw"]["submitted"] is True
    assert result["cancel"] == 0
    assert len(result["orders"]) == 23
    assert len(native_orders) == 23
    assert [item[0] for item in native_orders[:2]] == [23, 24]
    assert [item[0] for item in native_orders[2:22]] == [23 if index % 2 == 0 else 24 for index in range(20)]
    assert all(item[1] == 1101 and item[6] == 100 for item in native_orders)
    assert all(identity == main_thread for identity in native_threads)
    assert len(native_cancels) == (2 if mode == "health_gate" else 1)
    if mode == "health_gate":
        assert result["health_blocked"] is True
        assert len(result["query_during_fault"]) == 23
        assert result["cancel_during_fault"] == 0
    callbacks.on_order_error.assert_not_called()
    callbacks.on_cancel_error.assert_not_called()
    redis_common.build_redis_client.assert_not_called()
    socket.socket.assert_not_called()
    namespace["_shm_acceptance_state"]["last_log"] -= 11.0
    namespace["adjust"](context)
    print("OFFLINE_TRADING_ACCEPTANCE " + json.dumps({"native_orders": len(native_orders),
          "buy": True, "sell": True, "batch_items": 20, "native_cancels": len(native_cancels),
          "native_main_thread": True, "auto_submits": 0, "dry_run": False,
          "exec_acknowledged": channels[0].status()["acknowledged"],
          "health_gate_preserved": result.get("health_blocked", False)}))
finally:
    if trader is not None:
        trader.stop()
        if trader.client._transport_instance is not None:
            trader.client._transport_instance.stop()
    namespace["stop"](context)
'''


@pytest.mark.parametrize("mode", ["formal_trading", "health_gate", "missing_passorder", "missing_cancel",
                                  "missing_query", "dryrun_gateway", "missing_account_binding",
                                  "failed_account_binding"])
def test_trading_test4_routes_sdk_orders_to_native_and_preserves_health_gate(built_trading_test4, mode):
    """真实构建文件经过 SDK/SHM/主线程网关执行假买卖和撤单，不以 dry-run 成功代替路由验证。"""
    path, manifest = built_trading_test4
    assert manifest["mode"] == "TRADING_TEST"
    assert manifest["allow_order_methods"] is True
    assert manifest["auto_submit"] is False
    completed = subprocess.run([sys.executable, "-c", TRADING_WORKER, str(path), mode,
                                os.path.join(ROOT, "src")], cwd=str(path.parent),
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=25)
    output = completed.stdout.decode("utf-8", "replace")
    assert completed.returncode == 0, output
    summary = next(json.loads(line[len("OFFLINE_TRADING_ACCEPTANCE "):]) for line in output.splitlines()
                   if line.startswith("OFFLINE_TRADING_ACCEPTANCE "))
    if mode not in ("formal_trading", "health_gate"):
        assert summary["invalid_gateway_rejected"] is True
        assert "INIT_FAILED" in output and "INIT_OK" not in output
    else:
        assert summary["native_orders"] == 23
        assert summary["batch_items"] == 20
        assert summary["exec_acknowledged"] == 25
        assert summary["auto_submits"] == 0
        assert "INIT_OK TRADING_TEST rpc=shm push=shm background_threads=False" in output
        assert "ACCOUNT_BOUND TRADING_TEST account=offline-acceptance" in output
        assert "allow_order_methods=True auto_submit=False" in output
        assert "HEARTBEAT TRADING_TEST" in output


def test_trading_cli_build_records_explicit_permissions_and_ascii_encoding(tmp_path):
    """真实 CLI --mode trading 生成一致权限、标识与编码清单；默认只读行为继续兼容。"""
    path = tmp_path / "TEST4.py"
    completed = subprocess.run([sys.executable, os.path.join(TOOLS, "build_shm_qmt_acceptance.py"),
                                "--account-id", "offline-acceptance", "--mode", "trading",
                                "--output", str(path)], stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, timeout=10)
    assert completed.returncode == 0, completed.stdout.decode("utf-8", "replace")
    source = path.read_bytes().decode("ascii")
    ast.parse(source, feature_version=(3, 6))
    assert 'BIGQMT_ACCEPTANCE_MODE = "TRADING_TEST"' in source
    manifest = json.loads(path.with_name("TEST4.py.manifest.json").read_text())
    assert manifest["mode"] == "TRADING_TEST"
    assert manifest["allow_order_methods"] is True
    assert manifest["auto_submit"] is False
