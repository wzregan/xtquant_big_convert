"""离线基准：对比指定源码目录的序列化、缓存、账户查询和异步调用开销。"""

import argparse
import inspect
import json
import platform
import socket
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace


def median_ms(action, repeats=7):
    """预热后返回多次执行耗时中位数，单位为毫秒。"""
    action()
    durations = []
    for _ in range(repeats):
        started = time.perf_counter()
        action()
        durations.append((time.perf_counter() - started) * 1000)
    return round(statistics.median(durations), 3)


def forbid_connect(*args, **kwargs):
    """阻止基准意外连接真实网络；所有 RPC 使用替身。"""
    raise AssertionError("离线基准禁止网络连接")


def main():
    """按命令行选择源码和帧格式，输出可留存的 JSON 基准结果。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", default=str(Path(__file__).resolve().parents[1] / "src"))
    parser.add_argument("--wire-format", choices=("records", "columns_v1"), default="columns_v1")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(args.source_dir).resolve()))
    socket.socket.connect = forbid_connect
    socket.create_connection = forbid_connect

    import pandas as pd
    from bigqmt_signal_trader import xtquant_compat as compat
    from bigqmt_signal_trader.app import SignalTradingApp
    from bigqmt_signal_trader.local_cache import LocalMarketCache
    from bigqmt_signal_trader.models import OrderRequest, OrderSubmitResult
    from bigqmt_signal_trader.redis_rpc import (
        BigQmtRpcHandlers, OrderSettlement, RedisPubSubRpcService,
        encode_rpc_request_payload, decode_rpc_request_payload, to_jsonable,
    )

    result = {"python": platform.python_version(), "pandas": pd.__version__, "source_dir": args.source_dir, "wire_format": args.wire_format}
    fields = ("open", "high", "low", "close", "volume", "amount")
    frame = pd.DataFrame(
        {name: [float(i % 100 + 1) for i in range(240)] for name in fields},
        index=pd.date_range("2026-09-04 09:30", periods=240, freq="min").strftime("%Y%m%d%H%M%S"),
    )
    frame.index.name = "stime"
    payload = {"%06d.SH" % (600000 + i): frame for i in range(100)}
    supports_format = "dataframe_format" in inspect.signature(to_jsonable).parameters

    def encode():
        """包含帧转换、JSON 和既有 base64 外层编码的完整序列化。"""
        data = to_jsonable(payload, dataframe_format=args.wire_format) if supports_format else to_jsonable(payload)
        return encode_rpc_request_payload({"ok": True, "data": data})

    encoded = encode()

    def decode():
        """包含外层解码、JSON 解析及 Pandas 重建。"""
        return compat._restore_jsonable(json.loads(decode_rpc_request_payload(encoded)))["data"]

    restored = decode()
    equal = all(value.equals(restored[code]) for code, value in payload.items())
    if supports_format:
        for code, value in restored.items():
            pd.testing.assert_frame_equal(value, payload[code])
    result["serialization"] = {
        "symbols": 100, "bars": 240, "fields": 6,
        "encode_ms": median_ms(encode), "decode_ms": median_ms(decode),
        "bytes": len(encoded.encode("utf-8")), "frame_roundtrip_equal": equal,
    }

    with tempfile.TemporaryDirectory(prefix="bigqmt-benchmark-") as directory:
        history = pd.DataFrame(
            {name: [float(i % 100 + 1) for i in range(20000)] for name in fields},
            index=pd.date_range("2026-01-01", periods=20000, freq="min").strftime("%Y%m%d%H%M%S"),
        )
        history.index.name = "stime"
        cache = LocalMarketCache(directory, fmt="auto")
        cache.write("X", "1m", history)
        result["cache"] = {
            "rows": 20000, "format": cache.fmt,
            "hot_read_last_row_ms": median_ms(lambda: cache.read("X", "1m", count=1)),
            "unchanged_last_row_write_ms": median_ms(lambda: cache.write("X", "1m", history.tail(1))),
        }
        changed = history.tail(1).copy()

        def update():
            """每次修改最后一根 K 线，确保实际触发落盘。"""
            changed.iloc[0, 3] += 0.01
            cache.write("X", "1m", changed)

        result["cache"]["changed_last_row_write_ms"] = median_ms(update)

    counts = {"positions": 0, "asset": 0, "publish": 0}

    def counted(name, value):
        """记录账户查询次数并返回固定数据。"""
        counts[name] += 1
        return value

    provider = SimpleNamespace(get_positions=lambda *_: counted("positions", {}), get_asset=lambda *_: counted("asset", {}))
    options = {}
    if "position_sync_interval_seconds" in inspect.signature(SignalTradingApp).parameters:
        options["position_sync_interval_seconds"] = 3.0
    app = SignalTradingApp("audit", SimpleNamespace(fetch=lambda *_: []), None, provider, None, SimpleNamespace(publish=lambda *_: counted("publish", None)), None, **options)
    for _ in range(20):
        app.tick()
    result["idle_20_ticks"] = counts

    queries = []
    gateway = SimpleNamespace(query_orders=lambda *_: queries.append(1) or [])
    handlers = BigQmtRpcHandlers(account_id="audit", market_data=None, position_provider=None, order_gateway=gateway)
    service = RedisPubSubRpcService(None, handlers, account_id="audit", transport=SimpleNamespace())
    for i in range(20):
        order = OrderRequest(str(i), "audit", "BUY", "600000.SH", 100, 10.0, "LIMIT", "audit", str(i))
        service._pending_settlements.put(OrderSettlement(order, OrderSubmitResult("SUBMITTED", str(i)), time.monotonic() + 60))
    service.drain_pending()
    result["settlement_20_orders"] = {"full_order_queries_per_drain": len(queries)}

    done = threading.Event()

    def rpc_call(method, *args, **kwargs):
        """模拟耗时 50 毫秒的柜台查询和撤单。"""
        time.sleep(0.05)
        return {"success": True} if method == "cancel_order_stock_sysid" else []

    trader = compat.BigQmtXtTrader(account_id="audit")
    trader.client = SimpleNamespace(account_id="audit", call=rpc_call)
    trader.register_callback(SimpleNamespace(on_cancel_order_stock_async_response=lambda _: done.set()))
    timings = {}
    try:
        for method in ("query", "cancel"):
            samples = []
            for _ in range(7):
                done.clear()
                started = time.perf_counter()
                if method == "query":
                    trader.query_stock_positions_async("audit", lambda _: done.set())
                else:
                    trader.cancel_order_stock_async("audit", "sys-1")
                samples.append((time.perf_counter() - started) * 1000)
                if not done.wait(2):
                    raise AssertionError("异步任务未完成")
            timings[method + "_caller_ms"] = round(statistics.median(samples), 3)
    finally:
        trader.stop()
    result["async_rpc_delay_50ms"] = timings
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
