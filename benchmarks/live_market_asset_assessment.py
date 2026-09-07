"""以独立候选客户端验证真实行情和资产；不部署服务、不下载、不调用交易接口。"""

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import math
import sys
import threading
import time
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path


def main():
    """从命令行读取候选源码、现有本机配置和证据目录，执行有界只读测试。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge-source", required=True)
    parser.add_argument("--qmt-source", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--soak-seconds", type=int, default=180)
    args = parser.parse_args()
    sys.path[:0] = [args.bridge_source, args.qmt_source]
    from live_assessment import Assessment, distribution

    test = Assessment(args.config, args.output, {"cases": []}, 0)
    from qmt_grpc.infrastructure.xtquant.mappers import map_asset, map_position
    from qmt_grpc.market_worker.runtime import _AttachedMiniQmtLifecycle
    from qmt_grpc.modules.market_data.domain.models import MarketPeriod

    from bigqmt_signal_trader import xtquant_compat
    from bigqmt_signal_trader.sdk_trader import XtQuantTrader
    from xtquant.xttype import StockAccount

    allowed = {
        "ping",
        "get_deployment_info",
        "query_stock_asset",
        "get_asset",
        "get_positions",
        "query_stock_positions",
        "query_account_infos",
        "get_full_tick",
        "get_market_data",
        "get_market_data_ex",
        "get_local_data",
        "get_stock_list_in_sector",
        "get_instrument_detail",
        "get_trading_dates",
        "get_trading_calendar",
        "subscribe_whole_quote",
        "unsubscribe_whole_quote",
        "quote_keepalive",
        "query_account_status",
    }
    original_call = test.client_class.call
    counts = Counter()
    lock = threading.Lock()

    def readonly_call(client, method, *positional, **keywords):
        """所有测试客户端共享白名单，阻止隐式补数、部署、报单或撤单。"""
        with lock:
            counts[method] += 1
        if method not in allowed:
            raise AssertionError("只读测试禁止 RPC: " + method)
        keywords["use_formula"] = False
        return original_call(client, method, *positional, **keywords)

    test.client_class.call = readonly_call
    report = {
        "started_at": dt.datetime.now().isoformat(),
        "phase": "preflight",
        "cases": [],
        "scope": "候选客户端连接现有 QMT；本机只读；未启动完整 gRPC 服务",
        "subscriptions": [],
    }
    output = Path(args.output)
    records = (output / "query-records.jsonl").open("w", encoding="utf-8", buffering=1)
    performance = defaultdict(list)

    def save():
        """原子保存阶段结果，中断也保留已经完成的事实。"""
        report["rpc_counts"] = dict(counts)
        temp = output / "result.tmp"
        temp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(output / "result.json")

    def record(label, action, required=True):
        """计时一次完整 SDK 查询和检查，错误脱敏后记录；required 决定是否立即停止。"""
        start = time.perf_counter()
        row = {"label": label}
        try:
            value = action()
            row.update(status="PASS", result=value)
        except Exception as error:
            row.update(status="FAIL", error=test.sanitize(type(error).__name__ + ": " + str(error)))
            if required:
                raise
        finally:
            row["ms"] = (time.perf_counter() - start) * 1000
            with lock:
                records.write(json.dumps(row, ensure_ascii=False) + "\n")
                report["cases"].append(row)
                if row.get("status") == "PASS":
                    performance[label].append(row["ms"])
        return row.get("result")

    trader = XtQuantTrader(account_id=test.account, redis_config=test.config, timeout_seconds=8)
    trader.client = test.new_client()
    account = StockAccount(test.account, "STOCK")
    xtquant_compat.xtdata = test.data_class(test.client)
    data = xtquant_compat.xtdata
    lifecycle = _AttachedMiniQmtLifecycle()
    core_codes = ("000001.SZ", "600000.SH", "510300.SH", "159919.SZ", "511880.SH")
    history_codes = ("000001.SZ", "000002.SZ")
    fields = ["open", "high", "low", "close", "volume", "amount"]
    end_date = "20260904"

    def asset():
        """验证真实资产四个金额及 qmt-grpc 微元转换，不将账户或金额写入证据。"""
        raw = trader.query_stock_asset(account)
        assert raw.account_id == test.account
        mapped = map_asset(raw)
        values = {}
        for field in ("cash", "frozen_cash", "market_value", "total_asset"):
            value = getattr(raw, field)
            assert value is not None and math.isfinite(float(value)), field
            expected = int((Decimal(str(value)) * 1000000).quantize(Decimal("1")))
            assert getattr(mapped, field + "_micros") == expected, field
            values[field] = float(value)
        residual = values["total_asset"] - sum(
            values[k] for k in ("cash", "frozen_cash", "market_value")
        )
        assert abs(residual) <= 0.02, "资产分项与总额不一致"
        return {
            "account_matches": True,
            "money_mapping_matches": True,
            "balance_residual_yuan": round(residual, 6),
            "has_market_value": values["market_value"] > 0,
        }

    def positions():
        """辅助验证持仓对象可被实际 qmt-grpc 映射，不记录证券和数量。"""
        rows = trader.query_stock_positions(account)
        mapped = [map_position(row) for row in rows]
        assert all(row.account_id == test.account for row in mapped)
        return {"count": len(mapped)}

    def ticks(codes):
        """经实际 qmt-grpc 行情网关检查逐股完整性和价格、时间字段。"""
        result = gateway.load_full_ticks(tuple(codes))
        assert not result.errors, dict(result.errors)
        assert set(result.ticks) == set(codes)
        assert all(row.last_price_micros > 0 for row in result.ticks.values())
        return {
            "count": len(result.ticks),
            "source_times": sorted({row.source_time.isoformat() for row in result.ticks.values()}),
        }

    def history(codes, period="1d", count=5):
        """检查已缓存历史的形状、有效价格及重复索引，禁止把零填充当有效数据。"""
        answer = data.get_market_data_ex(
            fields, list(codes), period=period, count=count, end_time=end_date
        )
        assert set(answer) == set(codes)
        lengths = Counter()
        for code, frame in answer.items():
            assert len(frame) == count, (code, len(frame), count)
            assert not frame.index.has_duplicates and frame.index.is_monotonic_increasing, code
            assert set(fields).issubset(frame.columns), code
            for field in fields:
                assert all(math.isfinite(float(v)) for v in frame[field]), (code, field)
            assert (frame["close"] > 0).all(), code
            assert (frame["high"] >= frame["low"]).all(), code
            lengths[len(frame)] += 1
        return {"frames": len(answer), "lengths": dict(lengths)}

    try:
        ping = test.client.call("ping")
        assert ping.get("pong") is True and ping.get("account_id") == test.account
        assert ping.get("allow_order_methods") is False
        report["server"] = {
            key: ping.get(key)
            for key in ("version", "sdk_capabilities", "generation", "allow_order_methods")
        }
        report["sources"] = {}
        for module in (
            xtquant_compat,
            sys.modules[XtQuantTrader.__module__],
            sys.modules[map_asset.__module__],
        ):
            path = Path(module.__file__)
            report["sources"][module.__name__] = {
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        report["sdk_full_connect"] = trader.connect()
        gateway = lifecycle.start()
        report["phase"] = "queries"
        record("asset", asset)
        record("positions", positions)
        completed = threading.Event()
        asynchronous_assets = []

        def asset_callback(value):
            """异步资产回调同样经过真实业务映射，只记录是否有效。"""
            asynchronous_assets.append(map_asset(value).account_id == test.account)
            completed.set()

        trader.query_stock_asset_async(account, asset_callback)
        assert completed.wait(8) and asynchronous_assets == [True]
        report["asset_async_callback"] = True
        record("ticks_5", lambda: ticks(core_codes))
        codes100 = data.get_stock_list_in_sector("沪深A股")[:100]
        record("ticks_100", lambda: ticks(codes100), required=False)
        coverage = data.get_market_data_ex(
            fields, list(core_codes), period="1d", count=5, end_time=end_date
        )
        report["daily_coverage"] = {code: len(coverage.get(code, [])) for code in core_codes}
        for period, count in [("1d", 5), ("1m", 20), ("5m", 5)]:
            record(
                "history_" + period,
                lambda p=period, n=count: history(history_codes, p, n),
                required=False,
            )
        record("history_100x250", lambda: history(codes100, count=250), required=False)
        for period in (MarketPeriod.ONE_DAY, MarketPeriod.ONE_MINUTE):

            def mapped_bars():
                """真实行情网关转换 K 线，要求每个请求证券返回五根。"""
                answer = gateway.load_bars(
                    history_codes,
                    period=period,
                    start=None,
                    end=dt.datetime(2026, 9, 4, 23, 59, tzinfo=dt.timezone(dt.timedelta(hours=8))),
                    count=5,
                    dividend_type="none",
                    fill_data=True,
                )
                assert set(answer) == set(history_codes)
                assert all(len(rows) == 5 for rows in answer.values())
                return {"frames": len(answer), "bars": sum(len(rows) for rows in answer.values())}

            record("qmt_bars_" + period.value, mapped_bars, required=False)
        save()

        report["phase"] = "subscriptions"
        raw_snapshots = []
        raw_handle = data.subscribe_whole_quote(list(core_codes[:2]), callback=raw_snapshots.append)
        try:
            assert raw_snapshots and set(raw_snapshots[0]) == set(core_codes[:2])
            report["sdk_whole_quote_snapshot"] = {"covered": sorted(raw_snapshots[0])}
        finally:
            data.unsubscribe_quote(raw_handle)
        for period in (MarketPeriod.TICK, MarketPeriod.ONE_MINUTE, MarketPeriod.ONE_DAY):
            received = []
            ready = threading.Event()
            start = time.perf_counter()

            def callback(value):
                """只保存规范化回调时间和证券，不记录完整行情快照。"""
                received.append(
                    {
                        "instrument": value.instrument,
                        "source_time": value.tick.source_time.isoformat(),
                        "elapsed_ms": (time.perf_counter() - start) * 1000,
                    }
                )
                if {row["instrument"] for row in received} == set(core_codes[:2]):
                    ready.set()

            handles = {}
            item = {"period": period.value}
            try:
                handles = gateway.subscribe_many(core_codes[:2], period, callback)
                # qmt-grpc 全推刻意忽略订阅返回前的同步首推，由随后主动快照建立基线。
                if period is MarketPeriod.TICK:
                    item["baseline"] = ticks(core_codes[:2])
                else:
                    ready.wait(8)
                time.sleep(1)
                status = (
                    "SNAPSHOT_ONLY"
                    if period is MarketPeriod.TICK and not received
                    else (
                        "PASS"
                        if {row["instrument"] for row in received} == set(core_codes[:2])
                        else "NO_DATA"
                    )
                )
                item.update(
                    status=status,
                    events=received,
                    covered=sorted({x["instrument"] for x in received}),
                )
            except Exception as error:
                item.update(status="FAIL", error=test.sanitize(error))
            finally:
                gateway.unsubscribe_many(tuple(handles.values()))
            after = len(received)
            time.sleep(0.5)
            item["callbacks_after_unsubscribe"] = len(received) - after
            report["subscriptions"].append(item)
            save()

        report["phase"] = "soak"
        soak_start = time.monotonic()
        resources = []
        import psutil

        process = psutil.Process()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            while time.monotonic() - soak_start < args.soak_seconds:
                futures = [
                    pool.submit(record, "asset", asset),
                    pool.submit(record, "ticks_5", lambda: ticks(core_codes)),
                ]
                for future in futures:
                    future.result()
                time.sleep(0.2)
                if not resources or time.monotonic() - resources[-1]["at"] > 15:
                    resources.append(
                        {
                            "at": time.monotonic(),
                            "rss": process.memory_info().rss,
                            "threads": process.num_threads(),
                            "handles": process.num_handles(),
                        }
                    )
                    save()
        report["soak"] = {"seconds": time.monotonic() - soak_start, "resources": resources}

        report["phase"] = "client_reconnect"
        for number in range(10):
            lifecycle.stop()
            gateway = lifecycle.start()
            record("reconnect_ticks", lambda: ticks(core_codes))
            transport = getattr(trader.client, "_transport_instance", None)
            if transport is not None:
                transport.stop()
                trader.client._transport_instance = None
            record("reconnect_asset", asset)
        report["phase"] = "complete"
    except Exception as error:
        report.update(phase="failed", error=test.sanitize(type(error).__name__ + ": " + str(error)))
        raise
    finally:
        lifecycle.stop()
        trader.stop()
        report["performance"] = {
            label: distribution(samples) for label, samples in performance.items()
        }
        report["finished_at"] = dt.datetime.now().isoformat()
        save()
        records.close()
        test.record_file.close()
        test.client_class.call = original_call
        print(
            json.dumps(
                {
                    "phase": report["phase"],
                    "cases": len(report["cases"]),
                    "failures": [row for row in report["cases"] if row["status"] != "PASS"],
                    "performance": report["performance"],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
