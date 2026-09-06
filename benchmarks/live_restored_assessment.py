"""恢复后的有界数据质量、封装耗时与持续运行验收；禁止真实交易。"""

import argparse
import datetime as dt
import time
import traceback

from live_assessment import Assessment, distribution, shape


def frame_quality(value):
    """汇总公开 K 线的真实价格覆盖，避免零填充被计为可用历史。"""
    result = {}
    for code, frame in value.items() if isinstance(value, dict) else [("single_frame", value)]:
        if not hasattr(frame, "columns"):
            result[str(code)] = shape(frame)
            continue
        closes = frame["close"] if "close" in frame else None
        dates = list(frame["stime"] if "stime" in frame else frame.index)
        result[str(code)] = {"rows": len(frame), "positive_closes": int((closes > 0).sum()) if closes is not None else None,
                             "zero_closes": int((closes == 0).sum()) if closes is not None else None,
                             "first_date": str(dates[0]) if dates else None, "last_date": str(dates[-1]) if dates else None}
    return result


def main():
    """config 为本机配置；output 必须为新目录；持续负载和空闲观察均以秒计。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--soak-seconds", type=int, default=300)
    parser.add_argument("--idle-seconds", type=int, default=90)
    args = parser.parse_args()
    test = Assessment(args.config, args.output, {"cases": []}, args.soak_seconds)
    report = test.report
    report["data_quality"] = []
    try:
        test.baseline()
        test.resources()
        report["client_paths"] = {"formula_enabled": test.client.formula_server_config.get("enabled"),
                                  "local_cache_enabled": test.client.local_cache_config.get("enabled")}

        def bars(label, codes, fill, count=250):
            """读取指定股票的日线并保存质量摘要，时间窗口固定。"""
            row, value = test.call("get_market_data_ex", {"stock_list": codes, "field_list": ["close"],
                                   "period": "1d", "count": count, "end_time": "20260904", "fill_data": fill},
                                   phase="quality", label=label, timeout=30)
            report["data_quality"].append({"label": label, "call": row, "quality": frame_quality(value)})
            test.save("data_quality")
            return value

        bars("before_download_fill_true", ["000002.SZ"], True)
        bars("before_download_fill_false", ["000002.SZ"], False)
        test.call("download_history_data", {"stock_code": "000002.SZ", "period": "1d", "start_time": "20260901", "end_time": "20260904"},
                  phase="quality", label="bounded_download", timeout=30)
        bars("after_download_immediate", ["000002.SZ"], False)
        time.sleep(3)
        bars("after_download_3s", ["000002.SZ"], False)

        # 对比真实的两条客户端路径，浮点近似误差不当成行情不一致。
        import numpy as np
        params = {"stock_list": ["000001.SZ"], "field_list": ["close"], "period": "1d", "count": 5, "end_time": "20260904"}
        direct = test.new_client()
        direct.formula_server_config = {"enabled": True}
        try:
            raw = test.client.call("get_market_data_ex", params, use_formula=False)
            formula = direct.call("get_market_data_ex", params, use_formula=True)
            left = raw["000001.SZ"]["close"].to_numpy(dtype=float)
            right = formula["000001.SZ"]["close"].to_numpy(dtype=float)
            report["formula_comparison"] = {"rows": len(left), "finite": bool(np.isfinite(left).all() and np.isfinite(right).all()),
                                              "allclose_1e_6": bool(np.allclose(left, right, rtol=1e-6, atol=1e-6)),
                                              "max_abs_difference": float(np.max(np.abs(left - right))),
                                              "formula_router_supports": direct._formula_router().supports("get_market_data_ex")}
        except Exception as exc:
            report["formula_comparison"] = {"error": test.sanitize(exc)}
        finally:
            direct._transport().stop()

        # 记录封装内部触发了哪些 RPC，以判断长延迟是否来自重复补数和固定等待。
        client = test.new_client()
        original = client.call
        routes = []
        def traced(method, params=None, **kwargs):
            """保留方法名和耗时；不记录账户数据或配置。"""
            start = time.perf_counter()
            try:
                return original(method, params, **kwargs)
            finally:
                routes.append({"method": method, "ms": (time.perf_counter() - start) * 1000})
        client.call = traced
        data = test.data_class(client)
        report["wrapper_latency"] = []
        try:
            for method in ("get_market_data_ex", "get_market_data", "get_local_data"):
                samples = []
                calls = []
                value = None
                for _ in range(5):
                    routes.clear()
                    start = time.perf_counter()
                    value = getattr(data, method)(**params)
                    samples.append((time.perf_counter() - start) * 1000)
                    calls.append(list(routes))
                report["wrapper_latency"].append({"method": method, "latency": distribution(samples),
                                                    "routes": calls, "result": shape(value)})
        finally:
            client._transport().stop()
        test.save("wrapper_latency")
        for method, params in (("get_asset", {}), ("get_positions", {}),
                               ("get_full_tick", {"codes": ["000001.SZ", "510300.SH"]})):
            test.benchmark("restored_" + method, method, params, 30)
        # 只有完整历史已经确认的股票进入这项性能统计。
        test.benchmark("restored_daily_valid_1x250", "get_market_data_ex", {"stock_list": ["000001.SZ"],
                       "field_list": ["open", "high", "low", "close", "volume", "amount"],
                       "period": "1d", "count": 250, "end_time": "20260904", "fill_data": False}, 30)
        test.subscriptions()
        test.asynchronous()
        test.soak()
        test.save("idle_observation")
        deadline = time.monotonic() + args.idle_seconds
        while time.monotonic() < deadline:
            test.resources()
            test.save()
            time.sleep(min(10, max(0, deadline - time.monotonic())))
        test.resources()
        test.health()
        report["finished_at"] = dt.datetime.now().isoformat()
        test.save("complete")
    except Exception:
        report["fatal_error"] = test.sanitize(traceback.format_exc())
        test.save("interrupted")
        raise
    finally:
        test.client._transport().stop()
        test.record_file.close()


if __name__ == "__main__":
    main()
