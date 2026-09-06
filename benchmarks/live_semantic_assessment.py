"""补查真机 RPC 返回内容与 MiniQMT 结构约定，不提交交易或修改服务配置。"""

import argparse
import datetime as dt
import json
import time
import traceback

from live_assessment import Assessment, empty, shape


def inspect_public(value):
    """仅为公开行情、财务数据保留结构；字符串截断，绝不用于账户返回。"""
    result = shape(value)
    if isinstance(value, str):
        result["text_preview"] = value[:500]
        try:
            result["decoded_json_shape"] = shape(json.loads(value))
        except (TypeError, ValueError):
            result["is_json"] = False
    return result


def main():
    """config 是 Windows 本机配置；output 是独立补测证据目录。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    assessment = Assessment(args.config, args.output, {"cases": []}, 0)
    report = assessment.report
    report["semantic_checks"] = []
    data = assessment.data_class(assessment.client)

    def check(label, action, validator=None, public=True):
        """执行一次有界查询；validator 必须检查数据，不能仅检查非空。"""
        entry = {"label": label, "at": dt.datetime.now().isoformat()}
        started = time.perf_counter()
        value = None
        try:
            value = action()
            entry["ms"] = (time.perf_counter() - started) * 1000
            entry["result"] = inspect_public(value) if public else shape(value)
            entry["status"] = "EMPTY" if empty(value) else "RETURNED"
            if validator is not None:
                entry["validation"] = validator(value)
        except Exception as exc:
            entry.update(status="ERROR", error=assessment.sanitize(exc))
        report["semantic_checks"].append(entry)
        assessment.save("semantic_checks")
        return value

    def rpc(method, **params):
        """强制走当前 ZMQ 桥接，排除 FormulaServer 和客户端缓存。"""
        return assessment.client.call(method, params, timeout_seconds=20, use_formula=False)

    try:
        assessment.baseline()
        assessment.resources()
        report["client_paths"] = {"formula_enabled": assessment.client.formula_server_config.get("enabled"),
                                  "local_cache_enabled": assessment.client.local_cache_config.get("enabled")}
        dates = dict(stock_list=["000001.SZ"], start_time="20260101", end_time="20260904")
        for name in ("holder", "flow_holder"):
            check("top10_" + name, lambda name=name: rpc("get_top10_share_holder", data_name=name, **dates))
        check("holder_num", lambda: rpc("get_holder_num", **dates))
        check("longhubang", lambda: rpc("get_longhubang", stock_list=["000001.SZ"],
                                       start_time="20260901", end_time="20260904", count=5))
        def financial_validation(value):
            """检查股本数据非空、有限、为正及时间轴；不把返回行数当作财报数量。"""
            import numpy as np
            if not hasattr(value, "columns") or "total_capital" not in value:
                return {"has_capital_column": False}
            numbers = value["total_capital"].to_numpy(dtype=float)
            return {"has_capital_column": True, "rows": len(value),
                    "finite_values": int(np.isfinite(numbers).sum()), "positive_values": int((numbers > 0).sum()),
                    "unique_values": int(value["total_capital"].nunique()),
                    "first_index": str(value.index[0]) if len(value) else None,
                    "last_index": str(value.index[-1]) if len(value) else None}
        check("financial_Capital", lambda: rpc("get_financial_data", table_list=["Capital"], **dates), financial_validation)
        check("financial_dotted", lambda: rpc("get_financial_data", table_list=["CAPITALSTRUCTURE.total_capital"], **dates))
        check("raw_financial_dotted", lambda: rpc("get_raw_financial_data", field_list=["CAPITALSTRUCTURE.total_capital"], **dates))
        # 下载限定为一只股票的一张财务表；None 只代表调用返回，随后读取才能验证数据。
        check("download_financial_Capital", lambda: rpc("download_financial_data", table_list=["Capital"], **dates))
        check("financial_after_download", lambda: rpc("get_financial_data", table_list=["Capital"], **dates))

        params = dict(field_list=["close"], stock_list=["000001.SZ"], period="1d", count=5, end_time="20260904")
        def code_frames(value):
            """验证代码到数据帧的结构及时间索引，价格只在内存比较。"""
            return {"is_code_frame_dict": isinstance(value, dict) and all(c in value and hasattr(value[c], "columns") for c in params["stock_list"]),
                    "date_index": {c: [str(x) for x in frame.index] for c, frame in value.items() if hasattr(frame, "columns")} if isinstance(value, dict) else None}

        ex = check("wrapper_market_data_ex", lambda: data.get_market_data_ex(**params), code_frames)
        local = check("wrapper_local_data_cache_disabled", lambda: data.get_local_data(**params), code_frames)
        standard = check("wrapper_market_data", lambda: data.get_market_data(**params),
                         lambda v: {"is_field_frame_dict": isinstance(v, dict) and "close" in v and hasattr(v["close"], "columns")})
        if isinstance(ex, dict) and "000001.SZ" in ex:
            import numpy as np
            frame = ex["000001.SZ"]
            report["history_values_match"] = {
                "local": bool(hasattr(local, "columns") and "close" in local and np.allclose(local["close"], frame["close"], rtol=1e-6, atol=1e-6)),
                "market": bool(hasattr(standard, "columns") and "close" in standard and np.allclose(standard["close"], frame["close"], rtol=1e-6, atol=1e-6)),
            }
            # 这里只保留一只公开股票的五个历史收盘价，用于定位不同数据源的差异。
            report["public_close_samples"] = {"ex": list(frame["close"]),
                                               "local": list(local["close"]) if hasattr(local, "columns") else None,
                                               "market": list(standard["close"]) if hasattr(standard, "columns") else None}

        codes = rpc("get_stock_list_in_sector", sector_name="沪深A股")
        codes = [c for c in codes if c.endswith((".SH", ".SZ"))][:100]
        fields = ["open", "high", "low", "close", "volume", "amount"]
        def bulk_validation(value):
            """检查全部代码和数值单元；缺失不能被前几个样本掩盖。"""
            import numpy as np
            details = {}
            for code in codes:
                frame = value.get(code) if isinstance(value, dict) else None
                if frame is None or not hasattr(frame, "columns"):
                    details[code] = {"missing": True}
                    continue
                numeric = frame[[f for f in fields if f in frame]].to_numpy(dtype=float)
                dates_present = frame["stime"] if "stime" in frame else frame.index
                dates_text = [str(x) for x in dates_present]
                details[code] = {"rows": len(frame), "missing_fields": [f for f in fields if f not in frame],
                                 "finite_cells": int(np.isfinite(numeric).sum()), "cells": int(numeric.size),
                                 "positive_closes": int((frame["close"] > 0).sum()),
                                 "ohlc_consistent_rows": int(((frame["high"] >= frame[["open", "close", "low"]].max(axis=1)) &
                                                              (frame["low"] <= frame[["open", "close", "high"]].min(axis=1))).sum()),
                                 "unique_dates": len(set(dates_text)), "sorted_dates": dates_text == sorted(dates_text),
                                 "dates_past_end": sum(x[:8] > "20260904" for x in dates_text),
                                 "last_date": str(dates_present[-1] if not hasattr(dates_present, "iloc") else dates_present.iloc[-1]) if len(frame) else None}
            return {"requested": len(codes), "returned": len(value), "codes": details,
                    "total_rows": sum(v.get("rows", 0) for v in details.values())}
        check("bulk_daily_100x250", lambda: rpc("get_market_data_ex", stock_list=codes, field_list=fields,
                                               period="1d", count=250, end_time="20260904"), bulk_validation)
        check("wrapper_get_stock_type", lambda: data.get_stock_type("000001.SZ"))
        types = {code: rpc("get_stock_type", stock=code) for code in ("000001.SZ", "510300.SH")}
        report["raw_stock_type"] = types
        tick = rpc("get_full_tick", codes=["000001.SZ", "510300.SH"])
        report["tick_times"] = {c: {k: row[k] for k in ("time", "timetag") if k in row} for c, row in tick.items()}
        check("divid_factors_recheck", lambda: rpc("get_divid_factors", stock_code="000001.SZ", start_time="20250101", end_time="20260904"))
        check("ETF_list_recheck", lambda: rpc("get_ETF_list", market="SH", stock_code="510300.SH", type_list=[]))
        check("invalid_method_rejected", lambda: rpc("__assessment_invalid__"), public=False)
        assessment.health()
        assessment.resources()
        report["finished_at"] = dt.datetime.now().isoformat()
        assessment.save("complete")
    except Exception:
        report["fatal_error"] = assessment.sanitize(traceback.format_exc())
        assessment.save("interrupted")
        raise
    finally:
        data.stop_all_subscriptions()
        if data._quote_session is not None:
            data._quote_session.stop()
        assessment.client._transport().stop()
        assessment.record_file.close()


if __name__ == "__main__":
    main()
