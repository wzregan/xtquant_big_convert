"""对已运行的 QMT 桥接做有界真机验收；只提交被禁用的空交易请求。"""

import argparse
import concurrent.futures
import datetime as dt
import importlib.util
import json
import math
import os
import statistics
import subprocess
import sys
import threading
import time
import traceback
import uuid
from collections import Counter
from pathlib import Path


def distribution(values):
    """汇总毫秒样本；分位数采用最近秩法，保留样本数供解释置信度。"""
    ordered = sorted(values)
    if not ordered:
        return {"n": 0}
    return {
        "n": len(ordered), "min_ms": ordered[0], "mean_ms": statistics.mean(ordered),
        "p50_ms": statistics.median(ordered),
        "p95_ms": ordered[math.ceil(len(ordered) * .95) - 1],
        "p99_ms": ordered[math.ceil(len(ordered) * .99) - 1], "max_ms": ordered[-1],
    }


def empty(value):
    """区分空容器、空数据帧和有效的零值，避免把空返回当作数据可用。"""
    if value is None:
        return True
    if hasattr(value, "empty"):
        return bool(value.empty)
    if isinstance(value, (dict, list, tuple)):
        items = value.values() if isinstance(value, dict) else value
        return not value or all(empty(item) for item in items)
    return isinstance(value, str) and not value


def shape(value, depth=0):
    """只保留返回结构和行数，不保存资金、持仓明细、委托号或盘口价格。"""
    if hasattr(value, "shape"):
        return {"type": type(value).__name__, "shape": list(value.shape),
                "columns": [str(x) for x in getattr(value, "columns", [])],
                "last_index": str(value.index[-1]) if len(value) else None}
    if isinstance(value, dict):
        result = {"type": "dict", "count": len(value), "keys": list(map(str, value))[:16]}
        if depth < 2:
            frames = {str(k): shape(v, depth + 1) for k, v in value.items()
                      if hasattr(v, "shape") or isinstance(v, (list, dict))}
            result["children"] = dict(list(frames.items())[:5])
        return result
    if isinstance(value, (list, tuple)):
        return {"type": type(value).__name__, "count": len(value),
                "first": shape(value[0], depth + 1) if value and depth < 2 else None}
    return {"type": type(value).__name__, "empty": empty(value)}


class Assessment:
    """独立客户端测试会话；配置只在本机读取，结果仅记录脱敏结构。"""

    def __init__(self, config_path, output, plan, soak_seconds):
        """config_path 为本机配置路径；output 保存证据；soak_seconds 为持续测试秒数。"""
        self.output = Path(output)
        self.output.mkdir(parents=True, exist_ok=True)
        spec = importlib.util.spec_from_file_location("bigqmt_signal_trader_client_config", config_path)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config)
        config.BIGQMT_QUOTE_CLIENT_ID = "assessment-" + uuid.uuid4().hex
        sys.modules[spec.name] = config
        self.account = str(config.BIGQMT_ACCOUNT_ID)
        self.config = dict(config.BIGQMT_REDIS_CONFIG)
        self.config.update(local_cache_enabled=False)
        from bigqmt_signal_trader.xtquant_compat import BigQmtRpcClient, BigQmtXtData, BigQmtXtTrader
        self.client_class = BigQmtRpcClient
        self.data_class = BigQmtXtData
        self.trader_class = BigQmtXtTrader
        self.client = self.new_client()
        self.plan = plan
        self.soak_seconds = soak_seconds
        self.records = []
        self.values = {}
        self.lock = threading.Lock()
        self.record_file = (self.output / "calls.jsonl").open("a", encoding="utf-8", buffering=1)
        self.report = {"started_at": dt.datetime.now().isoformat(), "phase": "starting",
                       "availability": [], "performance": [], "concurrency": [],
                       "subscriptions": [], "recovery": [], "resources": [],
                       "scope": "Windows 本机 ZMQ；非交易时段；未提交真实委托"}
        self.option_code = None
        self.sector = "BIGQMT_TEST_" + uuid.uuid4().hex[:12]
        self.sector_touched = False

    def new_client(self):
        """创建使用部署配置的独立 RPC 客户端，不连接 Redis 或调用原生交易 SDK。"""
        client = self.client_class(account_id=self.account, redis_config=self.config, timeout_seconds=8)
        # 直接配置真实使用的属性，确保封装内部调用也不会绕到公式服务或缓存。
        client.formula_server_config = {"enabled": False}
        client.local_cache_config["enabled"] = False
        client.full_tick_cache_config["enabled"] = False
        return client

    def save(self, phase=None):
        """原子保存阶段结果，使中断也留下明确的已完成范围。"""
        if phase:
            self.report["phase"] = phase
            print(json.dumps({"phase": phase, "at": dt.datetime.now().isoformat()}, ensure_ascii=False), flush=True)
        self.report["updated_at"] = dt.datetime.now().isoformat()
        path = self.output / "report.json"
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps(self.report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        temp.replace(path)

    def sanitize(self, value):
        """诊断信息仅替换本账户标识，不收集原始业务返回。"""
        return str(value).replace(self.account, "<account>")[:1800]

    def validate(self, method, params, value):
        """对核心查询检查字段和返回关联，其余接口记录结构及空值状态。"""
        if method == "ping":
            assert value.get("pong") is True and value.get("account_id") == self.account
            assert value.get("allow_order_methods") is False
        elif method in ("get_ticks", "get_full_tick"):
            for code in params.get("codes", []):
                row = value.get(code)
                assert isinstance(row, dict) and float(row.get("lastPrice", 0)) > 0, "行情缺失或价格无效: " + code
        elif method in ("get_asset", "query_stock_asset"):
            assert str(value.get("account_id")) == self.account, "资产账户不匹配"
            for field in ("cash", "total_asset"):
                assert field in value and math.isfinite(float(value[field])), "资产字段无效: " + field
        elif method == "timetag_to_datetime":
            expected = dt.datetime.fromtimestamp(int(params["timetag"]) / 1000).strftime(params["format"])
            assert value == expected, "响应对应了错误的请求时间戳"
        elif method == "datetime_to_timetag":
            expected = int(dt.datetime.strptime(params["datetime_str"], params.get("format", "%Y%m%d%H%M%S")).timestamp() * 1000)
            assert value == expected, "时间转换不一致"

    def call(self, method, params=None, phase="availability", label=None, timeout=8, expected=None, client=None):
        """计时覆盖实际 RPC 和反序列化；timeout 为秒，expected 标明应被拒绝的请求。"""
        params = dict(params or {})
        client = client or self.client
        start = time.perf_counter()
        elapsed = None
        value = None
        record = {"method": method, "label": label or method, "phase": phase,
                  "time": dt.datetime.now().isoformat()}
        try:
            value = client.call(method, params, timeout_seconds=timeout, use_formula=False)
            elapsed = (time.perf_counter() - start) * 1000
            if expected:
                raise AssertionError("预期拒绝的请求被接受: " + expected)
            self.validate(method, params, value)
            record.update(status="EMPTY" if empty(value) else "OK", result=shape(value))
        except Exception as exc:
            message = self.sanitize(type(exc).__name__ + ": " + str(exc))
            lower = message.lower()
            if expected == "order_guard" and ("not allowed" in lower or "methods are disabled" in lower):
                status = "GUARDED"
            elif expected == "download_disabled" and "disabled" in lower:
                status = "CONFIG_DISABLED"
            elif expected == "invalid_input" and ("required" in lower or "not allowed" in lower or "invalid" in lower):
                status = "REJECTED"
            elif "timeout" in lower or "timed out" in lower:
                status = "TIMEOUT"
            elif any(word in lower for word in ("not supported", "not available", "unavailable", "not implemented", "权限", "无法连接行情")):
                status = "UNSUPPORTED"
            elif "argumenterror" in lower or "typeerror" in lower:
                status = "SIGNATURE_ERROR"
            elif isinstance(exc, AssertionError):
                status = "SEMANTIC_ERROR"
            else:
                status = "ERROR"
            record.update(status=status, error=message)
        # 结构检查与证据落盘不计入 RPC 延迟，避免大返回的诊断成本污染测量。
        record["ms"] = elapsed if elapsed is not None else (time.perf_counter() - start) * 1000
        with self.lock:
            self.records.append(record)
            self.record_file.write(json.dumps(record, ensure_ascii=False) + "\n")
        return record, value

    def health(self):
        """超时后只发送一次较长等待的心跳，避免继续堆积原生请求。"""
        result, _ = self.call("ping", phase="health", timeout=35)
        if result["status"] != "OK":
            raise RuntimeError("桥接健康检查失败，停止新增负载: " + result.get("error", ""))

    def baseline(self):
        """保存服务版本及能力，拒绝在已开启远程交易的环境中执行保护测试。"""
        self.save("baseline")
        for method in ("ping", "get_deployment_info", "probe_capabilities"):
            result, value = self.call(method, phase="baseline", timeout=30)
            if result["status"] != "OK":
                raise RuntimeError("基线读取失败: " + method + ": " + result.get("error", ""))
            value.pop("account_id", None)
            self.report[method] = value
        assert self.config.get("rpc_allow_order_methods") is False
        self.report["client_python"] = sys.version
        self.report["transport"] = self.client.transport_name

    def availability(self):
        """按显式清单测试每个 RPC；修改板块仅使用本轮随机名称并最终清理。"""
        self.save("availability")
        for index, case in enumerate(self.plan["cases"]):
            case = dict(case)
            method = case["method"]
            if case.get("skip"):
                self.report["availability"].append(dict(case, status="SKIPPED"))
                continue
            params = dict(case.get("params", {}))
            if "$OPTION" in json.dumps(params):
                if not self.option_code:
                    self.report["availability"].append(dict(case, status="SKIPPED", skip="未取得有效期权代码"))
                    continue
                params = {k: self.option_code if v == "$OPTION" else v for k, v in params.items()}
            params = {k: self.sector if v == "$SECTOR" else v for k, v in params.items()}
            if "$SECTOR" in json.dumps(case.get("params", {})):
                self.sector_touched = True
            result, value = self.call(method, params, timeout=case.get("timeout", 20), expected=case.get("expected"))
            self.report["availability"].append(dict(case, **{k: v for k, v in result.items() if k != "phase"}))
            self.values[method] = value
            if method == "get_option_list" and isinstance(value, list):
                codes = [str(v) for v in value if isinstance(v, str) and v]
                if codes:
                    self.option_code = codes[0]
            if result["status"] == "TIMEOUT":
                self.health()
            # 只重复纯查询。首调用保留，成功返回与空返回分别统计，下载和写入不循环。
            if case.get("repeat", True) and result["status"] in ("OK", "EMPTY"):
                repeats = 9 if result["status"] == "OK" else 2
                for _ in range(repeats):
                    follow, _ = self.call(method, params, phase="api_repeat", timeout=case.get("timeout", 20))
                    if follow["status"] not in ("OK", "EMPTY"):
                        if follow["status"] == "TIMEOUT":
                            self.health()
                        break
            if index % 8 == 0:
                self.save()
                print(json.dumps({"availability": index + 1, "total": len(self.plan["cases"]), "last": method, "status": result["status"]}, ensure_ascii=False), flush=True)
        self.save()

    def benchmark(self, label, method, params, n):
        """预热后采集 n 次固定形状查询，失败不混入成功延迟分位数。"""
        first, _ = self.call(method, params, phase="performance_warmup", label=label, timeout=30)
        if first["status"] not in ("OK", "EMPTY"):
            self.report["performance"].append({"label": label, "skipped": first})
            return
        rows = []
        for _ in range(n):
            row, _ = self.call(method, params, phase="performance", label=label, timeout=20)
            rows.append(row)
            if row["status"] == "TIMEOUT":
                self.health()
                break
        self.report["performance"].append({"label": label, "method": method,
                                           "statuses": dict(Counter(r["status"] for r in rows)),
                                           "latency": distribution([r["ms"] for r in rows if r["status"] in ("OK", "EMPTY")])})
        self.save()

    def performance(self):
        """覆盖常用接口、批量快照、不同历史数据规模及并发请求关联。"""
        self.save("performance")
        codes = self.values.get("get_stock_list_in_sector") or ["000001.SZ", "600000.SH"]
        codes = [str(c) for c in codes if isinstance(c, str) and c.endswith((".SH", ".SZ"))][:100]
        for method, params in (("ping", {}), ("get_asset", {}), ("get_positions", {}),
                               ("query_orders", {"strategy_name": ""}), ("query_trades", {"strategy_name": ""}),
                               ("get_instrument", {"code": "000001.SZ"})):
            self.benchmark(method, method, params, 100)
        for count in (1, 20, 100):
            self.benchmark("tick_" + str(min(count, len(codes))), "get_full_tick", {"codes": codes[:count]}, 100 if count == 1 else 50)
        for count, bars in ((1, 5), (1, 250), (20, 250), (100, 250)):
            self.benchmark("daily_%dx%d" % (min(count, len(codes)), bars), "get_market_data_ex",
                           {"stock_list": codes[:count], "field_list": ["open", "high", "low", "close", "volume", "amount"],
                            "period": "1d", "count": bars, "end_time": "20260904"}, 30)
        self.benchmark("minute_1x240", "get_market_data_ex", {"stock_list": ["000001.SZ"], "field_list": ["close", "volume"],
                                                             "period": "1m", "count": 240, "end_time": "20260904150000"}, 30)
        for workers in (1, 2, 4, 8):
            started = time.perf_counter()
            until = time.monotonic() + 15
            def worker(number):
                """为每个并发请求构造唯一时间戳，以检查响应串线。"""
                rows = []
                count = 0
                while time.monotonic() < until:
                    stamp = 1788451200000 + (number * 100000 + count) * 1000
                    row, _ = self.call("timetag_to_datetime", {"timetag": stamp, "format": "%Y%m%d%H%M%S"},
                                       phase="concurrency", label="threads_" + str(workers))
                    rows.append(row)
                    count += 1
                    if row["status"] != "OK":
                        break
                return rows
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                rows = [r for batch in pool.map(worker, range(workers)) for r in batch]
            elapsed = time.perf_counter() - started
            self.report["concurrency"].append({"workers": workers, "seconds": elapsed, "calls": len(rows),
                                                "qps": len(rows) / elapsed,
                                                "statuses": dict(Counter(r["status"] for r in rows)),
                                                "latency": distribution([r["ms"] for r in rows if r["status"] == "OK"])})
            self.save()

    def subscriptions(self):
        """验证快照首回调、订阅控制和客户端清理；休市时不声称验证了真实增量推送。"""
        self.save("subscriptions")
        data = self.data_class(self.client)
        for kind in ("whole", "tick", "1d", "1m"):
            events = []
            received = threading.Event()
            def callback(value):
                """只记录回调结构与时刻，不保存行情价格。"""
                events.append({"elapsed": time.monotonic() - start, "result": shape(value)})
                received.set()
            start = time.monotonic()
            seq = None
            before = threading.active_count()
            result = {"kind": kind}
            try:
                if kind == "whole":
                    seq = data.subscribe_whole_quote(["000001.SZ", "600000.SH"], callback=callback)
                else:
                    seq = data.subscribe_quote("000001.SZ", period=kind, count=1, callback=callback)
                result["subscribe_ms"] = (time.monotonic() - start) * 1000
                received.wait(5)
                time.sleep(2)
                result.update(status="OK" if events else "NO_DATA", events=events,
                              callback_count=len(events), native_incremental_verified=False)
            except Exception as exc:
                result.update(status="ERROR", error=self.sanitize(exc))
            finally:
                if seq is not None:
                    result["unsubscribe_result"] = data.unsubscribe_quote(seq)
                data.stop_all_subscriptions()
            time.sleep(.3)
            result.update(threads_before=before, threads_after=threading.active_count())
            self.report["subscriptions"].append(result)
            self.save()
        if data._quote_session is not None:
            data._quote_session.stop()

    def recovery(self):
        """只中断本测试客户端连接，检查重新建连和短超时后的回复隔离。"""
        self.save("client_recovery")
        for number in range(20):
            client = self.new_client()
            row, _ = self.call("ping", phase="reconnect", client=client)
            self.report["recovery"].append({"cycle": number, "status": row["status"], "ms": row["ms"]})
            client._transport().stop()
        for number in range(10):
            stamp = 1788451200000 + number * 1000
            row, _ = self.call("timetag_to_datetime", {"timetag": stamp, "format": "%Y%m%d%H%M%S"}, phase="short_timeout", timeout=.001)
            valid, _ = self.call("timetag_to_datetime", {"timetag": stamp + 1000000, "format": "%Y%m%d%H%M%S"}, phase="after_timeout")
            self.report["recovery"].append({"short_timeout": row["status"], "next_request": valid["status"], "ms": valid["ms"]})
        self.save()

    def asynchronous(self):
        """真机验证异步请求关联、回调交付以及四个常用账户查询回调。"""
        self.save("asynchronous")
        from bigqmt_signal_trader.xtquant_compat import StockAccount
        client = self.new_client()
        callbacks = []
        start = time.perf_counter()
        pending = []
        for number in range(32):
            stamp = 1788451200000 + number * 1000
            params = {"timetag": stamp, "format": "%Y%m%d%H%M%S"}
            pending.append((params, client.call_async("timetag_to_datetime", params, callback=callbacks.append)))
        enqueue_ms = (time.perf_counter() - start) * 1000
        for params, future in pending:
            self.validate("timetag_to_datetime", params, future.result(timeout=20))
        client._rpc_async_pool.shutdown(wait=True)
        client._rpc_async_dispatcher.shutdown(wait=True)
        client._transport().stop()
        trader = self.trader_class(account_id=self.account, redis_config=self.config, timeout_seconds=8)
        account = StockAccount(self.account, "STOCK")
        query_results = []
        for method in ("query_stock_asset_async", "query_stock_positions_async", "query_stock_orders_async", "query_stock_trades_async"):
            event = threading.Event()
            received = []
            def callback(value):
                """保存回调的结构并通知等待者，资金与委托明细不写入报告。"""
                received.append(shape(value))
                event.set()
            start = time.perf_counter()
            returned = getattr(trader, method)(account, callback)
            dispatch_ms = (time.perf_counter() - start) * 1000
            delivered = event.wait(12)
            query_results.append({"method": method, "dispatch_ms": dispatch_ms,
                                  "callback_received": delivered, "returned_none": returned is None,
                                  "result": received})
        trader.stop()
        self.report["asynchronous"] = {"rpc_requests": len(pending), "rpc_callbacks": len(callbacks),
                                        "all_responses_correlated": True, "enqueue_ms": enqueue_ms,
                                        "account_queries": query_results}
        self.save()

    def resources(self):
        """按 RPC 监听进程采集内存、句柄、线程和累计 CPU 秒，不读取桌面。"""
        script = "$p = Get-NetTCPConnection -LocalPort 15632 -State Listen | Select-Object -First 1; Get-Process -Id $p.OwningProcess | Select-Object Id,CPU,WorkingSet64,PrivateMemorySize64,HandleCount,@{N='Threads';E={$_.Threads.Count}} | ConvertTo-Json -Compress"
        result = subprocess.run(["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script],
                                capture_output=True, text=True, timeout=8)
        row = json.loads(result.stdout)
        row["at"] = dt.datetime.now().isoformat()
        row["phase"] = self.report["phase"]
        self.report["resources"].append(row)

    def soak(self):
        """四客户端线程持续混合查询，目标约每秒十次；每十秒采集进程资源。"""
        self.save("stability")
        start = time.monotonic()
        deadline = start + self.soak_seconds
        choices = [("ping", {}), ("get_full_tick", {"codes": ["000001.SZ"]}),
                   ("get_asset", {}), ("get_positions", {}),
                   ("query_orders", {"strategy_name": ""}), ("query_trades", {"strategy_name": ""})]
        def worker(number):
            """错开接口顺序并限制请求频率，错误时降低负载。"""
            rows = []
            index = number
            while time.monotonic() < deadline:
                tick = time.monotonic()
                method, params = choices[index % len(choices)]
                row, _ = self.call(method, params, phase="stability")
                rows.append(row)
                index += 1
                time.sleep(max(0, (.4 if row["status"] in ("OK", "EMPTY") else 2) - (time.monotonic() - tick)))
                if row["status"] == "TIMEOUT":
                    break
            return rows
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(worker, index) for index in range(4)]
            while any(not future.done() for future in futures):
                self.resources()
                self.save()
                print(json.dumps({"stability_seconds": round(time.monotonic() - start), "calls": sum(r["phase"] == "stability" for r in self.records)}, ensure_ascii=False), flush=True)
                time.sleep(min(10, max(0, deadline - time.monotonic())))
            rows = [row for future in futures for row in future.result()]
        self.report["stability"] = {"seconds": time.monotonic() - start, "calls": len(rows),
                                    "statuses": dict(Counter(r["status"] for r in rows)),
                                    "latency": distribution([r["ms"] for r in rows if r["status"] in ("OK", "EMPTY")])}
        self.resources()
        self.health()
        self.save()

    def run(self):
        """依次完成测试并保留中断证据；任何阶段都不改变服务端交易开关。"""
        try:
            self.baseline()
            self.resources()
            self.availability()
            self.performance()
            self.subscriptions()
            self.recovery()
            self.asynchronous()
            self.soak()
            self.report["finished_at"] = dt.datetime.now().isoformat()
            self.save("complete")
        except Exception:
            self.report["fatal_error"] = self.sanitize(traceback.format_exc())
            self.save("interrupted")
            raise
        finally:
            if self.sector_touched:
                try:
                    self.client.call("remove_sector", {"sector_name": self.sector}, timeout_seconds=8, use_formula=False)
                    members = self.client.call("get_stock_list_in_sector", {"sector_name": self.sector}, timeout_seconds=8, use_formula=False)
                    self.report["test_sector_cleanup"] = {"ok": not members}
                except Exception as exc:
                    self.report["test_sector_cleanup"] = {"ok": False, "error": self.sanitize(exc)}
                self.save()
            self.record_file.close()


def main():
    """命令行参数指定本机配置、显式测试清单和输出目录。"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--soak-seconds", type=int, default=600)
    args = parser.parse_args()
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    Assessment(args.config, args.output, plan, args.soak_seconds).run()


if __name__ == "__main__":
    main()
