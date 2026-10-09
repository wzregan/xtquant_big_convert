#!/usr/bin/env python
"""比较直接创建通道与后端工厂创建通道的本机合成通信性能。"""

import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import socket
import statistics
import sys
import threading
import time


ACCOUNT = "benchmark-synthetic-only"
TOPIC = "benchmark.synthetic.quote"


def distribution(values):
    """返回样本数和分位数；单位由调用方传入的数值决定。"""
    ordered = sorted(values)
    if not ordered:
        return {"samples": 0, "p50": None, "p95": None, "p99": None, "max": None}

    def percentile(ratio):
        """对排序后的样本做线性插值，避免小样本整数取整偏差。"""
        index = (len(ordered) - 1) * ratio
        low = int(index)
        high = min(low + 1, len(ordered) - 1)
        return ordered[low] + (ordered[high] - ordered[low]) * (index - low)

    return {
        "samples": len(ordered),
        "p50": percentile(0.50),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "max": ordered[-1],
    }


def temporary_endpoint():
    """取得本机临时 TCP 端点；释放后立即绑定，极小竞态由 ZMQ 明确报错。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
        reservation.bind(("127.0.0.1", 0))
        return "tcp://127.0.0.1:%d" % reservation.getsockname()[1]


def make_config(rpc_endpoint, push_endpoint):
    """只使用本机显式端点，禁止服务发现或连接用户的 Redis。"""
    return {
        "redis_enabled": False,
        "zmq": {
            "bind_address": rpc_endpoint,
            "connect_address": rpc_endpoint,
            "quote_push_bind_address": push_endpoint,
            "quote_push_connect_address": push_endpoint,
            "recv_timeout_seconds": 0.05,
            "stall_warn_seconds": 0.0,
        },
    }


def build_rpc(mode, config, api):
    """以相同参数创建原始通道或经注册表创建通道，均不提前启动连接。"""
    if mode == "direct":
        return api["ZmqTransport"].from_config(config["zmq"], account_id=ACCOUNT)
    return api["get_backend"]("zmq").build_rpc_transport(config, account_id=ACCOUNT)


def build_push(mode, role, config, api):
    """创建推送通道；role 指定发布端或订阅端，由调用方管理生命周期。"""
    if mode == "direct":
        field = "bind_address" if role == "server" else "connect_address"
        endpoint = config["zmq"]["quote_push_" + field]
        return api["ZmqQuotePushChannel"](**{field: endpoint})
    return api["get_backend"]("zmq").build_push_channel(role, config, account_id=ACCOUNT)


def construction_benchmark(mode, count, api):
    """测量无连接创建的工厂成本；每种对象预热后再测，单位为微秒。"""
    config = make_config("tcp://127.0.0.1:1", "tcp://127.0.0.1:2")
    builders = {
        "rpc": lambda: build_rpc(mode, config, api),
        "push_server": lambda: build_push(mode, "server", config, api),
        "push_client": lambda: build_push(mode, "client", config, api),
    }
    results = {}
    for name, builder in builders.items():
        for _index in range(100):
            builder()
        samples = []
        started = time.perf_counter()
        for _index in range(count):
            before = time.perf_counter()
            builder()
            samples.append((time.perf_counter() - before) * 1_000_000.0)
        duration = time.perf_counter() - started
        results[name] = {
            "latency_us": distribution(samples),
            "duration_seconds": duration,
            "objects_per_second": count / duration,
        }
    return {"mode": mode, "measurements": results}


def rpc_request(worker, sequence, data):
    """构造与正式传输一致的请求信封，只包含合成回显业务。"""
    return {
        "schema_version": 1,
        "request_id": "benchmark-%d-%d" % (worker, sequence),
        "account_id": ACCOUNT,
        "method": "synthetic_echo",
        "params": data,
    }


def rpc_benchmark(mode, workers, count, payload_name, data, api):
    """测量真实 TCP RPC；每线程固定连接并预热，耗时不包含校验与预热。"""
    config = make_config(temporary_endpoint(), temporary_endpoint())
    construction_start = time.perf_counter()
    server = build_rpc(mode, config, api)
    client = build_rpc(mode, config, api)
    construction_us = (time.perf_counter() - construction_start) * 1_000_000.0
    ready_events = [threading.Event() for _index in range(workers)]
    release = threading.Event()
    latencies = [[] for _index in range(workers)]
    failures = [[] for _index in range(workers)]
    first_call_ms = [None for _index in range(workers)]
    startup_failures = []
    threads = []

    def handler(request):
        """回显信封字段；不调用行情、账号或真实交易接口。"""
        return {
            "schema_version": 1,
            "request_id": request["request_id"],
            "account_id": ACCOUNT,
            "method": request["method"],
            "ok": True,
            "data": request["params"],
            "error": None,
        }

    def worker(index):
        """为当前线程预热独占 DEALER，统一起跑后逐次测量并校验回复。"""
        try:
            for warmup in range(20):
                request = rpc_request(index, -warmup - 1, data)
                before = time.perf_counter()
                response = client.send_request(request, 3.0)
                if warmup == 0:
                    first_call_ms[index] = (time.perf_counter() - before) * 1000.0
                if response.get("request_id") != request["request_id"] or response.get("data") != data:
                    raise RuntimeError("invalid warmup response")
        except Exception as exc:
            startup_failures.append("worker %d: %s" % (index, exc))
            ready_events[index].set()
            return
        ready_events[index].set()
        release.wait()
        assigned = count // workers + (index < count % workers)
        for sequence in range(assigned):
            request = rpc_request(index, sequence, data)
            before = time.perf_counter()
            try:
                response = client.send_request(request, 3.0)
                elapsed_ms = (time.perf_counter() - before) * 1000.0
                if (not response.get("ok") or response.get("request_id") != request["request_id"]
                        or response.get("data") != data):
                    raise RuntimeError("invalid benchmark response")
                latencies[index].append(elapsed_ms)
            except Exception as exc:
                failures[index].append({"sequence": sequence, "error": str(exc)})

    try:
        before = time.perf_counter()
        server.start_receiving(handler)
        server_start_ms = (time.perf_counter() - before) * 1000.0
        for index in range(workers):
            thread = threading.Thread(target=worker, args=(index,), name="benchmark-rpc-%d" % index)
            threads.append(thread)
            thread.start()
        for ready in ready_events:
            if not ready.wait(15.0):
                raise RuntimeError("RPC worker did not finish warmup")
        if startup_failures:
            raise RuntimeError("; ".join(startup_failures))
        before = time.perf_counter()
        release.set()
        for thread in threads:
            thread.join(30.0)
            if thread.is_alive():
                raise RuntimeError("RPC worker did not finish measured requests")
        duration = time.perf_counter() - before
        all_latencies = [value for group in latencies for value in group]
        all_failures = [value for group in failures for value in group]
        return {
            "mode": mode,
            "workers": workers,
            "payload": payload_name,
            "json_payload_bytes": len(json.dumps(data).encode("utf-8")),
            "request_count": count,
            "success_count": len(all_latencies),
            "failure_count": len(all_failures),
            "failures": all_failures[:20],
            "latency_ms": distribution(all_latencies),
            "duration_seconds": duration,
            "successful_requests_per_second": len(all_latencies) / duration,
            "construction_us": construction_us,
            "server_start_ms": server_start_ms,
            "first_request_ms": distribution([value for value in first_call_ms if value is not None]),
        }
    finally:
        release.set()
        for thread in threads:
            thread.join(5.0)
        client.stop()
        server.stop()


def push_benchmark(mode, scenario, count, rate, callback_delay_ms, api):
    """测量合成行情推送；序号核对交付，接收时间在同一单调时钟上计算。"""
    config = make_config(temporary_endpoint(), temporary_endpoint())
    before = time.perf_counter()
    publisher = build_push(mode, "server", config, api)
    subscriber = build_push(mode, "client", config, api)
    construction_us = (time.perf_counter() - before) * 1_000_000.0
    ready = threading.Event()
    complete = threading.Event()
    received = []
    receive_lock = threading.Lock()

    def callback(topic, data):
        """记录序号与接收时间；慢消费者场景模拟业务回调占用订阅线程。"""
        if data.get("warmup"):
            ready.set()
            return
        observed = time.perf_counter()
        with receive_lock:
            received.append((data["sequence"], observed, data["sent_at"]))
            if len(received) >= count:
                complete.set()
        if callback_delay_ms:
            time.sleep(callback_delay_ms / 1000.0)

    try:
        before = time.perf_counter()
        publisher.start_publisher()
        subscriber.start_subscriber([TOPIC], callback)
        socket_start_ms = (time.perf_counter() - before) * 1000.0
        # PUB/SUB 建连不等于订阅已生效；用实际收到的预热帧确认后才开始测量。
        before = time.perf_counter()
        deadline = before + 5.0
        while not ready.is_set() and time.perf_counter() < deadline:
            publisher.publish(TOPIC, {"warmup": True})
            ready.wait(0.01)
        if not ready.is_set():
            raise RuntimeError("push subscriber did not observe warmup frame")
        subscriber_ready_ms = (time.perf_counter() - before) * 1000.0
        start = time.perf_counter()
        for sequence in range(count):
            if rate:
                next_at = start + sequence / float(rate)
                remaining = next_at - time.perf_counter()
                if remaining > 0:
                    time.sleep(remaining)
            publisher.publish(TOPIC, {
                "sequence": sequence,
                "sent_at": time.perf_counter(),
                "quote": {"510300.SH": {"lastPrice": 4.123, "volume": sequence * 100}},
            })
        publish_end = time.perf_counter()
        # 持续收到帧时继续排空；无帧超过 0.75 秒或达到总上限才结束。
        # 时间上限给 sleep 调度误差留双倍余量，避免把仍在排队的帧误记为丢失。
        drain_limit = max(3.0, count * callback_delay_ms / 1000.0 * 2.0 + 1.0)
        drain_deadline = publish_end + drain_limit
        while not complete.wait(0.05) and time.perf_counter() < drain_deadline:
            with receive_lock:
                last_observed = received[-1][1] if received else publish_end
            if time.perf_counter() - max(last_observed, publish_end) > 0.75:
                break
        with receive_lock:
            snapshots = list(received)
        unique = set(sequence for sequence, _observed, _sent in snapshots)
        last_received = max([observed for _sequence, observed, _sent in snapshots] or [publish_end])
        delivered_duration = max(last_received, publish_end) - start
        inversions = sum(snapshots[index][0] <= snapshots[index - 1][0]
                         for index in range(1, len(snapshots)))
        return {
            "mode": mode,
            "scenario": scenario,
            "target_publish_rate": rate,
            "callback_delay_ms": callback_delay_ms,
            "publish_call_count": count,
            "received_count": len(snapshots),
            "unique_received_count": len(unique),
            "missing_count": count - len(unique),
            "duplicate_count": len(snapshots) - len(unique),
            "order_violations": inversions,
            "missing_sequence_examples": [value for value in range(count) if value not in unique][:20],
            "latency_ms": distribution([(observed - sent) * 1000.0
                                        for _sequence, observed, sent in snapshots]),
            "publish_duration_seconds": publish_end - start,
            "publish_calls_per_second": count / (publish_end - start),
            "delivery_duration_seconds": delivered_duration,
            "received_messages_per_second": len(snapshots) / delivered_duration,
            "construction_us": construction_us,
            "socket_start_ms": socket_start_ms,
            "subscriber_ready_ms": subscriber_ready_ms,
            "receive_drain_limit_seconds": drain_limit,
            "receive_quiet_seconds": 0.75,
            "delivery_contract": "best_effort_pub_sub_without_ack_or_replay",
        }
    finally:
        subscriber.stop()
        publisher.stop()


def merged_runs(rows, keys, latency_key, throughput_key):
    """按场景合并重复运行；分位数保留每次结果范围，不冒充原始样本合并。"""
    groups = {}
    for row in rows:
        key = tuple(row[name] for name in keys)
        groups.setdefault(key, []).append(row)
    results = []
    for key, runs in groups.items():
        merged = dict(zip(keys, key))
        merged["runs"] = len(runs)
        merged["throughput_median"] = statistics.median(row[throughput_key] for row in runs)
        merged["latency_ms_repeat_range"] = {
            quantile: {
                "min": min(row[latency_key][quantile] for row in runs),
                "max": max(row[latency_key][quantile] for row in runs),
            }
            for quantile in ("p50", "p95", "p99", "max")
            if all(row[latency_key][quantile] is not None for row in runs)
        }
        for name in ("request_count", "success_count", "failure_count", "publish_call_count",
                     "received_count", "missing_count", "duplicate_count", "order_violations"):
            if name in runs[0]:
                merged[name] = sum(row[name] for row in runs)
        results.append(merged)
    return results


def load_api(source_dir):
    """从指定源码目录导入，实现冻结初版与修改版的独立进程可比验证。"""
    sys.path.insert(0, str(source_dir))
    from bigqmt_signal_trader.quote_push_channel import ZmqQuotePushChannel
    from bigqmt_signal_trader.transports.backend import get_backend
    from bigqmt_signal_trader.transports.zmq_transport import ZmqTransport
    import zmq
    return {
        "ZmqTransport": ZmqTransport,
        "ZmqQuotePushChannel": ZmqQuotePushChannel,
        "get_backend": get_backend,
        "pyzmq_version": zmq.pyzmq_version(),
        "libzmq_version": zmq.zmq_version(),
    }


def main():
    """运行本机基准并写入 JSON；参数控制请求数量、并发、重复和推送负载。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=Path(__file__).resolve().parents[1] / "src")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--requests", type=int, default=3000)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 4, 20])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--construction-count", type=int, default=5000)
    parser.add_argument("--push-count", type=int, default=5000)
    parser.add_argument("--push-rate", type=int, default=2000)
    parser.add_argument("--cases", choices=["all", "rpc", "push"], default="all")
    parser.add_argument("--push-scenarios", nargs="+", choices=["paced", "burst", "slow_consumer_burst"],
                        default=["paced", "burst", "slow_consumer_burst"])
    args = parser.parse_args()
    if (min(args.requests, args.repeats, args.construction_count, args.push_count, args.push_rate) <= 0
            or not args.workers or min(args.workers) <= 0 or max(args.workers) > args.requests):
        parser.error("counts and worker settings must be positive; workers cannot exceed requests")
    source_dir = args.source_dir.resolve()
    api = load_api(source_dir)
    files = ["bigqmt_signal_trader/transports/backend.py", "bigqmt_signal_trader/transports/factory.py",
             "bigqmt_signal_trader/transports/zmq_transport.py", "bigqmt_signal_trader/quote_push_channel.py"]
    report = {
        "metadata": {
            "started_unix": time.time(),
            "source_dir": str(source_dir),
            "source_sha256": {name: hashlib.sha256((source_dir / name).read_bytes()).hexdigest() for name in files},
            "python": sys.version,
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "pyzmq_version": api["pyzmq_version"],
            "libzmq_version": api["libzmq_version"],
            "argv": sys.argv,
            "benchmark_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "scope": "Linux localhost synthetic ZMQ transport only; no QMT, gRPC or trading workload",
            "steady_state_boundary": "backend factory creates once; timed RPC/publish call the resulting channel directly",
        },
        "construction": [],
        "rpc": [],
        "push": [],
    }
    quote_batch = {"%06d.SH" % (510000 + index): {
        "lastPrice": 4.123 + index / 1000.0, "volume": 10000 + index, "amount": 12345.67,
        "bidPrice": [4.12, 4.11, 4.10], "askPrice": [4.13, 4.14, 4.15],
    } for index in range(32)}
    payloads = [("ping", {"value": 1}), ("quote_batch_32", quote_batch)]

    def save():
        """逐场景保存，使被打断的运行仍保留已经完成的证据。"""
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    for repeat in range(args.repeats):
        # 交替测试先后顺序，减少缓存、热身和宿主机负载变化的单向偏差。
        modes = ["direct", "backend"] if repeat % 2 == 0 else ["backend", "direct"]
        for mode in modes:
            row = construction_benchmark(mode, args.construction_count, api)
            row["repeat"] = repeat
            report["construction"].append(row)
            save()
        for payload_name, data in payloads if args.cases != "push" else []:
            for workers in args.workers:
                for mode in modes:
                    row = rpc_benchmark(mode, workers, args.requests, payload_name, data, api)
                    row["repeat"] = repeat
                    report["rpc"].append(row)
                    save()
                    print(json.dumps({name: row[name] for name in (
                        "mode", "workers", "payload", "success_count", "failure_count",
                        "latency_ms", "successful_requests_per_second")}), flush=True)
        scenarios = [("paced", args.push_rate, 0.0), ("burst", 0, 0.0),
                     ("slow_consumer_burst", 0, 0.5)]
        for scenario, rate, delay in scenarios:
            if args.cases == "rpc" or scenario not in args.push_scenarios:
                continue
            for mode in modes:
                row = push_benchmark(mode, scenario, args.push_count, rate, delay, api)
                row["repeat"] = repeat
                report["push"].append(row)
                save()
                print(json.dumps({name: row[name] for name in (
                    "mode", "scenario", "publish_call_count", "received_count", "missing_count",
                    "order_violations", "latency_ms", "received_messages_per_second")}), flush=True)
    report["rpc_summary"] = merged_runs(report["rpc"], ["mode", "workers", "payload"],
                                        "latency_ms", "successful_requests_per_second")
    report["push_summary"] = merged_runs(report["push"], ["mode", "scenario"],
                                         "latency_ms", "received_messages_per_second")
    report["metadata"]["finished_unix"] = time.time()
    save()
    print("Saved %s" % args.output, flush=True)


if __name__ == "__main__":
    main()
