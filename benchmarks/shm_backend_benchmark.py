"""独立进程共享内存基准；控制 Pipe 只传统计，合成正文全部经过实际 SHM 通道。

本脚本不连接 QMT，也不执行交易。正式测量应在源码冻结且没有其它回归负载时运行。
RPC 延迟含编码、排队、复制、响应解码；推送延迟从首次发布尝试到回调完成。
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid


ACCOUNT = "synthetic-shm-benchmark"
CONTEXT = multiprocessing.get_context("spawn")


def distribution(values):
    """对毫秒样本线性插值计算分位；同时返回样本数，空集合不伪造零延迟。"""
    ordered = sorted(values)
    result = {"samples": len(ordered)}
    for name, quantile in (("p50_ms", 0.50), ("p95_ms", 0.95), ("p99_ms", 0.99)):
        if not ordered:
            result[name] = None
            continue
        position = (len(ordered) - 1) * quantile
        lower = int(position)
        upper = min(lower + 1, len(ordered) - 1)
        result[name] = ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
    result["max_ms"] = max(ordered) if ordered else None
    return result


def modules(source):
    """使用调用者指定的源码，避免子进程误导入已安装的旧版本。"""
    sys.path.insert(0, source)
    from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport
    from bigqmt_signal_trader.shm_push_channel import SharedMemoryQuotePushChannel, PushBackpressureError
    from bigqmt_signal_trader.transports.shm_primitives import cleanup

    return SharedMemoryTransport, SharedMemoryQuotePushChannel, PushBackpressureError, cleanup


def peak_rss_bytes():
    """取得 POSIX 进程历史峰值 RSS；Windows 无此标准库观测时返回空值。"""
    if sys.platform.startswith("linux"):
        # fork/exec 的 ru_maxrss 可能继承父进程旧峰值，VmHWM 才是本次解释器的峰值。
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
    try:
        import resource

        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(value if sys.platform == "darwin" else value * 1024)
    except ImportError:
        return None


def cpu_result(start, elapsed):
    """给出进程 CPU 秒及相对于一个核心的平均占用，保留 RSS 为历史峰值的边界。"""
    value = time.process_time() - start
    return {"cpu_seconds": value, "average_cpu_cores": value / elapsed if elapsed else None,
            "process_lifetime_peak_rss_bytes": peak_rss_bytes(),
            "rss_observation": "proc_VmHWM" if sys.platform.startswith("linux") else "getrusage_or_unavailable"}


class RegionSampler(object):
    """可选 20ms 区域文件抽样，仅用于大报文资源补充观测，不是精确分配计数。"""

    def __init__(self, directory):
        """directory 必须是本场景独有目录；不扫描或清理任何其它服务资源。"""
        self.directory = Path(directory)
        self.done = threading.Event()
        self.values = {"interval_seconds": 0.02, "samples": 0, "max_region_file_count": 0,
                       "max_region_logical_bytes": 0, "max_region_allocated_bytes": 0}
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        """文件可能刚好被协议回收，忽略单次消失并保留已经观测的峰值。"""
        while not self.done.is_set():
            try:
                files = [path.stat() for path in self.directory.glob("*.region")]
                self.values["samples"] += 1
                self.values["max_region_file_count"] = max(self.values["max_region_file_count"], len(files))
                self.values["max_region_logical_bytes"] = max(self.values["max_region_logical_bytes"], sum(value.st_size for value in files))
                self.values["max_region_allocated_bytes"] = max(self.values["max_region_allocated_bytes"], sum(value.st_blocks * 512 for value in files))
            except FileNotFoundError:
                pass
            self.done.wait(0.02)

    def stop(self):
        """先停止自身观察线程，返回抽样下界，避免收尾删除目录时仍访问文件。"""
        self.done.set()
        self.thread.join(1.0)
        return dict(self.values)


def rpc_worker(config, source, pipe):
    """独立服务进程回显合成字段；控制命令启动、终止 CPU 采样。"""
    transport = None
    try:
        rpc_class = modules(source)[0]
        transport = rpc_class(account_id=ACCOUNT, **config)

        def echo(request):
            """实际解码后的请求正文成为响应正文，覆盖双向动态大报文。"""
            return {"ok": True, "request_id": request["request_id"], "data": request["params"]}

        transport.start_receiving(echo)
        pipe.send({"ready": True, "pid": os.getpid()})
        cpu_start, wall_start = None, None
        while True:
            command = pipe.recv()
            if command == "begin":
                cpu_start, wall_start = time.process_time(), time.perf_counter()
                pipe.send({"begun": True})
            elif command == "finish":
                transport.check_health()
                elapsed = time.perf_counter() - wall_start
                pipe.send({"cpu": cpu_result(cpu_start, elapsed), "status": transport.status()})
            elif command == "stop":
                transport.stop()
                pipe.send({"stopped": True})
                break
    except BaseException:
        pipe.send({"error": traceback.format_exc()})
    finally:
        if transport is not None:
            transport.stop()
        pipe.close()


def push_worker(config, source, pipe, count, delay_seconds=0.0, checkpoint=False):
    """独立消费者保存序号和端到端延迟；checkpoint 模式等待父进程强杀。"""
    channel = None
    try:
        _rpc_class, push_class, backpressure = modules(source)[:3]
        channel = push_class("client", config, ACCOUNT)
        rows = []
        invalid = []

        def received(topic, data):
            """核对实际正文并在成功回调结束时采样，慢回调显式占用处理时间。"""
            if data["index"] < 0:
                return
            if topic != "benchmark:tick" or data["blob"] != "x" * data["blob_bytes"]:
                invalid.append(data["index"])
            if delay_seconds:
                time.sleep(delay_seconds)
            rows.append((data["index"], (time.perf_counter() - data["sent"]) * 1000))

        channel.start_subscriber(["benchmark:tick"], received)
        pipe.send({"ready": True, "pid": os.getpid()})
        if pipe.recv() != "begin":
            raise RuntimeError("invalid consumer control command")
        cpu_start, wall_start = time.process_time(), time.perf_counter()
        pipe.send({"begun": True})
        deadline = time.monotonic() + 180.0
        health_lock_contentions = 0
        while len(rows) < count or channel.status()["acknowledged"] < count:
            try:
                channel.check_health()
            except backpressure:
                # 健康检查明确标注短暂锁忙可重试，不能由观察线程主动杀掉正常消费者。
                health_lock_contentions += 1
            if time.monotonic() > deadline:
                raise RuntimeError("consumer did not acknowledge all samples")
            time.sleep(0.001)
        elapsed = time.perf_counter() - wall_start
        result = {"rows": rows, "invalid": invalid, "cpu": cpu_result(cpu_start, elapsed),
                  "health_lock_contentions": health_lock_contentions,
                  "status": channel.status(), "elapsed_seconds": elapsed}
        if checkpoint:
            # 确认前缀 ACK 已提交后冻结控制线程；没有剩余消息，强杀不会切断回调。
            pipe.send(dict(result, checkpoint=True))
            pipe.recv()
        else:
            channel.stop()
            pipe.send(result)
    except BaseException:
        pipe.send({"error": traceback.format_exc()})
    finally:
        if channel is not None:
            channel.stop()
        pipe.close()


def receive(pipe, process, timeout=180.0):
    """有界收取控制消息并保留子进程错误，不能以空样本冒充成功。"""
    if not pipe.poll(timeout):
        raise RuntimeError("worker control timeout, pid=%s alive=%s" % (process.pid, process.is_alive()))
    result = pipe.recv()
    if "error" in result:
        raise RuntimeError(result["error"])
    return result


def launch(target, arguments):
    """spawn 重新导入和打开端点，禁止继承父进程 mmap/fd 作为跨进程验证。"""
    parent, child = CONTEXT.Pipe()
    process = CONTEXT.Process(target=target, args=arguments[:2] + (child,) + arguments[2:])
    process.start()
    child.close()
    ready = receive(parent, process, 15.0)
    if not ready.get("ready"):
        raise RuntimeError("worker was not ready")
    return process, parent, ready


def terminate(process, pipe):
    """异常收尾时先终止独立进程再关闭控制 Pipe，保证资源清理没有活使用者。"""
    if process is not None:
        process.join(0.1)
        if process.is_alive():
            process.kill()
            process.join(5.0)
    if pipe is not None:
        pipe.close()


def resource_report(directory, names, source):
    """全部句柄关闭后核对动态 blob 残留，并显式清理本次拥有的逻辑端点。"""
    cleanup = modules(source)[3]
    location = Path(directory)
    expected = {"bigqmt_" + hashlib.sha256(name.encode("utf-8")).hexdigest() + ".region"
                for name in names}
    before = sorted(path.name for path in location.iterdir())
    unexpected_regions = [name for name in before if name.endswith(".region") and name not in expected]
    sem_paths = []
    if os.name != "nt":
        from bigqmt_signal_trader.transports.shm_primitives import _semaphore_name

        sem_paths = [Path("/dev/shm") / ("sem." + _semaphore_name(name, directory).decode("ascii")[1:])
                     for name in names]
    for name in names:
        cleanup(name, directory)
    after = sorted(path.name for path in location.iterdir())
    remaining_semaphores = [str(path) for path in sem_paths if path.exists()]
    return {"files_before_explicit_cleanup": before, "unexpected_dynamic_regions": unexpected_regions,
            "files_after_explicit_cleanup": after, "named_semaphores_after_cleanup": remaining_semaphores,
            "clean": not unexpected_regions and not after and not remaining_semaphores}


def rpc_case(source, directory, workers, count, blob_bytes, repeat, paced_rate=0.0, sample_resources=False):
    """测量一个独立服务与一个多线程客户端，样本为完整双向回显往返。"""
    rpc_class = modules(source)[0]
    config = {"namespace": "rpc-bench-" + uuid.uuid4().hex, "directory": directory,
              "rpc_slots": 32, "rpc_slot_bytes": 65536}
    client = rpc_class(account_id=ACCOUNT, **config)
    process = pipe = None
    result = None
    sampler = RegionSampler(directory) if sample_resources else None
    try:
        started = time.perf_counter()
        process, pipe, ready = launch(rpc_worker, (config, source))
        server_init = time.perf_counter() - started
        blob = "x" * blob_bytes
        timeout = 60.0 if blob_bytes >= 20 * 1024 * 1024 else 10.0
        warm_start = time.perf_counter()
        for index in range(workers):
            client.send_request({"request_id": "warm-%d" % index, "method": "synthetic_echo",
                                 "params": {"index": index, "blob": blob}}, timeout)
        warmed = time.perf_counter() - warm_start
        barrier = threading.Barrier(workers + 1)

        def caller(worker):
            """每个线程有独立请求编号；返回真正成功的延迟及原始错误性质。"""
            samples, failures = [], []
            barrier.wait()
            for index in range(worker, count, workers):
                request_id = "sample-%d" % index
                request = {"request_id": request_id, "method": "synthetic_echo",
                           "params": {"index": index, "blob": blob}}
                before = time.perf_counter()
                try:
                    response = client.send_request(request, timeout)
                    elapsed = (time.perf_counter() - before) * 1000
                    if not response.get("ok") or response.get("request_id") != request_id or response.get("data") != request["params"]:
                        raise RuntimeError("echo payload mismatch")
                    samples.append(elapsed)
                except Exception as exc:
                    failures.append({"request_id": request_id, "type": type(exc).__name__, "error": str(exc),
                                     "outcome_unknown": getattr(exc, "outcome_unknown", None),
                                     "elapsed_ms": (time.perf_counter() - before) * 1000})
                if paced_rate:
                    time.sleep(max(0.0, 1.0 / paced_rate - (time.perf_counter() - before)))
            return samples, failures

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(caller, index) for index in range(workers)]
            pipe.send("begin")
            receive(pipe, process)
            cpu_start, wall_start = time.process_time(), time.perf_counter()
            barrier.wait()
            values = [future.result() for future in futures]
            elapsed = time.perf_counter() - wall_start
            parent_cpu = cpu_result(cpu_start, elapsed)
        pipe.send("finish")
        worker = receive(pipe, process)
        samples = [value for sample, _errors in values for value in sample]
        errors = [value for _samples, failed in values for value in failed]
        result = {"case": "rpc", "scenario": "paced" if paced_rate else "burst", "workers": workers,
                  "blob_bytes_per_direction": blob_bytes, "repeat": repeat, "paced_rate_per_second": paced_rate,
                  "requests": count, "success": len(samples), "failures": errors, "latency": distribution(samples),
                  "elapsed_seconds": elapsed, "success_per_second": len(samples) / elapsed,
                  "server_start_seconds_including_spawn": server_init, "warmup_requests": workers,
                  "warmup_seconds_including_first_client_open": warmed, "parent_cpu": parent_cpu,
                  "server_cpu": worker["cpu"], "server_status": worker["status"], "server_pid": ready["pid"]}
        client.stop()
        pipe.send("stop")
        receive(pipe, process)
        process.join(5.0)
        if process.exitcode != 0:
            raise RuntimeError("RPC worker exitcode %s" % process.exitcode)
    finally:
        client.stop()
        terminate(process, pipe)
        sampled = sampler.stop() if sampler is not None else None
        names = [client._name + suffix for suffix in ("", "_request", "_space")]
        names += [client._name + "_reply_%d" % index for index in range(config["rpc_slots"])]
        resources = resource_report(directory, names, source)
    result["resources"] = resources
    result["region_sampling"] = sampled
    return result


def push_case(source, directory, scenario, count, blob_bytes, repeat, delay_seconds=0.0, paced_rate=0.0,
              poll_seconds=0.05):
    """洪峰或慢回调采样；只重试明示未提交的背压，全部业务序号必须收到。"""
    _rpc_class, push_class, backpressure = modules(source)[:3]
    config = {"namespace": "push-bench-" + uuid.uuid4().hex, "directory": directory,
              "subscriber_id": "stable-benchmark", "push_poll_seconds": poll_seconds}
    if delay_seconds:
        config.update(market_push_bytes=16 * 1024, market_push_slots=8, publish_timeout_seconds=0.001)
    publisher = push_class("server", config, ACCOUNT)
    naming = push_class("client", config, ACCOUNT)
    process = pipe = None
    result = None
    try:
        started = time.perf_counter()
        publisher.start_publisher()
        process, pipe, ready = launch(push_worker, (config, source, count, delay_seconds))
        initialization = time.perf_counter() - started
        pipe.send("begin")
        receive(pipe, process)
        waits, retries = [], 0
        blob = "x" * blob_bytes
        cpu_start, wall_start = time.process_time(), time.perf_counter()
        for index in range(count):
            if paced_rate:
                time.sleep(max(0.0, wall_start + index / paced_rate - time.perf_counter()))
            data = {"index": index, "sent": time.perf_counter(), "blob": blob, "blob_bytes": blob_bytes}
            before = time.perf_counter()
            while True:
                try:
                    publisher.publish("benchmark:tick", data, message_id=str(index))
                    break
                except backpressure as exc:
                    if not process.is_alive():
                        receive(pipe, process, 1.0)
                        raise RuntimeError("consumer exited before all publications")
                    if not exc.retryable or time.perf_counter() - wall_start > 180.0:
                        raise
                    retries += 1
                    # 明确背压才重试，避免忙轮询把人工慢消费者变成 CPU 争抢测试。
                    time.sleep(0.0001)
            waits.append((time.perf_counter() - before) * 1000)
        publish_elapsed = time.perf_counter() - wall_start
        worker = receive(pipe, process)
        elapsed = time.perf_counter() - wall_start
        parent_cpu = cpu_result(cpu_start, elapsed)
        process.join(5.0)
        if process.exitcode != 0:
            raise RuntimeError("push worker exitcode %s" % process.exitcode)
        indices = [value[0] for value in worker["rows"]]
        result = {"case": "push", "scenario": scenario, "repeat": repeat, "blob_bytes": blob_bytes,
                  "issued": count, "committed": publisher.status()["published"], "received": len(indices),
                  "missing": len(set(range(count)) - set(indices)), "duplicates": len(indices) - len(set(indices)),
                  "out_of_order": sum(indices[index] <= indices[index - 1] for index in range(1, len(indices))),
                  "invalid_payloads": worker["invalid"], "explicit_retryable_backpressure": retries,
                  "publish_call_including_backpressure": distribution(waits),
                  "end_to_end_callback_completed": distribution([value[1] for value in worker["rows"]]),
                  "elapsed_seconds": elapsed, "publish_elapsed_seconds": publish_elapsed,
                  "received_per_second": len(indices) / elapsed, "committed_per_second": count / publish_elapsed,
                  "initialization_seconds_including_spawn": initialization, "callback_delay_seconds": delay_seconds,
                  "paced_rate_per_second": paced_rate,
                  "parent_cpu": parent_cpu, "consumer_cpu": worker["cpu"], "publisher_status": publisher.status(),
                  "consumer_status": worker["status"], "consumer_pid": ready["pid"], "config": config}
        result["consumer_health_lock_contentions"] = worker["health_lock_contentions"]
        publisher.check_health()
    finally:
        terminate(process, pipe)
        publisher.stop()
        resources = resource_report(directory, [publisher._name, publisher._space_name,
                                               naming._signal_name(naming._identity)], source)
    result["resources"] = resources
    return result


def recovery_case(source, directory, count, poll_seconds=0.05):
    """确认前缀后强杀独立消费者，在保留期内同身份重启并补收停机期间积压。"""
    push_class = modules(source)[1]
    config = {"namespace": "recovery-bench-" + uuid.uuid4().hex, "directory": directory,
              "subscriber_id": "stable-recovery", "push_poll_seconds": poll_seconds,
              "market_push_slots": max(4096, count + 1), "subscriber_retention_seconds": 30.0}
    publisher = push_class("server", config, ACCOUNT)
    naming = push_class("client", config, ACCOUNT)
    process = pipe = replacement = second_pipe = None
    prefix = count // 2
    waits = []

    def publish_range(start, stop):
        """完整记录两阶段合成消息，停机期间仍保持原始发送时间。"""
        for index in range(start, stop):
            before = time.perf_counter()
            publisher.publish("benchmark:tick", {"index": index, "sent": before, "blob": "x" * 1024,
                                                "blob_bytes": 1024}, message_id=str(index))
            waits.append((time.perf_counter() - before) * 1000)

    try:
        publisher.start_publisher()
        process, pipe, first_ready = launch(push_worker, (config, source, prefix, 0.0, True))
        pipe.send("begin")
        receive(pipe, process)
        before = time.perf_counter()
        cpu_start = time.process_time()
        publish_range(0, prefix)
        first = receive(pipe, process)
        if not first.get("checkpoint"):
            raise RuntimeError("consumer prefix was not confirmed")
        process.kill()
        process.join(5.0)
        stopped = time.perf_counter()
        publish_range(prefix, count)
        replacement, second_pipe, second_ready = launch(push_worker, (config, source, count - prefix))
        second_pipe.send("begin")
        receive(second_pipe, replacement)
        second = receive(second_pipe, replacement)
        recovered = time.perf_counter()
        replacement.join(5.0)
        if replacement.exitcode != 0:
            raise RuntimeError("recovery worker exitcode %s" % replacement.exitcode)
        rows = first["rows"] + second["rows"]
        indices = [value[0] for value in rows]
        result = {"case": "push_recovery", "issued": count, "committed": publisher.status()["published"],
                  "received": len(rows), "prefix_acknowledged": first["status"]["acknowledged"],
                  "resumed_acknowledged": second["status"]["acknowledged"],
                  "missing": len(set(range(count)) - set(indices)), "duplicates": len(indices) - len(set(indices)),
                  "out_of_order": sum(indices[index] <= indices[index - 1] for index in range(1, len(indices))),
                  "invalid_payloads": first["invalid"] + second["invalid"], "first_pid": first_ready["pid"],
                  "replacement_pid": second_ready["pid"], "killed_exitcode": process.exitcode,
                  "elapsed_seconds": recovered - before, "restart_seconds_including_spawn_and_backlog": recovered - stopped,
                  "received_per_second": len(rows) / (recovered - before), "publish_call": distribution(waits),
                  "end_to_end_callback_completed": distribution([value[1] for value in rows]),
                  "publisher_status": publisher.status(), "first_status": first["status"], "second_status": second["status"]}
        result.update(parent_cpu=cpu_result(cpu_start, recovered - before), first_consumer_cpu=first["cpu"],
                      second_consumer_cpu=second["cpu"])
        publisher.check_health()
    finally:
        terminate(process, pipe)
        terminate(replacement, second_pipe)
        publisher.stop()
        resources = resource_report(directory, [publisher._name, publisher._space_name,
                                               naming._signal_name(naming._identity)], source)
    result["resources"] = resources
    return result


def identity(source):
    """冻结 HEAD 和所有源码内容哈希；脏工作区按内容识别而不是只记录 Git 提交。"""
    root = Path(source).resolve().parent
    git = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(root), universal_newlines=True).strip()
    hashes = {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in sorted(Path(source).rglob("*.py"))}
    return {"head": git, "source_sha256": hashes, "source_dir": str(Path(source).resolve())}


def main():
    """运行指定矩阵并每个场景落盘，失败仍留下可审计的错误和源码身份。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", default=str(Path(__file__).resolve().parents[1] / "src"))
    parser.add_argument("--output", required=True)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--large-requests", type=int, default=40)
    parser.add_argument("--push-count", type=int, default=5000)
    parser.add_argument("--large-push-count", type=int, default=40)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--workers", nargs="+", type=int, default=[1, 4, 20])
    parser.add_argument("--cases", nargs="+", choices=["rpc", "push", "recovery"], default=["rpc", "push", "recovery"])
    parser.add_argument("--rpc-payloads", nargs="+", type=int, default=[0, 4096, 65536, 20 * 1024 * 1024])
    parser.add_argument("--paced-rpc-count", type=int, default=100)
    parser.add_argument("--paced-push-count", type=int, default=1000)
    parser.add_argument("--push-poll-seconds", type=float, default=0.05)
    parser.add_argument("--sample-resources", action="store_true")
    parser.add_argument("--push-scenarios", nargs="+", choices=["paced", "burst", "multi_kb", "large", "slow"],
                        default=["paced", "burst", "multi_kb", "large", "slow"])
    parser.add_argument("--directory-root", default="/dev/shm" if os.name != "nt" else None)
    args = parser.parse_args()
    if min(args.requests, args.large_requests, args.push_count, args.large_push_count, args.repeats, *args.workers) <= 0:
        parser.error("counts, repeats, and workers must be positive")
    source = str(Path(args.source_dir).resolve())
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    result = {"python": sys.version, "platform": platform.platform(), "parameters": vars(args),
              "identity_before": identity(source), "results": [], "errors": [],
              "boundaries": ["本机合成负载，无 QMT/Windows 实测、无交易、无断电持久化证明",
                             "样本成功不能外推长时间无丢失；稳定订阅恢复仅覆盖同发布代次和保留期",
                             "CPU 包含正文完整性核对与测量代码；父进程 RSS 是多场景累计历史峰值"]}

    def save():
        """原子替换结果文件，运行中断仍保留已完成矩阵。"""
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        temporary.replace(output)

    jobs = []
    for repeat in range(args.repeats):
        if "rpc" in args.cases:
            if args.paced_rpc_count > 0:
                jobs.append(("rpc", (1, args.paced_rpc_count, 4096, repeat, 20.0)))
            for blob_bytes in args.rpc_payloads:
                for workers in args.workers:
                    count = args.large_requests if blob_bytes >= 20 * 1024 * 1024 else args.requests
                    jobs.append(("rpc", (workers, count, blob_bytes, repeat, 0.0, args.sample_resources)))
        if "push" in args.cases:
            for scenario in args.push_scenarios:
                blob_bytes = {"paced": 1024, "burst": 1024, "multi_kb": 65536,
                              "large": 20 * 1024 * 1024, "slow": 1024}[scenario]
                count = args.large_push_count if scenario == "large" else (
                    args.paced_push_count if scenario == "paced" else args.push_count)
                jobs.append(("push", (scenario, count, blob_bytes, repeat, 0.0005 if scenario == "slow" else 0.0,
                                      100.0 if scenario == "paced" else 0.0, args.push_poll_seconds)))
    if "recovery" in args.cases:
        jobs.append(("recovery", (min(args.push_count, 2000), args.push_poll_seconds)))
    for number, (case, arguments) in enumerate(jobs, 1):
        try:
            with tempfile.TemporaryDirectory(prefix="bigqmt-shm-benchmark-", dir=args.directory_root) as directory:
                value = {"rpc": rpc_case, "push": push_case, "recovery": recovery_case}[case](source, directory, *arguments)
                result["results"].append(value)
                print(json.dumps({"job": number, "total": len(jobs), "case": case, "arguments": arguments,
                                  "success": value.get("success", value.get("received")),
                                  "clean": value["resources"]["clean"]}), flush=True)
        except Exception:
            result["errors"].append({"job": number, "case": case, "arguments": arguments, "error": traceback.format_exc()})
            print(json.dumps({"job": number, "case": case, "error": result["errors"][-1]["error"]}), flush=True)
        save()
    result["identity_after"] = identity(source)
    result["source_unchanged_during_measurement"] = result["identity_before"] == result["identity_after"]
    result["source_files_changed"] = [key for key, value in result["identity_before"]["source_sha256"].items()
                                      if result["identity_after"]["source_sha256"].get(key) != value]
    relevant = ["transports/shm_primitives.py", "transports/base.py"]
    if "rpc" in args.cases:
        relevant.extend(["transports/shm_transport.py", "redis_rpc.py"])
    if set(args.cases) & {"push", "recovery"}:
        relevant.extend(["shm_push_channel.py", "quote_push_channel.py"])
    transport_paths = {"src/bigqmt_signal_trader/" + suffix for suffix in relevant}
    result["transport_sources_unchanged_during_measurement"] = not (transport_paths & set(result["source_files_changed"]))
    result["valid"] = not result["errors"] and result["transport_sources_unchanged_during_measurement"] and all(
        value["resources"]["clean"] and not value.get("failures") and not value.get("missing")
        and not value.get("duplicates") and not value.get("out_of_order") and not value.get("invalid_payloads")
        for value in result["results"])
    save()
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    sys.exit(main())
