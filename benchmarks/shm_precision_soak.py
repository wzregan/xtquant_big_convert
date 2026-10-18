"""独立进程共享内存长时压力及精度验收，不连接 QMT、不执行任何交易。

RPC 正文与两种编码的推送正文只经过真实共享内存；Pipe 只传控制及计数。
逐字段类型、整数、IEEE754 位模式和全文摘要同时校验，避免近似比较掩盖误差。
"""

import argparse
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import random
import signal
import struct
import sys
import threading
import time
import traceback
import uuid


ACCOUNT = "synthetic-shm-precision-soak"
CONTEXT = multiprocessing.get_context("spawn")
TOPICS = {"quote": "precision:quote", "exec": "exec:precision"}


def load_modules(source):
    """从明确的冻结源码路径导入，避免已安装版本污染长时验证。"""
    if source not in sys.path:
        sys.path.insert(0, source)
    from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport
    from bigqmt_signal_trader.shm_push_channel import SharedMemoryQuotePushChannel
    from bigqmt_signal_trader.quote_subscription_manager import BufferedAcknowledgedPublisher
    from bigqmt_signal_trader.transports.shm_primitives import cleanup
    return SharedMemoryTransport, SharedMemoryQuotePushChannel, BufferedAcknowledgedPublisher, cleanup


def exact_form(value):
    """生成包含类型和浮点位模式的规范表示；不接受隐式数值类型转换。"""
    if isinstance(value, dict):
        return ["dict", [[key, exact_form(value[key])] for key in sorted(value)]]
    if isinstance(value, list):
        return ["list", [exact_form(item) for item in value]]
    if type(value) is float:
        return ["float64", struct.pack("!d", value).hex()]
    if value is None:
        return ["none"]
    if type(value) not in (str, int, bool):
        raise AssertionError("unsupported tested value type: %s" % type(value).__name__)
    return [type(value).__name__, value]


def digest(value):
    """对带类型的规范内容取摘要，浮点正负零也必须不同。"""
    data = json.dumps(exact_form(value), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def assert_exact(actual, expected, path="payload"):
    """递归核对全部字段；浮点用完整二进制位比较，禁止 approx 容差。"""
    if type(actual) is not type(expected):
        raise AssertionError("%s type mismatch: %s != %s" % (path, type(actual).__name__, type(expected).__name__))
    if isinstance(expected, dict):
        if actual.keys() != expected.keys():
            raise AssertionError("%s keys mismatch" % path)
        for key in expected:
            assert_exact(actual[key], expected[key], path + "." + key)
    elif isinstance(expected, list):
        if len(actual) != len(expected):
            raise AssertionError("%s list length mismatch" % path)
        for index, value in enumerate(expected):
            assert_exact(actual[index], value, "%s[%d]" % (path, index))
    elif type(expected) is float:
        if struct.pack("!d", actual) != struct.pack("!d", expected):
            raise AssertionError("%s IEEE754 bits mismatch" % path)
    elif actual != expected:
        raise AssertionError("%s exact value mismatch" % path)


def payload(seed, stream, sequence, blob_bytes=0):
    """按种子和序号重建正文；接收端不通过控制通道取得预期正文。"""
    token = hashlib.sha256(("%s:%s:%s" % (seed, stream, sequence)).encode("utf-8")).hexdigest()
    # 覆盖 JSON 常见 2**53 边界、int64、微元价格、负零、次正规数和非 ASCII 文本。
    return {"seed": seed, "stream": stream, "sequence": sequence,
            "integers": [2 ** 53 - 1, 2 ** 53, 2 ** 53 + 1, 2 ** 63 - 1, -(2 ** 63),
                         1700000000000000000 + sequence, 1700000000000 + sequence,
                         sequence * 1000001, -(sequence * 999983)],
            "floats": [0.0, -0.0, 0.1, -0.1, 5e-324, -5e-324, 1.7976931348623157e308,
                       2.2250738585072014e-308, float.fromhex("0x1.0000000000001p+0"),
                       (sequence % 1000000) / 1000000.0],
            "decimal_strings": ["0.1000000000000000000000000001", "9007199254740993.000001", "-0.000001"],
            "nested": {"flags": [True, False, None], "utf8": "共享内存精度校验📈\u0000\n" + token,
                       "empty_list": [], "empty_dict": {}},
            "token": token, "blob_bytes": blob_bytes,
            "blob": (token * ((blob_bytes + len(token) - 1) // len(token)))[:blob_bytes]}


def envelope(seed, stream, sequence, large_every, blob_bytes):
    """创建确定性正文与发送时间；大报文周期按每条流的序号独立计算。"""
    size = blob_bytes if large_every and (sequence + 1) % large_every == 0 else 0
    result = {"body": payload(seed, stream, sequence, size), "sent_ns": time.monotonic_ns()}
    result["sha256"] = digest(result)
    return result


def verify_envelope(value, seed, stream, sequence, large_every, blob_bytes):
    """核对协议字段、全文摘要与独立重建正文，返回端到端毫秒延迟。"""
    if type(value) is not dict or set(value) != {"body", "sent_ns", "sha256"}:
        raise AssertionError("envelope shape mismatch")
    if type(value["sent_ns"]) is not int or value["sent_ns"] <= 0:
        raise AssertionError("monotonic timestamp type/value mismatch")
    if digest({"body": value["body"], "sent_ns": value["sent_ns"]}) != value["sha256"]:
        raise AssertionError("envelope digest mismatch")
    size = blob_bytes if large_every and (sequence + 1) % large_every == 0 else 0
    assert_exact(value["body"], payload(seed, stream, sequence, size))
    return (time.monotonic_ns() - value["sent_ns"]) / 1000000.0


class Statistics:
    """线程安全流式计数及固定容量水库采样，内存不随消息总量增长。"""

    def __init__(self, seed):
        """固定种子仅影响延迟抽样，不影响逐条完整校验。"""
        self.lock = threading.Lock()
        self.counts = {}
        self.samples = []
        self.total = 0
        self.maximum = 0.0
        self.rng = random.Random(seed)
        self.first_error = None
        self.big_messages = 0

    def success(self, stream, milliseconds, large=False):
        """成功验证才增加计数；每条消息仍校验，只有延迟统计做有界抽样。"""
        with self.lock:
            self.counts[stream] = self.counts.get(stream, 0) + 1
            self.total += 1
            self.big_messages += int(large)
            self.maximum = max(self.maximum, milliseconds)
            if len(self.samples) < 4096:
                self.samples.append(milliseconds)
            else:
                index = self.rng.randrange(self.total)
                if index < 4096:
                    self.samples[index] = milliseconds

    def fail(self):
        """只保留首个完整异常，防止错误风暴使审计进程耗尽内存。"""
        with self.lock:
            if self.first_error is None:
                self.first_error = traceback.format_exc()

    def snapshot(self):
        """返回一致的计数快照；分位数明确来自最多 4096 条延迟水库样本。"""
        with self.lock:
            ordered = sorted(self.samples)
            return {"verified": self.total, "counts": dict(self.counts), "large_messages": self.big_messages,
                    "error": self.first_error, "latency_ms": {
                        "sample_count": len(ordered), "sample_kind": "reservoir_max_4096",
                        "p50": ordered[int((len(ordered) - 1) * .5)] if ordered else None,
                        "p99": ordered[int((len(ordered) - 1) * .99)] if ordered else None,
                        "max_all_messages": self.maximum}}


def process_sample(pid):
    """采样 Linux 当前 RSS、FD 与线程数；历史峰值单独记录，避免误判为泄漏。"""
    result = {"pid": pid}
    try:
        for line in Path("/proc/%s/status" % pid).read_text().splitlines():
            key, _, value = line.partition(":")
            if key in ("VmRSS", "VmHWM", "Threads"):
                result[key] = int(value.split()[0]) * (1024 if key != "Threads" else 1)
        result["fd_count"] = len(list(Path("/proc/%s/fd" % pid).iterdir()))
    except (FileNotFoundError, PermissionError):
        result["unavailable"] = True
    return result


def rpc_server(config, source, pipe, options):
    """独立 RPC 服务核验每个请求再回显，服务自身也记录成功总量。"""
    transport = None
    stats = Statistics(options["seed"])
    try:
        transport = load_modules(source)[0](account_id=ACCOUNT, **config)

        def received(request):
            """核验线程编号及序号对应的合成正文，禁止返回未经核验的数据。"""
            try:
                body = request["params"]["body"]
                latency = verify_envelope(request["params"], options["seed"], body["stream"],
                                          body["sequence"], options["rpc_large_every"], options["blob_bytes"])
                stats.success(body["stream"], latency, bool(body["blob_bytes"]))
                return {"ok": True, "request_id": request["request_id"], "data": request["params"]}
            except BaseException:
                stats.fail()
                raise

        transport.start_receiving(received)
        pipe.send({"ready": True, "pid": os.getpid()})
        while True:
            command = pipe.recv()
            if command["op"] == "stop":
                transport.stop()
                pipe.send({"stopped": True, "statistics": stats.snapshot()})
                break
            transport.check_health()
            pipe.send({"statistics": stats.snapshot(), "status": transport.status()})
    except BaseException:
        pipe.send({"error": traceback.format_exc()})
    finally:
        if transport is not None:
            transport.stop()
        pipe.close()


def push_consumer(config, source, pipe, options):
    """独立消费者逐条校验并维护每个日志顺序，ACK 由真实通道在回调成功后提交。"""
    channel = None
    stats = Statistics(options["seed"])
    counts = {"quote": 0, "exec": 0}
    last_pause = [time.monotonic()]
    pauses = [0]
    try:
        channel = load_modules(source)[1]("client", config, ACCOUNT)

        def received(topic, value):
            """消费暂停制造真实环形日志背压；正常回调必须完全匹配下一序号。"""
            try:
                lane = "exec" if topic.startswith("exec:") else "quote"
                if topic != TOPICS[lane]:
                    raise AssertionError("unexpected topic")
                sequence = counts[lane]
                stream = config["push_codec"] + ":" + lane
                latency = verify_envelope(value, options["seed"], stream, sequence,
                                          options["push_large_every"], options["blob_bytes"])
                if time.monotonic() - last_pause[0] >= options["slow_every_seconds"]:
                    time.sleep(options["slow_seconds"])
                    last_pause[0] = time.monotonic()
                    pauses[0] += 1
                counts[lane] += 1
                stats.success(lane, latency, bool(value["body"]["blob_bytes"]))
            except BaseException:
                stats.fail()
                raise

        channel.start_subscriber(list(TOPICS.values()), received)
        pipe.send({"ready": True, "pid": os.getpid()})
        while True:
            command = pipe.recv()
            if command["op"] == "drain":
                target = command["counts"]
                deadline = time.monotonic() + options["drain_seconds"]
                while True:
                    state = stats.snapshot()
                    if state["error"]:
                        raise AssertionError(state["error"])
                    if (all(state["counts"].get(lane, 0) == target[lane] for lane in target)
                            and channel.status()["acknowledged"] == sum(target.values())):
                        break
                    if time.monotonic() > deadline:
                        raise RuntimeError("consumer drain timeout: expected=%s actual=%s" % (target, state))
                    try:
                        channel.check_health()
                    except Exception as exc:
                        if not getattr(exc, "retryable", False):
                            raise
                    time.sleep(.01)
            if command["op"] == "stop":
                channel.stop()
                pipe.send({"stopped": True, "statistics": stats.snapshot(), "slow_pauses": pauses[0]})
                break
            try:
                channel.check_health()
            except Exception as exc:
                if not getattr(exc, "retryable", False):
                    raise
            pipe.send({"statistics": stats.snapshot(), "status": channel.status(), "slow_pauses": pauses[0]})
    except BaseException:
        pipe.send({"error": traceback.format_exc()})
    finally:
        if channel is not None:
            channel.stop()
        pipe.close()


def receive(pipe, process, timeout=15.0):
    """有界等待状态并传播真实子进程错误，不能把提前退出视为完成。"""
    if not pipe.poll(timeout):
        raise RuntimeError("control timeout pid=%s alive=%s" % (process.pid, process.is_alive()))
    result = pipe.recv()
    if result.get("error"):
        raise RuntimeError(result["error"])
    return result


def launch(target, config, source, options):
    """使用 spawn 创建新解释器和独立句柄，控制管道只传状态。"""
    parent, child = CONTEXT.Pipe()
    process = CONTEXT.Process(target=target, args=(config, source, child, options))
    process.start()
    child.close()
    try:
        ready = receive(parent, process)
        if not ready.get("ready"):
            raise RuntimeError("worker not ready")
    except BaseException:
        process.kill()
        process.join(5)
        parent.close()
        raise
    return process, parent


def source_manifest(source):
    """记录整个指定源码树的 Python 文件摘要，运行结束必须与开始相同。"""
    result = {}
    for path in sorted(Path(source).rglob("*.py")):
        result[str(path.relative_to(source))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"files": result, "tree_sha256": digest(result)}


def atomic_json(path, value):
    """原子替换进度文件并落盘，断电或中断不留下半个 JSON。"""
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def cleanup_resources(directory, names, source):
    """只清理本轮唯一目录和明确拥有的名字，并核对动态大报文及信号残留。"""
    cleanup = load_modules(source)[3]
    root = Path(directory)
    expected = {"bigqmt_" + hashlib.sha256(name.encode("utf-8")).hexdigest() + ".region" for name in names}
    before = sorted(path.name for path in root.iterdir())
    unexpected = [name for name in before if name.endswith(".region") and name not in expected]
    semaphores = []
    if os.name != "nt":
        from bigqmt_signal_trader.transports.shm_primitives import _semaphore_name
        semaphores = [Path("/dev/shm") / ("sem." + _semaphore_name(name, directory).decode("ascii")[1:]) for name in names]
    for name in names:
        cleanup(name, directory)
    after = sorted(path.name for path in root.iterdir())
    remaining = [str(path) for path in semaphores if path.exists()]
    return {"files_before_cleanup": before, "unexpected_dynamic_regions": unexpected,
            "files_after_cleanup": after, "named_semaphores_after_cleanup": remaining,
            "clean": not unexpected and not after and not remaining}


def run(args):
    """执行规定时长混合负载，完整排空、核对后才宣布通过。"""
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=False)
    directory = output / "shm"
    directory.mkdir()
    source = str(Path(args.source_dir).resolve())
    options = vars(args).copy()
    result = {"state": "starting", "options": options, "pid": os.getpid(),
              "started_unix_seconds": time.time(), "source_before": source_manifest(source),
              "precision_contract": "type_exact; integer_exact; float64_bits_exact; full_sha256; per_lane_sequence_exact",
              "rpc_codec": "json", "push_codecs": ["json", "msgpack"], "exceptions": []}
    atomic_json(output / "result.json", result)
    stop = threading.Event()
    started = threading.Event()
    interrupted = threading.Event()
    stats = Statistics(args.seed)
    producer_stats = {codec: Statistics(args.seed + 1) for codec in ("json", "msgpack")}
    control = {"start": None, "deadline": None}
    children, publishers, names, threads = {}, {}, [], []
    final_counts = {}
    client = None
    old_signals = {}
    begin = None

    def interrupted_signal(signum, frame):
        """外部终止保留 interrupted 状态，绝不把不足时长的样本当作通过。"""
        interrupted.set()
        stop.set()

    for signum in (signal.SIGTERM, signal.SIGINT):
        old_signals[signum] = signal.signal(signum, interrupted_signal)

    def event(value):
        """审计事件顺序追加，消息正文和全量延迟不留在内存中。"""
        with (output / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n")

    try:
        rpc_class, push_class, buffered_class, _cleanup = load_modules(source)
        rpc_config = {"namespace": "precision-rpc-" + uuid.uuid4().hex, "directory": str(directory),
                      "rpc_slots": 32, "rpc_slot_bytes": 65536}
        client = rpc_class(account_id=ACCOUNT, **rpc_config)
        names.extend([client._name + suffix for suffix in ("", "_request", "_space")])
        names.extend([client._name + "_reply_%d" % index for index in range(32)])
        children["rpc"] = launch(rpc_server, rpc_config, source, options)
        for codec in ("json", "msgpack"):
            config = {"namespace": "precision-push-" + codec + "-" + uuid.uuid4().hex,
                      "directory": str(directory), "push_codec": codec, "subscriber_id": "precision-consumer",
                      "market_push_slots": 128, "trade_push_slots": 64,
                      "market_push_bytes": 8 * 1024 * 1024, "trade_push_bytes": 4 * 1024 * 1024,
                      "publish_timeout_seconds": .005, "push_poll_seconds": .01,
                      "local_quote_queue_items": 4096, "local_exec_queue_items": 1024,
                      "local_quote_queue_bytes": 64 * 1024 * 1024,
                      "local_exec_queue_bytes": 16 * 1024 * 1024}
            raw = push_class("server", config, ACCOUNT)
            naming = push_class("client", config, ACCOUNT)
            names.extend([raw._name, raw._space_name, naming._signal_name(naming._identity)])
            publisher = buffered_class(raw, config)
            publishers[codec] = publisher
            publisher.start_publisher()
            children[codec] = launch(push_consumer, config, source, options)

        def caller(worker):
            """二十个长寿命线程按 1/5/20 活跃数轮换，响应必须匹配请求编号与完整正文。"""
            sequence = 0
            started.wait()
            try:
                while not stop.is_set() and time.monotonic() < control["deadline"]:
                    now = time.monotonic()
                    active = (1, 5, 20)[int((now - control["start"]) / args.concurrency_epoch_seconds) % 3]
                    if worker >= active:
                        stop.wait(.02)
                        continue
                    value = envelope(args.seed, "rpc:%s" % worker, sequence, args.rpc_large_every, args.blob_bytes)
                    request_id = "precision-%s-%s" % (worker, sequence)
                    response = client.send_request({"request_id": request_id, "method": "precision_echo", "params": value}, 15.0)
                    if response.get("ok") is not True or response.get("request_id") != request_id:
                        raise AssertionError("RPC response request identity mismatch")
                    assert_exact(response["data"], value)
                    latency = verify_envelope(response["data"], args.seed, "rpc:%s" % worker,
                                              sequence, args.rpc_large_every, args.blob_bytes)
                    stats.success(str(worker), latency, bool(value["body"]["blob_bytes"]))
                    sequence += 1
                    stop.wait(max(0.0, active / args.rpc_rate - (time.monotonic() - now)))
            except BaseException:
                stats.fail()
                stop.set()

        def producer(codec):
            """按单一生产顺序交替两条日志，真实发送线程遇背压时保留同一队首编号重试。"""
            counts = {"quote": 0, "exec": 0}
            emitted = 0
            started.wait()
            try:
                while not stop.is_set() and time.monotonic() < control["deadline"]:
                    due = control["start"] + emitted / args.push_rate
                    if stop.wait(max(0.0, due - time.monotonic())):
                        break
                    if time.monotonic() >= control["deadline"]:
                        break
                    lane = "exec" if emitted % 10 == 0 else "quote"
                    sequence = counts[lane]
                    value = envelope(args.seed, codec + ":" + lane, sequence, args.push_large_every, args.blob_bytes)
                    publishers[codec].publish(TOPICS[lane], value)
                    counts[lane] += 1
                    emitted += 1
                    producer_stats[codec].success(lane, 0.0, bool(value["body"]["blob_bytes"]))
                    if emitted % 100 == 0:
                        state = publishers[codec].status()
                        if state["sticky_error"] or not state["sender_alive"]:
                            raise RuntimeError("background sender failed: %s" % state)
            except BaseException:
                producer_stats[codec].fail()
                stop.set()
            finally:
                final_counts[codec] = counts

        for worker in range(20):
            threads.append(threading.Thread(target=caller, args=(worker,), name="precision-rpc-%s" % worker))
        for codec in publishers:
            threads.append(threading.Thread(target=producer, args=(codec,), name="precision-producer-" + codec))
        for thread in threads:
            thread.start()
        begin = time.monotonic()
        control.update(start=begin, deadline=begin + args.duration_seconds)
        started.set()
        result["state"] = "running"
        result["load_started_unix_seconds"] = time.time()
        result["child_pids"] = {key: value[0].pid for key, value in children.items()}
        atomic_json(output / "result.json", result)
        next_progress = begin
        while time.monotonic() < control["deadline"] and not stop.is_set():
            if time.monotonic() < next_progress:
                stop.wait(min(.2, next_progress - time.monotonic()))
                continue
            worker_states = {}
            for key, (process, pipe) in children.items():
                pipe.send({"op": "status"})
                worker_states[key] = receive(pipe, process)
                if worker_states[key]["statistics"]["error"]:
                    raise RuntimeError(worker_states[key]["statistics"]["error"])
            progress = {"state": "running", "unix_seconds": time.time(), "elapsed_seconds": time.monotonic() - begin,
                        "target_seconds": args.duration_seconds, "rpc": stats.snapshot(),
                        "producers": {key: value.snapshot() for key, value in producer_stats.items()},
                        "publishers": {key: value.status() for key, value in publishers.items()}, "workers": worker_states,
                        "resources": {key: process_sample(process.pid) for key, (process, _) in children.items()}}
            progress["resources"]["parent"] = process_sample(os.getpid())
            atomic_json(output / "progress.json", progress)
            event(progress)
            next_progress = time.monotonic() + args.progress_seconds
        for thread in threads:
            thread.join(20.0)
        if any(thread.is_alive() for thread in threads):
            raise RuntimeError("load threads did not stop")
        elapsed = time.monotonic() - begin
        result["load_elapsed_seconds"] = elapsed
        if interrupted.is_set():
            raise InterruptedError("external signal interrupted soak")
        for state in [stats.snapshot()] + [value.snapshot() for value in producer_stats.values()]:
            if state["error"]:
                raise RuntimeError(state["error"])
        if elapsed < args.duration_seconds:
            raise AssertionError("load ended before requested duration")
        result["state"] = "draining"
        atomic_json(output / "result.json", result)
        drained = {}
        for codec, publisher in publishers.items():
            deadline = time.monotonic() + args.drain_seconds
            while any(publisher.status()["local_pending"].values()):
                publisher.check_health()
                if time.monotonic() > deadline:
                    raise RuntimeError("publisher drain timeout")
                time.sleep(.01)
            process, pipe = children[codec]
            pipe.send({"op": "drain", "counts": final_counts[codec]})
            drained[codec] = receive(pipe, process, args.drain_seconds + 5)
            state = publisher.status()
            total = sum(final_counts[codec].values())
            if state["local_queued"] != total or state["shared_committed"] != total or state["overflow_count"]:
                raise AssertionError("publisher count/overflow mismatch: %s" % state)
            if drained[codec]["statistics"]["verified"] != total or drained[codec]["status"]["acknowledged"] != total:
                raise AssertionError("push ACK verification mismatch")
        process, pipe = children["rpc"]
        pipe.send({"op": "status"})
        server = receive(pipe, process)
        if server["statistics"]["verified"] != stats.snapshot()["verified"]:
            raise AssertionError("RPC server/client count mismatch")
        result.update(state="passed", rpc=stats.snapshot(), rpc_server=server, consumers=drained,
                      producers={key: value.snapshot() for key, value in producer_stats.items()},
                      publishers={key: value.status() for key, value in publishers.items()},
                      integrity={"missing": 0, "duplicates": 0, "out_of_order": 0, "precision_mismatches": 0,
                                 "corrupt_payloads": 0})
    except BaseException:
        result["state"] = "interrupted" if interrupted.is_set() else "failed"
        result["exceptions"].append(traceback.format_exc())
    finally:
        stop.set()
        started.set()
        for thread in threads:
            thread.join(20.0)
        if client is not None:
            try:
                client.stop()
            except BaseException:
                result["exceptions"].append(traceback.format_exc())
        exits = {}
        for key, (process, pipe) in children.items():
            forced = False
            try:
                if process.is_alive():
                    pipe.send({"op": "stop"})
                    receive(pipe, process, 10.0)
                process.join(5.0)
            except (BrokenPipeError, EOFError, RuntimeError, OSError):
                result["exceptions"].append(traceback.format_exc())
            if process.is_alive():
                forced = True
                process.kill()
                process.join(5.0)
            exits[key] = {"exitcode": process.exitcode, "forced": forced}
            pipe.close()
        for publisher in publishers.values():
            try:
                publisher.stop()
            except BaseException:
                result["exceptions"].append(traceback.format_exc())
        result["process_exits"] = exits
        try:
            result["resources"] = cleanup_resources(str(directory), names, source)
            result["source_after"] = source_manifest(source)
            result["source_unchanged"] = result["source_before"] == result["source_after"]
            if (not result["resources"]["clean"] or not result["source_unchanged"]
                    or any(value["exitcode"] != 0 or value["forced"] for value in exits.values())):
                result["exceptions"].append("cleanup/process exit/source immutability acceptance failed")
        except BaseException:
            result["exceptions"].append(traceback.format_exc())
        if result["state"] == "passed" and result["exceptions"]:
            result["state"] = "failed"
        result["finished_unix_seconds"] = time.time()
        result["total_elapsed_seconds"] = time.monotonic() - begin if begin is not None else None
        atomic_json(output / "result.json", result)
        atomic_json(output / "progress.json", {"state": result["state"], "result_path": str(output / "result.json"),
                                              "elapsed_seconds": result["total_elapsed_seconds"]})
        event({"state": result["state"], "unix_seconds": time.time(), "exceptions": result["exceptions"]})
        for signum, previous in old_signals.items():
            signal.signal(signum, previous)
    print(json.dumps({"state": result["state"], "result_path": str(output / "result.json"),
                      "elapsed_seconds": result["total_elapsed_seconds"], "exceptions": result["exceptions"]}, ensure_ascii=False))
    return 0 if result["state"] == "passed" else 2


def main():
    """解析离线压力参数；输出目录必须全新，避免覆盖既有审计证据。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-seconds", type=float, default=7200.0)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--source-dir", default=str(Path(__file__).resolve().parents[1] / "src"))
    parser.add_argument("--seed", type=int, default=20261016)
    parser.add_argument("--rpc-rate", type=float, default=100.0)
    parser.add_argument("--push-rate", type=float, default=1000.0)
    parser.add_argument("--blob-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--rpc-large-every", type=int, default=1000)
    parser.add_argument("--push-large-every", type=int, default=10000)
    parser.add_argument("--slow-every-seconds", type=float, default=30.0)
    parser.add_argument("--slow-seconds", type=float, default=.2)
    parser.add_argument("--concurrency-epoch-seconds", type=float, default=60.0)
    parser.add_argument("--progress-seconds", type=float, default=10.0)
    parser.add_argument("--drain-seconds", type=float, default=60.0)
    args = parser.parse_args()
    for key in ("duration_seconds", "rpc_rate", "push_rate", "slow_every_seconds", "slow_seconds",
                "concurrency_epoch_seconds", "progress_seconds", "drain_seconds"):
        if not math.isfinite(getattr(args, key)) or getattr(args, key) <= 0:
            parser.error("%s must be finite and positive" % key)
    if min(args.blob_bytes, args.rpc_large_every, args.push_large_every) < 0:
        parser.error("payload sizes and intervals cannot be negative")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
