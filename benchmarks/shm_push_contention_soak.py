"""真实跨进程 SHM 积压恢复压力测试；只发送合成行情和交易事件，不接入交易账户。"""

import argparse
import json
import multiprocessing
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import traceback
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from bigqmt_signal_trader.quote_subscription_manager import BufferedAcknowledgedPublisher
from bigqmt_signal_trader.shm_push_channel import SharedMemoryQuotePushChannel
from bigqmt_signal_trader.transports.shm_primitives import cleanup

ACCOUNT = "isolated-push-contention"
TOPICS = ("quote:synthetic", "exec:synthetic")


def payload(lane, sequence):
    """构造可逐字段重算的行情/买卖撤单事件，覆盖浮点和超过双精度整数范围的编号。"""
    if lane == 1:
        return {"id": 2 ** 60 + sequence, "side": ("BUY", "SELL", "CANCEL")[sequence % 3],
                "quantity": 100, "price_micros": 114092000 + sequence}
    return [{"code": "%06d.SH" % (510000 + n), "last": (114092 + sequence % 1000 + n) / 1000,
             "bid": [1.234567890123, 1.234567890122], "ask": [1.234567890124, 1.234567890125],
             "amount": 34227.6, "time": 1792648056000 + sequence, "volume": 2 ** 54 + n}
            for n in range(20)]


def consumer(settings, index, pipe):
    """独立消费者执行慢回调和稳定身份重连，保留计数以验证全部消息连续且精确。"""
    channel = None
    counts = [0, 0]
    latency = []
    reconnects = 0
    started = time.monotonic()
    slow_done = False
    expected = None

    def received(topic, row):
        """慢回调只阻塞行情；成功返回前逐字段核对并检测重复/乱序。"""
        nonlocal slow_done
        lane = TOPICS.index(topic)
        if index == 0 and lane == 0 and not slow_done and time.monotonic() - started >= 3:
            slow_done = True
            time.sleep(1.5)
        assert row["seq"] == counts[lane], (index, lane, row["seq"], counts)
        assert row["body"] == payload(lane, row["seq"]), (index, lane, "precision")
        latency.append((time.perf_counter() - row["sent"]) * 1000)
        counts[lane] += 1

    def connect():
        """使用相同稳定订阅身份重连，沿用已确认游标而不修改共享协议。"""
        result = SharedMemoryQuotePushChannel("client", dict(settings, subscriber_id="peer-%d" % index), ACCOUNT)
        result.start_subscriber(TOPICS, received)
        return result

    try:
        channel = connect()
        pipe.send({"ready": True, "pid": os.getpid()})
        while True:
            if pipe.poll(.01):
                command = pipe.recv()
                if command == "abort":
                    break
                expected = command
            if channel.status()["error"]:
                raise RuntimeError(channel.status()["error"])
            elapsed = time.monotonic() - started
            if index == 0 and reconnects < 2 and elapsed >= 8 + reconnects * 12 and expected is None:
                channel.stop()
                time.sleep(.5)
                channel = connect()
                reconnects += 1
            if expected is not None and counts == expected:
                deadline = time.monotonic() + 5
                while channel.status()["pending_acknowledgements"]:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("final acknowledgement did not finish")
                    time.sleep(.01)
                pipe.send({"counts": counts, "reconnects": reconnects, "slow_callback": slow_done,
                           "latency": distribution(latency), "status": channel.status()})
                break
    except BaseException:
        pipe.send({"error": traceback.format_exc(), "counts": counts})
    finally:
        if channel is not None:
            channel.stop()
        pipe.close()


def distribution(values):
    """返回毫秒延迟分位和最大值；全部样本包括注入慢回调及重连造成的等待。"""
    ordered = sorted(values)
    return {"count": len(values), **{name: ordered[min(len(ordered) - 1, int(len(ordered) * q))]
            if ordered else None for name, q in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99), ("max_ms", 1))}}


def run(seconds, rate, consumers, gil_hold, gil_gap=.003):
    """seconds/rate 控制持续秒数及批速；gil_hold/gil_gap 为回调占用和让出 GIL 的秒数。"""
    context = multiprocessing.get_context("spawn")
    namespace = "contention-" + uuid.uuid4().hex
    directory = tempfile.mkdtemp(prefix="shm-contention-")
    settings = {"namespace": namespace, "directory": directory,
                "market_push_slots": 64, "trade_push_slots": 64,
                "market_push_bytes": 2 * 1024 * 1024, "trade_push_bytes": 1024 * 1024,
                "max_push_subscribers": 16, "push_poll_seconds": .005,
                "lock_timeout_seconds": 1, "init_timeout_seconds": 10}
    raw = SharedMemoryQuotePushChannel("server", settings, ACCOUNT)
    publisher = BufferedAcknowledgedPublisher(raw)
    peers = []
    stop = threading.Event()
    counts = [0, 0]
    samples = []
    result = {"passed": False, "namespace": namespace, "pid": os.getpid(), "settings": settings,
              "load": {"seconds": seconds, "rate": rate, "consumers": consumers,
                       "gil_hold_seconds": gil_hold, "gil_gap_seconds": gil_gap}}

    def contend():
        """周期执行 CPU 运算，让探活返回与 QMT 回调竞争 GIL；空隙允许主线程继续生产。"""
        while not stop.is_set():
            until = time.perf_counter() + gil_hold
            value = 1
            while time.perf_counter() < until:
                value = (value * 3 + 7) % 99991
            stop.wait(gil_gap)

    thread = threading.Thread(target=contend, daemon=True)
    previous_interval = sys.getswitchinterval()
    try:
        publisher.start_publisher()
        for index in range(consumers):
            parent, child = context.Pipe()
            process = context.Process(target=consumer, args=(settings, index, child))
            process.start()
            child.close()
            peers.append((process, parent))
            assert parent.poll(15), "consumer startup timeout"
            assert parent.recv().get("ready"), "consumer startup failed"
        sys.setswitchinterval(max(previous_interval, gil_hold * 2))
        thread.start()
        started = time.monotonic()
        next_sample = 0
        while time.monotonic() - started < seconds:
            elapsed = time.monotonic() - started
            for lane, target in ((1, int(elapsed * max(1, rate / 10))), (0, int(elapsed * rate))):
                for _ in range(min(100, target - counts[lane])):
                    sequence = counts[lane]
                    publisher.publish(TOPICS[lane], {"seq": sequence, "sent": time.perf_counter(),
                                                    "body": payload(lane, sequence)})
                    counts[lane] += 1
            if elapsed >= next_sample:
                state = publisher.status()
                samples.append({"elapsed": elapsed, "state": state, "shared": raw.status()})
                print(json.dumps({"elapsed": round(elapsed, 2), "sent": counts,
                                  "pending": state["local_pending"], "overflow": state["overflow_count"]}), flush=True)
                if state["sticky_error"]:
                    raise RuntimeError(state["sticky_error"])
                next_sample += 5
            for process, pipe in peers:
                if pipe.poll():
                    raise RuntimeError(pipe.recv())
            time.sleep(.002)
        drain_started = time.monotonic()
        for process, pipe in peers:
            pipe.send(counts)
        results = []
        for process, pipe in peers:
            assert pipe.poll(30), "queue failed to drain without publisher restart"
            row = pipe.recv()
            assert row.get("counts") == counts and not row.get("error"), row
            results.append(row)
        deadline = time.monotonic() + 5
        while any(publisher.status()["local_pending"].values()):
            assert time.monotonic() < deadline, "publisher queue did not clear"
            time.sleep(.01)
        final = publisher.status()
        assert final["healthy"] and final["overflow_count"] == 0, final
        assert results[0]["slow_callback"] and results[0]["reconnects"] >= 1
        result.update(passed=True, sent=counts, elapsed=time.monotonic() - started,
                      drain_seconds=time.monotonic() - drain_started, consumers=results,
                      final=final, shared=raw.status(), samples=samples)
    except BaseException:
        result.update(error=traceback.format_exc(), sent=counts, final=publisher.status(),
                      shared=raw.status(), samples=samples)
    finally:
        stop.set()
        if thread.is_alive():
            thread.join(3)
        sys.setswitchinterval(previous_interval)
        for process, pipe in peers:
            if process.is_alive():
                try:
                    pipe.send("abort")
                except (OSError, EOFError):
                    pass
                process.join(3)
                if process.is_alive():
                    process.terminate()
                    process.join(3)
            pipe.close()
        names = {raw._name, raw._space_name}
        names.update(raw._signal_name(identity) for identity in raw._signals)
        publisher.stop()
        for name in names:
            cleanup(name, directory)
        Path(directory).rmdir()
    return result


def main():
    """独立运行并保存完整证据；任何精度、序号、恢复或积压失败返回非零退出码。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=180)
    parser.add_argument("--rate", type=float, default=80)
    parser.add_argument("--consumers", type=int, default=3)
    parser.add_argument("--gil-hold", type=float, default=.06)
    parser.add_argument("--gil-gap", type=float, default=.003)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run(args.seconds, args.rate, args.consumers, args.gil_hold, args.gil_gap)
    args.output.write_text(json.dumps(result, indent=2))
    print(json.dumps({k: v for k, v in result.items() if k != "samples"}), flush=True)
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
