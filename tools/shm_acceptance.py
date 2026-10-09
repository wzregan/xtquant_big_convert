#!/usr/bin/env python
# coding: utf-8
"""用标准库及本仓库代码验收独立进程 SHM；不加载 QMT，不访问券商或执行交易。"""

import argparse
import concurrent.futures
import json
import os
import platform
import subprocess
import sys
import tempfile
import threading
import time
import uuid


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.shm_push_channel import (  # noqa: E402
    PushBackpressureError, SharedMemoryQuotePushChannel,
)
from bigqmt_signal_trader.transports.shm_primitives import cleanup  # noqa: E402
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport  # noqa: E402


ACCOUNT = "__SHM_ACCEPTANCE_SYNTHETIC__"
TOPICS = ["quotes:synthetic", "exec:order"]


def _write_json(path, value):
    """原子写入 UTF-8 验收状态或报告，避免读取半个 JSON 文档。"""
    temporary = path + ".tmp"
    with open(temporary, "w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
    os.replace(temporary, path)


def _rpc_options(settings):
    """只传递两端匹配的基础配置，其余 RPC 参数采用生产默认值。"""
    return {"namespace": settings["namespace"], "directory": settings["directory"]}


def _worker(settings_path):
    """新解释器通过命名对象建立服务；仅允许合成回显和合成推送两种操作。"""
    with open(settings_path, "r", encoding="utf-8") as stream:
        settings = json.load(stream)
    publisher = SharedMemoryQuotePushChannel("server", settings, ACCOUNT)
    server = SharedMemoryTransport(account_id=ACCOUNT, **_rpc_options(settings))

    def handle(request):
        """处理验收数据，绝不委托给行情、策略或交易业务处理器。"""
        params = request.get("params") or {}
        if request.get("method") == "acceptance_echo":
            result = params
        elif request.get("method") == "acceptance_emit":
            index = int(params["index"])
            topic = TOPICS[index % len(TOPICS)]
            publisher.publish(topic, {"index": index}, message_id="acceptance-%d" % index)
            result = {"topic": topic, "index": index}
        else:
            raise ValueError("only synthetic acceptance methods are available")
        return {"ok": True, "request_id": request["request_id"], "data": result}

    try:
        publisher.start_publisher()
        server.start_receiving(handle)
        _write_json(settings["ready_path"], {"pid": os.getpid(), "ready": True})
        # 标准输入只控制验收进程退出；被验收的业务数据全部经过 SHM RPC/推送。
        for line in sys.stdin:
            if line.strip() == "stop":
                break
    finally:
        server.stop()
        publisher.stop()
    return 0


def _wait(predicate, timeout, description, worker=None):
    """有界等待进程状态或确认游标，子进程提前退出时立即报告失败。"""
    deadline = time.monotonic() + timeout
    while not predicate():
        if worker is not None and worker.poll() is not None:
            output, errors = worker.communicate()
            raise RuntimeError("worker exited: %s\n%s\n%s" % (worker.returncode, output, errors))
        if time.monotonic() >= deadline:
            raise RuntimeError("acceptance timeout: " + description)
        time.sleep(0.005)


def _call(client, method, params, timeout):
    """发送带独立编号的合成请求，并校验服务返回的业务成功标记。"""
    response = client.send_request({"schema_version": 1, "request_id": uuid.uuid4().hex,
                                    "account_id": ACCOUNT, "method": method, "params": params}, timeout)
    if not response.get("ok"):
        raise RuntimeError("synthetic RPC failed: %s" % response.get("error"))
    return response.get("data")


def _check_client(channel):
    """健康锁竞争只暂缓检查；其它协议故障立即让验收失败。"""
    try:
        channel.check_health()
    except PushBackpressureError:
        pass


def _stop_worker(worker):
    """先请求正常关闭；超时仅终止本脚本创建的子进程。"""
    if worker is None:
        return
    try:
        output, errors = worker.communicate(input="stop\n" if worker.poll() is None else None, timeout=5)
    except subprocess.TimeoutExpired:
        worker.terminate()
        try:
            output, errors = worker.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            worker.kill()
            output, errors = worker.communicate(timeout=5)
    if worker.returncode:
        raise RuntimeError("acceptance worker failed: %s\n%s\n%s" % (worker.returncode, output, errors))


def _accept(args):
    """运行独立进程 RPC、推送确认和同身份停止恢复，返回完整结果字典。"""
    namespace = args.namespace_prefix + "-" + uuid.uuid4().hex
    worker = None
    clients = []
    with tempfile.TemporaryDirectory(prefix="bigqmt-shm-acceptance-", dir=args.directory) as directory:
        settings = {"namespace": namespace, "directory": directory, "push_codec": "json",
                    "subscriber_id": "acceptance-consumer", "ready_path": os.path.join(directory, "ready.json")}
        settings_path = os.path.join(directory, "settings.json")
        _write_json(settings_path, settings)
        rpc = SharedMemoryTransport(account_id=ACCOUNT, **_rpc_options(settings))
        consumer = SharedMemoryQuotePushChannel("client", settings, ACCOUNT)
        clients.append(consumer)
        names = {rpc._name, rpc._name + "_request", rpc._name + "_space",
                 consumer._name, consumer._space_name, consumer._signal_name(consumer._identity)}
        names.update(rpc._name + "_reply_%d" % index for index in range(rpc.rpc_slots))
        try:
            worker = subprocess.Popen([sys.executable, "-S", os.path.abspath(__file__), "--worker", settings_path],
                                      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                      universal_newlines=True)
            _wait(lambda: os.path.isfile(settings["ready_path"]), args.timeout, "worker ready", worker)
            with open(settings["ready_path"], "r", encoding="utf-8") as stream:
                ready = json.load(stream)
            if ready["pid"] == os.getpid():
                raise RuntimeError("acceptance requires a genuinely separate process")
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(20, args.count)) as pool:
                results = list(pool.map(lambda index: _call(rpc, "acceptance_echo", {"index": index}, args.timeout),
                                        range(args.count)))
            if results != [{"index": index} for index in range(args.count)]:
                raise RuntimeError("concurrent RPC response correlation failed")
            large = "shared-memory-acceptance-" * 50000
            if _call(rpc, "acceptance_echo", {"blob": large}, args.timeout) != {"blob": large}:
                raise RuntimeError("external RPC payload did not round-trip")
            received = []
            guard = threading.Lock()

            def receive(topic, data):
                """保存业务回调结果，ACK 由通道在本回调成功之后写入。"""
                with guard:
                    received.append((topic, data["index"]))

            consumer.start_subscriber(TOPICS, receive)
            for index in range(args.count):
                _call(rpc, "acceptance_emit", {"index": index}, args.timeout)
                _check_client(consumer)
            _wait(lambda: consumer.status()["acknowledged"] == args.count, args.timeout, "push acknowledgements", worker)
            expected = [(TOPICS[index % len(TOPICS)], index) for index in range(args.count)]
            if sorted(received) != sorted(expected):
                raise RuntimeError("push content or topic mismatch")
            for topic in TOPICS:
                if [index for item, index in received if item == topic] != [index for item, index in expected if item == topic]:
                    raise RuntimeError("push lane ordering mismatch")
            consumer.stop()
            # 发布者保持存活，稳定身份退出期间的新事件须在保留期内恢复。
            _call(rpc, "acceptance_emit", {"index": args.count}, args.timeout)
            resumed = SharedMemoryQuotePushChannel("client", settings, ACCOUNT)
            clients.append(resumed)
            recovered = []
            resumed.start_subscriber(TOPICS, lambda topic, data: recovered.append((topic, data["index"])))
            _wait(lambda: resumed.status()["acknowledged"] == 1, args.timeout, "retained subscription recovery", worker)
            if recovered != [(TOPICS[args.count % len(TOPICS)], args.count)]:
                raise RuntimeError("stable subscriber recovery duplicated or lost messages")
            return {"ok": True, "python": sys.version.split()[0], "platform": platform.platform(),
                    "parent_pid": os.getpid(), "worker_pid": ready["pid"], "namespace": namespace,
                    "synthetic_account": ACCOUNT, "json_codec": True, "site_packages_disabled": bool(sys.flags.no_site),
                    "rpc_concurrent_successes": args.count, "rpc_external_payload_bytes": len(large.encode("utf-8")),
                    "push_successes": args.count, "push_acknowledged": consumer.status()["acknowledged"],
                    "recovered_unacknowledged": len(recovered), "qmt_started": False, "orders_submitted": 0}
        finally:
            # 先停止所有持有者再 unlink；不得清理仍被其它进程使用的命名对象。
            try:
                for client in reversed(clients):
                    client.stop()
                rpc.stop()
            finally:
                try:
                    _stop_worker(worker)
                finally:
                    for name in names:
                        cleanup(name, directory=directory)


def main():
    """解析验收参数并返回进程退出码，失败报告同样可保存供人工检查。"""
    parser = argparse.ArgumentParser(description="Independent-process SHM acceptance; synthetic data only; no QMT or trading.")
    parser.add_argument("--count", type=int, default=100, help="synthetic RPC and push samples, 1..10000")
    parser.add_argument("--timeout", type=float, default=10.0, help="bounded per-step timeout in seconds")
    parser.add_argument("--namespace-prefix", default="Local\\bigqmt-acceptance", help="Local\\ or Global\\ scope; a unique suffix is always appended")
    parser.add_argument("--directory", help="optional parent directory for POSIX test mappings and reports")
    parser.add_argument("--report", help="write the final JSON result to this file")
    parser.add_argument("--worker", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        return _worker(args.worker)
    if not 1 <= args.count <= 10000 or not 0 < args.timeout < float("inf"):
        parser.error("count must be 1..10000 and timeout must be positive and finite")
    try:
        result = _accept(args)
    except Exception as exc:
        result = {"ok": False, "python": sys.version.split()[0], "platform": platform.platform(),
                  "error": "%s: %s" % (type(exc).__name__, exc), "qmt_started": False, "orders_submitted": 0}
    if args.report:
        _write_json(os.path.abspath(args.report), result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
