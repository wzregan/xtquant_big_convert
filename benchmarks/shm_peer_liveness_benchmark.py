"""隔离进程验证 Win32 探活在 Python 线程竞争下的耗时，不连接 QMT 或交易账户。"""

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from bigqmt_signal_trader.transports import shm_primitives as primitives


def measure(rounds, switch_seconds):
    """rounds 是每组探测次数，switch_seconds 模拟原生回调占用 GIL 的线程切换间隔。"""
    peer = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(180)"])
    kernel = primitives._kernel32()
    handle = kernel.OpenProcess(0x1000 | 0x100000, False, peer.pid)
    assert handle
    birth = primitives._windows_birth(handle)
    kernel.CloseHandle(handle)
    stop = threading.Event()
    samples = {}
    calls = {}
    original = {}

    def busy():
        """独立线程持续执行 Python 运算，模拟 QMT 行情回调争用解释器。"""
        value = 1
        while not stop.is_set():
            value = (value * 3 + 7) % 99991

    def wrap(name, function):
        """计时包含 DLL 返回后重新取得 GIL 的等待，不修改操作系统返回值。"""
        def measured(*args):
            """记录单次系统接口的调用耗时，返回原始结果。"""
            start = time.perf_counter()
            try:
                return function(*args)
            finally:
                calls.setdefault(name, []).append(time.perf_counter() - start)
        return measured

    previous = sys.getswitchinterval()
    thread = threading.Thread(target=busy, daemon=True)
    try:
        for name in ("OpenProcess", "WaitForSingleObject", "GetProcessTimes", "CloseHandle"):
            original[name] = getattr(kernel, name)
            setattr(kernel, name, wrap(name, original[name]))
        for phase in ("idle", "gil_contention"):
            if phase == "gil_contention":
                sys.setswitchinterval(switch_seconds)
                thread.start()
            values = []
            for _ in range(rounds):
                start = time.perf_counter()
                assert primitives.process_alive(peer.pid, birth)
                values.append(time.perf_counter() - start)
            samples[phase] = summary(values)
        return {"pid": os.getpid(), "rounds": rounds, "switch_seconds": switch_seconds,
                "samples": samples, "native_calls": {k: summary(v) for k, v in calls.items()}}
    finally:
        stop.set()
        if thread.is_alive():
            thread.join(2)
        sys.setswitchinterval(previous)
        for name, function in original.items():
            setattr(kernel, name, function)
        peer.terminate()
        peer.wait(timeout=5)


def summary(values):
    """汇总毫秒延迟，以及超过真实后台发送 200 毫秒预算的次数。"""
    ordered = sorted(values)
    return {"p50_ms": ordered[len(ordered) // 2] * 1000,
            "p99_ms": ordered[min(len(ordered) - 1, int(len(ordered) * .99))] * 1000,
            "max_ms": max(values) * 1000,
            "over_200ms": sum(value >= .2 for value in values)}


def main():
    """在独立 Windows 进程中运行对照测试，JSON 输出可作为修复前后证据。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=60)
    parser.add_argument("--switch-seconds", type=float, default=.05)
    args = parser.parse_args()
    if os.name != "nt":
        parser.error("本诊断必须运行在 Windows")
    print(json.dumps(measure(args.rounds, args.switch_seconds)), flush=True)


if __name__ == "__main__":
    main()
