"""只在自建 Python 窗口中测试真实注入、消息调度和共享内存协议。"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import ctypes as ct
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time

import psutil

# 源码目录与构建目录分开；部署到同一目录时仍可直接运行。
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from native_control import Channel, ControlError, Target, WinAPI, inject


def main():
    """测试 DLL 与生产 DLL 分别校验宿主，所有启停状态仅存在于测试 DLL 中。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", type=Path, default=Path(__file__).resolve().parent,
                        help="生产与测试 DLL 及身份清单所在的构建目录")
    stage = parser.parse_args().stage.resolve()
    info = stage / "harness-window.json"
    info.unlink(missing_ok=True)
    child = subprocess.Popen([sys.executable, str(Path(__file__).with_name("controller_harness.py")), str(info)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    api = WinAPI()
    user = ct.WinDLL("user32", use_last_error=True)
    user.PostMessageW.argtypes = [ct.c_void_p, ct.c_uint32, ct.c_size_t, ct.c_ssize_t]
    user.PostMessageW.restype = ct.c_int
    hwnd = None
    results = {}
    try:
        end = time.monotonic() + 10
        while not info.exists():
            if child.poll() is not None:
                raise RuntimeError(child.communicate()[1].decode("utf-8", errors="replace"))
            if time.monotonic() >= end:
                raise RuntimeError("owned harness window did not start")
            time.sleep(0.02)
        data = json.loads(info.read_text(encoding="utf-8"))
        hwnd = data["hwnd"]
        actual_pid = data["pid"]
        handle = api.require(api.kernel.OpenProcess(0x400, 0, actual_pid), "OpenProcess")
        try:
            creation = api.creation(handle)
        finally:
            api.kernel.CloseHandle(handle)
        # Windows 虚拟环境入口可能是等待解释器子进程的启动器，窗口发布的 PID 才是实际宿主。
        target = Target(actual_pid, creation, 2, 19314672, psutil.Process(actual_pid).exe(), test=True)
        receipt = inject(target, stage / "QmtNativeStrategyControlTest.dll", hwnd)
        results["remote_load_and_initialize"] = receipt["injected"]
        listener = receipt["control"]["listener_thread_id"]
        again = inject(target, stage / "QmtNativeStrategyControlTest.dll", hwnd)
        assert again["already_loaded"] and again["already_initialized"]
        assert listener == again["control"]["listener_thread_id"]
        results["idempotent_injection"] = True
        with Channel(target) as channel:
            try:
                initial = channel.command(1)
            except ControlError:
                print(json.dumps({"diagnostic": channel.snapshot(), "reserved": list(channel.block.reserved)}))
                raise
            assert initial["strategy_count"] == initial["running_count"] == 1
            assert initial["same_qt_thread"] and initial["execution_thread_id"] == initial["gui_thread_id"]
            assert initial["execution_thread_id"] != initial["listener_thread_id"]
            results["owner_thread_dispatch"] = True
            stopped = channel.command(3)
            assert stopped["running_count"] == 0
            started = channel.command(2)
            assert started["running_count"] == 1
            results["mock_stop_start"] = True
            before = tuple(channel.block.reserved[:2])
            try:
                channel.command(2, model=9999)
            except ControlError as error:
                assert "TARGET_REJECTED" in str(error)
            else:
                raise AssertionError("wrong model must fail")
            assert tuple(channel.block.reserved[:2]) == before
            results["wrong_target_rejection"] = True
            try:
                channel.command(3, deadline_tick=api.kernel.GetTickCount64() - 1000)
            except ControlError as error:
                assert "COMMAND_EXPIRED" in str(error)
            else:
                raise AssertionError("expired command must fail")
            assert tuple(channel.block.reserved[:2]) == before
            results["expired_request_rejection"] = True

            api.require(user.PostMessageW(hwnd, 0x8100, 600, 0), "PostMessageW test delay")
            time.sleep(0.05)
            try:
                channel.command(3, timeout=0.15)
            except ControlError as error:
                assert "pending" in str(error)
            else:
                raise AssertionError("blocked GUI must time out")
            try:
                channel.command(2, timeout=0.15)
            except ControlError as error:
                assert "previous command remains pending" in str(error)
            else:
                raise AssertionError("must not overwrite outstanding request")
            time.sleep(0.65)
            assert channel.block.status == 13
            assert tuple(channel.block.reserved[:2]) == before
            assert channel.command(1)["running_count"] == 1
            results["no_late_stop_after_timeout"] = True
            results["pending_request_cannot_be_overwritten"] = True

            local_test = ct.WinDLL(str(stage / "QmtNativeStrategyControlTest.dll"))
            local_test.QmtCtl_InitializeRemote.argtypes = [ct.c_void_p]
            local_test.QmtCtl_InitializeRemote.restype = ct.c_uint32
            assert local_test.QmtCtl_InitializeRemote(None) == 6
            production = ct.WinDLL(str(stage / "QmtNativeStrategyControl.dll"))
            production.QmtCtl_InitializeRemote.argtypes = [ct.c_void_p]
            production.QmtCtl_InitializeRemote.restype = ct.c_uint32
            assert production.QmtCtl_InitializeRemote(None) == 1
            results["production_wrong_host_guard"] = True

        def status_once(_):
            """每个并发生产者独立持有句柄，真实互斥量负责序列化协议写入。"""
            begin = time.perf_counter()
            with Channel(target) as channel:
                value = channel.command(1)
            assert value["same_qt_thread"] and value["running_count"] == 1
            return value["completed_seq"], (time.perf_counter() - begin) * 1000

        with ThreadPoolExecutor(max_workers=4) as executor:
            measurements = list(executor.map(status_once, range(40)))
        sequences = [value[0] for value in measurements]
        assert len(set(sequences)) == 40
        results["concurrent_producers_40_success"] = True
        # 最后一个客户端退出后，DLL 自己的句柄仍维持控制入口，证明通道独立持有资源。
        with Channel(target) as channel:
            assert channel.command(1)["running_count"] == 1
        results["channel_survives_client_exit"] = True
        latencies = sorted(value[1] for value in measurements)
        result = {"tests": results, "qmt_process_tested": False, "owned_harness_pid": actual_pid,
                  "samples": len(latencies), "p50_ms": statistics.median(latencies),
                  "p95_ms": latencies[37], "max_ms": max(latencies)}
        (stage / "native-controller-harness-result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps(result))
    finally:
        if hwnd:
            user.PostMessageW(hwnd, 0x10, 0, 0)
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)


if __name__ == "__main__":
    main()
