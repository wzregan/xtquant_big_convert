"""在 QMT 回调中异步触发显式配置的独立 gRPC 计划任务。"""

import importlib
import os
import subprocess
import threading
import time

from .logging_setup import get_logger

_lock = threading.Lock()
_next_attempt = 0.0
_running = False


def ensure_companion(config=None):
    """最多每三十秒触发一次；Windows 任务必须设置 IgnoreNew，回调线程不等待外部进程。"""
    global _next_attempt, _running
    if os.name != "nt":
        return False
    with _lock:
        now = time.monotonic()
        if _running or now < _next_attempt:
            return False
        _next_attempt = now + 30
        if config is None:
            try:
                local = importlib.import_module("bigqmt_signal_trader_local_config")
                config = getattr(local, "BIGQMT_GRPC_COMPANION", {})
            except ImportError:
                return False
        if config.get("enabled") is not True:
            return False
        task_name = config.get("task_name")
        if not isinstance(task_name, str) or not task_name.strip():
            get_logger("grpc_companion").error("gRPC companion task_name is missing")
            return False
        _running = True
        thread = threading.Thread(target=_run_task, args=(task_name,), name="bigqmt-grpc-start")
        thread.daemon = True
        try:
            thread.start()
        except Exception:
            _running = False
            raise
        return True


def _run_task(task_name):
    """仅触发指定任务；不执行任意 shell 命令，也不改变任务或交易配置。"""
    global _running
    try:
        result = subprocess.run(
            ["schtasks.exe", "/Run", "/TN", task_name], timeout=10,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if result.returncode != 0:
            get_logger("grpc_companion").error(
                "gRPC companion launch failed, task=%s exit=%s", task_name, result.returncode
            )
    except Exception as error:
        get_logger("grpc_companion").error("gRPC companion launch error: %s", type(error).__name__)
    finally:
        with _lock:
            _running = False
