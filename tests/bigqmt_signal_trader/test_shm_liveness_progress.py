"""覆盖探活 GIL 交接、同进程多订阅去重及超时诊断，保留进程身份安全边界。"""

import ctypes
import os
from unittest.mock import Mock
from types import SimpleNamespace

import pytest

from bigqmt_signal_trader import shm_push_channel as module
from bigqmt_signal_trader.transports import shm_primitives as primitives
from test_shm_primitives_audit import windows  # noqa: F401
from test_shm_push_peer_probe_races import pair  # noqa: F401

# pytest 按同名参数注入已导入的 fixture，保留共享夹具以覆盖真实协议状态。
# ruff: noqa: F811


def test_short_mutex_contention_joins_wait_queue_before_yielding_gil(windows, monkeypatch):
    """短临界区竞争先排队等待，不能每次失败都让出 GIL 后被 CPU 回调反复抢占。"""
    kernel, _ = windows
    kernel._immediate_wait = Mock(side_effect=[258, 0])
    sleep = Mock(side_effect=AssertionError("短竞争不应先让出 GIL"))
    monkeypatch.setattr(primitives, "time", SimpleNamespace(monotonic=lambda: 10.0, sleep=sleep))
    assert primitives._take_windows_mutex(123, .2) == 0
    assert [call.args for call in kernel._immediate_wait.call_args_list] == [(123, 0), (123, 1)]
    kernel.WaitForSingleObject.assert_not_called()


@pytest.mark.parametrize("state,alive", [(0, False), (258, True), (0xFFFFFFFF, True)])
def test_process_probe_only_uses_nonblocking_gil_preserving_wait(windows, state, alive):
    """正常、已退出及不可判定状态保持原语义；进程探活不得走释放 GIL 的等待接口。"""
    kernel, _ = windows
    kernel._immediate_wait = Mock(return_value=state)
    kernel.WaitForSingleObject.side_effect = AssertionError("不应释放 GIL")
    assert primitives.process_alive(42, 0) is alive
    kernel._immediate_wait.assert_called_once_with(kernel.OpenProcess.return_value, 0)
    kernel.CloseHandle.assert_called_once()


@pytest.mark.skipif(os.name != "nt" or ctypes.sizeof(ctypes.c_void_p) != 8,
                    reason="需要真实 Win64 ctypes ABI")
def test_actual_win64_process_probe_retains_gil_but_blocking_wait_releases_it():
    """真实 DLL 绑定保留即时探活 GIL；阻塞等待仍可让其他 Python 线程推进。"""
    kernel = primitives._kernel32()
    for name in ("OpenProcess", "GetProcessTimes", "CloseHandle", "_immediate_wait"):
        assert getattr(kernel, name)._flags_ & ctypes._FUNCFLAG_PYTHONAPI
    assert not kernel.WaitForSingleObject._flags_ & ctypes._FUNCFLAG_PYTHONAPI


def test_same_process_consumers_are_probed_once_per_maintenance(pair, monkeypatch):
    """两个槽共享 PID/创建时间时只探活一次，下一轮重新检查，不能跨轮缓存存活结论。"""
    server, client = pair
    with server._region.locked(.2):
        first = server._consumer(client._slot)
        foreign = first._replace(pid=2147483600, birth=123)
        server._write_consumer(client._slot, foreign)
        slot = (client._slot + 1) % server.max_consumers
        server._write_consumer(slot, foreign._replace(identity=b"x" * 32, token=b"y" * 16))
        server._bump_consumers()
    checked = []

    def probe(pid, birth):
        """只记录外部进程探测；自身发布者快捷路径应维持原状。"""
        checked.append((pid, birth))
        return True

    monkeypatch.setattr(module, "process_alive", probe)
    try:
        for _ in range(2):
            server._next_maintenance = 0
            assert server.publish("exec:order", {"id": 1})
        assert checked == [(2147483600, 123)] * 2
        assert server.status()["peer_probe_count"] == 2
    finally:
        with server._region.locked(.2):
            server._write_consumer(slot, foreign._replace(state=0))
            server._write_consumer(client._slot, first)
            server._bump_consumers()


@pytest.mark.parametrize("enabled", [False, True])
def test_liveness_timeout_diagnostics_are_bounded_and_opt_in(pair, monkeypatch, capsys, enabled):
    """关闭调试时不输出；开启后同一秒限一次，并且状态保留累计次数。"""
    server, _ = pair
    monkeypatch.setenv("BIGQMT_DEBUG", "1" if enabled else "0")
    server._last_peer_probe_seconds = .3
    for _ in range(3):
        error = server._liveness_timeout("peer_probe")
        assert "peer_probe" in str(error)
    assert server.status()["liveness_timeouts"] == 3
    assert capsys.readouterr().out.count('"event": "push_liveness_timeout"') == int(enabled)


def test_probe_statistics_preserve_errors_and_identity_checks(pair, monkeypatch):
    """探活异常必须继续向上传递，计时不能把异常转换成存活结论。"""
    server, _ = pair

    def failed(pid, birth):
        """模拟操作系统探针明确报错，不允许诊断代码吞掉异常。"""
        raise OSError("probe failed")

    monkeypatch.setattr(module, "process_alive", failed)
    with pytest.raises(OSError, match="probe failed"):
        server._peer_alive(123, 456)
    assert server.status()["peer_probe_count"] == 1
    assert server.status()["max_peer_probe_seconds"] >= 0
