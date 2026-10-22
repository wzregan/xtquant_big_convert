"""审计共享内存系统调用故障及 Windows 替身，不把替身等同于内核实测。"""

import builtins
import ctypes
import errno
import io
import os
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from bigqmt_signal_trader.transports import shm_primitives as p
from bigqmt_signal_trader.transports.base import TransportError, TransportTimeout


class Mapping(bytearray):
    """提供真实 buffer 协议并允许注入映射关闭故障。"""

    def __init__(self, size):
        """分配指定字节数，记录映射关闭次数。"""
        super().__init__(size)
        self.closed = 0
        self.close_error = None

    def close(self):
        """关闭失败时保持资源可重试。"""
        if self.close_error:
            raise self.close_error
        self.closed += 1


@pytest.fixture
def windows(monkeypatch):
    """仅替换原语模块的平台视图，避免污染 pytest 自身的 POSIX 路径处理。"""
    kernel = SimpleNamespace(**{
        name: Mock(return_value=value) for name, value in {
            "CreateMutexW": 0x123456789, "CreateEventW": 0x234567890,
            "WaitForSingleObject": 0, "ReleaseMutex": 1, "CloseHandle": 1,
            "SetEvent": 1, "OpenProcess": 0x345678901, "GetCurrentProcess": -1,
            "GetProcessTimes": 1,
        }.items()
    })
    mappings = {}

    def map_region(_fd, length, tagname):
        """同名映射共享内容，模拟尺寸契约和创建失败的边界。"""
        if tagname not in mappings:
            mappings[tagname] = Mapping(length)
        return mappings[tagname]

    monkeypatch.setattr(p, "os", SimpleNamespace(name="nt", getpid=os.getpid))
    monkeypatch.setattr(p, "_kernel32", lambda: kernel)
    monkeypatch.setattr(p, "mmap", SimpleNamespace(mmap=map_region))
    monkeypatch.setattr(p.ctypes, "get_last_error", lambda: 5, raising=False)
    monkeypatch.setattr(p.ctypes, "WinError", lambda code: OSError(code, "synthetic Win32 error"), raising=False)
    return kernel, mappings


def test_windows_mapping_reopen_corruption_and_size_contract(windows):
    """映射复用保留数据，尺寸冲突或损坏元数据不能覆盖原内容。"""
    kernel, mappings = windows
    first = p.SharedRegion("audit", 128)
    first.buffer[:4] = b"keep"
    second = p.SharedRegion("audit", 128)
    assert second.buffer[:4] == b"keep"
    with pytest.raises(TransportError, match="size mismatch"):
        p.SharedRegion("audit", 256)
    metadata = mappings[p._windows_name("audit", "size")]
    metadata[:8] = b"corrupt!"
    with pytest.raises(TransportError, match="metadata is corrupt"):
        p.SharedRegion("audit", 128)
    assert first.buffer[:4] == b"keep"
    first.close()
    second.close()
    assert kernel.ReleaseMutex.call_count == 4


@pytest.mark.parametrize("failure", ["mutex", "wait", "release", "mapping"])
def test_windows_initialization_failure_releases_owned_handles(windows, monkeypatch, failure):
    """创建、等待或映射失败后已取得的句柄必须回收，不能留下半初始化对象。"""
    kernel, _mappings = windows
    if failure == "mutex":
        kernel.CreateMutexW.return_value = None
    elif failure == "wait":
        kernel.WaitForSingleObject.return_value = p._WAIT_TIMEOUT
    elif failure == "release":
        kernel.ReleaseMutex.return_value = 0
    else:
        monkeypatch.setattr(p.mmap, "mmap", Mock(side_effect=OSError("mapping failed")))
    with pytest.raises((TransportError, TransportTimeout)):
        p.SharedRegion("failed", 128, timeout_seconds=0)
    assert kernel.CloseHandle.call_count == (0 if failure == "mutex" else 1)
    assert kernel.ReleaseMutex.call_count == (1 if failure in ("release", "mapping") else 0)


def test_windows_mutex_wait_release_and_retryable_close_errors(windows):
    """互斥取得、释放和关闭失败均显式报错，失败后生命周期不能永久卡死。"""
    kernel, _mappings = windows
    region = p.SharedRegion("retry", 128)
    kernel.WaitForSingleObject.return_value = p._WAIT_TIMEOUT
    with pytest.raises(TransportTimeout):
        with region.locked(0):
            pytest.fail("不能进入未取得锁的临界区")
    kernel.WaitForSingleObject.return_value = p._WAIT_ABANDONED
    kernel.ReleaseMutex.return_value = 0
    with pytest.raises(TransportError, match="ReleaseMutex"):
        with region.locked(None) as abandoned:
            assert abandoned
    kernel.ReleaseMutex.return_value = 1
    with region.locked(0):
        assert region.buffer is not None
    kernel.CloseHandle.return_value = 0
    with pytest.raises(TransportError, match="CloseHandle"):
        region.close()
    kernel.CloseHandle.return_value = 1
    region.close()
    region.close()
    assert region._closed


def test_windows_event_creation_set_wait_and_close_failures(windows):
    """事件错误保留具体系统调用，成功路径消费事件，重复关闭无副作用。"""
    kernel, _mappings = windows
    kernel.CreateEventW.return_value = None
    with pytest.raises(TransportError, match="CreateEventW"):
        p.NamedSignal("failed-event")
    kernel.CreateEventW.return_value = 0x234567890
    signal = p.NamedSignal("event")
    assert signal.set()
    assert signal.wait(0)
    kernel.WaitForSingleObject.return_value = p._WAIT_TIMEOUT
    assert not signal.wait(0)
    kernel.WaitForSingleObject.return_value = p._WAIT_FAILED
    with pytest.raises(TransportError, match="WaitForSingleObject"):
        signal.wait(0)
    kernel.SetEvent.return_value = 0
    with pytest.raises(TransportError, match="SetEvent"):
        signal.set()
    kernel.CloseHandle.return_value = 0
    with pytest.raises(TransportError, match="CloseHandle event"):
        signal.close()
    kernel.CloseHandle.return_value = 1
    signal.close()
    signal.close()
    p.cleanup("event")


def test_windows_wait_rechecks_remaining_budget_after_early_timeout(windows, monkeypatch):
    """分段等待提前超时后继续使用同一个绝对截止时间。"""
    kernel, _mappings = windows
    kernel.WaitForSingleObject.side_effect = [p._WAIT_TIMEOUT, 0]
    monotonic = iter([10, 10, 10.25, 10.25])
    monkeypatch.setattr(p, "time", SimpleNamespace(monotonic=lambda: next(monotonic)))
    assert p._wait_handle(123, 1) == 0
    assert kernel.WaitForSingleObject.call_args_list[0].args == (123, 1000)
    assert kernel.WaitForSingleObject.call_args_list[1].args == (123, 750)


def test_windows_process_birth_matches_full_precision_and_conservative_failures(windows):
    """PID 复用和正常退出明确判死；权限或未知等待状态保守保活。"""
    kernel, _mappings = windows

    def process_times(_handle, creation, *_others):
        """填入高位创建时间，验证不会经浮点数丢失进程身份精度。"""
        creation._obj.dwHighDateTime = 123456
        creation._obj.dwLowDateTime = 789
        return 1

    kernel.GetProcessTimes.side_effect = process_times
    birth = (123456 << 32) | 789
    assert p.process_identity() == (os.getpid(), birth)
    kernel.WaitForSingleObject.return_value = p._WAIT_TIMEOUT
    assert p.process_alive(42, birth)
    assert not p.process_alive(42, birth + 1)
    assert p.process_alive(42, 0)
    kernel.WaitForSingleObject.return_value = p._WAIT_OBJECT_0
    assert not p.process_alive(42, birth)
    kernel.WaitForSingleObject.return_value = p._WAIT_FAILED
    assert p.process_alive(42, birth)
    kernel.OpenProcess.return_value = 0
    assert p.process_alive(42, birth)
    kernel.GetProcessTimes.side_effect = None
    kernel.GetProcessTimes.return_value = 0
    assert p.process_identity() == (os.getpid(), 0)


@pytest.mark.parametrize("error", [87, 1168])
def test_windows_missing_process_is_not_reported_alive(windows, monkeypatch, error):
    """系统明确返回无此进程时可以回收旧 owner。"""
    windows[0].OpenProcess.return_value = 0
    monkeypatch.setattr(p.ctypes, "get_last_error", lambda: error)
    assert not p.process_alive(123, 456)


@pytest.mark.parametrize("pointer_size", [4, 8])
def test_kernel_abi_initialization_and_racing_cache(monkeypatch, pointer_size):
    """验证完整 ctypes 签名和双重检查锁，不使用真实 Windows DLL。"""
    names = ("CreateMutexW", "ReleaseMutex", "CreateEventW", "SetEvent", "WaitForSingleObject",
             "CloseHandle", "OpenProcess", "GetCurrentProcess", "GetProcessTimes")
    kernel = SimpleNamespace(**{name: Mock() for name in names})
    immediate = SimpleNamespace(**{name: Mock() for name in names})
    original_wait = kernel.WaitForSingleObject
    monkeypatch.setattr(p, "_KERNEL32", None)
    monkeypatch.setattr(p.struct, "calcsize", lambda _format: pointer_size)
    loader = Mock(return_value=kernel)
    immediate_loader = Mock(return_value=immediate)
    monkeypatch.setattr(p.ctypes, "WinDLL", loader, raising=False)
    monkeypatch.setattr(p.ctypes, "PyDLL", immediate_loader)
    assert p._kernel32() is kernel
    assert p._kernel32() is kernel
    assert kernel.CreateMutexW.restype is ctypes.c_void_p
    assert kernel.CloseHandle.argtypes == [ctypes.c_void_p]
    assert loader.call_count == 1
    assert kernel.WaitForSingleObject is original_wait
    assert immediate_loader.call_count == (1 if pointer_size == 8 else 0)
    if pointer_size == 8:
        assert kernel._immediate_api is immediate
        assert kernel._immediate_wait is immediate.WaitForSingleObject
        assert kernel.ReleaseMutex is immediate.ReleaseMutex
        assert kernel.SetEvent is immediate.SetEvent
        for name in ("OpenProcess", "GetProcessTimes", "CloseHandle"):
            assert getattr(kernel, name) is getattr(immediate, name)
        assert immediate.WaitForSingleObject.argtypes == original_wait.argtypes
        assert immediate.WaitForSingleObject.restype is original_wait.restype
    else:
        assert kernel._immediate_wait is None
    monkeypatch.setattr(p, "_KERNEL32", None)

    class RaceLock:
        """模拟另一线程在当前线程等待初始化锁时已经完成装载。"""

        def __enter__(self):
            """进入锁后发布并发线程已创建的实例。"""
            p._KERNEL32 = kernel

        def __exit__(self, *_args):
            """不持有真实锁，不改变被测函数异常。"""
            return False

    monkeypatch.setattr(p, "_KERNEL_LOCK", RaceLock())
    assert p._kernel32() is kernel
    assert loader.call_count == 1


@pytest.mark.parametrize("result", [p._WAIT_OBJECT_0, p._WAIT_ABANDONED, p._WAIT_TIMEOUT, p._WAIT_FAILED])
def test_windows_immediate_wait_preserves_status_and_errors(windows, result):
    """零预算只探测一次，保留废弃锁和系统错误，禁止隐式进入阻塞等待。"""
    kernel, _mappings = windows
    kernel._immediate_wait = Mock(return_value=result)
    if result == p._WAIT_FAILED:
        with pytest.raises(TransportError, match="WaitForSingleObject"):
            p._wait_handle(123, 0)
    else:
        assert p._wait_handle(123, 0) == result
    kernel._immediate_wait.assert_called_once_with(123, 0)
    kernel.WaitForSingleObject.assert_not_called()


def test_windows_immediate_wait_contention_keeps_original_deadline(windows, monkeypatch):
    """非阻塞探测失败后使用释放 GIL 的等待，且扣除已花费的预算。"""
    kernel, _mappings = windows
    kernel._immediate_wait = Mock(return_value=p._WAIT_TIMEOUT)
    monotonic = iter([10, 10.25])
    monkeypatch.setattr(p, "time", SimpleNamespace(monotonic=lambda: next(monotonic)))
    assert p._wait_handle(123, 1) == 0
    kernel._immediate_wait.assert_called_once_with(123, 0)
    kernel.WaitForSingleObject.assert_called_once_with(123, 750)


@pytest.mark.parametrize("result", [p._WAIT_OBJECT_0, p._WAIT_ABANDONED, p._WAIT_TIMEOUT, p._WAIT_FAILED])
def test_windows_mutex_zero_budget_never_waits_without_gil(windows, result):
    """互斥零预算只探测一次，并保留废弃状态及系统错误语义。"""
    kernel, _mappings = windows
    kernel._immediate_wait = Mock(return_value=result)
    if result == p._WAIT_FAILED:
        with pytest.raises(TransportError, match="WaitForSingleObject"):
            p._take_windows_mutex(123, 0)
    else:
        assert p._take_windows_mutex(123, 0) == result
    kernel._immediate_wait.assert_called_once_with(123, 0)
    kernel.WaitForSingleObject.assert_not_called()


@pytest.mark.parametrize("expires", [False, True])
def test_windows_mutex_contention_backs_off_without_owning_lock(windows, monkeypatch, expires):
    """竞争期间短等待始终保留 GIL，重新调度的时间计入同一个截止。"""
    kernel, _mappings = windows
    now = [10.0]
    sleeps = []

    def sleep(seconds):
        """正常推进短暂退避，或模拟长原生调用阻塞后才重新取得 GIL。"""
        sleeps.append(seconds)
        now[0] += 2 if expires else seconds

    kernel._immediate_wait = Mock(side_effect=[p._WAIT_TIMEOUT, p._WAIT_TIMEOUT, p._WAIT_OBJECT_0])
    monkeypatch.setattr(p, "time", SimpleNamespace(monotonic=lambda: now[0], sleep=sleep))
    result = p._take_windows_mutex(123, 1)
    assert result == (p._WAIT_TIMEOUT if expires else p._WAIT_OBJECT_0)
    assert kernel._immediate_wait.call_count == (2 if expires else 3)
    assert sleeps == [0]
    kernel.WaitForSingleObject.assert_not_called()


def test_windows_mutex_long_contention_sleeps_periodically(windows, monkeypatch):
    """长期竞争由内核短等待消耗预算，不能忙等或进入释放 GIL 的无限等待。"""
    kernel, _mappings = windows
    now = [0.0]
    sleeps = []

    def sleep(seconds):
        """记录让出执行权动作；本例耗时来自内核等待。"""
        sleeps.append(seconds)

    def wait(handle, milliseconds):
        """模拟锁始终被占用，核对每次保留 GIL 的等待有严格上限。"""
        assert handle == 123 and 0 <= milliseconds <= 1
        now[0] += milliseconds / 1000.0
        return p._WAIT_TIMEOUT

    kernel._immediate_wait = Mock(side_effect=wait)
    monkeypatch.setattr(p, "time", SimpleNamespace(monotonic=lambda: now[0], sleep=sleep))
    assert p._take_windows_mutex(123, 0.002) == p._WAIT_TIMEOUT
    assert sleeps == [0]
    assert [call.args[1] for call in kernel._immediate_wait.call_args_list] == [0, 1, 1]
    kernel.WaitForSingleObject.assert_not_called()


@pytest.mark.skipif(os.name == "nt", reason="POSIX 原语及 libc 故障审计")
def test_posix_interrupted_lock_respects_deadline_and_unlock_retries(monkeypatch):
    """连续 EINTR 不能让有限加锁超时失效；释放锁被中断后仍应重试。"""
    import fcntl

    flock = Mock(side_effect=OSError(errno.EINTR, "interrupted"))
    monkeypatch.setattr(fcntl, "flock", flock)
    assert not p._take_flock(123, time.monotonic() - 1)
    flock.side_effect = [OSError(errno.EINTR, "interrupted"), None]
    assert p._take_flock(123, time.monotonic() + 1)
    flock.side_effect = [OSError(errno.EINTR, "interrupted"), None]
    p._release_flock(123)
    flock.side_effect = OSError(errno.EIO, "failed")
    with pytest.raises(TransportError, match="lock failed"):
        p._take_flock(123, time.monotonic() + 1)
    with pytest.raises(TransportError, match="unlock failed"):
        p._release_flock(123)


def test_thread_lock_retries_until_acquired(monkeypatch):
    """超大预算经分段等待仍可重试，不会把一次平台等待超时当成全部耗尽。"""
    lock = Mock()
    lock.acquire.side_effect = [False, False, True]
    assert p._take_thread_lock(lock, time.monotonic() + threading.TIMEOUT_MAX * 2)
    assert lock.acquire.call_count == 3
    assert lock.acquire.call_args.kwargs["timeout"] == threading.TIMEOUT_MAX


def test_nested_lifecycle_and_concurrent_closers():
    """嵌套操作正确计数，两个关闭者仅一个拥有回收权。"""
    lifecycle = p._Lifecycle()
    with lifecycle._operation():
        with lifecycle._operation():
            assert lifecycle._active == 2
        assert lifecycle._active == 1
    assert lifecycle._reserve_close()
    entered = threading.Event()
    result = []

    def second_close():
        """竞争关闭时等待第一个关闭者发布结果。"""
        entered.set()
        result.append(lifecycle._reserve_close())

    thread = threading.Thread(target=second_close)
    thread.start()
    assert entered.wait(1)
    time.sleep(0.01)
    assert thread.is_alive()
    lifecycle._finish_close(True)
    thread.join(1)
    assert result == [False]


@pytest.mark.skipif(os.name == "nt", reason="POSIX 原语及 libc 故障审计")
def test_posix_bad_directories_files_and_creation_failure(tmp_path, monkeypatch):
    """拒绝链接、其他用户目录、非普通文件，并在创建系统错误时报告传输错误。"""
    linked = tmp_path / "linked"
    linked.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(TransportError, match="symlink"):
        p.SharedRegion("unsafe", 64, linked)
    assert p._resource_directory(tmp_path) == str(tmp_path)
    stat_result = SimpleNamespace(st_mode=0, st_uid=os.getuid() + 1)
    with monkeypatch.context() as patch:
        patch.setattr(p.os, "stat", lambda _path: stat_result)
        patch.setattr(p.os, "makedirs", lambda *args, **kwargs: None)
        with pytest.raises(TransportError, match="owned"):
            p._resource_directory(tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(p.os, "makedirs", Mock(side_effect=OSError("full")))
        with pytest.raises(TransportError, match="cannot prepare"):
            p._resource_directory(tmp_path)
    path = str(tmp_path / "regular")
    with monkeypatch.context() as patch:
        patch.setattr(p.os, "fstat", lambda _fd: stat_result)
        with pytest.raises(TransportError, match="regular file"):
            p._open_private_file(path)
    with monkeypatch.context() as patch:
        actual_open = p._open_private_file

        def fail_region(path, exclusive=False):
            """只让业务映射创建失败，验证初始化锁仍会释放。"""
            if path.endswith(".region"):
                raise OSError(errno.ENOSPC, "full")
            return actual_open(path, exclusive)

        patch.setattr(p, "_open_private_file", fail_region)
        with pytest.raises(TransportError, match="initialization failed"):
            p.SharedRegion("full", 64, tmp_path, timeout_seconds=None)
    p.cleanup("full", tmp_path)


@pytest.mark.skipif(os.name == "nt", reason="POSIX 原语及 libc 故障审计")
def test_default_resource_directory_is_current_user_private(monkeypatch, tmp_path):
    """默认目录必须收紧到 0700，既有目录不能因 umask 漂移泄露共享内容。"""
    chmod = Mock()
    monkeypatch.setattr(p.os.path, "abspath", lambda _path: str(tmp_path))
    monkeypatch.setattr(p.os, "chmod", chmod)
    assert p._resource_directory(None) == str(tmp_path)
    chmod.assert_called_once_with(str(tmp_path), 0o700)


@pytest.mark.skipif(os.name == "nt", reason="POSIX 原语及 libc 故障审计")
def test_posix_identity_malformed_dead_and_unknown(monkeypatch):
    """损坏 /proc、僵尸进程、出生标记缺失与访问错误各有明确保守语义。"""
    monkeypatch.setattr(builtins, "open", lambda *args, **kwargs: io.StringIO("bad"))
    with pytest.raises(ValueError, match="invalid Linux"):
        p._linux_identity(42)
    for state in ("Z", "X"):
        monkeypatch.setattr(p, "_linux_identity", lambda _pid: (state, 123))
        assert not p.process_alive(42, 123)
    monkeypatch.setattr(p, "_linux_identity", lambda _pid: ("R", 123))
    assert p.process_alive(42, 0)
    assert p.process_alive("invalid", 123)
    monkeypatch.setattr(p, "_linux_identity", Mock(side_effect=RuntimeError("unknown")))
    assert p.process_alive(42, 123)
    monkeypatch.setattr(p, "_linux_identity", Mock(side_effect=OSError(errno.ESRCH, "gone")))
    assert not p.process_alive(42, 123)


@pytest.mark.skipif(os.name == "nt", reason="POSIX 原语及 libc 故障审计")
def test_signal_posix_creation_close_and_signal_failures(tmp_path, monkeypatch):
    """信号量关键系统调用失败不会静默丢通知，并允许关闭失败后重试。"""
    real_library = p._sem_library()
    with monkeypatch.context() as patch:
        fake = SimpleNamespace(sem_open=Mock(return_value=ctypes.c_void_p(-1).value))
        patch.setattr(p, "_sem_library", lambda: fake)
        with pytest.raises(TransportError, match="sem_open"):
            p.NamedSignal("fail", tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(p, "_resource_directory", Mock(side_effect=OSError("full")))
        with pytest.raises(TransportError, match="initialization failed"):
            p.NamedSignal("fail", tmp_path)
    signal = p.NamedSignal("calls", tmp_path)
    try:
        with monkeypatch.context() as patch:
            fake = SimpleNamespace(sem_getvalue=Mock(return_value=-1), sem_post=Mock(return_value=-1),
                                   sem_close=Mock(return_value=-1))
            patch.setattr(p, "_sem_library", lambda: fake)
            signal._set_lock.acquire()
            try:
                assert not signal.set()
            finally:
                signal._set_lock.release()
            with pytest.raises(TransportError, match="sem_getvalue"):
                signal.set()
            fake.sem_getvalue.return_value = 0
            with pytest.raises(TransportError, match="sem_post"):
                signal.set()
            with pytest.raises(TransportError, match="sem_close"):
                signal.close()
        assert not signal._closed
        assert real_library.sem_getvalue(signal._semaphore, ctypes.byref(ctypes.c_int())) == 0
    finally:
        signal.close()
        p.cleanup("calls", tmp_path)
        p.cleanup("fail", tmp_path)


@pytest.mark.parametrize("mode", ["success", "interrupted", "timeout", "error", "retry-timeout"])
@pytest.mark.skipif(os.name == "nt", reason="POSIX 原语及 libc 故障审计")
def test_legacy_semaphore_wait_uses_remaining_deadline(tmp_path, monkeypatch, mode):
    """旧 libc 实时时钟等待仍按单调时间预算处理 EINTR、超时和错误。"""
    signal = p.NamedSignal("legacy", tmp_path)
    try:
        fake = SimpleNamespace(sem_timedwait=Mock(), sem_trywait=Mock())
        fake.sem_timedwait.return_value = 0 if mode == "success" else -1
        fake.sem_trywait.return_value = -1
        error = {"interrupted": errno.EINTR, "timeout": errno.ETIMEDOUT,
                 "error": errno.EBADF, "retry-timeout": errno.ETIMEDOUT}.get(mode, 0)
        with monkeypatch.context() as patch:
            patch.setattr(p, "_sem_library", lambda: fake)
            patch.setattr(p.ctypes, "get_errno", lambda: error)
            if mode == "retry-timeout":
                fake.sem_timedwait.side_effect = [-1, 0]
                assert signal.wait(1)
            elif mode == "error":
                with pytest.raises(TransportError, match="semaphore wait failed"):
                    signal.wait(0)
            elif mode == "success":
                assert signal.wait(1)
            else:
                assert not signal.wait(0.001)
    finally:
        signal.close()
        p.cleanup("legacy", tmp_path)


@pytest.mark.skipif(os.name == "nt", reason="POSIX 原语及 libc 故障审计")
def test_posix_cleanup_errors_keep_remaining_resource_identity(tmp_path, monkeypatch):
    """权限错误必须报告，不能谎称已清理，也不能自动更换名字另建通信域。"""
    with monkeypatch.context() as patch:
        patch.setattr(p.os, "unlink", Mock(side_effect=OSError(errno.EACCES, "denied")))
        with pytest.raises(TransportError, match="cleanup failed"):
            p.cleanup("denied", tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(p, "_sem_library", lambda: SimpleNamespace(sem_unlink=lambda _name: -1))
        patch.setattr(p.ctypes, "get_errno", lambda: errno.EACCES)
        with pytest.raises(TransportError, match="semaphore cleanup failed"):
            p.cleanup("denied", tmp_path)


@pytest.mark.skipif(os.name == "nt", reason="POSIX 原语及 libc 故障审计")
def test_sem_library_legacy_loader_and_racing_cache(monkeypatch):
    """不含 sem_open 的 libc 回退 librt，并为旧接口设置正确 ABI。"""
    import ctypes.util

    names = ("sem_open", "sem_close", "sem_unlink", "sem_post", "sem_getvalue", "sem_trywait", "sem_timedwait")
    library = SimpleNamespace(**{name: Mock() for name in names})
    monkeypatch.setattr(p, "_SEM_LIB", None)
    loader = Mock(side_effect=[SimpleNamespace(), library])
    monkeypatch.setattr(p.ctypes, "CDLL", loader)
    monkeypatch.setattr(ctypes.util, "find_library", lambda _name: None)
    assert p._sem_library() is library
    assert p._sem_library() is library
    assert library.sem_open.restype is ctypes.c_void_p
    assert loader.call_args.args == ("librt.so.1",)
    monkeypatch.setattr(p, "_SEM_LIB", None)

    class RaceLock:
        """模拟并发线程已经完成信号量库初始化。"""

        def __enter__(self):
            """在取得锁时发布已加载实例。"""
            p._SEM_LIB = library

        def __exit__(self, *_args):
            """不改变异常传播。"""
            return False

    monkeypatch.setattr(p, "_SEM_LIB_LOCK", RaceLock())
    assert p._sem_library() is library
