"""验证共享内存原语的独立进程、崩溃互斥、有限等待与资源所有权。"""

import ast
import ctypes
import json
import os
from pathlib import Path
import queue
import signal
import struct
import subprocess
import sys
import threading
import time
import uuid

import pytest


SOURCE = Path(__file__).resolve().parents[2] / "src"
sys.path.insert(0, str(SOURCE))

from bigqmt_signal_trader.transports import shm_primitives as primitives  # noqa: E402
from bigqmt_signal_trader.transports.base import TransportError, TransportTimeout  # noqa: E402
from bigqmt_signal_trader.transports.shm_primitives import (  # noqa: E402
    NamedSignal, SharedRegion, cleanup, endpoint_name, process_alive, process_identity,
)


class Resources(object):
    """测试资源账本，失败路径也关闭实例并显式移除命名资源。"""

    def __init__(self, directory):
        """保存独立目录及随机命名前缀，避免测试碰到已有服务。"""
        self.directory = str(directory)
        self.prefix = uuid.uuid4().hex
        self.instances = []
        self.names = set()

    def name(self, suffix):
        """返回该测试内稳定、跨解释器可复现的资源名称。"""
        value = endpoint_name(self.prefix, "synthetic", suffix)
        self.names.add(value)
        return value

    def region(self, name, size=4096, **options):
        """创建并登记映射，测试结束后按实例生命周期回收。"""
        self.names.add(name)
        result = SharedRegion(name, size, self.directory, **options)
        self.instances.append(result)
        return result

    def signal(self, name):
        """创建并登记提示原语，名称允许独立进程重新打开。"""
        self.names.add(name)
        result = NamedSignal(name, self.directory)
        self.instances.append(result)
        return result

    def close(self):
        """先关闭全部句柄，再清理 POSIX 名称，避免仍运行的实例分裂锁域。"""
        for instance in reversed(self.instances):
            instance.close()
        for name in self.names:
            cleanup(name, self.directory)


@pytest.fixture
def resources(tmp_path):
    """为每个测试提供独立名字与目录，不借助 fork 继承文件描述符。"""
    result = Resources(tmp_path)
    try:
        yield result
    finally:
        result.close()


def _child(code, *arguments):
    """启动独立 Python 解释器重新 open 原语，避免继承父进程 mmap 或锁 fd。"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(SOURCE)
    return subprocess.Popen(
        [sys.executable, "-c", code] + [str(value) for value in arguments],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        universal_newlines=True, env=environment,
    )


def _finish_child(child, timeout=10.0):
    """限时检查子进程退出结果，失败时保留输出以定位真正的跨进程错误。"""
    output, _error = child.communicate(timeout=timeout)
    assert child.returncode == 0, output
    return output


def _stop_child(child):
    """回收异常或被强制崩溃的测试进程，避免把锁 owner 留给后续用例。"""
    if child.poll() is None:
        child.kill()
    child.communicate(timeout=5.0)


def _read_line(child, timeout=5.0):
    """跨 Windows/POSIX 限时读取就绪提示，不依赖 select 支持匿名管道。"""
    result = queue.Queue()

    def read():
        """由后台线程等待管道，主测试保留有界等待能力。"""
        result.put(child.stdout.readline())

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    return result.get(timeout=timeout)


def test_name_preserves_scope_and_has_no_component_collision():
    """命名跨解释器稳定，并保留显式 Global 作用域。"""
    assert endpoint_name("bigqmt", "account", "rpc").startswith("Local\\bigqmt_")
    assert endpoint_name("Global\\bigqmt", "account", "rpc").startswith("Global\\bigqmt_")
    assert endpoint_name("bigqmt", "account", "rpc") == endpoint_name("Local\\bigqmt", "account", "rpc")
    assert endpoint_name("a:b", "c", "d") != endpoint_name("a", "b:c", "d")
    code = "from bigqmt_signal_trader.transports.shm_primitives import endpoint_name; print(endpoint_name('bigqmt','account','rpc'))"
    child = _child(code)
    try:
        assert _finish_child(child).strip() == endpoint_name("bigqmt", "account", "rpc")
    finally:
        _stop_child(child)


def test_mmap_slice_struct_and_independent_open(resources):
    """完整原始映射可切片及 pack_into，不把同步元数据占用业务首部。"""
    name = resources.name("region")
    first = resources.region(name)
    second = resources.region(name)
    with first.locked(0) as abandoned:
        assert abandoned is False
        struct.pack_into("<Q", first.buffer, 0, 123456)
        first.buffer[32:36] = b"test"
    with second.locked(0):
        assert struct.unpack_from("<Q", second.buffer, 0)[0] == 123456
        assert second.buffer[32:36] == b"test"
    first.close()
    with second.locked(0):
        assert second.buffer[32:36] == b"test"


@pytest.mark.parametrize("size", [4095, 4097])
def test_existing_size_mismatch_is_rejected_without_data_change(resources, size):
    """同名精确字节尺寸冲突被拒绝，不能因 Windows 页对齐而误接受。"""
    name = resources.name("size")
    existing = resources.region(name)
    existing.buffer[:4] = b"keep"
    with pytest.raises(TransportError, match="size mismatch"):
        resources.region(name, size)
    assert existing.buffer[:4] == b"keep"


@pytest.mark.parametrize("value", [-1, float("inf"), float("nan"), "invalid"])
def test_invalid_timeouts_fail_before_waiting(resources, value):
    """锁、映射初始化和提示等待都拒绝无限、负值及非法超时。"""
    region = resources.region(resources.name("timeouts"))
    prompt = resources.signal(resources.name("prompt"))
    with pytest.raises(ValueError):
        with region.locked(value):
            pass
    with pytest.raises(ValueError):
        prompt.wait(value)
    with pytest.raises(ValueError):
        prompt.set(value)
    with pytest.raises(ValueError):
        resources.region(resources.name("invalid-open"), timeout_seconds=value)


@pytest.mark.parametrize("size", [0, -1, 1.5, "4096"])
def test_invalid_region_size_is_rejected(resources, size):
    """映射尺寸必须是正整数，不能截断浮点或静默转换字符串。"""
    with pytest.raises(ValueError):
        resources.region(resources.name("invalid-size"), size)


def test_signal_coalesces_pending_hints_and_consumes_once(resources):
    """多个 set 合并为一个待消费提示，wait(0) 在空提示时立即返回。"""
    name = resources.name("signal")
    publisher = resources.signal(name)
    subscriber = resources.signal(name)
    assert subscriber.wait(0) is False
    for _index in range(20):
        publisher.set()
    assert subscriber.wait(0) is True
    assert subscriber.wait(0) is False
    publisher.set()
    assert subscriber.wait(0) is True


def test_independent_process_region_and_signal_round_trip(resources):
    """完全独立进程重开映射和两个提示完成请求/回应，不靠 socket 或继承 fd。"""
    region_name = resources.name("cross-region")
    request_name = resources.name("cross-request")
    response_name = resources.name("cross-response")
    region = resources.region(region_name)
    request = resources.signal(request_name)
    response = resources.signal(response_name)
    code = """
import struct,sys
from bigqmt_signal_trader.transports.shm_primitives import SharedRegion,NamedSignal
region=SharedRegion(sys.argv[1],4096,sys.argv[4])
request=NamedSignal(sys.argv[2],sys.argv[4])
response=NamedSignal(sys.argv[3],sys.argv[4])
assert request.wait(3.0)
with region.locked(1.0):
    value=struct.unpack_from('<Q',region.buffer,0)[0]
    struct.pack_into('<Q',region.buffer,0,value+1)
response.set()
response.close();request.close();region.close()
print('OK')
"""
    child = _child(code, region_name, request_name, response_name, resources.directory)
    try:
        with region.locked(1.0):
            struct.pack_into("<Q", region.buffer, 0, 41)
        request.set()
        assert response.wait(5.0)
        with region.locked(1.0):
            assert struct.unpack_from("<Q", region.buffer, 0)[0] == 42
        assert _finish_child(child).strip() == "OK"
    finally:
        _stop_child(child)


def test_multiple_independent_processes_initialize_and_increment(resources):
    """并发首次创建不能截断已有数据，跨进程锁使复合读改写准确完成。"""
    name = resources.name("counter")
    code = """
import struct,sys
from bigqmt_signal_trader.transports.shm_primitives import SharedRegion
region=SharedRegion(sys.argv[1],4096,sys.argv[2])
for index in range(250):
    with region.locked(2.0):
        value=struct.unpack_from('<Q',region.buffer,0)[0]
        struct.pack_into('<Q',region.buffer,0,value+1)
print('DONE',flush=True)
# 保持映射存活到父进程重新打开，Windows 最后句柄关闭会销毁映射。
sys.stdin.readline()
region.close()
"""
    children = []
    environment = dict(os.environ, PYTHONPATH=str(SOURCE))
    try:
        for _index in range(4):
            children.append(subprocess.Popen(
                [sys.executable, "-c", code, name, resources.directory],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                universal_newlines=True, env=environment,
            ))
        for child in children:
            assert _read_line(child).strip() == "DONE"
        region = resources.region(name)
        with region.locked(1.0):
            assert struct.unpack_from("<Q", region.buffer, 0)[0] == 1000
        for child in children:
            child.stdin.write("stop\n")
            child.stdin.flush()
            _finish_child(child)
    finally:
        for child in children:
            _stop_child(child)


def test_owner_crash_releases_lock_and_reports_windows_abandoned(resources):
    """锁 owner 强制退出后锁可恢复；Windows abandoned 必须交给协议层。"""
    name = resources.name("crash")
    region = resources.region(name)
    code = """
import json,sys,time
from bigqmt_signal_trader.transports.shm_primitives import SharedRegion,process_identity
region=SharedRegion(sys.argv[1],4096,sys.argv[2])
with region.locked(1.0):
    print(json.dumps(process_identity()),flush=True)
    time.sleep(60)
"""
    child = _child(code, name, resources.directory)
    try:
        pid, birth = json.loads(_read_line(child))
        assert process_alive(pid, birth)
        before = time.monotonic()
        with pytest.raises(TransportTimeout):
            with region.locked(0.03):
                pass
        assert time.monotonic() - before < 0.3
        before = time.monotonic()
        with pytest.raises(TransportTimeout):
            resources.region(name, timeout_seconds=0.03)
        assert time.monotonic() - before < 0.3
        child.kill()
        child.communicate(timeout=5.0)
        assert not process_alive(pid, birth)
        with region.locked(0.5) as abandoned:
            assert abandoned is (os.name == "nt")
        with region.locked(0) as abandoned:
            assert abandoned is False
    finally:
        _stop_child(child)


def test_thread_lock_and_process_lock_share_single_timeout_budget(resources):
    """实例内线程互斥不能递归放行，同一 deadline 覆盖两级锁的等待。"""
    region = resources.region(resources.name("thread-lock"))
    acquired = threading.Event()
    release = threading.Event()

    def owner():
        """持锁等待测试线程明确释放，防止用固定 sleep 猜测竞争时序。"""
        with region.locked(1.0):
            acquired.set()
            release.wait(2.0)

    thread = threading.Thread(target=owner)
    thread.start()
    try:
        assert acquired.wait(1.0)
        before = time.monotonic()
        with pytest.raises(TransportTimeout):
            with region.locked(0.04):
                pass
        assert 0.025 <= time.monotonic() - before < 0.3
        with pytest.raises(TransportTimeout):
            with region.locked(0):
                pass
    finally:
        release.set()
        thread.join(1.0)


def test_wait_releases_gil_and_can_be_woken_from_another_thread(resources):
    """阻塞 C 等待释放 GIL，另一 Python 线程可 set 唤醒而非只能等超时。"""
    prompt = resources.signal(resources.name("gil"))
    started = threading.Event()
    result = []

    def waiter():
        """记录真实阻塞等待结果，由调用线程触发提示。"""
        started.set()
        result.append(prompt.wait(1.0))

    thread = threading.Thread(target=waiter)
    thread.start()
    assert started.wait(1.0)
    time.sleep(0.02)
    prompt.set()
    thread.join(0.5)
    assert result == [True]


def test_close_waits_for_active_wait_and_is_idempotent(resources):
    """关闭不得释放正在等待的 OS 句柄，新操作在关闭期间也被拒绝。"""
    prompt = resources.signal(resources.name("close-wait"))
    result = []

    def waiter():
        """用短有限等待观察 close 是否提前释放了等待资源。"""
        result.append(prompt.wait(0.2))

    thread = threading.Thread(target=waiter)
    thread.start()
    deadline = time.monotonic() + 1.0
    while prompt._active == 0 and time.monotonic() < deadline:
        time.sleep(0.001)
    assert prompt._active == 1
    closer = threading.Thread(target=prompt.close)
    closer.start()
    time.sleep(0.02)
    assert closer.is_alive()
    with pytest.raises(TransportError):
        prompt.set()
    closer.join(1.0)
    thread.join(1.0)
    assert result == [False]
    assert not closer.is_alive()
    prompt.close()
    with pytest.raises(TransportError):
        prompt.wait(0)


def test_close_inside_owned_lock_is_rejected_without_deadlock(resources):
    """同线程不能在自己持锁时等待自己关闭，拒绝后实例仍可正常使用。"""
    region = resources.region(resources.name("close-lock"))
    with region.locked(0):
        with pytest.raises(TransportError, match="active operation"):
            region.close()
    with region.locked(0):
        region.buffer[:1] = b"x"
    region.close()
    region.close()
    with pytest.raises(TransportError):
        with region.locked(0):
            pass


def test_exported_memoryview_must_release_before_close_can_retry(resources):
    """外部持有视图时关闭失败不提前关闭 fd，释放视图后允许再次关闭。"""
    region = resources.region(resources.name("exported-view"))
    view = memoryview(region.buffer)
    with pytest.raises(BufferError):
        region.close()
    with region.locked(0):
        view[:4] = b"safe"
        assert region.buffer[:4] == b"safe"
    view.release()
    region.close()
    region.close()


def test_process_identity_birth_and_conservative_errors(monkeypatch):
    """出生标记识别 PID 复用；读取失败不能错杀仍存活的 owner。"""
    pid, birth = process_identity()
    assert pid == os.getpid()
    assert isinstance(birth, int) and 0 < birth < 2 ** 63
    assert process_alive(pid, birth)
    assert not process_alive(pid, birth + 1)
    assert not process_alive(0, birth)
    if os.name != "nt":
        def unavailable(_pid):
            """模拟无权限读取进程身份，必须保守视作存活。"""
            raise PermissionError("restricted process information")

        monkeypatch.setattr(primitives, "_linux_identity", unavailable)
        assert process_alive(pid, birth)
        assert process_identity() == (pid, 0)


def test_cleanup_only_after_close_removes_old_data_and_pending_signal(resources):
    """普通 close 不取消资源名称；显式 cleanup 才清空 POSIX 历史映射和提示。"""
    name = resources.name("cleanup")
    region = resources.region(name)
    prompt = resources.signal(name)
    region.buffer[:4] = b"old!"
    prompt.set()
    region.close()
    prompt.close()
    cleanup(name, resources.directory)
    reopened = resources.region(name)
    restarted = resources.signal(name)
    assert reopened.buffer[:4] == b"\0" * 4
    assert restarted.wait(0) is False


@pytest.mark.skipif(os.name == "nt", reason="POSIX 命名信号量的多进程合并提示")
def test_concurrent_producers_merge_signal_without_token_accumulation(resources):
    """多个独立 producer 并发 set，未消费提示仍只能成功 wait 一次。"""
    name = resources.name("many-setters")
    prompt = resources.signal(name)
    code = """
import sys
from bigqmt_signal_trader.transports.shm_primitives import NamedSignal
prompt=NamedSignal(sys.argv[1],sys.argv[2])
for index in range(250):
    prompt.set()
prompt.close()
"""
    children = [_child(code, name, resources.directory) for _index in range(4)]
    try:
        for child in children:
            _finish_child(child)
        assert prompt.wait(0) is True
        assert prompt.wait(0) is False
    finally:
        for child in children:
            _stop_child(child)


@pytest.mark.skipif(os.name == "nt", reason="POSIX 非阻塞合并提示文件锁")
def test_signal_set_does_not_wait_for_suspended_foreign_setter(resources):
    """提示合并锁的 owner 悬停时立即返回 False，不能拖穿 RPC 总预算。"""
    name = resources.name("suspended-setter")
    prompt = resources.signal(name)
    code = """
import fcntl,sys,time
from bigqmt_signal_trader.transports.shm_primitives import NamedSignal
prompt=NamedSignal(sys.argv[1],sys.argv[2])
fcntl.flock(prompt._set_fd,fcntl.LOCK_EX)
print('LOCKED',flush=True)
time.sleep(60)
"""
    child = _child(code, name, resources.directory)
    try:
        assert _read_line(child).strip() == "LOCKED"
        before = time.monotonic()
        assert prompt.set() is False
        assert time.monotonic() - before < 0.1
        assert prompt.wait(0) is False
        child.kill()
        child.communicate(timeout=5.0)
        assert prompt.set() is True
        assert prompt.wait(0) is True
    finally:
        _stop_child(child)


@pytest.mark.skipif(os.name == "nt" or not hasattr(signal, "SIGUSR1"), reason="POSIX EINTR 恢复")
def test_named_semaphore_wait_survives_real_signal_interrupt(resources):
    """真实系统信号中断等待后继续按剩余预算等待，后续提示仍可送达。"""
    name = resources.name("interrupt")
    prompt = resources.signal(name)
    observed = []

    def handler(signum, _frame):
        """记录主线程收到的信号，证明测试确实经过 EINTR 路径。"""
        observed.append(signum)

    previous = signal.signal(signal.SIGUSR1, handler)
    code = """
import os,signal,sys,time
from bigqmt_signal_trader.transports.shm_primitives import NamedSignal
prompt=NamedSignal(sys.argv[1],sys.argv[2])
time.sleep(0.05)
os.kill(int(sys.argv[3]),signal.SIGUSR1)
time.sleep(0.05)
prompt.set()
prompt.close()
"""
    child = _child(code, name, resources.directory, os.getpid())
    try:
        assert prompt.wait(2.0)
        _finish_child(child)
        assert observed == [signal.SIGUSR1]
    finally:
        signal.signal(signal.SIGUSR1, previous)
        _stop_child(child)


@pytest.mark.skipif(os.name != "nt", reason="需要真实 Windows 内核 ABI")
def test_windows_handle_abi_matches_pointer_width():
    """真实 Win32 导入不能截断 64 位 HANDLE，所有使用的函数有明确 ABI。"""
    from ctypes import wintypes

    kernel = primitives._kernel32()
    assert ctypes.sizeof(wintypes.HANDLE) == ctypes.sizeof(ctypes.c_void_p)
    for name in ("CreateMutexW", "CreateEventW", "OpenProcess", "GetCurrentProcess"):
        assert getattr(kernel, name).restype is wintypes.HANDLE
    for name in ("WaitForSingleObject", "CloseHandle", "ReleaseMutex", "SetEvent", "GetProcessTimes"):
        assert getattr(kernel, name).argtypes[0] is wintypes.HANDLE


def test_windows_initialization_abandoned_is_forwarded_to_first_protocol_lock(monkeypatch):
    """模拟 Win32 abandoned：初始化提前拿到该状态也不能把它对协议静默吞掉。"""
    class WindowsOS(object):
        """只替换原语模块的平台分支，避免改变 pytest 或 pathlib 的全局平台。"""

        name = "nt"

    class Mapping(bytearray):
        """模拟支持 buffer 协议的页文件映射，用于 abandoned 状态流测试。"""

        def close(self):
            """本测试不模拟内核映射销毁，仅验证协议状态传递。"""
            pass

    class Kernel(object):
        """模拟初始化一次 abandoned 与随后正常取得 mutex。"""

        def __init__(self):
            """保留大于 32 位的句柄与确定的等待返回值。"""
            self.handle = 0x12345678ABCDEF01
            self.results = iter([128, 0, 0])
            self.releases = 0
            self.closed = []

        def CreateMutexW(self, _security, _owner, _name):
            """返回高位句柄，后续调用必须原样传递。"""
            return self.handle

        def WaitForSingleObject(self, handle, _milliseconds):
            """依次提供初始化 abandoned 及正常取得 mutex。"""
            assert handle == self.handle
            return next(self.results)

        def ReleaseMutex(self, handle):
            """记录每次获得所有权后都正确释放。"""
            assert handle == self.handle
            self.releases += 1
            return 1

        def CloseHandle(self, handle):
            """记录关闭路径未截断句柄高位。"""
            self.closed.append(handle)
            return 1

    kernel = Kernel()

    def mapping(_fd, length, tagname):
        """创建零填充业务和尺寸缓冲区，不声称模拟真实 Windows ABI。"""
        assert tagname.startswith("Local\\")
        return Mapping(length)

    monkeypatch.setattr(primitives, "os", WindowsOS())
    monkeypatch.setattr(primitives, "_kernel32", lambda: kernel)
    monkeypatch.setattr(primitives.mmap, "mmap", mapping)
    region = SharedRegion("abandoned-test", 4096)
    try:
        with region.locked(0) as abandoned:
            assert abandoned is True
        with region.locked(0) as abandoned:
            assert abandoned is False
    finally:
        region.close()
    assert kernel.releases == 3
    assert kernel.closed == [kernel.handle]


def test_windows_large_finite_timeout_never_overflows_or_becomes_infinite(monkeypatch):
    """超大但有限等待拆成最大有限 DWORD，避免浮点乘法溢出或 INFINITE。"""
    observed = []

    class Kernel(object):
        """用立即完成的对象校验等待毫秒参数，不进行实际长等待。"""

        def WaitForSingleObject(self, handle, milliseconds):
            """记录传入的有限等待值，再模拟对象已经可用。"""
            observed.append((handle, milliseconds))
            return 0

    monkeypatch.setattr(primitives, "_kernel32", lambda: Kernel())
    assert primitives._wait_handle(0x12345678ABCDEF01, 1e308) == 0
    assert observed == [(0x12345678ABCDEF01, 0xFFFFFFFE)]


def test_primitive_source_remains_python36_compatible():
    """QMT 内嵌解释器仍可解析原语，避免引入新 Python 语法或 shared_memory。"""
    path = SOURCE / "bigqmt_signal_trader" / "transports" / "shm_primitives.py"
    source = path.read_text(encoding="utf-8")
    if sys.version_info >= (3, 8):
        ast.parse(source, feature_version=(3, 6))
    else:
        ast.parse(source)
    assert "multiprocessing.shared_memory" not in source
