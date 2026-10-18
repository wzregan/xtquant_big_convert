"""独立进程可重开的共享内存、互斥与唤醒原语，不依赖网络连接。"""

import contextlib
import ctypes
import errno
import hashlib
import json
import math
import mmap
import operator
import os
import stat
import struct
import threading
import time

from .base import TransportError, TransportTimeout


_WAIT_OBJECT_0 = 0
_WAIT_ABANDONED = 128
_WAIT_TIMEOUT = 258
_WAIT_FAILED = 0xFFFFFFFF
_META_MAGIC = b"BQMTREG1"


def endpoint_name(namespace, account_id, suffix):
    """生成稳定名称；namespace 可显式指定 Windows Local\\ 或 Global\\ 作用域。"""
    scope, namespace = _windows_scope(str(namespace or "bigqmt"))
    parts = [namespace, str(account_id or ""), str(suffix or "")]
    digest = hashlib.sha256(json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()
    return scope + "bigqmt_" + digest


def _windows_scope(name):
    """保留显式会话作用域，缺省使用当前登录会话，不做权限失败降级。"""
    for prefix in ("Local\\", "Global\\"):
        if name.startswith(prefix):
            return prefix, name[len(prefix):]
    return "Local\\", name


def _windows_name(name, kind):
    """为映射、尺寸元数据和同步对象派生互不冲突的内核名称。"""
    scope, value = _windows_scope(str(name))
    return scope + "bigqmt_" + hashlib.sha256(value.encode("utf-8")).hexdigest() + "_" + kind


def _timeout(value):
    """校验秒单位超时，零表示只尝试一次；无限及负值必须显式拒绝。"""
    try:
        seconds = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("timeout_seconds must be finite and nonnegative")
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError("timeout_seconds must be finite and nonnegative")
    return seconds


def _take_thread_lock(lock, deadline):
    """按单调时钟预算获取线程锁，避免超大有限值溢出底层平台等待接口。"""
    if lock.acquire(False):
        return True
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        if lock.acquire(timeout=min(remaining, threading.TIMEOUT_MAX)):
            return True


class _Lifecycle(object):
    """关闭前等待所有本实例操作结束，禁止释放仍被等待者使用的句柄。"""

    def __init__(self):
        """初始化实例内操作计数；资源初始化失败也可安全回收。"""
        self._lifecycle = threading.Condition()
        self._active = 0
        self._active_threads = {}
        self._closing = False
        self._closed = False

    @contextlib.contextmanager
    def _operation(self):
        """为操作保留资源生命周期，退出时通知等待关闭的线程。"""
        thread_id = threading.get_ident()
        with self._lifecycle:
            if self._closed or self._closing:
                raise TransportError("shared-memory primitive is closed or closing")
            self._active += 1
            self._active_threads[thread_id] = self._active_threads.get(thread_id, 0) + 1
        try:
            yield
        finally:
            with self._lifecycle:
                self._active -= 1
                count = self._active_threads[thread_id] - 1
                if count:
                    self._active_threads[thread_id] = count
                else:
                    del self._active_threads[thread_id]
                self._lifecycle.notify_all()

    def _reserve_close(self):
        """取得唯一关闭权；同线程在锁内关闭明确拒绝，避免自我等待。"""
        with self._lifecycle:
            if self._active_threads.get(threading.get_ident()):
                raise TransportError("cannot close a primitive inside its active operation")
            while self._closing and not self._closed:
                self._lifecycle.wait()
            if self._closed:
                return False
            self._closing = True
            while self._active:
                self._lifecycle.wait()
            return True

    def _finish_close(self, success):
        """发布关闭结果；例如外部 memoryview 未释放时允许调用方修复后重试。"""
        with self._lifecycle:
            self._closed = bool(success)
            self._closing = False
            self._lifecycle.notify_all()


def _resource_directory(directory, create=True):
    """准备 POSIX 用户私有目录；显式目录由调用方提供，拒绝叶子符号链接。"""
    value = os.fspath(directory) if directory is not None else "/dev/shm/bigqmt-%d" % os.getuid()
    absolute = os.path.abspath(value)
    if os.path.islink(absolute):
        raise TransportError("shared-memory directory cannot be a symlink")
    if create:
        try:
            os.makedirs(absolute, mode=0o700, exist_ok=True)
            info = os.stat(absolute)
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
                raise TransportError("shared-memory directory must be owned by the current user")
            if directory is None:
                os.chmod(absolute, 0o700)
        except OSError as exc:
            raise TransportError("cannot prepare shared-memory directory: %s" % exc)
    return os.path.realpath(absolute)


def _resource_key(name):
    """用完整逻辑名派生文件名，禁止用户名称逃逸目录。"""
    return "bigqmt_" + hashlib.sha256(str(name).encode("utf-8")).hexdigest()


def _open_private_file(path, exclusive=False):
    """打开当前用户的常规资源文件，并拒绝符号链接与其它用户的文件。"""
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    if exclusive:
        flags |= os.O_EXCL
    fd = os.open(path, flags, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise TransportError("shared-memory resource must be a regular file owned by the current user")
        return fd
    except Exception:
        os.close(fd)
        raise


def _take_flock(fd, deadline):
    """非阻塞获取进程文件锁，短竞争快速重试、长竞争退避以避免忙等。"""
    import fcntl

    delay = 0.0001
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError as exc:
            if exc.errno == errno.EINTR:
                # 连续信号中断也消耗同一个超时预算，不能绕过下面的竞争等待检查。
                if time.monotonic() >= deadline:
                    return False
                continue
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise TransportError("shared-memory lock failed: %s" % exc)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(delay, remaining))
        delay = min(delay * 2.0, 0.002)


def _release_flock(fd):
    """释放当前实例取得的文件锁，不关闭仍供下一次操作使用的文件。"""
    import fcntl

    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
            return
        except OSError as exc:
            if exc.errno != errno.EINTR:
                raise TransportError("shared-memory unlock failed: %s" % exc)


_KERNEL32 = None
_KERNEL_LOCK = threading.Lock()


def _kernel32():
    """声明完整 Win32 ABI，尤其 HANDLE 必须在 64 位 Python 中保留全部位数。"""
    global _KERNEL32
    if _KERNEL32 is not None:
        return _KERNEL32
    with _KERNEL_LOCK:
        if _KERNEL32 is not None:
            return _KERNEL32
        from ctypes import wintypes

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        signatures = {
            "CreateMutexW": ([ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR], wintypes.HANDLE),
            "ReleaseMutex": ([wintypes.HANDLE], wintypes.BOOL),
            "CreateEventW": ([ctypes.c_void_p, wintypes.BOOL, wintypes.BOOL, wintypes.LPCWSTR], wintypes.HANDLE),
            "SetEvent": ([wintypes.HANDLE], wintypes.BOOL),
            "WaitForSingleObject": ([wintypes.HANDLE, wintypes.DWORD], wintypes.DWORD),
            "CloseHandle": ([wintypes.HANDLE], wintypes.BOOL),
            "OpenProcess": ([wintypes.DWORD, wintypes.BOOL, wintypes.DWORD], wintypes.HANDLE),
            "GetCurrentProcess": ([], wintypes.HANDLE),
            "GetProcessTimes": ([wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4, wintypes.BOOL),
        }
        for name, signature in signatures.items():
            function = getattr(kernel, name)
            function.argtypes, function.restype = signature
        _KERNEL32 = kernel
        return kernel


def _windows_error(message):
    """将 Win32 错误保留在传输异常中，权限失败必须明确暴露。"""
    return TransportError("%s: %s" % (message, ctypes.WinError(ctypes.get_last_error())))


def _wait_handle(handle, seconds):
    """在单调时钟预算内等待 Win32 对象，长有限超时拆分而非转成 INFINITE。"""
    kernel = _kernel32()
    deadline = time.monotonic() + seconds
    while True:
        remaining = max(deadline - time.monotonic(), 0.0)
        milliseconds = (0xFFFFFFFE if remaining >= 0xFFFFFFFE / 1000.0
                        else int(math.ceil(remaining * 1000.0)))
        result = kernel.WaitForSingleObject(handle, milliseconds)
        if result == _WAIT_FAILED:
            raise _windows_error("WaitForSingleObject failed")
        if result != _WAIT_TIMEOUT or time.monotonic() >= deadline:
            return result


class SharedRegion(_Lifecycle):
    """具备进程互斥的命名映射，原始 buffer 完整留给上层协议。"""

    def __init__(self, name, size, directory=None, timeout_seconds=5.0):
        """创建映射；size 为字节数，timeout_seconds 覆盖整个初始化锁等待预算。"""
        super(SharedRegion, self).__init__()
        seconds = _timeout(5.0 if timeout_seconds is None else timeout_seconds)
        deadline = time.monotonic() + seconds
        try:
            size = operator.index(size)
        except TypeError:
            raise ValueError("shared-memory size must be a positive integer")
        if size <= 0:
            raise ValueError("shared-memory size must be a positive integer")
        self.name = str(name)
        self.size = size
        self.buffer = None
        self._size_mapping = None
        self._mutex = None
        self._lock_fd = None
        self._region_fd = None
        self._local_lock = threading.Lock()
        self._initialization_abandoned = False
        try:
            if os.name == "nt":
                self._initialize_windows(deadline)
            else:
                self._initialize_posix(directory, deadline)
        except Exception as exc:
            self.close()
            if isinstance(exc, (TransportError, TransportTimeout, ValueError)):
                raise
            raise TransportError("shared-memory initialization failed: %s" % exc)

    def _initialize_windows(self, deadline):
        """在命名 mutex 内检查精确字节尺寸，避免映射页对齐掩盖尺寸冲突。"""
        kernel = _kernel32()
        self._mutex = kernel.CreateMutexW(None, False, _windows_name(self.name, "mutex"))
        if not self._mutex:
            raise _windows_error("CreateMutexW failed")
        result = _wait_handle(self._mutex, max(deadline - time.monotonic(), 0.0))
        if result not in (_WAIT_OBJECT_0, _WAIT_ABANDONED):
            raise TransportTimeout("shared-memory initialization mutex timed out")
        self._initialization_abandoned = result == _WAIT_ABANDONED
        try:
            # Python mmap 无法查询已有映射的精确尺寸；独立小映射保留该契约。
            self._size_mapping = mmap.mmap(-1, 32, tagname=_windows_name(self.name, "size"))
            marker = self._size_mapping[:8]
            if marker == _META_MAGIC:
                existing = struct.unpack_from("<Q", self._size_mapping, 8)[0]
                if existing != self.size:
                    raise TransportError("shared-memory size mismatch: existing=%d requested=%d" % (existing, self.size))
            elif marker != b"\0" * 8:
                raise TransportError("shared-memory size metadata is corrupt")
            self.buffer = mmap.mmap(-1, self.size, tagname=_windows_name(self.name, "region"))
            if marker != _META_MAGIC:
                struct.pack_into("<Q", self._size_mapping, 8, self.size)
                self._size_mapping[:8] = _META_MAGIC
        finally:
            if not kernel.ReleaseMutex(self._mutex):
                raise _windows_error("ReleaseMutex failed during initialization")

    def _initialize_posix(self, directory, deadline):
        """持有跨进程初始化锁后创建文件或核验已有尺寸，不截断已有数据。"""
        self.directory = _resource_directory(directory)
        key = _resource_key(self.name)
        self._lock_fd = _open_private_file(os.path.join(self.directory, key + ".lock"))
        if not _take_flock(self._lock_fd, deadline):
            raise TransportTimeout("shared-memory initialization file lock timed out")
        try:
            path = os.path.join(self.directory, key + ".region")
            try:
                self._region_fd = _open_private_file(path, exclusive=True)
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
                self._region_fd = _open_private_file(path)
                actual_size = os.fstat(self._region_fd).st_size
                if actual_size != self.size:
                    raise TransportError("shared-memory size mismatch: existing=%d requested=%d" % (actual_size, self.size))
            else:
                os.ftruncate(self._region_fd, self.size)
            self.buffer = mmap.mmap(self._region_fd, self.size)
        finally:
            _release_flock(self._lock_fd)

    @contextlib.contextmanager
    def locked(self, timeout_seconds=5.0):
        """在有限秒预算内取得独占锁；返回 Windows owner 崩溃标记供协议重建。"""
        seconds = _timeout(5.0 if timeout_seconds is None else timeout_seconds)
        deadline = time.monotonic() + seconds
        with self._operation():
            if not _take_thread_lock(self._local_lock, deadline):
                raise TransportTimeout("shared-memory thread lock timed out")
            acquired = False
            try:
                if os.name == "nt":
                    result = _wait_handle(self._mutex, max(deadline - time.monotonic(), 0.0))
                    acquired = result in (_WAIT_OBJECT_0, _WAIT_ABANDONED)
                    abandoned = result == _WAIT_ABANDONED or self._initialization_abandoned
                else:
                    acquired = _take_flock(self._lock_fd, deadline)
                    abandoned = False
                if not acquired:
                    raise TransportTimeout("shared-memory process lock timed out")
                self._initialization_abandoned = False
                yield abandoned
            finally:
                try:
                    if acquired:
                        if os.name == "nt":
                            if not _kernel32().ReleaseMutex(self._mutex):
                                raise _windows_error("ReleaseMutex failed")
                        else:
                            _release_flock(self._lock_fd)
                finally:
                    self._local_lock.release()

    def close(self):
        """等待本实例在途操作结束并幂等关闭，不自动移除其它进程使用的名称。"""
        if not self._reserve_close():
            return
        success = False
        try:
            if self.buffer is not None:
                self.buffer.close()
                self.buffer = None
            if self._size_mapping is not None:
                self._size_mapping.close()
                self._size_mapping = None
            if self._mutex is not None:
                if not _kernel32().CloseHandle(self._mutex):
                    raise _windows_error("CloseHandle failed")
                self._mutex = None
            for field in ("_region_fd", "_lock_fd"):
                fd = getattr(self, field)
                if fd is not None:
                    os.close(fd)
                    setattr(self, field, None)
            success = True
        finally:
            self._finish_close(success)


class _Timespec(ctypes.Structure):
    """POSIX 绝对超时结构，按本机 C long ABI 布局。"""

    _fields_ = [("tv_sec", ctypes.c_long), ("tv_nsec", ctypes.c_long)]


_SEM_LIB = None
_SEM_LIB_LOCK = threading.Lock()


def _sem_library():
    """加载具备释放 GIL 等待能力的 POSIX 命名信号量接口。"""
    global _SEM_LIB
    if _SEM_LIB is not None:
        return _SEM_LIB
    with _SEM_LIB_LOCK:
        if _SEM_LIB is not None:
            return _SEM_LIB
        library = ctypes.CDLL(None, use_errno=True)
        if not hasattr(library, "sem_open"):
            from ctypes import util as ctypes_util

            library = ctypes.CDLL(ctypes_util.find_library("rt") or "librt.so.1", use_errno=True)
        signatures = {
            "sem_open": ([ctypes.c_char_p, ctypes.c_int, ctypes.c_uint, ctypes.c_uint], ctypes.c_void_p),
            "sem_close": ([ctypes.c_void_p], ctypes.c_int),
            "sem_unlink": ([ctypes.c_char_p], ctypes.c_int),
            "sem_post": ([ctypes.c_void_p], ctypes.c_int),
            "sem_getvalue": ([ctypes.c_void_p, ctypes.POINTER(ctypes.c_int)], ctypes.c_int),
            "sem_trywait": ([ctypes.c_void_p], ctypes.c_int),
            "sem_timedwait": ([ctypes.c_void_p, ctypes.POINTER(_Timespec)], ctypes.c_int),
        }
        if hasattr(library, "sem_clockwait"):
            signatures["sem_clockwait"] = ([ctypes.c_void_p, ctypes.c_int, ctypes.POINTER(_Timespec)], ctypes.c_int)
        for name, signature in signatures.items():
            function = getattr(library, name)
            function.argtypes, function.restype = signature
        _SEM_LIB = library
        return library


def _semaphore_name(name, directory):
    """将私有目录也计入信号量名称，隔离不同测试目录及其它用户的资源。"""
    value = "%s\0%s" % (directory, name)
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return ("/bigqmt_%d_%s" % (os.getuid(), digest)).encode("ascii")


def _absolute_timespec(clock_value):
    """把绝对秒转换为规范化的 timespec，避免浮点舍入产生非法纳秒字段。"""
    seconds = int(clock_value)
    nanoseconds = int((clock_value - seconds) * 1_000_000_000)
    return _Timespec(seconds, min(max(nanoseconds, 0), 999_999_999))


class NamedSignal(_Lifecycle):
    """命名自动消费的唤醒提示；协议必须重查共享内存谓词，不能用它计消息数。"""

    def __init__(self, name, directory=None):
        """创建或打开提示；独立消费者应使用不同名称以获得各自唤醒。"""
        super(NamedSignal, self).__init__()
        self.name = str(name)
        self._event = None
        self._semaphore = None
        self._set_fd = None
        self._set_lock = threading.Lock()
        try:
            if os.name == "nt":
                self._event = _kernel32().CreateEventW(None, False, False, _windows_name(self.name, "event"))
                if not self._event:
                    raise _windows_error("CreateEventW failed")
            else:
                self.directory = _resource_directory(directory)
                self._set_fd = _open_private_file(os.path.join(self.directory, _resource_key(self.name) + ".signal.lock"))
                self._semaphore_name = _semaphore_name(self.name, self.directory)
                self._semaphore = _sem_library().sem_open(self._semaphore_name, os.O_CREAT, 0o600, 0)
                if self._semaphore in (None, ctypes.c_void_p(-1).value):
                    self._semaphore = None
                    raise TransportError("sem_open failed: %s" % os.strerror(ctypes.get_errno()))
        except Exception as exc:
            self.close()
            if isinstance(exc, TransportError):
                raise
            raise TransportError("named signal initialization failed: %s" % exc)

    def set(self, timeout_seconds=0.0):
        """合并提示并返回是否已有提示可消费；合并锁忙时默认立即返回 False。

        timeout_seconds 可指定有限等待预算，缺省不等待，避免悬停的提示发送者
        拖穿 RPC 总超时。返回 False 不撤销共享内存谓词，协议应定期重查谓词。
        """
        seconds = _timeout(timeout_seconds)
        deadline = time.monotonic() + seconds
        with self._operation():
            if os.name == "nt":
                if not _kernel32().SetEvent(self._event):
                    raise _windows_error("SetEvent failed")
                return True
            library = _sem_library()
            if not _take_thread_lock(self._set_lock, deadline):
                return False
            try:
                if not _take_flock(self._set_fd, deadline):
                    return False
                try:
                    value = ctypes.c_int()
                    if library.sem_getvalue(self._semaphore, ctypes.byref(value)) != 0:
                        raise TransportError("sem_getvalue failed: %s" % os.strerror(ctypes.get_errno()))
                    if value.value <= 0 and library.sem_post(self._semaphore) != 0:
                        raise TransportError("sem_post failed: %s" % os.strerror(ctypes.get_errno()))
                    return True
                finally:
                    _release_flock(self._set_fd)
            finally:
                self._set_lock.release()

    def wait(self, timeout_seconds):
        """有限等待并消费一个提示；零为非阻塞，超时返回 False。"""
        seconds = _timeout(timeout_seconds)
        with self._operation():
            if os.name == "nt":
                return _wait_handle(self._event, seconds) == _WAIT_OBJECT_0
            library = _sem_library()
            deadline = time.monotonic() + seconds
            while True:
                if seconds == 0:
                    result = library.sem_trywait(self._semaphore)
                elif hasattr(library, "sem_clockwait"):
                    # 超大有限预算按天分段，防止 C timespec 的整数范围溢出。
                    stamp = _absolute_timespec(min(deadline, time.monotonic() + 86400.0))
                    result = library.sem_clockwait(self._semaphore, time.CLOCK_MONOTONIC, ctypes.byref(stamp))
                else:
                    # 旧 libc 只有实时时钟接口；被信号中断后重新计算剩余预算。
                    stamp = _absolute_timespec(time.time() + min(max(deadline - time.monotonic(), 0.0), 86400.0))
                    result = library.sem_timedwait(self._semaphore, ctypes.byref(stamp))
                if result == 0:
                    return True
                error = ctypes.get_errno()
                if error == errno.EINTR:
                    if time.monotonic() >= deadline:
                        return False
                    continue
                if error in (errno.EAGAIN, errno.ETIMEDOUT):
                    if seconds == 0 or time.monotonic() >= deadline:
                        return False
                    continue
                raise TransportError("named semaphore wait failed: %s" % os.strerror(error))

    def close(self):
        """等待在途 set/wait 结束后幂等关闭，避免关闭阻塞等待中的句柄。"""
        if not self._reserve_close():
            return
        success = False
        try:
            if self._event is not None:
                if not _kernel32().CloseHandle(self._event):
                    raise _windows_error("CloseHandle event failed")
                self._event = None
            if self._semaphore is not None:
                if _sem_library().sem_close(self._semaphore) != 0:
                    raise TransportError("sem_close failed: %s" % os.strerror(ctypes.get_errno()))
                self._semaphore = None
            if self._set_fd is not None:
                os.close(self._set_fd)
                self._set_fd = None
            success = True
        finally:
            self._finish_close(success)


def _linux_identity(pid):
    """解析含空格或右括号的进程名，返回 Linux 状态和启动 tick。"""
    with open("/proc/%d/stat" % pid, "r") as handle:
        tail = handle.read().rpartition(") ")[2].split()
    if len(tail) <= 19:
        raise ValueError("invalid Linux process stat")
    return tail[0], int(tail[19])


def _windows_birth(handle):
    """返回 Windows FILETIME 创建时间，保留原始 100 纳秒整数精度。"""
    from ctypes import wintypes

    values = [wintypes.FILETIME() for _index in range(4)]
    if not _kernel32().GetProcessTimes(handle, *[ctypes.byref(value) for value in values]):
        raise _windows_error("GetProcessTimes failed")
    return (int(values[0].dwHighDateTime) << 32) | int(values[0].dwLowDateTime)


def process_identity():
    """返回当前 pid 与整数出生标记；不可读取时标记为零，存活检查保持保守。"""
    pid = os.getpid()
    try:
        birth = _windows_birth(_kernel32().GetCurrentProcess()) if os.name == "nt" else _linux_identity(pid)[1]
    except Exception:
        birth = 0
    return pid, birth


def process_alive(pid, birth_int):
    """核验 pid 及出生标记；明确已退出或复用返回 False，读取异常保守返回 True。"""
    try:
        pid, birth_int = int(pid), int(birth_int)
    except (TypeError, ValueError, OverflowError):
        return True
    if pid <= 0:
        return False
    try:
        if os.name != "nt":
            state, actual_birth = _linux_identity(pid)
            return state not in ("Z", "X") and (birth_int <= 0 or birth_int == actual_birth)
        kernel = _kernel32()
        handle = kernel.OpenProcess(0x1000 | 0x100000, False, pid)
        if not handle:
            return ctypes.get_last_error() not in (87, 1168)
        try:
            state = kernel.WaitForSingleObject(handle, 0)
            if state == _WAIT_OBJECT_0:
                return False
            if state != _WAIT_TIMEOUT:
                return True
            return birth_int <= 0 or _windows_birth(handle) == birth_int
        finally:
            kernel.CloseHandle(handle)
    except OSError as exc:
        return exc.errno not in (errno.ENOENT, errno.ESRCH)
    except Exception:
        return True


def cleanup(name, directory=None):
    """仅在全部使用者停止后显式清理 POSIX 名称；普通 close 不移除共享资源。"""
    if os.name == "nt":
        return
    location = _resource_directory(directory, create=False)
    key = _resource_key(name)
    for suffix in (".region", ".lock", ".signal.lock"):
        try:
            os.unlink(os.path.join(location, key + suffix))
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise TransportError("shared-memory cleanup failed: %s" % exc)
    if _sem_library().sem_unlink(_semaphore_name(str(name), location)) != 0:
        error = ctypes.get_errno()
        if error != errno.ENOENT:
            raise TransportError("named semaphore cleanup failed: %s" % os.strerror(error))
