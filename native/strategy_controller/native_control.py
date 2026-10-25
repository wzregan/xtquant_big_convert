#coding:utf-8
"""通过独立共享内存控制已核对的大 QMT 模型，并支持显式注入自有控制 DLL。"""

import argparse
from contextlib import contextmanager
import ctypes as ct
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import struct
import sys
import time
import xml.etree.ElementTree as ET


MAGIC = 0x43544D51
ABI = 1
FORMULA_SHA256 = "355fdbdc10d4661134b86725c479dbdfa3af5a1fb5a0d7c1977da39a73aacec4"
STATUS = {
    0: "OK", 1: "WRONG_HOST", 2: "FORMULA_MISSING", 3: "PROFILE_MISMATCH",
    4: "MANAGER_MISSING", 5: "NATIVE_EXCEPTION", 6: "INVALID_ARGUMENT", 7: "INIT_CONFLICT",
    8: "WINDOW_MISSING", 9: "MAPPING_ERROR", 10: "HOOK_ERROR", 11: "WRONG_THREAD",
    12: "TARGET_REJECTED", 13: "COMMAND_EXPIRED", 14: "EVENT_ERROR", 15: "NATIVE_STOP_REJECTED",
    16: "NOT_INITIALIZED", 17: "UNKNOWN_COMMAND", 18: "MANAGER_CHANGED", 19: "POST_MESSAGE_ERROR",
    20: "THREAD_ERROR", 0xFFFFFFFF: "IN_PROGRESS",
}


class ControlBlock(ct.Structure):
    """与原生 DLL 对齐的 256 字节协议，序列号为对齐的 64 位字段。"""
    _fields_ = [
        ("struct_bytes", ct.c_uint32), ("abi_version", ct.c_uint32), ("magic", ct.c_uint32), ("pid", ct.c_uint32),
        ("creation_time", ct.c_uint64), ("account_id", ct.c_uint64),
        ("allowed_model", ct.c_int32), ("gui_thread_id", ct.c_uint32),
        ("gui_hwnd", ct.c_uint64), ("manager", ct.c_uint64), ("manager_qt_thread", ct.c_uint64),
        ("request_seq", ct.c_int64), ("completed_seq", ct.c_int64), ("deadline_tick", ct.c_uint64),
        ("operation", ct.c_uint32), ("model", ct.c_int32), ("status", ct.c_uint32), ("win_error", ct.c_uint32),
        ("strategy_count", ct.c_int32), ("running_count", ct.c_int32),
        ("execution_qt_thread", ct.c_uint64), ("execution_thread_id", ct.c_uint32), ("native_result", ct.c_uint32),
        ("completed_tick", ct.c_uint64), ("initialized", ct.c_uint32), ("listener_thread_id", ct.c_uint32),
        ("dispatcher_nonce", ct.c_uint64), ("reserved", ct.c_uint64 * 13),
    ]


class InitArgs(ct.Structure):
    """固定宽度 UTF-16 初始化参数，避免 Linux 与 Windows 的 wchar_t 宽度差异。"""
    _fields_ = [
        ("struct_bytes", ct.c_uint32), ("abi_version", ct.c_uint32), ("pid", ct.c_uint32), ("allowed_model", ct.c_int32),
        ("account_id", ct.c_uint64), ("creation_time", ct.c_uint64), ("gui_hwnd_hint", ct.c_uint64),
        ("mapping_name", ct.c_uint16 * 64), ("reserved", ct.c_uint64 * 3),
    ]


class FileTime(ct.Structure):
    """Windows 精确进程创建时间的两个 32 位分量。"""
    _fields_ = [("low", ct.c_uint32), ("high", ct.c_uint32)]


class SecurityAttributes(ct.Structure):
    """调用期间持有安全描述符，内核对象不继承给子进程。"""
    _fields_ = [("length", ct.c_uint32), ("descriptor", ct.c_void_p), ("inherit", ct.c_int32)]


class ModuleEntry(ct.Structure):
    """使用加载器模块列表取得实际 HMODULE，避免把普通文件映射误认为 DLL 基址。"""
    _fields_ = [
        ("size", ct.c_uint32), ("module_id", ct.c_uint32), ("process_id", ct.c_uint32),
        ("global_usage", ct.c_uint32), ("process_usage", ct.c_uint32),
        ("base", ct.c_void_p), ("image_size", ct.c_uint32), ("module", ct.c_void_p),
        ("name", ct.c_uint16 * 256), ("path", ct.c_uint16 * 260),
    ]


class ControlError(RuntimeError):
    """保留已执行或仍待完成的序列号，禁止将未知结果自动重试。"""


@dataclass(frozen=True)
class Target:
    """目标身份；测试目标只用于自建窗口，生产目标由配置和进程路径共同验证。"""
    pid: int
    creation_time: int
    model: int
    account: int
    exe: str
    root: str = ""
    strategy: str = "QMT_TRADE_BACKEND"
    test: bool = False

    @property
    def mapping_name(self):
        """按精确创建时间隔离 PID 重用，Global 名称可跨 SSH 与桌面会话访问。"""
        return "Global\\QmtNativeCtl-%d-%016x" % (self.pid, self.creation_time)


class WinAPI:
    """绑定明确的 64 位 Windows 类型，所有调用句柄由创建它们的对象关闭。"""

    def __init__(self):
        """仅在 Windows x64 初始化，普通 Linux 测试仍可导入协议结构。"""
        if sys.platform != "win32" or ct.sizeof(ct.c_void_p) != 8:
            raise ControlError("Windows x64 is required")
        self.kernel = ct.WinDLL("kernel32", use_last_error=True)
        self.advapi = ct.WinDLL("advapi32", use_last_error=True)
        signatures = {
            "OpenProcess": (ct.c_void_p, [ct.c_uint32, ct.c_int32, ct.c_uint32]),
            "GetCurrentProcess": (ct.c_void_p, []),
            "GetProcessTimes": (ct.c_int32, [ct.c_void_p, *([ct.POINTER(FileTime)] * 4)]),
            "QueryFullProcessImageNameW": (ct.c_int32, [ct.c_void_p, ct.c_uint32, ct.c_wchar_p, ct.POINTER(ct.c_uint32)]),
            "CloseHandle": (ct.c_int32, [ct.c_void_p]), "LocalFree": (ct.c_void_p, [ct.c_void_p]),
            "CreateFileMappingW": (ct.c_void_p, [ct.c_void_p, ct.POINTER(SecurityAttributes), ct.c_uint32, ct.c_uint32, ct.c_uint32, ct.c_wchar_p]),
            "OpenFileMappingW": (ct.c_void_p, [ct.c_uint32, ct.c_int32, ct.c_wchar_p]),
            "MapViewOfFile": (ct.c_void_p, [ct.c_void_p, ct.c_uint32, ct.c_uint32, ct.c_uint32, ct.c_size_t]),
            "UnmapViewOfFile": (ct.c_int32, [ct.c_void_p]),
            "CreateEventW": (ct.c_void_p, [ct.POINTER(SecurityAttributes), ct.c_int32, ct.c_int32, ct.c_wchar_p]),
            "OpenEventW": (ct.c_void_p, [ct.c_uint32, ct.c_int32, ct.c_wchar_p]),
            "CreateMutexW": (ct.c_void_p, [ct.POINTER(SecurityAttributes), ct.c_int32, ct.c_wchar_p]),
            "ReleaseMutex": (ct.c_int32, [ct.c_void_p]), "SetEvent": (ct.c_int32, [ct.c_void_p]),
            "WaitForSingleObject": (ct.c_uint32, [ct.c_void_p, ct.c_uint32]),
            "GetTickCount64": (ct.c_uint64, []),
            "GetModuleHandleExW": (ct.c_int32, [ct.c_uint32, ct.c_void_p, ct.POINTER(ct.c_void_p)]),
            "GetModuleFileNameW": (ct.c_uint32, [ct.c_void_p, ct.c_wchar_p, ct.c_uint32]),
            "VirtualAllocEx": (ct.c_void_p, [ct.c_void_p, ct.c_void_p, ct.c_size_t, ct.c_uint32, ct.c_uint32]),
            "VirtualFreeEx": (ct.c_int32, [ct.c_void_p, ct.c_void_p, ct.c_size_t, ct.c_uint32]),
            "WriteProcessMemory": (ct.c_int32, [ct.c_void_p, ct.c_void_p, ct.c_void_p, ct.c_size_t, ct.POINTER(ct.c_size_t)]),
            "CreateRemoteThread": (ct.c_void_p, [ct.c_void_p, ct.c_void_p, ct.c_size_t, ct.c_void_p, ct.c_void_p, ct.c_uint32, ct.POINTER(ct.c_uint32)]),
            "GetExitCodeThread": (ct.c_int32, [ct.c_void_p, ct.POINTER(ct.c_uint32)]),
            "IsWow64Process": (ct.c_int32, [ct.c_void_p, ct.POINTER(ct.c_int32)]),
            "CreateToolhelp32Snapshot": (ct.c_void_p, [ct.c_uint32, ct.c_uint32]),
            "Module32FirstW": (ct.c_int32, [ct.c_void_p, ct.POINTER(ModuleEntry)]),
            "Module32NextW": (ct.c_int32, [ct.c_void_p, ct.POINTER(ModuleEntry)]),
            "ReadProcessMemory": (ct.c_int32, [ct.c_void_p, ct.c_void_p, ct.c_void_p, ct.c_size_t, ct.POINTER(ct.c_size_t)]),
        }
        for name, (result, args) in signatures.items():
            function = getattr(self.kernel, name)
            function.restype = result
            function.argtypes = args
        for name, args in {
            "OpenProcessToken": [ct.c_void_p, ct.c_uint32, ct.POINTER(ct.c_void_p)],
            "GetTokenInformation": [ct.c_void_p, ct.c_uint32, ct.c_void_p, ct.c_uint32, ct.POINTER(ct.c_uint32)],
            "ConvertSidToStringSidW": [ct.c_void_p, ct.POINTER(ct.c_void_p)],
            "ConvertStringSecurityDescriptorToSecurityDescriptorW": [ct.c_wchar_p, ct.c_uint32, ct.POINTER(ct.c_void_p), ct.c_void_p],
        }.items():
            function = getattr(self.advapi, name)
            function.restype = ct.c_int32
            function.argtypes = args

    def require(self, value, action):
        """立即读取失败调用的错误码，不将句柄为空解释为成功。"""
        if not value:
            raise ControlError("%s: %s" % (action, ct.WinError(ct.get_last_error())))
        return value

    @contextmanager
    def process(self, target, write=False):
        """校验精确路径和创建时间；write 只为显式注入申请进程写入及建线程权限。"""
        rights = 0x400 | (0x2 | 0x8 | 0x10 | 0x20 if write else 0)
        handle = self.require(self.kernel.OpenProcess(rights, 0, target.pid), "OpenProcess")
        try:
            buffer = ct.create_unicode_buffer(32768)
            size = ct.c_uint32(len(buffer))
            self.require(self.kernel.QueryFullProcessImageNameW(handle, 0, buffer, ct.byref(size)), "QueryFullProcessImageNameW")
            if str(Path(buffer.value)).casefold() != str(Path(target.exe)).casefold():
                raise ControlError("target executable changed")
            if self.creation(handle) != target.creation_time:
                raise ControlError("target process generation changed")
            wow = ct.c_int32()
            self.require(self.kernel.IsWow64Process(handle, ct.byref(wow)), "IsWow64Process")
            if wow.value:
                raise ControlError("32-bit target is unsupported")
            yield handle
        finally:
            self.kernel.CloseHandle(handle)

    def creation(self, process):
        """读取进程 FILETIME，返回未经浮点转换的 100 纳秒计数。"""
        fields = [FileTime() for _ in range(4)]
        self.require(self.kernel.GetProcessTimes(process, *(ct.byref(f) for f in fields)), "GetProcessTimes")
        return fields[0].low | fields[0].high << 32

    def sid(self, process):
        """读取当前或目标进程的用户 SID，用于向两个明确身份授权共享内核对象。"""
        token = ct.c_void_p()
        self.require(self.advapi.OpenProcessToken(process, 8, ct.byref(token)), "OpenProcessToken")
        try:
            size = ct.c_uint32()
            self.advapi.GetTokenInformation(token, 1, None, 0, ct.byref(size))
            if not size.value:
                raise ControlError("TokenUser size unavailable")
            buffer = ct.create_string_buffer(size.value)
            self.require(self.advapi.GetTokenInformation(token, 1, buffer, size.value, ct.byref(size)), "GetTokenInformation")
            sid = ct.c_void_p.from_buffer(buffer).value
            text = ct.c_void_p()
            self.require(self.advapi.ConvertSidToStringSidW(sid, ct.byref(text)), "ConvertSidToStringSidW")
            try:
                return ct.wstring_at(text)
            finally:
                self.kernel.LocalFree(text)
        finally:
            self.kernel.CloseHandle(token)


def identify_target(root, strategy, account):
    """从模型配置和唯一进程解析目标，账号必须与指定模型一致。"""
    import psutil
    api = WinAPI()
    root = Path(root).resolve()
    models = [p for p in ET.parse(root / "config/indexUserConfig.xml").findall(".//strategyTrade/item")
              if p.get("name") == strategy and p.get("account") == str(account)]
    if len(models) != 1:
        raise ControlError("expected exactly one matching model and account")
    exe = str(root / "bin.x64/XtItClient.exe")
    pids = [p.pid for p in psutil.process_iter(["name", "exe"])
            if (p.info["name"] or "").casefold() == "xtitclient.exe"
            and (p.info["exe"] or "").casefold() == exe.casefold()]
    if len(pids) != 1:
        raise ControlError("expected exactly one target QMT process")
    handle = api.require(api.kernel.OpenProcess(0x400, 0, pids[0]), "OpenProcess")
    try:
        creation = api.creation(handle)
    finally:
        api.kernel.CloseHandle(handle)
    target = Target(pids[0], creation, int(models[0].get("id")), int(account), exe, str(root), strategy)
    validate_target(target)
    return target


def validate_target(target):
    """每次操作重新核对模型绑定和私有接口版本；测试构建只允许自己的 Python 窗口进程。"""
    api = WinAPI()
    with api.process(target):
        if target.test:
            if Path(target.exe).name.casefold() != "python.exe":
                raise ControlError("test target must be a Python harness")
            return
        root = Path(target.root)
        if target.exe.casefold() != str(root / "bin.x64/XtItClient.exe").casefold():
            raise ControlError("target executable is outside the selected QMT installation")
        models = [p for p in ET.parse(root / "config/indexUserConfig.xml").findall(".//strategyTrade/item")
                  if p.get("name") == target.strategy and p.get("account") == str(target.account)
                  and p.get("id") == str(target.model)]
        if len(models) != 1:
            raise ControlError("model identity changed")
        if hashlib.sha256((root / "bin.x64/Formula.dll").read_bytes()).hexdigest() != FORMULA_SHA256:
            raise ControlError("unsupported Formula.dll identity")


class Channel:
    """管理共享映射、事件及生产者互斥量；客户端退出不关闭 DLL 持有的独立引用。"""

    def __init__(self, target, create=False):
        """create 用于注入前创建内核对象，ACL 仅允许系统、调用者和目标用户。"""
        self.api = WinAPI()
        self.target = target
        self.handles = []
        self.view = None
        self.block = None
        descriptor = ct.c_void_p()
        owns_mutex = False
        try:
            with self.api.process(target) as process:
                sids = {self.api.sid(process), self.api.sid(self.api.kernel.GetCurrentProcess()), "SY"}
            sddl = "D:P" + "".join("(A;;GA;;;%s)" % sid for sid in sorted(sids))
            self.api.require(self.api.advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW(sddl, 1, ct.byref(descriptor), None), "ConvertSecurityDescriptor")
            security = SecurityAttributes(ct.sizeof(SecurityAttributes), descriptor.value, 0)
            self.mutex = self._handle(self.api.kernel.CreateMutexW(ct.byref(security), 0, target.mapping_name + "-mutex"), "CreateMutexW")
            wait = self.api.kernel.WaitForSingleObject(self.mutex, 10000)
            if wait not in (0, 128):
                raise ControlError("control construction mutex unavailable: %d" % wait)
            owns_mutex = True
            if create:
                self.mapping = self._handle(self.api.kernel.CreateFileMappingW(ct.c_void_p(-1), ct.byref(security), 4, 0, ct.sizeof(ControlBlock), target.mapping_name), "CreateFileMappingW")
                created = ct.get_last_error() != 183
                self.event = self._handle(self.api.kernel.CreateEventW(ct.byref(security), 0, 0, target.mapping_name + "-request"), "CreateEventW")
            else:
                created = False
                self.mapping = self._handle(self.api.kernel.OpenFileMappingW(6, 0, target.mapping_name), "OpenFileMappingW")
                self.event = self._handle(self.api.kernel.OpenEventW(2, 0, target.mapping_name + "-request"), "OpenEventW")
            self.view = self.api.require(self.api.kernel.MapViewOfFile(self.mapping, 6, 0, 0, ct.sizeof(ControlBlock)), "MapViewOfFile")
            self.block = ControlBlock.from_address(self.view)
            if created:
                self.block.struct_bytes = ct.sizeof(ControlBlock)
                self.block.abi_version = ABI
                self.block.magic = MAGIC
                self.block.pid = target.pid
                self.block.creation_time = target.creation_time
                self.block.allowed_model = target.model
                self.block.account_id = target.account
                self.block.strategy_count = self.block.running_count = -1
            self.check_identity()
        except Exception:
            if owns_mutex:
                self.api.kernel.ReleaseMutex(self.mutex)
                owns_mutex = False
            self.close()
            raise
        finally:
            if owns_mutex:
                self.api.kernel.ReleaseMutex(self.mutex)
            if descriptor:
                self.api.kernel.LocalFree(descriptor)

    def _handle(self, value, operation):
        """保存成功创建的句柄，部分初始化失败时也可统一关闭。"""
        self.api.require(value, operation)
        self.handles.append(value)
        return value

    def check_identity(self):
        """拒绝协议、进程代际或目标账号不一致的共享映射。"""
        b = self.block
        if (b.struct_bytes, b.abi_version, b.magic, b.pid, b.creation_time, b.allowed_model, b.account_id) != (
            ct.sizeof(ControlBlock), ABI, MAGIC, self.target.pid, self.target.creation_time, self.target.model, self.target.account,
        ):
            raise ControlError("shared control identity mismatch")

    @contextmanager
    def locked(self, timeout=10):
        """在 timeout 秒内取得跨进程生产者互斥量；遗弃锁也必须先核对未完成请求。"""
        wait = self.api.kernel.WaitForSingleObject(self.mutex, int(timeout * 1000))
        if wait not in (0, 128):
            raise ControlError("control mutex unavailable: %d" % wait)
        try:
            self.check_identity()
            yield
        finally:
            self.api.kernel.ReleaseMutex(self.mutex)

    def snapshot(self):
        """返回当前缓存和请求状态；只有 request_seq 等于 completed_seq 时回复才完整。"""
        result = {name: getattr(self.block, name) for name, _ in ControlBlock._fields_ if name != "reserved"}
        result["status_name"] = STATUS.get(result["status"], "UNKNOWN")
        result["pending"] = result["request_seq"] != result["completed_seq"]
        result["same_qt_thread"] = bool(result["execution_qt_thread"] and result["execution_qt_thread"] == result["manager_qt_thread"])
        return result

    def command(self, operation, timeout=10, model=None, deadline_tick=None):
        """发布一次状态、启动或停止命令；timeout 单位为秒，超时结果未知时禁止自动重试。"""
        if operation not in (1, 2, 3) or not math.isfinite(timeout) or not 0 < timeout <= 60:
            raise ControlError("invalid command or timeout")
        validate_target(self.target)
        with self.locked(timeout):
            b = self.block
            if not b.initialized:
                raise ControlError("native controller is not initialized")
            if b.request_seq != b.completed_seq:
                raise ControlError("previous command remains pending: %d" % b.request_seq)
            if b.request_seq >= 0x7FFFFFFFFFFFFFFE:
                raise ControlError("control sequence exhausted")
            seq = b.completed_seq + 1
            b.model = self.target.model if model is None else model
            b.operation = operation
            b.deadline_tick = self.api.kernel.GetTickCount64() + int(timeout * 1000) if deadline_tick is None else deadline_tick
            # x64 对齐 64 位写入是原子的；输入字段先写，最后发布序列号，再通过内核事件唤醒。
            b.request_seq = seq
            if not self.api.kernel.SetEvent(self.event):
                b.request_seq = b.completed_seq
                self.api.require(False, "SetEvent")
            end = time.monotonic() + timeout
            while b.completed_seq != seq:
                if time.monotonic() >= end:
                    raise ControlError("command outcome is pending; inspect sequence %d before retrying" % seq)
                time.sleep(0.002)
            result = self.snapshot()
            if result["status"]:
                raise ControlError(json.dumps(result, ensure_ascii=True))
            return result

    def close(self):
        """释放本客户端的所有句柄，保留目标 DLL 对共享对象的引用。"""
        self.block = None
        if self.view:
            self.api.kernel.UnmapViewOfFile(self.view)
            self.view = None
        for handle in reversed(self.handles):
            self.api.kernel.CloseHandle(handle)
        self.handles.clear()

    def __enter__(self):
        """返回已核对的通道。"""
        return self

    def __exit__(self, *args):
        """在调用失败或正常结束时关闭客户端资源。"""
        self.close()


def module_base(pid, path):
    """从目标当前的映像映射取得完整 64 位基址，避免远程线程 DWORD 返回值截断。"""
    api = WinAPI()
    expected = str(Path(path)).casefold()
    snapshot = api.kernel.CreateToolhelp32Snapshot(0x18, pid)
    if snapshot == ct.c_void_p(-1).value:
        api.require(False, "CreateToolhelp32Snapshot")
    try:
        entry = ModuleEntry()
        entry.size = ct.sizeof(entry)
        found = api.kernel.Module32FirstW(snapshot, ct.byref(entry))
        while found:
            name = bytes(entry.path).decode("utf-16le").split("\0", 1)[0]
            if str(Path(name)).casefold() == expected:
                return entry.base
            found = api.kernel.Module32NextW(snapshot, ct.byref(entry))
        if ct.get_last_error() != 18:
            api.require(False, "Module32 enumeration")
    finally:
        api.kernel.CloseHandle(snapshot)
    return None


def verify_loaded_code(api, process, base, dll_path):
    """核对目标已加载的代码段，避免磁盘文件被换版后用新 RVA 调用旧映像。"""
    raw = Path(dll_path).read_bytes()
    nt = struct.unpack_from("<I", raw, 0x3C)[0]
    if raw[nt:nt + 4] != b"PE\0\0" or struct.unpack_from("<H", raw, nt + 4)[0] != 0x8664:
        raise ControlError("controller is not an x64 PE image")
    sections = struct.unpack_from("<H", raw, nt + 6)[0]
    optional_size = struct.unpack_from("<H", raw, nt + 20)[0]
    for index in range(sections):
        offset = nt + 24 + optional_size + index * 40
        flags = struct.unpack_from("<I", raw, offset + 36)[0]
        if not flags & 0x20000000:
            continue
        virtual_size, rva, raw_size, raw_offset = struct.unpack_from("<IIII", raw, offset + 8)
        size = min(virtual_size, raw_size)
        buffer = ct.create_string_buffer(size)
        received = ct.c_size_t()
        api.require(api.kernel.ReadProcessMemory(process, base + rva, buffer, size, ct.byref(received)), "ReadProcessMemory")
        if received.value != size or buffer.raw != raw[raw_offset:raw_offset + size]:
            raise ControlError("loaded controller code differs from the selected build")


def remote_call(api, process, address, payload, timeout=15):
    """把参数写入目标进程并调用已有函数；超时不能释放仍可能被远程线程使用的参数。"""
    memory = api.require(api.kernel.VirtualAllocEx(process, None, len(payload), 0x3000, 4), "VirtualAllocEx")
    thread = None
    finished = False
    try:
        buffer = ct.create_string_buffer(payload)
        written = ct.c_size_t()
        api.require(api.kernel.WriteProcessMemory(process, memory, buffer, len(payload), ct.byref(written)), "WriteProcessMemory")
        if written.value != len(payload):
            raise ControlError("incomplete remote parameter write")
        tid = ct.c_uint32()
        thread = api.require(api.kernel.CreateRemoteThread(process, None, 0, address, memory, 0, ct.byref(tid)), "CreateRemoteThread")
        wait = api.kernel.WaitForSingleObject(thread, int(timeout * 1000))
        if wait != 0:
            raise ControlError("remote thread pending: thread=%d parameter=%d" % (tid.value, memory))
        finished = True
        result = ct.c_uint32()
        api.require(api.kernel.GetExitCodeThread(thread, ct.byref(result)), "GetExitCodeThread")
        return result.value
    finally:
        if thread:
            api.kernel.CloseHandle(thread)
        if finished or thread is None:
            api.kernel.VirtualFreeEx(process, memory, 0, 0x8000)


def inject(target, dll_path, gui_hwnd_hint=0):
    """注入清单中指定的自有 DLL，再显式初始化；重复调用只复用已有控制器。"""
    validate_target(target)
    dll_path = Path(dll_path).resolve()
    manifest = json.loads(dll_path.with_suffix(".json").read_text(encoding="utf-8"))
    if manifest["test_build"] != target.test or manifest["abi_version"] != ABI or hashlib.sha256(dll_path.read_bytes()).hexdigest() != manifest["sha256"]:
        raise ControlError("controller DLL manifest or identity mismatch")
    api = WinAPI()
    with Channel(target, create=True) as channel, channel.locked(), api.process(target, write=True) as process:
        existing = module_base(target.pid, dll_path)
        already_loaded = existing is not None
        already_initialized = bool(channel.block.initialized)
        if existing is None:
            # LoadLibraryW 可能转发到 KernelBase；必须以实际所属模块的目标基址计算地址。
            local_address = ct.cast(api.kernel.LoadLibraryW, ct.c_void_p).value
            local_module = ct.c_void_p()
            api.require(api.kernel.GetModuleHandleExW(6, local_address, ct.byref(local_module)), "GetModuleHandleExW")
            name = ct.create_unicode_buffer(32768)
            api.require(api.kernel.GetModuleFileNameW(local_module, name, len(name)), "GetModuleFileNameW")
            target_base = module_base(target.pid, name.value)
            if target_base is None:
                raise ControlError("target loader module is missing")
            remote_address = target_base + local_address - local_module.value
            remote_call(api, process, remote_address, (str(dll_path) + "\0").encode("utf-16le"))
            existing = module_base(target.pid, dll_path)
            if existing is None:
                raise ControlError("remote DLL load did not produce the expected module")
        verify_loaded_code(api, process, existing, dll_path)
        local_dll = ct.WinDLL(str(dll_path), use_last_error=True)
        initialize = ct.cast(local_dll.QmtCtl_InitializeRemote, ct.c_void_p).value
        args = InitArgs(ct.sizeof(InitArgs), ABI, target.pid, target.model, target.account, target.creation_time, gui_hwnd_hint)
        encoded = (target.mapping_name + "\0").encode("utf-16le")
        if len(encoded) > ct.sizeof(args.mapping_name):
            raise ControlError("mapping name is too long")
        ct.memmove(ct.addressof(args) + InitArgs.mapping_name.offset, encoded, len(encoded))
        result = remote_call(api, process, existing + initialize - local_dll._handle, bytes(args))
        if result or not channel.block.initialized:
            raise ControlError("native initialization failed: %s; %s" % (STATUS.get(result, result), channel.snapshot()))
        return {"injected": True, "already_loaded": already_loaded, "already_initialized": already_initialized,
                "dll_sha256": manifest["sha256"], "control": channel.snapshot()}


def restart_bridge(target, timeout=15):
    """热重启已核对的唯一桥接模型，timeout 为各控制请求及桥接就绪等待的秒数。"""
    if target.strategy != "QMT_TRADE_BACKEND":
        raise ControlError("bridge-ready verification requires QMT_TRADE_BACKEND")
    directory = Path(target.root) / "userdata/log"
    offsets = {path.name: path.stat().st_size for path in directory.glob("XtClient_FormulaOutput_*.log")}
    result = {"pid": target.pid, "account": target.account, "model": target.model}
    stop_attempted = False
    with Channel(target) as channel:
        result["before"] = channel.command(1, timeout)
        if result["before"]["strategy_count"] != 1:
            raise ControlError("restart verification requires exactly one registered model")
        try:
            if result["before"]["running_count"]:
                stop_attempted = True
                result["stop"] = channel.command(3, timeout)
                if result["stop"]["running_count"] != 0:
                    raise ControlError("native stop did not reach stopped state")
                time.sleep(1)
        finally:
            if stop_attempted:
                end = time.monotonic() + timeout
                while channel.block.request_seq != channel.block.completed_seq and time.monotonic() < end:
                    time.sleep(0.05)
                if channel.block.request_seq != channel.block.completed_seq:
                    raise ControlError("native operation still pending; inspect before issuing another command")
                result["start"] = channel.command(2, timeout)
        if not stop_attempted:
            result["start"] = channel.command(2, timeout)
        end = time.monotonic() + timeout
        marker = "[QMT_GRPC_BRIDGE] READY account=%d " % target.account
        while time.monotonic() < end:
            result["after"] = channel.command(1, timeout)
            ready = False
            for path in directory.glob("XtClient_FormulaOutput_*.log"):
                with path.open("rb") as stream:
                    stream.seek(offsets.get(path.name, 0))
                    raw = stream.read()
                # READY 标记仅含 ASCII，直接检查字节可避开日志正在写入时的多字节截断。
                if marker.encode("ascii") in raw:
                    ready = True
            if ready and result["after"]["running_count"] == 1:
                result["bridge_ready"] = True
                return result
            time.sleep(0.2)
    raise ControlError("native start returned, but fresh bridge READY was not observed")


def main():
    """提供注入、缓存检查和三个明确模型操作，不把函数返回当作桥接就绪。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inject", "inspect", "status", "start", "stop", "restart"))
    parser.add_argument("--qmt-dir", default="C:/东北证券NET专业版")
    parser.add_argument("--strategy", default="QMT_TRADE_BACKEND")
    parser.add_argument("--account", default="19314672")
    parser.add_argument("--dll", default=str(Path(__file__).with_name("QmtNativeStrategyControl.dll")))
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    try:
        target = identify_target(args.qmt_dir, args.strategy, args.account)
        if args.command == "inject":
            result = inject(target, args.dll)
        else:
            if args.command in ("start", "stop", "restart"):
                # 明确的生命周期操作自行建立控制入口，QMT 换进程后也无需手工运行加载策略。
                inject(target, args.dll)
            if args.command == "restart":
                result = restart_bridge(target, args.timeout)
                print(json.dumps(result, ensure_ascii=True))
                return
            with Channel(target) as channel:
                if args.command == "inspect":
                    with channel.locked():
                        result = channel.snapshot()
                else:
                    result = channel.command({"status": 1, "start": 2, "stop": 3}[args.command], args.timeout)
        print(json.dumps(result, ensure_ascii=True))
    except (ControlError, OSError) as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=True))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
