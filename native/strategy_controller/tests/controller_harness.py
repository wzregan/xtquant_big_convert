"""创建自有 Windows 测试窗口；不加载券商程序，不发送任何交易请求。"""

import ctypes as ct
import json
import os
from pathlib import Path
import sys
import time


def main():
    """公开自己的窗口句柄并处理消息，测试 DLL 的钩子只安装到此进程。"""
    user = ct.WinDLL("user32", use_last_error=True)
    kernel = ct.WinDLL("kernel32", use_last_error=True)
    callback_type = ct.WINFUNCTYPE(ct.c_ssize_t, ct.c_void_p, ct.c_uint32, ct.c_size_t, ct.c_ssize_t)
    user.DefWindowProcW.argtypes = [ct.c_void_p, ct.c_uint32, ct.c_size_t, ct.c_ssize_t]
    user.DefWindowProcW.restype = ct.c_ssize_t
    user.DestroyWindow.argtypes = [ct.c_void_p]
    user.PostQuitMessage.argtypes = [ct.c_int]

    @callback_type
    def window_proc(window, message, wparam, lparam):
        """专用消息用于阻塞消息泵，验证已过期请求不会迟到后执行。"""
        if message == 0x8100:
            time.sleep(wparam / 1000)
            return 0
        if message == 0x10:
            user.DestroyWindow(window)
            return 0
        if message == 2:
            user.PostQuitMessage(0)
            return 0
        return user.DefWindowProcW(window, message, wparam, lparam)

    class WindowClass(ct.Structure):
        """Windows x64 的 WNDCLASSEXW 布局。"""
        _fields_ = [
            ("size", ct.c_uint32), ("style", ct.c_uint32), ("callback", callback_type),
            ("class_extra", ct.c_int), ("window_extra", ct.c_int),
            ("instance", ct.c_void_p), ("icon", ct.c_void_p), ("cursor", ct.c_void_p),
            ("background", ct.c_void_p), ("menu_name", ct.c_wchar_p), ("class_name", ct.c_wchar_p),
            ("small_icon", ct.c_void_p),
        ]

    class Message(ct.Structure):
        """消息循环使用的固定宽度 MSG 字段。"""
        _fields_ = [("window", ct.c_void_p), ("message", ct.c_uint32), ("wparam", ct.c_size_t),
                    ("lparam", ct.c_ssize_t), ("time", ct.c_uint32), ("x", ct.c_int), ("y", ct.c_int)]

    kernel.GetModuleHandleW.argtypes = [ct.c_wchar_p]
    kernel.GetModuleHandleW.restype = ct.c_void_p
    instance = kernel.GetModuleHandleW(None)
    klass = WindowClass()
    klass.size = ct.sizeof(klass)
    klass.callback = window_proc
    klass.instance = instance
    klass.class_name = "QmtCtlHarnessWindow"
    user.RegisterClassExW.argtypes = [ct.POINTER(WindowClass)]
    user.RegisterClassExW.restype = ct.c_uint16
    if not user.RegisterClassExW(ct.byref(klass)):
        raise ct.WinError(ct.get_last_error())
    user.CreateWindowExW.argtypes = [ct.c_uint32, ct.c_wchar_p, ct.c_wchar_p, ct.c_uint32,
                                   ct.c_int, ct.c_int, ct.c_int, ct.c_int,
                                   ct.c_void_p, ct.c_void_p, ct.c_void_p, ct.c_void_p]
    user.CreateWindowExW.restype = ct.c_void_p
    window = user.CreateWindowExW(0, klass.class_name, "Native controller owned test window", 0,
                                 0, 0, 1000, 600, None, None, instance, None)
    if not window:
        raise ct.WinError(ct.get_last_error())
    Path(sys.argv[1]).write_text(json.dumps({"hwnd": window, "exe": sys.executable, "pid": os.getpid()}), encoding="utf-8")
    message = Message()
    user.GetMessageW.argtypes = [ct.POINTER(Message), ct.c_void_p, ct.c_uint32, ct.c_uint32]
    user.GetMessageW.restype = ct.c_int
    user.TranslateMessage.argtypes = [ct.POINTER(Message)]
    user.DispatchMessageW.argtypes = [ct.POINTER(Message)]
    user.DispatchMessageW.restype = ct.c_ssize_t
    while True:
        result = user.GetMessageW(ct.byref(message), None, 0, 0)
        if result == -1:
            raise ct.WinError(ct.get_last_error())
        if result == 0:
            break
        user.TranslateMessage(ct.byref(message))
        user.DispatchMessageW(ct.byref(message))


if __name__ == "__main__":
    main()
