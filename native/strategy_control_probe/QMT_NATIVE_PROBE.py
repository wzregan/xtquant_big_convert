#coding:utf-8
"""在大 QMT 内加载自有 DLL，验证原生管理器；不启动、停止策略或发送委托。"""

import ctypes
import json


DLL_PATH = "C:/Users/root/code/qmt-native-controller-20261014/QmtNativeStrategyProbe.dll"
PROBE_VERSION = "QMT_NATIVE_PROBE_V2"
_DLL = None
_REPORT = None
_HANDLEBAR_LOGGED = False


class ProbeReport(ctypes.Structure):
    """与 DLL 固定的 64 字节只读报告逐字段对齐。"""
    _fields_ = [
        ("struct_bytes", ctypes.c_uint32), ("abi_version", ctypes.c_uint32),
        ("status", ctypes.c_uint32), ("pid", ctypes.c_uint32),
        ("tid", ctypes.c_uint32), ("bindings", ctypes.c_uint32),
        ("strategy_count", ctypes.c_int32), ("running_count", ctypes.c_int32),
        ("formula_base", ctypes.c_uint64), ("manager", ctypes.c_uint64),
        ("manager_thread", ctypes.c_uint64), ("caller_thread", ctypes.c_uint64),
    ]


def probe():
    """显式加载 DLL 并调用只读探针；保留模块引用至策略结束，打印版本和诊断状态。"""
    global _DLL, _REPORT
    stage = "LOAD_DLL"
    print("[QMT_NATIVE_PROBE] " + PROBE_VERSION + " " + stage + " " + DLL_PATH)
    try:
        if ctypes.sizeof(ProbeReport) != 64:
            raise RuntimeError("native probe structure mismatch")
        _DLL = ctypes.WinDLL(DLL_PATH)
        _DLL.QmtCtl_Probe.argtypes = [ctypes.POINTER(ProbeReport), ctypes.c_uint32]
        _DLL.QmtCtl_Probe.restype = ctypes.c_uint32
        stage = "CALL_NATIVE_PROBE"
        print("[QMT_NATIVE_PROBE] " + PROBE_VERSION + " " + stage)
        report = ProbeReport()
        status = int(_DLL.QmtCtl_Probe(ctypes.byref(report), ctypes.sizeof(report)))
    except Exception as error:
        # 先输出失败阶段，即使宿主未保留 Python traceback，也能区分加载与调用失败。
        print("[QMT_NATIVE_PROBE] " + json.dumps({
            "version": PROBE_VERSION, "stage": stage,
            "error_type": type(error).__name__, "error": str(error),
        }, ensure_ascii=True))
        raise
    _REPORT = {field: getattr(report, field) for field, _ in ProbeReport._fields_}
    _REPORT["version"] = PROBE_VERSION
    _REPORT["status_name"] = {
        0: "OK", 1: "WRONG_HOST", 2: "FORMULA_NOT_LOADED",
        3: "UNSUPPORTED_EXPORT_PROFILE", 4: "MANAGER_NOT_INITIALIZED",
        5: "NATIVE_EXCEPTION", 6: "INVALID_REPORT",
    }.get(status, "UNKNOWN")
    # 任一线程指针缺失时应报告未知，不能把查询失败解释为跨线程调用。
    _REPORT["same_qt_thread"] = (
        bool(report.bindings & 4) if report.manager_thread and report.caller_thread else None
    )
    _REPORT["probe_only"] = True
    print("[QMT_NATIVE_PROBE] " + json.dumps(_REPORT, ensure_ascii=True))
    return _REPORT


def init(ContextInfo):
    """记录策略初始化；复用脚本加载时的只读报告，不修改 ContextInfo 的交易状态。"""
    print("[QMT_NATIVE_PROBE] " + PROBE_VERSION + " INIT_OK")
    if _REPORT is None:
        probe()


def handlebar(ContextInfo):
    """仅记录首次策略回调，用于区分脚本执行和已启动的策略实例。"""
    global _HANDLEBAR_LOGGED
    if not _HANDLEBAR_LOGGED:
        _HANDLEBAR_LOGGED = True
        print("[QMT_NATIVE_PROBE] " + PROBE_VERSION + " HANDLEBAR_OK")


def stop(ContextInfo):
    """打印探针策略结束标记；本模块没有后台线程、钩子或待执行控制命令。"""
    print("[QMT_NATIVE_PROBE] stopped")


# 模型研究或普通脚本执行不一定调用 init；顶层入口保证两种运行方式都能输出诊断。
# 原生入口会拒绝外部 Python 宿主，只有 XtItClient 内才能访问已有的策略管理器。
print("[QMT_NATIVE_PROBE] " + PROBE_VERSION + " SCRIPT_LOADED")
probe()
