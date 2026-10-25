#coding:utf-8
"""追加到现有桥接策略末尾，在原始初始化完成后执行一次只读原生探针。"""

import base64 as _native_probe_base64
import json as _native_probe_json


_NATIVE_PROBE_SOURCE_B64 = "__NATIVE_PROBE_SOURCE_B64__"
_NATIVE_PROBE_NAMESPACE = None
_NATIVE_PROBE_BRIDGE_INIT = globals()["init"]


def _run_native_probe_once():
    """为探针保留独立全局空间与 DLL 引用，异常只记录日志，不改变桥接状态。"""
    global _NATIVE_PROBE_NAMESPACE
    if _NATIVE_PROBE_NAMESPACE is not None:
        return _NATIVE_PROBE_NAMESPACE.get("_REPORT")
    # 独立空间防止探针的 init、handlebar、stop 覆盖当前桥接的同名回调。
    _NATIVE_PROBE_NAMESPACE = {"__name__": "qmt_native_readonly_probe"}
    print("[QMT_NATIVE_PROBE] ATTACHED_LOADER_BEGIN")
    try:
        source = _native_probe_base64.b64decode(_NATIVE_PROBE_SOURCE_B64)
        exec(compile(source, "<qmt_native_readonly_probe>", "exec"), _NATIVE_PROBE_NAMESPACE)
    except Exception as error:
        print("[QMT_NATIVE_PROBE] " + _native_probe_json.dumps({
            "stage": "ATTACHED_LOADER", "error_type": type(error).__name__, "error": str(error),
        }, ensure_ascii=True))
    return _NATIVE_PROBE_NAMESPACE.get("_REPORT")


def init(ContextInfo):
    """先完成桥接初始化，再运行只读探针；ContextInfo 为 QMT 提供的原始上下文。"""
    # 原始初始化异常必须原样传播，不能让探针把失败的桥接报告为已经就绪。
    result = _NATIVE_PROBE_BRIDGE_INIT(ContextInfo)
    _run_native_probe_once()
    return result
