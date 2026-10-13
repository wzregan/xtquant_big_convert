"""受 BIGQMT_DEBUG 开关控制的有界诊断；仅写面板，不打开文件或网络。"""

import contextlib
import hashlib
import itertools
import json
import os
import threading
import time

_LOCAL = threading.local()
_SECRET_KEYS = frozenset(("token", "authorization", "password", "secret", "private_key"))


def enabled():
    """仅在显式启用 BIGQMT_DEBUG 时收集诊断，默认关闭。"""
    return os.environ.get("BIGQMT_DEBUG", "").strip().lower() in ("1", "true", "yes", "on")


def _safe(value, key="", depth=0):
    """限制深度、长度和数量；账户只留摘要，不展开原生对象或输出密钥。"""
    key = str(key).lower()
    if key in _SECRET_KEYS or any(key.endswith("_" + name) for name in _SECRET_KEYS):
        return "[REDACTED]"
    if key in ("account_id", "subaccount_id"):
        return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:160]
    if depth >= 3:
        return "[TRUNCATED]"
    if isinstance(value, dict):
        return {str(k): _safe(v, k, depth + 1)
                for k, v in itertools.islice(value.items(), 32)}
    if isinstance(value, (list, tuple)):
        return [_safe(v, depth=depth + 1) for v in value[:8]]
    return "<%s>" % type(value).__name__


def trace(event, **fields):
    """输出白名单调用点提供的字段；昂贵字段应由调用方先检查 enabled。"""
    if not enabled():
        return
    try:
        payload = dict(getattr(_LOCAL, "context", {}) or {})
        payload.update(fields)
        payload.update(event=event, timestamp=time.time(), thread_id=threading.get_ident())
        print("[bigqmt_debug] " + json.dumps(_safe(payload), ensure_ascii=True, sort_keys=True))
    except Exception:
        # 面板关闭、对象格式异常等诊断故障不能中断交易或掩盖原异常。
        pass


@contextlib.contextmanager
def request_scope(request_id, method):
    """为同线程原生调用关联 RPC 标识；离开后恢复上下文，防止串到下一请求。"""
    previous = getattr(_LOCAL, "context", None)
    _LOCAL.context = {"request_id": request_id, "method": method}
    try:
        yield
    finally:
        _LOCAL.context = previous


def native_fields(row, names):
    """只读取指定原生字段，缺失记为 null；禁止扫描整个柜台对象。"""
    result = {}
    for name in names:
        try:
            result[name] = getattr(row, name, None)
        except Exception:
            result[name] = "[UNREADABLE]"
    return result
