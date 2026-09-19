"""读取显式配置的当日 ETF 分类证据，并规范化无效合约日期。"""

import datetime
import hashlib
import json
import os
import re
import threading


ETF_SECTORS = {
    "ETF跨境型": "T0", "ETF债券型": "T0", "ETF商品型": "T0",
    "ETF货币型": "T0", "ETF股票型": "T1",
}
SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))
_lock = threading.Lock()
_cache = {}


def reference_digest(document):
    """计算除摘要字段之外的完整证据摘要，用于校验文件及规则版本追溯。"""
    body = dict(document)
    body.pop("sha256", None)
    return hashlib.sha256(json.dumps(
        body, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()


def validate_reference(document, now=None):
    """校验当日、来源、五类完整性及 T0/T1 冲突；now 必须为带时区时间。"""
    now = now or datetime.datetime.now(SHANGHAI)
    if not isinstance(document, dict):
        raise ValueError("ETF reference must be an object")
    if document.get("schema_version") != 1 or document.get("source") != "native-xtquant":
        raise ValueError("ETF reference schema/source is invalid")
    if document.get("sha256") != reference_digest(document):
        raise ValueError("ETF reference digest mismatch")
    _validate_reference_time(document, now)
    sectors = document.get("sectors", {})
    if set(sectors) != set(ETF_SECTORS):
        raise ValueError("ETF reference sector set is incomplete")
    cycles = {}
    for sector, cycle in ETF_SECTORS.items():
        codes = sectors[sector]
        if not isinstance(codes, list) or not codes:
            raise ValueError("ETF reference contains an empty sector")
        for code in codes:
            if not isinstance(code, str) or not re.match(r"^[0-9]{6}\.(SH|SZ)$", code):
                raise ValueError("ETF reference contains an invalid code")
            if code in cycles and cycles[code] != cycle:
                raise ValueError("ETF reference has conflicting settlement cycles")
            cycles[code] = cycle
    return cycles



def _validate_reference_time(document, now=None):
    """每次复用文件缓存都检查有效期，不重复遍历已通过校验的分类。"""
    now = now or datetime.datetime.now(SHANGHAI)
    # 采用固定格式，兼容大 QMT 的 Python 3.6，不依赖 fromisoformat。
    generated = datetime.datetime.strptime(
        document["generated_at"], "%Y-%m-%dT%H:%M:%S%z"
    )
    if generated.astimezone(SHANGHAI).date() != now.astimezone(SHANGHAI).date():
        raise ValueError("ETF reference is not from the current Shanghai day")
    if (generated - now).total_seconds() > 60:
        raise ValueError("ETF reference timestamp is in the future")


def load_reference(path=None, now=None):
    """读取配置路径；文件变更后立即重读，每次使用均检查上海日期。"""
    path = path or os.environ.get("BIGQMT_ETF_REFERENCE_FILE")
    if not path:
        return None
    stat = os.stat(path)
    key = (os.path.abspath(path), stat.st_mtime_ns, stat.st_size)
    with _lock:
        if _cache.get("key") != key:
            with open(path, "r", encoding="utf-8") as handle:
                document = json.load(handle)
            cycles = validate_reference(document, now)
            _cache.update(key=key, document=document, cycles=cycles)
        else:
            # 缓存只减少文件读取，不能延长当日证据有效期。
            _validate_reference_time(_cache["document"], now)
        return _cache["document"], _cache["cycles"]


def normalize_instrument_detail(code, raw):
    """保留原始无效日期；可选分类证据只补充结算字段，不覆盖行情或交易状态。"""
    if not isinstance(raw, dict) or not raw:
        return raw
    result = dict(raw)
    if "OpenDate" in result:
        value = result["OpenDate"]
        try:
            valid = datetime.datetime.strptime(str(value), "%Y%m%d").year > 1970
        except (ValueError, TypeError):
            valid = False
        if not valid:
            result.setdefault("RawOpenDate", value)
            result["OpenDate"] = None
            result["DataQuality"] = list(dict.fromkeys(list(result.get("DataQuality") or []) + ["INVALID_OPEN_DATE"]))
    path = os.environ.get("BIGQMT_ETF_REFERENCE_FILE")
    if not path:
        return result
    # 配置参考源后，ETF 的结算判定必须来自有效证据，不能退回先前缓存。
    code = str(code).strip().upper()
    is_etf = (
        code.endswith((".SH", ".SZ"))
        and (code.startswith(("15", "16", "50", "51", "52", "56", "58"))
             or "ETF" in str(raw.get("ProductID", "")).upper())
    )
    if not is_etf:
        return result
    try:
        reference, cycles = load_reference(path)
        cycle = cycles.get(code)
        if cycle is None:
            raise ValueError("ETF is absent from classification reference")
        existing = [result.get(key) for key in (
            "SettlementCycle", "settlement_cycle", "TradeCycle", "trade_cycle"
        ) if result.get(key) not in (None, "")]
        if existing:
            # 原始显式字段保留给上层验证，不以参考源掩盖供应商错误。
            return result
        result["SettlementCycle"] = cycle
        result["SettlementSource"] = reference["source"] + ":" + reference["sha256"]
        result["SettlementAsOf"] = reference["generated_at"]
    except (OSError, ValueError, KeyError, TypeError):
        result["SettlementCycle"] = "UNAVAILABLE"
        result["DataQuality"] = list(result.get("DataQuality") or []) + ["ETF_REFERENCE_UNAVAILABLE"]
    return result
