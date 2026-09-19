"""由已有原生 MiniQMT Python 只读导出 ETF 分类，原子替换独立实例的证据文件。"""

import argparse
import datetime
import hashlib
import json
import os
import tempfile


def export_reference(api, destination):
    """读取原生板块分类；destination 是独立 BigQMT 实例的本地 JSON 文件。"""
    sectors = {}
    for name in ("ETF跨境型", "ETF债券型", "ETF商品型", "ETF货币型", "ETF股票型"):
        codes = sorted(set(api.get_stock_list_in_sector(name)))
        if not codes:
            raise ValueError("empty native ETF sector: " + name)
        sectors[name] = codes
    t0 = set().union(*(set(codes) for name, codes in sectors.items() if name != "ETF股票型"))
    if t0.intersection(sectors["ETF股票型"]):
        raise ValueError("conflicting native ETF classifications")
    document = {
        "schema_version": 1, "source": "native-xtquant",
        "generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z"),
        "sectors": sectors,
    }
    document["sha256"] = hashlib.sha256(json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()
    parent = os.path.dirname(os.path.abspath(destination))
    os.makedirs(parent, exist_ok=True)
    try:
        with open(destination, "r", encoding="utf-8") as handle:
            previous = json.load(handle)
        generated = datetime.datetime.strptime(previous["generated_at"], "%Y-%m-%dT%H:%M:%S%z")
        shanghai = datetime.timezone(datetime.timedelta(hours=8))
        if (previous.get("sectors") == sectors
                and previous.get("source") == "native-xtquant"
                and generated.astimezone(shanghai).date() == datetime.datetime.now(shanghai).date()):
            previous_digest = previous.pop("sha256", None)
            actual = hashlib.sha256(json.dumps(
                previous, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")).hexdigest()
            if previous_digest == actual:
                # 分类未变化时保留证据版本，避免每五分钟无意义地改变交易规则哈希。
                return {name: len(codes) for name, codes in sectors.items()}
    except (OSError, ValueError, KeyError, TypeError):
        pass
    descriptor, temporary = tempfile.mkstemp(dir=parent, prefix=".etf-reference-", suffix=".json")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(document, handle, ensure_ascii=False, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return {name: len(codes) for name, codes in sectors.items()}


def main():
    """使用未安装兼容 shim 的原生解释器导出，禁止混入 BigQMT 兼容接口。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination")
    args = parser.parse_args()
    from xtquant import xtdata
    if "bigqmt_signal_trader" in str(getattr(xtdata, "__file__", "")) or hasattr(xtdata, "get_deployment_info"):
        raise RuntimeError("export requires native xtquant, not BigQMT compatibility shim")
    print(json.dumps(export_reference(xtdata, args.destination), ensure_ascii=False))


if __name__ == "__main__":
    main()
