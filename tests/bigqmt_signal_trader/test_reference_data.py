"""ETF 参考分类的有效期、完整性和日期占位值回归。"""

import datetime
import json

import pytest

from bigqmt_signal_trader.reference_data import (
    load_reference, normalize_instrument_detail, reference_digest, validate_reference,
)


def document(now=None):
    """构造五类均有证据且周期无冲突的参考快照。"""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    result = {
        "schema_version": 1, "source": "native-xtquant",
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "sectors": {
            "ETF跨境型": ["513100.SH"], "ETF债券型": ["511010.SH"],
            "ETF商品型": ["518880.SH"], "ETF货币型": ["511880.SH"],
            "ETF股票型": ["510300.SH", "159915.SZ"],
        },
    }
    result["sha256"] = reference_digest(result)
    return result


def test_reference_rejects_stale_empty_conflicting_and_tampered_data():
    """错误参考不能让 ETF 获得貌似可用的结算规则。"""
    now = datetime.datetime.now(datetime.timezone.utc)
    assert validate_reference(document(now))["510300.SH"] == "T1"
    with pytest.raises(ValueError, match="current Shanghai day"):
        validate_reference(document(now - datetime.timedelta(days=1)))
    for kind in ("empty", "conflict", "digest"):
        value = document()
        if kind == "empty":
            value["sectors"]["ETF股票型"] = []
        elif kind == "conflict":
            value["sectors"]["ETF股票型"].append("513100.SH")
        if kind != "digest":
            value["sha256"] = reference_digest(value)
        else:
            value["sha256"] = "0" * 64
        with pytest.raises(ValueError):
            validate_reference(value)


def test_file_replacement_and_rollover_are_visible_without_restart(tmp_path):
    """文件内容更新立即生效；内存缓存不能跨上海日期续用。"""
    now = datetime.datetime(2026, 9, 9, 12, tzinfo=datetime.timezone.utc)
    path = tmp_path / "etf.json"
    value = document(now)
    path.write_text(json.dumps(value), encoding="utf-8")
    assert load_reference(str(path), now)[1]["510300.SH"] == "T1"
    value["sectors"]["ETF股票型"].append("159919.SZ")
    value["sha256"] = reference_digest(value)
    path.write_text(json.dumps(value), encoding="utf-8")
    assert load_reference(str(path), now)[1]["159919.SZ"] == "T1"
    with pytest.raises(ValueError):
        load_reference(str(path), now + datetime.timedelta(days=1))


def test_details_keep_evidence_and_fail_closed_when_file_disappears(tmp_path, monkeypatch):
    """分类只补规则，当前状态保持原值；证据丢失立即变为不可识别。"""
    path = tmp_path / "etf.json"
    path.write_text(json.dumps(document()), encoding="utf-8")
    monkeypatch.setenv("BIGQMT_ETF_REFERENCE_FILE", str(path))
    raw = {"OpenDate": "19700101", "InstrumentStatus": 0, "PriceTick": .001}
    result = normalize_instrument_detail("510300.SH", raw)
    assert result["SettlementCycle"] == "T1"
    assert result["SettlementSource"].startswith("native-xtquant:")
    assert result["OpenDate"] is None
    assert result["RawOpenDate"] == "19700101"
    assert result["InstrumentStatus"] == 0
    assert "SettlementCycle" not in raw
    path.unlink()
    assert normalize_instrument_detail("510300.SH", raw)["SettlementCycle"] == "UNAVAILABLE"
    assert normalize_instrument_detail("510300.SH", {}) == {}


@pytest.mark.parametrize("value", [0, "19700101", "20260230", "", None])
def test_invalid_open_dates_are_not_exposed_as_real_listing_dates(value, monkeypatch):
    """非法日期保留原值和质量标记，正常日期保持不变。"""
    monkeypatch.delenv("BIGQMT_ETF_REFERENCE_FILE", raising=False)
    result = normalize_instrument_detail("301689.SZ", {"OpenDate": value})
    assert result["OpenDate"] is None
    assert result["RawOpenDate"] == value
    assert normalize_instrument_detail("600000.SH", {"OpenDate": "19991110"}) == {
        "OpenDate": "19991110"
    }
