"""原生未提供分类接口时，证券代码分类必须包含交易所信息。"""

from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
from bigqmt_signal_trader.code_utils import instrument_code_flags


def _provider(context=None):
    """只构造分类入口需要的上下文，避免测试初始化原生行情模块。"""
    provider = BigQmtMarketDataProvider.__new__(BigQmtMarketDataProvider)
    provider.context_info = context or SimpleNamespace()
    return provider


@pytest.mark.parametrize("code", ["520600.SH", "520870.SH", "530000.SH", "551000.SH"])
def test_hongkong_etf_prefix_is_fund_and_etf(code):
    """真实 ETF 的 52、53、55 号段不能从基金和 ETF 分类中丢失。"""
    result = _provider().get_instrument_type(code)
    assert result["fund"] and result["etf"]
    assert not result["stock"] and not result["index"]


@pytest.mark.parametrize(
    "code,stock,index",
    [
        ("000300.SH", False, True),
        ("000003.SZ", True, False),
        ("000001.SZ", True, False),
        ("399001.SZ", False, True),
        ("600000.SH", True, False),
        ("920001.BJ", True, False),
    ],
)
def test_exchange_disambiguates_stock_and_index(code, stock, index):
    """相同数字前缀在沪深两市含义不同，北交所股票也应被识别。"""
    result = _provider().get_instrument_type(code)
    assert result["stock"] is stock
    assert result["index"] is index


def test_variety_filter_preserves_correct_flags():
    """调用方指定分类键时只返回这些键，未知分类保持 False。"""
    assert _provider().get_instrument_type("520600.SH", ["etf", "stock", "other"]) == {
        "etf": True,
        "stock": False,
        "other": False,
    }


def test_native_classification_remains_authoritative():
    """原生接口存在时仍优先使用其明确结果，不用代码前缀覆盖。"""
    expected = {"etf": False}
    context = SimpleNamespace(get_instrument_type=lambda code, keys: expected)
    assert _provider(context).get_instrument_type("520600.SH") is expected


@pytest.mark.parametrize("code", ["520600.SH", "SH520600", "520600"])
def test_normalized_code_forms_have_identical_classification(code):
    """前缀、后缀和裸代码必须得到一致分类，避免调用入口造成差异。"""
    assert instrument_code_flags(code) == instrument_code_flags("520600.SH")


@pytest.mark.parametrize("code", ["rb2610.SF", "000001.HK", "520600.SZ"])
def test_foreign_or_unsupported_code_is_not_mainland_etf(code):
    """其他市场或不匹配交易所的代码不能借用沪市 ETF 号段。"""
    assert not any(instrument_code_flags(code).values())


@pytest.mark.parametrize("code", ["501018.SH", "160105.SZ"])
def test_lof_is_a_fund_but_not_an_etf(code):
    """基金分类保留 LOF，ETF 分类则不能把两者混同。"""
    result = instrument_code_flags(code)
    assert result["fund"] and not result["etf"]
