"""缓存启用时，历史下载和缓存未命中都不能耗用行情订阅配额。"""

from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from bigqmt_signal_trader.xtquant_compat import BigQmtXtData


@pytest.mark.parametrize("operation", ["download", "cache_miss"])
def test_history_cache_population_never_subscribes(monkeypatch, operation):
    """模拟有限订阅额度，重复历史读取只能走明确的终端本地读取契约。"""
    frame = pd.DataFrame(
        {
            "time": [1793844060000],
            "close": [1.2],
            "open": [1.2],
            "high": [1.2],
            "low": [1.2],
            "volume": [100],
            "amount": [12000],
        }
    )
    cache = SimpleNamespace(read=Mock(return_value=None), write=Mock())
    subscriptions = []

    def call(method, params, **kwargs):
        """仅模拟两项行情操作，未声明只读的缓存回填即视为订阅泄漏。"""
        if method == "download_history_data2":
            return True
        assert method == "get_market_data_ex"
        if params.get("subscribe") is not False:
            subscriptions.extend(params["stock_list"])
        assert kwargs.get("use_formula") is False
        return {code: frame.copy() for code in params["stock_list"]}

    client = SimpleNamespace(
        call=Mock(side_effect=call), local_cache_config={"fallback_rpc": True}
    )
    data = BigQmtXtData(client)
    monkeypatch.setattr(data, "_local_cache", lambda: cache)
    for code in ("159518.SZ", "510300.SH", "510500.SH"):
        if operation == "download":
            assert data.download_history_data(
                code, "1m", "20261105100100", "20261105100400", incrementally=False
            ) == {"finished": 1, "total": 1}
        else:
            assert (
                len(
                    data.get_local_data(
                        ["time", "close"],
                        [code],
                        "1m",
                        "20261105100100",
                        "20261105100400",
                    )[code]
                )
                == 1
            )
    assert subscriptions == []
    assert cache.write.call_count == 3
