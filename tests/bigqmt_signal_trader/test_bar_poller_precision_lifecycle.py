"""K 线轮询保持大整数签名精度，并明确拥有尚未退出的原生查询线程。"""

import threading

import pandas as pd
import pytest

from bigqmt_signal_trader.xtquant_compat import BigQmtXtData, _BarPoller


def test_bar_signature_keeps_large_integer_changes():
    """价格不变时数量从 2**53 增加一份仍必须触发新行情。"""
    before = pd.DataFrame(
        {"close": [4.101], "volume": [2**53]}, index=["20261016130700"]
    )
    after = before.copy()
    after.loc["20261016130700", "volume"] = 2**53 + 1
    assert _BarPoller._signature({"510300.SH": before}) != _BarPoller._signature(
        {"510300.SH": after}
    )


def test_sdk_bar_callback_keeps_large_integer_volume_and_index_time():
    """真实 DataFrame 经正式 SDK 回调转换仍保持大整数和微小价格的原值。"""
    from xtquant.xtdata import _sdk_quote_callback

    delivered = []
    frame = pd.DataFrame(
        {"close": [4.101001], "volume": [2**53 + 1]}, index=["20261016130700"]
    )
    _sdk_quote_callback(delivered.append)({"510300.SH": frame})
    row = delivered[0]["510300.SH"][0]
    assert row["volume"] == 2**53 + 1 and isinstance(row["volume"], int)
    assert row["close"].hex() == (4.101001).hex()
    assert row["time"] == 1792127220000


@pytest.mark.parametrize("stop_all", [False, True])
def test_inflight_bar_query_retains_owner_until_joined(stop_all):
    """停止超时后仍保留原订阅句柄；查询结束后可再次清理且不交付迟到结果。"""
    entered, release = threading.Event(), threading.Event()
    callbacks = []

    def fetch():
        """控制原生查询返回边界，不替换消费者停止逻辑。"""
        entered.set()
        release.wait(5)
        return {"510300.SH": [{"close": 4.101}]}

    poller = _BarPoller(fetch, callbacks.append, 30).start()
    data = BigQmtXtData.__new__(BigQmtXtData)
    data._bar_poller_lock = threading.Lock()
    data._bar_pollers = {71: poller}
    data._record_subscription = lambda *args, **kwargs: None
    action = (
        data.stop_all_subscriptions if stop_all else lambda: data.unsubscribe_quote(71)
    )
    try:
        assert entered.wait(2)
        with pytest.raises(RuntimeError, match="did not stop"):
            action()
        assert data._bar_pollers[71] is poller and poller._thread.is_alive()
    finally:
        release.set()
        poller._thread.join(2)
        action()
    assert not poller._thread.is_alive() and not data._bar_pollers and not callbacks
