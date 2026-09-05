"""逐提交审阅发现的日期、补数阈值及传输生命周期边界。"""

import datetime
import os
import sys
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
from bigqmt_signal_trader.adapters.market_bigqmt import _row_day, _frame_rows
from bigqmt_signal_trader.xtquant_compat import BigQmtXtData
from bigqmt_signal_trader.transports.zmq_transport import ZmqTransport


@pytest.mark.parametrize('raw', ['20260901', 20260901, '20260901150000',
                                 '2026-09-01 15:00:00', '2026-09-01T15:00:00+08:00',
                                 datetime.datetime(2026, 9, 1, 15)])
def test_dividend_days_distinguish_calendar_from_epoch(raw):
    """日历日期与带时分秒的日期必须保持同一天。"""
    assert _row_day({'stime': raw}) == '20260901'


def test_dividend_epoch_ms_and_invalid_date():
    """保留毫秒时间戳兼容，并拒绝无效日期。"""
    stamp = int(datetime.datetime(2026, 9, 1, 15).timestamp()*1000)
    assert _row_day({'time': stamp}) == '20260901'
    assert _row_day({'date': '20261399'}) == ''


@pytest.mark.parametrize('index', [[20260901, 20260902], ['20260901', '20260902']])
def test_factor_scan_reads_rows_by_position_and_preserves_time_index(index):
    """整数日期索引不能被误当行位置，时间轴在转换后仍可识别。"""
    pd = pytest.importorskip('pandas')
    rows = _frame_rows(pd.DataFrame({'close': [10.0, 11.0]}, index=index))
    assert [row['close'] for row in rows] == [10.0, 11.0]
    assert [_row_day(row) for row in rows] == ['20260901', '20260902']


@pytest.mark.parametrize('codes,served,heal', [
    (['a', 'b', 'c'], ['a', 'b'], False),
    (['a', 'b'], ['a'], False),
    (['a', 'b', 'c'], ['a'], True),
    (['a'], [], True),
    (['a', 'b', 'b', 'b'], ['a'], False),
])
def test_raw_history_heals_only_when_unique_symbols_are_mostly_missing(codes, served, heal):
    """少数缺码与恰好一半不触发下载，重复代码不改变缺失比例。"""
    data = BigQmtXtData(client=SimpleNamespace(call=Mock(return_value={'retried': True})))
    data._ensure_server_raw = Mock()
    initial = {code: [] for code in served}
    result = data._heal_adjusted('get_market_data_ex', {'stock_list': codes}, initial, wait_seconds=0)
    assert data._ensure_server_raw.called is heal
    assert data.client.call.called is heal
    if not heal:
        assert result is initial


def test_dealer_recreated_after_stop_in_a_different_thread():
    """工作线程存活但套接字已由 stop 关闭时，后续使用重新创建句柄。"""
    pytest.importorskip('zmq')
    transport = ZmqTransport(connect_address='inproc://offline-review-no-server')
    ready, resume = threading.Event(), threading.Event()
    sockets, errors = [], []

    def worker():
        """分两阶段获取同一线程的句柄，期间没有在途网络请求。"""
        try:
            sockets.append(transport._ensure_dealer())
            ready.set()
            assert resume.wait(3)
            sockets.append(transport._ensure_dealer())
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert ready.wait(3)
        transport.stop()
        resume.set()
        thread.join(3)
        assert not thread.is_alive()
        assert not errors
        assert sockets[0].closed
        assert sockets[1] is not sockets[0]
        assert not sockets[1].closed
    finally:
        resume.set()
        thread.join(3)
        transport.stop()
