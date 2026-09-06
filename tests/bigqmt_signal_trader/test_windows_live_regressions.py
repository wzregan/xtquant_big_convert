"""覆盖 Windows QMT 实测的旧 pandas、返回结构、历史占位及接口签名缺陷。"""

import os
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
from bigqmt_signal_trader.dataframe_wire import _encode_axis
from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
from bigqmt_signal_trader.xtquant_compat import BigQmtXtData


@pytest.mark.parametrize('bounds', [(0, 0, 1), (2, 8, 2), (8, 2, -2)])
def test_pandas_022_range_index_keeps_bounds(bounds):
    """旧版 RangeIndex 只暴露私有边界，空索引和非默认步长也必须保留。"""
    axis = pd.RangeIndex(*bounds, name='row')
    legacy = type('RangeIndex', (), {})()
    legacy._start, legacy._stop, legacy._step = bounds
    legacy.names, legacy.nlevels = axis.names, axis.nlevels
    legacy.tolist, legacy.get_level_values = axis.tolist, axis.get_level_values
    encoded = _encode_axis(legacy, lambda value: value)
    assert encoded['range'] == list(bounds)
    assert encoded['values'] == list(axis)


@pytest.mark.parametrize('empty', [False, True])
@pytest.mark.parametrize('source', ['native', 'context'])
def test_range_divid_preserves_dataframe(empty, source):
    """原生区间帧不能做布尔判断，空帧也不能退回日线猜测。"""
    frame = pd.DataFrame({'factor': [] if empty else [1.02]}, index=[] if empty else ['20260612'])
    native = SimpleNamespace(get_divid_factors=Mock(return_value=frame)) if source == 'native' else None
    context = SimpleNamespace(get_divid_factors=Mock(return_value=frame))
    provider = BigQmtMarketDataProvider(context)
    provider._native = lambda: native
    provider._expand_divid_factors = Mock(side_effect=AssertionError('不应展开区间'))
    assert provider.get_divid_factors('000001.SZ', '20260101', '20260904') is frame


def test_single_day_divid_frames_are_merged():
    """只支持单日的终端可混合返回空字典和帧，因子索引和列名不能丢失。"""
    frames = {'20260612': pd.DataFrame({'factor': [1.02]}, index=['20260612']),
              '20260613': {},
              '20260614': pd.DataFrame({'factor': [1.03]}, index=['20260614'])}
    context = SimpleNamespace(get_divid_factors=lambda code, day: frames[day])
    provider = BigQmtMarketDataProvider(context)
    def candidates(*args):
        """模拟已完成的日线候选扫描，隔离要验证的因子合并。"""
        provider._divid_scan_rows = 3
        return list(frames)
    provider._divid_candidate_days = candidates
    result = provider._expand_divid_factors('000001.SZ', '20260612', '20260614')
    pd.testing.assert_frame_equal(result, pd.DataFrame({'factor': [1.02, 1.03]}, index=['20260612', '20260614']))


def test_credit_probe_matches_real_compact_signature():
    """能力探测与正式合约查询一致传入账户和账户类型，不能误报终端错误。"""
    query = Mock(return_value=[])
    handlers = BigQmtRpcHandlers('acct', SimpleNamespace(), SimpleNamespace(),
                                order_gateway=SimpleNamespace(account_type='stock'),
                                qmt_api={'get_unclosed_compacts': query})
    result = handlers._handle_probe_capabilities({})
    query.assert_called_once_with('acct', 'STOCK')
    assert result['credit_probe']['get_unclosed_compacts'] == {'available': True, 'ok': True, 'rows': 0}


def _client(data):
    """提供禁用本地缓存的只读客户端桩，用调用次数验证没有误触发补数。"""
    return SimpleNamespace(call=Mock(return_value=data), local_cache_config={'enabled': False})


@pytest.mark.parametrize('shape', ['single', 'code', 'field'])
def test_market_data_shapes_do_not_download_or_wait(shape):
    """单股帧及两种分组结构都返回字段帧，正常查询只需一次 RPC。"""
    frame = pd.DataFrame({'close': [10., 11.], 'volume': [0., 5.]}, index=['20260903', '20260904'])
    raw = {'single': frame, 'code': {'000001.SZ': frame},
           'field': {field: pd.DataFrame([frame[field].values], index=['000001.SZ'], columns=frame.index)
                     for field in frame.columns}}[shape]
    client = _client(raw)
    with patch('bigqmt_signal_trader.xtquant_compat.time.sleep') as sleep:
        result = BigQmtXtData(client).get_market_data(['close', 'volume'], ['000001.SZ'], count=2)
    assert client.call.call_count == 1
    sleep.assert_not_called()
    assert list(result) == ['close', 'volume']
    assert list(result['close'].index) == ['000001.SZ']
    assert list(result['close'].columns) == list(frame.index)
    assert result['close'].iloc[0].tolist() == [10., 11.]
    assert result['volume'].iloc[0].tolist() == [0., 5.]


@pytest.mark.parametrize('method', ['get_market_data_ex', 'get_local_data'])
def test_code_frames_drop_only_leading_missing_prices(method):
    """零价占位不能充作历史，零成交量、负价和有效历史之后的行仍保持原样。"""
    frame = pd.DataFrame({'stime': ['2026090%d' % i for i in range(1, 7)],
                          'close': [0., float('nan'), 10., 0., -1., 11.], 'volume': [0.] * 6})
    client = _client(frame)
    result = getattr(BigQmtXtData(client), method)(['close', 'volume'], ['000001.SZ'], count=6)
    assert client.call.call_count == 1
    assert list(result) == ['000001.SZ']
    assert list(result['000001.SZ'].index) == ['20260903', '20260904', '20260905', '20260906']
    assert result['000001.SZ']['close'].tolist() == [10., 0., -1., 11.]
    assert len(frame) == 6


def test_zero_prefix_never_enters_cache(tmp_path):
    """过滤发生在写缓存之前，后续离线读不能再次拿到占位历史。"""
    client = _client({'000001.SZ': pd.DataFrame({'stime': ['20260903', '20260904'], 'close': [0., 11.]})})
    client.local_cache_config = {'enabled': True, 'dir': str(tmp_path), 'format': 'pkl'}
    data = BigQmtXtData(client)
    data.get_market_data_ex(['close'], ['000001.SZ'], count=2)
    result = data.get_local_data(['close'], ['000001.SZ'], count=2)
    assert result['000001.SZ']['close'].tolist() == [11.]
    assert client.call.call_count == 1


def test_all_missing_history_returns_empty_frame():
    """全零价没有有效历史，返回有字段的空帧而非假装满足 count。"""
    client = _client({'000001.SZ': pd.DataFrame({'close': [0., 0.]})})
    result = BigQmtXtData(client).get_market_data_ex(['close'], ['000001.SZ'], count=2)
    assert result['000001.SZ'].empty
    assert list(result['000001.SZ'].columns) == ['close']


def test_all_null_financial_data_is_not_success():
    """有日期的全空财务帧必须报告缺少数据，而不是通过非空结果验收。"""
    frame = pd.DataFrame({'total_capital': [float('nan')]}, index=['20260904'])
    provider = BigQmtMarketDataProvider(SimpleNamespace(get_financial_data=lambda *args: frame))
    with pytest.raises(RuntimeError, match='only missing values'):
        provider.get_financial_data(['000001.SZ'], ['Capital'])
    frame.iloc[0, 0] = 1.0
    assert provider.get_financial_data(['000001.SZ'], ['Capital']) is frame


def test_broken_vendor_etf_binding_reports_unavailable():
    """无法提供原生 ETF 语义时明确报告能力缺失，不返回错误含义的板块列表。"""
    context = SimpleNamespace(get_ETF_list=Mock(side_effect=NameError("name 'get_etf_list' is not defined")))
    provider = BigQmtMarketDataProvider(context)
    with pytest.raises(NotImplementedError, match='missing the get_etf_list binding'):
        provider.get_ETF_list('SH', '510300.SH')
