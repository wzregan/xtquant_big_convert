"""同备注、同系统号跨账户时，确认结果和执行事件必须相互隔离。"""

import os
import sys
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
import bigqmt_signal_trader_strategy as strategy
from bigqmt_signal_trader import exec_events
from bigqmt_signal_trader.order_watch import OrderWatchTable
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, CancelSettlement, OrderSettlement


@pytest.fixture
def event_sink(monkeypatch):
    """使用真实事件归一化函数和内存发布器，不连接外部传输。"""
    published = []
    config = {"account_id": "primary", "exec_events": {"enabled": True}}
    monkeypatch.setattr(strategy, '_exec_events', exec_events)
    monkeypatch.setattr(strategy, '_build_config', lambda: config)
    monkeypatch.setattr(strategy, '_exec_event_sink', lambda cfg: published)
    monkeypatch.setattr(strategy, '_exec_event_redis', lambda cfg: None)
    monkeypatch.setattr(strategy, '_held_presysid_orders', {})
    monkeypatch.setattr(strategy, '_instrument_name_cache', {})
    monkeypatch.setattr(strategy, '_rpc_service', None)
    monkeypatch.setattr(exec_events, 'publish_exec_event',
                        lambda sink, account_id, event: sink.append((account_id, event)))
    return published, config


def order(account_id, sysid='S1', status=50, remark='same'):
    """构造不同账号可重复使用同一备注及系统号的柜台回调。"""
    return SimpleNamespace(m_strAccountID=account_id, m_strOrderSysID=sysid,
                           m_strRemark=remark, m_nOrderStatus=status,
                           m_strInstrumentID='600000', m_strExchangeID='SH',
                           m_nOffsetFlag=48, m_nVolumeTotalOriginal=100,
                           m_dLimitPrice=10.0)


@pytest.mark.parametrize('kind', ['order', 'trade'])
def test_callback_topic_and_identity_use_actual_account(event_sink, monkeypatch, kind):
    """归一化载荷、发布主题和策略名查找使用同一个实际账号。"""
    published, _ = event_sink
    journal = {('primary', 'same'): (time.time(), 'wrong'),
               ('secondary', 'same'): (time.time(), 'right')}
    monkeypatch.setattr(strategy, '_rpc_service', SimpleNamespace(
        handlers=SimpleNamespace(_order_identity_local=journal)))
    strategy._publish_exec_event(kind, order('secondary'))
    assert len(published) == 1
    account_id, event = published[0]
    assert account_id == event['account_id'] == 'secondary'
    assert event['strategy_name'] == 'right'


@pytest.mark.parametrize('remark', ['same', ''])
def test_presysid_twins_cannot_remove_another_accounts_event(event_sink, remark):
    """无系统号的暂存事件按账号配对，包括无备注的字段配对路径。"""
    published, config = event_sink
    strategy._publish_exec_event('order', order('primary', '', remark=remark))
    strategy._publish_exec_event('order', order('secondary', '', 57, remark))
    assert len(strategy._held_presysid_orders) == 2
    strategy._publish_exec_event('order', order('primary', 'S1', remark=remark))
    assert len(strategy._held_presysid_orders) == 1
    config['exec_events']['hold_presysid_order_seconds'] = 0
    strategy._flush_held_presysid_orders(config)
    assert [account for account, _ in published] == ['primary', 'secondary', 'secondary']
    assert [event['event_type'] for _, event in published] == ['order', 'order', 'order_error']


def test_junk_error_uses_actual_account(event_sink):
    """废单及派生错误事件都发给实际账号。"""
    published, _ = event_sink
    strategy._publish_exec_event('order', order('secondary', status=57))
    assert [account for account, _ in published] == ['secondary', 'secondary']


def test_watch_does_not_confirm_another_accounts_submit_or_cancel():
    """其他账号的同备注委托和已撤状态均不能作为本账号的确认事实。"""
    watch = OrderWatchTable()
    watch.note({'account_id': 'secondary', 'user_order_id': 'same',
                'order_sys_id': 'S1', 'status': '54'})
    gateway = SimpleNamespace(query_orders=lambda *args: [])
    handlers = BigQmtRpcHandlers('primary', None, None, order_gateway=gateway)
    handlers.order_watch_table = watch
    submit = OrderSettlement(SimpleNamespace(account_id='primary', remark='same'),
                             SimpleNamespace(order_sys_id=''), time.monotonic()+10)
    cancel = CancelSettlement(SimpleNamespace(order_sys_id='S1'), 'primary',
                              SimpleNamespace(success=False), time.monotonic()+10)
    assert not handlers._apply_order_lookup(submit)
    assert submit.result.order_sys_id == ''
    assert not handlers._apply_cancel_lookup(cancel)
    assert cancel.result.success is False


def test_watch_preserves_same_identifier_in_each_account():
    """相同标识同时存在于多个账号时，各自读回自己的事实，缺账号不猜测。"""
    watch = OrderWatchTable()
    for account_id, sysid, status in [('primary', 'S1', '50'), ('secondary', 'S2', '54')]:
        watch.note(dict(account_id=account_id, user_order_id='same', order_sys_id=sysid, status=status))
    watch.note(dict(account_id='secondary', user_order_id='other', order_sys_id='S1', status='54'))
    assert watch.sysid_for_remark('same', account_id='primary') == 'S1'
    assert watch.sysid_for_remark('same', account_id='secondary') == 'S2'
    assert watch.status_for_sysid('S1', account_id='primary') == '50'
    assert watch.status_for_sysid('S1', account_id='secondary') == '54'
    assert watch.sysid_for_remark('same') is None


@pytest.mark.parametrize('wire', ['dict', 'json'])
def test_client_filters_other_accounts_before_order_barriers(wire, monkeypatch):
    """Redis 与共享推送载荷在进入屏障前过滤其他账号，避免错误回调或污染编号映射。"""
    import json
    from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader

    trader = BigQmtXtTrader(account_id='primary')
    received, inspected = [], []
    trader._order_barrier()
    trader.register_callback(SimpleNamespace(on_stock_order=received.append))
    monkeypatch.setattr(trader, '_hold_if_pending', lambda event: inspected.append(event) or False)
    try:
        for account_id in ['secondary', 'primary']:
            event = exec_events.normalize_order_event(order(account_id), account_id)
            trader._dispatch_event(event if wire == 'dict' else json.dumps(event))
        assert [event['account_id'] for event in inspected] == ['primary']
        assert [event.account_id for event in received] == ['primary']
    finally:
        trader.stop()
