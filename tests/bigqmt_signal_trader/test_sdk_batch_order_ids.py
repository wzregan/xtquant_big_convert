"""验证全量历史超过缓存容量时的持久编号、跨进程一致性和批量转换效率。"""

import multiprocessing
import sqlite3
from unittest.mock import Mock

import pytest

from bigqmt_signal_trader import xtquant_compat
from bigqmt_signal_trader.sdk_order_ids import OrderIdRegistry, trading_date
from bigqmt_signal_trader.sdk_trader import XtQuantTrader


def _concurrent_mapping(path, keys, start, pipe):
    """独立进程等待同一发令点，使用正式唯一约束同时注册重叠身份。"""
    registry = OrderIdRegistry(path)
    pipe.send("ready")
    assert start.wait(5)
    pipe.send(registry.remember_many(keys))
    pipe.close()


def test_batch_identity_order_duplicates_scoping_and_bounded_cache(tmp_path):
    """一万二千条身份映射只需一个连接，输出不依赖容量4096的缓存存留。"""
    registry = OrderIdRegistry(tmp_path / "ids.db")
    keys = [("A", "20261016", "SYS-%05d" % index) for index in range(12000)]
    keys += [keys[0], ("A", "20261017", "SYS-00000"), ("B", "20261016", "SYS-00000"), ("", "", "")]
    registry._connect = Mock(wraps=registry._connect)
    first = registry.remember_many(keys)
    assert registry._connect.call_count == 1
    assert first[-4] == first[0]
    assert len(set(first[:-4])) == 12000
    assert len({first[0], first[-3], first[-2]}) == 3
    assert first[-1] == 0
    assert len(registry._cache) == 4096
    assert registry.remember_many(keys) == first
    assert registry._connect.call_count == 2
    other = OrderIdRegistry(registry.path)
    assert other.remember_many(list(reversed(keys))) == list(reversed(first))
    assert registry.resolve("A", "20261016", first[0]) == "SYS-00000"
    with pytest.raises(ValueError, match="unknown"):
        registry.resolve("B", "20261016", first[0])


def test_batch_rollback_does_not_publish_uncommitted_ids(tmp_path):
    """数据库批次中途失败必须回滚全部新增身份，缓存不得保留幽灵编号。"""
    registry = OrderIdRegistry(tmp_path / "ids.db")
    with sqlite3.connect(registry.path) as db:
        db.execute("""CREATE TRIGGER fail_insert BEFORE INSERT ON order_ids
                    WHEN NEW.sysid='FAIL' BEGIN SELECT RAISE(ABORT,'injected failure'); END""")
    keys = [("A", "20261016", "FRESH"), ("A", "20261016", "FAIL")]
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        registry.remember_many(keys)
    assert not registry._cache
    with sqlite3.connect(registry.path) as db:
        assert db.execute("SELECT count(*) FROM order_ids").fetchone()[0] == 0
        db.execute("DROP TRIGGER fail_insert")
    result = registry.remember_many(keys)
    assert all(value > 0 for value in result)
    assert registry.remember_many(keys) == result


def test_batch_preserves_large_integer_ids_and_delimiters_in_identity(tmp_path):
    """批量读取保留大于2**53的编号，合同号内标点及中文不能被聚合格式误拆。"""
    registry = OrderIdRegistry(tmp_path / "ids.db")
    first = ("账户,甲:乙", "20261016", "合同:1,2\"3")
    second = ("账户,甲:乙", "20261017", "合同:1,2\"3")
    big_id = 2**53 + 1
    with sqlite3.connect(registry.path) as db:
        db.execute(
            "INSERT INTO order_ids(id,account,day,sysid) VALUES(?,?,?,?)",
            (big_id,) + first,
        )
    actual = registry.remember_many([first, second, first, ("", "", "")])
    assert actual == [big_id, big_id + 1, big_id, 0]
    assert registry.resolve(first[0], first[1], big_id) == first[2]
    assert OrderIdRegistry(registry.path).remember_many([second, first]) == [big_id + 1, big_id]


def test_invalid_batch_does_not_partially_register_identity(tmp_path):
    """先验证整个输入，非法身份不能导致前面的合法身份被提前提交。"""
    registry = OrderIdRegistry(tmp_path / "ids.db")
    with pytest.raises(ValueError, match="invalid"):
        registry.remember_many([("A", "20261016", "valid"), ("A", "", "invalid")])
    assert registry.remember_many([]) == []
    assert not registry._cache
    with sqlite3.connect(registry.path) as db:
        assert db.execute("SELECT count(*) FROM order_ids").fetchone()[0] == 0


def test_batch_concurrent_processes_use_same_persistent_ids(tmp_path):
    """不同进程相反顺序同时发现同一批合同号，必须收敛到同一整数映射。"""
    path = str(tmp_path / "ids.db")
    OrderIdRegistry(path)
    keys = [("A", "20261016", "SYS-%04d" % index) for index in range(1000)]
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    processes, pipes = [], []
    try:
        for current in (keys, list(reversed(keys))):
            parent, child = context.Pipe()
            process = context.Process(target=_concurrent_mapping, args=(path, current, start, child))
            process.start()
            child.close()
            processes.append(process)
            pipes.append(parent)
        for pipe in pipes:
            assert pipe.poll(10) and pipe.recv() == "ready"
        start.set()
        values = []
        for pipe in pipes:
            assert pipe.poll(10)
            values.append(pipe.recv())
        assert values[0] == list(reversed(values[1]))
        assert len(set(values[0])) == len(keys)
        with sqlite3.connect(path) as db:
            assert db.execute("SELECT count(*) FROM order_ids").fetchone()[0] == len(keys)
    finally:
        start.set()
        for process in processes:
            process.join(5)
            if process.is_alive():
                process.terminate()
                process.join(2)
            assert process.exitcode == 0
        for pipe in pipes:
            pipe.close()


@pytest.fixture
def trader(tmp_path, monkeypatch):
    """隔离配置及持久库，只替代原生 RPC 数据，不替代 SDK 对象转换和编号注册。"""
    monkeypatch.setattr(xtquant_compat, "load_client_config", lambda: {})
    monkeypatch.setenv("BIGQMT_ORDER_ID_DB", str(tmp_path / "ids.db"))
    value = XtQuantTrader(account_id="A")
    yield value
    value.stop()


def test_sdk_large_queries_and_execution_snapshot_map_whole_batch(trader):
    """12000条历史连续查询及联合快照只按批开库；字段、费用和编号逐条保持一致。"""
    rows = [dict(order_sys_id="SYS-%05d" % index, stock_code="510300.SH", action="BUY",
                 volume=100, status=54, order_time=20261016103000, price=4.101)
            for index in range(12000)]
    trades = [dict(row, traded_time=20261016103000, trade_id="FILL-%05d" % index)
              for index, row in enumerate(rows)]

    def rpc(method, params, **kwargs):
        """仅提供底层原生快照，仍由正式 SDK 执行全部字段及持久身份转换。"""
        if method == "query_stock_orders":
            return rows
        if method == "query_stock_trades":
            return trades
        assert method == "query_execution_snapshot"
        return {"orders": rows, "trades": trades}

    trader.client.call = rpc
    registry = trader._registry()
    registry._connect = Mock(wraps=registry._connect)
    first = trader.query_stock_orders("A")
    ids = [row.order_id for row in first]
    assert len(set(ids)) == 12000
    assert registry._connect.call_count == 1
    for _ in range(3):
        assert [row.order_id for row in trader.query_stock_orders("A")] == ids
    assert registry._connect.call_count == 4
    fills = trader.query_stock_trades("A")
    assert [row.order_id for row in fills] == ids
    assert all(row.commission is None for row in fills)
    snapshot = trader.query_execution_snapshot("A")
    assert [row.order_id for row in snapshot["orders"]] == ids
    assert [row.order_id for row in snapshot["trades"]] == ids
    assert registry._connect.call_count == 7
    assert len(registry._cache) == 4096
    assert all(row.order_status == 54 and row.price.hex() == (4.101).hex() for row in first)


def test_single_callback_uses_only_original_day_identity(trader):
    """历史回报不能先注册今天的同名合同号，再覆盖为历史日期造成多余身份。"""
    row = dict(order_sys_id="shared-sysid", order_time=20261014103000, action="BUY")
    order = trader._order_from_dict("A", row)
    fill = trader._trade_from_dict("A", dict(row, traded_time=20261014103000))
    assert order.order_id == fill.order_id > 0
    with sqlite3.connect(trader._registry().path) as db:
        assert db.execute("SELECT account,day,sysid FROM order_ids").fetchall() == [
            ("A", trading_date(20261014103000), "shared-sysid")]
