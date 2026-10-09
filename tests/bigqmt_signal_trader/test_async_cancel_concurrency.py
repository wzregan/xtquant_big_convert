"""复现开盘实测的异步撤单串行等待，并验证并发关闭与回调语义。"""

import threading
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from bigqmt_signal_trader.async_worker import AsyncWorker
from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader


def test_twenty_async_cancels_reach_rpc_together_and_keep_sequences():
    """二十笔必须同时进入 RPC；单线程实现会在屏障处超时，不能靠放宽耗时通过。"""
    trader = BigQmtXtTrader(account_id="acct")
    barrier = threading.Barrier(20)
    responses = []
    failures = []

    class Client:
        """用屏障模拟柜台在所有撤单进入后统一确认。"""

        account_id = "acct"

        def call(self, *args, **kwargs):
            """等待全部请求进入，不执行真实交易。"""
            barrier.wait(timeout=5)
            return {"success": True}

    class Callback:
        """按原委托号核对异步序号，允许不同委托的完成顺序变化。"""

        def on_cancel_order_stock_async_response(self, response):
            """保留全部成功响应。"""
            responses.append(response)

        def on_cancel_error(self, error):
            """保留异常，避免线程内断言失败被日志隐藏。"""
            failures.append(error)

    trader.client = Client()
    trader.callback = Callback()
    expected = {}
    try:
        for index in range(20):
            order_id = str(1000 + index)
            if index % 2:
                seq = trader.cancel_order_stock_sysid_async("acct", 0, order_id)
            else:
                seq = trader.cancel_order_stock_async("acct", order_id)
            expected[seq] = order_id
        assert trader.wait_async_requests(8)
        assert not failures
        assert len(responses) == 20
        assert {r.seq: r.order_sys_id for r in responses} == expected
        assert all(r.success and r.cancel_result == 0 for r in responses)
    finally:
        trader.stop()


def test_pending_cancel_does_not_block_async_queries_or_finish_early():
    """撤单及回调未结束时排空返回 False，同时异步查询仍可执行。"""
    trader = BigQmtXtTrader(account_id="acct")
    entered = threading.Event()
    release = threading.Event()
    queried = threading.Event()

    def cancel(account, order_id):
        """模拟等待柜台确认的撤单。"""
        entered.set()
        assert release.wait(5)
        return 0

    def query(account):
        """返回不依赖撤单结束的账户查询结果。"""
        return {"account_id": account}

    def callback(result):
        """确认查询已经到达调用方。"""
        queried.set()

    trader.cancel_order_stock = cancel
    trader.query_stock_asset = query
    try:
        trader.cancel_order_stock_async("acct", "1000")
        assert entered.wait(2)
        trader.query_stock_asset_async("acct", callback)
        assert queried.wait(2)
        assert not trader.wait_async_requests(.02)
        release.set()
        assert trader.wait_async_requests(2)
    finally:
        release.set()
        trader.stop()


def test_concurrent_worker_close_drains_all_accepted_tasks_and_is_bounded():
    """限时关闭后仍排空已接收任务，并拒绝新增任务；全部线程最终退出。"""
    worker = AsyncWorker("cancel-drain-test", max_workers=3)
    release = threading.Event()
    completed = []

    def task(index):
        """停在测试闸门，便于检查关闭期间的未完成计数。"""
        assert release.wait(5)
        completed.append(index)

    try:
        for index in range(12):
            worker.submit(task, index)
        worker.close(timeout=0)
        with pytest.raises(RuntimeError, match="stopped"):
            worker.submit(task, 12)
        assert not worker.wait(.01)
        release.set()
        assert worker.wait(2)
        worker.close(timeout=2)
        assert sorted(completed) == list(range(12))
        assert all(not thread.is_alive() for thread in worker._threads)
    finally:
        release.set()
        worker.close(2)


def test_wait_from_worker_callback_never_deadlocks():
    """回调不能等待包含自己的队列完成。"""
    worker = AsyncWorker("cancel-self-wait-test", max_workers=2)
    results = []

    def task():
        """记录自身队列的等待结果。"""
        results.append(worker.wait(2))

    try:
        worker.submit(task)
        assert worker.wait(2)
        assert results == [False]
    finally:
        worker.close(2)
