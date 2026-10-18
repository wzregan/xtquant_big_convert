"""可靠执行回报断连后的停止必须唤醒退避，真正阻塞的回调仍须显式失败。"""

import threading
import time

import pytest

from bigqmt_signal_trader.sdk_trader import XtQuantTrader


@pytest.mark.parametrize("worker_drain_seconds", [0.0, 0.9])
def test_disconnect_backoff_is_interrupted_by_sdk_stop(
    monkeypatch, worker_drain_seconds
):
    """断连回调与停止同时发生时，退避不得耗尽停止预算并永久锁住网关重建。"""
    disconnected = threading.Event()
    closed = threading.Event()

    class Channel:
        """只控制线程时序，不模拟交易业务或共享内存编解码。"""

        supports_acknowledgements = True

        def start_subscriber(self, topics, callback):
            """模拟已订阅后才发现发布者换代。"""

        def check_health(self):
            """确定性注入发布者换代，进入正式断连清理路径。"""
            raise RuntimeError("synthetic publisher generation changed")

        def stop(self):
            """模拟正常退出仍有极短清理开销，使旧一秒退避竞争稳定复现。"""
            time.sleep(0.05)
            closed.set()

    class Callback:
        """记录调用方开始重建的时刻。"""

        def on_disconnected(self):
            """触发停止方，回调本身没有阻塞。"""
            disconnected.set()

    trader = XtQuantTrader(account_id="isolated-stop", timeout_seconds=1)
    trader.callback = Callback()
    monkeypatch.setattr(trader, "_exec_events_redis_or_none", lambda: None)
    monkeypatch.setattr(trader, "_exec_transport_uses_push", lambda: True)
    monkeypatch.setattr(trader, "_build_quote_push_channel", Channel)
    monkeypatch.setattr(trader, "_probe_communication_health", lambda: None)
    if worker_drain_seconds:
        monkeypatch.setattr(
            trader._cancel_worker,
            "close",
            lambda timeout: time.sleep(worker_drain_seconds),
        )
    trader._start_event_listener()
    try:
        assert disconnected.wait(2)
        started = time.monotonic()
        assert trader.stop() == 0
        assert time.monotonic() - started < worker_drain_seconds + 0.5
        assert closed.is_set() and not trader._event_thread.is_alive()
    finally:
        trader._event_running = False
        trader._event_thread.join(2)
        trader.stop()


def test_callback_still_running_blocks_sdk_stop(monkeypatch):
    """可中断退避不能掩盖仍在执行的业务回调，失败时保留原线程所有权。"""
    entered = threading.Event()
    release = threading.Event()
    trader = XtQuantTrader(account_id="isolated-stop", timeout_seconds=0.1)

    def blocked_callback():
        """模拟用户回调尚未返回，此时不允许新实例消费同一回报。"""
        entered.set()
        release.wait(5)

    monkeypatch.setattr(trader, "_event_loop", blocked_callback)
    trader._start_event_listener()
    original = trader._event_thread
    try:
        assert entered.wait(2)
        with pytest.raises(RuntimeError, match="event listener did not stop"):
            trader.stop()
        assert trader._event_thread is original and original.is_alive()
    finally:
        release.set()
        original.join(2)
        trader.stop()
