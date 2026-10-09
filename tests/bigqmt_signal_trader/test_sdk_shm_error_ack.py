"""覆盖标准 SDK 首条错误回报的共享内存确认契约，复现 Windows gRPC 断连。"""

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from bigqmt_signal_trader.sdk_trader import XtQuantTrader


@pytest.mark.parametrize("kind", ["order_error", "cancel_error"])
@pytest.mark.parametrize("fails", [False, True])
def test_first_sdk_error_push_acknowledges_only_successful_callback(tmp_path, monkeypatch, kind, fails):
    """从未异步下单的 SDK 也能确认首条错误；回调失败必须向推送通道传播。"""
    monkeypatch.setenv("BIGQMT_ORDER_ID_DB", str(tmp_path / "ids.sqlite3"))
    trader = XtQuantTrader(account_id="acct")
    received = []

    class Callback:
        """保存经过标准 SDK 映射的错误对象。"""

        def on_order_error(self, value):
            """模拟成功处理或主动拒绝确认的业务回调。"""
            received.append(value)
            if fails:
                raise ValueError("business callback failed")

        def on_cancel_error(self, value):
            """撤单错误使用相同的确认要求。"""
            self.on_order_error(value)

    trader.callback = Callback()
    event = {"event_type": kind, "account_id": "acct", "order_sys_id": "1234",
             "error_id": 61270, "error_msg": "订单重传", "remark": "test-error"}
    try:
        if fails:
            with pytest.raises(ValueError, match="business callback failed"):
                trader._dispatch_event(event, acknowledged=True)
        else:
            trader._dispatch_event(event, acknowledged=True)
        assert len(received) == 1
        assert type(received[0].order_id) is int
        assert received[0].order_sys_id == "1234"
        assert received[0].error_msg == "订单重传"
    finally:
        trader.stop()
