"""覆盖读锁及确认锁超时的恢复、回调副作用与真实故障门禁。"""

import os
import sys
import threading

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader import shm_push_channel as module  # noqa: E402
from bigqmt_signal_trader.quote_subscription_manager import BufferedAcknowledgedPublisher  # noqa: E402
from bigqmt_signal_trader.transports.base import TransportTimeout  # noqa: E402
from test_shm_push_channel import factory, _wait  # noqa: E402,F401


@pytest.mark.parametrize("lane", ["exec:order", "ticks:510300.SH"])
@pytest.mark.parametrize("stage", ["read", "ack"])
def test_lock_timeouts_resume_without_repeating_callbacks(factory, monkeypatch, lane, stage):
    """连续三次传输超时后继续消费，业务回调只执行一次且发布端不留下故障锁。"""
    server, client = factory("server"), factory("client", "stable")
    publisher = BufferedAcknowledgedPublisher(server, {"push_background_sender": False})
    publisher.start_publisher()
    original = client._next_message if stage == "read" else client._ack
    attempts = []
    calls = []

    def transient_busy(*args):
        """注入实际消费循环会遇到的传输超时，不替换发布者或业务处理逻辑。"""
        attempts.append(args)
        if len(attempts) <= 3:
            raise TransportTimeout("shared-memory process lock timed out")
        return original(*args)

    monkeypatch.setattr(client, "_next_message" if stage == "read" else "_ack", transient_busy)
    client.start_subscriber([lane], lambda topic, data: calls.append(data))
    publisher.publish(lane, {"sequence": 1})
    assert publisher.flush(max_wall_seconds=1) == 1
    # 共享游标提交后还会发送空间提示，再移除本地待确认项；只等计数会撞到中间态。
    _wait(lambda: client.status()["acknowledged"] == 1
          and client.status()["pending_acknowledgements"] == 0)
    assert calls == [{"sequence": 1}]
    assert client.status()["consumer_lock_timeouts"] == 3
    assert client.status()["pending_acknowledgements"] == 0
    assert not client.status()["callback_failed"]
    assert client.check_health()["running"]
    assert publisher.status()["healthy"]
    publisher.publish(lane, {"sequence": 2})
    assert publisher.flush(max_wall_seconds=1) == 1
    _wait(lambda: client.status()["acknowledged"] == 2)
    assert calls == [{"sequence": 1}, {"sequence": 2}]


def test_callback_timeout_is_not_retried_as_a_transport_lock(factory):
    """业务回调自身抛出的超时仍是不可重复的业务故障，必须保留门禁。"""
    server, client = factory("server"), factory("client", "business-timeout")
    server.start_publisher()
    calls = []

    def callback(topic, data):
        """模拟业务已产生副作用后等待下游超时。"""
        calls.append(data)
        raise TransportTimeout("business request timed out after side effect")

    client.start_subscriber(["exec:order"], callback)
    server.publish("exec:order", 9)
    _wait(lambda: client.status()["callback_failed"])
    client._thread.join(1)
    with pytest.raises(module.PushCallbackError):
        server.check_health()
    assert calls == [9]
    assert client.status()["acknowledged"] == 0
    assert client.status()["consumer_lock_timeouts"] == 0


def test_message_id_cannot_be_reused_between_trade_and_quote_lanes(factory):
    """同一去重编号不能跨交易与行情日志复用，锁外 CRC 优化不得改变该契约。"""
    server = factory("server")
    server.start_publisher()
    server.publish("exec:order", {"sequence": 1}, message_id="cross-lane")
    with pytest.raises(module.SharedMemoryPushError, match="message_id reused"):
        server.publish("ticks:510300.SH", {"sequence": 1}, message_id="cross-lane")


def test_protocol_failure_is_not_mislabeled_callback_failure(factory, monkeypatch):
    """协议损坏保持失败门禁，但不再伪造业务回调失败证据。"""
    server, client = factory("server"), factory("client", "corruption")
    server.start_publisher()

    def corrupted_read(lane):
        """模拟共享记录校验失败，业务代码尚未执行。"""
        raise module.PushGapError("push payload CRC mismatch")

    monkeypatch.setattr(client, "_next_message", corrupted_read)
    client.start_subscriber(["exec:order"], lambda topic, data: pytest.fail("不应执行回调"))
    _wait(lambda: not client._running)
    client._thread.join(1)
    with pytest.raises(module.SharedMemoryPushError, match="subscriber failed") as error:
        server.check_health()
    assert not isinstance(error.value, module.PushCallbackError)
    assert not client.status()["callback_failed"]


def test_callback_stop_finishes_pending_ack_after_lock_timeouts(factory, monkeypatch):
    """回调主动停止后仍完成唯一一次确认，不能因暂时锁忙遗留已执行消息。"""
    server, client = factory("server"), factory("client", "stop-after-callback")
    server.start_publisher()
    original = client._ack
    attempts = []
    calls = []

    def busy_ack(lane, sequence):
        """前两次确认超时，第三次按真实共享游标提交。"""
        attempts.append(sequence)
        if len(attempts) < 3:
            raise TransportTimeout("ack lock timed out")
        return original(lane, sequence)

    def callback(topic, data):
        """业务副作用完成后请求停止消费。"""
        calls.append(data)
        client.stop()

    monkeypatch.setattr(client, "_ack", busy_ack)
    client.start_subscriber(["exec:order"], callback)
    server.publish("exec:order", 11)
    _wait(lambda: not client.status()["started"])
    assert calls == [11]
    assert attempts == [1, 1, 1]
    assert client.status()["acknowledged"] == 1
    assert server.check_health()["running"]


def test_shutdown_keeps_handles_until_pending_ack_can_finish(factory, monkeypatch):
    """长期确认锁忙时停止明确超时，保留线程和句柄；锁恢复后只补确认。"""
    server, client = factory("server"), factory("client", "stop-busy")
    server.start_publisher()
    original = client._ack
    release = threading.Event()
    calls = []

    def blocked_ack(lane, sequence):
        """确认持续超时，直到测试显式解除竞争。"""
        if not release.is_set():
            raise TransportTimeout("ack lock still busy")
        return original(lane, sequence)

    monkeypatch.setattr(client, "_ack", blocked_ack)
    try:
        client.start_subscriber(["exec:order"], lambda topic, data: calls.append(data))
        server.publish("exec:order", 12)
        _wait(lambda: client.status()["pending_acknowledgements"] == 1)
        with pytest.raises(module.SharedMemoryPushError, match="resources retained"):
            client.stop()
        assert client._region is not None
        assert client._thread.is_alive()
    finally:
        release.set()
    _wait(lambda: not client.status()["started"])
    assert calls == [12]
    assert client.status()["acknowledged"] == 1


def test_debug_lock_timeout_records_stage_only_when_enabled(factory, monkeypatch, capsys):
    """调试开关打开时记录锁等待阶段，不把正常运行变成持续日志输出。"""
    server, client = factory("server"), factory("client", "debug-lock")
    server.start_publisher()
    original = client._ack
    attempts = []

    def busy_once(lane, sequence):
        """制造一次确认锁超时，以便关联已完成回调的序号。"""
        attempts.append(sequence)
        if len(attempts) == 1:
            raise TransportTimeout("ack lock timed out")
        return original(lane, sequence)

    monkeypatch.setenv("BIGQMT_DEBUG", "1")
    monkeypatch.setattr(client, "_ack", busy_once)
    client.start_subscriber(["exec:order"], lambda topic, data: None)
    server.publish("exec:order", 13)
    _wait(lambda: client.status()["acknowledged"] == 1)
    output = capsys.readouterr().out
    assert '"event": "push_consumer_lock_busy"' in output
    assert '"stage": "ack"' in output
    assert '"pending_ack": 1' in output


@pytest.mark.parametrize("operation", ["publish", "duplicate", "read"])
def test_large_message_crc_does_not_hold_shared_mutex(factory, monkeypatch, operation):
    """大块 CRC 计算期间其他线程仍可取得共享锁，避免等待 GIL 放大跨进程锁占用。"""
    server, client = factory("server"), factory("client", "crc-reader")
    observer = factory("client", "crc-observer")
    release = threading.Event()
    monkeypatch.setattr(client, "_consume", lambda callback: release.wait(3))
    server.start_publisher()
    client.start_subscriber(["ticks:510300.SH"], lambda topic, data: None)
    observer._open()
    payload = {"values": "x" * 12000}
    if operation != "publish":
        server.publish("ticks:510300.SH", payload, message_id="stable")
    original = module.zlib.crc32
    observed = []

    def unlocked_crc(data):
        """在校验期间由另一线程取锁，跨 Windows 线程排除 mutex 可重入造成的假通过。"""
        acquired = []

        def probe():
            """独立句柄、独立线程只验证元数据锁可用，不修改共享记录。"""
            try:
                with observer._region.locked(0.1):
                    acquired.append(True)
            except TransportTimeout:
                acquired.append(False)

        thread = threading.Thread(target=probe)
        thread.start()
        thread.join(1)
        assert acquired == [True]
        observed.append(len(data))
        return original(data)

    monkeypatch.setattr(module.zlib, "crc32", unlocked_crc)
    try:
        if operation == "read":
            sequence, data = client._next_message(0)
            assert sequence == 1
            assert client._decode_payload(data)["data"] == payload
        else:
            assert server.publish("ticks:510300.SH", payload, message_id="stable")
        assert observed and min(observed) > 5120
    finally:
        release.set()
        client._thread.join(1)
