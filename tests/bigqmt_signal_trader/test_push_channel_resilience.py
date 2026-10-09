"""覆盖推送通道的畸形帧隔离、启动失败清理和回调内退订。"""

import os
import sys
import threading
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader import quote_push_channel as channels  # noqa: E402


class FakeSocket:
    """在指定启动步骤失败，并记录套接字关闭次数与所属线程。"""

    def __init__(self, fail_at=""):
        self.fail_at = fail_at
        self.closed_by = []

    def _fail(self, stage):
        """触发指定阶段的可识别异常。"""
        if stage == self.fail_at:
            raise RuntimeError("failed at " + stage)

    def bind(self, address):
        """模拟地址绑定。"""
        self._fail("bind")

    def connect(self, address):
        """模拟建立订阅连接。"""
        self._fail("connect")

    def setsockopt(self, option, value):
        """模拟订阅过滤器配置。"""
        self._fail("setsockopt")

    def close(self, linger=0):
        """记录关闭动作，确认失败路径只回收一次。"""
        assert linger == 0
        self.closed_by.append(threading.get_ident())


class FailingThread:
    """模拟系统拒绝启动线程，尚未取得任何套接字所有权。"""

    def __init__(self, **kwargs):
        pass

    def start(self):
        """报告线程启动失败。"""
        raise RuntimeError("failed at start")


class FakePubSub:
    """提供坏帧和后续合法帧，记录 Redis 订阅资源的线程归属。"""

    def __init__(self, subscribe_error=False):
        self.subscribe_error = subscribe_error
        self.messages = []
        self.owner = None
        self.closed_by = []
        self.closed = threading.Event()

    def subscribe(self, *topics):
        """模拟订阅成功或部分初始化失败。"""
        self.owner = threading.get_ident()
        if self.subscribe_error:
            raise RuntimeError("subscription failed")

    def get_message(self, timeout):
        """返回下一条推送；空队列也不访问外部服务。"""
        if self.messages:
            return self.messages.pop(0)
        return None

    def close(self):
        """记录关闭资源的线程。"""
        self.closed_by.append(threading.get_ident())
        self.closed.set()


def _fake_zmq_channel(fail_at=""):
    """创建离线 ZMQ 通道，socket 初始化由可控替身提供。"""
    socket = FakeSocket(fail_at)
    channel = channels.ZmqQuotePushChannel(bind_address="fake://bind", connect_address="fake://connect")
    channel._context = SimpleNamespace(socket=lambda kind: socket)
    channel._zmq = SimpleNamespace(PUB=1, SUB=2, SUBSCRIBE=3)
    return channel, socket


@pytest.mark.parametrize("stage", ["bind", "connect", "setsockopt", "start"])
def test_zmq_start_failure_releases_socket_and_resets_state(monkeypatch, stage):
    """任何分配后的启动失败都释放套接字，并恢复可重试的停止状态。"""
    channel, socket = _fake_zmq_channel(stage)
    if stage == "start":
        monkeypatch.setattr(channels.threading, "Thread", FailingThread)
    with pytest.raises(RuntimeError, match="failed at " + stage):
        if stage == "bind":
            channel.start_publisher()
        else:
            channel.start_subscriber(["quotes"], lambda topic, data: None)
    assert socket.closed_by == [threading.get_ident()]
    assert channel._pub is None
    assert channel._sub is None
    assert channel._sub_thread is None
    assert not channel._running


def test_zmq_poller_registration_failure_closes_owned_socket():
    """消费线程的轮询器初始化失败，也要在该线程回收套接字。"""
    channel, socket = _fake_zmq_channel()

    def fail_register(sub, flags):
        """模拟轮询注册失败。"""
        raise RuntimeError("poller registration failed")

    channel._zmq.Poller = lambda: SimpleNamespace(register=fail_register)
    channel._zmq.POLLIN = 1
    channel._running = True
    channel._sub = socket
    channel._sub_loop(socket, lambda topic, data: None)
    assert socket.closed_by == [threading.get_ident()]
    assert channel._sub is None
    assert not channel._running


def test_real_zmq_bad_frame_does_not_kill_subscriber_and_callback_can_stop():
    """通过真实临时端口验证坏帧后仍送达合法消息，回调可安全主动停止。"""
    zmq = pytest.importorskip("zmq")
    context = zmq.Context()
    publisher = context.socket(zmq.XPUB)
    port = publisher.bind_to_random_port("tcp://127.0.0.1")
    channel = channels.ZmqQuotePushChannel(
        connect_address="tcp://127.0.0.1:%s" % port, context=context)
    received = []
    stopped = threading.Event()

    def on_msg(topic, data):
        """记录合法消息，并在回调内完成退订后通知测试线程。"""
        received.append((topic, data))
        channel.stop()
        stopped.set()

    try:
        channel.start_subscriber(["quotes"], on_msg)
        thread = channel._sub_thread
        # XPUB 观察实际订阅完成，避免依赖固定休眠或消息碰巧到达。
        assert publisher.poll(2000, zmq.POLLIN)
        assert publisher.recv() == b"\x01quotes"
        publisher.send_multipart([b"quotes", b"\xc1"])
        publisher.send_multipart([
            b"quotes", channels.encode_push_payload({"data": {"price": 4.2}})])
        assert stopped.wait(2), "合法帧未送达或回调内 stop 失败"
        thread.join(2)
        assert not thread.is_alive()
        assert received == [("quotes", {"price": 4.2})]
        assert channel._sub is None
        assert not channel._running
    finally:
        channel.stop()
        publisher.close(linger=0)
        context.term()


@pytest.mark.parametrize("topic,template", [
    ("quotes", "bigqmt:quote_push:{account_id}:{topic}"),
    ("exec:order", "bigqmt:quote_push:{account_id}:{topic}"),
    ("exec:order", "custom:{topic}:account={account_id}:events"),
])
def test_redis_bad_frame_does_not_kill_subscriber_and_callback_can_stop(topic, template):
    """Redis 隔离坏帧并保留完整主题，自定义频道模板和回调内退订也受支持。"""
    pubsub = FakePubSub()
    redis = SimpleNamespace(pubsub=lambda **kwargs: pubsub)
    channel = channels.RedisQuotePushChannel(
        redis, account_id="account:main", channel_template=template)
    redis_channel = channel._channel(topic).encode("utf-8")
    pubsub.messages = [
        {"type": "message", "channel": redis_channel, "data": b"\xc1"},
        {"type": "message", "channel": redis_channel,
         "data": channels.encode_push_payload({"data": {"price": 4.2}})},
    ]
    received = []
    stopped = threading.Event()

    def on_msg(topic, data):
        """验证回调内 stop 不会尝试 join 自己。"""
        received.append((topic, data))
        channel.stop()
        stopped.set()

    try:
        channel.start_subscriber([topic], on_msg)
        assert stopped.wait(2), "合法帧未送达或回调内 stop 失败"
    finally:
        channel.stop()
    # 回调完成到 finally 清理之间可能有调度间隔，等待资源关闭的确定信号。
    assert pubsub.closed.wait(2)
    assert pubsub.owner is not None
    assert received == [(topic, {"price": 4.2})]
    assert not channel._running
    assert pubsub.closed_by == [pubsub.owner]


def test_redis_subscribe_failure_closes_partial_connection():
    """Redis 连接已创建但订阅失败时，仍释放该连接并清空实例引用。"""
    pubsub = FakePubSub(subscribe_error=True)
    channel = channels.RedisQuotePushChannel(SimpleNamespace(pubsub=lambda **kwargs: pubsub))
    channel.start_subscriber(["quotes"], lambda topic, data: None)
    channel._thread.join(2)
    assert pubsub.closed_by == [pubsub.owner]
    assert channel._pubsub is None
    assert not channel._running


def test_redis_thread_start_failure_resets_state(monkeypatch):
    """线程未能启动时不遗留虚假的运行状态。"""
    channel = channels.RedisQuotePushChannel(SimpleNamespace())
    monkeypatch.setattr(channels.threading, "Thread", FailingThread)
    with pytest.raises(RuntimeError, match="failed at start"):
        channel.start_subscriber(["quotes"], lambda topic, data: None)
    assert not channel._running
    assert channel._thread is None
    assert channel._pubsub is None
