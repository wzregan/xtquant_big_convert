"""验证统一后端后的 Redis 请求仍保持原有路由、等待及解码契约。"""

import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.redis_rpc import (  # noqa: E402
    TYPED_PAYLOAD_FLAG,
    decode_rpc_request_payload,
)
from bigqmt_signal_trader.transports.base import TransportTimeout  # noqa: E402
from bigqmt_signal_trader.transports.redis_transport import RedisTransport  # noqa: E402
from bigqmt_signal_trader.transports import redis_transport  # noqa: E402


class Clock:
    """用可控时钟覆盖超时边界，避免测试真实睡眠。"""

    def __init__(self):
        self.now = 0.0

    def time(self):
        """返回当前模拟时间，单位秒。"""
        return self.now


class FakeRedis:
    """记录 Redis 操作，通过预设响应模拟键、队列和订阅行为。"""

    def __init__(self, clock):
        self.clock = clock
        self.requests = []
        self.get_keys = []
        self.expirations = []
        self.waits = []
        self.deleted = []
        self.key_response = None
        self.queue_results = []
        self.pubsub_messages = []
        self.subscriptions = []
        self.closed = False
        self.on_send = None

    def _record_request(self, address, payload):
        """解码实际线上信封，并在发送后触发测试响应器。"""
        self.requests.append((address, json.loads(decode_rpc_request_payload(payload))))
        if self.on_send is not None:
            self.on_send(self.requests[-1][1])

    def rpush(self, address, payload):
        """记录单次队列请求，不自动重试。"""
        self._record_request(address, payload)

    def expire(self, address, ttl):
        """记录键的过期时间，单位秒。"""
        self.expirations.append((address, ttl))

    def get(self, address):
        """返回预设的响应键内容。"""
        self.get_keys.append(address)
        return self.key_response

    def blpop(self, address, timeout):
        """模拟阻塞取响应，每次消耗给定秒数。"""
        self.waits.append((address, timeout))
        self.clock.now += timeout
        result = self.queue_results.pop(0) if self.queue_results else None
        if isinstance(result, Exception):
            raise result
        return result

    def delete(self, address):
        """记录响应队列的清理。"""
        self.deleted.append(address)

    def pubsub(self, ignore_subscribe_messages):
        """返回本对象，模拟订阅实例。"""
        assert ignore_subscribe_messages
        return self

    def subscribe(self, address):
        """记录响应订阅地址。"""
        self.subscriptions.append(address)

    def publish(self, address, payload):
        """记录单次发布请求。"""
        self._record_request(address, payload)

    def get_message(self, timeout):
        """读取消息，无消息时推进到等待期限。"""
        if self.pubsub_messages:
            return self.pubsub_messages.pop(0)
        self.clock.now += timeout
        return None

    def close(self):
        """标记订阅连接已关闭。"""
        self.closed = True


@pytest.fixture
def redis(monkeypatch):
    """提供完全离线的 Redis 与时间源。"""
    clock = Clock()
    monkeypatch.setattr(redis_transport.time, "time", clock.time)
    return FakeRedis(clock)


def _request(**overrides):
    """生成实际 RPC 信封，可覆盖账号和响应路由提示。"""
    request = {
        "schema_version": 1,
        "request_id": "request-1",
        "account_id": "default",
        "method": "get_full_tick",
        "params": {"stock_list": ["510300.SH"]},
        "dataframe_format": "columns_v1",
    }
    request.update(overrides)
    return request


def _response(data=None, request_id="request-1"):
    """编码服务端响应，供所有读取路径共用。"""
    return json.dumps({"ok": True, "request_id": request_id, "data": data or {}})


def test_account_override_and_fast_key_response(redis):
    """请求账号决定队列及响应地址，已有响应不再进入 BLPOP。"""
    redis.key_response = _response({"lastPrice": 4.2})
    request = _request(account_id="alternate")
    response = RedisTransport(redis, account_id="default").send_request(request, 3)

    assert redis.requests[0][0] == "bigqmt:rpc:queue:alternate"
    sent = redis.requests[0][1]
    assert sent["account_id"] == "alternate"
    assert sent["reply_list"] == "bigqmt:rpc:respq:alternate:request-1"
    assert redis.get_keys == ["bigqmt:rpc:resp:alternate:request-1"]
    assert sent["dataframe_format"] == "columns_v1"
    assert "reply_list" not in request
    assert redis.waits == []
    assert response[TYPED_PAYLOAD_FLAG] is False


def test_request_reply_hints_and_ttl_control_both_ends(redis):
    """客户端监听请求声明的响应地址，避免自定义路由只在服务端生效。"""
    redis.queue_results = [("custom-list", _response())]
    request = _request(reply_key="custom-key", reply_list="custom-list",
                       reply_channel="custom-channel", ttl_seconds=123)
    RedisTransport(redis).send_request(request, 3)

    assert redis.get_keys == ["custom-key"]
    assert redis.waits == [("custom-list", 1)]
    assert redis.deleted == ["custom-list"]
    assert redis.expirations == [("bigqmt:rpc:queue:default", 123)]
    assert redis.requests[0][1]["reply_channel"] == "custom-channel"
    assert redis.requests[0][1]["ttl_seconds"] == 123


def test_default_ttl_and_empty_hints_are_filled_without_mutation(redis):
    """实例 TTL 补齐缺省值，空路由提示采用账号对应的默认地址。"""
    redis.key_response = _response()
    request = _request(reply_key="", reply_list="", reply_channel="")
    request.pop("account_id")
    RedisTransport(redis, account_id="instance", response_ttl_seconds=90).send_request(request, 3)

    sent = redis.requests[0][1]
    assert sent["account_id"] == "instance"
    assert sent["reply_key"] == "bigqmt:rpc:resp:instance:request-1"
    assert sent["ttl_seconds"] == 90
    assert redis.expirations == [("bigqmt:rpc:queue:instance", 90)]
    assert request["reply_key"] == ""
    assert "account_id" not in request


def test_socket_timeout_continues_receiving_without_resubmitting(redis):
    """连接读超时不会提前终止总等待窗口，更不能重新提交交易请求。"""
    redis.queue_results = [
        TimeoutError("Timeout reading from socket"),
        ("response-list", _response({"order_id": 42})),
    ]
    response = RedisTransport(redis).send_request(_request(method="order_stock"), 3)

    assert response["data"]["order_id"] == 42
    assert len(redis.waits) == 2
    assert len(redis.requests) == 1


def test_timeout_uses_total_window_and_preserves_routing_diagnostics(redis):
    """连续读超时最终转为传输超时，日志保留实际账号与请求队列。"""
    redis.queue_results = [TimeoutError("Timeout reading from socket")] * 3
    with pytest.raises(TransportTimeout, match="account_id=alternate.*queue:alternate"):
        RedisTransport(redis).send_request(_request(account_id="alternate"), 2)
    assert len(redis.waits) == 2
    assert len(redis.requests) == 1


def test_non_timeout_error_is_not_masked(redis):
    """连接类错误立即交给调用方，不伪装成等待超时。"""
    error = ConnectionError("connection refused")
    redis.queue_results = [error]
    with pytest.raises(ConnectionError) as raised:
        RedisTransport(redis).send_request(_request(), 3)
    assert raised.value is error
    assert len(redis.requests) == 1


@pytest.mark.parametrize("mode", ["queue", "pubsub"])
@pytest.mark.parametrize("typed", [False, True])
def test_response_paths_preserve_typed_payload_fast_path(redis, mode, typed):
    """所有响应路径携带类型标记，普通大行情结果可跳过递归还原。"""
    data = {"__bigqmt_type__": "dataframe", "records": []} if typed else {"price": 4.2}
    wire = _response(data)
    if mode == "queue":
        redis.queue_results = [("response-list", wire)]
    else:
        redis.pubsub_messages = [
            {"type": "message", "data": _response(request_id="unrelated")},
            {"type": "message", "data": wire},
        ]
    response = RedisTransport(redis).send_request(_request(), 3, transport=mode)
    assert response[TYPED_PAYLOAD_FLAG] is typed
    assert response["data"] == data
    if mode == "pubsub":
        assert redis.closed


def test_pubsub_custom_route_and_key_fallback_close_subscription(redis):
    """订阅模式也尊重自定义地址，超时后读取响应键并关闭订阅。"""
    redis.key_response = _response({"price": 4.2})
    response = RedisTransport(redis).send_request(
        _request(reply_channel="custom-channel", reply_key="custom-key"),
        2, transport="pubsub")
    assert redis.subscriptions == ["custom-channel"]
    assert redis.get_keys == ["custom-key"]
    assert response[TYPED_PAYLOAD_FLAG] is False
    assert redis.closed


def test_pubsub_timeout_closes_subscription(redis):
    """未收到响应时仍释放订阅，保持后续请求可用。"""
    with pytest.raises(TransportTimeout):
        RedisTransport(redis).send_request(_request(), 2, transport="pubsub")
    assert redis.closed
    assert len(redis.requests) == 1
