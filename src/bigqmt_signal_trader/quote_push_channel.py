"""Server→client whole-quote push channel.

The RPC transport is request/response only; whole-quote data needs the opposite
direction — the server pushes each incremental tick batch to every client
subscribed to that combination. This module provides one abstract channel with
two interchangeable implementations:

* :class:`ZmqQuotePushChannel` — a ``PUB`` socket on the server, a ``SUB`` socket
  per client. Native to no-redis deployments. Fire-and-forget: a client that is
  down simply misses frames (acceptable for incremental quote pushes).
* :class:`RedisQuotePushChannel` — redis ``publish``/``subscribe`` on a
  per-account, per-combination channel, for redis deployments.

Wire encoding is msgpack when available (smaller + faster for the
``{code: {field: number}}`` payload shape), falling back to stdlib json so the
channel stays usable without the optional dependency.
"""

import json
import threading

try:
    import msgpack

    _HAS_MSGPACK = True
except Exception:  # pragma: no cover - depends on optional dependency
    msgpack = None
    _HAS_MSGPACK = False


def encode_push_payload(payload):
    """Encode a push payload dict to bytes (msgpack preferred, json fallback)."""
    if _HAS_MSGPACK:
        return msgpack.packb(payload, use_bin_type=True)
    return json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")


def decode_push_payload(blob):
    """Inverse of :func:`encode_push_payload`. Accepts bytes or str.

    Encoding is not symmetric across deployments: a server without msgpack
    falls back to json while a client with msgpack installed decodes with
    msgpack — ``msgpack.unpackb`` then raises ``ExtraData`` on the json text
    (its first byte ``{`` parses as an int, leaving trailing bytes). So try
    msgpack first, and fall back to json when the bytes are not a single
    valid msgpack object.
    """
    if blob is None:
        return None
    if isinstance(blob, str):
        blob = blob.encode("utf-8")
    if _HAS_MSGPACK:
        try:
            return msgpack.unpackb(blob, raw=False)
        except Exception:
            pass
    return json.loads(blob.decode("utf-8"))


class QuotePushChannel(object):
    """行情与交易回报共用的推送接口，由调用方启动和关闭通道。"""

    @property
    def publisher_endpoint(self):
        """返回诊断用发布端点，没有独立端点的后端返回空字符串。"""
        return ""

    def start_publisher(self):
        """启动服务端发布资源。"""
        raise NotImplementedError

    def start_subscriber(self, topics, on_msg):
        """订阅主题，on_msg 接收主题和已解码的业务消息。"""
        raise NotImplementedError

    def publish(self, topic, data):
        """发布主题消息，data 为规范化后的业务字典。"""
        raise NotImplementedError

    def stop(self):
        """停止收发并释放本实例持有的资源。"""
        raise NotImplementedError


class ZmqQuotePushChannel(QuotePushChannel):
    @property
    def publisher_endpoint(self):
        """返回服务端发布地址，供订阅诊断使用。"""
        return str(self.bind_address or "")

    def __init__(self, bind_address=None, connect_address=None, context=None, print_prefix="[bigqmt_quote_push]"):
        self.bind_address = bind_address
        self.connect_address = connect_address
        self.print_prefix = print_prefix
        self._zmq = None
        self._context = context
        self._pub = None
        self._pub_lock = threading.Lock()
        self._sub = None
        self._sub_thread = None
        self._running = False

    def _ensure_context(self):
        if self._zmq is None:
            import zmq

            self._zmq = zmq
            if self._context is None:
                self._context = zmq.Context.instance()
        return self._zmq, self._context

    # -- server side ---------------------------------------------------------
    def start_publisher(self):
        """创建发布套接字；绑定失败时立即释放尚未交给实例的资源。"""
        zmq, ctx = self._ensure_context()
        if not self.bind_address:
            raise ValueError("bind_address is required to start a publisher")
        pub = ctx.socket(zmq.PUB)
        try:
            pub.bind(self.bind_address)
        except Exception:
            try:
                pub.close(linger=0)
            except Exception:
                pass
            raise
        self._pub = pub
        self._running = True

    def publish(self, topic, data):
        payload = encode_push_payload({"combo_key": topic, "data": data})
        frame = [str(topic).encode("utf-8"), payload]
        # PUB socket is not thread-safe; serialize under the lock and read the
        # socket inside it so a concurrent stop() (which nulls _pub) can't hand
        # us a closed socket.
        with self._pub_lock:
            pub = self._pub
            if pub is None:
                return
            try:
                pub.send_multipart(frame)
            except Exception as exc:
                print("%s zmq publish failed: %s" % (self.print_prefix, exc))

    # -- client side ---------------------------------------------------------
    def start_subscriber(self, topics, on_msg):
        """配置主题并启动订阅线程；启动完成后由该线程独占和关闭套接字。"""
        zmq, ctx = self._ensure_context()
        if not self.connect_address:
            raise ValueError("connect_address is required to start a subscriber")
        sub = ctx.socket(zmq.SUB)
        try:
            sub.connect(self.connect_address)
            for topic in topics or []:
                sub.setsockopt(zmq.SUBSCRIBE, str(topic).encode("utf-8"))
            self._sub = sub
            self._running = True
            self._sub_thread = threading.Thread(
                target=self._sub_loop, args=(sub, on_msg), name="bigqmt-quote-push-sub", daemon=True
            )
            self._sub_thread.start()
        except Exception:
            # 线程尚未启动，套接字仍归启动方所有，必须在此回收。
            self._running = False
            self._sub_thread = None
            self._sub = None
            try:
                sub.close(linger=0)
            except Exception:
                pass
            raise

    def _sub_loop(self, sub, on_msg):
        """逐帧解码并回调；单帧损坏不影响后续消息，套接字始终在本线程关闭。"""
        # 跨线程关闭 ZMQ 套接字可能触发 Windows signaler 断言并退出进程。
        try:
            poller = self._zmq.Poller()
            poller.register(sub, self._zmq.POLLIN)
            while self._running:
                try:
                    events = dict(poller.poll(200))
                except Exception:
                    break
                if sub not in events:
                    continue
                try:
                    frames = sub.recv_multipart(self._zmq.NOBLOCK)
                except Exception:
                    continue
                if len(frames) < 2:
                    continue
                topic = frames[0].decode("utf-8", errors="ignore")
                try:
                    data = decode_push_payload(frames[-1])
                except Exception as exc:
                    print("%s invalid push payload: %s" % (self.print_prefix, exc))
                    continue
                payload_data = data.get("data") if isinstance(data, dict) else data
                try:
                    on_msg(topic, payload_data)
                except Exception as exc:
                    print("%s subscriber callback failed: %s" % (self.print_prefix, exc))
        except Exception as exc:
            print("%s zmq subscriber failed: %s" % (self.print_prefix, exc))
        finally:
            self._running = False
            try:
                sub.close(linger=0)
            except Exception:
                pass
            if self._sub is sub:
                self._sub = None

    def stop(self):
        """请求停止并释放发布资源，订阅套接字由消费线程自行关闭。"""
        # 回调可主动退订；当前线程不得等待自己结束。
        self._running = False
        thread = self._sub_thread
        if thread is not None and thread is not threading.current_thread() and thread.is_alive():
            thread.join(1.0)
        self._sub_thread = None
        self._sub = None
        # The PUB socket is only touched by publisher threads under _pub_lock;
        # null it first so a racing publish() sees None and bails, then close.
        with self._pub_lock:
            pub = self._pub
            self._pub = None
        if pub is not None:
            try:
                pub.close(linger=0)
            except Exception:
                pass


class RedisQuotePushChannel(QuotePushChannel):
    def __init__(self, redis_client, account_id="", channel_template="bigqmt:quote_push:{account_id}:{topic}", print_prefix="[bigqmt_quote_push]"):
        self.redis = redis_client
        self.account_id = str(account_id or "")
        self.channel_template = channel_template
        self.print_prefix = print_prefix
        self._running = False
        self._pubsub = None
        self._thread = None

    def _channel(self, topic):
        return self.channel_template.format(account_id=self.account_id, topic=topic)

    # -- server side ---------------------------------------------------------
    def start_publisher(self):
        # Redis publish needs no setup; present for interface symmetry.
        self._running = True

    def publish(self, topic, data):
        payload = encode_push_payload({"combo_key": topic, "data": data})
        try:
            self.redis.publish(self._channel(topic), payload)
        except Exception as exc:
            print("%s redis publish failed: %s" % (self.print_prefix, exc))

    # -- client side ---------------------------------------------------------
    def start_subscriber(self, topics, on_msg):
        """启动拥有 Redis 订阅连接的线程，线程创建失败时恢复停止状态。"""
        self._thread = threading.Thread(
            target=self._sub_loop, args=(list(topics or []), on_msg), name="bigqmt-quote-push-sub", daemon=True
        )
        self._running = True
        try:
            self._thread.start()
        except Exception:
            self._running = False
            self._thread = None
            raise

    def _sub_loop(self, topics, on_msg):
        """在本线程创建和释放订阅连接，隔离畸形消息并继续消费。"""
        # 创建连接、主题转换和 subscribe 都可能失败，统一纳入资源清理范围。
        pubsub = None
        try:
            pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
            self._pubsub = pubsub
            # 订阅是精确频道匹配，预先保留原始主题，避免冒号或自定义模板后缀被截断。
            topics_by_channel = {self._channel(topic): str(topic) for topic in topics}
            pubsub.subscribe(*topics_by_channel)
            while self._running:
                try:
                    message = pubsub.get_message(timeout=0.2)
                except Exception:
                    break
                if not message or message.get("type") != "message":
                    continue
                channel = message.get("channel")
                if isinstance(channel, bytes):
                    channel = channel.decode("utf-8", errors="ignore")
                topic = topics_by_channel.get(str(channel))
                if topic is None:
                    continue
                try:
                    data = decode_push_payload(message.get("data"))
                except Exception as exc:
                    print("%s invalid push payload: %s" % (self.print_prefix, exc))
                    continue
                payload_data = data.get("data") if isinstance(data, dict) else data
                try:
                    on_msg(topic, payload_data)
                except Exception as exc:
                    print("%s subscriber callback failed: %s" % (self.print_prefix, exc))
        except Exception as exc:
            print("%s redis subscribe failed: %s" % (self.print_prefix, exc))
        finally:
            self._running = False
            try:
                if pubsub is not None:
                    pubsub.close()
            except Exception:
                pass
            if self._pubsub is pubsub:
                self._pubsub = None

    def stop(self):
        """通知消费线程退出；允许消息回调在本线程内停止订阅。"""
        self._running = False
        thread = self._thread
        if thread is not None and thread is not threading.current_thread() and thread.is_alive():
            thread.join(1.0)
        self._thread = None
        self._pubsub = None
