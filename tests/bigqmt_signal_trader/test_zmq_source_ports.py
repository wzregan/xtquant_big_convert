"""验证固定源端口的跨线程、跨进程租约与真实 TCP 收发，不连接 QMT。"""

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import zmq

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from bigqmt_signal_trader.redis_rpc import decode_rpc_request_payload, encode_rpc_request_payload
from bigqmt_signal_trader.transports.backend import get_backend
from bigqmt_signal_trader.transports.source_ports import (
    SourcePortLease, SourcePortUnavailable, connect_client_socket, normalize_source_ports,
)
from bigqmt_signal_trader.transports.zmq_transport import ZmqTransport


def available_ports(count):
    """通过系统分配互不重复的空闲端口供隔离测试使用。"""
    sockets = []
    try:
        for _ in range(count):
            item = socket.socket()
            item.bind(("127.0.0.1", 0))
            sockets.append(item)
        return [item.getsockname()[1] for item in sockets]
    finally:
        for item in sockets:
            item.close()


class SourcePortsTests(unittest.TestCase):
    """每项测试独占本地上下文与临时租约目录，结束时完整回收。"""

    def setUp(self):
        """准备本进程的 TCP 接收端和独立 ZMQ 上下文。"""
        self.directory = tempfile.TemporaryDirectory()
        self.context = zmq.Context()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(16)
        self.listener.settimeout(2)
        self.endpoint = "tcp://127.0.0.1:%s" % self.listener.getsockname()[1]
        self.resources = []

    def tearDown(self):
        """先关闭套接字，再关闭租约和上下文，避免残留线程或文件锁。"""
        for item, lease in reversed(self.resources):
            item.close(linger=0)
            if lease is not None:
                lease.close()
        self.listener.close()
        self.context.term()
        self.directory.cleanup()

    def factory(self):
        """创建只供本次测试连接使用的 DEALER。"""
        item = self.context.socket(zmq.DEALER)
        item.setsockopt(zmq.LINGER, 0)
        return item

    def connect(self, ports, endpoint=None, timeout=1.0):
        """建立测试连接并登记资源所有权。"""
        result = connect_client_socket(self.factory, endpoint or self.endpoint, ports,
                                       self.directory.name, timeout)
        self.resources.append(result)
        return result

    def test_real_tcp_peer_has_the_leased_source_port(self):
        """TCP 对端观察到的源端口必须等于租约端口。"""
        port = available_ports(1)[0]
        _, lease = self.connect([port])
        accepted, address = self.listener.accept()
        accepted.close()
        self.assertEqual(address, ("127.0.0.1", port))
        self.assertEqual(lease.port, port)

    def test_occupied_os_port_is_skipped_without_random_fallback(self):
        """库外占用端口时，只能尝试配置中的下一个端口。"""
        occupied = socket.socket()
        if os.name == "nt":
            occupied.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        occupied.bind(("127.0.0.1", 0))
        occupied.listen(1)
        port = available_ports(1)[0]
        try:
            _, lease = self.connect([occupied.getsockname()[1], port])
            accepted, address = self.listener.accept()
            accepted.close()
            self.assertEqual(address[1], port)
            self.assertEqual(lease.port, port)
        finally:
            occupied.close()

    def test_exhausted_pool_fails_without_allocating_a_socket(self):
        """所有端口均有租约时立即失败，不创建使用随机端口的连接。"""
        port = available_ports(1)[0]
        lease = SourcePortLease.try_acquire(port, self.directory.name)
        try:
            with patch.object(self, "factory", side_effect=AssertionError("unexpected socket")):
                with self.assertRaises(SourcePortUnavailable):
                    self.connect([port])
        finally:
            lease.close()

    def test_connection_failure_respects_budget_and_releases_lease(self):
        """总建连预算到期后，失败尝试不得残留端口租约。"""
        destination, source = available_ports(2)
        started = time.monotonic()
        with self.assertRaises(SourcePortUnavailable):
            self.connect([source], endpoint="tcp://127.0.0.1:%s" % destination, timeout=0.1)
        self.assertLess(time.monotonic() - started, 0.7)
        lease = SourcePortLease.try_acquire(source, self.directory.name)
        self.assertIsNotNone(lease)
        lease.close()

    def test_default_configuration_does_not_create_a_port_lease(self):
        """未启用源端口池时保持原连接路径，不访问租约目录。"""
        with patch.object(SourcePortLease, "try_acquire", side_effect=AssertionError("unexpected lease")):
            _, lease = self.connect(None)
        accepted, _ = self.listener.accept()
        accepted.close()
        self.assertIsNone(lease)

    def test_invalid_ports_and_non_loopback_endpoints_are_rejected(self):
        """拒绝歧义配置及源端口二次拼接，不能悄悄扩大连接范围。"""
        for ports in ([True], [0], [65536], ["50060"], "50060"):
            with self.subTest(ports=ports), self.assertRaises(ValueError):
                normalize_source_ports(ports)
        port = available_ports(1)[0]
        for endpoint in ("tcp://192.168.100.190:50060", "inproc://test",
                         "tcp://127.0.0.1:50068;127.0.0.1:50060"):
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                self.connect([port], endpoint=endpoint)

    def test_process_death_releases_the_port_lease(self):
        """子进程被终止后，操作系统必须释放其文件锁，无需删除文件。"""
        port = available_ports(1)[0]
        code = (
            "import sys,time;from bigqmt_signal_trader.transports.source_ports import SourcePortLease;"
            "lease=SourcePortLease.try_acquire(int(sys.argv[1]),sys.argv[2]);"
            "print('READY' if lease is not None else 'BUSY',flush=True);time.sleep(30)"
        )
        env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
        child = subprocess.Popen([sys.executable, "-c", code, str(port), self.directory.name],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        try:
            self.assertEqual(child.stdout.readline().strip(), b"READY")
            self.assertIsNone(SourcePortLease.try_acquire(port, self.directory.name))
        finally:
            child.kill()
            child.communicate(timeout=5)
        lease = SourcePortLease.try_acquire(port, self.directory.name)
        self.assertIsNotNone(lease)
        lease.close()

    def test_dead_thread_is_reaped_before_requesting_another_lease(self):
        """只有一个端口时，已退出线程的租约必须在新线程申请前回收。"""
        port = available_ports(1)[0]
        client = ZmqTransport(connect_address=self.endpoint, client_source_ports=[port],
                              client_source_lock_dir=self.directory.name)
        errors = []

        def connect_once():
            """在线程中建立连接并保留句柄供传输层回收。"""
            try:
                client._ensure_dealer()
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=connect_once)
        thread.start()
        thread.join(3)
        accepted, _ = self.listener.accept()
        accepted.close()
        try:
            self.assertEqual(errors, [])
            self.assertFalse(thread.is_alive())
            client._ensure_dealer()
            accepted, address = self.listener.accept()
            accepted.close()
            self.assertEqual(address[1], port)
        finally:
            client.stop()

    def test_push_factory_propagates_ports_and_releases_subscriber_lease(self):
        """实际推送经后端工厂创建，停止订阅后归还端口。"""
        publisher = self.context.socket(zmq.PUB)
        port = publisher.bind_to_random_port("tcp://127.0.0.1")
        source = available_ports(1)[0]
        client = get_backend("zmq").build_push_channel("client", config={"zmq": {
            "quote_push_connect_address": "tcp://127.0.0.1:%s" % port,
            "client_source_ports": [source], "client_source_lock_dir": self.directory.name,
        }})
        delivered = threading.Event()

        def on_message(topic, data):
            """只确认接收到的本地测试消息。"""
            if topic == "probe" and data == {"value": 1}:
                delivered.set()

        try:
            client.start_subscriber(["probe"], on_message)
            self.assertIsNone(SourcePortLease.try_acquire(source, self.directory.name))
            from bigqmt_signal_trader.quote_push_channel import encode_push_payload
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and not delivered.is_set():
                publisher.send_multipart([b"probe", encode_push_payload({"data": {"value": 1}})])
                delivered.wait(0.02)
            self.assertTrue(delivered.is_set())
        finally:
            client.stop()
            publisher.close(linger=0)
        lease = SourcePortLease.try_acquire(source, self.directory.name)
        self.assertIsNotNone(lease)
        lease.close()

    def test_rpc_threads_keep_independent_ports_and_correlated_replies(self):
        """多个 RPC 线程并发收发，不能共用源端口或串错响应。"""
        sources = available_ports(4)
        ready = threading.Event()
        stopping = threading.Event()
        endpoint = []
        errors = []

        def serve():
            """以本地 ROUTER 回送关联响应，完全不调用业务网关。"""
            router = self.context.socket(zmq.ROUTER)
            try:
                port = router.bind_to_random_port("tcp://127.0.0.1")
                endpoint.append("tcp://127.0.0.1:%s" % port)
                ready.set()
                while not stopping.is_set():
                    if router.poll(50):
                        identity, payload = router.recv_multipart()
                        request = json.loads(decode_rpc_request_payload(payload.decode("utf-8")))
                        response = {"request_id": request["request_id"], "ok": True,
                                    "data": request["params"]}
                        router.send_multipart([identity, encode_rpc_request_payload(response).encode("utf-8")])
            except Exception as error:
                errors.append(error)
            finally:
                router.close(linger=0)

        server = threading.Thread(target=serve)
        server.start()
        self.assertTrue(ready.wait(3))
        client = ZmqTransport(connect_address=endpoint[0], client_source_ports=sources,
                              client_source_lock_dir=self.directory.name)
        barrier = threading.Barrier(4)
        observed = []

        def query(index):
            """维持线程存活至全部完成，验证四个同时持有的独立租约。"""
            try:
                barrier.wait(timeout=3)
                result = client.send_request({"method": "ping", "params": {"index": index}}, 2)
                self.assertEqual(result["data"], {"index": index})
                dealer = client._ensure_dealer()
                observed.append(client._dealer_source_leases[dealer].port)
                barrier.wait(timeout=3)
            except Exception as error:
                errors.append(error)

        workers = [threading.Thread(target=query, args=(index,)) for index in range(4)]
        try:
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(5)
            self.assertTrue(all(not worker.is_alive() for worker in workers))
            self.assertEqual(errors, [])
            self.assertEqual(set(observed), set(sources))
        finally:
            client.stop()
            stopping.set()
            server.join(3)
        for port in sources:
            lease = SourcePortLease.try_acquire(port, self.directory.name)
            self.assertIsNotNone(lease)
            lease.close()


if __name__ == "__main__":
    unittest.main()
