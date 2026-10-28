"""为回环 ZMQ 客户端分配受限源端口，文件租约在进程退出时自动释放。"""

import errno
import math
import os
from pathlib import Path
import tempfile
import time

from .base import TransportError


class SourcePortUnavailable(TransportError):
    """配置端口不足、被占用或连接失败，调用方不得退回随机源端口。"""


def normalize_source_ports(ports):
    """校验并去重源端口列表；None 和空列表表示沿用系统分配行为。"""
    if ports is None:
        return ()
    if not isinstance(ports, (list, tuple)) or len(ports) > 256:
        raise ValueError("client_source_ports must be a list of at most 256 ports")
    result = []
    for port in ports:
        if isinstance(port, bool) or not isinstance(port, int) or not 1024 <= port <= 65535:
            raise ValueError("client_source_ports requires integer ports in 1024..65535")
        if port not in result:
            result.append(port)
    return tuple(result)


class SourcePortLease(object):
    """持有一个跨进程端口租约；所属套接字关闭后由调用方关闭租约。"""

    def __init__(self, port, handle):
        """接管已加锁文件句柄；handle 的生命周期覆盖对应套接字。"""
        self.port = port
        self._handle = handle

    @classmethod
    def try_acquire(cls, port, lock_dir=None):
        """非阻塞申请端口，已被租用则返回 None。

        lock_dir 是本机所有客户端进程共用的目录；缺省为当前用户临时目录。
        租约只避免本库客户端之间冲突，实际端口是否可用仍以 TCP 连接结果为准。
        """
        directory = Path(lock_dir) if lock_dir else Path(tempfile.gettempdir()) / "bigqmt-source-ports"
        directory.mkdir(parents=True, exist_ok=True)
        handle = (directory / ("%s.lock" % port)).open("a+b")
        try:
            if handle.seek(0, os.SEEK_END) == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            handle.close()
            if error.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                return None
            raise
        except BaseException:
            handle.close()
            raise
        return cls(port, handle)

    def close(self):
        """幂等释放文件锁；正常退出及进程被终止也由操作系统回收文件锁。"""
        handle, self._handle = self._handle, None
        if handle is not None:
            handle.close()


def connect_client_socket(socket_factory, endpoint, ports=None, lock_dir=None, timeout_seconds=1.0):
    """创建并连接客户端套接字，返回套接字与由调用方管理的端口租约。

    socket_factory 返回当前线程拥有、尚未连接的 ZMQ 套接字，负责设置身份和选项。
    ports 非空时只允许连接 127.0.0.1，并等待 TCP 建连事件验证源端口绑定成功。
    timeout_seconds 是遍历整个端口池的总秒数；失败尝试的套接字及租约在返回前释放。
    """
    ports = normalize_source_ports(ports)
    if not ports:
        socket = socket_factory()
        try:
            socket.connect(endpoint)
        except BaseException:
            socket.close(linger=0)
            raise
        return socket, None
    prefix = "tcp://127.0.0.1:"
    destination = endpoint[len(prefix):] if isinstance(endpoint, str) and endpoint.startswith(prefix) else ""
    if not destination or any(char not in "0123456789" for char in destination) or not 1024 <= int(destination) <= 65535:
        raise ValueError("fixed source ports require a plain tcp://127.0.0.1 endpoint")
    timeout = float(timeout_seconds)
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("source port connection timeout must be finite and positive")
    import zmq
    from zmq.utils.monitor import recv_monitor_message

    deadline = time.monotonic() + timeout
    last_error = "all configured ports are leased"
    for port in ports:
        if time.monotonic() >= deadline:
            break
        if port == int(destination):
            continue
        lease = SourcePortLease.try_acquire(port, lock_dir)
        if lease is None:
            continue
        socket = monitor = None
        connected = False
        try:
            socket = socket_factory()
            events = zmq.EVENT_CONNECTED | zmq.EVENT_CONNECT_RETRIED | zmq.EVENT_BIND_FAILED
            monitor = socket.get_monitor_socket(events=events)
            address = "tcp://127.0.0.1:%s;127.0.0.1:%s" % (port, destination)
            socket.connect(address)
            attempt_deadline = min(deadline, time.monotonic() + 0.2)
            while time.monotonic() < attempt_deadline:
                remaining = attempt_deadline - time.monotonic()
                if not monitor.poll(max(1, int(remaining * 1000))):
                    continue
                event = recv_monitor_message(monitor)
                if event["event"] == zmq.EVENT_CONNECTED:
                    connected = True
                    return socket, lease
                last_error = "source port %s could not connect (event=%s)" % (port, event["event"])
                break
            else:
                last_error = "source port %s connection did not complete" % port
        finally:
            if socket is not None and monitor is not None:
                try:
                    socket.disable_monitor()
                except zmq.ZMQError:
                    # 上层可能已终止 context；仍须回收监测端和失败尝试持有的租约。
                    pass
            if monitor is not None:
                monitor.close(linger=0)
            if not connected:
                if socket is not None:
                    socket.close(linger=0)
                lease.close()
    raise SourcePortUnavailable("no configured source port connected: %s" % last_error)
