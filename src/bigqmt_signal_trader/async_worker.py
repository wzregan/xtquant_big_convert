"""客户端异步 RPC 的惰性串行工作队列。"""

import logging
import queue
import threading
import time


class AsyncWorker:
    """保持提交顺序；关闭时执行完已接收的任务，不丢弃已入队的交易操作。"""

    def __init__(self, name):
        """创建未启动的工作队列，name 用于线程及错误日志标识。"""
        self.name = name
        self.queue = queue.Queue()
        self.thread = None
        self._lock = threading.Lock()
        self._closed = False

    def submit(self, function, *args):
        """提交回调及参数；关闭后拒绝新任务，首次提交时才创建线程。"""
        with self._lock:
            if self._closed:
                raise RuntimeError("async worker is stopped: %s" % self.name)
            if self.thread is None:
                self.thread = threading.Thread(target=self._run, name=self.name, daemon=True)
                self.thread.start()
            self.queue.put((function, args))

    def _run(self):
        """串行执行任务；用户回调出错不影响后续请求，完成计数包含回调执行。"""
        while True:
            task = self.queue.get()
            try:
                if task is None:
                    return
                function, args = task
                function(*args)
            except Exception:
                logging.getLogger(__name__).exception("async task failed: %s", self.name)
            finally:
                self.queue.task_done()

    def wait(self, timeout=10.0):
        """等待已入队任务及回调结束；timeout 为秒，超时或在自身回调中调用返回 False。"""
        if self.thread is threading.current_thread():
            return False
        deadline = time.monotonic() + max(0.0, float(timeout))
        with self.queue.all_tasks_done:
            while self.queue.unfinished_tasks:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.queue.all_tasks_done.wait(remaining)
        return True

    def close(self, timeout=1.0):
        """停止接收任务，等待最多 timeout 秒；超时后工作线程继续处理已接收任务并退出。"""
        with self._lock:
            if not self._closed:
                self._closed = True
                if self.thread is not None:
                    self.queue.put(None)
            thread = self.thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(max(0.0, float(timeout)))
