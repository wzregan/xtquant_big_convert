"""客户端异步 RPC 的惰性工作队列，默认串行，可显式限制并发数。"""

import logging
import queue
import threading
import time


class AsyncWorker:
    """默认保持提交顺序；并发模式按完成顺序返回，关闭后仍处理已接收任务。"""

    def __init__(self, name, max_workers=1):
        """创建惰性队列；max_workers 为并行执行上限，默认一条线程保留串行语义。"""
        if isinstance(max_workers, bool) or not isinstance(max_workers, int) or max_workers < 1:
            raise ValueError("max_workers must be a positive integer")
        self.name = name
        self.max_workers = max_workers
        self.queue = queue.Queue()
        self.thread = None
        self._threads = []
        self._lock = threading.Lock()
        self._closed = False

    def submit(self, function, *args):
        """提交回调及参数；关闭后拒绝新任务，线程数量不超过配置上限。"""
        with self._lock:
            if self._closed:
                raise RuntimeError("async worker is stopped: %s" % self.name)
            if len(self._threads) < self.max_workers:
                thread = threading.Thread(target=self._run,
                                          name="%s-%d" % (self.name, len(self._threads)), daemon=True)
                thread.start()
                self._threads.append(thread)
                if self.thread is None:
                    self.thread = thread
            self.queue.put((function, args))

    def _run(self):
        """取出并执行任务；回调异常不影响后续请求，完成计数包含回调执行。"""
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
        with self._lock:
            if threading.current_thread() in self._threads:
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
                # 哨兵排在已接收任务后面；每条线程各取一个，不能让其他线程永久等待。
                for unused in self._threads:
                    self.queue.put(None)
            threads = list(self._threads)
        deadline = time.monotonic() + max(0.0, float(timeout))
        for thread in threads:
            if thread is not threading.current_thread():
                thread.join(max(0.0, deadline - time.monotonic()))
