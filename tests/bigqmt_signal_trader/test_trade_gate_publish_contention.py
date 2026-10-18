"""门禁等发布完成时不反复抢共享锁，截止时间与真实故障仍保持失败关闭。"""

import contextlib
import tempfile
import threading
import time
import uuid
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import bigqmt_signal_trader_strategy as strategy
from bigqmt_signal_trader.quote_subscription_manager import BufferedAcknowledgedPublisher
from bigqmt_signal_trader.shm_push_channel import SharedMemoryQuotePushChannel, PushCallbackError, PushLockBusyError
from bigqmt_signal_trader.sdk_contract import ExecutionRejected
from bigqmt_signal_trader.transports.shm_primitives import cleanup


def _real_contention_scenario(install, label):
    """调度只控制线程何时争真实锁；发布、超时、门禁和消息提交均为生产逻辑。"""
    with tempfile.TemporaryDirectory(prefix="shm-gate-contention-") as directory:
        settings = {"directory": directory, "namespace": uuid.uuid4().hex,
                    "market_push_bytes": 65536, "trade_push_bytes": 65536,
                    "market_push_slots": 16, "trade_push_slots": 16, "max_push_subscribers": 2}
        raw = SharedMemoryQuotePushChannel("server", settings, "isolated-test-account")
        publisher = BufferedAcknowledgedPublisher(raw)
        publisher.start_publisher()
        original_locked = raw._region.locked
        original_publish = raw.publish
        original_health = raw.check_health
        first_publish_failed = threading.Event()
        external_held, external_release = threading.Event(), threading.Event()
        gate_active = threading.Event()
        first_health_finished = threading.Event()
        sender_rescheduled = threading.Event()
        scheduling = threading.Event()
        scheduling.set()
        health_calls, health_locks = [], []
        timeline = []
        holders = []

        @contextlib.contextmanager
        def scheduled_locked(timeout_seconds=5.0):
            """模拟健康读取持锁期间被调度延迟；同一调度规则同时用于 A 和 B。"""
            name = threading.current_thread().name
            sender = name == "bigqmt-shm-publisher"
            gate = name == "controlled-gate"
            if (sender and first_publish_failed.is_set() and scheduling.is_set()
                    and not sender_rescheduled.is_set()):
                # 首次健康读结束后，调度器只在连续 6ms 无读锁活动时安排发送线程。
                # 持续每 2ms 重查会饿死发送者；仅等待本地队首状态时能够让出该窗口。
                assert first_health_finished.wait(2)
                while scheduling.is_set():
                    while gate_active.is_set() and scheduling.is_set():
                        time.sleep(0.0005)
                    if not gate_active.wait(0.006):
                        # 只注入一次抢占调度；后续快照/复核/发布不重复收取人工等待。
                        sender_rescheduled.set()
                        break
            with original_locked(timeout_seconds) as abandoned:
                if gate:
                    health_locks.append(time.monotonic())
                    gate_active.set()
                    # 两段元数据锁各延迟 4ms，完整核验仍小于原来的 20ms 预算。
                    # 不能像旧夹具那样强制持锁 28ms，却要求严格预算实现必须放行。
                    time.sleep(0.004)
                try:
                    yield abandoned
                finally:
                    if gate:
                        gate_active.clear()

        def observed_health():
            """以完整健康操作计数，不把快照锁和复核锁误认为两次门禁重查。"""
            gate = threading.current_thread().name == "controlled-gate"
            if gate:
                health_calls.append(time.monotonic())
            try:
                return original_health()
            finally:
                if gate:
                    first_health_finished.set()

        def hold_first_publish():
            """独立线程持有真实锁，确保首次发布耗尽原来的 20ms 预算。"""
            with original_locked(1):
                external_held.set()
                assert external_release.wait(2)

        def observed_publish(*args, **kwargs):
            """第一次真实发布被锁阻塞，后续原样提交同一消息编号。"""
            if not first_publish_failed.is_set():
                holder = threading.Thread(target=hold_first_publish, name="initial-lock-holder")
                holders.append(holder)
                holder.start()
                assert external_held.wait(1)
                try:
                    return original_publish(*args, **kwargs)
                except Exception:
                    first_publish_failed.set()
                    raise
            result = original_publish(*args, **kwargs)
            timeline.append(("committed", time.monotonic()))
            return result

        raw._region.locked = scheduled_locked
        raw.publish = observed_publish
        raw.check_health = observed_health
        executed = []

        def execute(method, params=None):
            """记录正式门禁后是否执行，必须已存在真实共享提交事实。"""
            assert publisher.status()["shared_committed"] == 1
            executed.append(method)
            timeline.append(("executed", time.monotonic()))
            return {"ok": True}

        handlers = SimpleNamespace(handle=execute, _canonical_method=lambda method: method)
        install(handlers, publisher)
        result = {"case": label}

        def invoke_gate():
            """保留真实门禁返回和异常，不自动重试原生委托。"""
            start = time.monotonic()
            try:
                handlers.handle("submit_order", {})
                result["allowed"] = True
            except ExecutionRejected as error:
                result.update(allowed=False, error=str(error))
            finally:
                result["gate_seconds"] = time.monotonic() - start
                result["gate_exit_status"] = publisher.status()

        try:
            publisher.publish("exec:isolated-test-account", {"order_id": "pending-original"})
            deadline = time.monotonic() + 1
            while not publisher.status()["publish_lock_busy"]:
                assert time.monotonic() < deadline
                time.sleep(0.0005)
            external_release.set()
            for holder in holders:
                holder.join(1)
            gate_thread = threading.Thread(target=invoke_gate, name="controlled-gate")
            gate_thread.start()
            gate_thread.join(2)
            assert not gate_thread.is_alive()
            result.update(health_checks=len(health_calls), health_lock_acquisitions=len(health_locks), executions=executed,
                          timeline=timeline)
            return result
        finally:
            scheduling.clear()
            first_health_finished.set()
            external_release.set()
            raw._region.locked = original_locked
            publisher.stop()
            for name in (raw._name, raw._space_name):
                cleanup(name, directory)

def test_real_shared_lock_contention_allows_sender_to_commit_before_order():
    """真实共享介质受控争锁时只检查两次，原队首先提交才执行新委托。"""
    result = _real_contention_scenario(strategy._install_acknowledged_push_gate, "production")
    assert result["allowed"] is True
    assert result["health_checks"] == 2
    assert result["executions"] == ["submit_order"]
    assert result["gate_exit_status"]["shared_committed"] == 1
    assert result["timeline"][0][0] == "committed"
    assert result["timeline"][1][0] == "executed"


def _pending_gate(monkeypatch, on_sleep):
    """建立真实未提交队首；仅共享介质故障与单调时钟由测试精确控制。"""
    raw = Mock()
    raw.publish.side_effect = PushLockBusyError("original record pending")
    publisher = BufferedAcknowledgedPublisher(raw, {
        "push_background_sender": False, "local_exec_queue_items": 2,
        "local_queue_high_water": 1.0,
    })
    publisher.publish("exec:account", {"order_id": "original"})
    assert publisher.flush() == 0
    assert publisher.status()["publish_lock_busy"]
    execute = Mock(return_value={"ok": True})
    handlers = SimpleNamespace(handle=execute, _canonical_method=lambda method: method)
    strategy._install_acknowledged_push_gate(handlers, publisher)
    clock = [0.0]

    def sleep(seconds):
        """消费原等待预算后执行受控事件，不等待真实墙钟。"""
        clock[0] += seconds
        on_sleep(publisher, raw, clock)

    monkeypatch.setattr(strategy, "time", SimpleNamespace(monotonic=lambda: clock[0], sleep=sleep))
    return handlers, execute, publisher, raw, clock


@pytest.mark.parametrize("reason", ["closed", "sticky", "capacity", "high_water"])
def test_hard_reason_arriving_during_local_wait_blocks_immediately(monkeypatch, reason):
    """等待发布期间新增真实故障立即拒单，不因曾经健康检查成功而放行。"""
    def on_sleep(publisher, _raw, _clock):
        """在第一次本地等待后加入对应故障；高水位由真实第二条消息触发。"""
        if reason == "closed":
            publisher._closed = True
        elif reason == "sticky":
            publisher._sticky_error = "execution overflow"
        elif reason == "capacity":
            publisher._backpressure = True
        else:
            publisher.publish("exec:account", {"order_id": "another"})

    handlers, execute, publisher, raw, clock = _pending_gate(monkeypatch, on_sleep)
    with pytest.raises(ExecutionRejected):
        handlers.handle("submit_order", {})
    assert clock[0] == 0.002
    assert raw.check_health.call_count == 2
    assert publisher.status()["shared_committed"] == 0
    execute.assert_not_called()


def test_pending_record_times_out_without_repeated_shared_health_reads(monkeypatch):
    """未提交队首耗尽原 100ms 预算仍拒单，本地轮询不重抢共享锁。"""
    handlers, execute, publisher, raw, clock = _pending_gate(monkeypatch, lambda *_args: None)
    with pytest.raises(ExecutionRejected, match="original record pending"):
        handlers.handle("submit_order", {})
    assert clock[0] == pytest.approx(0.1)
    assert raw.check_health.call_count == 2
    assert publisher.status()["local_pending"]["exec"] == 1
    assert publisher.status()["shared_committed"] == 0
    execute.assert_not_called()


def test_commit_at_deadline_cannot_skip_final_health_validation(monkeypatch):
    """提交恰好发生在截止时刻仍保留阻断，不能用发布事实冒充最终健康检查。"""
    def on_sleep(publisher, raw, clock):
        """令原队首在最后时刻真实经过 flush 提交。"""
        clock[0] = 0.1
        raw.publish.side_effect = None
        publisher.flush()

    handlers, execute, publisher, raw, clock = _pending_gate(monkeypatch, on_sleep)
    with pytest.raises(ExecutionRejected):
        handlers.handle("submit_order", {})
    assert clock[0] == 0.1
    assert publisher.status()["shared_committed"] == 1
    assert raw.check_health.call_count == 3
    execute.assert_not_called()


def test_final_health_failure_after_commit_still_blocks_order(monkeypatch):
    """队首已提交也必须做最终共享健康检查，新出现的消费失败仍然拒单。"""
    def on_sleep(publisher, raw, _clock):
        """真实提交后才让下一次底层健康查询报告消费回调失败。"""
        raw.publish.side_effect = None
        publisher.flush()
        raw.check_health.side_effect = PushCallbackError("consumer callback failed after commit")

    handlers, execute, publisher, raw, _clock = _pending_gate(monkeypatch, on_sleep)
    with pytest.raises(ExecutionRejected, match="consumer callback failed after commit"):
        handlers.handle("submit_order", {})
    assert publisher.status()["shared_committed"] == 1
    assert raw.check_health.call_count == 4
    execute.assert_not_called()


@pytest.mark.parametrize("health_busy", [False, True])
def test_local_wait_polls_without_rechecking_until_real_commit(monkeypatch, health_busy):
    """多次本地轮询保持原队首；实际提交后恰好增加一次最终共享检查。"""
    polls = []

    def on_sleep(publisher, raw, _clock):
        """前三次轮询期间共享检查计数不增加，第三次执行真实发布。"""
        polls.append(raw.check_health.call_count)
        if len(polls) == 3:
            raw.publish.side_effect = None
            publisher.flush()
        # 两种 busy 可同时存在；发布完成后留下的健康 busy 必须靠最终真检查解除。
        publisher._health_busy = health_busy

    handlers, execute, publisher, raw, clock = _pending_gate(monkeypatch, on_sleep)
    handlers.handle("submit_order", {})
    assert polls == [2, 2, 2]
    assert clock[0] == 0.006
    assert raw.check_health.call_count == 4
    assert publisher.status()["shared_committed"] == 1
    execute.assert_called_once_with("submit_order", {})


def test_disabled_acknowledged_channel_reports_unhealthy_and_rejects_orders():
    """缺少可靠通道时 ping 明确返回故障，原生报单不能执行。"""
    execute = Mock(return_value={"ok": True})
    handlers = SimpleNamespace(handle=execute, _canonical_method=lambda method: method)
    strategy._install_acknowledged_push_gate(handlers, None)
    pong = handlers.handle("ping")
    assert pong["communication_health"]["new_orders_blocked"] is True
    execute.reset_mock()
    with pytest.raises(ExecutionRejected, match="acknowledged push is disabled"):
        handlers.handle("submit_order")
    execute.assert_not_called()


def test_hard_fault_arriving_with_retryable_probe_error_is_not_retried(monkeypatch):
    """健康探测报锁忙同时出现粘滞故障时，立即采用真实故障拒单。"""
    handlers, execute, publisher, raw, clock = _pending_gate(monkeypatch, lambda *_args: None)

    def check_health():
        """在真实缓冲状态中记录新硬故障，再抛出可重试的介质异常。"""
        publisher._sticky_error = "execution callback failed"
        raise PushLockBusyError("metadata busy")

    raw.check_health.side_effect = check_health
    with pytest.raises(ExecutionRejected, match="execution callback failed"):
        handlers.handle("submit_order")
    assert clock[0] == 0.0
    assert raw.check_health.call_count == 2
    execute.assert_not_called()


@pytest.mark.parametrize("reason", ["new_capacity_backpressure", "deadline_elapsed"])
def test_successful_probe_does_not_override_new_block_or_elapsed_budget(monkeypatch, reason):
    """成功的介质读取不能忽略读取期间新增的积压或已经耗尽的原预算。"""
    handlers, execute, publisher, raw, clock = _pending_gate(monkeypatch, lambda *_args: None)

    def check_health():
        """成功返回前精确引入状态变化或模拟读取线程被长时间调度延迟。"""
        if reason == "new_capacity_backpressure":
            publisher._backpressure = True
        else:
            clock[0] = 0.101
        return {}

    raw.check_health.side_effect = check_health
    with pytest.raises(ExecutionRejected):
        handlers.handle("submit_order")
    assert raw.check_health.call_count == 2
    execute.assert_not_called()
