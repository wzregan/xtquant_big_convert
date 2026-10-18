"""标准 xtquant 导入入口的交易契约；复用桥接查询及事件实现。"""

import math
import threading
import time
from concurrent.futures import Future

from .sdk_contract import check_execution_context
from .sdk_order_ids import OrderIdRegistry, trading_date
from .xtquant_compat import (
    BigQmtXtTrader,
    CompatObject,
    RpcServerRepliedError,
    _AcknowledgedExecutionEvent,
    _account_id,
    log,
)


class XtQuantTrader(BigQmtXtTrader):
    """对外提供普通整数委托号及真实账户状态，保留直接桥接入口的旧行为。"""

    def __init__(self, path=None, session_id=None, callback=None, **kwargs):
        """path/session_id 保持 SDK 签名；映射库由 BIGQMT_ORDER_ID_DB 指定。"""
        super(XtQuantTrader, self).__init__(path=path, session_id=session_id, **kwargs)
        self.callback = callback
        self._registry_instance = None
        self._registry_lock = threading.Lock()
        self._health_stop = threading.Event()
        self._health_thread = None
        self._status_signature = None
        self._sdk_capabilities = frozenset()
        self._sdk_generation = None
        self._sdk_connected = False

    def _registry(self):
        """惰性打开持久化映射；连接及纯资产查询不会创建数据库。"""
        with self._registry_lock:
            if self._registry_instance is None:
                self._registry_instance = OrderIdRegistry()
            return self._registry_instance

    def connect(self):
        """确认桥接协议和账户，成功返回零；连接过程不执行自动部署同步。"""
        try:
            pong = self.client.call("ping")
            if not isinstance(pong, dict) or pong.get("pong") is not True:
                raise ConnectionError("invalid BigQMT ping")
            if str(pong.get("account_id") or "") != str(self.client.account_id or ""):
                raise ConnectionError("BigQMT account_id mismatch")
            self._note_server_account_type(pong)
            # 标准 SDK 覆盖了父类 connect，必须同步保存门禁，不能等待后台首次探测。
            self._note_communication_health(pong)
            self._sdk_capabilities = frozenset(pong.get("sdk_capabilities") or ())
            if "account_status_v1" not in self._sdk_capabilities:
                raise ConnectionError("bridge requires account_status_v1")
            self._sdk_generation = pong.get("generation")
            self._sdk_connected = True
            self._publish_statuses(self.query_account_status())
        except Exception:
            self._sdk_connected = False
            log.exception("BigQMT SDK connect failed")
            return -1
        self._start_health_monitor()
        return 0

    def subscribe(self, account):
        """订阅已连接账户；类型不匹配时拒绝，避免用错误账户类型交易。"""
        if not self._sdk_connected or _account_id(account) != self.client.account_id:
            return -1
        try:
            statuses = self.query_account_status(account)
            if len(statuses) != 1 or statuses[0].account_type != account.account_type:
                return -1
            self._start_event_listener()
            self._publish_statuses(statuses)
            return 0
        except Exception:
            log.exception("BigQMT SDK subscribe failed")
            return -1

    def query_account_status(self, account=None):
        """查询真实账户状态；等待预算遵循客户端配置且最多五秒，异常不伪装为正常。"""
        account_id = _account_id(account, self.client.account_id)
        rows = self.client.call(
            "query_account_status",
            {"account_id": account_id},
            account_id=account_id,
            timeout_seconds=self._account_status_timeout_seconds(),
        )
        if not isinstance(rows, list) or len(rows) != 1:
            raise RuntimeError("query_account_status requires exactly one account")
        row = rows[0]
        if not isinstance(row, dict) or row.get("account_id") != account_id:
            raise RuntimeError("account status identity mismatch")
        if (
            type(row.get("status")) is not int
            or type(row.get("account_type")) is not int
        ):
            raise RuntimeError("invalid account status types")
        observed = row.get("observed_at")
        if (
            not isinstance(observed, (int, float))
            or not -2 <= time.time() - observed <= 5
        ):
            raise RuntimeError("account status is stale")
        if (
            self._sdk_generation is not None
            and row.get("generation") != self._sdk_generation
        ):
            raise ConnectionError("BigQMT deployment generation changed")
        return [CompatObject(**row)]

    def _account_status_timeout_seconds(self):
        """允许 QMT 历史查询占用策略线程，同时将断连探测与停止等待限制在五秒内。"""
        # 实机 100 证券历史首读约需 3.7 秒，固定两秒会把正常排队误判为断连。
        # 上限五秒保留故障探测边界，也为 qmt-grpc 默认八秒的停止调用预留清理时间。
        return min(self.client.timeout_seconds, 5.0)

    def _fire_account_status(self):
        """以真实查询驱动状态事件，不再从传输连通推断登录成功。"""
        self._publish_statuses(self.query_account_status())

    def _publish_statuses(self, statuses):
        """仅在账户状态变化时通知，避免每次健康查询触发业务重对账。"""
        for status in statuses:
            signature = (status.account_id, status.account_type, status.status)
            if signature != self._status_signature:
                self._status_signature = signature
                if self.callback is not None:
                    try:
                        self.callback.on_account_status(status)
                    except Exception:
                        log.exception("user callback failed: on_account_status")

    def _start_health_monitor(self):
        """启动账户状态监视；保留未退出的线程句柄，防止重复监视器。"""
        if self._health_thread is not None and self._health_thread.is_alive():
            return
        self._health_stop.clear()
        self._health_thread = threading.Thread(
            target=self._health_loop, name="bigqmt-sdk-account", daemon=True
        )
        self._health_thread.start()

    def _health_loop(self):
        """账户查询失败或服务端换代时发断连事件，由调用方重新连接并对账。"""
        while not self._health_stop.wait(1.0):
            try:
                statuses = self.query_account_status()
                if not self._health_stop.is_set():
                    self._publish_statuses(statuses)
            except Exception:
                self._sdk_connected = False
                if not self._health_stop.is_set():
                    log.warning("账户状态查询失败，断开 SDK 连接", exc_info=True)
                    if self.callback is not None:
                        try:
                            self.callback.on_disconnected()
                        except Exception:
                            log.exception("user callback failed: on_disconnected")
                return

    def stop(self):
        """停止健康检查和事件线程；尚未退出时显式失败，禁止重复实例接管。"""
        self._health_stop.set()
        self._sdk_connected = False
        pool = getattr(self.client, "_rpc_async_pool", None)
        if pool is not None:
            # 等待已发 RPC 完成后才能关闭共享传输，避免停机截断撤单结果。
            pool.shutdown(wait=True)
            self.client._rpc_async_pool = None
        result = super(XtQuantTrader, self).stop()
        thread = self._health_thread
        if thread is not None and thread is not threading.current_thread():
            # 在途查询由其所有者线程结束后才关闭 socket，避免跨线程关闭仍在 poll 的 ZMQ。
            thread.join(self._account_status_timeout_seconds() + 0.5)
            if thread.is_alive():
                raise RuntimeError("BigQMT account monitor did not stop")
        if self._event_thread is not None and self._event_thread.is_alive():
            raise RuntimeError("BigQMT event listener did not stop")
        transport = getattr(self.client, "_transport_instance", None)
        stop = getattr(transport, "stop", None)
        if callable(stop):
            stop()
            self.client._transport_instance = None
        return result

    def _redis_cache_enabled(self):
        """SDK 账户查询必须返回本次事实，不能用断连前的 Redis 账户快照兜底。"""
        return False

    def query_stock_asset(self, account):
        """读取完整资产事实；缺失或非法金额不能变成零，响应账户必须与请求一致。"""
        account_id = _account_id(account, self.client.account_id)
        if account_id != self.client.account_id:
            raise ValueError("asset query account does not match connection")
        row = self.client.call(
            "query_stock_asset", {"account_id": account_id}, account_id=account_id
        )
        if not isinstance(row, dict) or row.get("account_id") != account_id:
            raise ValueError("asset response account_id mismatch")
        values = {}
        for field in ("cash", "frozen_cash", "market_value", "total_asset"):
            value = row.get(field)
            # 旧桥接入口的默认零会把缺失冻结额或非法资金字符串伪装成完整快照。
            if value is None or isinstance(value, bool):
                raise ValueError("missing or invalid asset field: " + field)
            number = float(value)
            if not math.isfinite(number):
                raise ValueError("non-finite asset field: " + field)
            values[field] = number
        return CompatObject(
            account_id=account_id,
            available_cash=values["cash"],
            m_strAccountID=account_id,
            m_dCash=values["cash"],
            m_dAvailableCash=values["cash"],
            m_dFrozenCash=values["frozen_cash"],
            m_dTotalAsset=values["total_asset"],
            m_dMarketValue=values["market_value"],
            **values,
        )

    def _deliver_event(self, event):
        """错误回调保留整数编号，并在回调完成或失败后释放确认型推送的等待。"""
        kind = event.get("event_type")
        if kind not in ("order_error", "cancel_error"):
            return super(XtQuantTrader, self)._deliver_event(event)
        if self.callback is None:
            return
        sysid = str(event.get("order_sys_id") or "")
        value = CompatObject(
            account_id=self.client.account_id,
            order_sysid=sysid,
            order_sys_id=sysid,
            order_id=self._order_object_id(sysid),
            error_id=event.get("error_id", 0),
            error_msg=str(event.get("error_msg") or ""),
            stock_code=str(event.get("stock_code") or ""),
            strategy_name=str(event.get("strategy_name") or ""),
            order_remark=str(
                event.get("order_remark")
                or event.get("remark")
                or event.get("user_order_id")
                or ""
            ),
        )
        try:
            handler = (
                self.callback.on_order_error
                if kind == "order_error"
                else self.callback.on_cancel_error
            )
            handler(value)
        except Exception as exc:
            log.exception("user callback failed: %s", kind)
            if isinstance(event, _AcknowledgedExecutionEvent):
                event.callback_error = exc
        finally:
            # 子类覆盖了错误回调分发，也必须履行基类的确认契约，不能吞掉失败后 ACK。
            if isinstance(event, _AcknowledgedExecutionEvent):
                event.completed.set()

    def _order_id(self, order_sys_id):
        """提交缺少合同号时结果未知；收到合同号后才生成可持久化的整数。"""
        sysid = str(order_sys_id or "")
        if not sysid or sysid == "-1":
            raise RuntimeError("submission outcome unknown: missing broker order_sysid")
        return self._registry().remember(self.client.account_id, trading_date(), sysid)

    def _order_object_id(self, order_sys_id):
        """查询和回调的编号保持普通整数，缺少合同号保留零值。"""
        return self._registry().remember(
            self.client.account_id, trading_date(), str(order_sys_id or "")
        )

    def _async_response_order_id(self, order_sys_id, user_order_id):
        """异步合同号尚未返回时保留零，不能把 remark 注册为另一张委托。"""
        return self._order_object_id(order_sys_id)

    def _order_rpc_params(self, params):
        """异步单笔和批量请求也携带执行契约；停止后未发送工作在远端过期。"""
        result = dict(params)
        context = dict(result.get("execution_context") or {})
        context.setdefault("version", 1)
        context.setdefault("generation", self._sdk_generation)
        if not self._sdk_connected:
            context["execute_before"] = 0
        result["execution_context"] = context
        return result

    def _order_from_dict(self, account_id, item):
        """查询与回调统一按账户、委托日期和原始合同号映射。"""
        result = super(XtQuantTrader, self)._order_from_dict(account_id, item)
        result.order_id = self._registry().remember(
            account_id, trading_date(result.order_time), result.order_sysid
        )
        return result

    def _trade_from_dict(self, account_id, item):
        """成交使用同一合同号映射；费用字段继续保留未知值。"""
        result = super(XtQuantTrader, self)._trade_from_dict(account_id, item)
        result.order_id = self._registry().remember(
            account_id, trading_date(result.traded_time), result.order_sysid
        )
        # 原兼容层把缺少费用填成零；账本必须区分「免佣」和「费用尚未返回」。
        result.commission = (
            None if item.get("commission") is None else float(item["commission"])
        )
        return result

    def _resolve_order_sys_id(self, value):
        """整数必须在映射库中存在；显式字符串保持柜台系统号语义。"""
        if isinstance(value, int):
            return self._registry().resolve(
                self.client.account_id, trading_date(), value
            )
        if not isinstance(value, str) or not value:
            raise ValueError("invalid order_sysid")
        return value

    def cancel_order_stock_deferred(self, account, order_id, timeout_seconds=None):
        """发送撤单并返回结果 Future，远端仍按原协议确认，不伪造已撤状态。

        account 必须属于当前已连接账户；order_id 为当日持久化映射编号。
        timeout_seconds 是远端 RPC 等待秒数；超时通过 Future 原样传播，不重发。
        """
        result = Future()
        result.set_running_or_notify_cancel()
        account_id = _account_id(account, self.client.account_id)
        if not self._sdk_connected or account_id != self.client.account_id:
            result.set_result(-1)
            return result
        try:
            # 在调用者线程解析编号；只有传输等待进入池，不能跨账户解析同号委托。
            order_sysid = self._resolve_order_sys_id(order_id)
            source = self.client.call_async(
                "cancel_order_stock_sysid",
                {"account_id": account_id, "market": "", "order_sysid": order_sysid},
                account_id=account_id,
                timeout_seconds=timeout_seconds,
            )
        except Exception as error:
            result.set_exception(error)
            return result

        def finish(done):
            """保留原同步撤单的返回语义；结果不明时不回落重发。"""
            try:
                value = done.result()
                if isinstance(value, dict):
                    value = 0 if value.get("success") else -1
                elif isinstance(value, bool):
                    value = 0 if value else -1
                else:
                    value = 0 if isinstance(value, int) and value == 0 else -1
                result.set_result(value)
            except Exception as error:
                result.set_exception(error)

        source.add_done_callback(finish)
        return result

    def order_stock(
        self,
        account,
        stock_code,
        order_type,
        order_volume,
        price_type,
        price,
        strategy_name="",
        order_remark="",
    ):
        """按 SDK 参数提交；无明确拒绝事实时以异常保留结果未知语义。"""
        return self.order_stock_with_context(
            account,
            stock_code,
            order_type,
            order_volume,
            price_type,
            price,
            strategy_name,
            order_remark,
        )

    def order_stock_with_context(
        self,
        account,
        stock_code,
        order_type,
        order_volume,
        price_type,
        price,
        strategy_name="",
        order_remark="",
        *,
        execute_before=None,
    ):
        """携带可选执行期限提交；execute_before 为 UTC Unix 秒，原生 SDK 无需实现此扩展。"""
        if (
            not self._sdk_connected
            or "execution_context_v1" not in self._sdk_capabilities
        ):
            return -1
        account_id = _account_id(account, self.client.account_id)
        if account_id != self.client.account_id:
            return -1
        context = {
            "version": 1,
            "execute_before": execute_before,
            "generation": self._sdk_generation,
        }
        try:
            check_execution_context(context)
        except ValueError:
            return -1
        try:
            data = self.client.call(
                "order_stock",
                {
                    "account_id": account_id,
                    "stock_code": stock_code,
                    "order_type": order_type,
                    "order_volume": order_volume,
                    "price_type": price_type,
                    "price": price,
                    "strategy_name": strategy_name,
                    "order_remark": order_remark,
                    "execution_context": context,
                },
                account_id=account_id,
            )
        except RpcServerRepliedError as error:
            if error.error_code == "EXECUTION_NOT_STARTED":
                return -1
            raise
        return self._order_id(data.get("order_sys_id"))
