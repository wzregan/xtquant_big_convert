"""XtQuant 兼容客户端与 QMT 服务端共享的账户和执行契约。"""

import math
import time

# 仅映射明确的终端状态；未知文本不能按登录成功处理。
ACCOUNT_STATES = {
    "正常": 0,
    # 东北证券大 QMT 的 ACCOUNT 查询以此文本表示已登录，需对齐 SDK 的正常状态。
    "登录成功": 0,
    "连接中": 1,
    "准备登录": 1,
    "登录中": 2,
    "登陆中": 2,
    "失败": 3,
    "登录失败": 3,
    "初始化中": 4,
    "数据刷新校正中": 5,
    "收盘后": 6,
    "穿透副链接断开": 7,
    "系统停用": 8,
    "用户停用": 9,
}


class ExecutionRejected(ValueError):
    """表示服务端明确在进入原生下单之前拒绝请求。"""

    error_code = "EXECUTION_NOT_STARTED"


def account_status_row(row, account_id, account_type):
    """把 ACCOUNT 原始字典转为状态快照，保留状态文本及本次采集时间。"""
    actual_account = str(row.get("m_strAccountID") or "")
    if actual_account != account_id:
        raise ValueError("ACCOUNT row account_id mismatch")
    raw_status = str(row.get("m_strStatus") or "").strip()
    return {
        "account_id": actual_account,
        "account_type": int(row.get("m_nBrokerType") or account_type),
        "status": ACCOUNT_STATES.get(raw_status, -1),
        "status_msg": raw_status,
        "observed_at": time.time(),
    }


def check_execution_context(context):
    """检查可选执行契约；execute_before 为 UTC Unix 秒，不是 RPC 等待超时。"""
    if not isinstance(context, dict) or context.get("version") != 1:
        raise ExecutionRejected("unsupported execution context")
    deadline = context.get("execute_before")
    if deadline is not None:
        if isinstance(deadline, bool) or not isinstance(deadline, (int, float)):
            raise ExecutionRejected("invalid execute_before")
        if not math.isfinite(deadline) or time.time() >= deadline:
            raise ExecutionRejected("EXPIRED_BEFORE_SUBMISSION")
