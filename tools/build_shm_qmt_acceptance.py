# -*- coding: utf-8 -*-
"""复用正式单文件构建器，生成由用户在大 QMT 启动的 SHM 桥接。"""

import argparse
import ast
import base64
import hashlib
import json
import os
import pprint
import tempfile

import build_single_file


PROFILE_TAG = "TEST4_SHM_BRIDGE_V1"
DEFAULT_NAMESPACE = "Local\\bigqmt-test4-shm-v1"
MODES = {"read-only": "READ_ONLY", "trading": "TRADING_TEST"}


def acceptance_config(namespace=DEFAULT_NAMESPACE, directory=None, mode="read-only"):
    """返回验收配置；mode 决定交易权限，directory 仅用于 POSIX 本机回归。"""
    if mode not in MODES:
        raise ValueError("mode must be read-only or trading")
    shm = {
        "namespace": str(namespace),
        "rpc_slots": 32,
        "rpc_slot_bytes": 65536,
        "max_payload_bytes": 64 * 1024 * 1024,
        "wait_poll_seconds": 0.05,
        "market_push_bytes": 24 * 1024 * 1024,
        "trade_push_bytes": 8 * 1024 * 1024,
        "market_push_slots": 4096,
        "trade_push_slots": 1024,
        "max_push_subscribers": 64,
        "push_codec": "json",
        "push_poll_seconds": 0.05,
        "subscriber_retention_seconds": 300.0,
    }
    if directory is not None:
        shm["directory"] = os.path.abspath(directory)
    return {
        "transport": "shm",
        "redis_enabled": False,
        "rpc_allow_order_methods": mode == "trading",
        "rpc_background_threads": False,
        "rpc_process_in_listener": True,
        "rpc_listener_methods": ("*",),
        "schedule_adjust": True,
        "schedule_adjust_interval": "100nMilliSecond",
        "full_tick_cache_enabled": False,
        "download_jobs_enabled": False,
        "exec_events_enabled": True,
        "formula_server": {"enabled": False},
        "shm": shm,
    }


RUNTIME_WRAPPER = '''
import time as _shm_acceptance_time

_SHM_ACCEPTANCE_TAG = "TEST4_SHM_BRIDGE_V1"
_SHM_ACCEPTANCE_MODE = BIGQMT_ACCEPTANCE_MODE
_SHM_ACCEPTANCE_TRADING = _SHM_ACCEPTANCE_MODE == "TRADING_TEST"
_SHM_ACCEPTANCE_NATIVE_INIT = init
_SHM_ACCEPTANCE_NATIVE_ADJUST = adjust
_SHM_ACCEPTANCE_NATIVE_HANDLEBAR = handlebar
_shm_acceptance_state = {"started": None, "last_log": None, "adjust": 0, "handlebar": 0}


def _shm_acceptance_apply_profile():
    """按明确模式配置交易权限；保留共享推送健康门禁和空信号源。"""
    rpc = dict(_strategy._config.get("rpc") or {})
    handlers = _load_local_module("bigqmt_signal_trader.redis_rpc")
    excluded = {"reload_deployment", "sync_positions", "smt_appointment", "gen_factor_index",
                "call_formula", "subscribe_formula", "unsubscribe_formula", "get_formula_result",
                "create_sector", "create_sector_folder", "add_stock_to_sector",
                "remove_stock_from_sector", "reset_sector_stock_list", "add_sector", "remove_sector",
                "subscribe_l2thousand"}
    rpc.update({"enabled": True, "transport": "shm", "redis_enabled": False,
                "allow_order_methods": _SHM_ACCEPTANCE_TRADING, "background_threads": False,
                "warm_context_data": False})
    allowed = {method for method in handlers.READ_METHODS
               if method not in excluded and not method.startswith("download_")
               and not method.startswith("submit_download_")}
    if BIGQMT_ACCEPTANCE_HISTORY_DOWNLOAD:
        # 历史 Tick 缺口补齐依赖同步下载；只开放行情下载，不启动后台任务或财务全量下载。
        allowed.update({"download_history_data", "download_history_data2"})
    if _SHM_ACCEPTANCE_TRADING:
        allowed.update(handlers.ORDER_METHODS)
    rpc["allowed_methods"] = sorted(allowed)
    _strategy.configure(rpc=rpc, redis={}, position_sync_type="", position_sync_enabled=False,
                        signal_source_type="", source_type="", state_store_type="",
                        grpc_companion={"enabled": False, "auto_start": False},
                        formula_server={"enabled": False})


def _shm_acceptance_check_started(app):
    """校验真实服务、发布端及原生调度，避免底层捕获初始化错误后误报成功。"""
    service = getattr(_strategy, "_rpc_service", None)
    if app is None or service is None:
        raise RuntimeError("formal strategy/RPC initialization failed; inspect preceding log")
    backend = getattr(service, "communication_backend", None)
    transport = getattr(service, "_transport", None)
    if getattr(backend, "name", None) != "shm" or not transport.status().get("running"):
        raise RuntimeError("formal shared-memory RPC backend is not running")
    if service.background_threads or getattr(service, "redis", None) is not None:
        raise RuntimeError("acceptance profile must use QMT scheduler and no Redis")
    pair = getattr(_strategy, "_quote_subscription_service", None)
    if pair is None or not pair[1].supports_acknowledgements:
        raise RuntimeError("formal acknowledged push service is unavailable")
    push = getattr(pair[1], "_channel", pair[1])
    if not push.status().get("started"):
        raise RuntimeError("formal shared-memory publisher failed to start")
    if not getattr(_strategy, "_scheduled_adjust", False):
        raise RuntimeError("ContextInfo.run_time did not schedule adjust; use embedded QMT strategy mode")
    if service.handlers.allow_order_methods != _SHM_ACCEPTANCE_TRADING:
        raise RuntimeError("RPC order permission does not match explicit acceptance mode")
    if _SHM_ACCEPTANCE_TRADING:
        gateway_type = _load_local_module("bigqmt_signal_trader.adapters.order_bigqmt").BigQmtOrderGateway
        gateway = service.handlers.order_gateway
        if not isinstance(gateway, gateway_type):
            raise RuntimeError("TRADING_TEST requires the real BigQmtOrderGateway, not a dry-run gateway")
        for name in ("passorder", "cancel_func", "get_trade_detail_data"):
            if not callable(getattr(gateway, name, None)):
                raise RuntimeError("TRADING_TEST missing native API binding: %s" % name)
        handlers = _load_local_module("bigqmt_signal_trader.redis_rpc")
        if not handlers.ORDER_METHODS.issubset(service.handlers.allowed_methods):
            raise RuntimeError("TRADING_TEST order/batch/cancel RPC methods are not fully enabled")


def _shm_acceptance_heartbeat(force=False):
    """由 QMT 回调打印十秒心跳；不创建额外网络、文件或探针线程。"""
    now = _shm_acceptance_time.monotonic()
    previous = _shm_acceptance_state["last_log"]
    if not force and previous is not None and now - previous < 10.0:
        return
    _shm_acceptance_state["last_log"] = now
    service = getattr(_strategy, "_rpc_service", None)
    pair = getattr(_strategy, "_quote_subscription_service", None)
    rpc = service._transport.status() if service is not None else {}
    push = pair[1].status() if pair is not None else {}
    print("[%s] HEARTBEAT %s t=%.1fs adjust=%d handlebar=%d rpc_running=%s "
          "rpc_executed=%s rpc_responses=%s rpc_error=%s push_local_pending=%s push_committed=%s "
          "push_orders_blocked=%s push_health_busy=%s push_error=%s" % (
              _SHM_ACCEPTANCE_TAG, _SHM_ACCEPTANCE_MODE, now - (_shm_acceptance_state["started"] or now),
              _shm_acceptance_state["adjust"], _shm_acceptance_state["handlebar"],
              rpc.get("running", False), rpc.get("executed", 0), rpc.get("responses", 0),
              rpc.get("error") or "none",
              push.get("local_pending", {}), push.get("shared_committed", 0),
              push.get("new_orders_blocked", True), push.get("health_check_busy", False),
              push.get("sticky_error") or push.get("last_error") or "none"))


def _shm_acceptance_bind_account(ContextInfo):
    """交易模式先严格绑定原生回调；正式 init 后续再注册同一账号，失败不能被吞掉。"""
    if not _SHM_ACCEPTANCE_TRADING:
        return
    setter = getattr(ContextInfo, "set_account", None)
    if not callable(setter):
        raise RuntimeError("TRADING_TEST requires native ContextInfo.set_account for execution callbacks")
    setter(BIGQMT_ACCOUNT_ID)
    print("[%s] ACCOUNT_BOUND %s account=%s" % (
        _SHM_ACCEPTANCE_TAG, _SHM_ACCEPTANCE_MODE, BIGQMT_ACCOUNT_ID))


def init(ContextInfo):
    """启动明确模式的正式桥接；ContextInfo 由大 QMT 注入，必须支持 run_time。"""
    if not BIGQMT_ACCOUNT_ID or BIGQMT_ACCOUNT_ID == "YOUR_ACCOUNT_ID":
        raise RuntimeError("build TEST4 with --account-id matching the selected QMT account")
    _shm_acceptance_apply_profile()
    _shm_acceptance_state.update({"started": _shm_acceptance_time.monotonic(),
                                  "last_log": None, "adjust": 0, "handlebar": 0})
    print("[%s] INIT_BEGIN %s account=%s transport=shm namespace=%s" % (
        _SHM_ACCEPTANCE_TAG, _SHM_ACCEPTANCE_MODE, BIGQMT_ACCOUNT_ID, BIGQMT_REDIS_CONFIG["shm"]["namespace"]))
    try:
        _shm_acceptance_bind_account(ContextInfo)
        app = _SHM_ACCEPTANCE_NATIVE_INIT(ContextInfo)
        _shm_acceptance_check_started(app)
    except Exception as exc:
        _strategy.reset_app()
        print("[%s] INIT_FAILED %s: %s" % (_SHM_ACCEPTANCE_TAG, type(exc).__name__, exc))
        raise
    print("[%s] INIT_OK %s rpc=shm push=shm background_threads=False "
          "schedule=100nMilliSecond redis=False formula=False warmup=False companion=False "
          "allow_order_methods=%s auto_submit=False" % (
              _SHM_ACCEPTANCE_TAG, _SHM_ACCEPTANCE_MODE, _SHM_ACCEPTANCE_TRADING))
    _shm_acceptance_heartbeat(force=True)
    return app


def adjust(ContextInfo):
    """保留正式策略主线程处理，随后记录原生定时调度心跳。"""
    _shm_acceptance_state["adjust"] += 1
    result = _SHM_ACCEPTANCE_NATIVE_ADJUST(ContextInfo)
    _shm_acceptance_heartbeat()
    return result


def handlebar(ContextInfo):
    """保留正式逐行情回调，分别记录行情和定时器的调用次数。"""
    _shm_acceptance_state["handlebar"] += 1
    result = _SHM_ACCEPTANCE_NATIVE_HANDLEBAR(ContextInfo)
    _shm_acceptance_heartbeat()
    return result


def stop(ContextInfo):
    """用户停止策略时回收正式 RPC、推送通道及行情订阅。"""
    print("[%s] STOP_BEGIN %s" % (_SHM_ACCEPTANCE_TAG, _SHM_ACCEPTANCE_MODE))
    _strategy.reset_app()
    print("[%s] STOP_OK" % _SHM_ACCEPTANCE_TAG)


_shm_acceptance_apply_profile()
'''


def build_acceptance(output, account_id, account_type="STOCK", namespace=DEFAULT_NAMESPACE,
                     directory=None, mode="read-only", allow_history_download=False, debug=False):
    """构建 ASCII/GBK 单文件；账号/namespace 两端须一致，mode 决定交易权限。

    allow_history_download 显式开放历史行情缓存写入，独立于交易权限，默认关闭。
    debug 仅开启面板诊断，不启用文件日志、网络或更改交易权限，默认关闭。
    directory 仅用于 POSIX 本机回归，不应设置为 Windows 部署目录。
    """
    account_id = str(account_id).strip()
    if not account_id or not str(namespace).strip():
        raise ValueError("account_id and namespace must not be empty")
    settings = acceptance_config(namespace, directory, mode)
    mode_label = MODES[mode]
    output = os.path.abspath(output)
    os.makedirs(os.path.dirname(output), exist_ok=True)
    descriptor, base_path = tempfile.mkstemp(suffix="-formal-bigqmt.py")
    os.close(descriptor)
    original_output = build_single_file.OUT_PATH
    try:
        build_single_file.OUT_PATH = base_path
        build_single_file.main()
        with open(base_path, encoding="ascii") as stream:
            source = stream.read()
    finally:
        build_single_file.OUT_PATH = original_output
        os.unlink(base_path)
    start = source.index('BIGQMT_ACCOUNT_ID = "YOUR_ACCOUNT_ID"')
    end = source.index("# ===================================================================", start)
    block = ("BIGQMT_ACCOUNT_ID = %s\nBIGQMT_ACCOUNT_TYPE = %s\n"
             "BIGQMT_ACCEPTANCE_MODE = %s\nBIGQMT_ACCEPTANCE_HISTORY_DOWNLOAD = %r\n"
             "BIGQMT_DEBUG = %r\nos.environ['BIGQMT_DEBUG'] = '1' if BIGQMT_DEBUG else '0'\n"
             "BIGQMT_REDIS_CONFIG = %s\n") % (
        json.dumps(account_id, ensure_ascii=True), json.dumps(account_type, ensure_ascii=True),
        json.dumps(mode_label), bool(allow_history_download), bool(debug),
        pprint.pformat(settings, width=100))
    source = source[:start] + block + source[end:]
    source = source.replace("Single-file self-contained Big QMT Redis dry-run strategy.",
                            "Formal Big QMT SHM %s strategy (TEST4_SHM_BRIDGE_V1)." % mode_label, 1)
    # 验收期间只使用 QMT 面板日志，禁止历史文件日志设置引入额外 FileIO。
    source = source.replace("import types\n", "import types\nos.environ['BIGQMT_LOG_ENABLED'] = '0'\n"
                            "os.environ['BIGQMT_FORMULA_ENABLED'] = '0'\n", 1)
    source += RUNTIME_WRAPPER
    # 外壳全 ASCII；中文文档串在运行时还原，嵌入源码仍保持原始 UTF-8 字节。
    source = source.encode("ascii", "backslashreplace").decode("ascii")
    compile(source, output, "exec")
    tree = ast.parse(source)
    embedded = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                    and any(isinstance(target, ast.Name) and target.id == "_EMBEDDED_SOURCES"
                            for target in node.targets))
    data = source.encode("ascii")
    with open(output, "wb") as stream:
        stream.write(data)
    client = {"account_id": account_id, "account_type": str(account_type), "transport": "shm",
              "redis_enabled": False, "shm": settings["shm"], "formula_server": {"enabled": False}}
    manifest = {"profile": PROFILE_TAG, "mode": mode_label, "encoding": "ASCII; #coding:gbk",
                "file": os.path.basename(output), "sha256": hashlib.sha256(data).hexdigest(),
                "bytes": len(data), "client_config": client,
                "allow_order_methods": mode == "trading", "auto_submit": False,
                "allow_history_download": bool(allow_history_download),
                "debug": bool(debug),
                "rpc_background_threads": False, "warm_context_data": False,
                "grpc_companion": {"enabled": False, "auto_start": False},
                "embedded_sha256": {key: hashlib.sha256(base64.b64decode(value)).hexdigest()
                                    for key, value in sorted(embedded.items())}}
    for suffix, payload in ((".manifest.json", manifest), (".client.json", client)):
        with open(output + suffix, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=True, indent=2, sort_keys=True)
            stream.write("\n")
    print("WROTE %s sha256=%s bytes=%d" % (output, manifest["sha256"], len(data)))
    return manifest


def main():
    """解析构建参数；只写 Linux 构建产物，不连接或启动 Windows/QMT。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--account-type", default="STOCK")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=tuple(MODES), default="read-only",
                        help="trading 开放正式交易 RPC；两种模式都不自动产生订单")
    parser.add_argument("--directory", help="POSIX 本机回归目录，Windows 部署不要设置")
    parser.add_argument("--allow-history-download", action="store_true",
                        help="开放历史行情缓存补下载；独立于交易权限，不启用后台下载任务")
    parser.add_argument("--debug", action="store_true", help="开启有界面板诊断，默认关闭")
    args = parser.parse_args()
    build_acceptance(args.output, args.account_id, args.account_type, args.namespace, args.directory, args.mode,
                     args.allow_history_download, args.debug)


if __name__ == "__main__":
    main()
