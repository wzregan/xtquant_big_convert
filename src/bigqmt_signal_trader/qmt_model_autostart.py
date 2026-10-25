"""检查大 QMT 原生模型自启动配置，通过已绑定的交互计划任务打开客户端。"""

import argparse
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET


class ModelAutostartError(RuntimeError):
    """目标策略、账户或启动任务无法被唯一核对。"""


def inspect_model_autostart(install_dir, strategy_name, account_id):
    """读取指定安装根目录下的策略；账户为完整资金账号，不修改客户端配置。"""
    root = Path(install_dir).resolve()
    config = root / "config" / "indexUserConfig.xml"
    try:
        tree = ET.parse(config)
    except (OSError, ET.ParseError) as exc:
        raise ModelAutostartError("无法读取策略配置: %s" % config) from exc
    items = tree.findall(".//strategyTrade/item")
    matches = [item for item in items if item.get("name") == strategy_name and item.get("account") == str(account_id)]
    if len(matches) != 1:
        raise ModelAutostartError("策略与账户必须唯一匹配，实际匹配 %d 项" % len(matches))
    item = matches[0]
    strategy_id = item.get("id")
    if not strategy_id or sum(other.get("id") == strategy_id for other in items) != 1:
        raise ModelAutostartError("策略 ID 缺失或重复")
    script = root / "python" / (strategy_name + ".py")
    if Path(strategy_name).name != strategy_name or "/" in strategy_name or "\\" in strategy_name or not script.is_file():
        raise ModelAutostartError("策略必须对应安装目录内的 Python 文件")
    delay = item.get("timeInterval")
    try:
        delay_seconds = int(delay) if delay is not None else None
    except ValueError as exc:
        raise ModelAutostartError("timeInterval 必须是整数秒数") from exc
    # 负数不会经过原生倒计时的零值分支，不能按立即启动解释。
    enabled = item.get("startupAutorun") == "1"
    ready = enabled and delay_seconds is not None and delay_seconds >= 0
    return {
        "install_dir": str(root), "config_file": str(config),
        "strategy_name": strategy_name, "strategy_id": strategy_id,
        "account_id": str(account_id), "script": str(script),
        "run_mode": item.get("runMode"), "enabled": enabled,
        "delay_seconds": delay_seconds, "configuration_ready": ready,
    }


def ensure_client_via_task(task_name, install_dir):
    """启动指定交互任务；客户端已运行时不重启，不点击任何策略按钮。"""
    if os.name != "nt":
        raise ModelAutostartError("计划任务入口必须在 Windows 上执行")
    exe = str(Path(install_dir).resolve() / "bin.x64" / "XtItClient.exe")

    def literal(value):
        """按 PowerShell 字面量转义任务名与路径，避免输入被当作代码。"""
        return "'" + str(value).replace("'", "''") + "'"

    command = "\n".join([
        "$ErrorActionPreference='Stop'",
        "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8",
        "$expected=" + literal(exe),
        "$task=Get-ScheduledTask -TaskName " + literal(task_name),
        "if (@($task).Count -ne 1) { throw 'Task must be unique' }",
        "if (@($task.Actions).Count -ne 1 -or $task.Actions[0].Execute.Trim('\"') -ine $expected) { throw 'Task executable mismatch' }",
        "if ($task.Principal.LogonType.ToString() -ne 'Interactive') { throw 'Task requires an interactive logon token' }",
        "$matches=@(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'XtItClient.exe' -and $_.ExecutablePath -ieq $expected })",
        "if ($matches.Count -gt 1) { throw 'Multiple target QMT clients' }",
        "$started=$false",
        "if ($matches.Count -eq 0) { Start-ScheduledTask -InputObject $task; $started=$true }",
        "[pscustomobject]@{task_name=$task.TaskName; client_was_running=($matches.Count -eq 1); start_requested=$started; strategy_ready=$null} | ConvertTo-Json -Compress",
    ])
    encoded = base64.b64encode(command.encode("utf-16le")).decode("ascii")
    completed = subprocess.run(
        ["powershell.exe", "-NoProfile", "-EncodedCommand", encoded],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30,
    )
    if completed.returncode:
        raise ModelAutostartError(completed.stderr.decode("utf-8", errors="replace"))
    return json.loads(completed.stdout.decode("utf-8-sig"))


def main(argv=None):
    """输出原生配置或请求打开客户端；任务成功不代表桥接及交易已就绪。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("inspect", "ensure"))
    parser.add_argument("--dir", required=True, help="完整大 QMT 安装根目录")
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--task", help="直接启动同目录 XtItClient.exe 的交互计划任务")
    args = parser.parse_args(argv)
    try:
        result = inspect_model_autostart(args.dir, args.strategy, args.account)
        if args.action == "ensure":
            if not result["configuration_ready"]:
                raise ModelAutostartError("原生自启动未启用，或倒计时无效；未启动客户端")
            if not args.task:
                raise ModelAutostartError("ensure 需要明确指定 --task")
            result["task"] = ensure_client_via_task(args.task, args.dir)
        print(json.dumps(result, ensure_ascii=True))
    except (ModelAutostartError, subprocess.TimeoutExpired) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
