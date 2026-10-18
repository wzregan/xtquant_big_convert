"""核验共享内存通信范围内每个模块的行、分支覆盖率，不用总体平均掩盖薄弱模块。"""

import argparse
import json
from pathlib import Path


MODULES = (
    "transports/shm_primitives.py",
    "transports/shm_transport.py",
    "shm_push_channel.py",
    "transports/backend.py",
    "quote_subscription_manager.py",
    "whole_quote_session.py",
)


def check_report(report, minimum=99.0):
    """核对 coverage JSON；minimum 为每个模块两类覆盖率的最低百分比。"""
    rows, failures = [], []
    if report.get("meta", {}).get("branch_coverage") is not True:
        failures.append("报告未启用分支覆盖率")
    for module in MODULES:
        suffix = "bigqmt_signal_trader/" + module
        matches = [value for path, value in report.get("files", {}).items()
                   if path.replace("\\", "/").endswith(suffix)]
        if len(matches) != 1:
            failures.append("模块缺失或存在多份源码: " + module)
            continue
        summary = matches[0]["summary"]
        statements, branches = summary["num_statements"], summary["num_branches"]
        line_rate = 100.0 * summary["covered_lines"] / statements if statements else 0.0
        branch_rate = 100.0 * summary["covered_branches"] / branches if branches else 0.0
        row = {"module": module, "lines_percent": line_rate, "branches_percent": branch_rate,
               "covered_lines": summary["covered_lines"], "statements": statements,
               "covered_branches": summary["covered_branches"], "branches": branches,
               "excluded_lines": summary["excluded_lines"]}
        rows.append(row)
        if min(line_rate, branch_rate) < minimum or row["excluded_lines"]:
            failures.append("覆盖不足或含排除行: " + module)
    return {"minimum_percent": minimum, "passed": not failures, "modules": rows,
            "failures": failures}


def main():
    """读取报告并返回可用于持续集成门禁的退出码；不执行或连接任何交易服务。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = check_report(json.loads(args.report.read_text(encoding="utf-8")))
    encoded = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
