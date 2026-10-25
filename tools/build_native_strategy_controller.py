"""交叉构建生产控制 DLL 与只能操作自建窗口的测试 DLL。"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from build_native_strategy_probe import find_tool


def build(output_dir):
    """生成 Windows x64 DLL 及身份清单；测试版本不会接受 XtItClient 宿主。"""
    root = Path(__file__).resolve().parents[1]
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    compiler = find_tool("clang++", "clang++-18")
    linker = find_tool("lld-link", "lld-link-18")
    dlltool = find_tool("llvm-dlltool", "llvm-dlltool-18")
    imports = {
        "kernel32": ["GetModuleHandleW", "GetModuleHandleExW", "GetModuleFileNameW", "GetProcAddress",
                     "GetCurrentProcessId", "GetCurrentThreadId", "GetCurrentProcess", "GetProcessTimes",
                     "GetTickCount64", "GetLastError", "OpenFileMappingW", "MapViewOfFile", "UnmapViewOfFile",
                     "OpenEventW", "CreateEventW", "SetEvent", "WaitForSingleObject", "CreateThread", "CloseHandle"],
        "user32": ["EnumWindows", "IsWindowVisible", "GetClassNameW", "GetWindowThreadProcessId",
                   "GetWindowRect", "SetWindowsHookExW", "UnhookWindowsHookEx", "CallNextHookEx", "PostMessageW",
                   "GetThreadDesktop", "SetThreadDesktop"],
        "ntdll": ["__C_specific_handler"],
    }
    libraries = []
    for name, functions in imports.items():
        definition = output_dir / (name + ".def")
        definition.write_text("LIBRARY " + name + ".dll\nEXPORTS\n" + "\n".join(functions) + "\n", encoding="ascii")
        library = output_dir / (name + ".lib")
        subprocess.run([dlltool, "-m", "i386:x86-64", "-d", str(definition), "-l", str(library)], check=True)
        libraries.append(str(library))
    source = root / "native/strategy_controller/controller.cpp"
    results = []
    for is_test in (False, True):
        stem = "QmtNativeStrategyControlTest" if is_test else "QmtNativeStrategyControl"
        obj = output_dir / (stem + ".obj")
        command = [compiler, "--target=x86_64-pc-windows-msvc", "-std=c++17", "-O2", "-Wall", "-Wextra", "-Werror",
                   "-ffreestanding", "-fno-builtin", "-fno-stack-protector", "-fno-exceptions", "-fno-rtti", "-fms-extensions"]
        if is_test:
            command.append("-DQMT_CONTROLLER_TEST=1")
        subprocess.run(command + ["-c", str(source), "-o", str(obj)], check=True)
        dll = output_dir / (stem + ".dll")
        subprocess.run([linker, "/dll", "/entry:DllMain", "/nodefaultlib", "/machine:x64", "/dynamicbase", "/nxcompat", "/brepro",
                        "/out:" + str(dll), str(obj), *libraries], check=True)
        record = {"file": dll.name, "sha256": hashlib.sha256(dll.read_bytes()).hexdigest(),
                  "bytes": dll.stat().st_size, "test_build": is_test, "abi_version": 1}
        (output_dir / (stem + ".json")).write_text(json.dumps(record, indent=2), encoding="utf-8")
        results.append(record)
    # 控制入口和测试源码随身份清单一起交付，部署后可复现验证而不依赖临时工作目录。
    control_root = source.parent
    for name in ("native_control.py", "README.md"):
        (output_dir / name).write_bytes((control_root / name).read_bytes())
    test_dir = output_dir / "tests"
    test_dir.mkdir(exist_ok=True)
    for name in ("controller_harness.py", "verify_native_controller_windows.py"):
        (test_dir / name).write_bytes((control_root / "tests" / name).read_bytes())
    return results


def main():
    """接受构建目录，输出两种 DLL 身份，不注入或启动目标进程。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    print(json.dumps(build(parser.parse_args().output), ensure_ascii=False))


if __name__ == "__main__":
    main()
