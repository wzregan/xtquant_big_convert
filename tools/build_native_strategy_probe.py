"""利用已有 LLVM 工具链生成不依赖额外运行库的 Windows x64 原生探针。"""

import argparse
import base64
import hashlib
import json
from pathlib import Path
import shutil
import subprocess


def find_tool(*names):
    """从已有 PATH 中选择工具；不安装系统编译环境。"""
    for name in names:
        result = shutil.which(name)
        if result:
            return result
    raise RuntimeError("missing existing tool: %s" % ", ".join(names))


def build(output_dir):
    """在指定输出目录交叉编译，输出 DLL、结构化身份记录和可在 QMT 内执行的加载脚本。"""
    root = Path(__file__).resolve().parents[1]
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    clang = find_tool("clang++", "clang++-18")
    linker = find_tool("lld-link", "lld-link-18")
    dlltool = find_tool("llvm-dlltool", "llvm-dlltool-18")
    for library, functions in {
        "kernel32": ("GetModuleHandleW", "GetProcAddress", "GetModuleFileNameW", "GetCurrentProcessId", "GetCurrentThreadId"),
        "ntdll": ("__C_specific_handler",),
    }.items():
        definition = output_dir / (library + ".def")
        definition.write_text("LIBRARY " + library + ".dll\nEXPORTS\n" + "\n".join(functions) + "\n", encoding="ascii")
        subprocess.run([dlltool, "-m", "i386:x86-64", "-d", str(definition), "-l", str(output_dir / (library + ".lib"))], check=True)
    source = root / "native" / "strategy_control_probe" / "probe.cpp"
    obj = output_dir / "probe.obj"
    subprocess.run([
        clang, "--target=x86_64-pc-windows-msvc", "-std=c++17", "-O2", "-Wall", "-Wextra", "-Werror",
        "-ffreestanding", "-fno-builtin", "-fno-stack-protector", "-fno-exceptions", "-fno-rtti", "-fms-extensions",
        "-c", str(source), "-o", str(obj),
    ], check=True)
    dll = output_dir / "QmtNativeStrategyProbe.dll"
    subprocess.run([
        linker, "/dll", "/noentry", "/nodefaultlib", "/machine:x64", "/dynamicbase", "/nxcompat", "/brepro",
        "/out:" + str(dll), str(obj), str(output_dir / "kernel32.lib"), str(output_dir / "ntdll.lib"),
    ], check=True)
    template_path = root / "native" / "strategy_control_probe" / "QMT_NATIVE_PROBE.py"
    template = template_path.read_text(encoding="utf-8")
    # 部署件使用 QMT 的 GBK 编码；源码仍以 UTF-8 保存在仓库，避免乱码及双重解码。
    template = template.replace("#coding:utf-8", "#coding:gbk", 1)
    (output_dir / "QMT_NATIVE_PROBE.py").write_bytes(template.encode("gb18030"))
    loader_path = root / "native" / "strategy_control_probe" / "backend_probe_loader.py"
    loader = loader_path.read_text(encoding="utf-8")
    # 原始 UTF-8 源码连同编码头一起嵌入；加载空间与桥接回调隔离。
    loader = loader.replace("__NATIVE_PROBE_SOURCE_B64__", base64.b64encode(template_path.read_bytes()).decode("ascii"))
    loader = loader.replace("#coding:utf-8", "#coding:gbk", 1)
    loader_output = output_dir / "QMT_BACKEND_NATIVE_PROBE_LOADER.py"
    loader_output.write_bytes(loader.encode("gb18030"))
    result = {
        "dll": str(dll), "dll_sha256": hashlib.sha256(dll.read_bytes()).hexdigest(),
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "dll_bytes": dll.stat().st_size, "abi_version": 1, "probe_only": True,
        "attached_loader": str(loader_output),
        "attached_loader_sha256": hashlib.sha256(loader_output.read_bytes()).hexdigest(),
        "llvm": {"compiler": clang, "linker": linker, "dlltool": dlltool},
    }
    (output_dir / "build.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main():
    """接受输出目录并打印构建身份，不加载或运行生成的 DLL。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    print(json.dumps(build(parser.parse_args().output), ensure_ascii=False))


if __name__ == "__main__":
    main()
