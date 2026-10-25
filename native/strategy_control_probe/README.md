# QMT 进程内原生 DLL 探针

本模块验证“自有 DLL 由 QMT 加载，再访问其现有策略管理器”的前置条件。它只解析已有模块及读取策略计数，不导出启动或停止命令，不能当作已经完成的策略控制器使用。

## 构建

使用现有 LLVM 18，在 Ubuntu 交叉编译 Windows x64 DLL：

```bash
python tools/build_native_strategy_probe.py --output /data/code/artifacts/qmt-auto-start-20261014/native-probe-build
```

DLL 只有一个 C 接口 `QmtCtl_Probe`。调用者提供固定的 64 字节报告结构。Windows 基础类型和函数声明在源文件中列明，DLL 仅导入 Kernel32 和 Ntdll，不需要在服务器安装编译器或新的 C++ 运行库。

没有 `DllMain` 及动态全局初始化，复杂操作只在显式调用时执行，避免加载器锁问题。构建使用 `/brepro`，相同源码连续两次构建得到相同 SHA256。

## 部署和运行

Windows 部署目录：

```text
C:\Users\root\code\qmt-native-controller-20261014\QmtNativeStrategyProbe.dll
C:\东北证券NET专业版\python\QMT_NATIVE_PROBE.py
```

仓库中的 Python 模板是 UTF-8；构建生成的 QMT 脚本转换为 GB18030 并使用 `#coding:gbk`。加载脚本显式指定自有 DLL 的绝对路径，不替换 QMT 原有模块。

将 `.py` 复制到 `python` 目录只完成文件部署，不会自动注册模型或新增模型交易实例。QMT 手册中“导入公式”的默认格式是 `.rzrk`，不能将这个按钮与直接执行 `.py` 混为一谈。

首次注册按以下步骤进行：

1. 在“模型研究”的“我的策略”中右键新建 Python 模型，名称填写 `QMT_NATIVE_PROBE`。
2. 将已部署的 `QMT_NATIVE_PROBE.py` 内容放入模型编辑器并保存；基本信息中取消“启用本地 Python”，使 DLL 加载到 `XtItClient.exe` 中。
3. 在“模型交易”点击“新建策略交易”，明确选择 `QMT_NATIVE_PROBE`，并选择当前大赛账号 `19314672`。
4. 启动这个新实例，核对该行的策略名称及下方日志；更新探针后需重新加载代码并重新运行。

预期策略日志出现：

```text
[QMT_NATIVE_PROBE] {"status_name": "OK", ...}
```

V2 加载脚本会先输出 `SCRIPT_LOADED`、`LOAD_DLL` 和 `CALL_NATIVE_PROBE`。顶层直接执行只读探针，模型研究或普通脚本执行即使不调用 `init`，也能取得报告。实际策略初始化和首次行情回调另输出 `INIT_OK`、`HANDLEBAR_OK`，避免把脚本被执行误认为策略已开始运行。加载或调用异常会先输出对应阶段和错误信息。

### 使用现有桥接实例加载

构建还生成 `QMT_BACKEND_NATIVE_PROBE_LOADER.py`，可在备份并核对原文件 SHA256 后，追加到 `QMT_TRADE_BACKEND.py` 末尾。这样重启现有桥接实例即可验证，省去单独注册探针模型的步骤。

加载器先调用原始桥接 `init`，成功返回后才执行只读探针，并保留原始返回值。探针使用独立全局空间，避免覆盖桥接的 `handlebar`、`adjust`、`stop` 等回调；探针异常会输出 `ATTACHED_LOADER` 错误记录。原始桥接初始化异常继续向 QMT 传播。

此方式产生的第一条探针日志是 `ATTACHED_LOADER_BEGIN`。加载器追加到磁盘文件后，当前运行实例不会因此成为已验证的实例，仍需停止并重新启动这行 `QMT_TRADE_BACKEND`，再检查报告中的 PID 和状态。

应逐项核对报告：

- `pid` 与当前 `XtItClient.exe` 一致。
- `manager` 非零，证明访问的是进程内已经初始化的管理器。
- `strategy_count`、`running_count` 为有效计数。
- `bindings & 3 == 3` 表示核对了原生启动、停止函数的导出及 RVA，但本探针没有调用它们。
- 仅当 `manager_thread`、`caller_thread` 都非零时，才可判断是否在同一 Qt 线程；零值表示相关线程信息未取得。

## 限制及验证结果

原生符号和 RVA 对应本次检查的东北证券 NET 专业版 `2.1.19.0` 中的 Formula.dll，其他版本不匹配时返回 `UNSUPPORTED_EXPORT_PROFILE`。探针读取现有 `s_pInstance`，不会调用在空状态下创建新对象的 `instance()`。

已验证 Windows 外部 Python 可以加载 DLL，并返回 `WRONG_HOST`；空输出指针和错误结构大小均被拒绝。外部 Python 的成功加载不证明 DLL 已在 QMT 中加载，也不证明可以启动策略。

2026-10-14 22:36 的进程内实测已通过：当前 `XtItClient.exe` PID 为 `9372`，其模块列表包含探针 DLL；报告 `status_name=OK`、`bindings=3`、`strategy_count=1`、`running_count=1`，管理器地址非零。两项 Qt 线程指针均有效且不同，因此不能把 Python 回调线程当作管理器所属线程。此结论仅针对本次已核对的进程和版本，不应复用报告中的指针到其他进程。

22:42 的后续日志确认桥接重新输出 `READY account=19314672`，约 100 毫秒的定时回调持续运行。六项 Windows 外部追加加载器检查也通过，包括其他回调身份、原始初始化上下文与返回值、原始初始化错误传播、错误宿主拒绝、每次加载只执行一次，以及缺失 DLL 的异常隔离。这些检查没有执行原生启动或停止命令。

本机 QMT 手册的“功能扩展模块策略”说明正式扩展入口需要管理端 MD5 白名单。这里只准备了 Python 显式加载路径，是否受到当前账号权限限制，仍需 QMT 内运行验证；不修改白名单及检测逻辑。

运行证据保存在 `/data/code/artifacts/qmt-auto-start-20261014/`，主要包括 `native-probe-deployment.json`、`native-probe-outside-host.json`、`native-probe-attached-deployment.json`、`native-probe-attached-verification.json`、`native-probe-attached-in-qmt.json`、`native-probe-attached-ready.json`。后续控制器仍需独立的控制通道、正确的线程调度，以及真实停止和启动的验收。
