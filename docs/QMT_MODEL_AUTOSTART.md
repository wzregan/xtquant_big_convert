# 大 QMT 原生模型自启动

2026-10-14 在大赛提供的东北证券 NET 专业版 `2.1.19.0` 上核查。目标安装目录为 `C:\东北证券NET专业版`，目标资金账号为 `19314672`，策略为 `QMT_TRADE_BACKEND`。这些结论不自动适用于其他券商版本。

## 已确认的原生流程

`config\indexUserConfig.xml` 中的 `strategyTrade/item` 保存模型交易实例。当前目标实例为 `id="2"`，同时配置：

```xml
startupAutorun="1" timeInterval="10"
```

离线反汇编及当天日志相互印证以下路径：

```text
账户登录成功
  → CMainWindow::tryAutoRunStrategy
  → 检查登录模式、模型权限、自启动开关及运行状态
  → CTryAutoRunDialog 显示倒计时
  → 倒计时到零，调用 CStrategyTradeManager::runStrategy
  → 大 QMT 调用策略 init/handlebar
```

`timeInterval` 是此处的启动倒计时秒数，不是桥接请求轮询间隔。当前原生计时器每秒更新，日志显示实际在登录后约 11 秒触发启动：

| 原生日志时间 | 自动触发策略时间 | 启动原因 |
| --- | --- | --- |
| 00:43:13 | 00:43:24 | `auto run timeout` |
| 08:53:27 | 08:53:38 | `auto run timeout` |
| 12:09:17 | 12:09:28 | `auto run timeout` |

这是自动触发启动的运行证据；脚本初始化成功、SHM 请求可用、gRPC 就绪及交易门禁开放仍须分别检查。

## 命令入口

已安装包含新模块的转换层后，在 Windows 执行配置检查：

```powershell
python -m bigqmt_signal_trader.qmt_model_autostart inspect `
  --dir 'C:\东北证券NET专业版' `
  --strategy QMT_TRADE_BACKEND --account 19314672
```

使用现有的交互计划任务打开完整客户端：

```powershell
python -m bigqmt_signal_trader.qmt_model_autostart ensure `
  --dir 'C:\东北证券NET专业版' `
  --strategy QMT_TRADE_BACKEND --account 19314672 `
  --task QMTDesktopClient
```

`ensure` 校验账户、策略 ID、脚本文件、自启动开关、倒计时，以及计划任务的唯一性、可执行文件和交互登录方式。已有目标客户端时返回 `client_was_running=true`，不重启客户端或点击策略按钮。`strategy_ready=null` 明确表示该命令没有验证桥接就绪。

该入口可从 SSH 的非交互会话调用；已有计划任务负责在已登录的 Windows 桌面启动客户端。账户登录仍需成功。不要使用 `XtMiniQmt.exe linkMini` 替代完整客户端：它不能承载这里的模型交易回调。

本次还将独立命令文件部署到 `C:\Users\root\code\qmt-auto-start-20261014\qmt_model_autostart.py`，因此现有转换层安装无需整体升级即可使用。在此 Windows 服务器上已执行 `ensure` 幂等检查，目标 PID `2332` 保持不变。

从 Ubuntu 调用已经部署的入口：

```bash
ssh root@192.168.100.190 powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:/Users/root/code/qmt-auto-start-20261014/ensure-qmt.ps1
```

此完整命令已在当前 Windows 上执行成功，返回 `client_was_running=true`、`start_requested=false`。执行策略仅对本次运行的自有脚本生效，不修改系统策略。配置和现有启动器的回归测试共 30 项通过。

## 迭代时的区别

- 客户端重启并重新登录：原生配置支持倒计时自动启动策略。
- 客户端保持运行、只修改 Python 文件：已运行实例不会自动重新加载代码。原生流程会设置本进程中的 `m_bAutoruned` 标记，本次没有确认可从外部直接指定策略 ID 热重启的独立入口。
- 发布脚本后可将客户端重启纳入部署流程，再等待原生启动和桥接就绪。完整客户端重启后的恢复验收应包含 SHM `ping`、账户匹配、gRPC 和 S3 数据恢复。本次没有为了测试该入口重启运行中的 QMT/S3。

## 分析方法和边界

REA 已克隆到 `/data/code/rea`。采用其“文件身份 → 聚焦静态分析 → 运行证据核实”的流程；使用 REA CLI 保存了目标 EXE 的 PE 资源证据。当前主机没有 Ghidra/IDA/Hopper 原生分析引擎，函数分析使用已有 `pefile`/`capstone` 离线工具，对复制件解包，不修改已安装的 EXE/DLL。

Windows UI Automation 实测只返回一个顶层窗口、没有可读取的策略表格及按钮。静态发现 `CNativeEventFilter::nativeEventFilter` 对 `WM_GETOBJECT` 返回已处理，与该现象一致；因此此处不依赖控件名自动点击。微软对该消息的定义见 [WM_GETOBJECT](https://learn.microsoft.com/en-us/windows/win32/winauto/wm-getobject)。

完整诊断记录位于 `/data/code/artifacts/qmt-auto-start-20261014/`，其中 `binary-identity.json` 保存模块哈希，`startup-logs.json` 保存自动启动事件及日志行号，`ensure-command-result.json` 保存命令现场结果。REA 的 `rea-pe-evidence.json` 仅证明 PE 资源事实，不能用它替代上述函数分析和运行日志。

## 直接调用原生启动、停止函数

对当前复制的 `Formula.dll` 解包后核对导出和函数体，已经确认以下接口存在。这里给出的声明是根据修饰符号和反汇编还原的接口，不是券商公布的稳定 SDK：

```cpp
// 获取当前进程内的策略管理器实例。
CStrategyTradeManager* instance();
// 按策略 ID 启动，reason 是写入原生启动日志的 C++ 字符串。
void runStrategy(int strategy_id, const std::string& reason);
// 按策略 ID 停止，返回原生停止结果。
bool stopStrategy(int strategy_id);
```

已经观察到：

- `instance()` 读取本 DLL 的 `s_pInstance`；为空时才创建管理器。
- `runStrategy()` 使用管理器内部的策略表按 ID 查找，调用 `CTradeStrategyData::run()`，并发送策略状态变化信号。
- `CTradeStrategyData::run()` 检查运行状态及有效性，设置原生状态，再调用 `doRun()`。
- `runStrategy()` 本身返回 `void`，函数返回不证明 Python 初始化或桥接恢复成功。
- 当前 Qt 元调用表没有将 `runStrategy`、`stopStrategy` 注册成可按名称调用的槽；仅靠 `QMetaObject::invokeMethod(manager, "runStrategy", ...)` 不能建立这条调用链。

因此，“在 QMT 进程内使用已有管理器调用原生函数”是可继续验证的方向。外部 Python 自己 `LoadLibrary(Formula.dll)` 会加载到外部进程的地址空间，其管理器实例不是正在运行的 QMT 实例，不能因此控制已登录的客户端。[Windows 模块加载说明](https://learn.microsoft.com/en-us/windows/win32/api/libloaderapi/nf-libloaderapi-loadlibraryw)

调用封装还需要核对 C++ ABI：这是带隐含 `this` 参数的成员函数，`reason` 是 `std::string` 对象引用，不能直接传 `char*`。还应确认管理器的线程归属，并沿原生控制事件所在的线程调度；该私有函数在任意工作线程调用是否安全尚未实测。[Qt 线程与 QObject](https://doc.qt.io/archives/qt-5.15/threads-qobject.html)

待验证的执行方案为“外部命令 → 独立控制通道 → QMT 进程内的小型控制入口 → 原生线程上的 `stopStrategy/runStrategy` → 策略状态及 SHM/gRPC 验收”。控制入口应独立于被重启的交易桥接策略，否则停止桥接会同时失去启动通道。本次只完成函数及调用条件核查，尚未在运行中的 QMT 执行原生启动、停止调用。
