# QMT 原生策略控制与 DLL 注入

此模块针对已核对的东北证券 NET 专业版 `2.1.19.0`，从外部程序向已有 QMT 进程加载自有 DLL，并在策略管理器所属线程执行状态、启动和停止。它不改写券商 EXE、DLL、IP 检测、权限设置或白名单；私有接口的文件 SHA256 和导出 RVA 不匹配时拒绝操作。

## 控制链路

```text
外部 Python（可以是 SSH 会话）
  → 已核对 PID、进程创建时间、账号、模型 ID 和文件版本
  → 加载自有控制 DLL，再显式调用初始化导出
  → Global 命名共享内存、事件和生产者互斥量
  → QMT 进程内常驻通知线程
  → 管理器所属 GUI 线程的消息钩子
  → 已有 CStrategyTradeManager 的原生接口
```

加载阶段的 `DllMain` 只记录句柄。显式初始化安装控制通道，避免在加载器锁中等待线程或访问 QMT 的互斥量。常驻通知线程负责安装并持有钩子；临时初始化线程退出后控制入口仍可用。DLL 固定到进程生命周期，桥接策略停止时不会卸载它。

## 构建和部署

```bash
python tools/build_native_strategy_controller.py --output /data/code/artifacts/qmt-auto-start-20261014/native-controller-build
```

构建生产 DLL 与独立测试 DLL，并输出各自的 SHA256 清单。测试 DLL 只接受自建 Python 窗口进程，完全不绑定券商模块；生产 DLL 只接受 `XtItClient.exe`。Windows 使用 `native_control.py`、`QmtNativeStrategyControl.dll` 与对应 `.json` 清单放在同一目录。

构建目录同时包含控制入口与 `tests/`，Windows 可复现独立窗口测试：

```powershell
python .\tests\verify_native_controller_windows.py --stage .
```

当前部署目录为 `C:\Users\root\code\qmt-native-controller-20261014`。Windows 执行：

```powershell
$python = 'C:\Users\root\code\qmt-grpc-bigqmt-live-20261014\runtime\Scripts\python.exe'
$control = 'C:\Users\root\code\qmt-native-controller-20261014\native_control.py'
& $python $control inject
& $python $control status
& $python $control restart
```

`start`、`stop`、`restart` 会自动建立或复用当前进程的控制入口。QMT 完整客户端必须已运行并正常登录；此工具负责模型生命周期，客户端自身仍可通过原来的计划任务启动。默认绑定账号 `19314672`、模型 `QMT_TRADE_BACKEND`，每次操作重新检查配置及精确的进程代际。

`restart` 只用于已注册一个模型的桥接部署：先停止，保留回收窗口，再启动；除了原生计数，还等待本次启动新写入的 `READY account=...` 日志。`start` 的普通返回仅证明原生函数已返回，不代表 gRPC、账本或交易状态均已恢复。

已部署的 `restart-qmt-bridge.ps1` 在原生重启后继续检查外部 gRPC，保留 SDK 观察窗口。若出现 `NATIVE_REBUILD_BLOCKED`，只在排队交易命令为空、进程命令及父监督器匹配时回收这一代 gRPC 及工作进程，由原有监督器重建；它不清除交易故障锁或更换配置。此实例的恢复脚本位于部署目录 `recover_blocked_grpc_windows.py`，不属于通用原生 ABI。

恢复检查还等待调度器、执行器和对账退出启动阶段，完整状态单独输出；已有交易暂停不会因这条重启命令自动清除。

Ubuntu 上的一条远程命令：

```bash
ssh root@192.168.100.190 powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:/Users/root/code/qmt-native-controller-20261014/restart-qmt-bridge.ps1
```

## 协议与所有权

控制结构为 256 字节，初始化结构为 192 字节，UTF-16 字符宽度显式固定为 16 位。Windows 实际映射至少使用一个内存页，另有常驻线程的栈和内核对象；不能把协议字节数当作总内存占用。

外部互斥量序列化各生产者，只有一个未完成请求。输入字段先写、最后发布对齐的 64 位序列号；原生端用原子序列号提交完整回复。重复通知不会重复执行。超时后不覆盖未完成请求，也不自动重试；GUI 稍后收到已过期的请求会拒绝执行。

共享对象 ACL 仅授权系统、调用者和已核对的目标用户，保证 SSH 与桌面会话可通信。每个客户端关闭自己的句柄，DLL 自己持有映射、事件和钩子。完整 QMT 进程退出后，旧的指针与代际不可继续使用。

启动原因采用本次反汇编核对的 32 字节短字符串布局：缓冲区在对象起始处，容量字段在 `+0x18`；固定 14 字节内容、15 字节容量，不分配或移交跨运行库堆内存。它不是通用的 C++ ABI，当前 Formula.dll SHA256 固定为 `355fdbdc10d4661134b86725c479dbdfa3af5a1fb5a0d7c1977da39a73aacec4`。

## 实际验证

2026-10-15 已直接注入大赛 QMT 的 PID `9372`。命令在 GUI 线程 `2052` 执行，Qt 线程指针与管理器所属线程一致。实际停止、重新启动完成 `running_count: 1 → 0 → 1`，停止期间独立控制通道可查询，重新启动后观察到新的桥接 `READY`，客户端 PID 不变。

Windows 自建进程的 11 项检查通过：实际注入、重复注入、线程调度、模拟启停、错误目标拒绝、过期请求拒绝、超时后不迟到停止、不覆盖未完成请求、生产 DLL 错误宿主保护、40 次并发状态查询，以及客户端退出后通道继续存活。这些测试和实际原生启停不代表全天稳定性或交易执行性能。

另观察到外部 gRPC 旧 SDK 清理失败后不再重连。2026-10-15 01:03 对唯一失效子进程及其工作进程执行恢复，原监督器自动拉起新服务；随后账户、连接为 `READY`，完整对账 `PASSED`，交易门禁 `TRADING_READY`。这一状态来自实时 RPC，独立于原生启动返回值。

01:06 又通过完整远程命令执行一次原生重启和服务恢复；01:08 实时复查账户、连接 `READY`，对账 `PASSED`，交易门禁 `TRADING_READY`，QMT PID 仍为 `9372`。累计完成三次原生启停，没有提交交易订单。

证据保存在 `/data/code/artifacts/qmt-auto-start-20261014/`。相关 Windows API 设计参考 [CreateRemoteThread](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-createremotethread)、[命名空间与会话](https://learn.microsoft.com/en-us/windows/win32/termserv/kernel-object-namespaces)、[DLL 初始化约束](https://learn.microsoft.com/en-us/windows/win32/dlls/dynamic-link-library-best-practices)。
