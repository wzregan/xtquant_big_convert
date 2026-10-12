# BIG_CONVERT 与独立 qmt-grpc 一起启动

安装对应版本的两个项目后，在 Windows 上执行 `deploy/install_grpc_companion.ps1`，传入独立实例目录、配置文件和大 QMT 的 `python` 目录。安装器只配置指定任务，运行中的旧任务需先停止。已有启动脚本、任务定义和本地配置会备份到实例目录。

```powershell
.\install_grpc_companion.ps1 `
  -InstanceDir C:\Users\root\code\qmt-grpc-bigqmt `
  -ConfigPath C:\ProgramData\qmt-grpc-bigqmt\config.yaml `
  -QmtPythonDir 'C:\东北证券NET专业版(测试版)\python'
```

在大 QMT 中启动 BIG_CONVERT 后，初始化和后续回调会异步触发 `qmt-grpc-bigqmt`。计划任务的 IgnoreNew 和伴随进程的文件锁共同避免重复启动。任务在 `WAITING_FOR_BRIDGE` 状态等待真实 RPC ping；桥接就绪才创建 gRPC 子进程。子进程异常退出按 2～60 秒退避重试，持续运行一分钟后重置退避。`CHILD_RUNNING` 仅表示子进程存在，业务是否可用仍看 GetSystemStatus。

伴随日志位于独立配置目录下的 `logs/companion.log`。停止自动拉起时，将本地配置的 `BIGQMT_GRPC_COMPANION["enabled"]` 设为 False，重新加载 BIG_CONVERT，再停止对应计划任务。启动器不会重启 QMT、初始化账本或改变下单权限。

## 策略运行模式与配置生效

柜台验收时，应同时核对登录柜台和策略运行模式。东北证券模拟柜台下，策略的“实盘”模式会将委托发送到该模拟柜台；“模型模拟”使用策略内部撮合，不能证明柜台报撤单链路可用。模式名称本身不能证明账户是否使用真实资金，验收记录应保留登录柜台身份和原始委托回报。

修改入口读取的 `rpc_allow_order_methods` 或伴随启动配置后，需要在大 QMT 中停止并重新启动对应策略，再用 `ping` 检查实际开关。包热重载成功不代表策略入口保存的配置已更新。交易是否恢复仍以完整对账和 `TRADING_READY` 为准。

多个 SDK 客户端核对同一柜台委托时，必须共用相同的 `BIGQMT_ORDER_ID_DB`。该映射把柜台委托号转换为兼容 SDK 的整数订单号；各用一份数据库会造成跨进程编号不同，不能据此判定柜台丢单。详细部署要求见 [兼容说明](QMT_GRPC_COMPAT.md)。

## ETF 分类与可卖数量

配套 qmt-grpc 的柜台可卖确认版本不再使用 ETF 分类决定买入后何时可卖。策略只需读取子账户的 `Position.available`；已成交但待柜台确认的份额暂记 `in_transit`，分类缺失不再阻断正常委托，价格、数量、资金与风控仍照常校验。

伴随进程不再启动原生 MiniQMT 分类导出，不会周期刷新 `etf-reference.json`。安装脚本的 `NativePython` 和旧启动器的 `--reference-python`、`--reference-exporter` 参数保留兼容并忽略；旧分类文件环境变量不会传给服务子进程。更新 qmt-grpc 时需同时采用包含此行为的配套版本。

转换 SDK 中独立使用的参考文件解析接口仍保留，但它不再是正常 gRPC 下单或可卖确认的依赖。显式周期可作为参考返回；策略不能据此越过自己的可卖余额。

## 数据语义

无效 `OpenDate`（如 19700101）输出为 None，同时保留 `RawOpenDate` 与 `INVALID_OPEN_DATE`。午夜且零成交的证券占位 Tick 在 gRPC 返回 `TICK_PLACEHOLDER`，不作为交易参考价或订阅更新。完整公开 Tick 仍要求所有必需字段。

SDK Tick 的 `source_clock_ahead_ms` 与 `time_quality_flags` 标记源时间领先；原始 `source_time`、`received_at` 不做平移。这两个不同来源的时钟之差不能称为网络延迟。调用耗时需用调用侧单调时钟单独测量。

相同参数的重叠 Tick/批量详情读取可以共享一次 IPC 往返；请求完成后立即移除结果，后续请求重新读源。每位调用者独立保留总预算和取消权利。串行 SDK 通道中，不超过 100 只证券的查询优先于尚未开始的大查询；排队超过一秒后按到达顺序调度以防饥饿。正在执行的 SDK 调用不能被抢占。
