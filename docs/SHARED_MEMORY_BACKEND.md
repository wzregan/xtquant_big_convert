# 共享内存通信后端

`get_backend("shm")` 取得成套提供 RPC 与推送的同机共享内存后端。实现位于：

- `src/bigqmt_signal_trader/transports/shm_primitives.py`：命名映射、跨进程互斥及唤醒提示。
- `src/bigqmt_signal_trader/transports/shm_transport.py`：请求、响应、截止时间与执行前检查。
- `src/bigqmt_signal_trader/shm_push_channel.py`：独立消费者游标、确认、保留和背压。

此后端不创建 TCP 或 Redis 通道。外部客户端访问 qmt-grpc 的 gRPC 连接仍保持原有机制；这里替换的是外部兼容 SDK 与大 QMT 之间的本机通信。它不修改 QMT、券商权限或安全策略，也不承诺 QMT 内嵌环境允许创建这些系统对象。

## 选择后端

两端使用相同账号和 `shm` 配置，特别是命名空间及所有布局容量。沿用现有配置入口：

```python
BIGQMT_REDIS_CONFIG = {
    "transport": "shm",
    "redis_enabled": False,
    "rpc_background_threads": False,
    "formula_server": {"enabled": False},
    "shm": {
        "namespace": "bigqmt",
        "push_codec": "json",
        "rpc_slots": 32,
        "rpc_slot_bytes": 65536,
        "max_payload_bytes": 64 * 1024 * 1024,
        "market_push_bytes": 24 * 1024 * 1024,
        "trade_push_bytes": 8 * 1024 * 1024,
        "market_push_slots": 4096,
        "trade_push_slots": 1024,
        "max_push_subscribers": 64,
        "publish_timeout_seconds": 0.02,
        "subscriber_retention_seconds": 300.0,
    },
}
```

配置名称保留兼容性，不代表仍通过 Redis 传输。账号、允许交易方法、QMT 主线程调度等业务检查继续由原有业务层控制。切换协议或布局需要停止旧实例并重建；运行中不能通过修改容量去解释旧映射。

示例关闭独立的 FormulaServer 行情直连路径，保证这些行情读取也走共享内存 RPC。
SHM 默认由策略回调轮询，QMT 的实际回调周期仍会影响延迟；独立进程基准不包含这部分调度耗时。

正式桥接的原生回报先进入本地有界队列，再由策略调度提交到共享日志。每轮刷新默认最多处理
32 条、使用 10 毫秒预算；短暂的消费者 ACK 锁竞争会使用本轮剩余预算等待，避免立即失败导致
下一批报单被误拒绝。持续占锁或日志容量不足仍保留队首并阻止新单，消息编号在恢复时保持不变。

使用原始推送接口时，客户端可在 `shm.subscriber_id` 配置稳定身份。不同进程、不同业务消费者不得共用正在活跃的身份；正式 SDK 会为行情与交易事件区分身份。省略身份时使用随机临时身份，不保证进程退出后的消息恢复。

正式 SDK 的自动身份在同一客户端实例内稳定，可用于其断线重连；需要跨进程重启恢复时，应在客户端显式配置固定且独占的 `shm.subscriber_id`，例如 `qmt-grpc-main`。多个消费进程应各用不同身份，不要将示例身份无区别地复制给所有进程。

推送默认固定使用 `push_codec="json"`，不因某一端安装了 msgpack 而自动改变编码。只有两端都明确配置 `push_codec="msgpack"` 且都安装该依赖时，才使用 msgpack。显式依赖缺失在构造阶段报错；共享头记录编码 ID，消费者注册之前检查两端选择，不兼容时启动立即失败。本次编码声明对应推送协议版本 2，不能与未声明编码的旧版本混用，也不进行隐式猜测或自动协商。

## Windows 对象和会话

Windows 实现使用分页文件支持的命名映射、命名互斥体和事件。同名映射允许独立进程打开同一内存区域；最后一个相关句柄关闭后，不应再假定内容仍然存在。因此这不是断电持久化日志。[Microsoft：Creating Named Shared Memory](https://learn.microsoft.com/en-us/windows/win32/memory/creating-named-shared-memory)

- `namespace="bigqmt"` 默认使用 `Local\` 对象作用域。
- `namespace=r"Local\bigqmt"` 显式限定当前 Windows 登录会话。
- `namespace=r"Global\bigqmt"` 显式请求跨会话作用域，适用于确实需要让服务会话和桌面会话互通的配置。

`Local\` 和 `Global\` 大小写敏感。非会话零进程创建全局文件映射可能需要 `SeCreateGlobalPrivilege`；打开已存在的对象仍受访问权限控制。实现不会在 `Global\` 创建失败后偷偷降级到 `Local\`，否则两端可能实际访问不同对象。[Microsoft：Kernel object namespaces](https://learn.microsoft.com/en-us/windows/win32/termserv/kernel-object-namespaces)

对象名由命名空间、账号和用途派生。不同账号隔离，RPC、行情/交易推送及各消费者唤醒对象也分别命名。`directory` 仅指定 POSIX 验证环境的映射目录，不能改变 Windows 的会话和对象访问权限。

## 实际默认参数

以下参数均位于 `shm` 子配置中。大小以字节表示，时间以秒表示。

| 参数 | 默认值 | 含义 |
| --- | ---: | --- |
| `namespace` | `bigqmt` | 两端共享的命名空间，默认 `Local\` 作用域 |
| `push_codec` | `json` | 固定推送编码；可显式选择两端均支持的 `msgpack` |
| `directory` | 系统默认 | POSIX 映射目录；Windows 不用它定位命名对象 |
| `rpc_slots` | 32 | RPC 在途槽位数量 |
| `rpc_slot_bytes` | 65536 | 每槽内联报文字节数 |
| `max_payload_bytes` | 67108864 | 单个 RPC 请求或响应的最大报文字节数 |
| `wait_poll_seconds` | 0.05 | RPC 遗漏唤醒时重查共享状态的间隔 |
| `connect_timeout_seconds` | 5.0 | RPC 初始化和服务端启动等待预算 |
| `market_push_bytes` | 25165824 | 行情推送的独立数据区，24 MiB |
| `trade_push_bytes` | 8388608 | 交易推送的独立数据区，8 MiB |
| `market_push_slots` | 4096 | 行情日志最多保留的消息索引数 |
| `trade_push_slots` | 1024 | 交易日志最多保留的消息索引数 |
| `max_push_subscribers` | 64 | 推送消费者身份槽数量 |
| `publish_timeout_seconds` | 0.02 | 单次推送等待锁和可用容量的总预算 |
| `init_timeout_seconds` | 5.0 | 推送初始化及等待发布者的总预算 |
| `lock_timeout_seconds` | 1.0 | 推送消费者确认、生命周期操作的锁等待上限 |
| `subscriber_retention_seconds` | 300.0 | 稳定身份退出后保留未确认游标的期限 |
| `push_poll_seconds` | 0.05 | 推送遗漏唤醒时重查日志的间隔 |
| `subscriber_id` | 随机临时身份 | 显式提供后支持同发布代次内的保留恢复 |
| `client_id` | 无 | 原始通道接受的 `subscriber_id` 兼容别名 |

默认 RPC 内联映射约 2 MiB。推送数据区共 32 MiB，另有约 0.33 MiB 元数据及索引。大于内联槽的 RPC 报文使用请求令牌专属映射；`max_payload_bytes` 是单条上限，并非所有并发映射的总内存上限。高并发大报文需要同时考虑槽位数量、请求及响应映射和进程堆内副本。

## RPC 的执行和超时边界

请求只提交一次，随机令牌和发布代次共同防止旧响应覆盖新请求。大报文也按请求令牌独立命名，并带长度及 CRC；槽位包含提交状态和元数据校验。

1. 客户端提交后，服务端先领取请求。
2. 即便请求已经进入 QMT 本地调度队列，也必须在真正执行前检查原截止时间和槽位所有权。
3. 执行前过期可以报告未开始；执行后超时、执行中进程退出等情况报告结果未知。
4. 结果未知必须用原请求编号和业务标识核对，不得把超时当作“没有下单”并自动重报。

`SharedMemoryTimeout.outcome_unknown` 区分已确认未开始与可能已执行。`SharedMemoryPeerRestarted` 表示对端会话变化；`SharedMemoryCorruption` 表示提交或校验损坏。这些状态不等同于业务调用成功。

SDK 同时保留响应中的 `outcome_unknown`、`request_id` 和 `error_code`。例如批量业务已执行但响应超过容量时，返回 `SHM_RESPONSE_ENCODING` 和结果未知；批量交易客户端不会因此回退成逐笔重报，而是按原有未知结果路径返回 `-4` 并保留核对信息。

停止服务时，必须等实际业务回调退出后才交出服务端身份。停止过程中暂时无法获取共享锁会显式失败并保留资源，可再次调用 `stop()`；不能因为关闭超时就启动另一个业务执行者。严重的槽元数据损坏会拒绝继续执行；在两端均停止、完成业务核对后，可使用新的共同命名空间重新建立端点，旧资源的离线清理不得与活实例并行。

通信层响应意味着该 RPC 的处理结果已返回；下单响应不代表成交。成交和委托变化仍走推送事件。

## 推送的交付契约

推送采用两条独立可变长度日志：`exec:` 开头的主题进入交易日志，其余主题进入行情日志。行情积压不会占用交易数据区或交易索引；共享互斥仍意味着它们不是完全独立的 CPU 调度通道。

每条记录包含序号、数据位置、长度、CRC、提交标记及可选消息 ID 摘要。单个发布者持有当前代次，消费者各有行情/交易游标。一个消费者成功处理消息后才确认；多个消费者之间的确认互不代替。

- `publish(topic, data, message_id=None, timeout_seconds=None)`：成功返回 `True`；未启动、超大报文、损坏或容量不足均明确抛错。
- `PushBackpressureError.retryable=True`：本次未提交，调用方可按剩余预算重试。`timeout_seconds=0` 只立即尝试，不等待可用容量。
- 满载时保留未确认消息，不静默覆盖慢消费者。正式集成层先把事件放入有界本地缓冲，再在允许的策略回调阶段有界刷新；容量耗尽和永久故障必须对新交易写入可见。
- 同 `message_id`、同编码内容在该消息仍位于保留日志时重复发布不会新增记录；相同 ID 的不同内容会被拒绝。回收记录或重建发布代次后，不承诺跨窗口幂等。
- 唤醒对象只是提示。提交后的提示创建、发送失败不会改报“未提交”；消费者会定时重查共享状态，诊断计数记录提示失败或遗漏。
- 同一日志内按序消费；两条日志之间不保证全局到达顺序。

`supports_acknowledgements=True` 表示具有消费者确认机制，不表示永久存储或“恰好一次”。业务回调已产生副作用、随后进程在 ACK 前退出时，同稳定身份恢复可能再次收到该消息；应用仍需按业务事件标识处理重复。

## 身份保留、主题变化与故障恢复

稳定身份正常停止或被发现死亡后，在保留期内保留其游标。重建相同身份、相同主题且发布代次未改变时，从未 ACK 的位置恢复。临时身份正常停止会释放槽位。

保留期到期的稳定身份留下缺口墓碑；旧身份再次连接收到 `PushGapError`，不能假装从头或从最新位置无损续接。墓碑也占身份槽，因此默认最多容纳 64 个不同稳定身份；部署应复用稳定业务身份，而不是持续生成新的“稳定”ID。

运行中可调用 `update_topics(topics)` 在同一日志范围内增删主题并保留 ACK。同一通道实例正常停止后，也可先显式更新主题再启动；恢复时校验原身份所有权。另一实例不能用改主题绕过恢复指纹检查。跨行情/交易日志迁移需要显式新订阅身份。

业务回调抛错时，通道停止消费、不 ACK，并暴露 `PushCallbackError`、`code="callback_failed"` 和 `status()["callback_failed"]`。失败证据独立于租约状态，普通关闭、死亡回收及保留期到期都不会悄悄解除服务端交易门禁。只有明确处理后在允许的保留范围内恢复该身份，或显式重建发布代次并核对业务状态，才应继续。

发布者重启生成新代次。旧消费者通过 `check_health()` 或下一次读取发现变化并报告 `PushGapError`；跨代次不承诺保留历史。CRC、提交标记、元数据范围异常或持锁进程非正常退出同样显式失败。共享日志不替代券商委托/成交查询和启动对账。

`check_health()` 的元数据锁等待最多 20ms；暂时锁忙是可重试状态，不应误判为需要不断重建消费者。永久故障和回调失败则必须保留诊断证据。`status()` 只提供本地快照，不主动证明对端存活；判断健康应调用 `check_health()`。

## Windows / Python 3.6 独立进程验收

先用目标 Windows Python 在 QMT 外运行本仓库脚本。脚本使用独立命名空间、合成账号和合成消息，会创建一个新的 Python 进程；不启动 QMT，不加载券商交易处理器，不下单。

在仓库目录执行：

```powershell
python -S tools\shm_acceptance.py --report shm-acceptance.json
```

若目标 Python 不在 PATH，直接使用其完整可执行文件路径：

```powershell
& 'C:\Path\To\Python36\python.exe' -S tools\shm_acceptance.py --count 100 --report shm-acceptance.json
```

`-S` 禁用 site-packages，脚本显式设置 `push_codec="json"`，因此无需安装 pytest、Redis、ZeroMQ、msgpack 或 grpc。脚本兼容 Python 3.6 的标准库接口。协议回归另覆盖一端可用 msgpack、另一端 `-S` 无第三方依赖的双向独立进程默认配置通信。

验收项目包括：20 个线程通过独立服务进程发送并发 RPC、约 1.25 MB 请求和响应的外部映射、包含冒号的两类推送主题、回调后的 ACK、稳定身份退出期间新消息的恢复。每个阶段有超时，结束时先关闭持有者，再清理仅由本次验收使用的命名对象。

成功时进程退出码为 `0`，JSON 包含 `ok: true`、不同的父/子 PID、RPC 成功数、推送数、ACK 数和恢复数。失败时退出码为 `1`，报告保留异常类型和原因。示例环境参数可用 `--help` 查看。

若确实需要验证全局对象创建权限，可额外运行：

```powershell
python -S tools\shm_acceptance.py --namespace-prefix 'Global\bigqmt-acceptance' --report shm-global-acceptance.json
```

该命令的父子进程仍由同一登录会话启动，因此只能验证所请求的 `Global\` 对象创建/访问，不能单独证明跨 Windows 会话访问已配置正确。真正跨会话部署还需分别从目标桌面会话和服务会话验证。

Linux/POSIX 上的测试和此验收脚本成功，证明该平台下的协议与独立进程路径；不能替代目标 Windows、目标 Python 3.6、QMT 内嵌运行时及完整交易业务链路的实际验收。
