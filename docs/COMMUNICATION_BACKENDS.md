# 大 QMT 通信后端

外部 qmt-grpc 与大 QMT 之间的通信由转换层提供。后端只负责消息收发；
行情查询、交易权限、账号校验、委托确认和 QMT 原生 API 的线程约束仍由原业务层负责。

```text
外部 qmt-grpc / 行情 worker
    -> 兼容 SDK
    -> CommunicationBackend
         -> RpcTransport：请求及响应
         -> QuotePushChannel：行情与交易回报
    -> 大 QMT 桥接策略
    -> 原生 QMT API
```

qmt-grpc 对客户端提供的 gRPC 接口，以及主进程与行情 worker 的 IPC，不在这个后端的职责内。

## 接口与所有权

接口定义在 `src/bigqmt_signal_trader/transports/backend.py`，通过
`bigqmt_signal_trader.transports` 导出：

- `CommunicationBackend`：成套提供 RPC 工厂、推送工厂与调度能力，不持有连接。
- `get_backend(name)`：按配置名称选择后端。
- `register_backend(backend, replace=False)`：在初始化前注册；重复名称默认报错。
- `build_transport(name, config, ...)`：保留原工厂入口，内部委托给同一注册表。

RPC 工厂接收 `config、account_id、print_prefix`，返回 `RpcTransport`。
推送工厂额外接收 `role`，取值为 `client` 或 `server`，返回 `QuotePushChannel`。
工厂应只构造对象；创建出的通道由 SDK 或桥接服务启动和关闭。
RPC 服务端另外在 `config["server_side"]` 中传入 `True`，供后端准备服务端专用依赖；
客户端未设置时视为客户端。Redis 的接收连接超时等细节由 Redis 工厂自行处理。

SDK 构造时固定后端定义和配置快照，RPC、推送及回报监听沿用同一个定义。
`replace=True` 只影响此后创建的 SDK/服务；已经创建但尚未首次连接的 SDK 也保持原定义。
配置中的字典、列表、元组递归复制，连接、锁等注入对象保留身份和所有权。
首次并发调用通过初始化锁创建一个 RPC 实例，后续请求直接复用实例。

`RpcTransport` 的业务契约：

| 方法 | 语义 |
| --- | --- |
| `send_request(request, timeout_seconds)` | 按请求编号等待响应，超时秒数来自调用方 |
| `start_receiving(on_request, background_threads=...)` | 安装接收回调；回调可以只入队并延后回复 |
| `send_response(request, response)` | 回复原请求，保留其请求编号 |
| `stop()` | 释放本实例持有的资源 |
| `drain_request_queue(max_items)` | 声明支持策略回调轮询时必须实现，非阻塞收取有界批次 |

`QuotePushChannel` 实现 `start_publisher、start_subscriber、publish、stop`。
订阅回调接收 `(topic, data)`；`publisher_endpoint` 提供诊断端点，
没有独立端点时可沿用基类的空字符串。行情和交易回报使用同一接口，交易主题仍为
`exec:order、exec:trade、exec:order_error、exec:cancel_error`。

后端能力声明：

- `supports_scheduler_polling`：RPC 可由 QMT 策略回调非阻塞收取。
- `default_background_threads`：未配置接收线程时的默认行为。
- `execution_events_via_push`：可通过推送通道接收交易回报，只声明路由能力。
- `redis_auxiliary_default`：是否保留旧 Redis 日志等辅助功能的缺省开启行为，新后端默认关闭。
- `push_depends_on_redis`：推送本身依赖 Redis；Redis 日志不可达时，这条通道不能充当独立故障回退。
- `acknowledged_push`：回调完成后确认消费，队列容量不足时显式背压；不代表掉电持久化。

声明支持策略轮询却没有实现 `drain_request_queue`、工厂返回 `None` 等错误会在创建时报告。
纯后台接收后端的基类 `drain_request_queue` 为无操作；不支持轮询的后端不能关闭默认接收线程。
策略组装、RPC 启动、发布端启动中任一步失败，都会清理已取得的通道、清空运行状态并保留原异常，允许重新启动。

注册表只保存后端定义，各 SDK、服务实例分别创建自己的通道。
它不提供运行中的连接迁移或热切换。

## 已有后端

| 名称 | RPC | 行情与交易回报 | 默认接收线程 |
| --- | --- | --- | --- |
| `redis` | Redis | Redis | 关闭，保持历史默认值 |
| `zmq` | ZMQ ROUTER/DEALER | ZMQ PUB/SUB | 未显式配置时开启 |
| `mysql` | MySQL | 保留历史 Redis 组合 | 未显式配置时开启 |
| `shm` | 同机共享槽位及大报文映射 | 分开的行情、交易确认日志 | 关闭，策略线程轮询 |

以上 RPC 后端均支持策略回调轮询。配置 `rpc_background_threads=False` 时，
原生 API 仍由 QMT 策略回调执行；换通信方式不会把交易调用移到接收线程。

兼容已有 Redis 回报日志：配置并且可达时仍优先使用 Redis，失败时可回落到后端推送。
显式设置 `redis_enabled=False` 可禁止额外的日志、缓存、元数据和发现连接；
`False`、`0`、`"false"`、`"off"` 等常见布尔配置按统一规则解析，服务端也遵守该开关。
Redis RPC 和 MySQL 后端的 Redis 推送属于所选后端本身的依赖，此开关不会将它们改造成无 Redis 协议。
Redis 日志未启用时，Redis 后端可使用原生推送通道；日志开启但 Redis 故障时，客户端继续重试原通道。
缺少推送实现的后端会明确报错，不会自动改用 Redis 或 TCP。

基础推送接口没有隐含的 ACK、重放或可靠交付承诺；SHM 显式声明确认能力，
其回调 ACK、保留窗口、背压和故障恢复边界见 [共享内存后端](SHARED_MEMORY_BACKEND.md)。
ZMQ/Redis 都隔离畸形消息，保留完整主题（包括 `exec:order` 中的冒号），支持回调内停止订阅。
慢回调仍会拖延消费，PUB/SUB 在积压时可能缺帧；委托、成交最终状态应由业务层通过查询或可靠日志核对。

## 配置切换

服务端和客户端继续使用现有 `BIGQMT_REDIS_CONFIG`，字段名保持兼容：

```python
BIGQMT_REDIS_CONFIG = {
    "transport": "zmq",
    "redis_enabled": False,
    "rpc_background_threads": False,
    "zmq": {
        "connect_address": "tcp://127.0.0.1:15632",
        "quote_push_connect_address": "tcp://127.0.0.1:15633",
    },
}
```

上例是客户端配置；QMT 端相应使用 `bind_address` 和 `quote_push_bind_address`。
省略显式推送地址时，ZMQ 后端从实际 RPC 地址派生同主机、端口加一的地址。
非 TCP RPC 地址必须显式指定推送地址，不能据此推测一条 TCP 通道。

新后端的自定义参数放在通用字段中，客户端和 QMT 运行入口都透传：

```python
BIGQMT_REDIS_CONFIG = {
    "transport": "custom",
    "redis_enabled": False,
    "rpc_background_threads": False,
    "backend_config": {
        "custom": {"endpoint": "local-channel"},
    },
}
```

工厂收到展开后的 `config["custom"]`。已有外层字段优先于同名通用字段，
通用字段不能覆盖桥接的交易权限及账号门禁。
两端需要选择同一个后端并配置匹配端点，然后重启各自 SDK、服务和桥接策略。

## 新增后端

实现 RPC 和推送两个接口，在两端创建 SDK 或桥接服务之前导入注册模块。
以下两个通道类是需要自行编写的实现：

```python
from bigqmt_signal_trader.transports import CommunicationBackend, register_backend
from my_bridge_transport import CustomRpcTransport, CustomPushChannel

register_backend(CommunicationBackend(
    "custom",
    rpc_factory=CustomRpcTransport,
    push_factory=CustomPushChannel,
    supports_scheduler_polling=True,
    default_background_threads=False,
    execution_events_via_push=True,
))
```

上述类的构造签名须与工厂契约一致；两端都要提供此注册模块。
采用 QMT 单文件部署时，注册模块及实现也需要嵌入构建产物。
`shm` 已内置为正式后端；独立的 TEST4 探针不等同于该后端的部署和验收。

## 验证范围

`tests/bigqmt_signal_trader/test_communication_backend.py` 注册一个测试用进程内后端，
让正式 RPC 服务、处理器、SDK、行情订阅会话及交易回报转换共同收发消息。
它验证配置透传、账号及交易权限门禁、资源清理、端点派生和禁止隐藏的通道回落。
它不是 Windows 共享内存实现或性能验收。

`test_communication_backend_review.py` 补充注册替换、配置隔离、并发初始化、
禁止隐式 Redis、服务端真实组装及启动失败恢复；`test_redis_backend_rpc.py`
验证账号和回复路由、TTL、超时不重发、格式协商；`test_push_channel_resilience.py`
验证坏帧恢复、启动失败资源清理和回调内停止。

本地性能脚本为 `benchmarks/communication_backend_benchmark.py`，分别测量直接构造与后端工厂，
报告创建耗时、稳态 RPC 的成功数/分位数/吞吐，以及推送收到数、缺失、重复和逆序。
这些合成负载不包含 QMT 回调调度、券商处理、实盘成交或整日稳定性。
