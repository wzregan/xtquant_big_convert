# qmt-grpc 使用标准 XtQuant 兼容入口

本补丁将兼容责任集中在 `xtquant_big_convert`，qmt-grpc 继续使用现有 `XtQuantTradingGateway` 和行情 worker。QMT 内运行服务端；qmt-grpc 的独立 Python 环境安装本项目的 `xtquant` 兼容包。

## 保留原生 MiniQMT

- 原生 MiniQMT 环境继续加载券商提供的 `xtquant` SDK，现有行情、VIP 数据中心、交易、回调和对账路径保持原样。
- qmt-grpc 没有新增 BigQMT 后端类或桥接依赖。交易网关在下单时检测可选 `order_stock_with_context` 方法；原生 SDK 未提供该方法时，仍以原来的八个参数调用 `order_stock`。行情查询继续调用标准 XtData 接口。
- BigQMT 必须使用独立虚拟环境。本项目包含顶层 `xtquant` 包，不能安装到原生 MiniQMT 的现有环境中覆盖 SDK。
- 原生与 BigQMT 测试实例使用不同的数据库、日志、缓存、端口和凭证；不能让两个实例同时管理同一份交易账本。

## 已实现的兼容行为

标准入口 `from xtquant.xttrader import XtQuantTrader` 使用本项目 `sdk_trader.XtQuantTrader`。它复用已有桥接客户端的查询、订阅及事件实现；直接使用 `BigQmtXtTrader` 的旧桥接调用方式继续保留。

| 能力 | 行为 |
| --- | --- |
| 行情连接 | `xtdata.connect()` 校验桥接 ping；一秒内重复连接复用结果。`disconnect()` 关闭本地订阅与自有连接 |
| K 线回调 | 标准 `xtquant.xtdata` 入口输出证券到记录列表，时间为 Unix 毫秒；同一根 K 线的价格或成交量变化也触发轮询回调 |
| 资产快照 | 校验响应账户与四项金额，缺失、非法字符串、布尔值或无穷金额明确报错；真实零资产保留零 |
| 账户状态 | 查询 `ACCOUNT.m_strStatus`，将“正常”和东北证券实机的“登录成功”映射为 0；未知文本返回 INVALID，查询异常不转换成空列表 |
| 状态变化 | 每次查询结束后等待一秒再检查；RPC 等待采用客户端预算且最多五秒，允许历史查询排队；超时或服务端代次变化仍通知 `on_disconnected` 并要求重新对账 |
| 委托标识 | `order_id` 是普通整数，`order_sysid` 保留原始字符串；SQLite 映射覆盖账户、交易日和完整合同号 |
| 回调身份 | 委托、成交、报单错误、撤单错误使用同一映射；异步回报暂缺合同号时返回零并注明身份未确认 |
| 成交费用 | 缺少费用保留 `None`；明确的零费用保留零，交给 qmt-grpc 现有待结算机制处理 |
| 下单期限 | 新服务端声明 `execution_context_v1`，客户端透传 UTC Unix 秒截止时间，在实际 `passorder` 前复查期限、账户状态及服务端交易开关 |
| 未知结果 | 超时、断连或提交后未拿到合同号保留异常；只有明确的 `EXECUTION_NOT_STARTED` 返回拒绝，禁止盲目重试 |

账户字段来源：[迅投内置 API 数据结构](https://dict.thinktrader.net/innerApi/data_structure.html)。状态码遵循[迅投 XtTrader 文档](https://dict.thinktrader.net/nativeApi/xttrader.html)，其中正常状态为 `0`，不能将 ping 成功后合成的 `1` 当作正常。

终端账户查询仍是 QMT 本地事实快照，本补丁没有将其包装成同步查询柜台的保证。未知券商状态文本保持不可交易，实机验收时核对实际文本及掉线状态转换。

停止 SDK 时会等待在途账户查询退出后再关闭其传输，避免其他线程关闭仍在轮询的 ZMQ socket。账户监视查询失败会记录异常，不能只依据资产接口仍能查询来判断监视线程存活。

## 配置与版本约束

标准兼容客户端要求服务端声明 `account_status_v1`。连接旧服务端会明确失败，不会伪造正常状态继续交易。客户端连接不会自动同步或覆盖 QMT 部署文件。

RPC 端点和账户继续使用本项目现有 `BIGQMT_*` 配置。首轮使用 qmt-grpc 的附着式行情模式，`worker.xtdata_token_file` 设为 `null`；本项目未实现原生 VIP `xtdatacenter`。原生 MiniQMT 部署的 token 和启动方式无需改变。

设置 `BIGQMT_ORDER_ID_DB` 为独立测试环境下的持久化 SQLite 文件。默认位置是当前用户的 `~/.bigqmt/order_ids.sqlite3`。共享整数委托号的多个进程必须访问同一映射文件；跨机器不能各自建立不同映射后交换整数编号。映射库需随对应 qmt-grpc 账本备份，不能在重启或发布时清空。已有账本的切换和迁移不由本补丁自动执行。

首次出现的合同号会持久化写入；重复查询和回调命中有界内存缓存。编号采用数据库唯一约束，不使用可能碰撞的 CRC32 作为最终身份。

### 全市场截面批量

qmt-grpc 的 `server.full_tick_source_batch_size` 控制每次底层 Tick 读取的证券数，接受 1–10000 的整数，默认仍为 500。独立 BigQMT 实例可在实测后增大此值，例如：

```yaml
server:
  full_tick_source_batch_size: 6000
```

此参数合并到已有 `server` 配置，不替换监听地址等字段。首读最多 500 只，先返回第一批，再按配置合并后续读取；公开 gRPC 响应仍最多每批 500 项，原请求索引、重复证券、缺失与停牌状态保持原样。配置默认值不改变真实 MiniQMT 的读取批量。

证券元数据和交易规则改为共用本次读取的合约详情；下一次查询仍读取最新详情。这里没有跨请求缓存交易状态，停牌与复牌仍依据当前显式状态判断。资产查询继续读取原始资金，不使用旧资产缓存降低延迟。

对平价且字段不完整的 Tick，qmt-grpc 将当前批次需要核验的证券去重后批量读取详情。桥接提供可选 `xtdata.get_instrument_details(code_list)` 扩展（每次最多 500 个代码），服务端通过一次 `get_instruments` RPC 在 QMT 策略线程依次调用原生详情接口。公开 gRPC 协议不变；真实 MiniQMT 没有此扩展时仍调用原生 `get_instrument_detail`。新客户端连接旧桥接服务，仅在明确缺少批量方法时回退单项查询；超时或非法结果不能触发逐只重试。

批量详情失败项保留为空，完整行情和普通非平价缺失行情不会额外请求详情。只有本次有效详情明确说明停牌，才返回 `TICK_SUSPENDED`；详情缺失或复牌则保留原有完整性判断。该逻辑不依赖跨请求缓存。

行情 `connect()` 复用一秒内成功的真实桥接 RPC 通信，减少连续查询前重复 ping。FormulaServer 和本地缓存命中不能续期，超过一秒无成功桥接通信仍重新探测。原生 MiniQMT 的连接调用和账户监视机制不变。

## 执行边界

qmt-grpc 在自身所有者线程中继续检查本地交易门禁和有效期。可选方法只把有效期传给 RPC 客户端；QMT 服务端在原生报单前还会检查执行期限、服务端代次、真实账户状态和服务端交易开关。

客户端与 QMT 应运行在同一台 Windows 机器，保证执行期限使用同一系统时钟。若将其拆到不同机器，需要先验证时钟偏差。

本地门禁关闭不能单方面撤回已经交给远端的请求；已发送且结果不确定的委托继续进入 UNKNOWN 与对账流程。服务端交易开关是独立条件，本补丁没有把它冒充为 qmt-grpc 本地门禁的同步副本。

## 离线验收

桥接独立环境运行：

```bash
python -m pytest -q tests
```

指定 qmt-grpc 工作树的 `src` 路径，可以执行真实跨仓库契约测试：

```bash
QMT_GRPC_SOURCE=/path/to/qmt-grpc/src python -m pytest -q tests/bigqmt_signal_trader/test_sdk_client.py
```

该测试调用实际 qmt-grpc 网关、兼容 SDK、RPC 编解码及服务端下单逻辑，仅替换传输与 QMT 原生接口；覆盖行情启动、账户、资产、委托、成交回调和撤单。它没有启动真实柜台，也不能替代 Windows 盘中验收。

qmt-grpc 原生回归在其工作树运行：

```bash
umask 077
python -m pytest -q -p no:cacheprovider -m 'not live and not live_ha'
```

发布前还需要 Windows 独立实例的只读验证、实际账户状态样本、行情持续推送、断线恢复及非空历史委托/成交验证。离线测试不代表已经部署；部署与实机复测状态以对应验收报告为准，真实交易验收另行安排。

### Windows 行情与资产专项验证

`benchmarks/live_market_asset_assessment.py` 可以从独立目录加载两个工作树源码，读取现有本机配置，运行行情、资产、订阅及客户端重连测试。它在所有测试 RPC 客户端设置方法白名单，不允许下载、部署、报单或撤单；结果保留源码哈希、查询耗时和缺失历史覆盖。

标准 SDK K 线回调转换只作用于 `xtquant.xtdata` 模块入口，直接使用 `BigQmtXtData` 的旧帧回调保持兼容。当前 K 线更新仍由客户端轮询取得，默认周期三秒；休市首快照不能证明盘中更新时延。

### 2026-09-19：适配 master 的分钟流和历史 Tick

qmt-grpc 适配分支已基于 `origin/master` 的 `4689c2c` 重放，支持 `StreamBars`、
`GetBars(ENSURE_AVAILABLE)` 和 `GetTicksAt`。同时更新 QMT 内的桥接包与独立
qmt-grpc 环境的兼容 SDK；仅替换其中一端不能保证新参数及历史语义生效。

- 标准 `xtquant.xtdata.get_market_data_ex` 显式传 `subscribe=False`，并绕过公式直连、
  复权自愈下载和昨收补查。`LOCAL_ONLY` 缺失时保留缺失，不暗中订阅或下载。
  老终端如果不支持该参数，明确失败，不省略参数回退到自动订阅接口。
- 历史 Tick 的原生 `time` 毫秒列优先于展示用 `stime`。缺少 `time` 才解析明确源标签，
  保留毫秒；不把行号当时间，不裁剪零价行，不排序掩盖源数据乱序。
  gRPC 继续负责严格小于/包含时点、最大落后秒数、字段完整性和逐项状态判断。
- 下载完整透传 `incrementally=False`，强制刷新失败不能用客户端旧缓存报告成功。
  终端只有四参数全局下载时，用明确起点表示指定区间刷新；没有起点则拒绝非增量请求。
  逐证券下载任一失败不能被后续证券成功覆盖。
- 一分钟实时轮询默认取最近两根且 `fill_data=False`，先传递上一根最新内容，
  再由 qmt-grpc 的源推进规则闭合；比较两根内容，当前分钟未变也交付上一分钟晚到修订。
  现有 SDK 记录回调转换、GAP 和退订路径保持一致。
  它仍是轮询取得的原生分钟数据，不代表无损逐笔推送。
- qmt-grpc 在首次订阅和重连恢复时，有界暂存早于订阅响应到达的分钟事件，
  确认 worker 代次及句柄后按序投递；缓存溢出显式通知 GAP，失败请求清空暂存。
  订阅 RPC 不占用事件路由锁，已有行情仍能接收。
- 新历史 RPC 继承原有总预算和取消信号。取消会结束后台 IPC 排队/收包等待；
  已进入终端原生调用的工作仍不能被强制中断。

只读查询和全局下载参数依据[迅投 QMT 行情说明](https://dict.thinktrader.net/innerApi/question_answer.html)，
标准 SDK 时间字段及下载参数依据[XtData 文档](https://dict.thinktrader.net/nativeApi/xtdata.html)。

离线跨仓库测试需要把两个匹配工作树的 `src` 都加入 `PYTHONPATH`：

```bash
cd /data/code/qmt-grpc/.worktrees/bigqmt-backend-integration
umask 0077
PYTHONPATH=src:/data/code/xtquant_big_convert/.worktrees/qmt-grpc-sdk-compat/src \
  python -m pytest -q tests/integration/grpc/test_bigqmt_market_compat.py \
  tests/unit/market_data/test_history_request_cancellation.py
```

这些测试调用真实桥接处理器、兼容 SDK、本地认证 ZMQ、TLS gRPC 和公开客户端，
QMT 行情源及桥接 RPC 网络为测试替身。本轮未部署 Windows；盘中仍需核对终端参数支持、
历史下载完成后的覆盖、分钟闭合和时延，不能把离线通过当作实机验收。

只读资产查询可以单独验证 SDK 查询和 qmt-grpc 金额映射。若现有服务端没有 `account_status_v1`，标准交易生命周期仍会拒绝连接，不能将资产查询通过解释为完整 gRPC 服务已经可以启动。
