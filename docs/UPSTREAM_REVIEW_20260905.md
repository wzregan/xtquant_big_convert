# upstream/main 逐提交审阅与 rebase（2026-09-05）

## 范围与方法

- 原分支基点：`77b536c5676b69f30d3b85080902768f0f441a65`（0.2.5）。
- 上游固定端点：`d07d4d5802a88babd33e54b37c2377a3fd734a86`（0.3.22）。
- 共 **276 个提交：212 个非合并提交、64 个合并提交**。下表按审阅使用的 Git 拓扑顺序逐条记录。
- 审阅覆盖每条提交的补丁：运行代码、测试变化、文档及发布元数据。合并提交另检查父提交和解冲突差异；6 条包含 combined diff，另 58 条没有额外的 combined diff。
- 验证方式是上游端点基线、合并冲突相关回归、新增缺陷复现以及最终端点全量回归；**没有声称 276 个历史提交分别构建或运行过测试**。
- 本地优化先保存为 `b0cdf9d`，然后 rebase 为 `824921f`。备份分支为 `backup/main-before-upstream-rebase-20260905-b0cdf9d`。

## 审阅发现与处理

| 优先级 | 问题与影响 | 最终处理 |
|---|---|---|
| P1 | 多账户共用观察表，仅按备注/系统号查询，可能把其他账号的委托号或已撤状态作为本账号事实 | 观察表所有键带 account_id，提交和撤单确认严格读取本账号；缺账号只进入匿名空间 |
| P1 | 执行事件载荷含次级账号，但发布主题、策略名恢复及无系统号事件配对仍使用主账号或不含账号 | 正常回调、废单错误和暂存到期发布都采用事件账号；配对键包含账号；客户端在屏障和回调前过滤其他账号 |
| P2 | 因子日期解析先截前 13 位当毫秒时间戳，误读 YYYYMMDDhhmmss/ISO 日期；整数日期索引按标签取行导致丢行 | 日期格式优先验证，秒/毫秒时间戳单独解析；帧批量转行并保留时间索引 |
| P2 | “过半缺码”使用向下取整阈值，1/3 或 1/2 缺失也触发下载和等待 | 去重后使用严格过半判定；少数缺码不承担重读成本 |
| P2 | ZMQ stop 关闭其他线程的 DEALER，但线程局部仍保存旧句柄 | 再次使用检查 closed 并重建；不改变停止过程对无在途请求的要求 |

多账户最初 7 个用例在修复前失败，接收端另外 2 个用例也先复现失败后修复；日期/阈值/生命周期边界 15 个用例修复前为 11 失败、4 通过。复现均使用内存对象、替身或本进程套接字，不连接柜台。

## 合并决策

- 保留上游批量异步下单、独立回调线程、退出排空及 `7c97e63` 的“未知结果不重提”修复。本地后台队列只负责查询与撤单，不覆盖新的下单调度器。
- 保留本地 DataFrame 列式协商、完整索引/类型恢复，同时保留上游 Panel、原生标量快路和无需类型恢复时跳过遍历的优化。
- 保留有界内存缓存、部分字段合并、无变化免写，结合上游 MiniQMT 时间索引和默认下载字段修复。
- 委托观察表命中无需轮询；未命中时，同一轮多个待确认提交按账号共用一次查询。撤单仍沿用独立状态确认流程。
- `fill_data=False` 不能因旧终端参数不兼容而被静默省略；默认填充仍能使用旧签名。同步下单返回上游的整数子类 `OrderId`，`str(order_id)` 可取柜台系统号。
- 单文件生成器同步新增持仓刷新配置；Linux 路径测试按实际绝对路径断言。

## 使用边界

- 市场级 `get_full_tick(['SH'])` 默认只包含股票；全品种需 `types=['all']`。这是上游行为变化。
- `OrderId` 是桥接生成的整数，不等同于 MiniQMT 柜台内部整数 ID；取回原系统号和跨进程持久化仍应明确约定。
- 因子范围回退依赖已有日线的 `preClose/close` 差异找候选，最多探测 40 个候选日，不能作为任意长历史的完整因子表保证。
- 期权分析的 `as_of` 用于估值时间；自动行情取最新值，不是历史时点快照。历史计算应提供对应时点价格。
- 离线测试不能证明 Windows QMT、券商权限、真实回调时序或交易结果。本次未运行会下单的 `test_all_apis.py`，未部署、重启终端或推送远端。

## 验证

- 上游独立检出全量：**1582 通过、1 失败、1 跳过**；失败为 Linux 下 Windows 安装路径测试的预期路径，本地已有的可移植断言修复保留。
- 最终分支全量：**1663 通过、1 跳过，另有 3 个子测试通过**，用时 63.74 秒；唯一跳过是未安装可选 `pymongo bson` 的 FormulaServer 用例。
- 全量包含批量超时不重提、异步回调顺序、多账户确认与推送、缓存、编解码、单文件生成与加载等离线回归。
- 15 个相对上游修改的运行文件通过 Python 3.6 语法解析；这不是 QMT 内置 Python 3.6 的运行验证。
- `git diff --check` 通过；上游独立检出保持干净；276 条表格记录与审阅清单逐 SHA 匹配。

实际测试命令（DBUtils 3.2.0 仅装在临时依赖目录）：

```bash
PYTHONPATH=/tmp/xtquant_big_convert_test_deps PYTHONDONTWRITEBYTECODE=1 \
  python -m pytest tests -q -rs -p no:cacheprovider
```

验证元数据与源文件指纹见 [验证记录](UPSTREAM_REVIEW_20260905.validation.json)。

## 性能复核：上游 0.3.22 与集成结果

使用同一个 `benchmarks/offline_performance.py`，按顺序在同一机器运行上游独立检出和当前源码。Python 3.10.20、Pandas 2.3.3；预热后 7 次取中位数。行情是 100 个标的 × 240 根 × 6 字段的合成数据，缓存为临时 Parquet，RPC 是内存替身并禁止真实网络连接。

| 指标 | 上游 0.3.22 | 集成结果 |
|---|---:|---:|
| 完整行情编码 | 122.712 ms | 41.84 ms |
| 解码并重建帧 | 43.745 ms | 27.593 ms |
| 含外层编码的报文字节 | 3741901 字节 | 1757637 字节 |
| 2 万行缓存热读末尾 1 行 | 6.603 ms | 0.036 ms |
| 尾行未变化重复写 | 14.775 ms | 3.024 ms |
| 尾行变化实际落盘 | 14.946 ms | 10.111 ms |
| 20 笔待确认提交，每轮完整查单次数 | 40 次 | 1 次 |
| 模拟 50 ms RPC：异步查询调用方耗时 | 50.115 ms | 0.025 ms |
| 模拟 50 ms RPC：异步撤单调用方耗时 | 50.158 ms | 0.022 ms |
| 20 轮空闲调度：持仓 / 资产查询 | 40 / 20 | 1 / 1 |

当前编解码直接回环的帧逐一通过 `assert_frame_equal`；上游直接回环把原索引展开为列，未通过此项。这是编解码组件对比，不代表所有上游公共行情方法都丢索引，因为部分入口还会重新整理时间轴。异步耗时是调用方入队返回时间，后台 RPC 仍需约 50 ms，不能解读为柜台执行提速。

原始结果：[上游](../benchmarks/results/20260905-upstream-0.3.22.json)、[集成后](../benchmarks/results/20260905-rebased.json)。首次优化相对 0.2.5 的数据保留在 [历史记录](PERFORMANCE_OPTIMIZATION_20260905.md)。

## 逐提交记录

“保留”表示最终集成决定，不表示该历史提交单独通过所有测试。历史缺陷由后续提交修正时在该行注明。

| 序号 | 提交 | 标题 | 审阅结论 |
|---:|---|---|---|
| 1 | `9fb51e5` | Callback naming consistency, error-event fields, tick download race, call_async (#63 #64 #65 #66) | 回调别名、错误字段、tick 下载等待及 Future 请求；保留新增接口。 |
| 2 | `de5eaa8` | Hold order_error/cancel_error behind the async-response barrier too (#51) | 错误事件也遵守异步提交响应屏障，避免用户先收到委托错误后收到请求序号。 |
| 3 | `ef2f66e` | Release 0.2.6: callback field alignment (#64 #65), tick download race (#66), call_async (#63), barrier covers error events (#51) | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 4 | `10be6ba` | fix: Redis 5.0 + redis-py 8.x compatibility (protocol passthrough + m_ field aliases) | Redis protocol 透传及原生字段别名；本提交的位置别名变量缺陷由第 6 条修复。 |
| 5 | `ac0bdd3` | test merge pr67 | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 6 | `9087d66` | Fix NameError in PR #67 position aliases + add regression test | 修复位置字段别名 NameError，并加入对象字段回归。 |
| 7 | `c11b025` | Merge PR #67: redis-py 8.x RESP2 protocol passthrough + native m_ field aliases | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 8 | `94c250d` | Release 0.2.7: redis-py 8.x/Redis 5.x protocol passthrough + m_ field aliases (PR #67, @sunjian710) | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 9 | `dbac642` | fix(bigqmt): parse POSITION futures codes correctly; support BIGQMT_ACCOUNT_TYPE | 期货持仓代码和账号类型解析，保留大小写及市场映射。 |
| 10 | `1bb784e` | Merge pull request #68 from ReCodeLife/main | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 11 | `6eeab9a` | Isolate an unparsable row so it cannot take down the query | 坏行隔离到单行，其他有效查询结果仍可返回。 |
| 12 | `ed84d90` | Merge pull request #70 from litaolemo/fix/isolate-unparsable-row | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 13 | `70cf1eb` | Release 0.2.8: futures position codes (#68), per-row query isolation (#70) | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 14 | `9043380` | redis-py 3.x compat for protocol kwarg (#71), real order id in async_response (#72) | 兼容旧 redis-py；异步响应等待真实系统号，保留超时边界。 |
| 15 | `6965af8` | Release 0.2.9: redis-py 3.x protocol compat (#71), real order id in async_response (#72) | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 16 | `52ab104` | refactor(xtconstant): reverse constant dependency and port full native impl | 常量依赖反转；静态比对本地参考 SDK 的 533 个常量，无缺项和数值差异。 |
| 17 | `43c7bdd` | refactor(xttype): expand type definitions and sync compat imports | 原生对象字段与构造器扩充；保留兼容导出，不把静态类型存在等同于柜台能力。 |
| 18 | `35a5886` | Merge pull request #73 from ReCodeLife/main | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 19 | `ab971c0` | Break the xtquant/xtquant_compat import cycle | 惰性导入解除 xtquant 与兼容层循环依赖。 |
| 20 | `e7df920` | Merge pull request #74 from litaolemo/fix/xtquant-import-cycle | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 21 | `6af1f45` | Stop building a Redis client on every adjust tick | 复用下载任务 Redis 客户端，避免每个 adjust 周期重建连接池。 |
| 22 | `932034d` | Merge pull request #79 from litaolemo/fix/download-jobs-redis-leak | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 23 | `9e1b424` | Deliver exec events over zmq, not only Redis (#76) | 成交委托事件增加 ZMQ 推送路径；与后续 Redis 降级及监听选择修复共同保留。 |
| 24 | `8ccbc1d` | Merge pull request #80 from litaolemo/feat/zmq-exec-events | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 25 | `19305c3` | Release 0.2.10: zmq exec events (#76), Redis client leak (#79), import cycle (#74) | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 26 | `a322e93` | Add traded price to order callbacks | 订单回调补充成交均价，保持默认空值转换。 |
| 27 | `5941743` | Merge pull request #82 from yuchiwang/main | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 28 | `d84377f` | feat: implement query_position_statistics futures position statistics | 期货持仓统计；其新增无门控日志由第 30 条移除。 |
| 29 | `2c1a812` | Merge pull request #81 from ReCodeLife/main | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 30 | `4c7d1cb` | Remove ungated debug print from _publish_response (#81 follow-up) | 删除可能打印完整响应的无门控调试输出。 |
| 31 | `775381d` | Fix two single-file QMT sandbox failures (#76) | 处理沙箱函数体星号导入和 C++ 回调线程首次导入失败。 |
| 32 | `732ea5d` | Merge pull request #83 from litaolemo/fix/qmt-sandbox-loading | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 33 | `f6f906b` | Release 0.2.11: position statistics (#81), traded price (#82), QMT sandbox loading (#76) | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 34 | `7e182e9` | fix(bigqmt): pass account type to compact queries | 信用合约查询传入账户类型，避免固定 STOCK。 |
| 35 | `a6eeef6` | Merge pull request #87 from ljjtim/codex/credit-compact-account-type | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 36 | `d93fc74` | Integrate the single-file QMT build generators (#56) | 单文件生成器纳入仓库；合并时同步新增持仓刷新配置。 |
| 37 | `bba7e9c` | Add bigqmt-init, an interactive setup wizard | 交互初始化向导；ZMQ 默认端口与主机问题由第 137–138 条修正。 |
| 38 | `2839bf0` | Merge pull request #89 from litaolemo/feat/single-file-build-tools | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 39 | `2f8da7f` | Say what was searched when account_id is missing (#90) | 缺少账号时输出查找位置诊断，不输出凭据。 |
| 40 | `d0d0a52` | Merge pull request #91 from litaolemo/fix/account-id-error-message | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 41 | `18e9eed` | Release 0.2.12: bigqmt-init wizard, single-file QMT builds (#56) | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 42 | `e140c31` | Make account_type discoverable and visible (#92) | 从服务端发现实际账号类型并展示配置不一致。 |
| 43 | `9ade2f3` | Merge pull request #93 from litaolemo/fix/account-type-discovery | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 44 | `822e74b` | Release 0.2.13: account_type discovery (#92), traded_price confirmed live | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 45 | `5676eed` | feat(compat): 打新支持 - 接通 query_new_purchase_limit + 新增 ipo_subscribe | 申购额度接真实 RPC；本地继续保留错误可见性及策略线程调度。 |
| 46 | `001ffdd` | fix(compat): get_ipo_data 传参修复 - type 而非 account_id (8-28) | IPO 参数改为 type，避免误传 account_id。 |
| 47 | `769434f` | fix(compat): get_ipo_data 需主线程上下文 (加入 LISTENER_DEFERRED_METHODS) | IPO 查询转交策略主线程，符合交易上下文约束。 |
| 48 | `e852436` | feat(strategy): 子项目内置每日打新 (get_ipo_data + passorder, 8-28) | 曾自动每日申购；第 52 条完整移除此自动执行路径。 |
| 49 | `7301b0a` | fix(strategy): 打新幂等前置标记, 防止 adjust 重复触发 (8-28) | 自动申购幂等标志前置；所属自动申购逻辑最终已删除。 |
| 50 | `8a8a001` | feat(strategy): 打新只打沪深 (SH/SZ), 排除北交所 (8-28) | 自动申购限沪深；最终仅显式 API 保留默认沪深市场过滤。 |
| 51 | `cdc9ee6` | Merge pull request #96 from ThomasAnderson01/feat/ipo-subscribe | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 52 | `687eb64` | Make IPO subscription a call, not a behaviour (#96 follow-up) | 移除定时自动申购，只有显式调用才会下单；确认最终行为保留。 |
| 53 | `e245a05` | Keep get_ipo_data's mapping shape (#96 follow-up) | IPO 查询保留代码到详情的映射，不再被行归一化清空。 |
| 54 | `63456d7` | Merge pull request #98 from litaolemo/feat/ipo-explicit-api | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 55 | `0c244fb` | Document the IPO subscription interface | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 56 | `57e0368` | Merge pull request #99 from litaolemo/docs/ipo-readme | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 57 | `8e0f241` | Release 0.2.14: IPO subscription as an explicit API (#96, #98) | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 58 | `43a3cb3` | Make subscribe_quote an actual subscription (#95) | 真实 tick 推送与 K 线轮询订阅，保留取消订阅清理。 |
| 59 | `686e9cf` | Narrow whole-market snapshots to stocks by default (#104) | 行为变化：市场级全 tick 默认只含股票；需要全品种时显式 types=['all']。 |
| 60 | `f4352f0` | Keep the whole-quote primer in scope with its push (#104) | 全市场订阅首次快照与后续推送保持同一品种范围。 |
| 61 | `42e386f` | Merge pull request #105 from litaolemo/feat/full-tick-types | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 62 | `3911aab` | Merge main into subscribe-quote-push | 合并解冲突包含快照品种筛选和订阅轮询，两侧能力均保留。 |
| 63 | `e5933a9` | Merge pull request #100 from litaolemo/feat/subscribe-quote-push | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 64 | `658b77c` | Add CLAUDE.md | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 65 | `77377c6` | Stop uppercasing futures symbols (#95) | 期货合约不再一律大写。 |
| 66 | `4f5489a` | Merge pull request #97 from litaolemo/fix/futures-code-case | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 67 | `a94dfab` | Remove CLAUDE.md from the repository | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 68 | `eea3524` | Stop uppercasing codes on the get_full_tick path too (#95) | get_full_tick 同样保留期货代码大小写。 |
| 69 | `00b59e2` | Merge pull request #106 from litaolemo/fix/market-code-case | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 70 | `3acaed7` | Release 0.2.15: whole-market snapshots, subscribe_quote, futures code case | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 71 | `6ab11c9` | Report which build is deployed, and expose it over RPC | 构建版本通过 RPC 可查询；版本号不是实盘部署健康证明。 |
| 72 | `e734f81` | Sync the deployment from the client, on an explicit opt-in (#107 follow-up) | 部署同步保留显式 opt-in 和本地配置保护；本次未调用同步或重启。 |
| 73 | `906714a` | Document version detection and deployment sync | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 74 | `7de4b7c` | Merge pull request #107 from litaolemo/feat/version-check | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 75 | `6ae329c` | Release 0.2.16: deployment version detection and sync | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 76 | `72dd2b8` | fix(qmt): harden ZMQ startup and RPC compatibility | 精简 QMT、纯 ZMQ 启动及缓存回退；保留导入和请求生命周期防护。 |
| 77 | `643adb9` | Merge PR #108: harden QMT ZMQ startup and RPC compatibility | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 78 | `488e089` | Release 0.3.0: pure-ZMQ editor entry, trimmed-QMT fallbacks, cache/timeout fixes (PR #108 @amigobot) | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 79 | `c42895e` | Fix version to 0.3.0 for the release | 修正发布版本标记为 0.3.0。 |
| 80 | `b74793c` | Bump the version stamp to 0.3.0 | 补齐构建版本标记，避免客户端发现错误版本。 |
| 81 | `f8c7bf8` | Merge pull request #110 from litaolemo/fix/version-stamp-030 | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 82 | `f4d49ff` | Recover a long code list instead of losing all of it (#104) | 长代码列表恢复和分段结果处理，失败诊断由第 99 条继续修正。 |
| 83 | `48e62a8` | Merge pull request #111 from litaolemo/fix/large-code-list | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 84 | `93798a7` | Let credit orders reach passorder as credit orders (#103) | 信用订单类型透传，避免被现金股票类型校验拒绝。 |
| 85 | `988fa41` | Merge pull request #112 from litaolemo/feat/credit-order-types | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 86 | `b2b98bc` | Release 0.3.1: credit order types, long code lists, version stamp | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 87 | `080c480` | Correct the PyPI note: 0.2.10-0.2.16 were the skipped uploads, not 0.3.0 | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 88 | `523635a` | Correct the PyPI gap: 0.2.10-0.2.15, and 0.3.1 is uploaded | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 89 | `72ab3b6` | Add probe_capabilities RPC, deploy quickstart, latency report, launcher lock guard | 能力探测、启动预热与锁屏保护；不运行桌面登录或真实部署。 |
| 90 | `056392a` | Return the types MiniQMT returns (#113) (#114) | 同步下单返回整数子类 OrderId，撤单返回 0/-1；保留整数到系统号映射。 |
| 91 | `0ef2d8a` | Serialize a pandas Panel instead of reducing it to is_copy (#115) (#117) | 旧 pandas Panel 按 item 转为 DataFrame；合并时同时保留旧式鸭子类型帧和列式协商。 |
| 92 | `10cb282` | Release 0.3.2: MiniQMT return types, pandas Panel, docs | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 93 | `fb41369` | Say which order_type was rejected, and by which build (#92) (#118) | 不支持的 order_type 错误明确包含类型与构建信息。 |
| 94 | `7b1f3b3` | Release 0.3.3: an error that points at the real problem | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 95 | `bb15650` | Fix all three backtest problems from one report (#109) (#119) | 回测数据路径及重跑停止旧服务；保留生命周期清理。 |
| 96 | `d9d3a7e` | Raise the default RPC timeout from 6s to 30s (#120) | 默认 RPC 等待由 6 秒增为 30 秒；保留单请求 timeout 覆盖。 |
| 97 | `d026815` | Release 0.3.4: backtest re-runs, and a timeout that fits the calls | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 98 | `9c07ad5` | docs: README 加上 bigqmt-dashboard 的介绍 | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 99 | `c7e7886` | Raise the real failure, not "No active exception to reraise" (#104) (#124) | 长列表回退抛出真实异常，避免裸 raise 掩盖失败原因。 |
| 100 | `5ecef2d` | Release 0.3.5: the long-list recovery reports what actually failed | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 101 | `e627cfe` | Refuse a field FormulaServer answers with NaN (#104) (#125) | FormulaServer 不满足请求字段时拒绝快路，避免返回伪 NaN 数据。 |
| 102 | `6934d85` | Say the file is not Python, instead of a bare NameError (#102) (#126) | 非 Python 文件给出可读错误。 |
| 103 | `7a7c7a0` | fix: 可转债的最小申报量、市场推断与报价精度 (#121) | 可转债市场推断、10 张申报单位与三位价格精度。 |
| 104 | `7cb8e71` | Release 0.3.6: two silent-wrong paths closed | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 105 | `d1b9500` | Make a blocked bridge say so, and pay the first-call cost at startup (#127) | 启动预热、阻塞诊断与 watchdog，保留受限日志输出。 |
| 106 | `2716786` | Release 0.3.7: tell a blocked bridge from a dead one | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 107 | `ff4a0db` | subscribe_quote polls live bars via RPC, not the FormulaServer snapshot (#104) | 订阅 K 线强制实时 RPC，防止公式快照滞后。 |
| 108 | `ad84309` | Warn when FormulaServer's intraday snapshot lags (detected live) | 公式盘中快照滞后告警；第 110 条进一步自动回退。 |
| 109 | `f0dd090` | Stop rebuilding a payload that has nothing to rebuild | 无类型标记响应跳过递归恢复；合并后的列式 DataFrame 仍正常恢复。 |
| 110 | `c1274bf` | Auto-failover to RPC bridge when FormulaServer's snapshot is stale | 公式快照过期时 RPC 回退和冷却；保留普通标量编码快路。 |
| 111 | `b4eb759` | Remember which fromlist names are not modules | 缓存不存在的 fromlist 模块，减少沙箱重复失败导入。 |
| 112 | `af92d04` | Lead the "order not found" message with the run mode (#122) | 订单未找到的诊断先说明运行模式，避免误判柜台拒单。 |
| 113 | `5066910` | login: field-focus verification + fix account-field coordinates | 登录输入框焦点确认；仅静态审阅及离线 UI 替身测试。 |
| 114 | `6116e5e` | login: soften foreground requirement, rely on field-level verification | 弱化前台窗口依赖，继续通过字段验证登录输入。 |
| 115 | `413749d` | zmq: never block the router thread on a muted peer; shed to a bounded queue | ZMQ 路由器非阻塞发送与有界背压，防止慢客户端卡住路由线程。 |
| 116 | `fe84262` | login field verification + zmq backpressure shed + linkmini scope note (#116) | 整合登录及背压修复和使用范围说明。 |
| 117 | `11c02c7` | login: TAB to the password field + title containment match + Enter submit (#128) | TAB 切密码框、标题匹配和 Enter 提交，保留后续 DPI 修复。 |
| 118 | `4e3c6a7` | Document qmt_launcher Python API in README (close/open/restart/status helpers) | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 119 | `156444c` | fix: 补上 6 个漏掉的 MiniQMT 兼容层包装（close #130） | 补齐板块和参考数据的 6 个兼容包装，仍依赖柜台支持。 |
| 120 | `5166c1e` | get_stock_type: refuse rather than pass on the server's constant 0 | get_stock_type 不再把固定 0 伪装为有效结果，显式拒绝不支持能力。 |
| 121 | `7cec684` | Merge pull request #132 from litaolemo/fix/sector-data-methods | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 122 | `cd16d48` | Report the account type on_account_status, and say when it disagrees (#103) | 账户状态回调报告服务端账号类型，并提示声明不一致。 |
| 123 | `ee68f6e` | Say when the entry is running as a script, not as a strategy (#123) | 检测脚本运行缺少策略生命周期，输出无服务监听的原因。 |
| 124 | `03724de` | Fill in the XtOrder / XtTrade fields the bridge never sent (#133) | 补齐 XtOrder/XtTrade 属性；保留成交金额、原生别名和标识恢复。 |
| 125 | `6a07da0` | Don't read xtconstant names Big QMT's own xtquant doesn't have | 本地常量回退兼容精简 QMT，避免导入不存在的枚举。 |
| 126 | `bac6307` | strategy_name: QMT never reports it, so recover it from our own record (#133) | 订单身份登记补齐策略名；实际原生字段问题由第 243–244 条修正。 |
| 127 | `f20c58c` | Wire the order-identity store independently of the transport (#133) | 身份存储与传输类型解耦，ZMQ 也能访问配置的 Redis。 |
| 128 | `91b9937` | Pin that redis and zmq return identical values; reuse the cached redis client | Redis/ZMQ 返回值一致性与复用已有客户端。 |
| 129 | `053c9dc` | Download jobs need a redis, not a redis TRANSPORT | 下载任务依赖 Redis 服务，不限定 RPC 必须走 Redis。 |
| 130 | `4fb949d` | Refuse download jobs with no worker; reload the package without a restart | 无后台 worker 时拒绝假下载任务，新增包重载接口。 |
| 131 | `b821531` | Reload: get the reply out before taking the socket away | 重载先把响应发出再关闭套接字，防止自身响应丢失。 |
| 132 | `230709a` | Document reload_deployment / reload_status in the RPC reference | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 133 | `a006257` | Release 0.3.8 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 134 | `8e8ff3c` | Docs: reload_deployment, the #133 fields, and two statements that went stale | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 135 | `7b823b6` | fix: 期货 / ETF 期权 opType 直通（对着官方枚举） | 期货 0–15、ETF 期权 50–59 操作类型保留原生含义。 |
| 136 | `29ce7d3` | Merge pull request #131 from litaolemo/fix/futures-option-optype-passthrough | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 137 | `9861df4` | fix: align generated ZMQ port with transport | 向导生成端口遵循传输端口计算规则。 |
| 138 | `1ff8ff1` | fix: generate matching ZMQ host endpoints | 生成的 ZMQ connect/bind 主机匹配，避免客户端连接错误地址。 |
| 139 | `fd1f23b` | Merge pull request #137 from willzhqiang/codex/fix-zmq-config-port | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 140 | `7b86521` | fix: none 复权读取缺股票时自愈（大 QMT raw 非全市场自动补齐，含测试） | 原始行情缺码时补数重试；第 145 条收紧触发条件。 |
| 141 | `9aee5b9` | fix: tick 链路三件套——entry 入库并绑定 QMT 注入的下载全局函数、fastpath 拒绝非 bar 周期（含测试） | 新增入口绑定下载函数并禁止非 K 线走公式快路；入口本体在第 143 条移除。 |
| 142 | `7bd86f5` | fix: download_history_data2 由 local cache 开关统一控制客户端拉取（禁用时仅服务端下载，tick/K线一致，含测试） | 禁用本地缓存时下载只留服务端，不额外拉取客户端副本。 |
| 143 | `addd4e3` | refactor: 删除 entry 壳——runtime 直挂自动捕获 QMT 注入全局函数，官方挂载形态为 strategy 直挂（含测试） | 移除重复入口，runtime 挂载自动捕获注入的 QMT 全局函数。 |
| 144 | `47210ab` | refactor: QMT 注入函数名单收敛至 strategy 单一事实源——runtime 删本地 19 名手抄表，捕获改走 capture_qmt_injected_funcs（名单自 _EXTRA_QMT_GLOBAL_FUNCS 派生），新增派生/挂载/源码三重防漂移测试（含测试） | 注入函数名单集中到 strategy，避免多份名单漂移。 |
| 145 | `cf839d9` | fix: none 复权自愈判据收敛为过半缺失——少数常驻无数据代码（退市/停牌/无权限）不再每次读都付下载+重读成本；get_market_data 的 field 键形状从嵌套层取已返回代码集（含测试） | 发现“过半缺失”阈值实现偏小；本次改为唯一代码缺失数严格大于一半。 |
| 146 | `6458c3e` | fix: cache 禁用时服务端下载失败不再报满进度——异常传至禁用分支直接抛出（无拉取可兜底，报 {finished: total} 即 issue #47 假进度），cache 启用路径维持 best-effort 不变（含测试） | 缓存禁用时服务端下载失败继续抛出，不报虚假完成进度。 |
| 147 | `e886d42` | Merge pull request #134 from cnwuwil/main | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 148 | `fab589e` | Stop log handlers accumulating on every restart and reload (#139) | 重启和重载移除旧日志 handler，避免重复输出与句柄积累。 |
| 149 | `bf308b5` | Release 0.3.9 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 150 | `4aeb9f8` | fix: make QMT login DPI-safe | 登录坐标处理 DPI 缩放；未执行真实 Windows 登录。 |
| 151 | `17084f0` | Merge pull request #140 from willzhqiang/codex/fix-qmt-login-dpi | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 152 | `19abc49` | feat: add local option IV and Greeks analytics | 本地 IV/Greeks 数学与单位换算；as_of 的行情时点限制见本文。 |
| 153 | `26fd165` | feat: harden live option chain analytics | 期权链行情与价格来源回退；不把当前行情解释为历史 as_of 行情。 |
| 154 | `c469287` | perf: read option metadata from FormulaServer | 期权元数据走 FormulaServer 快路，保留不可服务时回退。 |
| 155 | `da94729` | refactor: isolate option analytics client workflow | 期权分析流程抽离模块，保留显式价格参数与计算接口。 |
| 156 | `2e9a4da` | Merge pull request #138 from willzhqiang/codex/add-option-analytics | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 157 | `4c8d090` | Release 0.3.10 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 158 | `df92de5` | fix: verify QMT login reaches the main window | 登录完成需主窗口证据，不能仅以密码提交视为成功。 |
| 159 | `9e464eb` | Merge pull request #141 from willzhqiang/codex/verify-qmt-login-completion | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 160 | `8fa974f` | Release 0.3.11 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 161 | `9de6a22` | probe_capabilities: report the sector write channel (#142) | 能力探测补充板块写通道，探测结果仍与终端权限相关。 |
| 162 | `a446994` | README: add the Star History chart | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 163 | `68ad9e7` | README: star history chart with a sealed token | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 164 | `033d80e` | Stop an unreachable redis eating callbacks (#145); one log file per process (#144) | Redis 连续失败后执行事件改走可用推送；日志按进程聚合。 |
| 165 | `12a01a5` | Let a deployment say it has no redis (#147) | 显式 redis_enabled=False 配置保留。 |
| 166 | `c94a9e7` | The no-redis build now says it has no redis (#147) | 无 Redis 单文件模板明确禁用 Redis，避免隐藏超时。 |
| 167 | `1779e2d` | Release 0.3.12 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 168 | `1291e69` | Whole-quote subscriptions stop uppercasing futures symbols (#95) | 整推订阅保留期货代码大小写。 |
| 169 | `dd3c2c1` | Fix option tick snapshots and subscriptions | 期权缺失 tick 通过最新 tick 行恢复；订阅失败回滚句柄。合并时兼容旧 ori 签名。 |
| 170 | `65d9051` | Merge PR #146: full-QMT option tick snapshots and subscriptions | 解冲突合并期货大小写与期权 tick/订阅分支，无丢失其中一侧修复。 |
| 171 | `2e931d0` | fix: confirm falsey cancel results by order status | 撤单返回假值也读取委托状态确认，不直接当作拒绝。 |
| 172 | `af7a474` | Merge PR #149: confirm falsey cancel results by order status | 合并解冲突已检查，保留父提交结果；未新增独立运行行为。 |
| 173 | `165c80a` | Hand the backtest client its ending before closing the socket (#150) | 回测结束先交付结束应答再停止传输，避免客户端悬挂。 |
| 174 | `3d8aef1` | Sector writes verify instead of failing silently (#143) | 板块写入增加结果验证与显式替代通道；柜台能力仍需终端验证。 |
| 175 | `52261b8` | CHANGELOG: the #95 futures-case subscription fix was missing | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 176 | `d7f8718` | A live order must not be reported as a rejection (#152) | 已提交但未取到系统号不能当作拒单并重提；保留不确定结果提示。 |
| 177 | `644149c` | Release 0.3.13 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 178 | `de056b1` | Single-file builds carry the zmq bind address and transport (#153) | 单文件构建写入完整 ZMQ 传输及地址配置。 |
| 179 | `957d529` | qmt-trader CLI: --dry-run must not place a real order | 修复 CLI --dry-run 意外进入真实下单路径；确认最终分支提前返回。 |
| 180 | `3f85379` | Release 0.3.14 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 181 | `74f0e4c` | qmt-trader CLI: cancel result inversion, global flags, path order, two smaller bugs | CLI 撤单 0 成功判断、全局参数和模块路径顺序修复。 |
| 182 | `d7988fc` | Verify truthy cancels against the order snapshot too (#151) | 撤单真值也需状态验证；保留 53/54、51/52 与非撤单终态区别。 |
| 183 | `3e6825c` | Client exec-event listener must mirror the server's sink choice (#144) | 客户端事件监听随服务端选择 Redis 或推送通道。 |
| 184 | `3c7cbf5` | The strategy name on an order is the user's to choose (#154) | 策略名归调用者指定，服务端默认值保留配置能力。 |
| 185 | `a6bffcc` | Attribute strategy_name without Redis: in-process order identity journal (#156) | 无 Redis 时从进程内、按账号和备注索引的身份日志恢复策略名。 |
| 186 | `66b9db9` | Release 0.3.15 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 187 | `e444ed9` | test_all_apis.py must follow the configured transport (#157) | 综合接口脚本使用实际配置传输；脚本可能下单，本次未运行。 |
| 188 | `7804b95` | Drain queued async orders at stop()/process exit (#156) | 进程退出和 stop 排空已接收的异步委托；合并完整保留。 |
| 189 | `fe4c277` | Release 0.3.16 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 190 | `df99ed1` | Stop re-dialing a dead native SDK on every data call (#160) | 原生 SDK 失败负缓存，避免每次行情调用重拨失效连接。 |
| 191 | `1468d66` | xt_trader.sync_deployment() called a method that only exists on xtdata | 交易客户端部署同步改调实际存在的方法。 |
| 192 | `0077eef` | contracts: keep Protocol import working on py3.6 without typing_extensions | Python 3.6 无 typing_extensions 时 Protocol 回退。 |
| 193 | `a4b81d7` | Merge pull request #159 from karlthas007/fix/py36-protocol-fallback | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 194 | `f64822c` | Test the py3.6 Protocol fallback in contracts.py (#159) | 补充 Python 3.6 Protocol 导入行为回归。 |
| 195 | `eb2b9a7` | Merge branch 'review/159' | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 196 | `899a1dc` | deploy: one-shot Windows deployment script for the redis bridge | Windows 一键部署脚本；其默认账号和下单开关由第 198 条修正。 |
| 197 | `d029208` | Merge pull request #158 from karlthas007/feat/one-click-deploy | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 198 | `e56c97e` | Review fixes for the deploy bundle: placeholder account, orders off by default, friendly missing-config message | 部署模板改占位账号和默认关闭下单；本次未运行部署脚本。 |
| 199 | `0a6f653` | Merge branch 'review/158' | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 200 | `996e887` | fix(qmt): 修复沙箱启动与 RPC 隔离问题 | 沙箱常量导入与交易快照主线程隔离修复。 |
| 201 | `fd32f39` | Merge pull request #162 from amigobot/main | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 202 | `fa98700` | Merge branch 'review/162' | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 203 | `c04ef28` | Hold pre-sysid order events; fill instrument_name in callbacks (#161) | 暂存无系统号事件、补齐证券名；本次补上多账户配对和到期发布隔离。 |
| 204 | `be1deae` | Release 0.3.17 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 205 | `619b70b` | Honest no-ops for terminal-maintained downloads; survive redis without streams (#163) | 终端自维护数据下载明确 no-op，旧 Redis 无 streams 时可继续运行。 |
| 206 | `dae4ffc` | Filtered order queries must name their rows; add probe_order_identity (#156) | 筛选策略名时补齐查询结果身份，增加诊断能力。 |
| 207 | `129ae45` | Settle orders from the callback-fed watch table, not a poll loop (#164) | 发现观察表备注和系统号未按账号隔离；本次修复提交/撤单确认读写。 |
| 208 | `b02fd1f` | fix: per-request account_type resolution via BIGQMT_ACCOUNT_TYPE_MAP | 按请求账号识别类型；共享请求状态问题由第 209 条改正。 |
| 209 | `ff0ed18` | Address PR #135 review: per-request account_id, drop reload, add tests | 请求账号通过参数传递，移除共享可变请求状态和不必要的 reload。 |
| 210 | `e75f0e7` | Merge PR #135: per-request account_type for dual-account deployments | 合并解冲突已检查，保留父提交结果；未新增独立运行行为。 |
| 211 | `34cee0d` | fill_data must reach big QMT, not be dropped on the way (#167) | fill_data 透传；合并保留本地更严格的 False 语义，旧签名仅在默认填充时降级。 |
| 212 | `a1f5174` | CHANGELOG: one 新增 and one 修复 in the unreleased section | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 213 | `e936131` | README: a 讨论组 section with the WeChat group QR | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 214 | `c76067c` | README: move 讨论组 above the config wizard | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 215 | `96bf766` | README: refresh the group QR (group renamed to qmt 交流群) | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 216 | `0ff981a` | probe_capabilities reports whether the #164 watch table is live | 探测观察表是否实际接入策略生命周期。 |
| 217 | `4bb91de` | get_divid_factors answers a range, not silently one day (#165) | 发现日期时间误读及整数日期索引丢行；本次修复。候选扫描范围限制保留并披露。 |
| 218 | `03cdd34` | Release 0.3.18 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 219 | `0d4f8b2` | deploy: guard self-copy + proxy hints on download failures | 部署脚本自复制保护和代理失败诊断；未执行外部部署。 |
| 220 | `0b14a0e` | Merge pull request #169 from karlthas007/fix/deploy-self-copy-and-proxy-hint | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 221 | `24ea5cf` | feat: multi-account RPC service (step 2 of dual-account support) | 多账户服务共享处理器，查询和撤单传递请求账号；本次补齐共享观察表/推送隔离。 |
| 222 | `73dcba4` | Merge pull request #171 from fancyfanfan/feat/multi-account-rpc-service | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 223 | `e18e164` | Release 0.3.19 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 224 | `46189db` | Measure where the RPC round trip actually goes (#104) | RPC 分段计时与客户端展示，区分服务器处理和往返等待。 |
| 225 | `4851ee7` | fix: fill weekly-bar preClose from daily bars (#166) | 周线 preClose 从同复权口径首个日线补齐，保留取数范围。 |
| 226 | `a6893ba` | Merge pull request #172 from litaolemo/fix/166-weekly-pre-close | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 227 | `944e3ed` | Merge remote-tracking branch 'origin/main' | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 228 | `d059c34` | Decompose the RPC round trip with wall-clock stamps (#104) | 墙钟时间戳拆分链路耗时；跨机器时钟偏差仍需单独解释。 |
| 229 | `8f3e97a` | Record why get_divid_factors stays off the FormulaServer fast path | 记录除权因子不能直接走公式快路的理由，保留 RPC 路径。 |
| 230 | `92e1eba` | feat: expose m_dTradeAmount as trade_amount on ORDER rows (#173) | 订单原生成交金额 m_dTradeAmount 映射为 trade_amount。 |
| 231 | `de0b34c` | docs: correct the #173 rationale to what the live terminal actually shows | 修正文档及代码注释中的终端行为解释，保留真实字段映射实现。 |
| 232 | `b51c793` | feat(qmt.py): show trade_amount in the orders output (#173) | CLI 订单输出成交金额。 |
| 233 | `368847e` | Merge pull request #175 from litaolemo/feat/173-order-trade-amount | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 234 | `dcfd587` | fix(qmt.py): --table rendered one blank row instead of the data | CLI 表格行提取修复，避免只打印一个空行。 |
| 235 | `bd03e54` | Merge pull request #176 from litaolemo/fix/qmt-cli-table-rows | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 236 | `efbe2db` | Wake the router thread when a reply is queued (#104) | 响应入队唤醒 ROUTER，减少轮询等待；保留路由线程拥有套接字的约束。 |
| 237 | `b5f1b4c` | Merge PR #177: wake the router thread when a reply is queued (#104) | 合并解冲突已检查，保留父提交结果；未新增独立运行行为。 |
| 238 | `f44a6d3` | fix: callbacks never carried strategy_name (#174) | 订单和成交事件共同恢复策略名；本次恢复时使用事件实际账号。 |
| 239 | `d8cf9f7` | Merge PR #178: callbacks never carried strategy_name (#174) | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 240 | `a0d19fc` | Release 0.3.20 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 241 | `ccb5470` | fix: qmt.py trades dropped strategy_name (#174 follow-up) | CLI 成交输出保留策略名。 |
| 242 | `94b8c42` | Merge pull request #179 from litaolemo/fix/174-cli-trades-strategy-name | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 243 | `b96a892` | fix: 策略名一直在回调里，读的是错字段 (#174) | 策略名读取真实报单来源 m_strSource，身份日志仍作为回退。 |
| 244 | `ab414c5` | fix: 两条路径的策略名候选表补齐并钉死 (#174 后续) | 订单查询与回调两条候选字段表补齐，保留一致性回归。 |
| 245 | `6f738cb` | fix: 批量下单的应答不该被其中一笔的结算扣住 (#181) | 批量响应不被其中一笔的单笔结算扣留。 |
| 246 | `72f0730` | Merge PR #184 | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 247 | `66c344a` | Batch queued async orders into one RPC; fire callbacks off the submit worker (#181) | 积压异步委托合成批量 RPC，用户回调移至独立调度线程；合并完整保留。 |
| 248 | `39e10c2` | Merge PR #185 | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 249 | `dc9351c` | README: a lookup table for the four ways an order can fail | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 250 | `282a1d6` | README: fix the RPC table, its counts, and a footnote #143 made false | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 251 | `8ee6ab4` | README: how to keep the original xtquant / xtdata imports | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 252 | `ffe7d50` | fix: restore MiniQMT history and ZMQ callback behavior | MiniQMT 时间索引优先、历史字段及纯 ZMQ 回调修复；与本地缓存/帧元数据合并。 |
| 253 | `1242a3c` | Merge PR #182: MiniQMT history semantics and zmq exec-event callbacks | 合并解冲突已检查，保留父提交结果；未新增独立运行行为。 |
| 254 | `dcf172f` | Opt zmq/mysql into the adjust-thread drain; move the ping log off the hot path (#104) | ZMQ/MySQL 支持 adjust 线程 drain，显式 False 配置不被默认值覆盖。 |
| 255 | `4ff3c81` | Merge PR #183: opt zmq/mysql into the adjust-thread drain (#104) | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 256 | `986a136` | CHANGELOG: record #183's live four-cell measurement (#104) | 检查文档、测试或元数据差异，保留；不据此宣称实盘验证。 |
| 257 | `a8af642` | fix: async orders swallowed by the batch endpoint's idempotency (#190) | 修复批量幂等吞掉重复备注的异步订单；此提交随后回退。 |
| 258 | `08aee25` | Revert "fix: async orders swallowed by the batch endpoint's idempotency (#190)" | 显式回退第 257 条；最终行为以第 259–260 条重做为准。 |
| 259 | `f402dda` | fix: async orders swallowed by the batch endpoint's idempotency (#190) | 重新实现异步批量非幂等语义，主动重复下单不会被备注吞掉。 |
| 260 | `75793db` | Keep the auto-tag's remark shape, and correct the verification claim (#190) | 自动标签保持既有格式，修正验证声明；不把历史说明当当前实测。 |
| 261 | `5366803` | Merge PR #191: async orders swallowed by the batch endpoint's idempotency (#190) | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 262 | `e4c4564` | test: pin the #190 reporter's tight async loop end to end (#191) | 紧循环异步批量提交端到端替身回归，最终完整保留。 |
| 263 | `7eda4cb` | Merge pull request #192 from litaolemo/test/190-tight-loop-regression | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 264 | `62c6a05` | Stop the zmq entries forcing background threads, fix the perf tables (#188, #187) | ZMQ 入口尊重后台线程显式 False，并更正性能表。 |
| 265 | `f94f8b7` | Release 0.3.21 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
| 266 | `7c97e63` | fix: a timed-out async batch must not be resubmitted -- it doubled orders (#195) | 关键修复：批量超时/结果未知不逐笔重提；明确服务器拒绝才允许回退，完整保留。 |
| 267 | `b43d5c2` | Merge pull request #196 from litaolemo/fix/batch-timeout-double-submit | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 268 | `c3de7a6` | fix: close secondary listen_redis in MultiAccountRpcServiceManager.stop() | 多账户停止关闭次级 Redis 监听连接，避免重载后旧线程抢请求。 |
| 269 | `e80e29f` | Merge PR #194: close secondary listen_redis on multi-account stop (@fancyfanfan) | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 270 | `a714d96` | fix(runtime): honor explicit quote push configuration | runtime 尊重 quote_push 显式开关与端口等配置。 |
| 271 | `6e6f0bf` | Merge PR #198: forward quote_push config from the runtime entry (@amigobot) | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 272 | `30109ac` | One DEALER per thread; quiet redis shutdown (#186, #189) | 每线程独占 DEALER、Redis 安静退出；本次补上 stop 后其他存活线程的关闭句柄重建。 |
| 273 | `1c04150` | Merge PR #193: per-thread DEALER + quiet redis shutdown (#186, #189) | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 274 | `5e593c5` | feat: raw native-passorder passthrough (route 2) | 原生 passorder 低层透传，继续受交易 RPC 开关及策略线程执行限制；未实盘调用。 |
| 275 | `599e23c` | Merge PR #197: raw native-passorder passthrough (route 2) | 父提交已逐条审阅；无额外 combined diff，保留合并关系。 |
| 276 | `d07d4d5` | Release 0.3.22 | 检查版本与变更记录，发布元数据保留；对应运行行为见关联提交。 |
