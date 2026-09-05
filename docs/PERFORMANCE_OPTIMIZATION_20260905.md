# 2026-09-05 性能优化与兼容性修复

> 这是首次优化相对 0.2.5 的历史记录。随后已集成上游 0.3.22；当前行为、验证及性能对比见
> [逐提交审阅与 rebase 记录](UPSTREAM_REVIEW_20260905.md)。下文末尾的旧版差异清单不代表当前状态。

本轮完成行情编码、缓存读写、账户空闲刷新、委托批量确认和客户端异步请求优化。
对照版本为 `77b536c`（0.2.5）。工作区改动尚未发布，也未部署到真实 QMT。

## 离线性能对比

同机 Python 3.10.20、Pandas 2.3.3，预热后取 7 次中位数。行情采用合成数据，
缓存使用临时 Parquet 文件，RPC 使用内存替身，脚本禁止建立网络连接。

| 场景 | 修改前 | 修改后 |
|---|---:|---:|
| 100 个标的 × 240 根 × 6 字段，完整编码 | 149.635 ms | 43.545 ms |
| 同一报文解码并重建 DataFrame | 41.558 ms | 27.861 ms |
| 含既有 base64 外层的报文大小 | 3,741,901 字节 | 1,757,637 字节 |
| 2 万行缓存，热读末尾 1 行 | 5.995 ms | 0.039 ms |
| 最后一行未变化，重复写入 | 13.116 ms | 2.959 ms |
| 最后一行发生变化，实际落盘 | 12.753 ms | 11.722 ms |
| 20 轮空闲调度：持仓 / 资产查询次数 | 40 / 20 | 1 / 1 |
| 同账户 20 笔待确认委托，每轮完整查单次数 | 40 | 1 |
| 注入 50 ms RPC 延迟，异步查询调用方等待 | 50.140 ms | 0.028 ms |
| 注入 50 ms RPC 延迟，异步撤单调用方等待 | 50.195 ms | 0.021 ms |

编码和解码合计耗时减少约 63%，报文体积减少约 53%。修改后的帧逐一通过
`assert_frame_equal`，旧版本会丢失索引，不能通过该项检查。

异步接口缩短的是调用方阻塞时间；RPC 自身仍耗时约 50 ms，脚本等待每个回调完成后才
开始下一次测量。空闲调度对比为 3 秒内连续 20 轮，并启用实际快照接收端。真实柜台延迟、
QMT 主循环占用 GIL、Windows 磁盘性能不在这组测量范围内。

原始结果：[修改前](../benchmarks/results/20260905-baseline.json)、
[修改后](../benchmarks/results/20260905-optimized.json)。
复现脚本：[offline_performance.py](../benchmarks/offline_performance.py)。

```bash
mkdir -p /tmp/xtquant_big_convert_baseline_77b536c
git archive 77b536c src | tar -x -C /tmp/xtquant_big_convert_baseline_77b536c
PYTHONDONTWRITEBYTECODE=1 python benchmarks/offline_performance.py \
  --source-dir /tmp/xtquant_big_convert_baseline_77b536c/src \
  --wire-format records --output /tmp/bigqmt-before.json
PYTHONDONTWRITEBYTECODE=1 python benchmarks/offline_performance.py \
  --output /tmp/bigqmt-after.json
```

## 具体变化

- 行情序列化：新增行列轴元数据，保留证券索引、时间索引、时区、重复列和常见数值类型；
  数值列批量转换，避免逐单元格构造字典，大整数不经过浮点矩阵。
- 协议协商：新客户端在请求顶层声明 `dataframe_format="columns_v1"`，服务端才按列编码；
  旧客户端和未知格式仍使用 `records`。新客户端接受旧服务端响应。没有安装 Pandas 的
  客户端继续请求 `records`。外层 JSON/base64、Redis/ZMQ/MySQL 信封保持原有机制。
- 本地缓存：增加 64 MiB / 32 文件的有界 LRU，读取前核对文件身份、修改时间和大小；
  外部替换文件后失效。切片后返回独立帧，重复数据不重写。部分字段更新保留未提供字段，
  显式提供的 NaN 可以覆盖旧值。临时文件使用唯一名称，同一实例的合并写入由锁保护。
- 账户刷新：无信号时跳过风控持仓查询，纯 RPC 入口默认每 3 秒更新账户快照；成交事件和
  显式同步立即读取最新快照。没有快照接收端时默认关闭同步。信号模式保留每轮同步默认值；
  风控持仓读取失败发生在信号认领前，信号保持可重试。
- 委托确认：同一轮按账户查询一次，再按备注索引匹配；快照不跨轮复用。仍在策略线程
  调用柜台接口，保留到期诊断和备注精确匹配。
- 异步查询与撤单：惰性工作队列执行 RPC 和回调，调用方立即返回。提供查询 callback 时
  返回 `None`；省略 callback 的桥接扩展仍返回序号，但现在会实际执行查询。修复
  `query_stock_orders_async(account, callback, cancelable_only)` 和账户类查询的 callback
  位置参数。用户回调异常不会终止后续请求，下单的响应/事件顺序屏障继续生效。
- 参数及返回值：下单策略名和备注可省略；`get_market_data_ex(timeout_seconds=...)`
  正确传给传输层；同步撤单返回整数 `0` / `-1`；申购额度转发真实查询并在策略线程执行。
- 填充语义：大 QMT 行情调用保留 `fill_data`。旧原始接口不能接收显式 `False` 时转到标准
  接口；FormulaServer 对该参数拒绝直接路由，交给策略 RPC，避免静默按默认值填充。
  参数依据：[迅投内置行情接口](https://dict.thinktrader.net/dictionary/stock.html)。

## 升级要点

同步撤单的成功判定需要使用 `== 0`，依据
[XtTrader 接口约定](https://dict.thinktrader.net/nativeApi/xttrader.html)：

```python
result = xt_trader.cancel_order_stock(account, order_id)
if result == 0:
    print("撤单请求成功")
```

异步结果应在回调中消费，不能在调用返回后立即假设结果已经到达。桥接扩展
`wait_async_requests(timeout=10.0)` 可等待查询和撤单结束，`wait_async_orders` 等待下单结束。
`stop()` 关闭队列并最多等待 1 秒；尚在执行的已接收任务继续处理后退出，停止后新请求会
抛出异常。因此需要确保队列完成后再退出进程。

新客户端配旧服务端时，旧协议没有提供的索引信息仍无法凭空恢复。索引修复和按列编码
需要更新服务端源码及客户端；原始数组接口和 FormulaServer 的既有轻量响应继续被兼容解析。

服务端私有配置 `BIGQMT_REDIS_CONFIG` 可设置：

```python
"position_sync_interval_seconds": 3.0,  # 秒；0 恢复每轮同步
```

客户端 `BIGQMT_LOCAL_CACHE_CONFIG` 可设置：

```python
"memory_max_bytes": 64 * 1024 * 1024,
"memory_max_entries": 32,  # 任一上限设为 0 即禁用内存缓存
```

内存上限约束缓存保存的帧，不代表进程 RSS 上限；同一文件跨进程同时增量写入仍不提供事务
合并保证。有变化的数据仍整文件原子替换，历史很长且持续更新的场景可进一步考虑按日期分区。

## 验证与剩余差异

离线测试：**492 passed，1 skipped**。跳过项为缺少可选 `pymongo.bson` 的 BSON 对照测试。
MySQL 传输的 SQLite 替身测试使用临时目录安装的 DBUtils 3.2.0。修正了原有启动器测试在
Linux 下把 `D:` 当绝对盘符的预期，生产启动器逻辑未改。

```bash
PYTHONPATH=/tmp/xtquant_big_convert_test_deps PYTHONDONTWRITEBYTECODE=1 \
  python -m pytest tests/bigqmt_signal_trader tests/bigqmt_backtest -q -rs -p no:cacheprovider
git diff --check
```

修改涉及的 10 个运行模块通过 Python 3.6 语法解析检查；这不等同于在 QMT 内置 Python 中
运行验证。本轮没有运行 `test_all_apis.py` 的真机交易路径，没有连接真实账户或发送委托。

仍需独立处理和真机对账的差异：

1. `order_id` 仍是桥接系统委托号字符串，与 MiniQMT 的内部整数 ID 不同。完整对齐需要
   一致的持久映射以及重连、撤单、委托/成交回报关联验证。
2. 单股 `subscribe_quote/subscribe_quote2` 仍没有原生连续订阅语义；已有连续推送能力
   位于 `subscribe_whole_quote`。单股回调形状和各 K 线周期也需要一起对齐。
3. 构造器 callback、账户类型/状态枚举、连接与退订生命周期、部分交易对象字段，以及
   融资融券/期货/期权指令仍存在差异；本轮没有将其标记为完整兼容。
4. 部分下载函数、`incrementally`、本地数据 `data_dir/fill_data` 以及 tick 结构化数组的
   dtype 仍需补齐。接口名可路由不表示参数、返回值和权限全部等价。
