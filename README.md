# record_merger — 多来源记录去重与合并库

纯 Python 3 标准库实现，零依赖。解决多来源同步场景下的记录重复到达、
字段互补/矛盾、重复投递需幂等的问题。

## 文件

| 文件 | 说明 |
|---|---|
| `record_merger.py` | 库源码（仅标准库） |
| `test_record_merger.py` | 12 个自测：三种策略、冲突留痕、幂等重放、顺序无关性 |
| `benchmark.py` | 百万级性能基准 + 幂等重放校验 |
| `make_sample_report.py` | 生成冲突报告样例 |
| `examples/conflict_report_sample.json` | 冲突报告样例 |

## 运行命令

```bash
python3 test_record_merger.py        # 自测（含幂等重放测试）
python3 benchmark.py                 # 100 万条基准（可传参：records unique_keys）
python3 make_sample_report.py        # 重新生成报告样例
```

## 三种合并策略

- `overwrite`：整记录覆盖，优先级最高来源的记录整体胜出。
- `keep_earliest`：保留 `_ts` 时间戳最早的记录。
- `merge`（默认）：逐字段合并。缺失值（`None`/`""`/`[]`/`{}`）**不得**覆盖
  已有值；字段互补时直接并集；冲突时按来源优先级取值。

## 冲突留痕与报告

每次冲突记录：字段名、双方来源与取值、最终采用的来源与取值
（`Conflict` 结构，`merger.conflicts`）。`merger.report()` 输出：

- `conflicts.by_field`：按字段统计冲突次数；
- `conflicts.won_by_source`：各来源胜出的冲突次数；
- `conflict_log`：逐条冲突明细（`keep_conflict_log=False` 时只保留聚合，
  供大规模跑批省内存）。

样例见 `examples/conflict_report_sample.json`。

## 幂等设计

每条记录计算内容指纹（规范化 JSON 的 SHA-1），存入 `merger._seen` 集合；
已处理过的记录直接跳过（`skipped`），不触发合并、不产生新冲突。
因此同一批数据重复投递：合并结果与冲突报告完全一致，仅计数类字段
（`received`、`skipped_duplicates`）增长，无重复副作用。
`test_replay_same_batch` / `test_replay_after_other_records` 与
`benchmark.py` 内置的 replay check 均验证该性质。

## 确定性（决胜规则）

所有取舍落在同一个全序上，合并等价于全序取 min（满足交换律/结合律），
**结果不依赖到达顺序**：

1. 来源优先级高者胜（`source_priority` 越靠前越高，未列出来源最低）；
2. 优先级相同：来源名字典序小者胜；
3. 来源也相同：取值按规范化 JSON 串字典序小者胜。

元字段同样确定化：`_source` 取（优先级, 来源名）最小者，`_ts` 取最早者。
`test_order_independence_*` 用 5 种随机乱序验证结果完全一致。

## 索引结构

- `store: dict[key -> record]`：主键哈希索引，O(1) 查重与定位；
- `_prov: dict[key -> (base_source, {field: source})]`：字段级溯源，
  与基准来源相同的字段不重复存储（压缩）；
- `_seen: set[fingerprint]`：已处理记录指纹集合，O(1) 幂等去重。
内存复杂度 O(唯一主键数)，与投递次数无关。

## 性能数据（实测）

环境：Python 3.12.3，Linux x86-64（16 核，15 GiB RAM）。
负载：100 万条记录、40 万唯一主键、4 个来源、字段随机互补/冲突
（产生 1,005,523 次字段冲突），`merge` 策略，`keep_conflict_log=False`。

| 指标 | 数值 |
|---|---|
| 合并耗时 | **8.4 s**（约 116,000 条/秒，含数据生成） |
| 内存峰值（进程 RSS） | **475 MiB**（`ru_maxrss`，全新进程单轮合并） |
| Python 堆峰值 | 420 MiB（`tracemalloc`） |
| 合并后记录数 | 367,269 |
| 幂等重放 | 通过：store 不变、无新增冲突、100 万条全部 skipped |

复现：`python3 benchmark.py 1000000 400000`
