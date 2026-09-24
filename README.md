# dedup_merge — 记录去重与合并库

纯 Python 3 标准库实现，无第三方依赖。面向多来源同步场景：重复到达的记录按主键
去重，字段互补则合并、矛盾则按来源优先级裁决，每次冲突完整留痕，同一批数据重复
投递幂等。

## 功能

- **三种合并策略**（`Strategy`）：
  - `OVERWRITE` 覆盖：整条记录按来源优先级取舍，高优先级来源的记录整体生效。
  - `KEEP_EARLIEST` 保留最早：`timestamp` 最小的记录保留（缺失视为 +inf）。
  - `MERGE_FIELDS` 按键合并（默认）：字段级合并。`None`/缺失字段**绝不覆盖**已有值；
    双方都有值且不同时记为冲突，按来源优先级取值。
- **冲突留痕**：每次冲突记录主键、字段、双方来源与取值、最终采用的来源与值；
  `report.by_field()` 按字段汇总冲突次数。
- **幂等重放**：每条记录以规范 JSON（`sort_keys`）的 SHA1 指纹入 `_seen` 集合，
  完全相同的记录重复投递直接跳过，不合并、不记冲突、无任何副作用。第二次投递
  同一批数据，合并结果与冲突报告与第一次完全一致（仅 `ingested` /
  `duplicates_skipped` 等计数类字段变化）。
- **确定性决胜规则**（不依赖到达顺序）：候选按元组
  `(-来源优先级, 来源名, 值的规范JSON)` 取最小者胜。即：
  1. 来源优先级高者胜（`priorities` 配置，未配置的来源默认为 0）；
  2. 优先级相同，来源名字典序小者胜；
  3. 来源也相同，值的规范 JSON 字典序小者胜。
  三步都是全序比较，任意到达顺序、任意 shuffle 结果一致（有测试保证）。

## 索引结构

全部为哈希表，单条记录处理均摊 O(字段数)：

| 索引 | 内容 | 用途 |
|---|---|---|
| `_records` | 主键 → 合并后记录 | 主索引，按主键 O(1) 定位重复 |
| `_record_source` / `_record_ts` | 主键 → 当前记录来源 / 时间戳 | OVERWRITE / KEEP_EARLIEST 裁决 |
| `_field_sources` | 主键 → {字段 → 来源} | MERGE_FIELDS 字段级溯源（留痕用） |
| `_seen` | 记录指纹 SHA1 集合 | 幂等去重，重复投递 O(1) 跳过 |

## 使用

```python
from dedup_merge import Merger, Strategy

m = Merger(strategy=Strategy.MERGE_FIELDS,
           priorities={"crm": 3, "web": 2, "import": 1})
m.ingest({"id": 1, "source": "web", "name": "Alice W", "email": "a@web.com"})
m.ingest({"id": 1, "source": "crm", "name": "Alice", "email": None, "vip": True})

m.records[1]            # {'id': 1, 'name': 'Alice', 'email': 'a@web.com', 'vip': True}
m.report.by_field()     # {'name': 1}
m.report.to_json()      # 完整冲突留痕
```

冲突报告样例见 `examples/conflict_report_sample.json`（由
`examples/make_sample.py` 生成），单条冲突形如：

```json
{
  "key": "u-1001", "field": "name",
  "existing_source": "web",  "existing_value": "Alice W",
  "incoming_source": "crm",  "incoming_value": "Alice",
  "chosen_source": "crm",    "chosen_value": "Alice"
}
```

## 性能数据

测试环境：Python 3.12.3，Linux x86_64。数据：100 万条记录、约 24.5 万个唯一主键、
5 个来源、8 个数据字段（30% 缺失），共触发约 248 万次字段冲突。

| 配置 | 耗时 | 吞吐 | 内存峰值 (RSS) |
|---|---|---|---|
| MERGE_FIELDS + 完整留痕 | 17.9 s | ~5.6 万条/s | 838 MiB |
| MERGE_FIELDS + 关闭留痕（`trace_conflicts=False`） | 11.6 s | ~8.6 万条/s | 409 MiB |
| 幂等重放 100 万条（全部命中 `_seen` 跳过） | 5.0 s | ~20 万条/s | 不增长 |

内存大头是 248 万个冲突留痕对象（每条含双方来源与取值）；关闭留痕后内存即
回落到索引本身规模。留痕对象使用 `__slots__` 压缩过。指纹为 20 字节 SHA1，
100 万条记录的 `_seen` 集合约 80 MiB。

## 运行命令

```bash
python3 -m unittest discover -s tests -v   # 自测（含幂等重放、确定性、三策略）
python3 examples/make_sample.py            # 生成冲突报告样例
python3 bench.py 1000000 250000            # 性能基准（总条数 唯一主键数）
```

## 文件结构

- `dedup_merge/merger.py` — 核心：`Merger` 与三种策略、决胜规则、幂等指纹
- `dedup_merge/conflict.py` — `Conflict` 留痕与 `ConflictReport` 汇总
- `tests/test_merger.py` — 11 个用例：三策略、缺失不覆盖、优先级裁决、
  冲突留痕内容、按字段统计、乱序确定性、幂等重放、部分重叠重放
- `examples/make_sample.py` + `examples/conflict_report_sample.json` — 报告样例
- `bench.py` — 百万级基准与重放验证
