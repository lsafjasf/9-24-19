"""record_merger: 多来源记录去重与合并库（仅标准库）。

核心特性：
- 按主键识别重复，支持三种策略：overwrite / keep_earliest / merge（按键合并）。
- merge 策略下缺失值（None/""/[]/{}）不会覆盖已有值；冲突按来源优先级取值。
- 每次冲突留痕（字段、双方来源与取值、最终采用值与来源），报告可按字段统计。
- 幂等：同一记录（内容指纹）重复到达会被跳过，重复投递同批数据结果与报告一致。
- 确定性：所有比较都落在全序规则上，结果不依赖到达顺序。

决胜规则（来源优先级相同时）：
1. 来源优先级高的胜（source_priority 列表越靠前优先级越高，未列出的来源最低）。
2. 优先级相同：来源名字典序小者胜。
3. 来源也相同：取值按 JSON 规范化串字典序小者胜。
该规则构成全序，合并等价于在全序上取 min，满足交换律/结合律，因此与到达顺序无关。
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

STRATEGY_OVERWRITE = "overwrite"        # 整记录覆盖：高优先级来源的记录整体胜出
STRATEGY_KEEP_EARLIEST = "keep_earliest"  # 保留时间戳最早的记录
STRATEGY_MERGE = "merge"                # 按键合并：逐字段合并

_MISSING = (None, "", [], {})


def _is_missing(value: Any) -> bool:
    return value in _MISSING


def _canon(value: Any) -> str:
    """值的规范化串，用于确定性比较与指纹计算。"""
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def _fingerprint(record: Dict[str, Any]) -> str:
    return hashlib.sha1(_canon(record).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Conflict:
    key: Any
    field: str
    existing_source: Optional[str]
    existing_value: Any
    incoming_source: Optional[str]
    incoming_value: Any
    chosen_source: Optional[str]
    chosen_value: Any

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "field": self.field,
            "existing": {"source": self.existing_source, "value": self.existing_value},
            "incoming": {"source": self.incoming_source, "value": self.incoming_value},
            "chosen": {"source": self.chosen_source, "value": self.chosen_value},
        }


class Merger:
    """记录合并器。

    参数：
        key_field:        主键字段名。
        source_field:     来源字段名（记录中标识来源的字段）。
        ts_field:         时间戳字段名（keep_earliest 策略使用）。
        strategy:         overwrite / keep_earliest / merge。
        source_priority:  来源优先级列表，越靠前优先级越高。
        keep_conflict_log: 是否保留逐条冲突明细（大规模跑批可关闭以省内存，
                          仅保留按字段/来源的聚合计数）。
    """

    def __init__(
        self,
        key_field: str = "id",
        source_field: str = "_source",
        ts_field: str = "_ts",
        strategy: str = STRATEGY_MERGE,
        source_priority: Optional[List[str]] = None,
        keep_conflict_log: bool = True,
    ) -> None:
        if strategy not in (STRATEGY_OVERWRITE, STRATEGY_KEEP_EARLIEST, STRATEGY_MERGE):
            raise ValueError(f"unknown strategy: {strategy}")
        self.key_field = key_field
        self.source_field = source_field
        self.ts_field = ts_field
        self.strategy = strategy
        self.keep_conflict_log = keep_conflict_log

        priority = list(source_priority or [])
        self._prio: Dict[str, int] = {s: i for i, s in enumerate(priority)}
        self._default_prio = len(priority)

        # 主索引：主键 -> 合并后的记录（哈希索引，O(1) 查找）
        self.store: Dict[Any, Dict[str, Any]] = {}
        # 字段级溯源：主键 -> (基准来源, {与基准来源不同的字段: 来源})
        self._prov: Dict[Any, Tuple[Optional[str], Dict[str, str]]] = {}
        # 幂等去重：已处理记录的内容指纹集合
        self._seen: set = set()

        self.conflicts: List[Conflict] = []
        self.conflicts_by_field: Counter = Counter()
        self.conflicts_won_by_source: Counter = Counter()
        self.stats: Counter = Counter()  # received / skipped_duplicates / inserted / updated

    # ---- 确定性决胜规则 -------------------------------------------------

    def _prio_rank(self, source: Optional[str]) -> int:
        return self._prio.get(source, self._default_prio)

    def _value_rank(self, source: Optional[str], value: Any) -> Tuple:
        """字段取值的全序：优先级 -> 来源名 -> 规范化值，min 胜。"""
        return (self._prio_rank(source), "" if source is None else source, _canon(value))

    def _record_rank(self, record: Dict[str, Any]) -> Tuple:
        src = record.get(self.source_field)
        ts = record.get(self.ts_field)
        ts_key = (ts is None, "" if ts is None else str(ts))
        if self.strategy == STRATEGY_KEEP_EARLIEST:
            # 时间戳最早者胜；并列再按优先级/来源/内容
            return (ts_key, self._prio_rank(src), "" if src is None else src, _canon(record))
        # overwrite：优先级最高者胜；并列按来源名/时间戳/内容
        return (self._prio_rank(src), "" if src is None else src, ts_key, _canon(record))

    # ---- 主流程 --------------------------------------------------------

    def process(self, record: Dict[str, Any]) -> str:
        """处理一条记录，返回动作：inserted / updated / skipped。"""
        self.stats["received"] += 1
        fp = _fingerprint(record)
        if fp in self._seen:
            self.stats["skipped_duplicates"] += 1
            return "skipped"
        self._seen.add(fp)

        key = record[self.key_field]
        existing = self.store.get(key)
        if existing is None:
            self.store[key] = dict(record)
            src = record.get(self.source_field)
            self._prov[key] = (src, {})
            self.stats["inserted"] += 1
            return "inserted"

        if self.strategy == STRATEGY_MERGE:
            self._merge_fields(key, existing, record)
        else:
            # overwrite / keep_earliest：整记录按全序决胜，min 保留
            if self._record_rank(record) < self._record_rank(existing):
                self.store[key] = dict(record)
                self._prov[key] = (record.get(self.source_field), {})
        self.stats["updated"] += 1
        return "updated"

    def merge_batch(self, records: Iterable[Dict[str, Any]]) -> Counter:
        for rec in records:
            self.process(rec)
        return self.stats

    def _field_source(self, key: Any, field_name: str) -> Optional[str]:
        base, overrides = self._prov[key]
        return overrides.get(field_name, base)

    def _set_field_source(self, key: Any, field_name: str, source: Optional[str]) -> None:
        base, overrides = self._prov[key]
        if source == base:
            overrides.pop(field_name, None)
        else:
            overrides[field_name] = source  # type: ignore[index]

    def _merge_fields(self, key: Any, existing: Dict[str, Any], incoming: Dict[str, Any]) -> None:
        inc_src = incoming.get(self.source_field)
        # 元字段也需确定性：_source 取 (优先级, 来源名) 最小者；_ts 取最早者
        cur_src = existing.get(self.source_field)
        if (self._prio_rank(inc_src), "" if inc_src is None else inc_src) < \
           (self._prio_rank(cur_src), "" if cur_src is None else cur_src):
            existing[self.source_field] = inc_src
        cur_ts, inc_ts = existing.get(self.ts_field), incoming.get(self.ts_field)
        if inc_ts is not None and (cur_ts is None or str(inc_ts) < str(cur_ts)):
            existing[self.ts_field] = inc_ts
        prio = self._prio
        default_prio = self._default_prio
        base, overrides = self._prov[key]
        for fname, new_val in incoming.items():
            if fname in (self.key_field, self.source_field, self.ts_field):
                continue
            old_val = existing.get(fname)
            if new_val in _MISSING:
                continue  # 缺失值不得覆盖已有值
            if old_val in _MISSING:
                existing[fname] = new_val
                if inc_src == base:
                    overrides.pop(fname, None)
                else:
                    overrides[fname] = inc_src
                continue
            if old_val == new_val:
                continue  # 值一致，无冲突
            old_src = overrides.get(fname, base)
            # 冲突：按全序决胜（优先级 -> 来源名 -> 规范化值，min 胜）
            old_rank = (prio.get(old_src, default_prio), "" if old_src is None else old_src)
            new_rank = (prio.get(inc_src, default_prio), "" if inc_src is None else inc_src)
            if new_rank < old_rank or (
                new_rank == old_rank and _canon(new_val) < _canon(old_val)
            ):
                chosen_src, chosen_val = inc_src, new_val
                existing[fname] = new_val
                if inc_src == base:
                    overrides.pop(fname, None)
                else:
                    overrides[fname] = inc_src
            else:
                chosen_src, chosen_val = old_src, old_val
            self._record_conflict(key, fname, old_src, old_val, inc_src, new_val,
                                  chosen_src, chosen_val)

    def _record_conflict(self, key, fname, old_src, old_val, inc_src, inc_val,
                         chosen_src, chosen_val) -> None:
        self.conflicts_by_field[fname] += 1
        self.conflicts_won_by_source[chosen_src] += 1
        self.stats["conflicts"] += 1
        if self.keep_conflict_log:
            self.conflicts.append(Conflict(key, fname, old_src, old_val,
                                           inc_src, inc_val, chosen_src, chosen_val))

    # ---- 报告 ----------------------------------------------------------

    def report(self) -> Dict[str, Any]:
        rep: Dict[str, Any] = {
            "strategy": self.strategy,
            "stats": dict(self.stats),
            "conflicts": {
                "total": self.stats.get("conflicts", 0),
                "by_field": dict(sorted(self.conflicts_by_field.items())),
                "won_by_source": dict(sorted(self.conflicts_won_by_source.items())),
            },
        }
        if self.keep_conflict_log:
            rep["conflict_log"] = [c.to_dict() for c in self.conflicts]
        return rep

    def result(self) -> List[Dict[str, Any]]:
        """合并结果，按主键规范化串排序，保证输出确定。"""
        return [self.store[k] for k in sorted(self.store, key=_canon)]
