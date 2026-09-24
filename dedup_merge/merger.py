"""记录去重与合并核心库。

决胜规则（确定性，不依赖到达顺序）：
  候选之间按元组 (-来源优先级, 来源名, 值的规范JSON) 取最小者胜。
  即：优先级高者胜 -> 并列时来源名字典序小者胜 -> 再并列时值的
  规范 JSON（sort_keys 序列化）字典序小者胜。完全确定，与到达顺序无关。

keep_earliest 策略先比较 timestamp（小者胜，缺失视为 +inf），
timestamp 相同再套用上述决胜规则。

幂等：每条记录以规范 JSON 的 SHA1 指纹入 _seen 集合，完全相同的记录
重复投递时直接跳过，不产生任何合并动作与冲突记录，因此重复投递同一批
数据，合并结果与冲突报告与第一次完全一致（仅计数类字段变化）。
"""
from __future__ import annotations

import hashlib
import json
import math
from enum import Enum
from typing import Any, Dict, Iterable, Optional, Tuple

from .conflict import Conflict, ConflictReport

_ABSENT = object()  # 字段不存在（区别于值为 None）


class Strategy(Enum):
    OVERWRITE = "overwrite"            # 覆盖：整条记录按来源优先级取舍
    KEEP_EARLIEST = "keep_earliest"    # 保留最早：timestamp 最小者保留
    MERGE_FIELDS = "merge_fields"      # 按键合并：字段级合并，缺失不覆盖


def _canon(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


class Merger:
    def __init__(
        self,
        key: str = "id",
        source: str = "source",
        timestamp: str = "timestamp",
        strategy: Strategy = Strategy.MERGE_FIELDS,
        priorities: Optional[Dict[str, int]] = None,
        trace_conflicts: bool = True,
    ) -> None:
        self.key_field = key
        self.source_field = source
        self.timestamp_field = timestamp
        self.strategy = strategy
        self.priorities = dict(priorities or {})
        self.trace_conflicts = trace_conflicts

        # 索引结构（全部为哈希表，均摊 O(1) 查找）：
        # _records:       主键 -> 合并后记录            主索引
        # _record_source: 主键 -> 当前整条记录的来源    overwrite/keep_earliest 用
        # _record_ts:     主键 -> 当前记录的时间戳      keep_earliest 用
        # _field_sources: 主键 -> {字段 -> 来源}        merge_fields 字段级溯源
        # _seen:          记录指纹(SHA1)集合            幂等去重索引
        self._records: Dict[Any, dict] = {}
        self._record_source: Dict[Any, str] = {}
        self._record_ts: Dict[Any, float] = {}
        self._field_sources: Dict[Any, Dict[str, str]] = {}
        self._seen: set = set()

        self.report = ConflictReport()
        self.ingested = 0            # 计数类字段
        self.duplicates_skipped = 0  # 计数类字段

    # ------------------------------------------------------------------ API
    def ingest(self, record: dict) -> bool:
        """处理一条记录。返回 False 表示重复投递被跳过（无副作用）。"""
        fp = self._fingerprint(record)
        if fp in self._seen:
            self.duplicates_skipped += 1
            return False
        self._seen.add(fp)
        self.ingested += 1

        key = record[self.key_field]
        if key not in self._records:
            self._records[key] = self._strip_meta(record)
            self._record_source[key] = record.get(self.source_field, "")
            self._record_ts[key] = record.get(self.timestamp_field, math.inf)
            self._field_sources[key] = {
                f: record.get(self.source_field, "") for f in self._records[key]
            }
            return True

        if self.strategy is Strategy.MERGE_FIELDS:
            self._merge_fields(key, record)
        else:
            self._merge_whole(key, record)
        return True

    def ingest_batch(self, records: Iterable[dict]) -> None:
        for r in records:
            self.ingest(r)

    @property
    def records(self) -> Dict[Any, dict]:
        return self._records

    def get(self, key: Any) -> Optional[dict]:
        return self._records.get(key)

    def __len__(self) -> int:
        return len(self._records)

    # -------------------------------------------------------------- internal
    def _fingerprint(self, record: dict) -> bytes:
        return hashlib.sha1(_canon(record).encode("utf-8")).digest()

    def _strip_meta(self, record: dict) -> dict:
        return {
            f: v for f, v in record.items()
            if f not in (self.source_field, self.timestamp_field)
        }

    def _priority(self, source: str) -> int:
        return self.priorities.get(source, 0)

    def _win_key(self, source: str, value: Any) -> Tuple:
        """越小越优：优先级高者胜，并列比来源名，再并列比值。"""
        return (-self._priority(source), source, _canon(value))

    def _pick(self, a: Tuple[str, Any], b: Tuple[str, Any]) -> Tuple[str, Any]:
        """在 (来源, 值) 两个候选中确定性地选出胜者。"""
        return min((a, b), key=lambda c: self._win_key(c[0], c[1]))

    def _data_fields(self, record: dict):
        for f, v in record.items():
            if f not in (self.key_field, self.source_field, self.timestamp_field):
                yield f, v

    def _merge_fields(self, key: Any, incoming: dict) -> None:
        existing = self._records[key]
        provenance = self._field_sources[key]
        in_src = incoming.get(self.source_field, "")
        for field, in_val in self._data_fields(incoming):
            if in_val is None:
                continue  # 缺失值不得覆盖已有值
            ex_val = existing.get(field)
            if ex_val is None:
                existing[field] = in_val  # 补缺，不算冲突
                provenance[field] = in_src
            elif ex_val != in_val:
                ex_src = provenance.get(field, "")
                chosen_src, chosen_val = self._pick((ex_src, ex_val), (in_src, in_val))
                if self.trace_conflicts:
                    self.report.add(Conflict(
                        key=key, field=field,
                        existing_source=ex_src, existing_value=ex_val,
                        incoming_source=in_src, incoming_value=in_val,
                        chosen_source=chosen_src, chosen_value=chosen_val,
                    ))
                existing[field] = chosen_val
                provenance[field] = chosen_src

    def _merge_whole(self, key: Any, incoming: dict) -> None:
        existing = self._records[key]
        in_src = incoming.get(self.source_field, "")
        ex_src = self._record_source[key]
        in_rec = self._strip_meta(incoming)

        if self.strategy is Strategy.KEEP_EARLIEST:
            in_ts = incoming.get(self.timestamp_field, math.inf)
            ex_key = (self._record_ts[key],) + self._win_key(ex_src, existing)
            in_key = (in_ts,) + self._win_key(in_src, in_rec)
        else:  # OVERWRITE
            in_ts = self._record_ts[key]
            ex_key = self._win_key(ex_src, existing)
            in_key = self._win_key(in_src, in_rec)

        winner_is_incoming = in_key < ex_key
        winner_src = in_src if winner_is_incoming else ex_src
        winner_rec = in_rec if winner_is_incoming else existing

        # 字段级留痕：双方都有且取值不同的字段记为冲突
        for field, w_val in winner_rec.items():
            l_val = (existing if winner_is_incoming else in_rec).get(field, _ABSENT)
            if l_val is not _ABSENT and l_val != w_val and self.trace_conflicts:
                self.report.add(Conflict(
                    key=key, field=field,
                    existing_source=ex_src, existing_value=existing.get(field),
                    incoming_source=in_src, incoming_value=in_rec.get(field),
                    chosen_source=winner_src, chosen_value=w_val,
                ))
        if winner_is_incoming:
            self._records[key] = in_rec
            self._record_source[key] = in_src
            self._record_ts[key] = in_ts
