"""冲突留痕与汇总报告。"""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, asdict
from typing import Any, Iterator, List


@dataclass(frozen=True, slots=True)
class Conflict:
    """一次字段级冲突的完整留痕。"""
    key: Any                 # 主键
    field: str               # 冲突字段
    existing_source: str     # 已存值的来源
    existing_value: Any      # 已存值
    incoming_source: str     # 新值的来源
    incoming_value: Any      # 新值
    chosen_source: str       # 最终采用值的来源
    chosen_value: Any        # 最终采用值


class ConflictReport:
    """累积冲突记录，并按字段汇总冲突次数。"""

    def __init__(self) -> None:
        self.conflicts: List[Conflict] = []
        self._by_field: Counter = Counter()

    def add(self, conflict: Conflict) -> None:
        self.conflicts.append(conflict)
        self._by_field[conflict.field] += 1

    @property
    def total(self) -> int:
        return len(self.conflicts)

    def by_field(self) -> dict:
        """按字段统计冲突次数（按字段名排序，保证输出确定）。"""
        return dict(sorted(self._by_field.items()))

    def __iter__(self) -> Iterator[Conflict]:
        return iter(self.conflicts)

    def __len__(self) -> int:
        return len(self.conflicts)

    def to_dict(self) -> dict:
        return {
            "total_conflicts": self.total,
            "by_field": self.by_field(),
            "conflicts": [asdict(c) for c in self.conflicts],
        }

    def to_json(self, **kwargs) -> str:
        kwargs.setdefault("ensure_ascii", False)
        kwargs.setdefault("indent", 2)
        return json.dumps(self.to_dict(), **kwargs)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ConflictReport) and self.to_dict() == other.to_dict()
