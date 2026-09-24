"""record_merger 自测：策略正确性、冲突留痕、幂等重放、顺序无关性。"""
import copy
import json
import random
import sys
import unittest

sys.path.insert(0, ".")
from record_merger import (Merger, STRATEGY_MERGE, STRATEGY_OVERWRITE,
                           STRATEGY_KEEP_EARLIEST)

PRIO = ["crm", "erp", "web"]  # crm 优先级最高


def rec(rid, source, ts=None, **fields):
    r = {"id": rid, "_source": source}
    if ts is not None:
        r["_ts"] = ts
    r.update(fields)
    return r


class TestStrategies(unittest.TestCase):
    def test_overwrite_high_priority_wins(self):
        m = Merger(strategy=STRATEGY_OVERWRITE, source_priority=PRIO)
        m.process(rec(1, "web", name="from_web", age=30))
        m.process(rec(1, "crm", name="from_crm"))
        self.assertEqual(m.store[1]["name"], "from_crm")
        self.assertNotIn("age", m.store[1])  # 整记录覆盖

    def test_keep_earliest(self):
        m = Merger(strategy=STRATEGY_KEEP_EARLIEST, source_priority=PRIO)
        m.process(rec(1, "crm", ts="2024-05-01", name="newer"))
        m.process(rec(1, "web", ts="2024-01-01", name="older"))
        self.assertEqual(m.store[1]["name"], "older")

    def test_merge_missing_never_overwrites(self):
        m = Merger(strategy=STRATEGY_MERGE, source_priority=PRIO)
        m.process(rec(1, "web", name="alice", phone="123"))
        m.process(rec(1, "crm", name=None, phone="", email="a@x.com"))
        self.assertEqual(m.store[1]["name"], "alice")
        self.assertEqual(m.store[1]["phone"], "123")
        self.assertEqual(m.store[1]["email"], "a@x.com")

    def test_merge_conflict_resolved_by_priority(self):
        m = Merger(strategy=STRATEGY_MERGE, source_priority=PRIO)
        m.process(rec(1, "web", city="shanghai"))
        m.process(rec(1, "crm", city="beijing"))
        self.assertEqual(m.store[1]["city"], "beijing")  # crm 优先

    def test_merge_field_complement(self):
        m = Merger(strategy=STRATEGY_MERGE, source_priority=PRIO)
        m.process(rec(1, "crm", name="alice"))
        m.process(rec(1, "erp", phone="123"))
        m.process(rec(1, "web", email="a@x.com"))
        self.assertEqual(m.store[1]["name"], "alice")
        self.assertEqual(m.store[1]["phone"], "123")
        self.assertEqual(m.store[1]["email"], "a@x.com")


class TestConflictTrace(unittest.TestCase):
    def test_conflict_log_contents(self):
        m = Merger(strategy=STRATEGY_MERGE, source_priority=PRIO)
        m.process(rec(1, "web", city="shanghai"))
        m.process(rec(1, "crm", city="beijing"))
        self.assertEqual(len(m.conflicts), 1)
        c = m.conflicts[0]
        self.assertEqual(c.field, "city")
        self.assertEqual(c.existing_source, "web")
        self.assertEqual(c.incoming_source, "crm")
        self.assertEqual(c.chosen_source, "crm")
        self.assertEqual(c.chosen_value, "beijing")

    def test_report_by_field(self):
        m = Merger(strategy=STRATEGY_MERGE, source_priority=PRIO)
        m.process(rec(1, "web", city="s1", zip="100"))
        m.process(rec(1, "crm", city="s2", zip="200"))
        m.process(rec(2, "erp", city="s3"))
        m.process(rec(2, "crm", city="s4"))
        rep = m.report()
        self.assertEqual(rep["conflicts"]["total"], 3)
        self.assertEqual(rep["conflicts"]["by_field"], {"city": 2, "zip": 1})
        self.assertEqual(rep["conflicts"]["won_by_source"], {"crm": 3})


class TestIdempotentReplay(unittest.TestCase):
    def _batch(self):
        return [
            rec(1, "web", name="alice", city="sh"),
            rec(1, "crm", city="bj", phone="1"),
            rec(2, "erp", name="bob"),
            rec(2, "web", name="bob", email="b@x.com"),
            rec(3, "crm", name="carol"),
        ]

    def test_replay_same_batch(self):
        m = Merger(strategy=STRATEGY_MERGE, source_priority=PRIO)
        batch = self._batch()
        m.merge_batch(copy.deepcopy(batch))
        store1 = copy.deepcopy(m.store)
        report1 = m.report()

        m.merge_batch(copy.deepcopy(batch))  # 重复投递同一批
        self.assertEqual(m.store, store1)                 # 结果一致
        self.assertEqual(m.conflicts, m.conflicts[:len(m.conflicts)])  # 无新增
        report2 = m.report()
        # 报告一致（除计数类字段 received / skipped_duplicates）
        self.assertEqual(report2["conflicts"], report1["conflicts"])
        self.assertEqual(report2["conflict_log"], report1["conflict_log"])
        self.assertEqual(report2["stats"]["inserted"], report1["stats"]["inserted"])
        self.assertEqual(report2["stats"]["updated"], report1["stats"]["updated"])
        self.assertEqual(m.stats["skipped_duplicates"], len(batch))
        self.assertEqual(m.stats["received"], 2 * len(batch))

    def test_replay_after_other_records(self):
        m = Merger(strategy=STRATEGY_MERGE, source_priority=PRIO)
        m.merge_batch(self._batch())
        store1 = copy.deepcopy(m.store)
        conflicts1 = len(m.conflicts)
        m.process(rec(9, "web", name="intruder"))
        m.merge_batch(self._batch())  # 中间插入新数据后重放旧批，仍无副作用
        self.assertEqual(len(m.conflicts), conflicts1)
        self.assertEqual(m.store[1], store1[1])
        self.assertEqual(m.store[2], store1[2])


class TestDeterminism(unittest.TestCase):
    def test_order_independence_same_priority(self):
        # 两个未列入优先级表的来源（优先级相同），结果不得依赖到达顺序
        base = [
            rec(i, src, val=f"{src}-{i}", common="x")
            for i in range(50)
            for src in ("srcA", "srcB")
        ]
        results = []
        for seed in range(5):
            batch = copy.deepcopy(base)
            random.Random(seed).shuffle(batch)
            m = Merger(strategy=STRATEGY_MERGE)  # 无优先级表：所有来源平级
            m.merge_batch(batch)
            results.append(json.dumps(m.result(), sort_keys=True))
        self.assertEqual(len(set(results)), 1)

    def test_order_independence_with_priority(self):
        base = [
            rec(i, src, ts=f"2024-01-{i % 28 + 1:02d}", a=f"{src}{i}", b=i)
            for i in range(50)
            for src in PRIO
        ]
        results = []
        for seed in range(5):
            batch = copy.deepcopy(base)
            random.Random(seed).shuffle(batch)
            m = Merger(strategy=STRATEGY_MERGE, source_priority=PRIO)
            m.merge_batch(batch)
            results.append(json.dumps(m.result(), sort_keys=True))
        self.assertEqual(len(set(results)), 1)

    def test_tie_break_rule(self):
        # 平级来源：来源名字典序小者胜
        m = Merger(strategy=STRATEGY_MERGE)
        m.process(rec(1, "srcB", v="b"))
        m.process(rec(1, "srcA", v="a"))
        self.assertEqual(m.store[1]["v"], "a")
        self.assertEqual(m.conflicts[0].chosen_source, "srcA")


if __name__ == "__main__":
    unittest.main(verbosity=2)
