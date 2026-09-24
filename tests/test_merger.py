import copy
import random
import unittest

from dedup_merge import Merger, Strategy

PRIORITIES = {"crm": 3, "web": 2, "import": 1}


def rec(rid, source, ts=None, **fields):
    r = {"id": rid, "source": source}
    if ts is not None:
        r["timestamp"] = ts
    r.update(fields)
    return r


class TestMergeFields(unittest.TestCase):
    def setUp(self):
        self.m = Merger(strategy=Strategy.MERGE_FIELDS, priorities=PRIORITIES)

    def test_missing_never_overwrites(self):
        self.m.ingest(rec(1, "web", name="Alice", email="a@x.com"))
        self.m.ingest(rec(1, "crm", name=None, email=None, phone="123"))
        r = self.m.get(1)
        self.assertEqual(r["name"], "Alice")
        self.assertEqual(r["email"], "a@x.com")
        self.assertEqual(r["phone"], "123")  # 互补字段合并
        self.assertEqual(self.m.report.total, 0)  # 补缺不算冲突

    def test_conflict_resolved_by_priority(self):
        self.m.ingest(rec(1, "web", name="WebName"))
        self.m.ingest(rec(1, "crm", name="CrmName"))
        self.assertEqual(self.m.get(1)["name"], "CrmName")
        # 反向到达结果相同（高优先级总是胜出）
        m2 = Merger(strategy=Strategy.MERGE_FIELDS, priorities=PRIORITIES)
        m2.ingest(rec(1, "crm", name="CrmName"))
        m2.ingest(rec(1, "web", name="WebName"))
        self.assertEqual(m2.get(1)["name"], "CrmName")

    def test_conflict_trace(self):
        self.m.ingest(rec(1, "web", name="WebName", city="SH"))
        self.m.ingest(rec(1, "crm", name="CrmName", city="BJ"))
        self.assertEqual(self.m.report.total, 2)
        c = next(c for c in self.m.report if c.field == "name")
        self.assertEqual(c.existing_source, "web")
        self.assertEqual(c.existing_value, "WebName")
        self.assertEqual(c.incoming_source, "crm")
        self.assertEqual(c.incoming_value, "CrmName")
        self.assertEqual(c.chosen_source, "crm")
        self.assertEqual(c.chosen_value, "CrmName")
        self.assertEqual(self.m.report.by_field(), {"city": 1, "name": 1})

    def test_equal_priority_deterministic(self):
        # 优先级相同：按来源名字典序，再按值，结果与到达顺序无关
        batch = [
            rec(1, "b_src", name="bbb"),
            rec(1, "a_src", name="aaa"),
            rec(1, "c_src", name="ccc"),
        ]
        results = set()
        for _ in range(20):
            random.shuffle(batch)
            m = Merger(strategy=Strategy.MERGE_FIELDS)  # 全部默认优先级 0
            m.ingest_batch(copy.deepcopy(batch))
            results.add(str(sorted(m.get(1).items())))
        self.assertEqual(len(results), 1)
        m = Merger(strategy=Strategy.MERGE_FIELDS)
        m.ingest_batch(copy.deepcopy(batch))
        self.assertEqual(m.get(1)["name"], "aaa")  # a_src 字典序最小且值最小


class TestOverwrite(unittest.TestCase):
    def test_whole_record_replaced_by_priority(self):
        m = Merger(strategy=Strategy.OVERWRITE, priorities=PRIORITIES)
        m.ingest(rec(1, "crm", name="CrmName", city="BJ"))
        m.ingest(rec(1, "web", name="WebName", city="SH"))
        # crm 优先级高，web 整条不生效
        self.assertEqual(m.get(1), {"id": 1, "name": "CrmName", "city": "BJ"})
        self.assertEqual(m.report.total, 2)  # 两个不同字段均留痕
        self.assertEqual(self.m_last_chosen(m), "crm")

    def m_last_chosen(self, m):
        return m.report.conflicts[0].chosen_source

    def test_higher_priority_arriving_later_wins(self):
        m = Merger(strategy=Strategy.OVERWRITE, priorities=PRIORITIES)
        m.ingest(rec(1, "web", name="WebName"))
        m.ingest(rec(1, "crm", name="CrmName", phone="9"))
        self.assertEqual(m.get(1), {"id": 1, "name": "CrmName", "phone": "9"})


class TestKeepEarliest(unittest.TestCase):
    def test_earliest_timestamp_kept(self):
        m = Merger(strategy=Strategy.KEEP_EARLIEST, priorities=PRIORITIES)
        m.ingest(rec(1, "crm", ts=200, name="Late"))
        m.ingest(rec(1, "web", ts=100, name="Early"))
        m.ingest(rec(1, "import", ts=300, name="Latest"))
        self.assertEqual(m.get(1)["name"], "Early")

    def test_same_timestamp_deterministic(self):
        batch = [rec(1, "b", ts=100, name="B"), rec(1, "a", ts=100, name="A")]
        results = set()
        for _ in range(10):
            random.shuffle(batch)
            m = Merger(strategy=Strategy.KEEP_EARLIEST)
            m.ingest_batch(copy.deepcopy(batch))
            results.add(m.get(1)["name"])
        self.assertEqual(len(results), 1)


class TestIdempotentReplay(unittest.TestCase):
    def _batch(self):
        return [
            rec(1, "web", name="WebName", email="a@x.com"),
            rec(1, "crm", name="CrmName", city="BJ"),
            rec(2, "import", name="X", phone="1"),
            rec(2, "web", name="X2", phone=None),
            rec(3, "crm", name="Solo"),
        ]

    def test_replay_identical(self):
        for strategy in Strategy:
            m = Merger(strategy=strategy, priorities=PRIORITIES)
            m.ingest_batch(copy.deepcopy(self._batch()))
            snapshot_records = copy.deepcopy(m.records)
            snapshot_report = m.report.to_dict()
            # 重复投递同一批（换序也试）
            batch2 = self._batch()
            random.shuffle(batch2)
            m.ingest_batch(copy.deepcopy(batch2))
            self.assertEqual(m.records, snapshot_records, strategy)
            self.assertEqual(m.report.to_dict(), snapshot_report, strategy)
            self.assertEqual(m.duplicates_skipped, len(batch2))

    def test_partial_overlap_still_merges(self):
        m = Merger(strategy=Strategy.MERGE_FIELDS, priorities=PRIORITIES)
        m.ingest_batch(self._batch())
        n_conflicts = m.report.total
        # 重复一条 + 一条真正的新数据
        m.ingest(rec(1, "web", name="WebName", email="a@x.com"))  # 重复，跳过
        m.ingest(rec(1, "crm", name="CrmName", city="SH"))        # 新记录，参与合并
        self.assertEqual(m.duplicates_skipped, 1)
        self.assertEqual(m.report.total, n_conflicts + 1)  # city 冲突 BJ vs SH
        self.assertEqual(m.get(1)["city"], "BJ")  # 同来源同优先级，值字典序小者胜


class TestReportSample(unittest.TestCase):
    def test_report_json_shape(self):
        m = Merger(strategy=Strategy.MERGE_FIELDS, priorities=PRIORITIES)
        m.ingest_batch(self._batch())
        d = m.report.to_dict()
        self.assertIn("total_conflicts", d)
        self.assertIn("by_field", d)
        self.assertEqual(d["total_conflicts"], sum(d["by_field"].values()))

    def _batch(self):
        return [
            rec(1, "web", name="WebName", email="a@x.com"),
            rec(1, "crm", name="CrmName", email="crm@x.com"),
            rec(2, "import", name="X"),
            rec(2, "web", name="Y"),
        ]


if __name__ == "__main__":
    unittest.main()
