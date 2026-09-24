"""生成冲突报告样例 examples/conflict_report_sample.json"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dedup_merge import Merger, Strategy

m = Merger(strategy=Strategy.MERGE_FIELDS,
           priorities={"crm": 3, "web": 2, "import": 1})
m.ingest_batch([
    {"id": "u-1001", "source": "web",    "name": "Alice W", "email": "alice@web.com", "city": "Shanghai"},
    {"id": "u-1001", "source": "crm",    "name": "Alice",   "email": None,            "city": "Beijing", "vip": True},
    {"id": "u-1001", "source": "import", "name": "Alice W", "email": "alice@old.com", "city": "Beijing"},
    {"id": "u-1002", "source": "import", "name": "Bob",     "phone": "111"},
    {"id": "u-1002", "source": "web",    "name": "Bobby",   "phone": "222"},
])

out = {
    "merged_records": [m.records[k] for k in sorted(m.records)],
    "conflict_report": m.report.to_dict(),
}
path = os.path.join(os.path.dirname(__file__), "conflict_report_sample.json")
with open(path, "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2)
print(f"written: {path}")
print(json.dumps(out["conflict_report"]["by_field"], ensure_ascii=False))
