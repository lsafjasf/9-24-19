"""生成冲突报告样例 examples/conflict_report_sample.json"""
import json
from record_merger import Merger, STRATEGY_MERGE

m = Merger(strategy=STRATEGY_MERGE, source_priority=["crm", "erp", "web"])
m.merge_batch([
    {"id": 1, "_source": "web",  "name": "Alice", "city": "Shanghai", "zip": "200000"},
    {"id": 1, "_source": "crm",  "city": "Beijing", "phone": "13800000000"},
    {"id": 1, "_source": "erp",  "zip": "100000", "city": "Beijing"},
    {"id": 2, "_source": "erp",  "name": "Bob", "email": "bob@corp.com"},
    {"id": 2, "_source": "web",  "name": "Bob", "email": ""},
])
rep = m.report()
rep["merged_result"] = m.result()
with open("examples/conflict_report_sample.json", "w", encoding="utf-8") as f:
    json.dump(rep, f, ensure_ascii=False, indent=2)
print(json.dumps(rep, ensure_ascii=False, indent=2))
