"""百万级记录合并基准：python3 bench.py [总记录数] [唯一主键数]"""
import gc
import os
import random
import resource
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dedup_merge import Merger, Strategy

SOURCES = ["crm", "web", "import", "app", "partner"]
PRIORITIES = {"crm": 5, "web": 4, "import": 3, "app": 2, "partner": 1}
FIELDS = ["name", "email", "phone", "city", "addr", "tag", "level", "note"]


def gen_records(total, unique_keys, seed=42):
    rng = random.Random(seed)
    for i in range(total):
        key = rng.randrange(unique_keys)
        src = SOURCES[i % len(SOURCES)]
        r = {"id": key, "source": src, "timestamp": rng.randrange(10**6)}
        for f in FIELDS:
            if rng.random() < 0.7:  # 30% 缺失
                r[f] = f"{f}-{key}-{rng.randrange(3)}"  # 小范围取值制造冲突
            else:
                r[f] = None
        yield r


def peak_rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def main():
    total = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    unique = int(sys.argv[2]) if len(sys.argv) > 2 else 250_000
    gc.collect()
    base = peak_rss_mb()

    t0 = time.perf_counter()
    m = Merger(strategy=Strategy.MERGE_FIELDS, priorities=PRIORITIES)
    m.ingest_batch(gen_records(total, unique))
    t1 = time.perf_counter()

    print(f"strategy           : {m.strategy.value}")
    print(f"input records      : {total:,}")
    print(f"unique keys        : {len(m):,}")
    print(f"conflicts traced   : {m.report.total:,}")
    print(f"elapsed            : {t1 - t0:.2f} s  ({total / (t1 - t0):,.0f} rec/s)")
    print(f"peak RSS           : {peak_rss_mb():.0f} MiB (process baseline {base:.0f} MiB)")

    # 幂等重放验证（同一批数据再投一遍）
    t2 = time.perf_counter()
    m.ingest_batch(gen_records(total, unique))
    t3 = time.perf_counter()
    print(f"replay skipped     : {m.duplicates_skipped:,} in {t3 - t2:.2f} s")


if __name__ == "__main__":
    main()
