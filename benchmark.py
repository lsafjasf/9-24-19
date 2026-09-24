"""基准：100 万条记录合并的耗时与内存峰值。

运行：python3 benchmark.py [n_records] [n_keys]
"""
import gc
import resource
import sys
import time
import tracemalloc

from record_merger import Merger, STRATEGY_MERGE

SOURCES = ["crm", "erp", "web", "app"]


def gen_records(n, n_keys, seed=42):
    """确定性生成 n 条记录：多来源、字段互补且部分冲突。"""
    import random
    rng = random.Random(seed)
    for i in range(n):
        key = rng.randrange(n_keys)
        src = SOURCES[rng.randrange(len(SOURCES))]
        r = {"id": key, "_source": src, "_ts": f"2024-01-{rng.randrange(28) + 1:02d}"}
        # 每个来源随机携带部分字段，值与来源相关 -> 制造冲突
        if rng.random() < 0.8:
            r["name"] = f"name-{key}-{src}"
        if rng.random() < 0.6:
            r["email"] = f"user{key}@{src}.com"
        if rng.random() < 0.5:
            r["phone"] = f"1{rng.randrange(10**10)}"
        if rng.random() < 0.4:
            r["city"] = f"city-{rng.randrange(100)}"
        if rng.random() < 0.3:
            r["tags"] = [src, f"t{rng.randrange(10)}"]
        yield r


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    n_keys = int(sys.argv[2]) if len(sys.argv) > 2 else 400_000
    print(f"records={n:,}  unique_keys={n_keys:,}  strategy=merge")

    # ---- 第 1 轮：纯计时（不含 tracemalloc 开销）----
    gc.collect()
    t0 = time.perf_counter()
    m = Merger(strategy=STRATEGY_MERGE, source_priority=SOURCES,
               keep_conflict_log=False)  # 大规模跑批只保留聚合计数
    m.merge_batch(gen_records(n, n_keys))
    elapsed = time.perf_counter() - t0
    print(f"elapsed:        {elapsed:.2f} s  ({n / elapsed:,.0f} rec/s)")
    print(f"merged records: {len(m.store):,}")
    print(f"stats:          {dict(m.stats)}")
    print(f"conflict by_field: {dict(m.report()['conflicts']['by_field'])}")

    # 幂等重放校验：同批数据重放，结果与冲突计数不得变化
    snap = {k: dict(v) for k, v in m.store.items()}
    conflicts_before = m.stats["conflicts"]
    skipped_before = m.stats["skipped_duplicates"]
    m.merge_batch(gen_records(n, n_keys))
    assert all(m.store[k] == v for k, v in snap.items()), "replay changed store!"
    assert m.stats["conflicts"] == conflicts_before, "replay added conflicts!"
    assert m.stats["skipped_duplicates"] == skipped_before + n
    print("replay check:   OK (store unchanged, no new conflicts, "
          f"skipped={m.stats['skipped_duplicates']:,})")
    del m, snap
    gc.collect()

    # ---- 第 2 轮：内存测量（tracemalloc 有运行时开销，不计时）----
    tracemalloc.start()
    m2 = Merger(strategy=STRATEGY_MERGE, source_priority=SOURCES,
                keep_conflict_log=False)
    m2.merge_batch(gen_records(n, n_keys))
    py_peak, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss_peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(f"python peak:    {py_peak / 2**20:.1f} MiB (tracemalloc, 含第1轮快照已释放)")
    print(f"process RSS峰值: {rss_peak_kb / 1024:.1f} MiB (ru_maxrss, 全进程)")


if __name__ == "__main__":
    main()
