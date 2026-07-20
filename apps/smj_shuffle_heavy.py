"""
Forces a shuffle-heavy Sort Merge Join with spillage:
  - broadcast join disabled  -> guarantees SMJ regardless of size
  - AQE disabled             -> no adaptive partition coalescing hiding the effect
  - shuffle.partitions = 8   -> too few for this data volume -> fat partitions
  - executor memory pinned to 1g -> each task's external sort spills to disk

Run (from inside the spark-master container):
    /opt/spark/bin/spark-submit smj_shuffle_heavy.py
"""
import time
from pyspark.sql import SparkSession

MASTER_URL = "spark://spark-master:7077"
DATA_DIR = "/opt/spark/work-dir/data"

spark = (
    SparkSession.builder
    .appName("smj-shuffle-heavy")
    .master(MASTER_URL)
    .config("spark.executor.memory", "512m")
    .config("spark.executor.cores", "1")
    .config("spark.cores.max", "2")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", f"file://{DATA_DIR}/spark-events")
    .getOrCreate()
)

events = spark.read.parquet(f"{DATA_DIR}/fact_events")
orders = spark.read.parquet(f"{DATA_DIR}/fact_orders")

joined = events.join(orders, "user_id")

print("\n=== PHYSICAL PLAN (expect Exchange hashpartitioning -> Sort -> SortMergeJoin on both sides) ===")
joined.explain(True)

start = time.time()
cnt = joined.count()
elapsed = time.time() - start

print(f"\njoined row count = {cnt:,}")
print(f"elapsed seconds  = {elapsed:.1f}")
print("\nNow check the Spark UI (or History Server at http://localhost:18080 after this exits):")
print("  - Stages tab -> click into the join's shuffle stage")
print("  - Look at 'Shuffle Read'/'Shuffle Write' size and, in the task table,")
print("    the 'Spill (Memory)' / 'Spill (Disk)' columns -> should be non-zero")

spark.stop()