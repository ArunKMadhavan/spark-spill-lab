"""
Same join as smj_shuffle_heavy.py, but both sides are written bucketed + sorted
on the join key first. Spark can then skip the Exchange (shuffle) entirely,
since matching bucket files already sit on the correct/consistent partitioning.

Run (from inside the spark-master container):
    /opt/spark/bin/spark-submit smj_bucketed.py
"""
import time
from pyspark.sql import SparkSession
import shutil

MASTER_URL = "spark://spark-master:7077"
DATA_DIR = "/opt/spark/work-dir/data"
BUCKETS = 16   # must be IDENTICAL on both tables to avoid a shuffle

spark = (
    SparkSession.builder
    .appName("smj-bucketed")
    .master(MASTER_URL)
    .config("spark.executor.memory", "512m")
    .config("spark.executor.cores", "1")
    .config("spark.cores.max", "2")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.warehouse.dir", f"{DATA_DIR}/warehouse")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", f"file://{DATA_DIR}/spark-events")
    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS smj_demo")
spark.sql("USE smj_demo")

events = spark.read.parquet(f"{DATA_DIR}/fact_events").repartition(BUCKETS, "user_id")
orders = spark.read.parquet(f"{DATA_DIR}/fact_orders").repartition(BUCKETS, "user_id")

shutil.rmtree(f"{DATA_DIR}/warehouse/smj_demo.db/events_bucketed", ignore_errors=True)
shutil.rmtree(f"{DATA_DIR}/warehouse/smj_demo.db/orders_bucketed", ignore_errors=True)
 
(events.write.mode("overwrite")
 .bucketBy(BUCKETS, "user_id").sortBy("user_id")
 .saveAsTable("smj_demo.events_bucketed"))
 
(orders.write.mode("overwrite")
 .bucketBy(BUCKETS, "user_id").sortBy("user_id")
 .saveAsTable("smj_demo.orders_bucketed"))
 
events_b = spark.table("smj_demo.events_bucketed")
orders_b = spark.table("smj_demo.orders_bucketed")


 
joined = events_b.join(orders_b, "user_id")

print("\n=== PHYSICAL PLAN (Exchange should be GONE; maybe even Sort) ===")
joined.explain(True)

start = time.time()
cnt = joined.count()
elapsed = time.time() - start

print(f"\njoined row count = {cnt:,}")
print(f"elapsed seconds  = {elapsed:.1f}")
print("\nCompare against smj_shuffle_heavy.py's stage in the History Server (localhost:18080): "
      "no Exchange stage before the join, and Spill columns should read ~0.")

spark.stop()