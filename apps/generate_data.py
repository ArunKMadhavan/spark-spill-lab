"""
Generates two large synthetic datasets for a Sort-Merge-Join exercise.

Run (from inside the spark-master container):
    /opt/spark/bin/spark-submit generate_data.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---- adjust these if they don't match your docker-compose file ----
MASTER_URL = "spark://spark-master:7077"
DATA_DIR = "/opt/spark/work-dir/data"   # must be mounted at the SAME path on master + BOTH workers
NUM_USERS = 2_000_000
NUM_EVENTS = 20_000_000
NUM_ORDERS = 15_000_000
# ---------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("gen-data")
    .master(MASTER_URL)
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", f"file://{DATA_DIR}/spark-events")
    .config("spark.sql.warehouse.dir", f"{DATA_DIR}/warehouse")
    .getOrCreate()
)


def make_events():
    return (
        spark.range(0, NUM_EVENTS)
        .withColumnRenamed("id", "event_id")
        .withColumn("user_id", (F.rand(seed=42) * NUM_USERS).cast("long"))
        .withColumn("event_type", (F.rand(seed=7) * 5).cast("int"))
        .withColumn("amount", F.round(F.rand(seed=13) * 500, 2))
        .withColumn("event_ts", F.expr("date_add(current_date(), cast(rand()*365 as int))"))
    )


def make_orders():
    return (
        spark.range(0, NUM_ORDERS)
        .withColumnRenamed("id", "order_id")
        .withColumn("user_id", (F.rand(seed=99) * NUM_USERS).cast("long"))
        .withColumn("order_amount", F.round(F.rand(seed=21) * 2000, 2))
        .withColumn("order_ts", F.expr("date_add(current_date(), cast(rand()*365 as int))"))
    )


events = make_events().repartition(16)
orders = make_orders().repartition(16)

events.write.mode("overwrite").parquet(f"{DATA_DIR}/fact_events")
orders.write.mode("overwrite").parquet(f"{DATA_DIR}/fact_orders")

events_cnt = spark.read.parquet(f"{DATA_DIR}/fact_events").count()
orders_cnt = spark.read.parquet(f"{DATA_DIR}/fact_orders").count()

print(f"events written & read back: {events_cnt:,} (expected {NUM_EVENTS:,})")
print(f"orders written & read back: {orders_cnt:,} (expected {NUM_ORDERS:,})")

if events_cnt < NUM_EVENTS or orders_cnt < NUM_ORDERS:
    print("!! Count mismatch -> DATA_DIR is likely NOT a real shared volume across "
          "both workers. Each worker can only see the part-files it wrote locally. "
          "Check the `volumes:` section of your docker-compose file.")

spark.stop()