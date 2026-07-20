# Spark Sort Merge Join: Spill, Bucketing, and Bucket Count

Companion repo for the article [I Bucketed My Spark Join to Fix Spill. It Still Spilled. Here's What I Missed]https://medium.com/@arunkumarmadhavannair/i-bucketed-my-spark-join-to-fix-spill-it-still-spilled-heres-what-i-missed-04df04c1c5af?sharedUserId=arunkumarmadhavannair

A minimal, reproducible Spark cluster and three PySpark scripts that walk through:

1. A naive Sort Merge Join that spills to disk
2. Bucketing the tables to eliminate the shuffle — and finding that spill still happens
3. Sizing the bucket count correctly to drive spill to zero

## Cluster

- 1 Spark master, 2 workers (2 cores / 3 GB each)
- Standalone mode, Spark History Server on port 18080
- Image: `spark:python3-java17`

## Layout

```
.
├── docker-compose.yml
├── apps/
│   ├── generate_data.py         # writes fact_events (20M) + fact_orders (15M) as Parquet
│   ├── smj_shuffle_heavy.py     # naive SMJ, shows spill
│   └── smj_bucketed.py          # bucketed SMJ, tune BUCKETS to see spill disappear
└── data/                        # shared volume for parquet files and event logs
```

## Run it

```bash
mkdir -p apps data data/spark-events
docker compose up -d
docker compose ps               # confirm master + 2 workers + history server are up

docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/apps/generate_data.py
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/apps/smj_shuffle_heavy.py
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/apps/smj_bucketed.py
```

## Web UIs

- Master: http://localhost:8080
- Worker 1: http://localhost:8081
- Worker 2: http://localhost:8082
- History Server: http://localhost:18080

## What to look at

After each run, open the History Server, click the completed application, then:

- **SQL/DataFrame tab** for the visual physical plan
- **Stages tab** → the join stage → *Show Additional Metrics* → *Select All* → look at **Spill (Memory)** and **Spill (Disk)** columns

## Tuning knobs to experiment with

In `smj_shuffle_heavy.py` / `smj_bucketed.py`:

- `spark.executor.memory` — set to `512m` to force spill for the exercise; raise for realistic runs
- `spark.sql.shuffle.partitions` — only takes effect when there's an actual shuffle
- `BUCKETS` (in `smj_bucketed.py`) — the real lever for match-buffer spill in a bucketed join. Try 8, 16, 32.
