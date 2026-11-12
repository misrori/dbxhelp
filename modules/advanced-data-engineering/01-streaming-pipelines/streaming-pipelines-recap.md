# Recap: Databricks Streaming and Lakeflow Spark Declarative Pipelines

> **Course 1 of 4 · Advanced Data Engineering with Databricks** · ⏱ ~9 minutes
>
> Production streaming: how **Structured Streaming** keeps state, emits results and recovers, and how **declarative pipelines** apply it to ingestion, joins, deduplication and **CDC**.

**The one idea to remember:** a stream is an **unbounded table** processed incrementally. **Checkpoints** make it exactly-once, **watermarks** bound its state, and declarative pipelines manage most of this for you.

## What we covered

| Topic | Key takeaway |
|---|---|
| Streaming concepts | Unbounded table, micro-batches, latency vs cost, batch ↔ streaming with the same API |
| Structured Streaming basics | `readStream` / `writeStream`, sources and sinks, **triggers**, **output modes**, **checkpoints** |
| Stateful processing | Aggregations, **windows** (tumbling, sliding, session), **watermarks**, late data |
| Streaming joins & deduplication | Stream-static vs stream-stream joins, `dropDuplicatesWithinWatermark` |
| Streaming in declarative pipelines | Streaming tables, Auto Loader and Kafka ingestion, append flows, quarantine |
| CDC and SCD | **AUTO CDC** with `SEQUENCE BY`, SCD Type 1 and 2, snapshots, change data feed |

## Structured Streaming in one example

```python
from pyspark.sql import functions as F

events = (spark.readStream                       # source: Delta table, Auto Loader, Kafka…
          .table("main.bronze.device_events"))

per_window = (events
    .withWatermark("event_ts", "10 minutes")     # accept data up to 10 min late, then drop state
    .groupBy(F.window("event_ts", "5 minutes"), "device_id")
    .agg(F.avg("temp").alias("avg_temp")))

(per_window.writeStream
    .outputMode("append")                        # emit each window once, when it is final
    .option("checkpointLocation", "/Volumes/main/chk/device_5min")
    .trigger(availableNow=True)                  # process what's there, then stop
    .toTable("main.silver.device_5min"))
```

| Concept | Options |
|---|---|
| **Trigger** | default / `processingTime="1 minute"` (continuous micro-batches) · **`availableNow=True`** (incremental batch, then stop) · real-time mode (very low latency) |
| **Output mode** | **Append**: only new, final rows (aggregations need a watermark) · **Update**: changed rows (use with `foreachBatch` + `MERGE`) · **Complete**: whole result each trigger (keeps all state) |
| **Checkpoint** | Source offsets + committed batches + state. **One per query.** Never share it or delete it |

## State, windows and watermarks

```
tumbling  |----|----|----|      fixed, non-overlapping: "per 5 minutes"
sliding   |------|               overlapping: "last 10 minutes, every 5 minutes"
            |------|
session   |--|   |-----|         gap-based: "activity bursts per user"
```

- **Stateless** operations (`select`, `filter`, stream-static join) don't remember anything between micro-batches.
- **Stateful** operations (aggregations, `dropDuplicates`, stream-stream joins) keep **state** in the state store (RocksDB), and it is saved in the checkpoint.
- **Watermark** = the latest event time seen minus the allowed delay. Windows older than the watermark are **finalized** and their state is **dropped**. Events arriving later than that can be ignored.
- A longer delay accepts later data, but keeps **more state** and delays append-mode output.
- Changing the aggregation keys, the window, or the number of stateful operators requires a **new checkpoint**.

## Streaming joins and deduplication

| Join | How it works | Watch out |
|---|---|---|
| **Stream-static** | Each micro-batch joins with the **current** version of a Delta table | Dimension changes are **not** re-applied to past results. Use an MV when results must stay correct |
| **Stream-stream** | Both sides buffered in state until they can match | Needs **watermarks on both sides** plus a **time-range condition**, otherwise state grows forever |

```python
deduped = (events
    .withWatermark("event_ts", "1 hour")
    .dropDuplicatesWithinWatermark(["event_id"]))   # bounded state
# dropDuplicates(["event_id"]) without a watermark keeps every key forever
```

**`foreachBatch`** runs batch code per micro-batch, e.g. a `MERGE` into Delta or writes to several sinks. It is at-least-once, so make it **idempotent** (MERGE is), and deduplicate inside the batch first.

## Streaming in declarative pipelines

```sql
-- Bronze from Kafka: decode the value, keep metadata
CREATE OR REFRESH STREAMING TABLE events_bronze AS
SELECT CAST(value AS STRING) AS payload, timestamp AS kafka_ts, topic, partition, offset
FROM STREAM read_kafka(bootstrapServers => '${kafka_servers}', subscribe => 'events');

-- Silver: parse, validate, deduplicate
CREATE OR REFRESH STREAMING TABLE events_silver (
  CONSTRAINT valid_device EXPECT (device_id IS NOT NULL) ON VIOLATION DROP ROW
) AS SELECT payload:device_id::string AS device_id, payload:temp::double AS temp,
            payload:ts::timestamp AS event_ts
FROM STREAM(events_bronze);
```

- Pipelines manage **checkpoints, retries and the schema location** for you.
- **Append flows**: several sources (e.g. one Kafka topic per region) write into one streaming table, and backfills run as a separate `ONCE` flow.
- **Quarantine**: route rows that fail a rule into a separate table instead of dropping them silently.
- Use a **materialized view** for aggregates that must reflect late updates. A streaming table processes each row only once.
- A streaming read **fails** if its source table gets UPDATEs or DELETEs. Use `skipChangeCommits` or read the **change data feed**.

## CDC and slowly changing dimensions

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

dp.create_streaming_table("customers_silver")

dp.create_auto_cdc_flow(
    target="customers_silver",
    source="customers_cdc_clean",
    keys=["customer_id"],
    sequence_by=col("event_ts"),                 # out-of-order events resolved by this column
    apply_as_deletes=expr("op = 'DELETE'"),
    except_column_list=["op", "event_ts"],
    stored_as_scd_type=2,                        # 1 = overwrite, 2 = keep history
    track_history_column_list=["address", "segment"],
)
```

| | SCD Type 1 | SCD Type 2 |
|---|---|---|
| Keeps | Latest row per key | Every version, with `__START_AT` / `__END_AT` |
| Current rows | All | `WHERE __END_AT IS NULL` |
| Typical question | "What is the customer's address?" | "Which segment was the customer in **when** they ordered?" |

- **AUTO CDC FROM SNAPSHOT:** for sources that only deliver full snapshots.
- **Change data feed (CDF)** (`delta.enableChangeDataFeed = true`) exposes row-level inserts, updates and deletes of a Delta table, so downstream processing touches only what changed.

## Try it: incremental, exactly-once, stateful

> Streams the synthetic **retail** orders with `availableNow` (the trigger Serverless supports). Run the processing twice to see the checkpoint at work.

```python run
# ✏️ FILL IN: the catalog.schema you can write to
WORK_SCHEMA = "FILL_IN"   # e.g. "dbacademy.labuser1234567_1700000000"
```

```python run
# Load the retail tables from GitHub and create a volume for checkpoints
import urllib.request as u; exec(u.urlopen("https://raw.githubusercontent.com/misrori/dbxhelp/main/dbxdata.py").read().decode())
S = load_dataset("retail", WORK_SCHEMA)
CHK = files_volume(WORK_SCHEMA) + "/checkpoints"
```

```python run
# Incremental bronze → silver stream. The checkpoint remembers what was processed
from pyspark.sql import functions as F

def rows_in(q):
    return sum(p["numInputRows"] if isinstance(p, dict) else p.numInputRows for p in q.recentProgress)

def run_silver():
    q = (spark.readStream.table("fact_orders")
         .filter("status <> 'TEST'")
         .withColumn("processed_at", F.current_timestamp())
         .writeStream
         .option("checkpointLocation", f"{CHK}/orders_silver")
         .trigger(availableNow=True)
         .toTable("orders_silver_stream"))
    q.awaitTermination()
    return rows_in(q)

print("1st run, rows processed:", run_silver())
print("2nd run, rows processed:", run_silver(), "← nothing new, nothing reprocessed")
```

```python run
# New data arrives: only the new rows are processed
spark.sql("""
INSERT INTO fact_orders
SELECT order_id + 1000000, current_date(), customer_id, product_id, quantity, discount, 'COMPLETED'
FROM fact_orders LIMIT 100
""")
print("3rd run, rows processed:", run_silver())
display(spark.sql("SELECT count(*) AS silver_rows FROM orders_silver_stream"))
```

```python run
# Stateful: weekly revenue per category (stream-static join + windowed aggregation)
weekly = (spark.readStream.table("fact_orders")
    .join(spark.read.table("dim_product"), "product_id")                  # stream-static join
    .withColumn("order_ts", F.col("order_date").cast("timestamp"))
    .groupBy(F.window("order_ts", "7 days"), "category")
    .agg(F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("revenue"), F.count("*").alias("orders")))

q = (weekly.writeStream
     .outputMode("complete")                       # small aggregate: rewrite the full result each trigger
     .option("checkpointLocation", f"{CHK}/weekly")
     .trigger(availableNow=True)
     .toTable("orders_weekly_stream"))
q.awaitTermination()
display(spark.sql("SELECT window.start, category, revenue, orders FROM orders_weekly_stream ORDER BY window.start DESC, revenue DESC LIMIT 15"))
```

> In production, a large aggregate would use **append** mode plus `withWatermark(...)`, so finished windows are written once and their state is dropped.

## Decision cheat sheet

| Situation | Choose |
|---|---|
| Hourly SLA, files land continuously | `availableNow` trigger / **triggered** pipeline + schedule |
| Seconds of latency on Kafka events | Continuous micro-batches or **real-time mode** |
| Per-window aggregates written to Delta once | **Append** mode + **watermark** |
| Upsert each micro-batch into a table | **`foreachBatch` + `MERGE`** (idempotent) |
| Duplicate events within an hour | `dropDuplicatesWithinWatermark` |
| Change feed with out-of-order updates and deletes | **AUTO CDC** + `SEQUENCE BY` |
| History of dimension changes | **SCD Type 2** |
| Propagate updates and deletes from silver to gold | **Change data feed** |

## Common mistakes

- ❌ An append-mode aggregation without a watermark. This is not allowed, and state is unbounded.
- ❌ Complete mode on a large keyspace. It keeps all state forever.
- ❌ Deleting or sharing a checkpoint to "fix" a stream. This causes duplicates or gaps.
- ❌ A stream-stream join without watermarks and a time condition.
- ❌ Streaming from a table that receives UPDATEs or DELETEs without CDF or `skipChangeCommits`.
- ❌ `trigger(once=True)` in new code. Use **`availableNow`**.

**Next course:** *Databricks Data Privacy*, which protects the data these pipelines produce.

<!-- quiz:start -->
## Check yourself

**1. A streaming aggregation per 5-minute window must be written to a Delta table in append mode. What else is required?**

- a) Complete output mode
- b) A watermark on the event-time column
- c) A Spark cache on the source
- d) trigger(once=True)

<details><summary>Answer</summary>

**b)** Append mode only emits final results. With an aggregation, Spark needs a watermark to know when a window is complete and its state can be dropped.

</details>

**2. A job should process all newly arrived data every hour and then stop, to keep costs low. Which trigger fits?**

- a) processingTime='1 hour'
- b) availableNow=True, scheduled hourly by a job
- c) Continuous trigger
- d) Real-time mode

<details><summary>Answer</summary>

**b)** availableNow processes everything available (possibly in several batches) and then stops. A scheduled job turns it into an incremental batch. processingTime would keep the cluster running.

</details>

**3. Which statement about streaming checkpoints is TRUE?**

- a) Two queries writing to different tables can share one checkpoint
- b) Deleting the checkpoint is a safe way to recover from failures
- c) Each query needs its own checkpoint, which stores offsets and state
- d) Checkpoints are only needed for stateless queries

<details><summary>Answer</summary>

**c)** The checkpoint holds source offsets, committed batches and state, giving exactly-once processing. Never share or delete it.

</details>

**4. A stream of orders is joined to a customer dimension that changes, and gold results must always reflect the current customer data. What is the best choice?**

- a) A stream-static join into a streaming table
- b) A materialized view that joins orders and customers
- c) A stream-stream join without watermarks
- d) Complete output mode

<details><summary>Answer</summary>

**b)** A stream-static join uses the dimension as of processing time and never revisits old rows. A materialized view recomputes correctly when either side changes.

</details>

**5. Duplicate events with the same event_id can arrive up to 30 minutes apart. How do you deduplicate with bounded state?**

- a) `dropDuplicates(['event_id'])`
- b) `withWatermark('event_ts', '30 minutes').dropDuplicatesWithinWatermark(['event_id'])`
- c) `distinct()` in complete mode
- d) A MERGE in foreachBatch without dedup

<details><summary>Answer</summary>

**b)** dropDuplicatesWithinWatermark keeps keys only as long as the watermark requires. dropDuplicates without a watermark stores every key forever.

</details>

<!-- quiz:end -->
