# Recap: Build Data Pipelines with Lakeflow Spark Declarative Pipelines

> **Course 3 of 4 · Data Engineering with Databricks** · ⏱ ~9 minutes
>
> Building a **bronze → silver → gold** pipeline *declaratively*: you describe the tables, and the pipeline handles the order, incremental processing, checkpoints, retries and data quality. (Formerly *Delta Live Tables / DLT*.)

**The one idea to remember:** use **streaming tables** for ingestion and row-level transforms, and **materialized views** for aggregates and joins that must stay correct. Add **expectations** to every layer you care about.

## What we covered

| Topic | Key takeaway |
|---|---|
| Why declarative pipelines | Define *what* the tables are, not *how* to run them. Dependencies are resolved automatically |
| Dataset types | **Streaming tables**, **materialized views**, temporary views, and **flows** |
| Developing a pipeline | **Lakeflow Pipelines Editor**, pipeline settings, dry run, and the pipeline graph |
| Medallion with Auto Loader | `STREAM read_files()` into bronze, cleaned silver, aggregated gold |
| Data quality | **Expectations**: warn, drop or fail |
| Running in production | Triggered vs continuous, development vs production mode, scheduling with a job |
| Monitoring | Pipeline graph, data quality metrics, and the **event log** |
| Change data capture | **AUTO CDC INTO** (formerly APPLY CHANGES) for **SCD Type 1 and 2** |

## Dataset types

| Object | Semantics | Typical layer |
|---|---|---|
| **Streaming table (ST)** | Incremental and append-oriented: each source row is processed **once** | Bronze ingestion, silver cleansing, CDC targets |
| **Materialized view (MV)** | Query result that is kept **correct** on every refresh. It is refreshed incrementally when possible and reflects updates and deletes | Gold aggregates, joins with changing dimensions |
| **Temporary view** | Not persisted, visible only inside the pipeline | Intermediate logic |
| **Flow** | A query that writes into a target. Several **append flows** can feed one ST | Fan-in from many sources, backfills |

`STREAM(...)` / `spark.readStream` gives a **streaming** read, so the result is a streaming table. A plain `FROM` / `spark.read` gives a **batch** read, so the result is a materialized view.

## A medallion pipeline in SQL

```sql
-- Bronze: incremental file ingestion with Auto Loader
CREATE OR REFRESH STREAMING TABLE orders_bronze
COMMENT 'Raw orders'
AS SELECT *, _metadata.file_name AS source_file, current_timestamp() AS ingested_at
FROM STREAM read_files('/Volumes/shop/raw/orders/', format => 'json');

-- Silver: clean + data quality
CREATE OR REFRESH STREAMING TABLE orders_silver (
  CONSTRAINT valid_id     EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_amt EXPECT (amount > 0)           ON VIOLATION FAIL UPDATE,
  CONSTRAINT has_email    EXPECT (email IS NOT NULL)                      -- warn only
)
AS SELECT order_id, lower(email) AS email, CAST(amount AS DECIMAL(10,2)) AS amount,
          CAST(order_ts AS TIMESTAMP) AS order_ts
FROM STREAM(orders_bronze);

-- Gold: aggregate for BI
CREATE OR REFRESH MATERIALIZED VIEW daily_revenue
AS SELECT date(order_ts) AS order_date, sum(amount) AS revenue, count(*) AS orders
FROM orders_silver
GROUP BY ALL;
```

Tables reference each other by name, and the pipeline builds the **dependency graph** from those references. You never write "run bronze first".

## The same in Python

```python
from pyspark import pipelines as dp        # older code: import dlt
from pyspark.sql import functions as F

@dp.table(comment="Raw orders")
def orders_bronze():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("/Volumes/shop/raw/orders/"))

@dp.table
@dp.expect_or_drop("valid_id", "order_id IS NOT NULL")
@dp.expect_or_fail("positive_amt", "amount > 0")
def orders_silver():
    return spark.readStream.table("orders_bronze").withColumn("email", F.lower("email"))

@dp.materialized_view
def daily_revenue():
    return (spark.read.table("orders_silver")
            .groupBy(F.to_date("order_ts").alias("order_date"))
            .agg(F.sum("amount").alias("revenue")))
```

SQL and Python files can be mixed in the same pipeline.

## Data quality with expectations

| Expectation | Invalid rows | Pipeline |
|---|---|---|
| `EXPECT (...)` / `@dp.expect` (default: **warn**) | Kept | Continues. Violations are counted |
| `ON VIOLATION DROP ROW` / `@dp.expect_or_drop` | **Dropped** | Continues. Violations are counted |
| `ON VIOLATION FAIL UPDATE` / `@dp.expect_or_fail` | — | The update **fails**. Nothing is committed for that flow |

- Violation metrics show up in the pipeline UI and in the **event log**.
- `expect_all`, `expect_all_or_drop` and `expect_all_or_fail` take a dictionary of rules.
- **Quarantine pattern:** don't just drop bad rows. Write them to a separate table with the inverted rule, so you can inspect them.

## Developing and running a pipeline

| Setting | Options | Guidance |
|---|---|---|
| **Editor** | Lakeflow Pipelines Editor (multi-file, graph, data preview) | Use **dry run** to validate code and the graph without processing data |
| **Destination** | Default catalog and schema in Unity Catalog | Tables are published as UC objects |
| **Compute** | **Serverless** (recommended) or classic | Serverless gives fast start and incremental MV refresh |
| **Pipeline mode** | **Triggered**: process available data, then stop. **Continuous**: always running | Triggered with a schedule for hourly or daily SLAs, continuous for low latency |
| **Run type** | Development: reuses compute, no retries. Production: retries and restarts | Production for scheduled runs |
| **Refresh** | Normal update, refresh selected tables, **full refresh** | Full refresh recomputes from scratch. Careful with sources that no longer hold old data |
| **Configuration** | Key-value parameters (e.g. `source_path`) | Avoid hard-coded paths. Read them in code with `spark.conf.get("source_path")` |

Schedule the pipeline with its own schedule, or as a **pipeline task in a Lakeflow Job** (course 2).

## Monitoring: the event log

- **Pipeline graph:** status, row counts and timing per dataset.
- **Data quality tab:** expectation pass and fail counts per table.
- **Event log:** a Delta table with every update, flow progress, data quality metric and error.

```sql
SELECT timestamp, event_type, message
FROM event_log(TABLE(orders_silver))     -- or event_log('<pipeline-id>')
WHERE event_type IN ('flow_progress', 'update_progress')
ORDER BY timestamp DESC;
```

## Change data capture with AUTO CDC

```sql
CREATE OR REFRESH STREAMING TABLE customers_silver;

CREATE FLOW customers_cdc AS AUTO CDC INTO customers_silver
FROM STREAM(customers_cdc_bronze)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY event_ts                 -- decides which change wins (handles out-of-order data)
COLUMNS * EXCEPT (operation, event_ts)
STORED AS SCD TYPE 2;                -- or SCD TYPE 1
```

| | SCD Type 1 | SCD Type 2 |
|---|---|---|
| Target holds | Latest state, one row per key | One row **per version** |
| History | Overwritten | Kept in **`__START_AT` / `__END_AT`** |
| Current rows | All rows | `WHERE __END_AT IS NULL` |

AUTO CDC replaces hand-written `MERGE` logic for ordering, deduplication, deletes and SCD2 versioning. Use **AUTO CDC FROM SNAPSHOT** when the source only delivers full snapshots.

## Common mistakes

- ❌ A **streaming table** for a gold aggregate that must reflect late corrections. Use an **MV**.
- ❌ A **materialized view** for raw, high-volume append ingestion. Use an **ST**.
- ❌ `EXPECT` without an action, expecting it to remove rows. The default only **warns**.
- ❌ **Continuous** mode for a daily report. Use **triggered** mode with a schedule.
- ❌ A full refresh of a bronze ST whose source (e.g. Kafka) no longer holds old events. That data is lost.
- ❌ Hand-written MERGE for out-of-order CDC. **AUTO CDC + `SEQUENCE BY`** handles it.

**Next course:** *DevOps Essentials for Data Engineering*, which takes this code into a tested, versioned deployment.

<!-- quiz:start -->
## Check yourself

**1. A gold table aggregates revenue per day, and upstream orders can be corrected or deleted days later. Which dataset type keeps the result correct?**

- a) Streaming table
- b) Materialized view
- c) Temporary view
- d) Append flow

<details><summary>Answer</summary>

**b)** A materialized view is kept correct on every refresh, including updates and deletes in its sources. A streaming table processes each input row only once.

</details>

**2. Rows with a NULL order_id must be removed from the silver table, but the pipeline must keep running. Which expectation do you use?**

- a) `EXPECT (order_id IS NOT NULL)`
- b) `EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW`
- c) `EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE`
- d) A CHECK constraint on the table

<details><summary>Answer</summary>

**b)** DROP ROW removes invalid rows and records the count. Plain EXPECT only warns, and FAIL UPDATE stops the update.

</details>

**3. Which statement makes a streaming read in a SQL pipeline definition?**

- a) `FROM orders_bronze`
- b) `FROM STREAM(orders_bronze)`
- c) `FROM LIVE.orders_bronze`
- d) `FROM TABLE(orders_bronze)`

<details><summary>Answer</summary>

**b)** `STREAM(...)` (or `STREAM read_files(...)`) gives an incremental, streaming read, producing a streaming table. A plain FROM is a batch read.

</details>

**4. A change feed contains inserts, updates and deletes that may arrive out of order. The business needs the full history of customer addresses. What do you use?**

- a) MERGE INTO in a foreachBatch
- b) AUTO CDC with SEQUENCE BY ... STORED AS SCD TYPE 2
- c) AUTO CDC STORED AS SCD TYPE 1
- d) A materialized view with ROW_NUMBER()

<details><summary>Answer</summary>

**b)** AUTO CDC uses the sequence column to order changes correctly. SCD Type 2 keeps every version with __START_AT and __END_AT.

</details>

**5. A pipeline feeds a dashboard that is refreshed once per day. Which pipeline mode is the most cost-effective?**

- a) Continuous
- b) Triggered, scheduled daily
- c) Continuous with development mode
- d) Triggered, with a full refresh every run

<details><summary>Answer</summary>

**b)** Triggered mode processes the available data and then stops. Continuous mode keeps compute running all the time, and full refresh reprocesses everything.

</details>

<!-- quiz:end -->
