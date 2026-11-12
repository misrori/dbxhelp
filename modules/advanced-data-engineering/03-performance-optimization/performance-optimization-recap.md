# Recap: Databricks Performance Optimization

> **Course 3 of 4 · Advanced Data Engineering with Databricks** · ⏱ ~9 minutes
>
> Why queries are slow and what to do about it: **file layout and data skipping**, the classic Spark problems (**skew, shuffle, spill, serialization**), and choosing **compute**.

**The one idea to remember:** first **read less data** (good layout, data skipping), then **move less data** (avoid shuffles, broadcast small sides), and only then **add hardware**. The query profile and the Spark UI tell you which problem you have.

## What we covered

| Topic | Key takeaway |
|---|---|
| Spark architecture | Driver, executors, **jobs → stages → tasks**. Shuffles create stage boundaries |
| File layout | Small files, **data skipping** with file statistics, partitioning vs Z-order vs **liquid clustering** |
| Managed maintenance | `OPTIMIZE`, `VACUUM`, optimized writes, auto compaction, **predictive optimization** |
| Skew | One task takes much longer than the rest. Fix it with AQE, salting, or filtering hot keys |
| Shuffle | Wide transformations move data across the network. Broadcast joins avoid it |
| Spill | Not enough memory per task, so data goes to disk. Fix it with more or smaller partitions or bigger nodes |
| Serialization | Python UDFs are slow. Prefer **built-in functions**, then pandas UDFs |
| Compute & diagnostics | Cluster sizing, **Photon**, serverless, **query profile**, **Spark UI**, caching |

## How Spark executes a query

```
Query ─► Job ─► Stage 1 (scan, filter, project)  ─┐ shuffle (exchange)
                                                  ├► Stage 2 (join / aggregate) ─► write
                                                  │
         one task per partition, run in parallel on executor cores
```

- **Narrow** transformations (`filter`, `select`, `withColumn`) stay in the same stage.
- **Wide** transformations (`groupBy`, `join`, `distinct`, `orderBy`) need a **shuffle**, which starts a new stage.
- **Adaptive Query Execution (AQE)**, on by default, re-plans at stage boundaries using real data sizes:
  - switches to a **broadcast join**;
  - **coalesces** small partitions;
  - splits **skewed** partitions.

## File layout and data skipping

Delta stores **min/max statistics** per file, so a selective filter can **skip files**. That only works when similar values sit in the same files.

| Technique | Use when | Notes |
|---|---|---|
| **Liquid clustering** `CLUSTER BY (cols)` | **Default for new tables**. Filter or join on 1–4 columns, any cardinality | Incremental, and keys can be changed without a rewrite |
| **`CLUSTER BY AUTO`** | Managed tables with unknown or changing query patterns | Databricks picks the keys (needs predictive optimization) |
| **Partitioning** | Very large tables (many TB) with a **low-cardinality** column used for lifecycle (e.g. date), partitions ≥ ~1 GB | Over-partitioning leads to millions of small files |
| **Z-order** (legacy) | Existing partitioned tables | Not incremental. Must be re-run on all data |

```sql
CREATE TABLE sales (sale_ts TIMESTAMP, customer_id BIGINT, store_id INT, amount DOUBLE)
CLUSTER BY (customer_id, sale_ts);

OPTIMIZE sales;                     -- compacts small files and clusters new data
ANALYZE TABLE sales COMPUTE STATISTICS FOR ALL COLUMNS;
```

- **Small-file problem:** many tiny files from streaming or over-partitioning mean slow listing and slow scans. Fix it with `OPTIMIZE`, auto compaction and optimized writes.
- **Predictive optimization** on UC managed tables runs `OPTIMIZE`, `VACUUM` and `ANALYZE` for you.
- Write predicates on the column itself (`sale_ts >= '2026-01-01'`), not on a function of it (`date(sale_ts) = …`), so that skipping works.

## The four classic problems

| Problem | Symptom in Spark UI / query profile | Fixes |
|---|---|---|
| **Skew** | One task runs far longer than the median. Rows per task are very uneven | AQE skew join (on by default), **salting** hot keys, filtering or handling NULL / "unknown" keys separately |
| **Shuffle** | Large *shuffle read/write* bytes, many exchanges | **Broadcast** the small side, filter and select early, pre-aggregate, cluster on join keys |
| **Spill** | *Spill (memory/disk)* > 0 in sort, aggregate or join stages | More partitions (smaller tasks), memory-optimized nodes, less data per key |
| **Serialization** | Slow Python UDF stages, Photon falls back | Replace UDFs with **built-in functions**. Otherwise use **pandas (vectorized) UDFs** |

```python
from pyspark.sql import functions as F

# ❌ row-by-row Python UDF: data shipped to Python one row at a time
# clean = F.udf(lambda s: s.strip().lower())

# ✅ built-in, runs in the JVM / Photon
df = df.withColumn("email", F.lower(F.trim("email")))

# ✅ broadcast a small dimension to avoid shuffling the big fact table
result = facts.join(F.broadcast(stores), "store_id")
```

## Joins and shuffle partitions

| Join strategy | When | Cost |
|---|---|---|
| **Broadcast hash join** | One side is small (under `spark.sql.autoBroadcastJoinThreshold`, or AQE decides at runtime) | **No shuffle** of the big side, the fastest option |
| Shuffle hash join | Medium side fits in memory per partition | One shuffle |
| **Sort-merge join** | Both sides large | Shuffle **and** sort on both sides |

- `spark.sql.shuffle.partitions` (default 200): with AQE, set it high (or `auto`) and let coalescing shrink it.
- Too few partitions give huge tasks and spill. Too many give scheduling overhead and tiny output files.
- Missing statistics can hide a broadcast opportunity. Run `ANALYZE TABLE`, or add a `/*+ BROADCAST(dim) */` hint.

## Compute choices

| Choice | Guidance |
|---|---|
| **Serverless** | Default for jobs, pipelines and SQL: fast start, auto-scaling, no tuning |
| **Photon** | Vectorized engine for SQL and DataFrame workloads. Big gains on scans, joins, aggregations and Delta writes |
| Node type | Memory-optimized for large shuffles, joins and caching. Compute-optimized for CPU-heavy transforms. Storage-optimized (local SSD) for the **disk cache** |
| Autoscaling | Good for variable load. Fixed size for steady streaming workloads |
| Job clusters | Production jobs. All-purpose clusters are for interactive work |

**Caching:**
- **Disk cache**: automatic on SSD-backed nodes and SQL warehouses, and always consistent.
- **Spark cache** (`df.cache()`): manual, uses executor memory, and can go stale.
- **Query result cache**: repeated identical SQL queries.

## Diagnosing a slow query

1. Open the **query profile** (SQL / serverless) or the **Spark UI** (classic).
2. **Files pruned ≈ 0** on a selective filter? → layout problem: clustering, statistics, predicate.
3. **Sort-merge join** with a small side? → broadcast / statistics.
4. **Shuffle bytes** huge? → filter early, broadcast, pre-aggregate.
5. **Max task time ≫ median**? → skew.
6. **Spill** > 0? → partitions or memory.
7. Only then: bigger compute, or Photon if it isn't on yet.

## Try it: layout, join strategy, serialization

> Uses the synthetic **retail** tables, plus 5 million generated rows for the UDF comparison.

```python run
# ✏️ FILL IN: the catalog.schema you can write to
WORK_SCHEMA = "FILL_IN"   # e.g. "dbacademy.labuser1234567_1700000000"
```

```python run
# Load the retail tables from GitHub
import urllib.request as u; exec(u.urlopen("https://raw.githubusercontent.com/misrori/dbxhelp/main/dbxdata.py").read().decode())
S = load_dataset("retail", WORK_SCHEMA)
```

```python run
# Liquid clustering: create a clustered copy and inspect its layout
spark.sql("CREATE OR REPLACE TABLE orders_clustered CLUSTER BY (customer_id, order_date) AS SELECT * FROM fact_orders")
spark.sql("OPTIMIZE orders_clustered")
display(spark.sql("DESCRIBE DETAIL orders_clustered").select("clusteringColumns", "numFiles", "sizeInBytes"))
```

```python run
# Join strategy: look for BroadcastHashJoin. The small dimension is broadcast, so the fact table isn't shuffled
spark.sql("""
SELECT p.category, sum(f.quantity * p.unit_price) AS revenue
FROM fact_orders f JOIN dim_product p ON f.product_id = p.product_id
GROUP BY ALL
""").explain()
```

```python run
# Serialization: a row-by-row Python UDF vs built-in functions on 5 million rows
import time
from pyspark.sql import functions as F

df = spark.range(5_000_000).withColumn("email", F.concat(F.lit("  User"), F.col("id").cast("string"), F.lit("@Example.COM ")))
clean_udf = F.udf(lambda s: s.strip().lower())

for name, col in [("Python UDF", clean_udf("email")), ("built-in", F.lower(F.trim("email")))]:
    start = time.time()
    df.select(F.sum(F.length(col))).collect()
    print(f"{name:<11} {time.time() - start:5.1f} s")
```

## Common mistakes

- ❌ Scaling up the cluster when the profile shows **no data skipping**. Fix the layout first.
- ❌ Partitioning a 200 GB table, or partitioning by a high-cardinality column. Use **liquid clustering**.
- ❌ Scheduling `ZORDER` jobs on managed tables with liquid clustering and predictive optimization.
- ❌ Row-by-row Python UDFs for logic that built-in functions already cover.
- ❌ Forcing `spark.sql.shuffle.partitions = 2000` everywhere instead of letting AQE coalesce.
- ❌ `df.cache()` for a dashboard read by many users. Rely on the disk cache or result cache.

**Next course:** *Automated Deployment with Databricks Asset Bundles*.

<!-- quiz:start -->
## Check yourself

**1. A query filters a 5 TB table on customer_id, and the query profile shows almost no files pruned. What is the best first step?**

- a) Double the cluster size
- b) Partition the table by customer_id
- c) Use liquid clustering on customer_id (then OPTIMIZE)
- d) Cache the table with df.cache()

<details><summary>Answer</summary>

**c)** No pruning means a layout problem. Liquid clustering co-locates customer_id values so file statistics can skip files. Partitioning by a high-cardinality column creates millions of tiny files.

</details>

**2. In the Spark UI, one task of a stage runs for 25 minutes while the median task takes 20 seconds. What is the most likely cause?**

- a) Data skew on the join or grouping key
- b) Too many executors
- c) Missing Photon
- d) Disk cache disabled

<details><summary>Answer</summary>

**a)** A single task far slower than the median is the classic sign of skew: one key holds much more data. AQE skew handling, salting, or treating hot keys separately helps.

</details>

**3. A 2 TB fact table is joined to a 20 MB store dimension, and the plan shows a sort-merge join with a large shuffle. What do you do?**

- a) Increase spark.sql.shuffle.partitions to 5000
- b) Broadcast the small dimension (fresh statistics or a BROADCAST hint)
- c) Convert the dimension to CSV
- d) Repartition the fact table by store_id before every query

<details><summary>Answer</summary>

**b)** A broadcast hash join ships the small table to every executor, so the large side never needs to be shuffled.

</details>

**4. A transformation trims and lower-cases emails using a Python UDF and is slow. What is the best fix?**

- a) Rewrite it with built-in functions: lower(trim(email))
- b) Increase driver memory
- c) Cache the DataFrame before the UDF
- d) Run it on a single-node cluster

<details><summary>Answer</summary>

**a)** Python UDFs serialize rows between the JVM and Python and block Photon. Built-in functions run natively and are much faster.

</details>

**5. Stage metrics show large 'spill (disk)' values during an aggregation. Which change addresses spill directly?**

- a) Fewer, larger shuffle partitions
- b) More (smaller) partitions or more memory per task
- c) Disable AQE
- d) Add a ZORDER on the aggregation key

<details><summary>Answer</summary>

**b)** Spill happens when a task's data doesn't fit in memory. Smaller tasks (more partitions) or memory-optimized nodes reduce it.

</details>

<!-- quiz:end -->
