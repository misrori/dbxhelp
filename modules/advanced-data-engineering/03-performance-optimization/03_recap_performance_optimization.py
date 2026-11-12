# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Advanced Data Engineering with Databricks</span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">Recap 3: Databricks Performance Optimization</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Recap: Databricks Performance Optimization
# MAGIC
# MAGIC > **Course 3 of 4 · Advanced Data Engineering with Databricks** · ⏱ ~9 minutes
# MAGIC >
# MAGIC > Why queries are slow and what to do about it: **file layout and data skipping**, the classic Spark problems (**skew, shuffle, spill, serialization**), and choosing **compute**.
# MAGIC
# MAGIC **The one idea to remember:** first **read less data** (good layout, data skipping), then **move less data** (avoid shuffles, broadcast small sides), and only then **add hardware**. The query profile and the Spark UI tell you which problem you have.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we covered
# MAGIC
# MAGIC | Topic | Key takeaway |
# MAGIC |---|---|
# MAGIC | Spark architecture | Driver, executors, **jobs → stages → tasks**. Shuffles create stage boundaries |
# MAGIC | File layout | Small files, **data skipping** with file statistics, partitioning vs Z-order vs **liquid clustering** |
# MAGIC | Managed maintenance | `OPTIMIZE`, `VACUUM`, optimized writes, auto compaction, **predictive optimization** |
# MAGIC | Skew | One task takes much longer than the rest. Fix it with AQE, salting, or filtering hot keys |
# MAGIC | Shuffle | Wide transformations move data across the network. Broadcast joins avoid it |
# MAGIC | Spill | Not enough memory per task, so data goes to disk. Fix it with more or smaller partitions or bigger nodes |
# MAGIC | Serialization | Python UDFs are slow. Prefer **built-in functions**, then pandas UDFs |
# MAGIC | Compute & diagnostics | Cluster sizing, **Photon**, serverless, **query profile**, **Spark UI**, caching |

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Spark executes a query
# MAGIC
# MAGIC ```
# MAGIC Query ─► Job ─► Stage 1 (scan, filter, project)  ─┐ shuffle (exchange)
# MAGIC                                                   ├► Stage 2 (join / aggregate) ─► write
# MAGIC                                                   │
# MAGIC          one task per partition, run in parallel on executor cores
# MAGIC ```
# MAGIC
# MAGIC - **Narrow** transformations (`filter`, `select`, `withColumn`) stay in the same stage.
# MAGIC - **Wide** transformations (`groupBy`, `join`, `distinct`, `orderBy`) need a **shuffle**, which starts a new stage.
# MAGIC - **Adaptive Query Execution (AQE)**, on by default, re-plans at stage boundaries using real data sizes:
# MAGIC   - switches to a **broadcast join**;
# MAGIC   - **coalesces** small partitions;
# MAGIC   - splits **skewed** partitions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## File layout and data skipping
# MAGIC
# MAGIC Delta stores **min/max statistics** per file, so a selective filter can **skip files**. That only works when similar values sit in the same files.
# MAGIC
# MAGIC | Technique | Use when | Notes |
# MAGIC |---|---|---|
# MAGIC | **Liquid clustering** `CLUSTER BY (cols)` | **Default for new tables**. Filter or join on 1–4 columns, any cardinality | Incremental, and keys can be changed without a rewrite |
# MAGIC | **`CLUSTER BY AUTO`** | Managed tables with unknown or changing query patterns | Databricks picks the keys (needs predictive optimization) |
# MAGIC | **Partitioning** | Very large tables (many TB) with a **low-cardinality** column used for lifecycle (e.g. date), partitions ≥ ~1 GB | Over-partitioning leads to millions of small files |
# MAGIC | **Z-order** (legacy) | Existing partitioned tables | Not incremental. Must be re-run on all data |
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE sales (sale_ts TIMESTAMP, customer_id BIGINT, store_id INT, amount DOUBLE)
# MAGIC CLUSTER BY (customer_id, sale_ts);
# MAGIC
# MAGIC OPTIMIZE sales;                     -- compacts small files and clusters new data
# MAGIC ANALYZE TABLE sales COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ```
# MAGIC
# MAGIC - **Small-file problem:** many tiny files from streaming or over-partitioning mean slow listing and slow scans. Fix it with `OPTIMIZE`, auto compaction and optimized writes.
# MAGIC - **Predictive optimization** on UC managed tables runs `OPTIMIZE`, `VACUUM` and `ANALYZE` for you.
# MAGIC - Write predicates on the column itself (`sale_ts >= '2026-01-01'`), not on a function of it (`date(sale_ts) = …`), so that skipping works.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The four classic problems
# MAGIC
# MAGIC | Problem | Symptom in Spark UI / query profile | Fixes |
# MAGIC |---|---|---|
# MAGIC | **Skew** | One task runs far longer than the median. Rows per task are very uneven | AQE skew join (on by default), **salting** hot keys, filtering or handling NULL / "unknown" keys separately |
# MAGIC | **Shuffle** | Large *shuffle read/write* bytes, many exchanges | **Broadcast** the small side, filter and select early, pre-aggregate, cluster on join keys |
# MAGIC | **Spill** | *Spill (memory/disk)* > 0 in sort, aggregate or join stages | More partitions (smaller tasks), memory-optimized nodes, less data per key |
# MAGIC | **Serialization** | Slow Python UDF stages, Photon falls back | Replace UDFs with **built-in functions**. Otherwise use **pandas (vectorized) UDFs** |
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC # ❌ row-by-row Python UDF: data shipped to Python one row at a time
# MAGIC # clean = F.udf(lambda s: s.strip().lower())
# MAGIC
# MAGIC # ✅ built-in, runs in the JVM / Photon
# MAGIC df = df.withColumn("email", F.lower(F.trim("email")))
# MAGIC
# MAGIC # ✅ broadcast a small dimension to avoid shuffling the big fact table
# MAGIC result = facts.join(F.broadcast(stores), "store_id")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joins and shuffle partitions
# MAGIC
# MAGIC | Join strategy | When | Cost |
# MAGIC |---|---|---|
# MAGIC | **Broadcast hash join** | One side is small (under `spark.sql.autoBroadcastJoinThreshold`, or AQE decides at runtime) | **No shuffle** of the big side, the fastest option |
# MAGIC | Shuffle hash join | Medium side fits in memory per partition | One shuffle |
# MAGIC | **Sort-merge join** | Both sides large | Shuffle **and** sort on both sides |
# MAGIC
# MAGIC - `spark.sql.shuffle.partitions` (default 200): with AQE, set it high (or `auto`) and let coalescing shrink it.
# MAGIC - Too few partitions give huge tasks and spill. Too many give scheduling overhead and tiny output files.
# MAGIC - Missing statistics can hide a broadcast opportunity. Run `ANALYZE TABLE`, or add a `/*+ BROADCAST(dim) */` hint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute choices
# MAGIC
# MAGIC | Choice | Guidance |
# MAGIC |---|---|
# MAGIC | **Serverless** | Default for jobs, pipelines and SQL: fast start, auto-scaling, no tuning |
# MAGIC | **Photon** | Vectorized engine for SQL and DataFrame workloads. Big gains on scans, joins, aggregations and Delta writes |
# MAGIC | Node type | Memory-optimized for large shuffles, joins and caching. Compute-optimized for CPU-heavy transforms. Storage-optimized (local SSD) for the **disk cache** |
# MAGIC | Autoscaling | Good for variable load. Fixed size for steady streaming workloads |
# MAGIC | Job clusters | Production jobs. All-purpose clusters are for interactive work |
# MAGIC
# MAGIC **Caching:**
# MAGIC - **Disk cache**: automatic on SSD-backed nodes and SQL warehouses, and always consistent.
# MAGIC - **Spark cache** (`df.cache()`): manual, uses executor memory, and can go stale.
# MAGIC - **Query result cache**: repeated identical SQL queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnosing a slow query
# MAGIC
# MAGIC 1. Open the **query profile** (SQL / serverless) or the **Spark UI** (classic).
# MAGIC 2. **Files pruned ≈ 0** on a selective filter? → layout problem: clustering, statistics, predicate.
# MAGIC 3. **Sort-merge join** with a small side? → broadcast / statistics.
# MAGIC 4. **Shuffle bytes** huge? → filter early, broadcast, pre-aggregate.
# MAGIC 5. **Max task time ≫ median**? → skew.
# MAGIC 6. **Spill** > 0? → partitions or memory.
# MAGIC 7. Only then: bigger compute, or Photon if it isn't on yet.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common mistakes
# MAGIC
# MAGIC - ❌ Scaling up the cluster when the profile shows **no data skipping**. Fix the layout first.
# MAGIC - ❌ Partitioning a 200 GB table, or partitioning by a high-cardinality column. Use **liquid clustering**.
# MAGIC - ❌ Scheduling `ZORDER` jobs on managed tables with liquid clustering and predictive optimization.
# MAGIC - ❌ Row-by-row Python UDFs for logic that built-in functions already cover.
# MAGIC - ❌ Forcing `spark.sql.shuffle.partitions = 2000` everywhere instead of letting AQE coalesce.
# MAGIC - ❌ `df.cache()` for a dashboard read by many users. Rely on the disk cache or result cache.
# MAGIC
# MAGIC **Next course:** *Automated Deployment with Databricks Asset Bundles*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check yourself
# MAGIC
# MAGIC Answer the questions below to check your understanding.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div id="dbxquiz" style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 760px"></div>
# MAGIC <script>
# MAGIC function dbxQuiz(root, questions) {
# MAGIC   if (typeof root === 'string') root = document.getElementById(root);
# MAGIC   var i = 0, score = 0, picked = null, shown = false;
# MAGIC   function esc(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/`([^`]+)`/g, '<code>$1</code>'); }
# MAGIC   function btn(label, primary) { return '<button style="margin-top:14px;padding:8px 18px;border-radius:4px;border:none;cursor:pointer;background:' + (primary ? '#2272B4' : '#E0E0E0') + ';color:' + (primary ? '#fff' : '#333') + ';font-size:0.95em">' + label + '</button>'; }
# MAGIC   function render() {
# MAGIC     if (i >= questions.length) {
# MAGIC       root.innerHTML = '<div style="padding:20px;border:1px solid #E0E0E0;border-radius:8px;text-align:center;background:#fff"><div style="font-size:1.3em;font-weight:bold;color:#333">Score: ' + score + ' / ' + questions.length + '</div>' + btn('Retake', true) + '</div>';
# MAGIC       root.querySelector('button').onclick = function () { i = 0; score = 0; picked = null; shown = false; render(); };
# MAGIC       return;
# MAGIC     }
# MAGIC     var Q = questions[i], h = '<div style="padding:20px;border:1px solid #E0E0E0;border-radius:8px;background:#fff">';
# MAGIC     h += '<div style="color:#999;font-size:0.85em;margin-bottom:6px">Question ' + (i + 1) + ' of ' + questions.length + '</div>';
# MAGIC     h += '<div style="font-weight:bold;color:#333;margin-bottom:12px;line-height:1.5">' + esc(Q.q) + '</div>';
# MAGIC     Q.o.forEach(function (t, k) {
# MAGIC       var bg = '#fff', bd = '#e0e0e0';
# MAGIC       if (shown && k === Q.c) { bg = '#E8F5E9'; bd = '#43A047'; }
# MAGIC       else if (shown && k === picked) { bg = '#FFEBEE'; bd = '#E53935'; }
# MAGIC       else if (!shown && k === picked) { bg = '#E3F2FD'; bd = '#2272B4'; }
# MAGIC       h += '<div data-k="' + k + '" style="padding:10px 14px;margin:6px 0;border:2px solid ' + bd + ';background:' + bg + ';border-radius:6px;cursor:' + (shown ? 'default' : 'pointer') + ';color:#333">' + String.fromCharCode(97 + k) + ') ' + esc(t) + '</div>';
# MAGIC     });
# MAGIC     if (shown) {
# MAGIC       h += '<div style="margin-top:12px;padding:12px;background:#F5F5F5;border-radius:6px;color:#333"><strong>' + (picked === Q.c ? '&#9989; Correct.' : '&#10060; Not quite.') + '</strong> ' + esc(Q.e) + '</div>' + btn(i + 1 < questions.length ? 'Next' : 'See score', true);
# MAGIC     } else {
# MAGIC       h += btn('Submit', picked !== null);
# MAGIC     }
# MAGIC     root.innerHTML = h + '</div>';
# MAGIC     if (!shown) root.querySelectorAll('[data-k]').forEach(function (el) { el.onclick = function () { picked = +el.getAttribute('data-k'); render(); }; });
# MAGIC     root.querySelector('button').onclick = function () {
# MAGIC       if (!shown) { if (picked === null) return; if (picked === Q.c) score++; shown = true; }
# MAGIC       else { i++; picked = null; shown = false; }
# MAGIC       render();
# MAGIC     };
# MAGIC   }
# MAGIC   render();
# MAGIC }
# MAGIC dbxQuiz('dbxquiz', [{"q": "A query filters a 5 TB table on customer_id, and the query profile shows almost no files pruned. What is the best first step?", "o": ["Double the cluster size", "Partition the table by customer_id", "Use liquid clustering on customer_id (then OPTIMIZE)", "Cache the table with df.cache()"], "c": 2, "e": "No pruning means a layout problem. Liquid clustering co-locates customer_id values so file statistics can skip files. Partitioning by a high-cardinality column creates millions of tiny files."}, {"q": "In the Spark UI, one task of a stage runs for 25 minutes while the median task takes 20 seconds. What is the most likely cause?", "o": ["Data skew on the join or grouping key", "Too many executors", "Missing Photon", "Disk cache disabled"], "c": 0, "e": "A single task far slower than the median is the classic sign of skew: one key holds much more data. AQE skew handling, salting, or treating hot keys separately helps."}, {"q": "A 2 TB fact table is joined to a 20 MB store dimension, and the plan shows a sort-merge join with a large shuffle. What do you do?", "o": ["Increase spark.sql.shuffle.partitions to 5000", "Broadcast the small dimension (fresh statistics or a BROADCAST hint)", "Convert the dimension to CSV", "Repartition the fact table by store_id before every query"], "c": 1, "e": "A broadcast hash join ships the small table to every executor, so the large side never needs to be shuffled."}, {"q": "A transformation trims and lower-cases emails using a Python UDF and is slow. What is the best fix?", "o": ["Rewrite it with built-in functions: lower(trim(email))", "Increase driver memory", "Cache the DataFrame before the UDF", "Run it on a single-node cluster"], "c": 0, "e": "Python UDFs serialize rows between the JVM and Python and block Photon. Built-in functions run natively and are much faster."}, {"q": "Stage metrics show large 'spill (disk)' values during an aggregation. Which change addresses spill directly?", "o": ["Fewer, larger shuffle partitions", "More (smaller) partitions or more memory per task", "Disable AQE", "Add a ZORDER on the aggregation key"], "c": 1, "e": "Spill happens when a task's data doesn't fit in memory. Smaller tasks (more partitions) or memory-optimized nodes reduce it."}]);
# MAGIC </script>
