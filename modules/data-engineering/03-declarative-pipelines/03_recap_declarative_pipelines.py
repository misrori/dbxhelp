# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Data Engineering with Databricks</span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">Recap 3: Build Data Pipelines with Lakeflow Spark Declarative Pipelines</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Recap: Build Data Pipelines with Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC > **Course 3 of 4 · Data Engineering with Databricks** · ⏱ ~9 minutes
# MAGIC >
# MAGIC > Building a **bronze → silver → gold** pipeline *declaratively*: you describe the tables, and the pipeline handles the order, incremental processing, checkpoints, retries and data quality. (Formerly *Delta Live Tables / DLT*.)
# MAGIC
# MAGIC **The one idea to remember:** use **streaming tables** for ingestion and row-level transforms, and **materialized views** for aggregates and joins that must stay correct. Add **expectations** to every layer you care about.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we covered
# MAGIC
# MAGIC | Topic | Key takeaway |
# MAGIC |---|---|
# MAGIC | Why declarative pipelines | Define *what* the tables are, not *how* to run them. Dependencies are resolved automatically |
# MAGIC | Dataset types | **Streaming tables**, **materialized views**, temporary views, and **flows** |
# MAGIC | Developing a pipeline | **Lakeflow Pipelines Editor**, pipeline settings, dry run, and the pipeline graph |
# MAGIC | Medallion with Auto Loader | `STREAM read_files()` into bronze, cleaned silver, aggregated gold |
# MAGIC | Data quality | **Expectations**: warn, drop or fail |
# MAGIC | Running in production | Triggered vs continuous, development vs production mode, scheduling with a job |
# MAGIC | Monitoring | Pipeline graph, data quality metrics, and the **event log** |
# MAGIC | Change data capture | **AUTO CDC INTO** (formerly APPLY CHANGES) for **SCD Type 1 and 2** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset types
# MAGIC
# MAGIC | Object | Semantics | Typical layer |
# MAGIC |---|---|---|
# MAGIC | **Streaming table (ST)** | Incremental and append-oriented: each source row is processed **once** | Bronze ingestion, silver cleansing, CDC targets |
# MAGIC | **Materialized view (MV)** | Query result that is kept **correct** on every refresh. It is refreshed incrementally when possible and reflects updates and deletes | Gold aggregates, joins with changing dimensions |
# MAGIC | **Temporary view** | Not persisted, visible only inside the pipeline | Intermediate logic |
# MAGIC | **Flow** | A query that writes into a target. Several **append flows** can feed one ST | Fan-in from many sources, backfills |
# MAGIC
# MAGIC `STREAM(...)` / `spark.readStream` gives a **streaming** read, so the result is a streaming table. A plain `FROM` / `spark.read` gives a **batch** read, so the result is a materialized view.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A medallion pipeline in SQL
# MAGIC
# MAGIC ```sql
# MAGIC -- Bronze: incremental file ingestion with Auto Loader
# MAGIC CREATE OR REFRESH STREAMING TABLE orders_bronze
# MAGIC COMMENT 'Raw orders'
# MAGIC AS SELECT *, _metadata.file_name AS source_file, current_timestamp() AS ingested_at
# MAGIC FROM STREAM read_files('/Volumes/shop/raw/orders/', format => 'json');
# MAGIC
# MAGIC -- Silver: clean + data quality
# MAGIC CREATE OR REFRESH STREAMING TABLE orders_silver (
# MAGIC   CONSTRAINT valid_id     EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT positive_amt EXPECT (amount > 0)           ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT has_email    EXPECT (email IS NOT NULL)                      -- warn only
# MAGIC )
# MAGIC AS SELECT order_id, lower(email) AS email, CAST(amount AS DECIMAL(10,2)) AS amount,
# MAGIC           CAST(order_ts AS TIMESTAMP) AS order_ts
# MAGIC FROM STREAM(orders_bronze);
# MAGIC
# MAGIC -- Gold: aggregate for BI
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW daily_revenue
# MAGIC AS SELECT date(order_ts) AS order_date, sum(amount) AS revenue, count(*) AS orders
# MAGIC FROM orders_silver
# MAGIC GROUP BY ALL;
# MAGIC ```
# MAGIC
# MAGIC Tables reference each other by name, and the pipeline builds the **dependency graph** from those references. You never write "run bronze first".

# COMMAND ----------

# MAGIC %md
# MAGIC ## The same in Python
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp        # older code: import dlt
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC @dp.table(comment="Raw orders")
# MAGIC def orders_bronze():
# MAGIC     return (spark.readStream.format("cloudFiles")
# MAGIC             .option("cloudFiles.format", "json")
# MAGIC             .load("/Volumes/shop/raw/orders/"))
# MAGIC
# MAGIC @dp.table
# MAGIC @dp.expect_or_drop("valid_id", "order_id IS NOT NULL")
# MAGIC @dp.expect_or_fail("positive_amt", "amount > 0")
# MAGIC def orders_silver():
# MAGIC     return spark.readStream.table("orders_bronze").withColumn("email", F.lower("email"))
# MAGIC
# MAGIC @dp.materialized_view
# MAGIC def daily_revenue():
# MAGIC     return (spark.read.table("orders_silver")
# MAGIC             .groupBy(F.to_date("order_ts").alias("order_date"))
# MAGIC             .agg(F.sum("amount").alias("revenue")))
# MAGIC ```
# MAGIC
# MAGIC SQL and Python files can be mixed in the same pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality with expectations
# MAGIC
# MAGIC | Expectation | Invalid rows | Pipeline |
# MAGIC |---|---|---|
# MAGIC | `EXPECT (...)` / `@dp.expect` (default: **warn**) | Kept | Continues. Violations are counted |
# MAGIC | `ON VIOLATION DROP ROW` / `@dp.expect_or_drop` | **Dropped** | Continues. Violations are counted |
# MAGIC | `ON VIOLATION FAIL UPDATE` / `@dp.expect_or_fail` | — | The update **fails**. Nothing is committed for that flow |
# MAGIC
# MAGIC - Violation metrics show up in the pipeline UI and in the **event log**.
# MAGIC - `expect_all`, `expect_all_or_drop` and `expect_all_or_fail` take a dictionary of rules.
# MAGIC - **Quarantine pattern:** don't just drop bad rows. Write them to a separate table with the inverted rule, so you can inspect them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Developing and running a pipeline
# MAGIC
# MAGIC | Setting | Options | Guidance |
# MAGIC |---|---|---|
# MAGIC | **Editor** | Lakeflow Pipelines Editor (multi-file, graph, data preview) | Use **dry run** to validate code and the graph without processing data |
# MAGIC | **Destination** | Default catalog and schema in Unity Catalog | Tables are published as UC objects |
# MAGIC | **Compute** | **Serverless** (recommended) or classic | Serverless gives fast start and incremental MV refresh |
# MAGIC | **Pipeline mode** | **Triggered**: process available data, then stop. **Continuous**: always running | Triggered with a schedule for hourly or daily SLAs, continuous for low latency |
# MAGIC | **Run type** | Development: reuses compute, no retries. Production: retries and restarts | Production for scheduled runs |
# MAGIC | **Refresh** | Normal update, refresh selected tables, **full refresh** | Full refresh recomputes from scratch. Careful with sources that no longer hold old data |
# MAGIC | **Configuration** | Key-value parameters (e.g. `source_path`) | Avoid hard-coded paths. Read them in code with `spark.conf.get("source_path")` |
# MAGIC
# MAGIC Schedule the pipeline with its own schedule, or as a **pipeline task in a Lakeflow Job** (course 2).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring: the event log
# MAGIC
# MAGIC - **Pipeline graph:** status, row counts and timing per dataset.
# MAGIC - **Data quality tab:** expectation pass and fail counts per table.
# MAGIC - **Event log:** a Delta table with every update, flow progress, data quality metric and error.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT timestamp, event_type, message
# MAGIC FROM event_log(TABLE(orders_silver))     -- or event_log('<pipeline-id>')
# MAGIC WHERE event_type IN ('flow_progress', 'update_progress')
# MAGIC ORDER BY timestamp DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change data capture with AUTO CDC
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE customers_silver;
# MAGIC
# MAGIC CREATE FLOW customers_cdc AS AUTO CDC INTO customers_silver
# MAGIC FROM STREAM(customers_cdc_bronze)
# MAGIC KEYS (customer_id)
# MAGIC APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC SEQUENCE BY event_ts                 -- decides which change wins (handles out-of-order data)
# MAGIC COLUMNS * EXCEPT (operation, event_ts)
# MAGIC STORED AS SCD TYPE 2;                -- or SCD TYPE 1
# MAGIC ```
# MAGIC
# MAGIC | | SCD Type 1 | SCD Type 2 |
# MAGIC |---|---|---|
# MAGIC | Target holds | Latest state, one row per key | One row **per version** |
# MAGIC | History | Overwritten | Kept in **`__START_AT` / `__END_AT`** |
# MAGIC | Current rows | All rows | `WHERE __END_AT IS NULL` |
# MAGIC
# MAGIC AUTO CDC replaces hand-written `MERGE` logic for ordering, deduplication, deletes and SCD2 versioning. Use **AUTO CDC FROM SNAPSHOT** when the source only delivers full snapshots.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common mistakes
# MAGIC
# MAGIC - ❌ A **streaming table** for a gold aggregate that must reflect late corrections. Use an **MV**.
# MAGIC - ❌ A **materialized view** for raw, high-volume append ingestion. Use an **ST**.
# MAGIC - ❌ `EXPECT` without an action, expecting it to remove rows. The default only **warns**.
# MAGIC - ❌ **Continuous** mode for a daily report. Use **triggered** mode with a schedule.
# MAGIC - ❌ A full refresh of a bronze ST whose source (e.g. Kafka) no longer holds old events. That data is lost.
# MAGIC - ❌ Hand-written MERGE for out-of-order CDC. **AUTO CDC + `SEQUENCE BY`** handles it.
# MAGIC
# MAGIC **Next course:** *DevOps Essentials for Data Engineering*, which takes this code into a tested, versioned deployment.

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
# MAGIC dbxQuiz('dbxquiz', [{"q": "A gold table aggregates revenue per day, and upstream orders can be corrected or deleted days later. Which dataset type keeps the result correct?", "o": ["Streaming table", "Materialized view", "Temporary view", "Append flow"], "c": 1, "e": "A materialized view is kept correct on every refresh, including updates and deletes in its sources. A streaming table processes each input row only once."}, {"q": "Rows with a NULL order_id must be removed from the silver table, but the pipeline must keep running. Which expectation do you use?", "o": ["`EXPECT (order_id IS NOT NULL)`", "`EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW`", "`EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE`", "A CHECK constraint on the table"], "c": 1, "e": "DROP ROW removes invalid rows and records the count. Plain EXPECT only warns, and FAIL UPDATE stops the update."}, {"q": "Which statement makes a streaming read in a SQL pipeline definition?", "o": ["`FROM orders_bronze`", "`FROM STREAM(orders_bronze)`", "`FROM LIVE.orders_bronze`", "`FROM TABLE(orders_bronze)`"], "c": 1, "e": "`STREAM(...)` (or `STREAM read_files(...)`) gives an incremental, streaming read, producing a streaming table. A plain FROM is a batch read."}, {"q": "A change feed contains inserts, updates and deletes that may arrive out of order. The business needs the full history of customer addresses. What do you use?", "o": ["MERGE INTO in a foreachBatch", "AUTO CDC with SEQUENCE BY ... STORED AS SCD TYPE 2", "AUTO CDC STORED AS SCD TYPE 1", "A materialized view with ROW_NUMBER()"], "c": 1, "e": "AUTO CDC uses the sequence column to order changes correctly. SCD Type 2 keeps every version with __START_AT and __END_AT."}, {"q": "A pipeline feeds a dashboard that is refreshed once per day. Which pipeline mode is the most cost-effective?", "o": ["Continuous", "Triggered, scheduled daily", "Continuous with development mode", "Triggered, with a full refresh every run"], "c": 1, "e": "Triggered mode processes the available data and then stops. Continuous mode keeps compute running all the time, and full refresh reprocesses everything."}]);
# MAGIC </script>
