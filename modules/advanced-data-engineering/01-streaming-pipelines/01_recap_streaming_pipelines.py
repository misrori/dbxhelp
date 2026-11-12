# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Advanced Data Engineering with Databricks</span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">Recap 1: Databricks Streaming and Lakeflow Spark Declarative Pipelines</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Recap: Databricks Streaming and Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC > **Course 1 of 4 · Advanced Data Engineering with Databricks** · ⏱ ~9 minutes
# MAGIC >
# MAGIC > Production streaming: how **Structured Streaming** keeps state, emits results and recovers, and how **declarative pipelines** apply it to ingestion, joins, deduplication and **CDC**.
# MAGIC
# MAGIC **The one idea to remember:** a stream is an **unbounded table** processed incrementally. **Checkpoints** make it exactly-once, **watermarks** bound its state, and declarative pipelines manage most of this for you.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we covered
# MAGIC
# MAGIC | Topic | Key takeaway |
# MAGIC |---|---|
# MAGIC | Streaming concepts | Unbounded table, micro-batches, latency vs cost, batch ↔ streaming with the same API |
# MAGIC | Structured Streaming basics | `readStream` / `writeStream`, sources and sinks, **triggers**, **output modes**, **checkpoints** |
# MAGIC | Stateful processing | Aggregations, **windows** (tumbling, sliding, session), **watermarks**, late data |
# MAGIC | Streaming joins & deduplication | Stream-static vs stream-stream joins, `dropDuplicatesWithinWatermark` |
# MAGIC | Streaming in declarative pipelines | Streaming tables, Auto Loader and Kafka ingestion, append flows, quarantine |
# MAGIC | CDC and SCD | **AUTO CDC** with `SEQUENCE BY`, SCD Type 1 and 2, snapshots, change data feed |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Structured Streaming in one example
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC events = (spark.readStream                       # source: Delta table, Auto Loader, Kafka…
# MAGIC           .table("main.bronze.device_events"))
# MAGIC
# MAGIC per_window = (events
# MAGIC     .withWatermark("event_ts", "10 minutes")     # accept data up to 10 min late, then drop state
# MAGIC     .groupBy(F.window("event_ts", "5 minutes"), "device_id")
# MAGIC     .agg(F.avg("temp").alias("avg_temp")))
# MAGIC
# MAGIC (per_window.writeStream
# MAGIC     .outputMode("append")                        # emit each window once, when it is final
# MAGIC     .option("checkpointLocation", "/Volumes/main/chk/device_5min")
# MAGIC     .trigger(availableNow=True)                  # process what's there, then stop
# MAGIC     .toTable("main.silver.device_5min"))
# MAGIC ```
# MAGIC
# MAGIC | Concept | Options |
# MAGIC |---|---|
# MAGIC | **Trigger** | default / `processingTime="1 minute"` (continuous micro-batches) · **`availableNow=True`** (incremental batch, then stop) · real-time mode (very low latency) |
# MAGIC | **Output mode** | **Append**: only new, final rows (aggregations need a watermark) · **Update**: changed rows (use with `foreachBatch` + `MERGE`) · **Complete**: whole result each trigger (keeps all state) |
# MAGIC | **Checkpoint** | Source offsets + committed batches + state. **One per query.** Never share it or delete it |

# COMMAND ----------

# MAGIC %md
# MAGIC ## State, windows and watermarks
# MAGIC
# MAGIC ```
# MAGIC tumbling  |----|----|----|      fixed, non-overlapping: "per 5 minutes"
# MAGIC sliding   |------|               overlapping: "last 10 minutes, every 5 minutes"
# MAGIC             |------|
# MAGIC session   |--|   |-----|         gap-based: "activity bursts per user"
# MAGIC ```
# MAGIC
# MAGIC - **Stateless** operations (`select`, `filter`, stream-static join) don't remember anything between micro-batches.
# MAGIC - **Stateful** operations (aggregations, `dropDuplicates`, stream-stream joins) keep **state** in the state store (RocksDB), and it is saved in the checkpoint.
# MAGIC - **Watermark** = the latest event time seen minus the allowed delay. Windows older than the watermark are **finalized** and their state is **dropped**. Events arriving later than that can be ignored.
# MAGIC - A longer delay accepts later data, but keeps **more state** and delays append-mode output.
# MAGIC - Changing the aggregation keys, the window, or the number of stateful operators requires a **new checkpoint**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming joins and deduplication
# MAGIC
# MAGIC | Join | How it works | Watch out |
# MAGIC |---|---|---|
# MAGIC | **Stream-static** | Each micro-batch joins with the **current** version of a Delta table | Dimension changes are **not** re-applied to past results. Use an MV when results must stay correct |
# MAGIC | **Stream-stream** | Both sides buffered in state until they can match | Needs **watermarks on both sides** plus a **time-range condition**, otherwise state grows forever |
# MAGIC
# MAGIC ```python
# MAGIC deduped = (events
# MAGIC     .withWatermark("event_ts", "1 hour")
# MAGIC     .dropDuplicatesWithinWatermark(["event_id"]))   # bounded state
# MAGIC # dropDuplicates(["event_id"]) without a watermark keeps every key forever
# MAGIC ```
# MAGIC
# MAGIC **`foreachBatch`** runs batch code per micro-batch, e.g. a `MERGE` into Delta or writes to several sinks. It is at-least-once, so make it **idempotent** (MERGE is), and deduplicate inside the batch first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming in declarative pipelines
# MAGIC
# MAGIC ```sql
# MAGIC -- Bronze from Kafka: decode the value, keep metadata
# MAGIC CREATE OR REFRESH STREAMING TABLE events_bronze AS
# MAGIC SELECT CAST(value AS STRING) AS payload, timestamp AS kafka_ts, topic, partition, offset
# MAGIC FROM STREAM read_kafka(bootstrapServers => '${kafka_servers}', subscribe => 'events');
# MAGIC
# MAGIC -- Silver: parse, validate, deduplicate
# MAGIC CREATE OR REFRESH STREAMING TABLE events_silver (
# MAGIC   CONSTRAINT valid_device EXPECT (device_id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC ) AS SELECT payload:device_id::string AS device_id, payload:temp::double AS temp,
# MAGIC             payload:ts::timestamp AS event_ts
# MAGIC FROM STREAM(events_bronze);
# MAGIC ```
# MAGIC
# MAGIC - Pipelines manage **checkpoints, retries and the schema location** for you.
# MAGIC - **Append flows**: several sources (e.g. one Kafka topic per region) write into one streaming table, and backfills run as a separate `ONCE` flow.
# MAGIC - **Quarantine**: route rows that fail a rule into a separate table instead of dropping them silently.
# MAGIC - Use a **materialized view** for aggregates that must reflect late updates. A streaming table processes each row only once.
# MAGIC - A streaming read **fails** if its source table gets UPDATEs or DELETEs. Use `skipChangeCommits` or read the **change data feed**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC and slowly changing dimensions
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql.functions import col, expr
# MAGIC
# MAGIC dp.create_streaming_table("customers_silver")
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="customers_silver",
# MAGIC     source="customers_cdc_clean",
# MAGIC     keys=["customer_id"],
# MAGIC     sequence_by=col("event_ts"),                 # out-of-order events resolved by this column
# MAGIC     apply_as_deletes=expr("op = 'DELETE'"),
# MAGIC     except_column_list=["op", "event_ts"],
# MAGIC     stored_as_scd_type=2,                        # 1 = overwrite, 2 = keep history
# MAGIC     track_history_column_list=["address", "segment"],
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC | | SCD Type 1 | SCD Type 2 |
# MAGIC |---|---|---|
# MAGIC | Keeps | Latest row per key | Every version, with `__START_AT` / `__END_AT` |
# MAGIC | Current rows | All | `WHERE __END_AT IS NULL` |
# MAGIC | Typical question | "What is the customer's address?" | "Which segment was the customer in **when** they ordered?" |
# MAGIC
# MAGIC - **AUTO CDC FROM SNAPSHOT:** for sources that only deliver full snapshots.
# MAGIC - **Change data feed (CDF)** (`delta.enableChangeDataFeed = true`) exposes row-level inserts, updates and deletes of a Delta table, so downstream processing touches only what changed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decision cheat sheet
# MAGIC
# MAGIC | Situation | Choose |
# MAGIC |---|---|
# MAGIC | Hourly SLA, files land continuously | `availableNow` trigger / **triggered** pipeline + schedule |
# MAGIC | Seconds of latency on Kafka events | Continuous micro-batches or **real-time mode** |
# MAGIC | Per-window aggregates written to Delta once | **Append** mode + **watermark** |
# MAGIC | Upsert each micro-batch into a table | **`foreachBatch` + `MERGE`** (idempotent) |
# MAGIC | Duplicate events within an hour | `dropDuplicatesWithinWatermark` |
# MAGIC | Change feed with out-of-order updates and deletes | **AUTO CDC** + `SEQUENCE BY` |
# MAGIC | History of dimension changes | **SCD Type 2** |
# MAGIC | Propagate updates and deletes from silver to gold | **Change data feed** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common mistakes
# MAGIC
# MAGIC - ❌ An append-mode aggregation without a watermark. This is not allowed, and state is unbounded.
# MAGIC - ❌ Complete mode on a large keyspace. It keeps all state forever.
# MAGIC - ❌ Deleting or sharing a checkpoint to "fix" a stream. This causes duplicates or gaps.
# MAGIC - ❌ A stream-stream join without watermarks and a time condition.
# MAGIC - ❌ Streaming from a table that receives UPDATEs or DELETEs without CDF or `skipChangeCommits`.
# MAGIC - ❌ `trigger(once=True)` in new code. Use **`availableNow`**.
# MAGIC
# MAGIC **Next course:** *Databricks Data Privacy*, which protects the data these pipelines produce.

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
# MAGIC dbxQuiz('dbxquiz', [{"q": "A streaming aggregation per 5-minute window must be written to a Delta table in append mode. What else is required?", "o": ["Complete output mode", "A watermark on the event-time column", "A Spark cache on the source", "trigger(once=True)"], "c": 1, "e": "Append mode only emits final results. With an aggregation, Spark needs a watermark to know when a window is complete and its state can be dropped."}, {"q": "A job should process all newly arrived data every hour and then stop, to keep costs low. Which trigger fits?", "o": ["processingTime='1 hour'", "availableNow=True, scheduled hourly by a job", "Continuous trigger", "Real-time mode"], "c": 1, "e": "availableNow processes everything available (possibly in several batches) and then stops. A scheduled job turns it into an incremental batch. processingTime would keep the cluster running."}, {"q": "Which statement about streaming checkpoints is TRUE?", "o": ["Two queries writing to different tables can share one checkpoint", "Deleting the checkpoint is a safe way to recover from failures", "Each query needs its own checkpoint, which stores offsets and state", "Checkpoints are only needed for stateless queries"], "c": 2, "e": "The checkpoint holds source offsets, committed batches and state, giving exactly-once processing. Never share or delete it."}, {"q": "A stream of orders is joined to a customer dimension that changes, and gold results must always reflect the current customer data. What is the best choice?", "o": ["A stream-static join into a streaming table", "A materialized view that joins orders and customers", "A stream-stream join without watermarks", "Complete output mode"], "c": 1, "e": "A stream-static join uses the dimension as of processing time and never revisits old rows. A materialized view recomputes correctly when either side changes."}, {"q": "Duplicate events with the same event_id can arrive up to 30 minutes apart. How do you deduplicate with bounded state?", "o": ["`dropDuplicates(['event_id'])`", "`withWatermark('event_ts', '30 minutes').dropDuplicatesWithinWatermark(['event_id'])`", "`distinct()` in complete mode", "A MERGE in foreachBatch without dedup"], "c": 1, "e": "dropDuplicatesWithinWatermark keeps keys only as long as the watermark requires. dropDuplicates without a watermark stores every key forever."}]);
# MAGIC </script>
