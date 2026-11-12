# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Data Engineering with Databricks</span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">Recap 1: Data Ingestion with Lakeflow Connect</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Recap: Data Ingestion with Lakeflow Connect
# MAGIC
# MAGIC > **Course 1 of 4 · Data Engineering with Databricks** · ⏱ ~8 minutes
# MAGIC >
# MAGIC > Getting data **into** the lakehouse: files from cloud storage, semi-structured formats, and enterprise sources, all landing in **Unity Catalog** bronze tables.
# MAGIC
# MAGIC **The one idea to remember:** pick the *most managed* ingestion option that fits the source. Then make the load **incremental** and **traceable**, so you only process new data and always know where each row came from.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we covered
# MAGIC
# MAGIC | Lesson | Key takeaway |
# MAGIC |---|---|
# MAGIC | 01 Lab environment | Unity Catalog: `catalog.schema.table` and **volumes** for files |
# MAGIC | 02A CTAS & COPY INTO | Batch load with `read_files()`; **incremental, idempotent** load with `COPY INTO` |
# MAGIC | 02B Streaming tables | `CREATE OR REFRESH STREAMING TABLE` + Auto Loader, the recommended incremental method |
# MAGIC | 03 Metadata columns | `_metadata.file_name`, `file_modification_time`, `current_timestamp()` for lineage |
# MAGIC | 04 CSV & rescued data | CSV options, explicit schemas, and `_rescued_data` for rows that don't fit |
# MAGIC | 06 JSON | Flatten JSON strings, use `from_json` / STRUCT, and the `VARIANT` type |
# MAGIC | 08 Lakeflow Connect | **Managed connectors** for databases and SaaS, plus Partner Connect |
# MAGIC | 09 MERGE INTO (bonus) | Upserts with schema enforcement and evolution |
# MAGIC | Appendix | XML ingestion, Auto Loader in Python |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The ingestion menu
# MAGIC
# MAGIC Start at the top of the list and move down only when you need to:
# MAGIC
# MAGIC | Option | Use for |
# MAGIC |---|---|
# MAGIC | **Managed connectors** (Lakeflow Connect) | SaaS apps (Salesforce, Workday, ServiceNow…) and databases (SQL Server, PostgreSQL…). Fully managed, incremental, CDC-aware |
# MAGIC | **Standard connectors** | Files in cloud object storage (Auto Loader, `COPY INTO`, `read_files`) and message buses (Kafka, Kinesis…) |
# MAGIC | **Upload UI / Add data** | One-off CSV, JSON or Excel files into a table or a **volume** |
# MAGIC | **Partner Connect** (Fivetran, …) | Sources not covered natively, or a tool the company already uses |
# MAGIC | **Custom code** (JDBC / REST in a notebook) | Anything else. You own pagination, retries and incrementality |
# MAGIC
# MAGIC Everything should end up in **Unity Catalog-governed** tables. Volume paths look like `/Volumes/<catalog>/<schema>/<volume>/<path>`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Three ways to load files
# MAGIC
# MAGIC ```sql
# MAGIC -- 1) Batch: CTAS + read_files()  → re-reads everything every time
# MAGIC CREATE OR REPLACE TABLE bronze_users AS
# MAGIC SELECT * FROM read_files('/Volumes/shop/raw/users/', format => 'parquet');
# MAGIC
# MAGIC -- 2) Incremental batch: COPY INTO  → idempotent, already-loaded files are skipped
# MAGIC CREATE TABLE IF NOT EXISTS bronze_sales;
# MAGIC COPY INTO bronze_sales
# MAGIC FROM '/Volumes/shop/raw/sales/'
# MAGIC FILEFORMAT = PARQUET
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC -- 3) Incremental: streaming table + Auto Loader  → recommended
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_orders
# MAGIC AS SELECT * FROM STREAM read_files('/Volumes/shop/raw/orders/', format => 'json');
# MAGIC
# MAGIC REFRESH STREAMING TABLE bronze_orders;   -- pick up new files
# MAGIC ```
# MAGIC
# MAGIC | | CTAS + `read_files` | `COPY INTO` | Streaming table / Auto Loader |
# MAGIC |---|---|---|---|
# MAGIC | Re-run behaviour | Reloads **all** files | Loads only **new** files | Processes only **new** files |
# MAGIC | Scale | Small / one-off | Thousands of files | **Millions** of files |
# MAGIC | Schema drift | Re-infers | `mergeSchema` | Evolution modes, schema hints, `_rescued_data` |
# MAGIC | State | None | Tracked in the table | Checkpoint (managed for you) |
# MAGIC
# MAGIC > A streaming table created in Databricks SQL is refreshed by a **serverless pipeline** that Databricks creates for you. It does not use warehouse compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Traceability: metadata columns
# MAGIC
# MAGIC Every file source exposes a hidden `_metadata` column. Select it explicitly when you load:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE bronze_users AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   cast(from_unixtime(user_first_touch_timestamp / 1000000) AS DATE) AS first_touch_date,
# MAGIC   _metadata.file_name              AS source_file,
# MAGIC   _metadata.file_modification_time AS file_modified_at,
# MAGIC   current_timestamp()              AS ingested_at
# MAGIC FROM read_files('/Volumes/shop/raw/users/', format => 'parquet');
# MAGIC ```
# MAGIC
# MAGIC This tells you **which file** a row came from and **when** it was loaded. That's essential for debugging and for reprocessing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV and the rescued data column
# MAGIC
# MAGIC CSV needs options. The defaults often give a single column, or headers loaded as data:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM read_files(
# MAGIC   '/Volumes/shop/raw/sales-csv/',
# MAGIC   format    => 'csv',
# MAGIC   header    => true,
# MAGIC   delimiter => '|',
# MAGIC   schema    => 'order_id INT, email STRING, amount DOUBLE',
# MAGIC   rescuedDataColumn => '_rescued_data'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC - **`_rescued_data`** catches values that don't match the schema (wrong type, extra columns) as JSON, so **nothing is silently lost**.
# MAGIC - When the schema is inferred, `read_files` / Auto Loader add `_rescued_data` automatically.
# MAGIC - Missing header? Set `header => false` and supply the column names through `schema`.
# MAGIC - Typical lab pattern: ingest, inspect rows `WHERE _rescued_data IS NOT NULL`, then fix and coalesce them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## JSON and semi-structured data
# MAGIC
# MAGIC ```sql
# MAGIC -- JSON files → table (nested fields become STRUCT / ARRAY)
# MAGIC SELECT * FROM read_files('/Volumes/shop/raw/events/', format => 'json');
# MAGIC
# MAGIC -- A JSON *string* column (e.g. Kafka value, after unbase64 + cast to STRING):
# MAGIC SELECT value:device, value:items[0].sku::int FROM bronze_kafka;               -- colon syntax
# MAGIC SELECT from_json(value, schema_of_json('<sample json>')) AS v FROM bronze_kafka; -- → STRUCT
# MAGIC SELECT parse_json(value) AS v FROM bronze_kafka;                               -- → VARIANT
# MAGIC ```
# MAGIC
# MAGIC | Need | Use |
# MAGIC |---|---|
# MAGIC | Quick extraction from a JSON string | `col:field.subfield`, cast with `::type` |
# MAGIC | Typed nested columns | `from_json(col, schema)` → STRUCT, then `select v.*` to flatten |
# MAGIC | Flexible, evolving schema, fast queries | **`VARIANT`** via `parse_json()` |
# MAGIC | One row per array element | `explode()` / `explode_outer()` |
# MAGIC | XML (appendix) | `read_files(..., format => 'xml', rowTag => 'book')` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enterprise sources: Lakeflow Connect managed connectors
# MAGIC
# MAGIC - In the workspace, open **Data Ingestion**. It shows the Databricks connectors, file upload, and Fivetran (partner) connectors.
# MAGIC - A managed connector creates an **ingestion pipeline**:
# MAGIC   - a **connection** (credentials stored as a UC object);
# MAGIC   - the source objects to sync;
# MAGIC   - the destination **catalog and schema**.
# MAGIC - **SaaS connectors** run fully serverless. **Database connectors** add an **ingestion gateway** that reads the change log (CDC).
# MAGIC - Each source table lands as a **streaming table**, with history as **SCD Type 1** (overwrite) or **SCD Type 2** (keep history).
# MAGIC - Schedule it from the pipeline itself or from a Lakeflow Job.
# MAGIC
# MAGIC > Federation **queries data in place**. Lakeflow Connect **ingests** it. These are different tools for different needs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: MERGE INTO for upserts
# MAGIC
# MAGIC ```sql
# MAGIC MERGE WITH SCHEMA EVOLUTION INTO users_target t
# MAGIC USING users_updates s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC ```
# MAGIC
# MAGIC - Atomic insert, update and delete in a single statement.
# MAGIC - **Schema enforcement** by default: new source columns cause an error. Use `WITH SCHEMA EVOLUTION` to add them to the target.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decision cheat sheet
# MAGIC
# MAGIC | Situation | Choose |
# MAGIC |---|---|
# MAGIC | Salesforce / SQL Server tables, keep them in sync | **Managed connector** |
# MAGIC | New files land in a volume every hour | **Streaming table** (Auto Loader) or `COPY INTO` on a schedule |
# MAGIC | Millions of small files, continuous arrival | **Auto Loader** |
# MAGIC | A one-off Excel file from a business user | **Upload UI** |
# MAGIC | You must know which file each row came from | Add **`_metadata`** columns |
# MAGIC | Bad CSV rows must not be lost | **`_rescued_data`** |
# MAGIC | Incoming changes must update existing rows | **`MERGE INTO`** (or AUTO CDC in pipelines, course 3) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common mistakes
# MAGIC
# MAGIC - ❌ Expecting `COPY INTO` to reload every file on each run. It is **idempotent** and skips files it has already loaded. Use `'force' = 'true'` to reload.
# MAGIC - ❌ CTAS on a schedule for growing data: this reprocesses **everything** on every run.
# MAGIC - ❌ Ignoring `_rescued_data`: bad values quietly turn into NULLs in your analysis.
# MAGIC - ❌ Writing a custom REST poller for a source that has a **managed connector**.
# MAGIC - ❌ Hard-coding credentials. Use **UC connections** or **secrets**.
# MAGIC
# MAGIC **Next course:** *Deploy Workloads with Lakeflow Jobs*, which schedules and orchestrates these loads.

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
# MAGIC dbxQuiz('dbxquiz', [{"q": "You run the same COPY INTO command every hour against a volume. What happens to files that were already loaded in a previous run?", "o": ["They are loaded again, creating duplicates", "They are skipped, because COPY INTO is idempotent", "The command fails until the files are deleted", "They are reloaded only if mergeSchema is true"], "c": 1, "e": "COPY INTO tracks which files it has loaded and skips them on re-runs. Use COPY_OPTIONS ('force' = 'true') to reload on purpose."}, {"q": "A CSV load must keep values that don't match the defined schema instead of turning them into NULLs. What do you use?", "o": ["The `_metadata` column", "`header => true`", "The rescued data column (`_rescued_data`)", "`mergeSchema`"], "c": 2, "e": "The rescued data column stores non-conforming values as JSON so nothing is silently lost. You can then inspect rows where `_rescued_data IS NOT NULL`."}, {"q": "Which expression adds the source file name to every ingested row?", "o": ["`input_file()`", "`_metadata.file_name`", "`current_timestamp()`", "`_rescued_data.file`"], "c": 1, "e": "Every file-based source exposes a hidden `_metadata` column with fields like file_name, file_path and file_modification_time. You must select it explicitly."}, {"q": "The team needs to keep 40 Salesforce objects in sync in Unity Catalog with minimal maintenance. What is the best fit?", "o": ["A notebook that calls the Salesforce REST API on a schedule", "Lakehouse Federation", "A Lakeflow Connect managed connector", "COPY INTO from a nightly CSV export"], "c": 2, "e": "Start with the most managed option. The Salesforce managed connector provides incremental ingestion into streaming tables, with schema-change handling and no custom code."}, {"q": "A JSON payload's schema keeps changing and analysts need fast queries on nested fields. Which approach fits best?", "o": ["Store it as STRING and use regexp_extract", "`parse_json()` into a VARIANT column", "Flatten every field into its own column by hand", "Convert it to CSV first"], "c": 1, "e": "VARIANT stores semi-structured data efficiently without a fixed schema, and supports path queries like `v:items[0].sku::int`."}]);
# MAGIC </script>
