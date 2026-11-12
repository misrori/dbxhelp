# Recap: Data Ingestion with Lakeflow Connect

> **Course 1 of 4 · Data Engineering with Databricks** · ⏱ ~8 minutes
>
> Getting data **into** the lakehouse: files from cloud storage, semi-structured formats, and enterprise sources, all landing in **Unity Catalog** bronze tables.

**The one idea to remember:** pick the *most managed* ingestion option that fits the source. Then make the load **incremental** and **traceable**, so you only process new data and always know where each row came from.

## What we covered

| Lesson | Key takeaway |
|---|---|
| 01 Lab environment | Unity Catalog: `catalog.schema.table` and **volumes** for files |
| 02A CTAS & COPY INTO | Batch load with `read_files()`; **incremental, idempotent** load with `COPY INTO` |
| 02B Streaming tables | `CREATE OR REFRESH STREAMING TABLE` + Auto Loader, the recommended incremental method |
| 03 Metadata columns | `_metadata.file_name`, `file_modification_time`, `current_timestamp()` for lineage |
| 04 CSV & rescued data | CSV options, explicit schemas, and `_rescued_data` for rows that don't fit |
| 06 JSON | Flatten JSON strings, use `from_json` / STRUCT, and the `VARIANT` type |
| 08 Lakeflow Connect | **Managed connectors** for databases and SaaS, plus Partner Connect |
| 09 MERGE INTO (bonus) | Upserts with schema enforcement and evolution |
| Appendix | XML ingestion, Auto Loader in Python |

## The ingestion menu

Start at the top of the list and move down only when you need to:

| Option | Use for |
|---|---|
| **Managed connectors** (Lakeflow Connect) | SaaS apps (Salesforce, Workday, ServiceNow…) and databases (SQL Server, PostgreSQL…). Fully managed, incremental, CDC-aware |
| **Standard connectors** | Files in cloud object storage (Auto Loader, `COPY INTO`, `read_files`) and message buses (Kafka, Kinesis…) |
| **Upload UI / Add data** | One-off CSV, JSON or Excel files into a table or a **volume** |
| **Partner Connect** (Fivetran, …) | Sources not covered natively, or a tool the company already uses |
| **Custom code** (JDBC / REST in a notebook) | Anything else. You own pagination, retries and incrementality |

Everything should end up in **Unity Catalog-governed** tables. Volume paths look like `/Volumes/<catalog>/<schema>/<volume>/<path>`.

## Three ways to load files

```sql
-- 1) Batch: CTAS + read_files()  → re-reads everything every time
CREATE OR REPLACE TABLE bronze_users AS
SELECT * FROM read_files('/Volumes/shop/raw/users/', format => 'parquet');

-- 2) Incremental batch: COPY INTO  → idempotent, already-loaded files are skipped
CREATE TABLE IF NOT EXISTS bronze_sales;
COPY INTO bronze_sales
FROM '/Volumes/shop/raw/sales/'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true');

-- 3) Incremental: streaming table + Auto Loader  → recommended
CREATE OR REFRESH STREAMING TABLE bronze_orders
AS SELECT * FROM STREAM read_files('/Volumes/shop/raw/orders/', format => 'json');

REFRESH STREAMING TABLE bronze_orders;   -- pick up new files
```

| | CTAS + `read_files` | `COPY INTO` | Streaming table / Auto Loader |
|---|---|---|---|
| Re-run behaviour | Reloads **all** files | Loads only **new** files | Processes only **new** files |
| Scale | Small / one-off | Thousands of files | **Millions** of files |
| Schema drift | Re-infers | `mergeSchema` | Evolution modes, schema hints, `_rescued_data` |
| State | None | Tracked in the table | Checkpoint (managed for you) |

> A streaming table created in Databricks SQL is refreshed by a **serverless pipeline** that Databricks creates for you. It does not use warehouse compute.

## Traceability: metadata columns

Every file source exposes a hidden `_metadata` column. Select it explicitly when you load:

```sql
CREATE OR REPLACE TABLE bronze_users AS
SELECT
  *,
  cast(from_unixtime(user_first_touch_timestamp / 1000000) AS DATE) AS first_touch_date,
  _metadata.file_name              AS source_file,
  _metadata.file_modification_time AS file_modified_at,
  current_timestamp()              AS ingested_at
FROM read_files('/Volumes/shop/raw/users/', format => 'parquet');
```

This tells you **which file** a row came from and **when** it was loaded. That's essential for debugging and for reprocessing.

## CSV and the rescued data column

CSV needs options. The defaults often give a single column, or headers loaded as data:

```sql
SELECT * FROM read_files(
  '/Volumes/shop/raw/sales-csv/',
  format    => 'csv',
  header    => true,
  delimiter => '|',
  schema    => 'order_id INT, email STRING, amount DOUBLE',
  rescuedDataColumn => '_rescued_data'
);
```

- **`_rescued_data`** catches values that don't match the schema (wrong type, extra columns) as JSON, so **nothing is silently lost**.
- When the schema is inferred, `read_files` / Auto Loader add `_rescued_data` automatically.
- Missing header? Set `header => false` and supply the column names through `schema`.
- Typical lab pattern: ingest, inspect rows `WHERE _rescued_data IS NOT NULL`, then fix and coalesce them.

## JSON and semi-structured data

```sql
-- JSON files → table (nested fields become STRUCT / ARRAY)
SELECT * FROM read_files('/Volumes/shop/raw/events/', format => 'json');

-- A JSON *string* column (e.g. Kafka value, after unbase64 + cast to STRING):
SELECT value:device, value:items[0].sku::int FROM bronze_kafka;               -- colon syntax
SELECT from_json(value, schema_of_json('<sample json>')) AS v FROM bronze_kafka; -- → STRUCT
SELECT parse_json(value) AS v FROM bronze_kafka;                               -- → VARIANT
```

| Need | Use |
|---|---|
| Quick extraction from a JSON string | `col:field.subfield`, cast with `::type` |
| Typed nested columns | `from_json(col, schema)` → STRUCT, then `select v.*` to flatten |
| Flexible, evolving schema, fast queries | **`VARIANT`** via `parse_json()` |
| One row per array element | `explode()` / `explode_outer()` |
| XML (appendix) | `read_files(..., format => 'xml', rowTag => 'book')` |

## Enterprise sources: Lakeflow Connect managed connectors

- In the workspace, open **Data Ingestion**. It shows the Databricks connectors, file upload, and Fivetran (partner) connectors.
- A managed connector creates an **ingestion pipeline**:
  - a **connection** (credentials stored as a UC object);
  - the source objects to sync;
  - the destination **catalog and schema**.
- **SaaS connectors** run fully serverless. **Database connectors** add an **ingestion gateway** that reads the change log (CDC).
- Each source table lands as a **streaming table**, with history as **SCD Type 1** (overwrite) or **SCD Type 2** (keep history).
- Schedule it from the pipeline itself or from a Lakeflow Job.

> Federation **queries data in place**. Lakeflow Connect **ingests** it. These are different tools for different needs.

## Bonus: MERGE INTO for upserts

```sql
MERGE WITH SCHEMA EVOLUTION INTO users_target t
USING users_updates s
ON t.id = s.id
WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

- Atomic insert, update and delete in a single statement.
- **Schema enforcement** by default: new source columns cause an error. Use `WITH SCHEMA EVOLUTION` to add them to the target.

## Decision cheat sheet

| Situation | Choose |
|---|---|
| Salesforce / SQL Server tables, keep them in sync | **Managed connector** |
| New files land in a volume every hour | **Streaming table** (Auto Loader) or `COPY INTO` on a schedule |
| Millions of small files, continuous arrival | **Auto Loader** |
| A one-off Excel file from a business user | **Upload UI** |
| You must know which file each row came from | Add **`_metadata`** columns |
| Bad CSV rows must not be lost | **`_rescued_data`** |
| Incoming changes must update existing rows | **`MERGE INTO`** (or AUTO CDC in pipelines, course 3) |

## Common mistakes

- ❌ Expecting `COPY INTO` to reload every file on each run. It is **idempotent** and skips files it has already loaded. Use `'force' = 'true'` to reload.
- ❌ CTAS on a schedule for growing data: this reprocesses **everything** on every run.
- ❌ Ignoring `_rescued_data`: bad values quietly turn into NULLs in your analysis.
- ❌ Writing a custom REST poller for a source that has a **managed connector**.
- ❌ Hard-coding credentials. Use **UC connections** or **secrets**.

**Next course:** *Deploy Workloads with Lakeflow Jobs*, which schedules and orchestrates these loads.

<!-- quiz:start -->
## Check yourself

**1. You run the same COPY INTO command every hour against a volume. What happens to files that were already loaded in a previous run?**

- a) They are loaded again, creating duplicates
- b) They are skipped, because COPY INTO is idempotent
- c) The command fails until the files are deleted
- d) They are reloaded only if mergeSchema is true

<details><summary>Answer</summary>

**b)** COPY INTO tracks which files it has loaded and skips them on re-runs. Use COPY_OPTIONS ('force' = 'true') to reload on purpose.

</details>

**2. A CSV load must keep values that don't match the defined schema instead of turning them into NULLs. What do you use?**

- a) The `_metadata` column
- b) `header => true`
- c) The rescued data column (`_rescued_data`)
- d) `mergeSchema`

<details><summary>Answer</summary>

**c)** The rescued data column stores non-conforming values as JSON so nothing is silently lost. You can then inspect rows where `_rescued_data IS NOT NULL`.

</details>

**3. Which expression adds the source file name to every ingested row?**

- a) `input_file()`
- b) `_metadata.file_name`
- c) `current_timestamp()`
- d) `_rescued_data.file`

<details><summary>Answer</summary>

**b)** Every file-based source exposes a hidden `_metadata` column with fields like file_name, file_path and file_modification_time. You must select it explicitly.

</details>

**4. The team needs to keep 40 Salesforce objects in sync in Unity Catalog with minimal maintenance. What is the best fit?**

- a) A notebook that calls the Salesforce REST API on a schedule
- b) Lakehouse Federation
- c) A Lakeflow Connect managed connector
- d) COPY INTO from a nightly CSV export

<details><summary>Answer</summary>

**c)** Start with the most managed option. The Salesforce managed connector provides incremental ingestion into streaming tables, with schema-change handling and no custom code.

</details>

**5. A JSON payload's schema keeps changing and analysts need fast queries on nested fields. Which approach fits best?**

- a) Store it as STRING and use regexp_extract
- b) `parse_json()` into a VARIANT column
- c) Flatten every field into its own column by hand
- d) Convert it to CSV first

<details><summary>Answer</summary>

**b)** VARIANT stores semi-structured data efficiently without a fixed schema, and supports path queries like `v:items[0].sku::int`.

</details>

<!-- quiz:end -->
