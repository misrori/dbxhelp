# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Building Reliable Conversational Agents with Genie</span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">Supplement: Metric Views</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 Demo - Metric Views Hands-on
# MAGIC ### Duration - ~25 Minutes
# MAGIC
# MAGIC > *Define a metric once, then ask any question at any grain and get the correct number.*
# MAGIC
# MAGIC In this demo you build a small retail star schema, see **why pre-aggregated views give wrong answers**, then create a **Unity Catalog metric view** with joins, a global filter, composed measures and agent metadata. You query it with `MEASURE()` and get it ready for a **Genie Agent** and an **AI/BI dashboard**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC **By the end of this demo, you will be able to:**
# MAGIC
# MAGIC - Explain why ratio and distinct-count KPIs break in pre-aggregated views
# MAGIC - Create a metric view in YAML with joins, a filter, fields, measures and agent metadata
# MAGIC - Query a metric view with `MEASURE()` at different grains
# MAGIC - Add window measures (trailing 7 days, cumulative)
# MAGIC - Use the metric view as a data source in a Genie Agent and an AI/BI dashboard

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## REQUIRED - SELECT SERVERLESS COMPUTE
# MAGIC
# MAGIC <div style="font-size: 1em; border-left: 4px solid #2E7D32; background: #E8F5E9; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC <span style="font-size: 24px">&#9989;</span>
# MAGIC <div>
# MAGIC <strong style="color: #1B5E20; font-size: 1.1em">Runs on Serverless</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333">Attach this notebook to <strong>Serverless</strong> (the default). Serverless always runs a recent runtime, so every metric view feature used here (YAML spec 1.1, agent metadata, window measures) is available. A classic cluster also works if it runs <strong>DBR 17.3 LTS or above</strong>.</p>
# MAGIC <p style="margin: 8px 0 0 0; color: #333">You only need a Unity Catalog <strong>schema you can write to</strong> (in the lab: your own schema in <code>dbacademy</code>). You fill it in once in section A.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Setup: Choose Where to Work and Load the Data
# MAGIC
# MAGIC **Fill in one variable:** the `catalog.schema` you can write to. Nothing is created outside it, and no `CREATE SCHEMA` permission is needed.
# MAGIC
# MAGIC - In the Databricks Academy lab, this is usually `dbacademy.<your labuser schema>`. Leave `FILL_IN` and run the cell once, and the error message suggests your schema.
# MAGIC - Elsewhere, use any schema you own, e.g. `main.my_sandbox`.

# COMMAND ----------

# DBTITLE 1,FILL IN: catalog.schema to work in
WORK_SCHEMA = "FILL_IN"   # e.g. "dbacademy.labuser1234567_1700000000"

# COMMAND ----------

# MAGIC %md
# MAGIC The next cell downloads the synthetic **retail** dataset from the [dbxhelp GitHub repo](https://github.com/misrori/dbxhelp/tree/main/data/retail) and creates three Delta tables in `WORK_SCHEMA`:
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC |---|---|---|
# MAGIC | `dim_customer` | 500 | Customers with `region` and `segment` |
# MAGIC | `dim_product` | 40 | Products in 5 categories. **Books are cheap, Electronics are expensive** |
# MAGIC | `fact_orders` | 60,000 | One row per order, Jan 2025 - Oct 2026. Includes `CANCELLED` and `TEST` orders |
# MAGIC
# MAGIC Cheap products are ordered much more often than expensive ones. This skew is what makes the "average of averages" mistake visible.
# MAGIC
# MAGIC The tables get **comments** and **primary/foreign keys with `RELY`**. Both help Genie (and the optimizer) understand the model. The cell also runs `USE CATALOG` / `USE SCHEMA`, so the SQL cells below can use short table names.

# COMMAND ----------

# DBTITLE 1,Load the synthetic data from GitHub into Delta tables
import urllib.request as u; exec(u.urlopen("https://raw.githubusercontent.com/misrori/dbxhelp/main/dbxdata.py").read().decode())
S = load_dataset("retail", WORK_SCHEMA)   # S = `catalog`.`schema`, used inside the YAML definitions below

# COMMAND ----------

# DBTITLE 1,Quick look at the skew
# MAGIC %sql
# MAGIC SELECT p.category, count(*) AS orders, round(avg(p.unit_price), 2) AS avg_unit_price
# MAGIC FROM fact_orders f JOIN dim_product p USING (product_id)
# MAGIC GROUP BY ALL ORDER BY orders DESC;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. The Problem: Pre-Aggregated Views Give Wrong Answers
# MAGIC
# MAGIC <p style="font-size: 1em; line-height: 1.6; color: #333">A common pattern is to build a "reporting view" at a fixed grain, for example <strong>category &times; month</strong>, holding the KPIs <em>average order value (AOV)</em> and <em>unique customers</em>. It works perfectly at that grain. But Genie, or a colleague, will eventually ask a question at a <strong>different grain</strong>.</p>

# COMMAND ----------

# DBTITLE 1,A classic pre-aggregated reporting view
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW v_sales_by_category_month
# MAGIC COMMENT 'Pre-aggregated reporting view at category x month grain'
# MAGIC AS
# MAGIC SELECT
# MAGIC   p.category                                                AS product_category,
# MAGIC   date_trunc('MONTH', f.order_date)                         AS order_month,
# MAGIC   SUM(f.quantity * p.unit_price * (1 - f.discount))         AS net_revenue,
# MAGIC   COUNT(*)                                                  AS completed_orders,
# MAGIC   SUM(f.quantity * p.unit_price * (1 - f.discount)) / COUNT(*) AS avg_order_value,
# MAGIC   COUNT(DISTINCT f.customer_id)                             AS unique_customers
# MAGIC FROM fact_orders f
# MAGIC JOIN dim_product p USING (product_id)
# MAGIC WHERE f.status = 'COMPLETED'
# MAGIC GROUP BY ALL;

# COMMAND ----------

# MAGIC %md
# MAGIC **Question:** *"What was the average order value and how many unique customers did we have in 2025?"*
# MAGIC
# MAGIC The "obvious" query on the reporting view, next to the **correct** answer computed from the detail rows:

# COMMAND ----------

# DBTITLE 1,Wrong (from the view) vs correct (from detail rows)
# MAGIC %sql
# MAGIC WITH from_view AS (
# MAGIC   SELECT 'From pre-aggregated view' AS method,
# MAGIC          round(AVG(avg_order_value), 2) AS avg_order_value,   -- average of averages
# MAGIC          SUM(unique_customers)          AS unique_customers   -- double counts customers
# MAGIC   FROM v_sales_by_category_month
# MAGIC   WHERE year(order_month) = 2025
# MAGIC ),
# MAGIC correct AS (
# MAGIC   SELECT 'Correct (detail rows)' AS method,
# MAGIC          round(SUM(f.quantity * p.unit_price * (1 - f.discount)) / COUNT(*), 2),
# MAGIC          COUNT(DISTINCT f.customer_id)
# MAGIC   FROM fact_orders f JOIN dim_product p USING (product_id)
# MAGIC   WHERE f.status = 'COMPLETED' AND year(f.order_date) = 2025
# MAGIC )
# MAGIC SELECT * FROM from_view UNION ALL SELECT * FROM correct;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; border-left: 4px solid #f44336; background: #ffebee; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC <span style="font-size: 24px">&#9888;&#65039;</span>
# MAGIC <div>
# MAGIC <strong style="color: #c62828; font-size: 1.1em">Two wrong numbers from one innocent question</strong>
# MAGIC <ul style="margin: 8px 0 0 16px; color: #333">
# MAGIC <li><strong>AOV:</strong> averaging per-category averages gives the rare, expensive Electronics orders the same weight as the many cheap Books orders, so the result is far too high.</li>
# MAGIC <li><strong>Unique customers:</strong> a distinct count is <em>not additive</em>. The same customer shows up in many months and categories, so the sum is several times larger than the real 500.</li>
# MAGIC </ul>
# MAGIC <p style="margin: 8px 0 0 0; color: #333">An LLM writing SQL against this view will happily produce these numbers. A <strong>metric view</strong> fixes this by design.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Create the Metric View
# MAGIC
# MAGIC The YAML below defines:
# MAGIC
# MAGIC - **source** `fact_orders`, plus a global **filter** that always removes `TEST` orders
# MAGIC - **joins** to `dim_customer` and `dim_product` (star schema)
# MAGIC - **fields** (dimensions) with `display_name` and `synonyms`
# MAGIC - **measures**, including *composed* measures (`MEASURE(a) / MEASURE(b)`) and measure-level `FILTER (WHERE …)` rules
# MAGIC - **formats** (currency, percentage) that dashboards apply automatically
# MAGIC
# MAGIC > The YAML is written in a Python string so the fully qualified source names can be filled in for your catalog. In your own projects you would write it directly in a SQL cell or in the Catalog Explorer editor.

# COMMAND ----------

# DBTITLE 1,CREATE VIEW ... WITH METRICS LANGUAGE YAML
metric_view_yaml = """
version: 1.1
comment: Retail sales KPIs. Single source of truth for revenue, orders, AOV and customers. TEST orders are always excluded.
source: "__S__.fact_orders"
filter: source.status <> 'TEST'

joins:
  - name: customer
    source: "__S__.dim_customer"
    'on': source.customer_id = customer.customer_id
  - name: product
    source: "__S__.dim_product"
    'on': source.product_id = product.product_id

fields:
  - name: order_date
    expr: source.order_date
    comment: Date the order was placed
    display_name: Order Date
    synonyms: ['date', 'purchase date', 'day']
  - name: order_month
    expr: DATE_TRUNC('MONTH', source.order_date)
    comment: First day of the order month
    display_name: Order Month
    synonyms: ['month']
  - name: order_year
    expr: YEAR(source.order_date)
    display_name: Order Year
    synonyms: ['year']
  - name: order_status
    expr: INITCAP(source.status)
    comment: Completed, Open or Cancelled
    display_name: Order Status
  - name: region
    expr: customer.region
    comment: Sales region of the customer
    display_name: Region
    synonyms: ['market', 'territory', 'geo']
  - name: customer_segment
    expr: customer.segment
    display_name: Customer Segment
    synonyms: ['segment', 'customer type']
  - name: product_category
    expr: product.category
    display_name: Product Category
    synonyms: ['category', 'product line']
  - name: product_name
    expr: product.product_name
    display_name: Product
    synonyms: ['item', 'sku']

measures:
  - name: order_count
    expr: COUNT(1)
    comment: Number of orders in any status (TEST excluded)
    display_name: Orders (all statuses)
  - name: completed_orders
    expr: COUNT(1) FILTER (WHERE source.status = 'COMPLETED')
    comment: Number of completed orders
    display_name: Completed Orders
    synonyms: ['orders', 'number of orders', 'sales count']
  - name: gross_revenue
    expr: SUM(source.quantity * product.unit_price) FILTER (WHERE source.status = 'COMPLETED')
    comment: Revenue before discounts, completed orders only
    display_name: Gross Revenue
    format: {type: currency, currency_code: USD, decimal_places: {type: exact, places: 2}, abbreviation: compact}
  - name: net_revenue
    expr: SUM(source.quantity * product.unit_price * (1 - source.discount)) FILTER (WHERE source.status = 'COMPLETED')
    comment: Revenue after discounts, completed orders only. This is THE revenue KPI.
    display_name: Net Revenue
    synonyms: ['revenue', 'sales', 'turnover', 'income']
    format: {type: currency, currency_code: USD, decimal_places: {type: exact, places: 2}, abbreviation: compact}
  - name: avg_order_value
    expr: MEASURE(net_revenue) / MEASURE(completed_orders)
    comment: Net revenue divided by completed orders
    display_name: Average Order Value
    synonyms: ['AOV', 'average basket', 'average order size']
    format: {type: currency, currency_code: USD, decimal_places: {type: exact, places: 2}}
  - name: unique_customers
    expr: COUNT(DISTINCT source.customer_id) FILTER (WHERE source.status = 'COMPLETED')
    comment: Distinct customers with at least one completed order
    display_name: Unique Customers
    synonyms: ['active customers', 'buyers', 'customer count']
  - name: revenue_per_customer
    expr: MEASURE(net_revenue) / MEASURE(unique_customers)
    display_name: Revenue per Customer
    format: {type: currency, currency_code: USD, decimal_places: {type: exact, places: 2}}
  - name: discount_rate
    expr: 1 - MEASURE(net_revenue) / MEASURE(gross_revenue)
    comment: Share of gross revenue given away as discount
    display_name: Discount Rate
    format: {type: percentage, decimal_places: {type: max, places: 1}}
  - name: cancellation_rate
    expr: COUNT(1) FILTER (WHERE source.status = 'CANCELLED') / COUNT(1)
    comment: Cancelled orders divided by all orders
    display_name: Cancellation Rate
    synonyms: ['cancel rate', 'churned orders share']
    format: {type: percentage, decimal_places: {type: max, places: 1}}
""".replace("__S__", S)

spark.sql(f"""
CREATE OR REPLACE VIEW {S}.sales_metrics
WITH METRICS
LANGUAGE YAML
AS $$
{metric_view_yaml}
$$
""")
print(f"Created metric view {S}.sales_metrics")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; border-left: 4px solid #1976d2; background: #e3f2fd; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC <span style="font-size: 24px">&#8505;&#65039;</span>
# MAGIC <div>
# MAGIC <strong style="color: #0d47a1; font-size: 1.1em">Look at it in Catalog Explorer</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333">Open <strong>Catalog</strong> &rarr; your catalog &rarr; your schema &rarr; <code>sales_metrics</code>. The <strong>Overview</strong> tab lists fields and measures, and <strong>Edit</strong> opens the visual editor (with a <code>&lt;&gt;</code> YAML tab). Creating a metric view from the UI works the same way: open any table &rarr; <strong>Create &rarr; Metric view</strong>.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Query the Metric View with `MEASURE()`
# MAGIC
# MAGIC Fields are selected like normal columns. Measures are wrapped in `MEASURE()`. The **grouping is up to the query**.
# MAGIC
# MAGIC First the same question as before: *AOV and unique customers in 2025*. Compare it with the "correct" row from section C.

# COMMAND ----------

# DBTITLE 1,Same question, correct answer
# MAGIC %sql
# MAGIC SELECT
# MAGIC   round(MEASURE(avg_order_value), 2) AS avg_order_value,
# MAGIC   MEASURE(unique_customers)          AS unique_customers
# MAGIC FROM sales_metrics
# MAGIC WHERE order_year = 2025;

# COMMAND ----------

# MAGIC %md
# MAGIC Now the **same measures at different grains**, without changing the definition:

# COMMAND ----------

# DBTITLE 1,By category
# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_category,
# MAGIC   MEASURE(completed_orders)               AS completed_orders,
# MAGIC   round(MEASURE(net_revenue), 0)          AS net_revenue,
# MAGIC   round(MEASURE(avg_order_value), 2)      AS avg_order_value,
# MAGIC   round(MEASURE(discount_rate) * 100, 1)  AS discount_rate_pct
# MAGIC FROM sales_metrics
# MAGIC WHERE order_year = 2025
# MAGIC GROUP BY ALL
# MAGIC ORDER BY net_revenue DESC;

# COMMAND ----------

# DBTITLE 1,By region and segment, with a filter on a joined dimension
# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   customer_segment,
# MAGIC   MEASURE(unique_customers)                 AS customers,
# MAGIC   round(MEASURE(revenue_per_customer), 2)   AS revenue_per_customer,
# MAGIC   round(MEASURE(cancellation_rate) * 100, 1) AS cancellation_rate_pct
# MAGIC FROM sales_metrics
# MAGIC WHERE product_category = 'Electronics'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY region, customer_segment;

# COMMAND ----------

# DBTITLE 1,Monthly trend (try the Visualization tab)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_month,
# MAGIC   MEASURE(net_revenue)      AS net_revenue,
# MAGIC   MEASURE(avg_order_value)  AS avg_order_value
# MAGIC FROM sales_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY order_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining a metric view with another table
# MAGIC
# MAGIC Metric views **can't be joined directly**. Aggregate in a CTE first, then join the CTE:

# COMMAND ----------

# DBTITLE 1,CTE pattern for joins
# MAGIC %sql
# MAGIC WITH revenue_by_region AS (
# MAGIC   SELECT region, MEASURE(net_revenue) AS net_revenue
# MAGIC   FROM sales_metrics
# MAGIC   WHERE order_year = 2025
# MAGIC   GROUP BY region
# MAGIC ),
# MAGIC targets AS (
# MAGIC   SELECT * FROM VALUES ('EMEA', 4500000), ('North America', 3000000), ('APAC', 1700000), ('LATAM', 800000) AS t(region, target)
# MAGIC )
# MAGIC SELECT r.region, round(r.net_revenue) AS net_revenue, t.target,
# MAGIC        round(r.net_revenue / t.target * 100, 1) AS pct_of_target
# MAGIC FROM revenue_by_region r JOIN targets t USING (region)
# MAGIC ORDER BY pct_of_target DESC;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; border-left: 4px solid #ff9800; background: #fff3e0; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC <span style="font-size: 24px">&#128269;</span>
# MAGIC <div>
# MAGIC <strong style="color: #e65100; font-size: 1.1em">Try it: what fails?</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333">Uncomment and run the cell below. <code>SELECT *</code> and selecting a measure without <code>MEASURE()</code> are not allowed. This is a feature: no one can accidentally re-aggregate a ratio.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM sales_metrics;
# MAGIC -- SELECT region, SUM(net_revenue) FROM sales_metrics GROUP BY region;

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Inspect the Definition
# MAGIC
# MAGIC `DESCRIBE TABLE EXTENDED … AS JSON` returns the full YAML (in *View Text*) and the agent metadata for every column. This is the same information Genie reads.

# COMMAND ----------

# DBTITLE 1,Describe the metric view
# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED sales_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Window Measures (Trailing and Cumulative)
# MAGIC
# MAGIC Window measures compute values *over a range of an ordered field*, for example trailing 7 days or running totals. They go in a separate metric view here, so the main one stays simple.
# MAGIC
# MAGIC - `range: trailing 7 day` covers the previous 7 days, **excluding** the current day (the default)
# MAGIC - `range: cumulative` is a running total up to and including the current day
# MAGIC - `semiadditive: last`: when you don't group by the window's `order` field, the value of the last period is returned

# COMMAND ----------

# DBTITLE 1,Metric view with window measures
window_yaml = """
version: 1.1
comment: Daily revenue with trailing and cumulative window measures
source: "__S__.fact_orders"
filter: source.status = 'COMPLETED'
joins:
  - name: product
    source: "__S__.dim_product"
    'on': source.product_id = product.product_id
fields:
  - name: order_date
    expr: source.order_date
    display_name: Order Date
measures:
  - name: daily_revenue
    expr: SUM(source.quantity * product.unit_price * (1 - source.discount))
    display_name: Daily Net Revenue
  - name: revenue_trailing_7d
    expr: SUM(source.quantity * product.unit_price * (1 - source.discount))
    display_name: Net Revenue (trailing 7 days)
    window:
      - order: order_date
        range: trailing 7 day
        semiadditive: last
  - name: revenue_cumulative
    expr: SUM(source.quantity * product.unit_price * (1 - source.discount))
    display_name: Net Revenue (cumulative)
    window:
      - order: order_date
        range: cumulative
        semiadditive: last
""".replace("__S__", S)

spark.sql(f"""
CREATE OR REPLACE VIEW {S}.sales_metrics_daily
WITH METRICS
LANGUAGE YAML
AS $$
{window_yaml}
$$
""")

display(spark.sql("""
SELECT order_date,
       round(MEASURE(daily_revenue))        AS daily_revenue,
       round(MEASURE(revenue_trailing_7d))  AS revenue_trailing_7d,
       round(MEASURE(revenue_cumulative))   AS revenue_cumulative
FROM sales_metrics_daily
GROUP BY ALL
ORDER BY order_date DESC
LIMIT 14
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## H. Use the Metric View in a Genie Agent
# MAGIC
# MAGIC <div style="font-size: 1em; border-left: 4px solid #1976d2; background: #e3f2fd; padding: 16px 20px; border-radius: 4px; margin: 16px 0">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px">
# MAGIC <span style="font-size: 24px">&#8505;&#65039;</span>
# MAGIC <div>
# MAGIC <strong style="color: #0d47a1; font-size: 1.1em">Naming</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333"><strong>Genie Agents</strong> were formerly known as <strong>Genie Spaces</strong>. Course material may use either name.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC 1. Open **Genie** in the left sidebar and click **New**.
# MAGIC 2. **Add data:** select `sales_metrics` from your demo schema. Start with the metric view **only**, so Genie has to use the governed measures.
# MAGIC 3. Pick a **pro or serverless SQL warehouse**.
# MAGIC 4. Add short **instructions** that give business context, not formulas, for example:
# MAGIC    > *You are a sales analyst assistant for a retail company. "Revenue" always means net revenue. Fiscal year = calendar year. When the user says "last quarter", use the last complete calendar quarter.*
# MAGIC 5. Ask these questions and open **Show code** each time. The SQL should use `MEASURE(...)`:
# MAGIC    - *What was our revenue by region in 2025?*
# MAGIC    - *Which product line has the highest AOV?* (uses synonyms: *product line* → `product_category`, *AOV* → `avg_order_value`)
# MAGIC    - *How many active customers did we have in Q3 2026?*
# MAGIC    - *Show the monthly cancel rate for Enterprise customers.*
# MAGIC 6. **Compare:** create a second Genie Agent with only the raw tables (`fact_orders`, `dim_customer`, `dim_product`) and ask *"What was the average order value in 2025?"*. Does it exclude TEST and CANCELLED orders? Does it compute the ratio correctly?
# MAGIC 7. Add **benchmarks**: save the questions above with their expected `MEASURE()` SQL, so you can re-test after every change.
# MAGIC
# MAGIC > **Tip:** an existing Genie Agent can also be turned into a metric view: kebab menu → **Export to metric view**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Use the Metric View in an AI/BI Dashboard
# MAGIC
# MAGIC 1. **New → Dashboard → Data tab → Add data**.
# MAGIC 2. **Option 1, UC metric view as dataset:** pick `sales_metrics`. All fields and measures appear, with display names and formats applied automatically. You can't edit the dataset query, but you can add *custom calculations*.
# MAGIC 3. **Option 2, SQL dataset:** write a query with `MEASURE()` when you need filtering or reshaping:
# MAGIC    ```sql
# MAGIC    SELECT order_month, region, MEASURE(net_revenue) AS net_revenue
# MAGIC    FROM sales_metrics WHERE order_year = 2026 GROUP BY ALL
# MAGIC    ```
# MAGIC 4. **Option 3, local metric view (new):** pick a **table** (e.g. `fact_orders`) as the source. The *local metric view* editor opens, where you define dimensions, measures and joins **only for this dashboard**, without publishing anything to Unity Catalog.
# MAGIC 5. **Export as metric view (Public Preview):** on a dataset's kebab menu, choose *Create Metric View*, then pick the catalog and schema and **Save**. The local definition becomes a governed UC metric view that Genie can use as well.

# COMMAND ----------

# MAGIC %md
# MAGIC ## J. Cleanup (Optional)
# MAGIC
# MAGIC Keep the objects if you still want to build the Genie Agent or dashboard. Otherwise uncomment and run. This removes only the demo tables and views, not your schema:

# COMMAND ----------

# DBTITLE 1,Drop the demo objects (your schema itself is kept)
# for obj in ["sales_metrics_daily", "sales_metrics", "v_sales_by_category_month"]:
#     spark.sql(f"DROP VIEW IF EXISTS {S}.{obj}")
# for tbl in ["fact_orders", "dim_product", "dim_customer"]:
#     spark.sql(f"DROP TABLE IF EXISTS {S}.{tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Pre-aggregated views break **non-additive** KPIs (ratios, distinct counts) as soon as someone asks at another grain.
# MAGIC - A **metric view** stores the *definition* (source, joins, filter, fields, measures) in Unity Catalog. The **grain is chosen at query time**.
# MAGIC - Query with `MEASURE()`. `SELECT *` and direct joins aren't allowed, so use a CTE.
# MAGIC - **Agent metadata** (`comment`, `display_name`, `synonyms`, `format`) makes Genie and dashboards understand your vocabulary.
# MAGIC - Dashboards can use UC metric views, SQL over metric views, or **local metric views**, and can **export** a dataset to a UC metric view.
