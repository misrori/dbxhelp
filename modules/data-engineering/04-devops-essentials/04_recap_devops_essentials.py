# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Data Engineering with Databricks</span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">Recap 4: DevOps Essentials for Data Engineering</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Recap: DevOps Essentials for Data Engineering
# MAGIC
# MAGIC > **Course 4 of 4 · Data Engineering with Databricks** · ⏱ ~8 minutes
# MAGIC >
# MAGIC > Applying software engineering practices to data work: **modular code, tests, version control, and automated deployment** across dev, test and prod.
# MAGIC
# MAGIC **The one idea to remember:** treat pipelines as **software**. Logic goes in testable modules, everything lives in **Git**, and environments are promoted **automatically**. Nobody copies notebooks by hand.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we covered
# MAGIC
# MAGIC | Topic | Key takeaway |
# MAGIC |---|---|
# MAGIC | Software engineering & DevOps | Why data teams need versioning, testing, automation and environments |
# MAGIC | CI/CD fundamentals | **Continuous integration** (merge often, test automatically) and **continuous deployment** (release automatically) |
# MAGIC | Modularizing code | Business logic in **functions and modules**, notebooks as thin entry points |
# MAGIC | Unit testing | **pytest** + PySpark testing utilities on small DataFrames |
# MAGIC | Integration testing | Run the real pipeline or job on test data and check the outputs |
# MAGIC | Version control | **Databricks Git folders**: branch, commit, push, pull, pull request |
# MAGIC | Deploying assets | UI, REST API, SDK, CLI, and **bundles**. Automate the deployment in CI |

# COMMAND ----------

# MAGIC %md
# MAGIC ## DevOps and CI/CD in one picture
# MAGIC
# MAGIC ```
# MAGIC  feature branch ──► Pull request ──► CI: lint · unit tests · validate · deploy to TEST · integration tests
# MAGIC                                         │
# MAGIC  merge to main  ──────────────────────► CD: deploy the SAME code to PROD (service principal)
# MAGIC ```
# MAGIC
# MAGIC | Environment | Purpose | Typical isolation |
# MAGIC |---|---|---|
# MAGIC | **Dev** | Each developer experiments safely | Personal schema / catalog, dev workspace |
# MAGIC | **Test / Staging** | Automated tests on realistic data | Separate catalog or workspace |
# MAGIC | **Prod** | Business-facing workloads | Separate workspace, runs as a **service principal** |
# MAGIC
# MAGIC Code and configuration are the **same** everywhere. Only **parameters** (catalog, paths, schedules) differ per environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modular code
# MAGIC
# MAGIC ```python
# MAGIC # src/transforms.py: pure functions, easy to test
# MAGIC from pyspark.sql import DataFrame, functions as F
# MAGIC
# MAGIC def clean_orders(df: DataFrame) -> DataFrame:
# MAGIC     return (df.filter(F.col("amount") > 0)
# MAGIC               .withColumn("email", F.lower(F.trim("email"))))
# MAGIC
# MAGIC def add_net_amount(df: DataFrame, vat: float) -> DataFrame:
# MAGIC     return df.withColumn("net_amount", F.round(F.col("amount") / (1 + vat), 2))
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC # notebook / job task: thin entry point
# MAGIC from transforms import clean_orders, add_net_amount
# MAGIC
# MAGIC catalog = dbutils.widgets.get("catalog")                # environment comes from a parameter
# MAGIC raw = spark.read.table(f"{catalog}.bronze.orders")
# MAGIC (raw.transform(clean_orders)
# MAGIC     .transform(add_net_amount, vat=0.27)
# MAGIC     .write.mode("overwrite").saveAsTable(f"{catalog}.silver.orders"))
# MAGIC ```
# MAGIC
# MAGIC - Functions take a DataFrame and return a DataFrame, and chain with `.transform()`.
# MAGIC - **No hard-coded** catalogs, paths or credentials. Use parameters and **secrets**.
# MAGIC - Notebooks can import `.py` files from the same Git folder (workspace files).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unit tests with pytest
# MAGIC
# MAGIC ```python
# MAGIC # tests/test_transforms.py
# MAGIC from pyspark.testing import assertDataFrameEqual
# MAGIC from transforms import clean_orders
# MAGIC
# MAGIC def test_clean_orders_removes_negative_and_normalizes_email(spark):
# MAGIC     src = spark.createDataFrame([(1, 10.0, " A@X.COM "), (2, -5.0, "b@x.com")],
# MAGIC                                 "id INT, amount DOUBLE, email STRING")
# MAGIC     expected = spark.createDataFrame([(1, 10.0, "a@x.com")],
# MAGIC                                      "id INT, amount DOUBLE, email STRING")
# MAGIC     assertDataFrameEqual(clean_orders(src), expected)
# MAGIC ```
# MAGIC
# MAGIC | Test level | What it checks | Where it runs |
# MAGIC |---|---|---|
# MAGIC | **Unit** | A single function on tiny in-memory data | pytest locally (Databricks Connect) or in a notebook, and in **CI** |
# MAGIC | **Integration** | The deployed job or pipeline end-to-end on **test data** | Test/staging environment, after deployment |
# MAGIC | **Data quality** | Rules on real data: expectations, constraints, row-count checks | In the pipeline, every run |
# MAGIC
# MAGIC - `assertDataFrameEqual` compares schema and rows (order-insensitive by default). `assertSchemaEqual` compares only the schema.
# MAGIC - Don't compare DataFrames with `collect() == collect()`, because it is order-sensitive and ignores the schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Version control with Git folders
# MAGIC
# MAGIC | Task | In Databricks |
# MAGIC |---|---|
# MAGIC | Clone a repo | *Workspace → Create → **Git folder*** → repository URL |
# MAGIC | Create / switch branch | Git dialog (click the branch name) |
# MAGIC | Commit & push | Git dialog → select changes → message → **Commit & Push** |
# MAGIC | Get others' changes | **Pull** |
# MAGIC | Review & merge | **Pull request** in the Git provider (GitHub, Azure DevOps, GitLab…) |
# MAGIC
# MAGIC - One Git folder **per developer**, with short-lived **feature branches**. Never develop on `main`.
# MAGIC - Notebooks are stored as source files (`.py` / `.sql`), so diffs and code reviews work.
# MAGIC - Production should **not** run from someone's personal Git folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying Databricks assets
# MAGIC
# MAGIC | Method | Good for |
# MAGIC |---|---|
# MAGIC | **UI** | Learning and one-offs. Not repeatable |
# MAGIC | **REST API / Python SDK** | Custom automation and scripts |
# MAGIC | **Databricks CLI** | Scripting from a terminal or CI runner |
# MAGIC | **Bundles** (Declarative Automation Bundles, formerly *Databricks Asset Bundles*) | **Recommended**: jobs, pipelines and code described as YAML and deployed per target |
# MAGIC
# MAGIC ```yaml
# MAGIC # databricks.yml (minimal)
# MAGIC bundle:
# MAGIC   name: orders_etl
# MAGIC include:
# MAGIC   - resources/*.yml
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development     # names prefixed "[dev user]", schedules paused
# MAGIC     default: true
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     workspace:
# MAGIC       host: https://prod.cloud.databricks.com
# MAGIC ```
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle validate -t dev
# MAGIC databricks bundle deploy   -t dev
# MAGIC databricks bundle run      -t dev orders_job
# MAGIC ```
# MAGIC
# MAGIC > Bundles are covered in depth in the advanced course *Automated Deployment with Databricks Asset Bundles*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decision cheat sheet
# MAGIC
# MAGIC | Situation | Choose |
# MAGIC |---|---|
# MAGIC | Verify a transformation function is correct | **Unit test** with `assertDataFrameEqual` |
# MAGIC | Verify the deployed job works with the real platform | **Integration test** in a test environment |
# MAGIC | Two engineers changing the same pipeline | **Feature branches** + **pull requests** |
# MAGIC | The same job in dev, test and prod with different catalogs | **Parameters** / bundle **variables** per target |
# MAGIC | Repeatable deployment from CI | **Bundles** + **Databricks CLI** |
# MAGIC | Who runs production jobs? | A **service principal**, not a person |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common mistakes
# MAGIC
# MAGIC - ❌ All logic inside notebooks, "tested" by running them by hand.
# MAGIC - ❌ Copying notebooks between workspaces manually to promote code.
# MAGIC - ❌ Hard-coded `prod` catalog names or passwords in code.
# MAGIC - ❌ Working directly on `main` in a shared Git folder.
# MAGIC - ❌ Personal access tokens of a human user in CI. Use a **service principal** with OAuth.
# MAGIC
# MAGIC **You finished the Data Engineering path.** Next up: *Advanced Data Engineering with Databricks*.

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
# MAGIC dbxQuiz('dbxquiz', [{"q": "What is the main purpose of continuous integration (CI) for a data engineering team?", "o": ["Deploy every commit to production immediately", "Merge changes frequently and validate them automatically with tests", "Back up notebooks every night", "Run all jobs continuously"], "c": 1, "e": "CI means integrating changes often and running automated checks (lint, unit tests, validation) on every change. Continuous deployment then releases the validated code."}, {"q": "Which is the best way to make a PySpark transformation unit-testable?", "o": ["Keep it in one long notebook cell", "Write it as a function that takes and returns a DataFrame, in a separate module", "Read the source table inside the function using a hard-coded name", "Test it by running the production job"], "c": 1, "e": "Pure DataFrame-in, DataFrame-out functions in modules can be imported by notebooks and jobs, and tested on small in-memory data with pytest."}, {"q": "Which utility compares two DataFrames for both schema and rows in a unit test, ignoring row order by default?", "o": ["`df1.collect() == df2.collect()`", "`assertDataFrameEqual(actual, expected)`", "`df1.exceptAll(df2).count() == 0`", "`assertSchemaEqual(actual, expected)`"], "c": 1, "e": "pyspark.testing.assertDataFrameEqual checks schema and data, ignoring row order by default. assertSchemaEqual checks only the schema."}, {"q": "Two engineers change the same pipeline at the same time. Which workflow keeps this safe?", "o": ["Both edit the same shared Git folder on main", "Each works on a feature branch in their own Git folder and merges through a pull request", "One exports the notebook as DBC and emails it", "Use notebook revision history to undo conflicts"], "c": 1, "e": "Feature branches in personal Git folders and reviewed pull requests isolate changes and make conflicts visible before merging."}, {"q": "The same job must be deployed to dev, test and prod with a different catalog in each. What is the recommended approach?", "o": ["Create the job by hand in each workspace", "Copy the job JSON and edit it per environment", "Define it once in a bundle and use targets with variables", "Hard-code the catalog and change it before each deployment"], "c": 2, "e": "A bundle describes the job once. Targets and variables supply the environment-specific values, and `databricks bundle deploy -t <target>` promotes it."}]);
# MAGIC </script>
