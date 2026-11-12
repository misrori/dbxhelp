# Recap: DevOps Essentials for Data Engineering

> **Course 4 of 4 · Data Engineering with Databricks** · ⏱ ~8 minutes
>
> Applying software engineering practices to data work: **modular code, tests, version control, and automated deployment** across dev, test and prod.

**The one idea to remember:** treat pipelines as **software**. Logic goes in testable modules, everything lives in **Git**, and environments are promoted **automatically**. Nobody copies notebooks by hand.

## What we covered

| Topic | Key takeaway |
|---|---|
| Software engineering & DevOps | Why data teams need versioning, testing, automation and environments |
| CI/CD fundamentals | **Continuous integration** (merge often, test automatically) and **continuous deployment** (release automatically) |
| Modularizing code | Business logic in **functions and modules**, notebooks as thin entry points |
| Unit testing | **pytest** + PySpark testing utilities on small DataFrames |
| Integration testing | Run the real pipeline or job on test data and check the outputs |
| Version control | **Databricks Git folders**: branch, commit, push, pull, pull request |
| Deploying assets | UI, REST API, SDK, CLI, and **bundles**. Automate the deployment in CI |

## DevOps and CI/CD in one picture

```
 feature branch ──► Pull request ──► CI: lint · unit tests · validate · deploy to TEST · integration tests
                                        │
 merge to main  ──────────────────────► CD: deploy the SAME code to PROD (service principal)
```

| Environment | Purpose | Typical isolation |
|---|---|---|
| **Dev** | Each developer experiments safely | Personal schema / catalog, dev workspace |
| **Test / Staging** | Automated tests on realistic data | Separate catalog or workspace |
| **Prod** | Business-facing workloads | Separate workspace, runs as a **service principal** |

Code and configuration are the **same** everywhere. Only **parameters** (catalog, paths, schedules) differ per environment.

## Modular code

```python
# src/transforms.py: pure functions, easy to test
from pyspark.sql import DataFrame, functions as F

def clean_orders(df: DataFrame) -> DataFrame:
    return (df.filter(F.col("amount") > 0)
              .withColumn("email", F.lower(F.trim("email"))))

def add_net_amount(df: DataFrame, vat: float) -> DataFrame:
    return df.withColumn("net_amount", F.round(F.col("amount") / (1 + vat), 2))
```

```python
# notebook / job task: thin entry point
from transforms import clean_orders, add_net_amount

catalog = dbutils.widgets.get("catalog")                # environment comes from a parameter
raw = spark.read.table(f"{catalog}.bronze.orders")
(raw.transform(clean_orders)
    .transform(add_net_amount, vat=0.27)
    .write.mode("overwrite").saveAsTable(f"{catalog}.silver.orders"))
```

- Functions take a DataFrame and return a DataFrame, and chain with `.transform()`.
- **No hard-coded** catalogs, paths or credentials. Use parameters and **secrets**.
- Notebooks can import `.py` files from the same Git folder (workspace files).

## Unit tests with pytest

```python
# tests/test_transforms.py
from pyspark.testing import assertDataFrameEqual
from transforms import clean_orders

def test_clean_orders_removes_negative_and_normalizes_email(spark):
    src = spark.createDataFrame([(1, 10.0, " A@X.COM "), (2, -5.0, "b@x.com")],
                                "id INT, amount DOUBLE, email STRING")
    expected = spark.createDataFrame([(1, 10.0, "a@x.com")],
                                     "id INT, amount DOUBLE, email STRING")
    assertDataFrameEqual(clean_orders(src), expected)
```

| Test level | What it checks | Where it runs |
|---|---|---|
| **Unit** | A single function on tiny in-memory data | pytest locally (Databricks Connect) or in a notebook, and in **CI** |
| **Integration** | The deployed job or pipeline end-to-end on **test data** | Test/staging environment, after deployment |
| **Data quality** | Rules on real data: expectations, constraints, row-count checks | In the pipeline, every run |

- `assertDataFrameEqual` compares schema and rows (order-insensitive by default). `assertSchemaEqual` compares only the schema.
- Don't compare DataFrames with `collect() == collect()`, because it is order-sensitive and ignores the schema.

## Version control with Git folders

| Task | In Databricks |
|---|---|
| Clone a repo | *Workspace → Create → **Git folder*** → repository URL |
| Create / switch branch | Git dialog (click the branch name) |
| Commit & push | Git dialog → select changes → message → **Commit & Push** |
| Get others' changes | **Pull** |
| Review & merge | **Pull request** in the Git provider (GitHub, Azure DevOps, GitLab…) |

- One Git folder **per developer**, with short-lived **feature branches**. Never develop on `main`.
- Notebooks are stored as source files (`.py` / `.sql`), so diffs and code reviews work.
- Production should **not** run from someone's personal Git folder.

## Deploying Databricks assets

| Method | Good for |
|---|---|
| **UI** | Learning and one-offs. Not repeatable |
| **REST API / Python SDK** | Custom automation and scripts |
| **Databricks CLI** | Scripting from a terminal or CI runner |
| **Bundles** (Declarative Automation Bundles, formerly *Databricks Asset Bundles*) | **Recommended**: jobs, pipelines and code described as YAML and deployed per target |

```yaml
# databricks.yml (minimal)
bundle:
  name: orders_etl
include:
  - resources/*.yml
targets:
  dev:
    mode: development     # names prefixed "[dev user]", schedules paused
    default: true
  prod:
    mode: production
    workspace:
      host: https://prod.cloud.databricks.com
```

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      -t dev orders_job
```

> Bundles are covered in depth in the advanced course *Automated Deployment with Databricks Asset Bundles*.

## Try it: write and test a transformation

> Runs on in-memory DataFrames, so no data or schema is needed.

```python run
# Modular, testable transformations: DataFrame in, DataFrame out
from pyspark.sql import DataFrame, Row, functions as F

def clean_orders(df: DataFrame) -> DataFrame:
    return df.filter(F.col("amount") > 0).withColumn("email", F.lower(F.trim("email")))

def add_net_amount(df: DataFrame, vat: float) -> DataFrame:
    return df.withColumn("net_amount", F.round(F.col("amount") / (1 + vat), 2))
```

```python run
# Unit tests with pyspark.testing. They pass silently
from pyspark.testing import assertDataFrameEqual

src = spark.createDataFrame([(1, 10.0, " A@X.COM "), (2, -5.0, "b@x.com")], "id INT, amount DOUBLE, email STRING")
expected = spark.createDataFrame([(1, 10.0, "a@x.com")], "id INT, amount DOUBLE, email STRING")

assertDataFrameEqual(clean_orders(src), expected)
assertDataFrameEqual(src.transform(clean_orders).transform(add_net_amount, vat=0.25).select("net_amount"),
                     [Row(net_amount=8.0)])
print("✅ all tests passed")
```

```python run
# A failing test shows exactly which rows differ
try:
    assertDataFrameEqual(clean_orders(src),
                         spark.createDataFrame([(1, 10.0, "A@X.COM")], "id INT, amount DOUBLE, email STRING"))
except AssertionError as e:
    print("❌ test failed (as expected):\n", str(e)[:900])
```

> In a real project these functions live in `src/`, the tests in `tests/test_*.py`, and CI runs `pytest` on every pull request.

## Decision cheat sheet

| Situation | Choose |
|---|---|
| Verify a transformation function is correct | **Unit test** with `assertDataFrameEqual` |
| Verify the deployed job works with the real platform | **Integration test** in a test environment |
| Two engineers changing the same pipeline | **Feature branches** + **pull requests** |
| The same job in dev, test and prod with different catalogs | **Parameters** / bundle **variables** per target |
| Repeatable deployment from CI | **Bundles** + **Databricks CLI** |
| Who runs production jobs? | A **service principal**, not a person |

## Common mistakes

- ❌ All logic inside notebooks, "tested" by running them by hand.
- ❌ Copying notebooks between workspaces manually to promote code.
- ❌ Hard-coded `prod` catalog names or passwords in code.
- ❌ Working directly on `main` in a shared Git folder.
- ❌ Personal access tokens of a human user in CI. Use a **service principal** with OAuth.

**You finished the Data Engineering path.** Next up: *Advanced Data Engineering with Databricks*.

<!-- quiz:start -->
## Check yourself

**1. What is the main purpose of continuous integration (CI) for a data engineering team?**

- a) Deploy every commit to production immediately
- b) Merge changes frequently and validate them automatically with tests
- c) Back up notebooks every night
- d) Run all jobs continuously

<details><summary>Answer</summary>

**b)** CI means integrating changes often and running automated checks (lint, unit tests, validation) on every change. Continuous deployment then releases the validated code.

</details>

**2. Which is the best way to make a PySpark transformation unit-testable?**

- a) Keep it in one long notebook cell
- b) Write it as a function that takes and returns a DataFrame, in a separate module
- c) Read the source table inside the function using a hard-coded name
- d) Test it by running the production job

<details><summary>Answer</summary>

**b)** Pure DataFrame-in, DataFrame-out functions in modules can be imported by notebooks and jobs, and tested on small in-memory data with pytest.

</details>

**3. Which utility compares two DataFrames for both schema and rows in a unit test, ignoring row order by default?**

- a) `df1.collect() == df2.collect()`
- b) `assertDataFrameEqual(actual, expected)`
- c) `df1.exceptAll(df2).count() == 0`
- d) `assertSchemaEqual(actual, expected)`

<details><summary>Answer</summary>

**b)** pyspark.testing.assertDataFrameEqual checks schema and data, ignoring row order by default. assertSchemaEqual checks only the schema.

</details>

**4. Two engineers change the same pipeline at the same time. Which workflow keeps this safe?**

- a) Both edit the same shared Git folder on main
- b) Each works on a feature branch in their own Git folder and merges through a pull request
- c) One exports the notebook as DBC and emails it
- d) Use notebook revision history to undo conflicts

<details><summary>Answer</summary>

**b)** Feature branches in personal Git folders and reviewed pull requests isolate changes and make conflicts visible before merging.

</details>

**5. The same job must be deployed to dev, test and prod with a different catalog in each. What is the recommended approach?**

- a) Create the job by hand in each workspace
- b) Copy the job JSON and edit it per environment
- c) Define it once in a bundle and use targets with variables
- d) Hard-code the catalog and change it before each deployment

<details><summary>Answer</summary>

**c)** A bundle describes the job once. Targets and variables supply the environment-specific values, and `databricks bundle deploy -t <target>` promotes it.

</details>

<!-- quiz:end -->
