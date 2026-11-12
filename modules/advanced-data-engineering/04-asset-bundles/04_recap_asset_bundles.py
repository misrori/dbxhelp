# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Advanced Data Engineering with Databricks</span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">Recap 4: Automated Deployment with Databricks Asset Bundles</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Recap: Automated Deployment with Databricks Asset Bundles
# MAGIC
# MAGIC > **Course 4 of 4 · Advanced Data Engineering with Databricks** · ⏱ ~9 minutes
# MAGIC >
# MAGIC > Describing jobs, pipelines and code **as YAML in Git**, and deploying the **same project** to dev, test and prod with the Databricks CLI and a CI/CD pipeline. (Bundles are now also called *Declarative Automation Bundles*.)
# MAGIC
# MAGIC **The one idea to remember:** one bundle, many **targets**. The code and resource definitions never change between environments, only **variables and target overrides** do. CI runs `validate` → `deploy` → `run` for you.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we covered
# MAGIC
# MAGIC | Topic | Key takeaway |
# MAGIC |---|---|
# MAGIC | DevOps & CI/CD review | Why manual UI deployments don't scale |
# MAGIC | What a bundle is | A project folder with `databricks.yml`: **resources** + **targets** + **variables** |
# MAGIC | Bundle configuration | `include`, resource YAML files, substitutions, **deployment modes** |
# MAGIC | Deploying with the CLI | `bundle init` · `validate` · `deploy` · `run` · `summary` · `destroy` |
# MAGIC | Templates & project structure | `src/`, `resources/`, `tests/`, building **wheels** as artifacts |
# MAGIC | CI/CD automation | GitHub Actions running tests and bundle commands as a **service principal** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bundle anatomy
# MAGIC
# MAGIC ```
# MAGIC orders_etl/
# MAGIC ├── databricks.yml            # bundle name, includes, variables, targets
# MAGIC ├── resources/
# MAGIC │   ├── orders.job.yml        # one file per job / pipeline
# MAGIC │   └── orders.pipeline.yml
# MAGIC ├── src/                      # notebooks, pipeline sources, Python package
# MAGIC ├── tests/                    # pytest unit tests
# MAGIC └── pyproject.toml            # optional: build a wheel
# MAGIC ```
# MAGIC
# MAGIC ```yaml
# MAGIC # databricks.yml
# MAGIC bundle:
# MAGIC   name: orders_etl
# MAGIC
# MAGIC include:
# MAGIC   - resources/*.yml
# MAGIC
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     description: Target catalog
# MAGIC     default: dev
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     default: true
# MAGIC     workspace:
# MAGIC       host: https://dev.cloud.databricks.com
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     workspace:
# MAGIC       host: https://prod.cloud.databricks.com
# MAGIC     variables:
# MAGIC       catalog: prod
# MAGIC     run_as:
# MAGIC       service_principal_name: 6f1c0000-aaaa-bbbb-cccc-000000000000
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resources: jobs and pipelines as YAML
# MAGIC
# MAGIC ```yaml
# MAGIC # resources/orders.job.yml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     orders_job:
# MAGIC       name: orders-etl-${bundle.target}
# MAGIC       tasks:
# MAGIC         - task_key: ingest
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../src/ingest.py
# MAGIC             base_parameters:
# MAGIC               catalog: ${var.catalog}
# MAGIC         - task_key: transform
# MAGIC           depends_on: [{ task_key: ingest }]
# MAGIC           pipeline_task:
# MAGIC             pipeline_id: ${resources.pipelines.orders_pipeline.id}
# MAGIC       schedule:
# MAGIC         quartz_cron_expression: "0 0 2 * * ?"
# MAGIC         timezone_id: UTC
# MAGIC ```
# MAGIC
# MAGIC - **Resource types:** jobs, pipelines, dashboards, schemas, volumes, models, serving endpoints, apps…
# MAGIC - **Substitutions:** `${var.catalog}`, `${bundle.target}`, `${workspace.current_user.userName}`, `${resources.pipelines.orders_pipeline.id}`.
# MAGIC - **Existing UI job → YAML:** `databricks bundle generate job --existing-job-id 123`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables and overrides
# MAGIC
# MAGIC The value of a variable is resolved in this order of precedence (highest first):
# MAGIC
# MAGIC 1. `--var="catalog=prod"` on the CLI
# MAGIC 2. `BUNDLE_VAR_catalog` environment variable (great for CI secrets)
# MAGIC 3. `targets.<target>.variables`
# MAGIC 4. The variable's `default`
# MAGIC
# MAGIC - A variable can be a **lookup**, which resolves an ID by name: `lookup: { warehouse: "Shared SQL" }`.
# MAGIC - **Never** put credentials in `databricks.yml`. Inject them through `BUNDLE_VAR_…`, or use secret scopes.
# MAGIC - Bundle variables are **deployment-time** values. Values that change **per run** belong in **job parameters**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployment modes
# MAGIC
# MAGIC | `mode: development` | `mode: production` |
# MAGIC |---|---|
# MAGIC | Resource names prefixed **`[dev <user>]`**, so each developer gets an isolated copy | Names as written |
# MAGIC | **Schedules and triggers paused** | Schedules active |
# MAGIC | Concurrent runs allowed, deployment lock off | Validates `run_as`, permissions, and that pipelines are not in development mode |
# MAGIC | Fast iteration (e.g. `--cluster-id` override) | Cluster overrides not allowed |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The CLI lifecycle
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle init default-python      # start from a template
# MAGIC databricks bundle validate -t dev          # check syntax, references, variables
# MAGIC databricks bundle deploy   -t dev          # upload files + create/update resources (idempotent)
# MAGIC databricks bundle run      -t dev orders_job
# MAGIC databricks bundle summary  -t dev          # what is deployed, with URLs
# MAGIC databricks bundle destroy  -t dev          # remove everything this bundle deployed to dev
# MAGIC ```
# MAGIC
# MAGIC - Deploys are **declarative**: a resource removed from the YAML is removed from the workspace on the next deploy, and manual UI edits are overwritten.
# MAGIC - **Artifacts:** the bundle can build a **wheel** (`artifacts:` with `build: uv build --wheel`) and attach it to tasks, so prod runs exactly the tested version.

# COMMAND ----------

# MAGIC %md
# MAGIC ## CI/CD with GitHub Actions
# MAGIC
# MAGIC ```yaml
# MAGIC # .github/workflows/deploy.yml (simplified)
# MAGIC on:
# MAGIC   pull_request:
# MAGIC   push:
# MAGIC     branches: [main]
# MAGIC jobs:
# MAGIC   deploy:
# MAGIC     runs-on: ubuntu-latest
# MAGIC     env:
# MAGIC       DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
# MAGIC       DATABRICKS_CLIENT_ID: ${{ secrets.SP_CLIENT_ID }}          # service principal (OAuth M2M)
# MAGIC       DATABRICKS_CLIENT_SECRET: ${{ secrets.SP_CLIENT_SECRET }}
# MAGIC     steps:
# MAGIC       - uses: actions/checkout@v4
# MAGIC       - uses: databricks/setup-cli@main
# MAGIC       - run: pip install pytest pyspark && pytest tests/
# MAGIC       - run: databricks bundle validate -t staging
# MAGIC       - run: databricks bundle deploy -t staging && databricks bundle run -t staging integration_tests
# MAGIC       - if: github.ref == 'refs/heads/main'
# MAGIC         run: databricks bundle deploy -t prod
# MAGIC ```
# MAGIC
# MAGIC - **Pull request:** unit tests, `validate`, deploy to **staging**, integration tests.
# MAGIC - **Merge to main:** deploy the **same commit** to **prod**.
# MAGIC - Authenticate CI as a **service principal** with OAuth, or with workload identity federation. Don't use a person's PAT.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decision cheat sheet
# MAGIC
# MAGIC | Situation | Choose |
# MAGIC |---|---|
# MAGIC | Same job in dev, staging and prod with different catalogs | One bundle, **targets** + **variables** |
# MAGIC | Developers must not overwrite each other's jobs | **`mode: development`** (user-prefixed copies) |
# MAGIC | A prod job must not run as the engineer who deployed it | **`run_as`** service principal |
# MAGIC | A job already exists in the UI | `bundle generate job` |
# MAGIC | A password is needed at deploy time | `BUNDLE_VAR_…` from the CI secret store |
# MAGIC | Prod must run exactly the tested code | Build a **wheel artifact** in the bundle |
# MAGIC | Catch config errors before deploying | `bundle validate` in CI |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common mistakes
# MAGIC
# MAGIC - ❌ Recreating jobs by hand in each workspace, or copying job JSON between them.
# MAGIC - ❌ Hard-coded catalog names in code instead of `${var.catalog}`.
# MAGIC - ❌ Expecting a dev deployment to run on schedule. Development mode **pauses** schedules.
# MAGIC - ❌ `mode: development` on the prod target (prefixed names, paused schedules).
# MAGIC - ❌ Editing a bundle-managed job in the UI. The next deploy overwrites it.
# MAGIC - ❌ CI authenticated with a human's personal access token.
# MAGIC
# MAGIC **You finished the Advanced Data Engineering path.**

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
# MAGIC dbxQuiz('dbxquiz', [{"q": "Which file defines a bundle's name, included resource files, variables and targets?", "o": ["bundle.json", "databricks.yml", "pyproject.toml", "requirements.txt"], "c": 1, "e": "databricks.yml at the project root is the bundle configuration. Resources are usually split into files under resources/ and pulled in with include."}, {"q": "After `databricks bundle deploy -t dev`, the scheduled job doesn't start on its schedule. Why?", "o": ["The deploy failed silently", "Development mode pauses schedules and triggers", "Schedules only work with job clusters", "The job must be run with bundle run first"], "c": 1, "e": "mode: development prefixes resource names with [dev user] and pauses schedules, so dev copies don't run on their own."}, {"q": "A CI pipeline must pass a database password to the bundle at deploy time without storing it in Git. What do you use?", "o": ["Write it into databricks.yml under the prod target", "Set it as the BUNDLE_VAR_<name> environment variable from the CI secret store", "Hard-code it in the notebook", "Put it in the variable's default value"], "c": 1, "e": "BUNDLE_VAR_<name> environment variables override bundle variables, and their values can come from the CI system's secret store."}, {"q": "Production jobs must not run with the identity of the engineer who deployed them. Which bundle setting handles this?", "o": ["`mode: development`", "`run_as` with a service principal on the prod target", "`permissions: CAN_VIEW`", "`--force` on deploy"], "c": 1, "e": "run_as sets the identity that jobs and pipelines run with. Production mode validates it, and a service principal is the recommended choice."}, {"q": "In which order should a CI/CD pipeline typically run the bundle commands for a staging deployment?", "o": ["deploy → validate → run", "run → deploy → validate", "validate → deploy → run", "destroy → deploy → validate"], "c": 2, "e": "validate catches configuration errors, deploy creates or updates the resources, and run executes the job, e.g. integration tests, against the deployed version."}]);
# MAGIC </script>
