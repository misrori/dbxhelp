# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Data Engineering with Databricks</span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">Recap 2: Deploy Workloads with Lakeflow Jobs</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Recap: Deploy Workloads with Lakeflow Jobs
# MAGIC
# MAGIC > **Course 2 of 4 · Data Engineering with Databricks** · ⏱ ~8 minutes
# MAGIC >
# MAGIC > Turning notebooks, SQL and pipelines into **reliable, scheduled, observable** production workloads.
# MAGIC
# MAGIC **The one idea to remember:** a job is a **DAG of tasks**. Dependencies, triggers, parameters and conditions decide *what* runs and *when*. Repair runs and notifications keep it healthy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we covered
# MAGIC
# MAGIC | Lesson | Key takeaway |
# MAGIC |---|---|
# MAGIC | 1 Creating a job | Single notebook task in the **Jobs UI**: configure, run, review the run |
# MAGIC | 2 Lab: multiple tasks | Several tasks with **dependencies**, building a DAG |
# MAGIC | 3 Scheduling options | **Scheduled**, **file arrival**, **continuous** triggers, plus **task parameters** |
# MAGIC | 4 Controlling the flow | **Run if** conditions, **repair runs**, **task values**, the **If/else** task |
# MAGIC | 5 Lab: modular orchestration | **Run Job** task: a parent ("master") job calling other jobs |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Jobs, tasks and the DAG
# MAGIC
# MAGIC ```
# MAGIC Ingest_Source_1 ─┐
# MAGIC Ingest_Source_2 ─┼─► Clean_Data ─► If/else: bad_records > 0 ─true──► Fix_Bad_Records
# MAGIC Ingest_Source_3 ─┘                                           └false─► Aggregate_Records
# MAGIC ```
# MAGIC
# MAGIC | Building block | What it is |
# MAGIC |---|---|
# MAGIC | **Job** | A named workflow with triggers, parameters, notifications and permissions |
# MAGIC | **Task** | One unit of work: notebook, Python script/wheel, **SQL**, **dashboard**, **pipeline**, dbt, **Run Job**, **If/else**, **For each** |
# MAGIC | **Dependency** | "Depends on" edges between tasks. Tasks without dependencies run **in parallel** |
# MAGIC | **Compute** | **Serverless** (recommended), a **job cluster** that tasks of the job can share, or a **SQL warehouse** for SQL and dashboard tasks |
# MAGIC
# MAGIC Job clusters are created for the run and terminated afterwards. They are cheaper than all-purpose clusters, which are meant for interactive development.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Triggers: when does a job run?
# MAGIC
# MAGIC | Trigger | Starts a run | Use when |
# MAGIC |---|---|---|
# MAGIC | **Manual / API** | *Run now*, REST API, another orchestrator | Ad hoc runs, testing |
# MAGIC | **Scheduled** | Cron-style schedule with a time zone (UI or cron syntax) | Data arrives at **known times**; reports due at fixed times |
# MAGIC | **File arrival** | New files in a UC **volume** or **external location** (checked about every minute; use **Test trigger** to verify the path) | Files arrive at **unpredictable** times |
# MAGIC | **Table update** | One or more UC tables are updated | Run right after an upstream team's table changes |
# MAGIC | **Continuous** | A new run starts as soon as the previous one ends | Always-on streaming workloads |
# MAGIC
# MAGIC Data-driven triggers (file arrival, table update) avoid **empty runs** and runs that start **too early**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters and dynamic values
# MAGIC
# MAGIC ```python
# MAGIC # In the task notebook, read a task/job parameter like a widget
# MAGIC catalog = dbutils.widgets.get("catalog")
# MAGIC
# MAGIC # Pass a value to downstream tasks
# MAGIC dbutils.jobs.taskValues.set(key="bad_records", value=5)
# MAGIC ```
# MAGIC
# MAGIC | Kind | Example | Scope |
# MAGIC |---|---|---|
# MAGIC | **Job parameters** | `env = prod`, referenced as `{{job.parameters.env}}` | Pushed down to every task |
# MAGIC | **Task parameters** | `catalog = dbacademy` | One task |
# MAGIC | **Dynamic value references** | `{{job.id}}`, `{{job.run_id}}`, `{{job.start_time.iso_date}}`, `{{tasks.Clean_Data.values.bad_records}}` | Resolved at run time |
# MAGIC | **Task values** | `dbutils.jobs.taskValues.set / get` | Pass small results between tasks |
# MAGIC
# MAGIC Parameters make one notebook reusable across catalogs, environments and dates. Never hard-code them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Controlling the flow
# MAGIC
# MAGIC **Run if dependencies:** when should a task with upstream tasks start?
# MAGIC
# MAGIC | Run if | Runs when | Typical use |
# MAGIC |---|---|---|
# MAGIC | **All succeeded** (default) | every upstream task succeeded | Normal flow |
# MAGIC | At least one succeeded | ≥ 1 succeeded | Partial-source processing |
# MAGIC | None failed | nothing failed | Skip-tolerant flows |
# MAGIC | **All done** | everything finished, whatever the result | **Cleanup**, final notification |
# MAGIC | At least one failed / All failed | something (everything) failed | **Alerting**, fallback |
# MAGIC
# MAGIC **If/else condition task:** branch on a value, e.g. `{{tasks.Clean_Data.values.bad_records}} > 0`. The *true* branch fixes the bad records, and the *false* branch goes straight to aggregation.
# MAGIC
# MAGIC **For each task:** run a nested task once per item of a list (e.g. per country), with a **concurrency** setting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## When things fail
# MAGIC
# MAGIC - **Repair run:** after a failure, re-run **only the failed tasks and their dependents**. Successful tasks are not executed again, which saves time and money.
# MAGIC - **Retries:** per task, a retry count and interval. Retries are off by default for most tasks.
# MAGIC - **Timeout:** the maximum duration of a task. When exceeded, the status is *Timed Out*. With retries, the timeout applies to each attempt.
# MAGIC - **Duration threshold:** sends a **warning** only and does not stop the task.
# MAGIC - **Notifications:** email, Slack or webhooks on start, success, failure or duration warning, at job or task level.
# MAGIC - **Max concurrent runs:** the default is **1**. Extra triggered runs are skipped unless **queueing** is enabled.
# MAGIC - Task statuses to recognise: *Succeeded*, *Failed*, ***Upstream failed*** (a dependency failed), *Excluded* (condition not met), *Timed Out*, *Skipped*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modular orchestration: Run Job task
# MAGIC
# MAGIC ```
# MAGIC Master_Job
# MAGIC  ├─ Run Job: Ingestion_Job ─┐
# MAGIC  ├─ Run Job: Reference_Job ─┼─► Run Job: Reporting_Job
# MAGIC ```
# MAGIC
# MAGIC - Split large workflows into **smaller, reusable jobs** owned by different people or teams.
# MAGIC - The parent job controls the order and passes parameters down.
# MAGIC - Each child job keeps its own run history, permissions and compute settings.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decision cheat sheet
# MAGIC
# MAGIC | Situation | Choose |
# MAGIC |---|---|
# MAGIC | Files land at random times during the day | **File arrival** trigger, not a 5-minute schedule |
# MAGIC | Downstream job must run after another team's table is refreshed | **Table update** trigger |
# MAGIC | A cleanup task must always run | Run if **All done** |
# MAGIC | Only the third of five tasks failed | **Repair run** |
# MAGIC | Different logic when bad records are found | **If/else** task with a **task value** |
# MAGIC | Same notebook for 12 countries | **For each** task with parameters |
# MAGIC | A 40-task workflow maintained by three teams | Several jobs combined with **Run Job** tasks |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common mistakes
# MAGIC
# MAGIC - ❌ Raising the duration **warning** threshold to fix *Timed Out* failures. It's the **timeout** that stops the task.
# MAGIC - ❌ Re-running the whole job after one task failed. Use **repair run**.
# MAGIC - ❌ Running production jobs on an **all-purpose** cluster. Use **serverless** or a **job cluster**.
# MAGIC - ❌ Hard-coding catalog or schema names in task notebooks. Use **parameters**.
# MAGIC - ❌ Expecting overlapping runs: max concurrent runs is **1** by default.
# MAGIC
# MAGIC **Next course:** *Build Data Pipelines with Lakeflow Spark Declarative Pipelines*, the pipelines these jobs will trigger.

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
# MAGIC dbxQuiz('dbxquiz', [{"q": "Files from a partner arrive in a Unity Catalog volume at unpredictable times. How should the processing job be triggered?", "o": ["A schedule every 5 minutes", "A continuous trigger", "A file arrival trigger on the volume", "Manually by an operator"], "c": 2, "e": "A file arrival trigger starts the job only when new files appear. There are no empty runs and no delay waiting for the next schedule."}, {"q": "In a 5-task job, only task 3 failed after tasks 1 and 2 succeeded. What is the most efficient way to finish the run after fixing the bug?", "o": ["Run the whole job again", "Use Repair run to re-run the failed task and its dependents", "Clone the job and run only task 3", "Increase the retry count and wait"], "c": 1, "e": "Repair run re-executes only the failed tasks and their downstream dependents. Successful tasks are not repeated."}, {"q": "A cleanup task must run at the end of every job run, even when upstream tasks fail. Which Run if condition do you set?", "o": ["All succeeded", "At least one succeeded", "None failed", "All done"], "c": 3, "e": "'All done' runs once all upstream tasks have finished, whatever their result. That makes it the right choice for cleanup and final notifications."}, {"q": "The notebook of task Clean_Data calls `dbutils.jobs.taskValues.set(key='bad_records', value=5)`. How does a downstream If/else task reference this value?", "o": ["`{{job.parameters.bad_records}}`", "`{{tasks.Clean_Data.values.bad_records}}`", "`dbutils.widgets.get('bad_records')`", "`${bad_records}`"], "c": 1, "e": "Task values are referenced with `{{tasks.<task_key>.values.<key>}}` in downstream tasks, including If/else conditions."}, {"q": "A task keeps ending with status 'Timed Out' although its runs are legitimately getting longer. What should you change?", "o": ["The duration warning threshold", "The task timeout", "Max concurrent runs", "The notification settings"], "c": 1, "e": "The timeout stops the task. The duration threshold only sends a warning and never stops it."}]);
# MAGIC </script>
