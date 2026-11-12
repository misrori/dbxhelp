# Recap: Deploy Workloads with Lakeflow Jobs

> **Course 2 of 4 · Data Engineering with Databricks** · ⏱ ~8 minutes
>
> Turning notebooks, SQL and pipelines into **reliable, scheduled, observable** production workloads.

**The one idea to remember:** a job is a **DAG of tasks**. Dependencies, triggers, parameters and conditions decide *what* runs and *when*. Repair runs and notifications keep it healthy.

## What we covered

| Lesson | Key takeaway |
|---|---|
| 1 Creating a job | Single notebook task in the **Jobs UI**: configure, run, review the run |
| 2 Lab: multiple tasks | Several tasks with **dependencies**, building a DAG |
| 3 Scheduling options | **Scheduled**, **file arrival**, **continuous** triggers, plus **task parameters** |
| 4 Controlling the flow | **Run if** conditions, **repair runs**, **task values**, the **If/else** task |
| 5 Lab: modular orchestration | **Run Job** task: a parent ("master") job calling other jobs |

## Jobs, tasks and the DAG

```
Ingest_Source_1 ─┐
Ingest_Source_2 ─┼─► Clean_Data ─► If/else: bad_records > 0 ─true──► Fix_Bad_Records
Ingest_Source_3 ─┘                                           └false─► Aggregate_Records
```

| Building block | What it is |
|---|---|
| **Job** | A named workflow with triggers, parameters, notifications and permissions |
| **Task** | One unit of work: notebook, Python script/wheel, **SQL**, **dashboard**, **pipeline**, dbt, **Run Job**, **If/else**, **For each** |
| **Dependency** | "Depends on" edges between tasks. Tasks without dependencies run **in parallel** |
| **Compute** | **Serverless** (recommended), a **job cluster** that tasks of the job can share, or a **SQL warehouse** for SQL and dashboard tasks |

Job clusters are created for the run and terminated afterwards. They are cheaper than all-purpose clusters, which are meant for interactive development.

## Triggers: when does a job run?

| Trigger | Starts a run | Use when |
|---|---|---|
| **Manual / API** | *Run now*, REST API, another orchestrator | Ad hoc runs, testing |
| **Scheduled** | Cron-style schedule with a time zone (UI or cron syntax) | Data arrives at **known times**; reports due at fixed times |
| **File arrival** | New files in a UC **volume** or **external location** (checked about every minute; use **Test trigger** to verify the path) | Files arrive at **unpredictable** times |
| **Table update** | One or more UC tables are updated | Run right after an upstream team's table changes |
| **Continuous** | A new run starts as soon as the previous one ends | Always-on streaming workloads |

Data-driven triggers (file arrival, table update) avoid **empty runs** and runs that start **too early**.

## Parameters and dynamic values

```python
# In the task notebook, read a task/job parameter like a widget
catalog = dbutils.widgets.get("catalog")

# Pass a value to downstream tasks
dbutils.jobs.taskValues.set(key="bad_records", value=5)
```

| Kind | Example | Scope |
|---|---|---|
| **Job parameters** | `env = prod`, referenced as `{{job.parameters.env}}` | Pushed down to every task |
| **Task parameters** | `catalog = dbacademy` | One task |
| **Dynamic value references** | `{{job.id}}`, `{{job.run_id}}`, `{{job.start_time.iso_date}}`, `{{tasks.Clean_Data.values.bad_records}}` | Resolved at run time |
| **Task values** | `dbutils.jobs.taskValues.set / get` | Pass small results between tasks |

Parameters make one notebook reusable across catalogs, environments and dates. Never hard-code them.

## Controlling the flow

**Run if dependencies:** when should a task with upstream tasks start?

| Run if | Runs when | Typical use |
|---|---|---|
| **All succeeded** (default) | every upstream task succeeded | Normal flow |
| At least one succeeded | ≥ 1 succeeded | Partial-source processing |
| None failed | nothing failed | Skip-tolerant flows |
| **All done** | everything finished, whatever the result | **Cleanup**, final notification |
| At least one failed / All failed | something (everything) failed | **Alerting**, fallback |

**If/else condition task:** branch on a value, e.g. `{{tasks.Clean_Data.values.bad_records}} > 0`. The *true* branch fixes the bad records, and the *false* branch goes straight to aggregation.

**For each task:** run a nested task once per item of a list (e.g. per country), with a **concurrency** setting.

## When things fail

- **Repair run:** after a failure, re-run **only the failed tasks and their dependents**. Successful tasks are not executed again, which saves time and money.
- **Retries:** per task, a retry count and interval. Retries are off by default for most tasks.
- **Timeout:** the maximum duration of a task. When exceeded, the status is *Timed Out*. With retries, the timeout applies to each attempt.
- **Duration threshold:** sends a **warning** only and does not stop the task.
- **Notifications:** email, Slack or webhooks on start, success, failure or duration warning, at job or task level.
- **Max concurrent runs:** the default is **1**. Extra triggered runs are skipped unless **queueing** is enabled.
- Task statuses to recognise: *Succeeded*, *Failed*, ***Upstream failed*** (a dependency failed), *Excluded* (condition not met), *Timed Out*, *Skipped*.

## Modular orchestration: Run Job task

```
Master_Job
 ├─ Run Job: Ingestion_Job ─┐
 ├─ Run Job: Reference_Job ─┼─► Run Job: Reporting_Job
```

- Split large workflows into **smaller, reusable jobs** owned by different people or teams.
- The parent job controls the order and passes parameters down.
- Each child job keeps its own run history, permissions and compute settings.

## Decision cheat sheet

| Situation | Choose |
|---|---|
| Files land at random times during the day | **File arrival** trigger, not a 5-minute schedule |
| Downstream job must run after another team's table is refreshed | **Table update** trigger |
| A cleanup task must always run | Run if **All done** |
| Only the third of five tasks failed | **Repair run** |
| Different logic when bad records are found | **If/else** task with a **task value** |
| Same notebook for 12 countries | **For each** task with parameters |
| A 40-task workflow maintained by three teams | Several jobs combined with **Run Job** tasks |

## Common mistakes

- ❌ Raising the duration **warning** threshold to fix *Timed Out* failures. It's the **timeout** that stops the task.
- ❌ Re-running the whole job after one task failed. Use **repair run**.
- ❌ Running production jobs on an **all-purpose** cluster. Use **serverless** or a **job cluster**.
- ❌ Hard-coding catalog or schema names in task notebooks. Use **parameters**.
- ❌ Expecting overlapping runs: max concurrent runs is **1** by default.

**Next course:** *Build Data Pipelines with Lakeflow Spark Declarative Pipelines*, the pipelines these jobs will trigger.

<!-- quiz:start -->
## Check yourself

**1. Files from a partner arrive in a Unity Catalog volume at unpredictable times. How should the processing job be triggered?**

- a) A schedule every 5 minutes
- b) A continuous trigger
- c) A file arrival trigger on the volume
- d) Manually by an operator

<details><summary>Answer</summary>

**c)** A file arrival trigger starts the job only when new files appear. There are no empty runs and no delay waiting for the next schedule.

</details>

**2. In a 5-task job, only task 3 failed after tasks 1 and 2 succeeded. What is the most efficient way to finish the run after fixing the bug?**

- a) Run the whole job again
- b) Use Repair run to re-run the failed task and its dependents
- c) Clone the job and run only task 3
- d) Increase the retry count and wait

<details><summary>Answer</summary>

**b)** Repair run re-executes only the failed tasks and their downstream dependents. Successful tasks are not repeated.

</details>

**3. A cleanup task must run at the end of every job run, even when upstream tasks fail. Which Run if condition do you set?**

- a) All succeeded
- b) At least one succeeded
- c) None failed
- d) All done

<details><summary>Answer</summary>

**d)** 'All done' runs once all upstream tasks have finished, whatever their result. That makes it the right choice for cleanup and final notifications.

</details>

**4. The notebook of task Clean_Data calls `dbutils.jobs.taskValues.set(key='bad_records', value=5)`. How does a downstream If/else task reference this value?**

- a) `{{job.parameters.bad_records}}`
- b) `{{tasks.Clean_Data.values.bad_records}}`
- c) `dbutils.widgets.get('bad_records')`
- d) `${bad_records}`

<details><summary>Answer</summary>

**b)** Task values are referenced with `{{tasks.<task_key>.values.<key>}}` in downstream tasks, including If/else conditions.

</details>

**5. A task keeps ending with status 'Timed Out' although its runs are legitimately getting longer. What should you change?**

- a) The duration warning threshold
- b) The task timeout
- c) Max concurrent runs
- d) The notification settings

<details><summary>Answer</summary>

**b)** The timeout stops the task. The duration threshold only sends a warning and never stops it.

</details>

<!-- quiz:end -->
