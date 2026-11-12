# Recap: Automated Deployment with Databricks Asset Bundles

> **Course 4 of 4 · Advanced Data Engineering with Databricks** · ⏱ ~9 minutes
>
> Describing jobs, pipelines and code **as YAML in Git**, and deploying the **same project** to dev, test and prod with the Databricks CLI and a CI/CD pipeline. (Bundles are now also called *Declarative Automation Bundles*.)

**The one idea to remember:** one bundle, many **targets**. The code and resource definitions never change between environments, only **variables and target overrides** do. CI runs `validate` → `deploy` → `run` for you.

## What we covered

| Topic | Key takeaway |
|---|---|
| DevOps & CI/CD review | Why manual UI deployments don't scale |
| What a bundle is | A project folder with `databricks.yml`: **resources** + **targets** + **variables** |
| Bundle configuration | `include`, resource YAML files, substitutions, **deployment modes** |
| Deploying with the CLI | `bundle init` · `validate` · `deploy` · `run` · `summary` · `destroy` |
| Templates & project structure | `src/`, `resources/`, `tests/`, building **wheels** as artifacts |
| CI/CD automation | GitHub Actions running tests and bundle commands as a **service principal** |

## Bundle anatomy

```
orders_etl/
├── databricks.yml            # bundle name, includes, variables, targets
├── resources/
│   ├── orders.job.yml        # one file per job / pipeline
│   └── orders.pipeline.yml
├── src/                      # notebooks, pipeline sources, Python package
├── tests/                    # pytest unit tests
└── pyproject.toml            # optional: build a wheel
```

```yaml
# databricks.yml
bundle:
  name: orders_etl

include:
  - resources/*.yml

variables:
  catalog:
    description: Target catalog
    default: dev

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dev.cloud.databricks.com
  prod:
    mode: production
    workspace:
      host: https://prod.cloud.databricks.com
    variables:
      catalog: prod
    run_as:
      service_principal_name: 6f1c0000-aaaa-bbbb-cccc-000000000000
```

## Resources: jobs and pipelines as YAML

```yaml
# resources/orders.job.yml
resources:
  jobs:
    orders_job:
      name: orders-etl-${bundle.target}
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ../src/ingest.py
            base_parameters:
              catalog: ${var.catalog}
        - task_key: transform
          depends_on: [{ task_key: ingest }]
          pipeline_task:
            pipeline_id: ${resources.pipelines.orders_pipeline.id}
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: UTC
```

- **Resource types:** jobs, pipelines, dashboards, schemas, volumes, models, serving endpoints, apps…
- **Substitutions:** `${var.catalog}`, `${bundle.target}`, `${workspace.current_user.userName}`, `${resources.pipelines.orders_pipeline.id}`.
- **Existing UI job → YAML:** `databricks bundle generate job --existing-job-id 123`.

## Variables and overrides

The value of a variable is resolved in this order of precedence (highest first):

1. `--var="catalog=prod"` on the CLI
2. `BUNDLE_VAR_catalog` environment variable (great for CI secrets)
3. `targets.<target>.variables`
4. The variable's `default`

- A variable can be a **lookup**, which resolves an ID by name: `lookup: { warehouse: "Shared SQL" }`.
- **Never** put credentials in `databricks.yml`. Inject them through `BUNDLE_VAR_…`, or use secret scopes.
- Bundle variables are **deployment-time** values. Values that change **per run** belong in **job parameters**.

## Deployment modes

| `mode: development` | `mode: production` |
|---|---|
| Resource names prefixed **`[dev <user>]`**, so each developer gets an isolated copy | Names as written |
| **Schedules and triggers paused** | Schedules active |
| Concurrent runs allowed, deployment lock off | Validates `run_as`, permissions, and that pipelines are not in development mode |
| Fast iteration (e.g. `--cluster-id` override) | Cluster overrides not allowed |

## The CLI lifecycle

```bash
databricks bundle init default-python      # start from a template
databricks bundle validate -t dev          # check syntax, references, variables
databricks bundle deploy   -t dev          # upload files + create/update resources (idempotent)
databricks bundle run      -t dev orders_job
databricks bundle summary  -t dev          # what is deployed, with URLs
databricks bundle destroy  -t dev          # remove everything this bundle deployed to dev
```

- Deploys are **declarative**: a resource removed from the YAML is removed from the workspace on the next deploy, and manual UI edits are overwritten.
- **Artifacts:** the bundle can build a **wheel** (`artifacts:` with `build: uv build --wheel`) and attach it to tasks, so prod runs exactly the tested version.

## CI/CD with GitHub Actions

```yaml
# .github/workflows/deploy.yml (simplified)
on:
  pull_request:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ secrets.SP_CLIENT_ID }}          # service principal (OAuth M2M)
      DATABRICKS_CLIENT_SECRET: ${{ secrets.SP_CLIENT_SECRET }}
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: pip install pytest pyspark && pytest tests/
      - run: databricks bundle validate -t staging
      - run: databricks bundle deploy -t staging && databricks bundle run -t staging integration_tests
      - if: github.ref == 'refs/heads/main'
        run: databricks bundle deploy -t prod
```

- **Pull request:** unit tests, `validate`, deploy to **staging**, integration tests.
- **Merge to main:** deploy the **same commit** to **prod**.
- Authenticate CI as a **service principal** with OAuth, or with workload identity federation. Don't use a person's PAT.

## Decision cheat sheet

| Situation | Choose |
|---|---|
| Same job in dev, staging and prod with different catalogs | One bundle, **targets** + **variables** |
| Developers must not overwrite each other's jobs | **`mode: development`** (user-prefixed copies) |
| A prod job must not run as the engineer who deployed it | **`run_as`** service principal |
| A job already exists in the UI | `bundle generate job` |
| A password is needed at deploy time | `BUNDLE_VAR_…` from the CI secret store |
| Prod must run exactly the tested code | Build a **wheel artifact** in the bundle |
| Catch config errors before deploying | `bundle validate` in CI |

## Common mistakes

- ❌ Recreating jobs by hand in each workspace, or copying job JSON between them.
- ❌ Hard-coded catalog names in code instead of `${var.catalog}`.
- ❌ Expecting a dev deployment to run on schedule. Development mode **pauses** schedules.
- ❌ `mode: development` on the prod target (prefixed names, paused schedules).
- ❌ Editing a bundle-managed job in the UI. The next deploy overwrites it.
- ❌ CI authenticated with a human's personal access token.

**You finished the Advanced Data Engineering path.**

<!-- quiz:start -->
## Check yourself

**1. Which file defines a bundle's name, included resource files, variables and targets?**

- a) bundle.json
- b) databricks.yml
- c) pyproject.toml
- d) requirements.txt

<details><summary>Answer</summary>

**b)** databricks.yml at the project root is the bundle configuration. Resources are usually split into files under resources/ and pulled in with include.

</details>

**2. After `databricks bundle deploy -t dev`, the scheduled job doesn't start on its schedule. Why?**

- a) The deploy failed silently
- b) Development mode pauses schedules and triggers
- c) Schedules only work with job clusters
- d) The job must be run with bundle run first

<details><summary>Answer</summary>

**b)** mode: development prefixes resource names with [dev user] and pauses schedules, so dev copies don't run on their own.

</details>

**3. A CI pipeline must pass a database password to the bundle at deploy time without storing it in Git. What do you use?**

- a) Write it into databricks.yml under the prod target
- b) Set it as the BUNDLE_VAR_<name> environment variable from the CI secret store
- c) Hard-code it in the notebook
- d) Put it in the variable's default value

<details><summary>Answer</summary>

**b)** BUNDLE_VAR_<name> environment variables override bundle variables, and their values can come from the CI system's secret store.

</details>

**4. Production jobs must not run with the identity of the engineer who deployed them. Which bundle setting handles this?**

- a) `mode: development`
- b) `run_as` with a service principal on the prod target
- c) `permissions: CAN_VIEW`
- d) `--force` on deploy

<details><summary>Answer</summary>

**b)** run_as sets the identity that jobs and pipelines run with. Production mode validates it, and a service principal is the recommended choice.

</details>

**5. In which order should a CI/CD pipeline typically run the bundle commands for a staging deployment?**

- a) deploy → validate → run
- b) run → deploy → validate
- c) validate → deploy → run
- d) destroy → deploy → validate

<details><summary>Answer</summary>

**c)** validate catches configuration errors, deploy creates or updates the resources, and run executes the job, e.g. integration tests, against the deployed version.

</details>

<!-- quiz:end -->
