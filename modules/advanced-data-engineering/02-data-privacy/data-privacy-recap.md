# Recap: Databricks Data Privacy

> **Course 2 of 4 · Advanced Data Engineering with Databricks** · ⏱ ~8 minutes
>
> Protecting personal data across the lakehouse: **who can see what**, how to **de-identify** PII, and how to **really delete** data when a person asks for it.

**The one idea to remember:** put privacy into the **platform**, not into every job. Use Unity Catalog permissions, **row filters and column masks** (ideally driven by **tags**), de-identify early in the medallion flow, and design deletes that also remove old file versions.

## What we covered

| Topic | Key takeaway |
|---|---|
| Governance & regulations | GDPR / CCPA principles: minimization, purpose limitation, the right to be forgotten |
| Securing data in Unity Catalog | Least-privilege grants, groups, service principals, audit logs |
| Fine-grained access | **Dynamic views**, **row filters**, **column masks**, ABAC with **governed tags** |
| De-identification | **Pseudonymization** (hashing, tokenization) vs **anonymization** (generalization, suppression) |
| PII detection | **Data classification** finds PII columns and tags them automatically |
| Deleting data | `DELETE` / `MERGE`, **change data feed** for propagation, `REORG … PURGE`, **`VACUUM`** |

## Least privilege in Unity Catalog

```sql
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO `analysts`;      -- gold only, no raw PII
REVOKE ALL PRIVILEGES ON SCHEMA prod.bronze FROM `analysts`;
SHOW GRANTS ON SCHEMA prod.gold;
```

- Reading a table needs **all three**: `USE CATALOG`, `USE SCHEMA` and `SELECT`.
- Grant to **groups**, run production as **service principals**, and keep raw PII in a **restricted** bronze schema.
- Unity Catalog privileges are **allow-only** (there is no `DENY`), so restrict by granting narrowly.
- **Audit:** `system.access.audit` and **lineage** show who read PII and where it flowed.

## Fine-grained access: who sees which rows and columns

```sql
-- Column mask: only pii_readers see the full email
CREATE FUNCTION prod.sec.mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN email
            ELSE regexp_replace(email, '^[^@]+', '***') END;
ALTER TABLE prod.silver.customers ALTER COLUMN email SET MASK prod.sec.mask_email;

-- Row filter: EU analysts see EU rows only
CREATE FUNCTION prod.sec.eu_only(region STRING)
RETURN is_account_group_member('admins') OR region = 'EU';
ALTER TABLE prod.silver.customers SET ROW FILTER prod.sec.eu_only ON (region);
```

| Technique | How | Best for |
|---|---|---|
| **Dynamic view** | A view with `CASE WHEN is_account_group_member(...)` | Classic approach. Users must query the **view**, not the table |
| **Row filter / column mask** | A function attached **to the table** | The policy travels with the table: batch, streaming, MVs |
| **ABAC policy + governed tags** | One policy on a catalog or schema for every column tagged e.g. `pii = email` | Scale: new tables are protected **without code changes** |

```sql
ALTER TABLE prod.silver.customers ALTER COLUMN email SET TAGS ('pii' = 'email');

CREATE POLICY mask_pii_email ON CATALOG prod
COLUMN MASK prod.sec.mask_email
TO `account users` EXCEPT `pii_readers`
FOR TABLES MATCH COLUMNS has_tag_value('pii', 'email') AS c ON COLUMN c;
```

## De-identification methods

| Method | Example | Reversible? | Still joinable? | Result |
|---|---|---|---|---|
| **Hashing** (salted / keyed) | `sha2(concat(email, secret('pii','salt')), 256)` | No | **Yes** | Pseudonymized |
| **Tokenization** | Random token, mapping kept in a locked **vault table** | **Yes**, with vault access | Yes | Pseudonymized |
| **Generalization** | Age → `30-39`, ZIP → first 3 digits, date → month | No | Partly | Anonymized |
| **Suppression** | Drop or NULL the column | No | No | Anonymized |
| **Masking** | `***-**-1234` at query time | Original still stored | — | Access control |
| Text masking | `ai_mask(comment, ARRAY('email', 'phone'))` | No | — | PII in free text |

- **Pseudonymized** data is still **personal data** under GDPR, because re-identification is possible.
- **Anonymized** data (generalized, suppressed, aggregated) is no longer personal data, provided nobody can be re-identified. Think about **k-anonymity**: every combination of attributes should match at least *k* people.
- Keep salts and keys in **secrets**. A plain `sha2(email)` is guessable.

## A privacy-aware medallion flow

```
landing ─► bronze   raw data incl. PII        restricted schema, very few principals
        ─► silver   hashed / tokenized IDs, PII columns tagged → masks via ABAC
        ─► gold     aggregated / generalized, no direct identifiers → broad access
```

1. **Data classification** scans tables, detects PII (emails, phone numbers, IBANs, names…) and applies **governed tags** automatically.
2. **Tags + ABAC** mask everything tagged, including columns added later.
3. De-identify in the **bronze → silver** step, e.g. with pipeline code or a UDF.
4. Put **expectations** on silver, so the pipeline fails if PII-looking values appear where they shouldn't.

## The right to be forgotten

```sql
-- 1) delete everyone in the request table, starting at bronze
MERGE INTO prod.bronze.customers t
USING (SELECT customer_id FROM prod.ops.gdpr_requests WHERE status = 'pending') r
ON t.customer_id = r.customer_id
WHEN MATCHED THEN DELETE;

-- 2) propagate to silver/gold: DELETE / MERGE, or apply deletes read from the change data feed
-- 3) deletion vectors only *mark* rows → rewrite the files
REORG TABLE prod.bronze.customers APPLY (PURGE);
-- 4) remove old file versions after the retention period (default 7 days)
VACUUM prod.bronze.customers;
```

- Until `VACUUM`, deleted rows are still readable with **time travel** (`VERSION AS OF`).
- **Downstream streaming tables** fail on deletes in their source. Use `skipChangeCommits` plus DML on the target, or the **change data feed**.
- **Upstream copies** (landing files, Kafka, source systems) must be deleted too, or the data comes back on reprocessing.
- **Design for deletion:** keep PII in a separate table keyed by a surrogate ID, so an erasure touches only one small table.
- **Retention rules:** e.g. auto TTL (`DELETE ROWS 365 DAYS AFTER event_ts`) on managed tables with predictive optimization.

## Try it: de-identify, mask, filter, forget

> Uses the synthetic **retail** customers, which include a fake `email`, `birth_year` and `postal_code`. Everything runs in your own schema.

```python run
# ✏️ FILL IN: the catalog.schema you can write to
WORK_SCHEMA = "FILL_IN"   # e.g. "dbacademy.labuser1234567_1700000000"
```

```python run
# Load the retail tables from GitHub
import urllib.request as u; exec(u.urlopen("https://raw.githubusercontent.com/misrori/dbxhelp/main/dbxdata.py").read().decode())
S = load_dataset("retail", WORK_SCHEMA)
```

```python run
# Pseudonymize + generalize: hash the email, bucket the birth year, truncate the postal code
display(spark.sql("""
SELECT customer_id,
       sha2(concat(email, 'demo-salt-keep-real-ones-in-secrets'), 256) AS email_hash,    -- pseudonymized, joinable
       concat(CAST(floor(birth_year / 10) * 10 AS STRING), 's')       AS birth_decade,  -- generalized
       concat(left(postal_code, 2), '**')                             AS postal_area,   -- generalized
       region, segment
FROM dim_customer
LIMIT 10
"""))
```

```python run
# Column mask: only members of 'pii_readers' see full emails
spark.sql(f"""
CREATE OR REPLACE FUNCTION {S}.mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN email
            ELSE regexp_replace(email, '^[^@]+', '***') END
""")
spark.sql(f"ALTER TABLE dim_customer ALTER COLUMN email SET MASK {S}.mask_email")
display(spark.sql("SELECT customer_id, email, region FROM dim_customer LIMIT 5"))
```

```python run
# Row filter: non-admins see EMEA customers only
spark.sql(f"""
CREATE OR REPLACE FUNCTION {S}.emea_only(region STRING)
RETURN is_account_group_member('admins') OR region = 'EMEA'
""")
spark.sql(f"ALTER TABLE dim_customer SET ROW FILTER {S}.emea_only ON (region)")
display(spark.sql("SELECT region, count(*) AS customers FROM dim_customer GROUP BY ALL"))
```

```python run
# Remove the policies again
spark.sql("ALTER TABLE dim_customer DROP ROW FILTER")
spark.sql("ALTER TABLE dim_customer ALTER COLUMN email DROP MASK")
```

```python run
# Right to be forgotten: after DELETE, time travel still shows the row
spark.sql("DELETE FROM dim_customer WHERE customer_id = 42")
before = spark.sql("DESCRIBE HISTORY dim_customer").filter("operation = 'DELETE'").first()["version"] - 1
display(spark.sql(f"SELECT customer_id, email FROM dim_customer VERSION AS OF {before} WHERE customer_id = 42"))
```

```python run
# Physically remove it: purge deletion vectors, then VACUUM (removes files older than the retention, default 7 days)
display(spark.sql("REORG TABLE dim_customer APPLY (PURGE)"))
display(spark.sql("VACUUM dim_customer"))
```

## Decision cheat sheet

| Requirement | Choose |
|---|---|
| Analysts may count unique customers but not see emails | **Salted hashing** |
| Support must re-identify a customer in rare cases | **Tokenization** + vault table |
| Public / partner dataset | **Generalization + suppression** (anonymization) |
| Different people see different detail on the same table | **Column mask** / row filter |
| Hundreds of tables with PII, new ones added weekly | **Data classification + governed tags + ABAC** |
| Customer asks to be forgotten | `DELETE`/`MERGE` in every layer → `REORG … PURGE` → `VACUUM` |
| Propagate deletes from silver to gold | **Change data feed** |

## Common mistakes

- ❌ Calling unsalted `sha2(email)` "anonymization". It is guessable pseudonymization.
- ❌ Running `DELETE` only. Old versions remain via time travel until `VACUUM`, and deletion vectors need `REORG … PURGE` first.
- ❌ Forgetting downstream streaming tables and upstream raw files.
- ❌ Copy-pasting masking logic into every job instead of attaching **policies** to tables and tags.
- ❌ Granting `ALL PRIVILEGES` to make access "just work".

**Next course:** *Databricks Performance Optimization*.

<!-- quiz:start -->
## Check yourself

**1. Analysts must count distinct customers and join datasets by customer, but must never see email addresses. Which technique fits best?**

- a) Suppression (drop the email column)
- b) Salted (keyed) hashing of the email
- c) Generalization of the email domain
- d) Plain sha2(email) without a salt

<details><summary>Answer</summary>

**b)** Salted hashing keeps a stable, joinable identifier without exposing the value. Unsalted hashes of guessable values can be brute-forced, and suppression removes the join key.

</details>

**2. Is pseudonymized data (hashed or tokenized identifiers) still personal data under GDPR?**

- a) No, hashing makes it anonymous
- b) Yes, because re-identification is possible with additional information
- c) Only if the hash is SHA-1
- d) Only for tokenization, not for hashing

<details><summary>Answer</summary>

**b)** Pseudonymization reduces risk, but the data can still be linked back to a person. Only properly anonymized data (e.g. generalized or aggregated) falls outside GDPR.

</details>

**3. New tables with PII columns are added every week, and all of them must be masked automatically. What scales best?**

- a) A dynamic view per table
- b) ALTER TABLE ... SET MASK on every new table
- c) Data classification + governed tags + an ABAC column-mask policy on the catalog
- d) Masking logic inside every pipeline

<details><summary>Answer</summary>

**c)** Classification tags PII columns, and an ABAC policy on the catalog masks every matching column, now and in the future, without code changes.

</details>

**4. A table uses deletion vectors. After deleting a customer for a GDPR request, what must happen before VACUUM physically removes the data?**

- a) OPTIMIZE ZORDER BY customer_id
- b) REORG TABLE ... APPLY (PURGE)
- c) ANALYZE TABLE
- d) Nothing, DELETE rewrites the files immediately

<details><summary>Answer</summary>

**b)** With deletion vectors, DELETE only marks rows. REORG ... APPLY (PURGE) rewrites the files, and VACUUM then removes the old files after the retention period.

</details>

**5. After deleting rows in a silver table, the downstream streaming table that reads it starts failing. What is a correct fix?**

- a) Delete the streaming table's checkpoint
- b) Use skipChangeCommits (and delete in the target with DML), or read the change data feed
- c) Switch the source to complete output mode
- d) Disable deletion vectors

<details><summary>Answer</summary>

**b)** A plain streaming read only supports appends. skipChangeCommits ignores non-append commits, so you apply the delete downstream yourself. The change data feed delivers the deletes as rows you can process.

</details>

<!-- quiz:end -->
