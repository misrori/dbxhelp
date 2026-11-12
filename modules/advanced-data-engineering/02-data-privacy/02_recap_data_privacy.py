# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="font-size: 1em; display: flex; align-items: center; gap: 8px; padding: 8px 16px; background: #F8F9FA; border-bottom: 2px solid #E0E0E0; margin: 0; line-height: 1">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"/>
# MAGIC     <div style="color: #666">
# MAGIC         <span style="font-weight: bold; color: #333">Advanced Data Engineering with Databricks</span>
# MAGIC         <span style="margin-left: 8px; color: #999">|</span>
# MAGIC         <span style="margin-left: 8px">Recap 2: Databricks Data Privacy</span>
# MAGIC     </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Recap: Databricks Data Privacy
# MAGIC
# MAGIC > **Course 2 of 4 · Advanced Data Engineering with Databricks** · ⏱ ~8 minutes
# MAGIC >
# MAGIC > Protecting personal data across the lakehouse: **who can see what**, how to **de-identify** PII, and how to **really delete** data when a person asks for it.
# MAGIC
# MAGIC **The one idea to remember:** put privacy into the **platform**, not into every job. Use Unity Catalog permissions, **row filters and column masks** (ideally driven by **tags**), de-identify early in the medallion flow, and design deletes that also remove old file versions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we covered
# MAGIC
# MAGIC | Topic | Key takeaway |
# MAGIC |---|---|
# MAGIC | Governance & regulations | GDPR / CCPA principles: minimization, purpose limitation, the right to be forgotten |
# MAGIC | Securing data in Unity Catalog | Least-privilege grants, groups, service principals, audit logs |
# MAGIC | Fine-grained access | **Dynamic views**, **row filters**, **column masks**, ABAC with **governed tags** |
# MAGIC | De-identification | **Pseudonymization** (hashing, tokenization) vs **anonymization** (generalization, suppression) |
# MAGIC | PII detection | **Data classification** finds PII columns and tags them automatically |
# MAGIC | Deleting data | `DELETE` / `MERGE`, **change data feed** for propagation, `REORG … PURGE`, **`VACUUM`** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Least privilege in Unity Catalog
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG prod TO `analysts`;
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO `analysts`;      -- gold only, no raw PII
# MAGIC REVOKE ALL PRIVILEGES ON SCHEMA prod.bronze FROM `analysts`;
# MAGIC SHOW GRANTS ON SCHEMA prod.gold;
# MAGIC ```
# MAGIC
# MAGIC - Reading a table needs **all three**: `USE CATALOG`, `USE SCHEMA` and `SELECT`.
# MAGIC - Grant to **groups**, run production as **service principals**, and keep raw PII in a **restricted** bronze schema.
# MAGIC - Unity Catalog privileges are **allow-only** (there is no `DENY`), so restrict by granting narrowly.
# MAGIC - **Audit:** `system.access.audit` and **lineage** show who read PII and where it flowed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fine-grained access: who sees which rows and columns
# MAGIC
# MAGIC ```sql
# MAGIC -- Column mask: only pii_readers see the full email
# MAGIC CREATE FUNCTION prod.sec.mask_email(email STRING)
# MAGIC RETURN CASE WHEN is_account_group_member('pii_readers') THEN email
# MAGIC             ELSE regexp_replace(email, '^[^@]+', '***') END;
# MAGIC ALTER TABLE prod.silver.customers ALTER COLUMN email SET MASK prod.sec.mask_email;
# MAGIC
# MAGIC -- Row filter: EU analysts see EU rows only
# MAGIC CREATE FUNCTION prod.sec.eu_only(region STRING)
# MAGIC RETURN is_account_group_member('admins') OR region = 'EU';
# MAGIC ALTER TABLE prod.silver.customers SET ROW FILTER prod.sec.eu_only ON (region);
# MAGIC ```
# MAGIC
# MAGIC | Technique | How | Best for |
# MAGIC |---|---|---|
# MAGIC | **Dynamic view** | A view with `CASE WHEN is_account_group_member(...)` | Classic approach. Users must query the **view**, not the table |
# MAGIC | **Row filter / column mask** | A function attached **to the table** | The policy travels with the table: batch, streaming, MVs |
# MAGIC | **ABAC policy + governed tags** | One policy on a catalog or schema for every column tagged e.g. `pii = email` | Scale: new tables are protected **without code changes** |
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE prod.silver.customers ALTER COLUMN email SET TAGS ('pii' = 'email');
# MAGIC
# MAGIC CREATE POLICY mask_pii_email ON CATALOG prod
# MAGIC COLUMN MASK prod.sec.mask_email
# MAGIC TO `account users` EXCEPT `pii_readers`
# MAGIC FOR TABLES MATCH COLUMNS has_tag_value('pii', 'email') AS c ON COLUMN c;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## De-identification methods
# MAGIC
# MAGIC | Method | Example | Reversible? | Still joinable? | Result |
# MAGIC |---|---|---|---|---|
# MAGIC | **Hashing** (salted / keyed) | `sha2(concat(email, secret('pii','salt')), 256)` | No | **Yes** | Pseudonymized |
# MAGIC | **Tokenization** | Random token, mapping kept in a locked **vault table** | **Yes**, with vault access | Yes | Pseudonymized |
# MAGIC | **Generalization** | Age → `30-39`, ZIP → first 3 digits, date → month | No | Partly | Anonymized |
# MAGIC | **Suppression** | Drop or NULL the column | No | No | Anonymized |
# MAGIC | **Masking** | `***-**-1234` at query time | Original still stored | — | Access control |
# MAGIC | Text masking | `ai_mask(comment, ARRAY('email', 'phone'))` | No | — | PII in free text |
# MAGIC
# MAGIC - **Pseudonymized** data is still **personal data** under GDPR, because re-identification is possible.
# MAGIC - **Anonymized** data (generalized, suppressed, aggregated) is no longer personal data, provided nobody can be re-identified. Think about **k-anonymity**: every combination of attributes should match at least *k* people.
# MAGIC - Keep salts and keys in **secrets**. A plain `sha2(email)` is guessable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A privacy-aware medallion flow
# MAGIC
# MAGIC ```
# MAGIC landing ─► bronze   raw data incl. PII        restricted schema, very few principals
# MAGIC         ─► silver   hashed / tokenized IDs, PII columns tagged → masks via ABAC
# MAGIC         ─► gold     aggregated / generalized, no direct identifiers → broad access
# MAGIC ```
# MAGIC
# MAGIC 1. **Data classification** scans tables, detects PII (emails, phone numbers, IBANs, names…) and applies **governed tags** automatically.
# MAGIC 2. **Tags + ABAC** mask everything tagged, including columns added later.
# MAGIC 3. De-identify in the **bronze → silver** step, e.g. with pipeline code or a UDF.
# MAGIC 4. Put **expectations** on silver, so the pipeline fails if PII-looking values appear where they shouldn't.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The right to be forgotten
# MAGIC
# MAGIC ```sql
# MAGIC -- 1) delete everyone in the request table, starting at bronze
# MAGIC MERGE INTO prod.bronze.customers t
# MAGIC USING (SELECT customer_id FROM prod.ops.gdpr_requests WHERE status = 'pending') r
# MAGIC ON t.customer_id = r.customer_id
# MAGIC WHEN MATCHED THEN DELETE;
# MAGIC
# MAGIC -- 2) propagate to silver/gold: DELETE / MERGE, or apply deletes read from the change data feed
# MAGIC -- 3) deletion vectors only *mark* rows → rewrite the files
# MAGIC REORG TABLE prod.bronze.customers APPLY (PURGE);
# MAGIC -- 4) remove old file versions after the retention period (default 7 days)
# MAGIC VACUUM prod.bronze.customers;
# MAGIC ```
# MAGIC
# MAGIC - Until `VACUUM`, deleted rows are still readable with **time travel** (`VERSION AS OF`).
# MAGIC - **Downstream streaming tables** fail on deletes in their source. Use `skipChangeCommits` plus DML on the target, or the **change data feed**.
# MAGIC - **Upstream copies** (landing files, Kafka, source systems) must be deleted too, or the data comes back on reprocessing.
# MAGIC - **Design for deletion:** keep PII in a separate table keyed by a surrogate ID, so an erasure touches only one small table.
# MAGIC - **Retention rules:** e.g. auto TTL (`DELETE ROWS 365 DAYS AFTER event_ts`) on managed tables with predictive optimization.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decision cheat sheet
# MAGIC
# MAGIC | Requirement | Choose |
# MAGIC |---|---|
# MAGIC | Analysts may count unique customers but not see emails | **Salted hashing** |
# MAGIC | Support must re-identify a customer in rare cases | **Tokenization** + vault table |
# MAGIC | Public / partner dataset | **Generalization + suppression** (anonymization) |
# MAGIC | Different people see different detail on the same table | **Column mask** / row filter |
# MAGIC | Hundreds of tables with PII, new ones added weekly | **Data classification + governed tags + ABAC** |
# MAGIC | Customer asks to be forgotten | `DELETE`/`MERGE` in every layer → `REORG … PURGE` → `VACUUM` |
# MAGIC | Propagate deletes from silver to gold | **Change data feed** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common mistakes
# MAGIC
# MAGIC - ❌ Calling unsalted `sha2(email)` "anonymization". It is guessable pseudonymization.
# MAGIC - ❌ Running `DELETE` only. Old versions remain via time travel until `VACUUM`, and deletion vectors need `REORG … PURGE` first.
# MAGIC - ❌ Forgetting downstream streaming tables and upstream raw files.
# MAGIC - ❌ Copy-pasting masking logic into every job instead of attaching **policies** to tables and tags.
# MAGIC - ❌ Granting `ALL PRIVILEGES` to make access "just work".
# MAGIC
# MAGIC **Next course:** *Databricks Performance Optimization*.

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
# MAGIC dbxQuiz('dbxquiz', [{"q": "Analysts must count distinct customers and join datasets by customer, but must never see email addresses. Which technique fits best?", "o": ["Suppression (drop the email column)", "Salted (keyed) hashing of the email", "Generalization of the email domain", "Plain sha2(email) without a salt"], "c": 1, "e": "Salted hashing keeps a stable, joinable identifier without exposing the value. Unsalted hashes of guessable values can be brute-forced, and suppression removes the join key."}, {"q": "Is pseudonymized data (hashed or tokenized identifiers) still personal data under GDPR?", "o": ["No, hashing makes it anonymous", "Yes, because re-identification is possible with additional information", "Only if the hash is SHA-1", "Only for tokenization, not for hashing"], "c": 1, "e": "Pseudonymization reduces risk, but the data can still be linked back to a person. Only properly anonymized data (e.g. generalized or aggregated) falls outside GDPR."}, {"q": "New tables with PII columns are added every week, and all of them must be masked automatically. What scales best?", "o": ["A dynamic view per table", "ALTER TABLE ... SET MASK on every new table", "Data classification + governed tags + an ABAC column-mask policy on the catalog", "Masking logic inside every pipeline"], "c": 2, "e": "Classification tags PII columns, and an ABAC policy on the catalog masks every matching column, now and in the future, without code changes."}, {"q": "A table uses deletion vectors. After deleting a customer for a GDPR request, what must happen before VACUUM physically removes the data?", "o": ["OPTIMIZE ZORDER BY customer_id", "REORG TABLE ... APPLY (PURGE)", "ANALYZE TABLE", "Nothing, DELETE rewrites the files immediately"], "c": 1, "e": "With deletion vectors, DELETE only marks rows. REORG ... APPLY (PURGE) rewrites the files, and VACUUM then removes the old files after the retention period."}, {"q": "After deleting rows in a silver table, the downstream streaming table that reads it starts failing. What is a correct fix?", "o": ["Delete the streaming table's checkpoint", "Use skipChangeCommits (and delete in the target with DML), or read the change data feed", "Switch the source to complete output mode", "Disable deletion vectors"], "c": 1, "e": "A plain streaming read only supports appends. skipChangeCommits ignores non-append commits, so you apply the delete downstream yourself. The change data feed delivers the deletes as rows you can process."}]);
# MAGIC </script>
