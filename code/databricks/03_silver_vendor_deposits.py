# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Vendor Deposits
# MAGIC
# MAGIC Bronze → Silver for the vendor deposit feed.
# MAGIC
# MAGIC Handles four defects present in the delivered files:
# MAGIC
# MAGIC | Defect | Where | Handling |
# MAGIC |---|---|---|
# MAGIC | Schema drift `payment_method` → `method` | `deposits_vendor_20240302.csv` | Declared alias map |
# MAGIC | Cross-file duplicates `VDEP002`, `VDEP005` | 0301 and 0302 | Dedupe on `deposit_id`, last file wins |
# MAGIC | Negative amount `VDEP001` = -250.00 | 0301 | QUARANTINE |
# MAGIC | Orphan client `VDEP020` → `CL099` | 0303 | QUARANTINE, auto-released later |
# MAGIC
# MAGIC Aliases are **declared, never inferred**. An unmapped column is a BLOCK-severity
# MAGIC failure that aborts the batch — silently dropping a column is how a feed quietly
# MAGIC loses a field for months.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import uuid

CATALOG = "workspace"
BRONZE = f"{CATALOG}.deriv_assement_bronze"
SILVER = f"{CATALOG}.deriv_assement_silver"

RUN_ID = str(uuid.uuid4())

# Declared alias map. Add to this deliberately, in code review — never infer.
COLUMN_ALIASES = {"method": "payment_method", "payment_type": "payment_method", "amount": "amount_usd"}

CANONICAL = ["deposit_id", "client_id", "deposit_date", "amount_usd", "payment_method",
             "currency_original", "exchange_rate", "status", "processing_days", "fee_usd"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Bronze and normalise schema drift

# COMMAND ----------

raw = spark.table(f"{BRONZE}.stream_client_deposits")

present = set(raw.columns)

# BLOCK check: any source column that is neither canonical nor a declared alias.
unmapped = [c for c in present
            if not c.startswith("_") and c not in CANONICAL and c not in COLUMN_ALIASES
            and c not in ("delta_created_ts", "delta_created_dt", "_rescued_data")]
if unmapped:
    raise Exception(
        f"[BLOCK] Unmapped columns in vendor feed: {unmapped}. "
        f"Batch aborted before any Silver write. Declare an alias or extend the contract."
    )

# Apply aliases. coalesce() lets a single pass handle both file layouts, because
# Auto Loader's schema evolution leaves BOTH payment_method and method present,
# each null for the files that did not carry it.
for alias, canon in COLUMN_ALIASES.items():
    if alias in present:
        raw = (raw.withColumn(canon, F.coalesce(F.col(canon), F.col(alias)))
               if canon in present else raw.withColumnRenamed(alias, canon))

normalised = (
    raw.select(
        F.col("deposit_id").cast("string"),
        F.col("client_id").cast("string"),
        F.col("deposit_date").cast("date"),
        F.col("amount_usd").cast("decimal(18,2)"),
        F.col("payment_method").cast("string"),
        F.col("currency_original").cast("string"),
        F.col("exchange_rate").cast("decimal(18,6)"),
        F.col("status").cast("string"),
        F.col("processing_days").cast("int"),
        F.col("fee_usd").cast("decimal(18,2)"),
        F.col("_source_file").alias("source_file"),
    )
    .withColumn("source_system", F.lit("VENDOR"))
    .withColumn("row_hash", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in CANONICAL]), 256))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data quality gate
# MAGIC
# MAGIC Severity drives a **distinct** action. Nothing is "log and continue".
# MAGIC
# MAGIC - **BLOCK** → abort, nothing lands, page on-call
# MAGIC - **QUARANTINE** → withhold the row, persist it, re-drive next run
# MAGIC - **WARN** → load the row, record evidence for stewardship

# COMMAND ----------

signup = spark.table(f"{BRONZE}.client_signup").select(
    "client_id", "signup_date", "kyc_status")

j = normalised.join(signup, "client_id", "left")

checks = j.select(
    "*",
    # QUARANTINE
    F.when(F.col("amount_usd").isNull() | (F.col("amount_usd") <= 0),
           F.concat(F.lit("non-positive amount_usd="), F.col("amount_usd"))).alias("f_amount_positive"),
    F.when(F.col("signup_date").isNull(),
           F.concat(F.lit("client_id "), F.col("client_id"), F.lit(" not in client_signup"))
           ).alias("f_client_exists"),
    F.when(F.col("payment_method").isNull(),
           F.lit("payment_method missing after alias mapping")).alias("f_payment_method"),
    # WARN
    F.when(F.col("deposit_date") < F.col("signup_date"),
           F.concat(F.lit("deposit_date "), F.col("deposit_date"),
                    F.lit(" precedes signup_date "), F.col("signup_date"))).alias("w_before_signup"),
    F.when((F.col("amount_usd") > 0) & (F.col("fee_usd") > 0) &
           (F.abs(F.col("fee_usd") / F.col("amount_usd") - F.lit(0.01)) > F.lit(0.002)),
           F.concat(F.lit("fee is "), F.round(F.col("fee_usd") / F.col("amount_usd") * 100, 2),
                    F.lit("% of amount, expected ~1.00%"))).alias("w_fee_tolerance"),
    F.when(F.col("kyc_status") != "approved",
           F.concat(F.lit("kyc_status="), F.col("kyc_status"))).alias("w_kyc"),
)
# NOTE: no .cache() here — serverless compute rejects PERSIST/CACHE TABLE
# ([NOT_SUPPORTED_WITH_SERVERLESS]). `checks` is re-derived by each downstream
# action instead; the plan is cheap and serverless disk caching covers the scans.

QUARANTINE_COLS = {"f_amount_positive": "amount_positive",
                   "f_client_exists": "client_exists",
                   "f_payment_method": "payment_method_present"}
WARN_COLS = {"w_before_signup": "deposit_not_before_signup",
             "w_fee_tolerance": "fee_within_tolerance",
             "w_kyc": "kyc_approved_for_deposit"}
ACTIONS = {"amount_positive": "withhold row, raise vendor ticket",
           "client_exists": "withhold row, park for late-arriving dimension",
           "payment_method_present": "withhold row, raise vendor ticket",
           "deposit_not_before_signup": "load, flag for stewardship review",
           "fee_within_tolerance": "load, flag for finance review",
           "kyc_approved_for_deposit": "load, route to compliance queue"}

dq_rows = None
for col, name in {**QUARANTINE_COLS, **WARN_COLS}.items():
    sev = "QUARANTINE" if col in QUARANTINE_COLS else "WARN"
    part = (checks.filter(F.col(col).isNotNull())
            .select(F.lit(RUN_ID).alias("run_id"), F.lit(name).alias("check_name"),
                    F.lit(sev).alias("severity"), F.lit("vendor_deposit").alias("entity"),
                    F.col("deposit_id").alias("record_key"), "source_file",
                    F.col(col).alias("detail"), F.lit(ACTIONS[name]).alias("on_failure"),
                    F.current_timestamp().alias("detected_at")))
    dq_rows = part if dq_rows is None else dq_rows.union(part)

if dq_rows is not None:
    dq_rows.write.format("delta").mode("append").saveAsTable(f"{SILVER}.dq_result")

quarantined = checks.filter(
    F.col("f_amount_positive").isNotNull() |
    F.col("f_client_exists").isNotNull() |
    F.col("f_payment_method").isNotNull())

(quarantined.select(
    F.lit(RUN_ID).alias("run_id"), "deposit_id", "client_id", "source_file",
    F.to_json(F.struct(*CANONICAL)).alias("raw_payload"),
    F.concat_ws(" | ", F.col("f_amount_positive"), F.col("f_client_exists"),
                F.col("f_payment_method")).alias("failed_checks"),
    F.current_timestamp().alias("quarantined_at"), F.lit(None).cast("timestamp").alias("resolved_at"))
 .write.format("delta").mode("append").saveAsTable(f"{SILVER}.quarantine_deposit"))

clean = checks.filter(
    F.col("f_amount_positive").isNull() &
    F.col("f_client_exists").isNull() &
    F.col("f_payment_method").isNull())

print(f"DQ: {checks.count()} in, {clean.count()} clean, {quarantined.count()} quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deduplicate on the business key
# MAGIC
# MAGIC The vendor redelivers rows across files. Last file wins, so a **corrected** row
# MAGIC supersedes the original while an **identical** row collapses harmlessly.
# MAGIC On the delivered data: 22 clean → 20 distinct.

# COMMAND ----------

w = Window.partitionBy("deposit_id").orderBy(F.col("source_file").desc())
deduped = (clean.withColumn("rn", F.row_number().over(w))
           .filter("rn = 1").drop("rn")
           .select(*CANONICAL, "source_system", "source_file", "row_hash"))

print(f"Deduplicated to {deduped.count()} distinct deposit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Idempotent MERGE
# MAGIC
# MAGIC The `row_hash` predicate on `whenMatchedUpdate` is what makes replay a true no-op:
# MAGIC an unchanged row matches the key but fails the predicate, so no write occurs and
# MAGIC `updated_at` is not churned.

# COMMAND ----------

if not spark.catalog.tableExists(f"{SILVER}.silver_deposit"):
    (deduped.withColumn("is_quarantined", F.lit(False))
     .withColumn("effective_from", F.current_timestamp())
     .withColumn("updated_at", F.current_timestamp())
     .write.format("delta").partitionBy("deposit_date")
     .saveAsTable(f"{SILVER}.silver_deposit"))
else:
    tgt = DeltaTable.forName(spark, f"{SILVER}.silver_deposit")
    (tgt.alias("t").merge(
        deduped.alias("s"),
        "t.deposit_id = s.deposit_id AND t.source_system = s.source_system")
     .whenMatchedUpdate(
         condition="t.row_hash <> s.row_hash",
         set={c: f"s.{c}" for c in CANONICAL + ["source_file", "row_hash"]} |
             {"updated_at": "current_timestamp()"})
     .whenNotMatchedInsert(
         values={c: f"s.{c}" for c in CANONICAL + ["source_system", "source_file", "row_hash"]} |
                {"is_quarantined": "false", "effective_from": "current_timestamp()",
                 "updated_at": "current_timestamp()"})
     .execute())

display(spark.sql(f"""
    SELECT source_system, count(*) rows, sum(amount_usd) total_usd,
           min(deposit_date) from_date, max(deposit_date) to_date
    FROM {SILVER}.silver_deposit GROUP BY source_system"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Release quarantined rows whose blocker has cleared
# MAGIC
# MAGIC This is what makes the late-arriving-dimension case self-healing. When `CL099`
# MAGIC eventually appears in `client_signup`, `VDEP020` flows into Silver on the next
# MAGIC run with **no manual replay**.
# MAGIC
# MAGIC Ageing alert: a row quarantined for more than 7 days means the dimension feed is
# MAGIC broken rather than merely late.

# COMMAND ----------

q = DeltaTable.forName(spark, f"{SILVER}.quarantine_deposit")
(q.alias("q").merge(signup.select("client_id").alias("c"),
                    "q.client_id = c.client_id AND q.resolved_at IS NULL")
 .whenMatchedUpdate(set={"resolved_at": "current_timestamp()"})
 .execute())

display(spark.sql(f"""
    SELECT deposit_id, client_id, failed_checks,
           datediff(current_date(), CAST(quarantined_at AS DATE)) AS age_days,
           CASE WHEN datediff(current_date(), CAST(quarantined_at AS DATE)) > 7
                THEN 'ESCALATE - dimension feed likely broken' ELSE 'monitor' END AS action
    FROM {SILVER}.quarantine_deposit WHERE resolved_at IS NULL"""))
