# Databricks notebook source
# MAGIC %md
# MAGIC # Reconciliation — vendor feed vs warehouse `client_deposit`
# MAGIC
# MAGIC **Two-tier by design.** A single matching strategy is fragile: identifier-only
# MAGIC matching reports false breaks whenever two systems use different id namespaces,
# MAGIC which is exactly what happens here.
# MAGIC
# MAGIC The recon window is derived from **event dates, never the file label** —
# MAGIC `deposits_vendor_20240303.csv` is labelled 3 March but contains 24–28 February.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SILVER = f"{CATALOG}.deriv_assement_silver"

sd = spark.table(f"{SILVER}.silver_deposit")
v = sd.filter("source_system = 'VENDOR'").alias("v")
w = sd.filter("source_system = 'WAREHOUSE'").alias("w")

win = v.agg(F.min("deposit_date").alias("d_from"), F.max("deposit_date").alias("d_to")).first()
print(f"Recon window from EVENT dates: {win.d_from} .. {win.d_to}")

# COMMAND ----------

# Tier 1 — exact deposit_id. Cheap and unambiguous.
t1 = (v.join(w, F.col("v.deposit_id") == F.col("w.deposit_id"))
      .select(F.lit("TIER1_ID").alias("match_tier"), F.lit("MATCHED").alias("break_type"),
              F.col("v.deposit_id").alias("deposit_id_vendor"),
              F.col("w.deposit_id").alias("deposit_id_warehouse"),
              F.col("v.client_id"), F.col("v.deposit_date"),
              F.col("v.amount_usd").alias("amount_vendor"),
              F.col("w.amount_usd").alias("amount_warehouse"),
              (F.col("v.amount_usd") - F.col("w.amount_usd")).alias("variance_usd")))

# Tier 2 — composite business key, for feeds that do not share an id namespace.
t2 = (v.join(w, (F.col("v.client_id") == F.col("w.client_id")) &
                (F.col("v.deposit_date") == F.col("w.deposit_date")) &
                (F.abs(F.col("v.amount_usd") - F.col("w.amount_usd")) < 0.01))
      .select(F.lit("TIER2_BUSINESS_KEY"), F.lit("MATCHED"),
              F.col("v.deposit_id"), F.col("w.deposit_id"), F.col("v.client_id"),
              F.col("v.deposit_date"), F.col("v.amount_usd"), F.col("w.amount_usd"),
              (F.col("v.amount_usd") - F.col("w.amount_usd"))))

print(f"Tier 1 matches: {t1.count()}   Tier 2 matches: {t2.count()}")

# COMMAND ----------

matched_v = t1.select("deposit_id_vendor").union(t2.select("v.deposit_id"))
matched_w = t1.select("deposit_id_warehouse").union(t2.select("w.deposit_id"))

v_only = (v.join(matched_v, F.col("v.deposit_id") == F.col("deposit_id_vendor"), "left_anti")
          .select(F.lit("UNMATCHED"), F.lit("IN_VENDOR_NOT_IN_WAREHOUSE"),
                  F.col("deposit_id"), F.lit(None).cast("string"), F.col("client_id"),
                  F.col("deposit_date"), F.col("amount_usd"),
                  F.lit(None).cast("decimal(18,2)"), F.col("amount_usd")))

# Warehouse-only is scoped to the vendor's own event window; without that it would
# report every warehouse deposit ever loaded.
w_only = (w.filter(F.col("deposit_date").between(win.d_from, win.d_to))
          .join(matched_w, F.col("w.deposit_id") == F.col("deposit_id_warehouse"), "left_anti")
          .select(F.lit("UNMATCHED"), F.lit("IN_WAREHOUSE_NOT_IN_VENDOR"),
                  F.lit(None).cast("string"), F.col("deposit_id"), F.col("client_id"),
                  F.col("deposit_date"), F.lit(None).cast("decimal(18,2)"),
                  F.col("amount_usd"), -F.col("amount_usd")))

print(f"Breaks: {v_only.count()} vendor-only, {w_only.count()} warehouse-only")

result = (t1.union(t2).union(v_only).union(w_only)
          .withColumn("run_id", F.lit(spark.conf.get("spark.databricks.job.runId", "manual")))
          .withColumn("created_at", F.current_timestamp()))
result.write.format("delta").mode("append").option("mergeSchema", "true") \
      .saveAsTable(f"{SILVER}.reconciliation_result")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control totals
# MAGIC
# MAGIC This is the check that catches a silently truncated file, which row-level
# MAGIC matching alone will not.

# COMMAND ----------

display(sd.groupBy("deposit_date", "source_system")
        .agg(F.count("*").alias("rows"), F.sum("amount_usd").alias("total_usd"))
        .orderBy("deposit_date", "source_system"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result and interpretation
# MAGIC
# MAGIC ```
# MAGIC Tier 1: 0 matches    Tier 2: 0 matches
# MAGIC 20 vendor-only, 1 warehouse-only (DEP008, CL012, 2024-02-25, $350.00)
# MAGIC Vendor control total: 20 rows / $28,525.00
# MAGIC ```
# MAGIC
# MAGIC Zero matches at **both** tiers is the finding, and it is not a pipeline failure.
# MAGIC Vendor ids are `VDEP001–022`, warehouse ids are `DEP001–020`, with no overlap, and
# MAGIC Tier 2 confirms no economic duplicates either.
# MAGIC
# MAGIC This feed is **net-new deposit traffic**, not a mirror of `client_deposit`. So
# MAGIC reconciliation here is a *completeness and control-total* check, not a row-for-row
# MAGIC tie-out. Reporting "100% break rate" would be technically true and completely
# MAGIC misleading.

# COMMAND ----------

display(spark.sql(f"""
    SELECT break_type, count(*) breaks, sum(abs(variance_usd)) abs_variance,
           CASE WHEN sum(abs(variance_usd)) > 10000 THEN 'PAGE_IMMEDIATELY'
                WHEN max(datediff(current_date(), CAST(created_at AS DATE))) > 3
                     THEN 'ESCALATE_TO_VENDOR' ELSE 'MONITOR' END AS action
    FROM {SILVER}.reconciliation_result
    WHERE break_type <> 'MATCHED' GROUP BY break_type"""))
