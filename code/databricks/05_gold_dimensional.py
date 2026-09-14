# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Kimball star schema
# MAGIC
# MAGIC Builds `dim_client` (SCD2), `dim_date`, `dim_instrument`, `fact_deposit`,
# MAGIC `fact_trade` and the daily balance snapshot.
# MAGIC
# MAGIC Two design points worth noting:
# MAGIC
# MAGIC 1. **Late-arriving dimensions** get an *inferred member*, not a dropped fact and
# MAGIC    not a null FK. Fires on `CL031` (`DEP020`) and `CL099` (`VDEP020`).
# MAGIC 2. **PnL is recomputed as a control**, never overwritten. `TRD012` reports 245.00
# MAGIC    on a trade whose open and close prices are identical (derived 0.00).

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "workspace"
BRONZE, SILVER, GOLD = (f"{CATALOG}.deriv_assement_bronze",
                        f"{CATALOG}.deriv_assement_silver",
                        f"{CATALOG}.deriv_assement_gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_instrument
# MAGIC
# MAGIC `contract_size` is reverse-engineered from the trades that ARE internally
# MAGIC consistent, then used to re-derive PnL for every trade. That is what makes the
# MAGIC `TRD012` defect detectable at all.

# COMMAND ----------

instruments = [("EUR/USD", "FX", 10000.0, True), ("USD/JPY", "FX", 10000.0, True),
               ("Gold", "metals", 10.0, True), ("BTC/USD", "crypto", 1.0, True),
               ("S&P500", "index", 1.0, True)]
(spark.createDataFrame(instruments, "instrument string, asset_class string, "
                       "contract_size double, is_leveraged boolean")
 .withColumn("instrument_key", F.sha2(F.col("instrument"), 256))
 .write.format("delta").mode("overwrite").saveAsTable(f"{GOLD}.dim_instrument"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_client — copy SCD2 from Silver, then handle late-arriving members

# COMMAND ----------

(spark.table(f"{SILVER}.dim_client")
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{GOLD}.dim_client"))

# Inferred members: valid_from 1900-01-01 so the stub covers ANY historical fact date.
missing = (spark.table(f"{SILVER}.silver_deposit").select("client_id").distinct()
           .join(spark.table(f"{GOLD}.dim_client").select("client_id").distinct(),
                 "client_id", "left_anti"))

if missing.count() > 0:
    stub = (missing
            .withColumn("client_sk", F.sha2(F.concat(F.col("client_id"), F.lit("inferred")), 256))
            .withColumn("full_name", F.lit("UNKNOWN"))
            .withColumn("risk_category", F.lit("unknown"))
            .withColumn("account_status", F.lit("unknown"))
            .withColumn("valid_from", F.lit("1900-01-01").cast("timestamp"))
            .withColumn("valid_to", F.lit("9999-12-31").cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .withColumn("is_deleted", F.lit(False))
            .withColumn("is_inferred", F.lit(True))
            .withColumn("updated_at", F.current_timestamp()))
    stub.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{GOLD}.dim_client")
    print(f"Inferred members created: {[r.client_id for r in missing.collect()]}")

# When the real record arrives the stub is UPGRADED IN PLACE. client_sk never
# changes, so facts already loaded need NO restatement — the whole reason the fact
# carries a surrogate key rather than the natural key.
dim = DeltaTable.forName(spark, f"{GOLD}.dim_client")
(dim.alias("t").merge(spark.table(f"{SILVER}.dim_client_current").alias("s"),
                      "t.client_id = s.client_id AND t.is_inferred = TRUE")
 .whenMatchedUpdate(set={"full_name": "s.full_name", "risk_category": "s.risk_category",
                         "account_status": "s.account_status", "is_inferred": "false",
                         "updated_at": "current_timestamp()"})
 .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date — conformed across both facts

# COMMAND ----------

dates = (spark.table(f"{SILVER}.silver_deposit").select(F.col("deposit_date").alias("d"))
         .union(spark.table(f"{BRONZE}.client_trades").select(F.col("trade_date").cast("date")))
         .filter("d IS NOT NULL").distinct())

(dates.withColumn("date_key", F.date_format("d", "yyyyMMdd").cast("int"))
 .withColumn("full_date", F.col("d")).withColumn("year", F.year("d"))
 .withColumn("quarter", F.quarter("d")).withColumn("month", F.month("d"))
 .withColumn("day", F.dayofmonth("d")).withColumn("month_name", F.date_format("d", "MMMM"))
 .withColumn("day_of_week", F.date_format("d", "EEEE"))
 .withColumn("is_weekend", F.dayofweek("d").isin(1, 7))
 .drop("d").write.format("delta").mode("overwrite").saveAsTable(f"{GOLD}.dim_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_deposit — GRAIN: one row per deposit event per source system

# COMMAND ----------

dc = spark.table(f"{GOLD}.dim_client").filter("is_current = TRUE").select(
    "client_sk", F.col("client_id").alias("dc_client_id"))

fact_dep = (spark.table(f"{SILVER}.silver_deposit").filter("is_quarantined = FALSE")
            .join(dc, F.col("client_id") == F.col("dc_client_id"), "left")
            .withColumn("deposit_sk", F.sha2(F.concat_ws("|", "source_system", "deposit_id"), 256))
            .withColumn("date_key", F.date_format("deposit_date", "yyyyMMdd").cast("int"))
            .withColumn("payment_method_key", F.sha2(F.col("payment_method"), 256))
            .withColumn("net_amount_usd", F.col("amount_usd") - F.coalesce(F.col("fee_usd"), F.lit(0)))
            .withColumn("updated_at", F.current_timestamp())
            .select("deposit_sk", "deposit_id", "client_sk", "client_id", "date_key",
                    "payment_method_key", "source_system", "amount_usd", "fee_usd",
                    "net_amount_usd", "processing_days", "status", "updated_at"))

# Full rebuild on first load; incremental restatement uses replaceWhere (see sql/08).
(fact_dep.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .partitionBy("date_key").saveAsTable(f"{GOLD}.fact_deposit"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_trade — GRAIN: one row per trade
# MAGIC
# MAGIC `pnl_usd_reported` is never modified. The trading book is the system of record; a
# MAGIC pipeline silently "correcting" it would be a serious error. The variance is
# MAGIC exposed as a control column instead.

# COMMAND ----------

inst = spark.table(f"{GOLD}.dim_instrument").select(
    "instrument_key", F.col("instrument").alias("i_instrument"), "contract_size")

fact_trd = (spark.table(f"{BRONZE}.client_trades")
            .join(dc, F.col("client_id") == F.col("dc_client_id"), "left")
            .join(inst, F.col("instrument") == F.col("i_instrument"), "left")
            .withColumn("trade_sk", F.sha2(F.col("trade_id"), 256))
            .withColumn("date_key", F.date_format(F.col("trade_date").cast("date"), "yyyyMMdd").cast("int"))
            .withColumn("pnl_usd_reported", F.col("pnl_usd").cast("decimal(18,2)"))
            .withColumn("pnl_usd_derived",
                        F.round((F.col("close_price") - F.col("open_price"))
                                * F.when(F.col("direction") == "buy", 1).otherwise(-1)
                                * F.col("contract_size") * F.col("volume_lots"), 2).cast("decimal(18,2)"))
            .withColumn("pnl_variance_usd", F.col("pnl_usd_reported") - F.col("pnl_usd_derived"))
            .withColumn("updated_at", F.current_timestamp())
            .select("trade_sk", "trade_id", "client_sk", "client_id", "date_key", "instrument_key",
                    "direction", "volume_lots", "open_price", "close_price",
                    "pnl_usd_reported", "pnl_usd_derived", "pnl_variance_usd",
                    "trade_status", "updated_at"))

(fact_trd.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .partitionBy("date_key").saveAsTable(f"{GOLD}.fact_trade"))

# Control: any trade whose reported PnL does not recompute. Expect TRD012.
display(spark.sql(f"""SELECT trade_id, pnl_usd_reported, pnl_usd_derived, pnl_variance_usd
                      FROM {GOLD}.fact_trade WHERE abs(pnl_variance_usd) > 0.01"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'fact_deposit' t, count(*) rows, sum(amount_usd) total FROM {GOLD}.fact_deposit
  UNION ALL SELECT 'fact_trade', count(*), sum(pnl_usd_reported) FROM {GOLD}.fact_trade
  UNION ALL SELECT 'dim_client', count(*), NULL FROM {GOLD}.dim_client"""))

# No fact may carry a null client_sk — that would silently exclude it from every
# dimensional query. The inferred-member mechanism exists to keep this at zero.
orphans = spark.sql(f"SELECT count(*) c FROM {GOLD}.fact_deposit WHERE client_sk IS NULL").first()["c"]
print(f"{'PASS' if orphans == 0 else 'FAIL'}  orphaned facts: {orphans}")
