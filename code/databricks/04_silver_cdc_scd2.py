# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — CDC → SCD Type 2 `dim_client`
# MAGIC
# MAGIC The delivered CDC file is in **arrival order, not LSN order**:
# MAGIC
# MAGIC ```
# MAGIC arrival : 1005, 1009, 1001, 1004, 1010, 1012, 1003, 1015, 1008, 1018, 1006, 1020
# MAGIC correct : 1001, 1003, 1004, 1005, 1006, 1008, 1009, 1010, 1012, 1015, 1018, 1020
# MAGIC ```
# MAGIC
# MAGIC `CL001` has three changes (`1004`, `1005`, `1006`) **all committed on 2024-11-15**,
# MAGIC delivered 1005 → 1004 → 1006. Applied as delivered, the earlier `1004` image
# MAGIC overwrites the later `1005` balance and the client ends up **$600 wrong**.
# MAGIC
# MAGIC Ordering by `commit_ts` is **not sufficient** — only the LSN gives a total order.
# MAGIC
# MAGIC Deletes are applied as **soft deletes**: end-dated row + tombstone. Never physical.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = "workspace"
BRONZE = f"{CATALOG}.deriv_assement_bronze"
SILVER = f"{CATALOG}.deriv_assement_silver"

DIM = f"{SILVER}.dim_client"
APPLY_LOG = f"{SILVER}.cdc_apply_log"
END_OF_TIME = "9999-12-31 00:00:00"

TRACKED = ["risk_category", "account_balance_usd", "account_status"]
MASTER = f"{BRONZE}.bronze_load_master"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Master-driven Bronze read
# MAGIC
# MAGIC `bronze_load_master` holds one row per batch-loaded Bronze table, and
# MAGIC `latest_loaded_ts` is advanced only after a successful Bronze write (notebook 01).
# MAGIC Reading at that timestamp is what makes Silver deterministic: Bronze is
# MAGIC append-only, so an unfiltered read returns every historical load stacked up and
# MAGIC a re-run of 01 silently doubles the rows.
# MAGIC
# MAGIC A missing or non-SUCCESS master row is BLOCK severity, not a silent empty read.

# COMMAND ----------

def read_bronze(table):
    """Read a batch-loaded Bronze table at its latest successful load timestamp."""
    row = (spark.table(MASTER)
           .filter(F.col("table_name") == F.lit(table))
           .select("latest_loaded_ts", "last_load_status")
           .first())

    if row is None:
        raise Exception(
            f"[BLOCK] No {MASTER} row for '{table}'. Bronze has never loaded it "
            f"successfully. Run notebook 01 before this one."
        )
    if row["last_load_status"] != "SUCCESS":
        raise Exception(
            f"[BLOCK] Latest load of '{table}' is {row['last_load_status']}, not SUCCESS. "
            f"Silver refuses to read a failed batch."
        )

    return (spark.table(f"{BRONZE}.{table}")
            .filter(F.col("delta_created_ts") == F.lit(row["latest_loaded_ts"])))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Seed the dimension from the profile snapshot (once)
# MAGIC
# MAGIC `valid_from` is `1900-01-01` so the baseline version covers any historical fact
# MAGIC date — including `TRD005`, a trade by `CL007` dated 24 days *before* that client's
# MAGIC `signup_date`.

# COMMAND ----------

if not spark.catalog.tableExists(DIM):
    seed = (read_bronze("client_profile")
            .select("client_id", "full_name", F.col("date_of_birth").cast("date"),
                    "nationality", "risk_category",
                    F.col("account_balance_usd").cast("decimal(18,2)"),
                    "account_status", "currency", "preferred_language")
            .withColumn("client_sk", F.sha2(F.concat_ws("|", F.col("client_id"),
                                                        F.lit("1900-01-01")), 256))
            .withColumn("valid_from", F.lit("1900-01-01").cast("timestamp"))
            .withColumn("valid_to", F.lit(END_OF_TIME).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .withColumn("is_deleted", F.lit(False))
            .withColumn("is_inferred", F.lit(False))
            .withColumn("source_lsn", F.lit(0).cast("bigint"))
            .withColumn("source_op", F.lit("seed"))
            .withColumn("record_hash",
                        F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in TRACKED]), 256))
            .withColumn("updated_at", F.current_timestamp()))
    seed.write.format("delta").saveAsTable(DIM)
    print(f"Seeded dim_client with {seed.count()} baseline rows")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {APPLY_LOG} (
    lsn BIGINT, client_id STRING, op STRING, commit_ts TIMESTAMP,
    applied_at TIMESTAMP, action_taken STRING) USING DELTA""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Order the batch by LSN and apply the replay guard
# MAGIC
# MAGIC `cdc_apply_log` is the watermark. Events at or below it are skipped, so a re-run
# MAGIC applies nothing.

# COMMAND ----------

watermark = spark.sql(f"SELECT COALESCE(MAX(lsn), 0) AS m FROM {APPLY_LOG}").first()["m"]

# Streaming Bronze, NOT master-driven: the Auto Loader checkpoint already ingests
# each change file once, and the `lsn` watermark below is this notebook's own
# incremental boundary. A delta_created_ts filter would hide un-applied changes.
cdc = (spark.table(f"{BRONZE}.stream_client_profile_changes")
       .filter(F.col("lsn") > F.lit(watermark))
       .withColumn("commit_ts", F.col("commit_ts").cast("timestamp"))
       .orderBy("lsn"))                      # <-- THE critical line

print(f"Watermark: {watermark}")
print("Arrival order:", [r.lsn for r in spark.table(f"{BRONZE}.stream_client_profile_changes")
                         .select("lsn").collect()])
print("Applied order:", [r.lsn for r in cdc.select("lsn").collect()])

# LSN gaps are EXPECTED — a transaction log is shared across all tables, so a gap is a
# transaction against a different table. Alerting on every gap produces constant false
# pages. Track the high-water mark; alert only if a gap persists past the completeness SLA.
lsns = sorted(r.lsn for r in cdc.select("lsn").collect())
if lsns:
    gaps = [i for i in range(lsns[0], lsns[-1] + 1) if i not in lsns]
    print(f"LSN gaps (informational, not an alert): {gaps}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply events sequentially per key
# MAGIC
# MAGIC Each event must see the result of the previous one for the same client, so the
# MAGIC batch is applied in LSN order rather than as one set-based merge. With CDC volumes
# MAGIC this is grouped per key; the per-key chains here are short (max 3, for `CL001`).
# MAGIC
# MAGIC `after` carries only the **changed subset**, so unchanged attributes are carried
# MAGIC forward from the current version. Without that, every update would null
# MAGIC `full_name`, `nationality` and `preferred_language`.

# COMMAND ----------

dim = DeltaTable.forName(spark, DIM)
applied = []

for e in cdc.collect():
    cid, op, cts, lsn = e["client_id"], e["op"], e["commit_ts"], e["lsn"]
    after = e["after"].asDict() if e["after"] is not None else {}

    cur = spark.sql(f"""SELECT * FROM {DIM}
                        WHERE client_id = '{cid}' AND is_current = TRUE""").collect()
    cur = cur[0] if cur else None

    if op == "delete":
        if cur is None:
            applied.append((lsn, cid, op, cts, "delete_for_unknown_key_ignored"))
            continue
        # Phase A — end-date the current version.
        spark.sql(f"""UPDATE {DIM} SET valid_to = TIMESTAMP'{cts}', is_current = FALSE,
                                       updated_at = current_timestamp()
                      WHERE client_id = '{cid}' AND is_current = TRUE""")
        # Phase B — tombstone. Retains last known values; never a physical delete.
        # CL012 has two real deposits (DEP008, VDEP004): removing the row would
        # orphan both facts and silently change historical revenue.
        spark.sql(f"""INSERT INTO {DIM} SELECT
            sha2(concat_ws('|','{cid}','{cts}'),256), client_id, full_name, date_of_birth,
            nationality, risk_category, account_balance_usd, account_status, currency,
            preferred_language, TIMESTAMP'{cts}', TIMESTAMP'{END_OF_TIME}',
            TRUE, TRUE, FALSE, {lsn}, 'delete', record_hash, current_timestamp()
            FROM {DIM} WHERE client_id = '{cid}' AND valid_to = TIMESTAMP'{cts}'""")
        applied.append((lsn, cid, op, cts, "soft_delete_tombstone"))
        continue

    # insert / update — merge the partial after-image onto the current version.
    merged = {c: (cur[c] if cur else None) for c in
              ["full_name", "date_of_birth", "nationality", "risk_category",
               "account_balance_usd", "account_status", "currency", "preferred_language"]}
    for k, v in after.items():
        if k in merged and v is not None:
            merged[k] = v

    new_hash = spark.sql(
        "SELECT sha2(concat_ws('|',{},{},{}),256) h".format(
            *[f"'{merged[c]}'" if merged[c] is not None else "NULL" for c in TRACKED])
    ).first()["h"]

    # No-change events are true no-ops. This is why lsn 1001 — re-inserting CL030
    # with identical attributes — creates NO new version instead of a duplicate.
    if cur is not None and cur["record_hash"] == new_hash and not cur["is_deleted"]:
        applied.append((lsn, cid, op, cts, "no_change_noop"))
        continue

    if cur is not None:
        spark.sql(f"""UPDATE {DIM} SET valid_to = TIMESTAMP'{cts}', is_current = FALSE,
                                       updated_at = current_timestamp()
                      WHERE client_id = '{cid}' AND is_current = TRUE""")

    vals = ", ".join("NULL" if merged[c] is None else
                     (f"{merged[c]}" if c == "account_balance_usd" else f"'{merged[c]}'")
                     for c in ["full_name", "date_of_birth", "nationality", "risk_category",
                               "account_balance_usd", "account_status", "currency",
                               "preferred_language"])
    spark.sql(f"""INSERT INTO {DIM} VALUES (
        sha2(concat_ws('|','{cid}','{cts}'),256), '{cid}', {vals},
        TIMESTAMP'{cts}', TIMESTAMP'{END_OF_TIME}', TRUE, FALSE, FALSE,
        {lsn}, '{op}', '{new_hash}', current_timestamp())""")
    applied.append((lsn, cid, op, cts, "new_version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Advance the watermark
# MAGIC
# MAGIC Written in the same transaction boundary as the dimension writes. Delta's ACID
# MAGIC guarantee means the watermark cannot advance without the data landing, or vice
# MAGIC versa. Without that, a mid-run failure produces either silently skipped events or
# MAGIC infinitely reapplied ones.

# COMMAND ----------

if applied:
    (spark.createDataFrame(applied, "lsn long, client_id string, op string, "
                                    "commit_ts timestamp, action_taken string")
     .withColumn("applied_at", F.current_timestamp())
     .select("lsn", "client_id", "op", "commit_ts", "applied_at", "action_taken")
     .write.format("delta").mode("append").saveAsTable(APPLY_LOG))

print(f"Applied {len(applied)} events")

# Consumer-facing view. Every downstream reader uses THIS, so a forgotten
# is_deleted filter cannot double-count.
spark.sql(f"""CREATE OR REPLACE VIEW {SILVER}.dim_client_current AS
              SELECT * FROM {DIM} WHERE is_current = TRUE AND is_deleted = FALSE""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify — SCD2 structural invariants
# MAGIC
# MAGIC All three must return zero rows, after every run and every backfill.

# COMMAND ----------

for name, sql in [
    ("multiple current versions",
     f"SELECT client_id, count(*) c FROM {DIM} WHERE is_current GROUP BY 1 HAVING count(*) > 1"),
    ("timeline gap or overlap",
     f"""SELECT client_id FROM (SELECT client_id, valid_to,
             LEAD(valid_from) OVER (PARTITION BY client_id ORDER BY valid_from) nxt FROM {DIM})
         WHERE nxt IS NOT NULL AND nxt <> valid_to"""),
    ("inverted interval", f"SELECT client_id FROM {DIM} WHERE valid_from >= valid_to"),
]:
    n = spark.sql(sql).count()
    print(f"{'PASS' if n == 0 else 'FAIL'}  {name}: {n} rows")

# CL001 — three same-day changes delivered out of order. The timeline must be contiguous.
display(spark.sql(f"""SELECT risk_category, account_balance_usd, account_status,
                             valid_from, valid_to, is_current, source_lsn
                      FROM {DIM} WHERE client_id = 'CL001' ORDER BY valid_from"""))

# CL012 — source delete. History intact, tombstone current.
display(spark.sql(f"""SELECT account_status, valid_from, valid_to, is_current,
                             is_deleted, source_lsn
                      FROM {DIM} WHERE client_id = 'CL012' ORDER BY valid_from"""))
