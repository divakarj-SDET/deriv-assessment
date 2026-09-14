# Part 2 - Gold Dimensional Model
# Databricks / PySpark
#
# Assumptions:
# - Part 1 has produced canonical Silver tables.
# - Catalog/schema names follow the assessment convention.
# - Silver table names can be adjusted in the configuration section.
#
# This script creates conformed dimensions and facts using Delta MERGE.
# It is intentionally explicit about columns so schema drift columns from
# Bronze (for example _rescued_data/_corrupt_record) cannot leak into Gold.

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "workspace"
SILVER = f"{CATALOG}.deriv_assement_silver"
GOLD = f"{CATALOG}.deriv_assement_gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def merge_delta(source_df, target_name, merge_condition, update_map, insert_map):
    if not spark.catalog.tableExists(target_name):
        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_name)
        )
        return

    target = DeltaTable.forName(spark, target_name)
    (
        target.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )


def table_exists(name):
    return spark.catalog.tableExists(name)


# ---------------------------------------------------------------------------
# 1. dim_date
# ---------------------------------------------------------------------------

date_sql = f"""
CREATE TABLE IF NOT EXISTS {GOLD}.dim_date (
    date_sk INT,
    calendar_date DATE,
    calendar_year INT,
    quarter_num INT,
    month_num INT,
    month_name STRING,
    week_num INT,
    day_name STRING,
    is_weekend BOOLEAN
)
USING DELTA
"""
spark.sql(date_sql)

# Build the date range from the transaction/profile data rather than hardcoding
# a business-specific range.
date_sources = []

if table_exists(f"{SILVER}.silver_client_deposit"):
    date_sources.append(
        spark.table(f"{SILVER}.silver_client_deposit")
        .select(F.to_date("deposit_date").alias("d"))
    )

if table_exists(f"{SILVER}.silver_client_trades"):
    date_sources.append(
        spark.table(f"{SILVER}.silver_client_trades")
        .select(F.to_date("trade_ts").alias("d"))
    )

if date_sources:
    date_df = date_sources[0]
    for x in date_sources[1:]:
        date_df = date_df.unionByName(x)

    bounds = date_df.where(F.col("d").isNotNull()).agg(
        F.min("d").alias("min_d"),
        F.max("d").alias("max_d")
    ).first()

    if bounds and bounds["min_d"] and bounds["max_d"]:
        dates = spark.sql(
            f"SELECT explode(sequence("
            f"to_date('{bounds['min_d']}'), "
            f"to_date('{bounds['max_d']}'), "
            f"interval 1 day)) AS calendar_date"
        )

        dates = (
            dates
            .withColumn("date_sk", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
            .withColumn("calendar_year", F.year("calendar_date"))
            .withColumn("quarter_num", F.quarter("calendar_date"))
            .withColumn("month_num", F.month("calendar_date"))
            .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
            .withColumn("week_num", F.weekofyear("calendar_date"))
            .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
            .withColumn("is_weekend", F.dayofweek("calendar_date").isin([1, 7]))
            .select(
                "date_sk", "calendar_date", "calendar_year", "quarter_num",
                "month_num", "month_name", "week_num", "day_name", "is_weekend"
            )
        )

        merge_delta(
            dates,
            f"{GOLD}.dim_date",
            "t.date_sk = s.date_sk",
            {
                "calendar_date": "s.calendar_date",
                "calendar_year": "s.calendar_year",
                "quarter_num": "s.quarter_num",
                "month_num": "s.month_num",
                "month_name": "s.month_name",
                "week_num": "s.week_num",
                "day_name": "s.day_name",
                "is_weekend": "s.is_weekend",
            },
            {
                "date_sk": "s.date_sk",
                "calendar_date": "s.calendar_date",
                "calendar_year": "s.calendar_year",
                "quarter_num": "s.quarter_num",
                "month_num": "s.month_num",
                "month_name": "s.month_name",
                "week_num": "s.week_num",
                "day_name": "s.day_name",
                "is_weekend": "s.is_weekend",
            }
        )


# ---------------------------------------------------------------------------
# 2. dim_client
# ---------------------------------------------------------------------------

client_src = spark.table(f"{SILVER}.silver_client_signup")

client_df = (
    client_src
    .select(
        "client_id",
        *[c for c in ["signup_channel", "country", "signup_date"] if c in client_src.columns]
    )
    .dropDuplicates(["client_id"])
    .withColumn("client_sk", F.xxhash64("client_id"))
    .withColumn("is_current", F.lit(True))
    .withColumn("created_ts", F.current_timestamp())
    .withColumn("updated_ts", F.current_timestamp())
)

# Ensure optional fields exist.
for c, typ in [
    ("signup_channel", "string"),
    ("country", "string"),
    ("signup_date", "date"),
]:
    if c not in client_df.columns:
        client_df = client_df.withColumn(c, F.lit(None).cast(typ))

client_df = client_df.select(
    "client_sk", "client_id", "signup_channel", "country",
    "signup_date", "is_current", "created_ts", "updated_ts"
)

client_df.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD}.dim_client")


# ---------------------------------------------------------------------------
# 3. dim_client_profile_scd2
# ---------------------------------------------------------------------------
#
# The Part 1 CDC pipeline already maintains an SCD2 Silver table. Gold keeps
# the same historical versions and assigns a deterministic surrogate key to
# each (client_id, effective_from_ts) version.

profile = spark.table(f"{SILVER}.dim_client_profile_scd2")

profile_df = (
    profile
    .withColumn(
        "client_profile_sk",
        F.xxhash64("client_id", "effective_from_ts")
    )
    .select(
        "client_profile_sk",
        "client_id",
        *[
            c for c in [
                "account_status",
                "risk_level",
                "profile_segment",
                "email",
                "phone",
                "effective_from_ts",
                "effective_to_ts",
                "is_current",
                "is_deleted",
                "source_lsn",
                "created_ts",
                "updated_ts",
            ]
            if c in profile.columns
        ]
    )
)

# Add absent optional fields for a stable Gold contract.
profile_defaults = {
    "account_status": "string",
    "risk_level": "string",
    "profile_segment": "string",
    "email": "string",
    "phone": "string",
    "effective_from_ts": "timestamp",
    "effective_to_ts": "timestamp",
    "is_current": "boolean",
    "is_deleted": "boolean",
    "source_lsn": "long",
    "created_ts": "timestamp",
    "updated_ts": "timestamp",
}

for c, typ in profile_defaults.items():
    if c not in profile_df.columns:
        profile_df = profile_df.withColumn(c, F.lit(None).cast(typ))

profile_df = profile_df.select(
    "client_profile_sk", "client_id", "account_status", "risk_level",
    "profile_segment", "email", "phone", "effective_from_ts",
    "effective_to_ts", "is_current", "is_deleted", "source_lsn",
    "created_ts", "updated_ts"
)

profile_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{GOLD}.dim_client_profile_scd2"
)


# ---------------------------------------------------------------------------
# 4. dim_payment_method
# ---------------------------------------------------------------------------

deposit = spark.table(f"{SILVER}.silver_client_deposit")

method_col = "payment_method" if "payment_method" in deposit.columns else "method"

payment_df = (
    deposit
    .select(F.col(method_col).cast("string").alias("payment_method_code"))
    .where(F.col("payment_method_code").isNotNull())
    .dropDuplicates()
    .withColumn("payment_method_sk", F.xxhash64("payment_method_code"))
    .withColumn("payment_method_name", F.col("payment_method_code"))
    .withColumn("provider", F.lit(None).cast("string"))
    .withColumn("is_active", F.lit(True))
    .select(
        "payment_method_sk",
        "payment_method_code",
        "payment_method_name",
        "provider",
        "is_active"
    )
)

payment_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{GOLD}.dim_payment_method"
)


# ---------------------------------------------------------------------------
# 5. dim_instrument
# ---------------------------------------------------------------------------

trade = spark.table(f"{SILVER}.silver_client_trades")

if "instrument_id" in trade.columns:
    instrument_df = (
        trade.select("instrument_id")
        .where(F.col("instrument_id").isNotNull())
        .dropDuplicates()
        .withColumn("instrument_sk", F.xxhash64("instrument_id"))
    )

    for c, typ in [
        ("instrument_name", "string"),
        ("instrument_type", "string"),
        ("base_currency", "string"),
        ("quote_currency", "string"),
    ]:
        instrument_df = instrument_df.withColumn(c, F.lit(None).cast(typ))

    instrument_df = (
        instrument_df
        .withColumn("is_active", F.lit(True))
        .select(
            "instrument_sk", "instrument_id", "instrument_name",
            "instrument_type", "base_currency", "quote_currency", "is_active"
        )
    )

    instrument_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{GOLD}.dim_instrument"
    )


# ---------------------------------------------------------------------------
# 6. fact_deposit
# ---------------------------------------------------------------------------
#
# Profile key resolution uses transaction timestamp against SCD2 effective
# windows. This is what preserves historical client attributes.

client = spark.table(f"{GOLD}.dim_client")
profile = spark.table(f"{GOLD}.dim_client_profile_scd2")
payment = spark.table(f"{GOLD}.dim_payment_method")
date_dim = spark.table(f"{GOLD}.dim_date")

d = spark.table(f"{SILVER}.silver_client_deposit").alias("d")

fact_deposit_df = (
    d
    .join(client.alias("c"), F.col("d.client_id") == F.col("c.client_id"), "left")
    .join(
        profile.alias("p"),
        (F.col("d.client_id") == F.col("p.client_id")) &
        (F.to_timestamp(F.col("d.deposit_date")) >= F.col("p.effective_from_ts")) &
        (F.to_timestamp(F.col("d.deposit_date")) < F.col("p.effective_to_ts")),
        "left"
    )
    .join(
        payment.alias("pm"),
        F.col(f"d.{method_col}") == F.col("pm.payment_method_code"),
        "left"
    )
    .join(
        date_dim.alias("dd"),
        F.to_date(F.col("d.deposit_date")) == F.col("dd.calendar_date"),
        "left"
    )
    .withColumn("deposit_sk", F.xxhash64(F.col("d.deposit_id")))
    .select(
        "deposit_sk",
        F.col("d.deposit_id").alias("deposit_id"),
        F.coalesce(F.col("c.client_sk"), F.lit(0)).alias("client_sk"),
        F.coalesce(F.col("p.client_profile_sk"), F.lit(0)).alias("profile_sk"),
        F.coalesce(F.col("dd.date_sk"), F.lit(0)).alias("deposit_date_sk"),
        F.coalesce(F.col("pm.payment_method_sk"), F.lit(0)).alias("payment_method_sk"),
        F.col("d.amount_usd").cast("decimal(18,2)").alias("amount_usd"),
        F.col("d.amount_original").cast("decimal(18,2)").alias("amount_original")
        if "amount_original" in d.columns else
        F.lit(None).cast("decimal(18,2)").alias("amount_original"),
        F.col("d.exchange_rate").cast("decimal(18,8)").alias("exchange_rate")
        if "exchange_rate" in d.columns else
        F.lit(None).cast("decimal(18,8)").alias("exchange_rate"),
        F.col("d.fee_usd").cast("decimal(18,2)").alias("fee_usd")
        if "fee_usd" in d.columns else
        F.lit(None).cast("decimal(18,2)").alias("fee_usd"),
        F.col("d.currency_original").alias("currency_original")
        if "currency_original" in d.columns else
        F.lit(None).cast("string").alias("currency_original"),
        F.col("d.status").alias("status")
        if "status" in d.columns else
        F.lit(None).cast("string").alias("status"),
        F.col("d.processing_days").cast("int").alias("processing_days")
        if "processing_days" in d.columns else
        F.lit(None).cast("int").alias("processing_days"),
        F.to_timestamp(F.col("d.deposit_date")).alias("transaction_ts"),
        F.current_timestamp().alias("loaded_ts"),
    )
    .dropDuplicates(["deposit_id"])
)

fact_deposit_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{GOLD}.fact_deposit"
)


# ---------------------------------------------------------------------------
# 7. fact_trade
# ---------------------------------------------------------------------------

t = spark.table(f"{SILVER}.silver_client_trades").alias("t")

trade_fact = (
    t
    .join(client.alias("c"), F.col("t.client_id") == F.col("c.client_id"), "left")
    .join(
        profile.alias("p"),
        (F.col("t.client_id") == F.col("p.client_id")) &
        (F.to_timestamp(F.col("t.trade_ts")) >= F.col("p.effective_from_ts")) &
        (F.to_timestamp(F.col("t.trade_ts")) < F.col("p.effective_to_ts")),
        "left"
    )
    .join(
        date_dim.alias("dd"),
        F.to_date(F.col("t.trade_ts")) == F.col("dd.calendar_date"),
        "left"
    )
)

if table_exists(f"{GOLD}.dim_instrument") and "instrument_id" in t.columns:
    inst = spark.table(f"{GOLD}.dim_instrument").alias("i")
    trade_fact = trade_fact.join(
        inst,
        F.col("t.instrument_id") == F.col("i.instrument_id"),
        "left"
    )
else:
    trade_fact = trade_fact.withColumn("instrument_sk", F.lit(0).cast("long"))

select_exprs = [
    F.xxhash64(F.col("t.trade_id")).alias("trade_sk"),
    F.col("t.trade_id").alias("trade_id"),
    F.coalesce(F.col("c.client_sk"), F.lit(0)).alias("client_sk"),
    F.coalesce(F.col("p.client_profile_sk"), F.lit(0)).alias("profile_sk"),
    F.coalesce(F.col("dd.date_sk"), F.lit(0)).alias("trade_date_sk"),
]

if "instrument_id" in t.columns and table_exists(f"{GOLD}.dim_instrument"):
    select_exprs.append(F.coalesce(F.col("i.instrument_sk"), F.lit(0)).alias("instrument_sk"))

for c, typ in [
    ("quantity", "decimal(18,8)"),
    ("price", "decimal(18,8)"),
    ("notional_usd", "decimal(18,2)"),
    ("fee_usd", "decimal(18,2)"),
]:
    if c in t.columns:
        select_exprs.append(F.col(f"t.{c}").cast(typ).alias(c))
    else:
        select_exprs.append(F.lit(None).cast(typ).alias(c))

for c in ["side", "status"]:
    if c in t.columns:
        select_exprs.append(F.col(f"t.{c}").alias(c))
    else:
        select_exprs.append(F.lit(None).cast("string").alias(c))

select_exprs.extend([
    F.to_timestamp(F.col("t.trade_ts")).alias("trade_ts")
    if "trade_ts" in t.columns
    else F.lit(None).cast("timestamp").alias("trade_ts"),
    F.current_timestamp().alias("loaded_ts"),
])

trade_fact_df = trade_fact.select(*select_exprs).dropDuplicates(["trade_id"])

trade_fact_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{GOLD}.fact_trade"
)

print("Part 2 Gold dimensional model created successfully.")
