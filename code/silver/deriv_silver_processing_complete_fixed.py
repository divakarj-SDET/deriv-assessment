# Databricks / PySpark
# DERIV Assessment - Complete Silver Processing
#
# Purpose
# -------
# 1. Process batch JSON Bronze tables into canonical Silver tables.
# 2. Process vendor deposit Bronze stream using Auto Loader -> Bronze -> foreachBatch -> Silver.
# 3. Handle vendor schema drift: payment_method -> method.
# 4. Deduplicate by business key.
# 5. Apply DQ rules and quarantine invalid records.
# 6. Process client profile CDC into an SCD Type 2 Silver dimension.
#
# IMPORTANT
# ---------
# This file is intentionally plain Python.
# Do NOT put Markdown, Mermaid, or ``` fences inside this file.
# Paste/upload it as a Databricks Python source file or import it into a notebook.
#
# Unity Catalog:
# - Use _metadata.file_path when file metadata is required.
# - Do not use input_file_name() for UC Volumes.
#
# Streaming:
# - availableNow=True is useful for the assessment because it processes currently
#   available files and then terminates.
# - For continuous production processing, replace availableNow=True with an
#   appropriate processing trigger.
#
# Silver contract:
# - Bronze-only fields such as _corrupt_record and _rescued_data are NOT copied
#   into the Silver business contract.
# - _source_file is retained as technical lineage metadata.
# - _source_file_modification_time is retained only if it already exists in the
#   Silver target schema; see the explicit mapping below.

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime


# =============================================================================
# 0. CONFIGURATION
# =============================================================================

CATALOG = "workspace"

BRONZE_SCHEMA = f"{CATALOG}.deriv_assement_bronze"
SILVER_SCHEMA = f"{CATALOG}.deriv_assement_silver"

BATCH_ROOT = "/Volumes/workspace/deriv_assement/data/batch/"
STREAM_ROOT = "/Volumes/workspace/deriv_assement/data/stream/"

# Bronze batch tables
BRONZE_SIGNUP = f"{BRONZE_SCHEMA}.bronze_client_signup"
BRONZE_PROFILE = f"{BRONZE_SCHEMA}.bronze_client_profile"
BRONZE_DEPOSIT = f"{BRONZE_SCHEMA}.bronze_client_deposit"
BRONZE_TRADES = f"{BRONZE_SCHEMA}.bronze_client_trades"

# Bronze streaming tables
STREAM_DEPOSIT_BRONZE = f"{BRONZE_SCHEMA}.stream_client_deposits"
STREAM_CDC_BRONZE = f"{BRONZE_SCHEMA}.stream_client_profile_cdc"

# Silver tables
SILVER_SIGNUP = f"{SILVER_SCHEMA}.silver_client_signup"
SILVER_PROFILE_CURRENT = f"{SILVER_SCHEMA}.silver_client_profile"
SILVER_DEPOSIT = f"{SILVER_SCHEMA}.silver_client_deposit"
SILVER_TRADES = f"{SILVER_SCHEMA}.silver_client_trades"

SILVER_PROFILE_SCD2 = f"{SILVER_SCHEMA}.dim_client_profile_scd2"

# Quarantine / audit
QUARANTINE_DEPOSIT = f"{SILVER_SCHEMA}.quarantine_client_deposits"
QUARANTINE_TRADE = f"{SILVER_SCHEMA}.quarantine_client_trades"
QUARANTINE_PROFILE_CDC = f"{SILVER_SCHEMA}.quarantine_profile_cdc"

CDC_APPLIED_EVENTS = f"{SILVER_SCHEMA}.cdc_applied_events"
SILVER_LOAD_AUDIT = f"{SILVER_SCHEMA}.silver_load_audit"

# Streaming checkpoints
SILVER_STREAM_CHECKPOINT = (
    f"{STREAM_ROOT}_silver_checkpoints/client_deposits/"
)

CDC_STREAM_CHECKPOINT = (
    f"{STREAM_ROOT}_silver_checkpoints/client_profile_cdc/"
)


# =============================================================================
# 1. CREATE SCHEMAS
# =============================================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")


# =============================================================================
# 2. HELPERS
# =============================================================================

def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def add_if_missing(df: DataFrame, column_name: str, data_type: str) -> DataFrame:
    if column_name not in df.columns:
        return df.withColumn(column_name, F.lit(None).cast(data_type))
    return df


def ensure_columns(df: DataFrame, columns_and_types: dict) -> DataFrame:
    for name, data_type in columns_and_types.items():
        df = add_if_missing(df, name, data_type)
    return df


def write_quarantine(
    df: DataFrame,
    table_name: str,
    batch_id=None,
    reason: str = None
):
    if df is None:
        return

    if df.limit(1).count() == 0:
        return

    qdf = df

    if reason is not None:
        qdf = qdf.withColumn("_dq_reason", F.lit(reason))

    qdf = (
        qdf
        .withColumn("_dq_batch_id", F.lit(batch_id).cast("long"))
        .withColumn("_dq_created_ts", F.current_timestamp())
    )

    (
        qdf.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )


def merge_explicit(
    source_df: DataFrame,
    target_table: str,
    merge_condition: str,
    update_map: dict,
    insert_map: dict
):
    """
    Explicit Delta MERGE.

    Never use whenMatchedUpdateAll()/whenNotMatchedInsertAll() in the Silver
    layer because Bronze often contains technical/rescued fields that are not
    part of the canonical Silver schema.
    """

    target = DeltaTable.forName(spark, target_table)

    (
        target.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )


# =============================================================================
# 3. BATCH SIGNUP -> SILVER
# =============================================================================

def process_batch_signup():

    if not table_exists(BRONZE_SIGNUP):
        print(f"Skipping: {BRONZE_SIGNUP} does not exist")
        return

    df = spark.table(BRONZE_SIGNUP)

    required = ["client_id"]

    for c in required:
        if c not in df.columns:
            raise ValueError(f"Required column missing from signup: {c}")

    df = ensure_columns(
        df,
        {
            "signup_channel": "string",
            "country": "string",
            "signup_date": "date",
            "_source_file": "string",
            "delta_created_ts": "timestamp",
            "delta_created_dt": "date",
        },
    )

    valid = (
        df
        .filter(F.col("client_id").isNotNull())
        .dropDuplicates(["client_id"])
        .select(
            "client_id",
            "signup_channel",
            "country",
            "signup_date",
            "_source_file",
            "delta_created_ts",
            "delta_created_dt",
        )
    )

    if not table_exists(SILVER_SIGNUP):
        valid.write.format("delta").mode("overwrite").saveAsTable(SILVER_SIGNUP)
    else:
        merge_explicit(
            valid,
            SILVER_SIGNUP,
            "t.client_id = s.client_id",
            {
                "signup_channel": "s.signup_channel",
                "country": "s.country",
                "signup_date": "s.signup_date",
                "_source_file": "s._source_file",
                "delta_created_ts": "s.delta_created_ts",
                "delta_created_dt": "s.delta_created_dt",
            },
            {
                "client_id": "s.client_id",
                "signup_channel": "s.signup_channel",
                "country": "s.country",
                "signup_date": "s.signup_date",
                "_source_file": "s._source_file",
                "delta_created_ts": "s.delta_created_ts",
                "delta_created_dt": "s.delta_created_dt",
            },
        )


# =============================================================================
# 4. BATCH PROFILE -> SILVER CURRENT
# =============================================================================

def process_batch_profile():

    if not table_exists(BRONZE_PROFILE):
        print(f"Skipping: {BRONZE_PROFILE} does not exist")
        return

    df = spark.table(BRONZE_PROFILE)

    if "client_id" not in df.columns:
        raise ValueError("Required column missing from profile: client_id")

    df = ensure_columns(
        df,
        {
            "account_status": "string",
            "risk_level": "string",
            "profile_segment": "string",
            "email": "string",
            "phone": "string",
            "_source_file": "string",
            "delta_created_ts": "timestamp",
            "delta_created_dt": "date",
        },
    )

    valid = (
        df
        .filter(F.col("client_id").isNotNull())
        .dropDuplicates(["client_id"])
        .select(
            "client_id",
            "account_status",
            "risk_level",
            "profile_segment",
            "email",
            "phone",
            "_source_file",
            "delta_created_ts",
            "delta_created_dt",
        )
    )

    if not table_exists(SILVER_PROFILE_CURRENT):
        valid.write.format("delta").mode("overwrite").saveAsTable(
            SILVER_PROFILE_CURRENT
        )
    else:
        merge_explicit(
            valid,
            SILVER_PROFILE_CURRENT,
            "t.client_id = s.client_id",
            {
                "account_status": "s.account_status",
                "risk_level": "s.risk_level",
                "profile_segment": "s.profile_segment",
                "email": "s.email",
                "phone": "s.phone",
                "_source_file": "s._source_file",
                "delta_created_ts": "s.delta_created_ts",
                "delta_created_dt": "s.delta_created_dt",
            },
            {
                "client_id": "s.client_id",
                "account_status": "s.account_status",
                "risk_level": "s.risk_level",
                "profile_segment": "s.profile_segment",
                "email": "s.email",
                "phone": "s.phone",
                "_source_file": "s._source_file",
                "delta_created_ts": "s.delta_created_ts",
                "delta_created_dt": "s.delta_created_dt",
            },
        )


# =============================================================================
# 5. BATCH DEPOSITS -> SILVER
# =============================================================================

def canonicalize_deposit(df: DataFrame) -> DataFrame:

    # Vendor schema drift:
    # 20240301 / 20240303: payment_method
    # 20240302: method
    if "payment_method" not in df.columns and "method" in df.columns:
        df = df.withColumnRenamed("method", "payment_method")

    df = ensure_columns(
        df,
        {
            "deposit_id": "string",
            "client_id": "string",
            "deposit_date": "date",
            "amount_usd": "decimal(18,2)",
            "payment_method": "string",
            "currency_original": "string",
            "exchange_rate": "decimal(18,8)",
            "status": "string",
            "processing_days": "int",
            "fee_usd": "decimal(18,2)",
            "_source_file": "string",
            "_source_file_modification_time": "timestamp",
            "delta_created_ts": "timestamp",
            "delta_created_dt": "date",
        },
    )

    return df.select(
        F.col("deposit_id").cast("string").alias("deposit_id"),
        F.col("client_id").cast("string").alias("client_id"),
        F.to_date("deposit_date").alias("deposit_date"),
        F.col("amount_usd").cast("decimal(18,2)").alias("amount_usd"),
        F.col("payment_method").cast("string").alias("payment_method"),
        F.col("currency_original").cast("string").alias("currency_original"),
        F.col("exchange_rate").cast("decimal(18,8)").alias("exchange_rate"),
        F.col("status").cast("string").alias("status"),
        F.col("processing_days").cast("int").alias("processing_days"),
        F.col("fee_usd").cast("decimal(18,2)").alias("fee_usd"),
        F.col("_source_file").cast("string").alias("_source_file"),
        F.col("_source_file_modification_time").cast("timestamp").alias(
            "_source_file_modification_time"
        ),
        F.col("delta_created_ts").cast("timestamp").alias("delta_created_ts"),
        F.col("delta_created_dt").cast("date").alias("delta_created_dt"),
    )


def process_batch_deposits():

    if not table_exists(BRONZE_DEPOSIT):
        print(f"Skipping: {BRONZE_DEPOSIT} does not exist")
        return

    raw = spark.table(BRONZE_DEPOSIT)
    df = canonicalize_deposit(raw)

    # Deduplicate deterministically.
    w = Window.partitionBy("deposit_id").orderBy(
        F.col("delta_created_ts").desc_nulls_last(),
        F.col("_source_file").desc_nulls_last(),
    )

    df = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # DQ
    invalid = (
        F.col("deposit_id").isNull()
        | F.col("client_id").isNull()
        | F.col("amount_usd").isNull()
        | (F.col("amount_usd") < 0)
    )

    bad = df.filter(invalid)
    good = df.filter(~invalid)

    write_quarantine(
        bad,
        QUARANTINE_DEPOSIT,
        reason="Missing required field or negative amount"
    )

    # Referential integrity.
    if table_exists(SILVER_SIGNUP):
        clients = spark.table(SILVER_SIGNUP).select("client_id").dropDuplicates()

        unknown = (
            good.alias("d")
            .join(clients.alias("c"), "client_id", "left_anti")
        )

        good = (
            good
            .join(clients, "client_id", "left_semi")
        )

        write_quarantine(
            unknown,
            QUARANTINE_DEPOSIT,
            reason="Unknown client_id"
        )

    # Create canonical table.
    # IMPORTANT: only create Silver from selected canonical columns.
    if not table_exists(SILVER_DEPOSIT):
        good.write.format("delta").mode("overwrite").saveAsTable(
            SILVER_DEPOSIT
        )
        return

    target_cols = set(spark.table(SILVER_DEPOSIT).columns)

    # Build mappings only for columns that physically exist in the target.
    update_map = {}
    insert_map = {}

    canonical_cols = [
        "client_id",
        "deposit_date",
        "amount_usd",
        "payment_method",
        "currency_original",
        "exchange_rate",
        "status",
        "processing_days",
        "fee_usd",
        "_source_file",
        "delta_created_ts",
        "delta_created_dt",
    ]

    for c in canonical_cols:
        if c in target_cols:
            update_map[c] = f"s.{c}"
            insert_map[c] = f"s.{c}"

    # Optional technical metadata.
    # Only map it if the target actually contains it.
    if "_source_file_modification_time" in target_cols:
        update_map["_source_file_modification_time"] = (
            "s._source_file_modification_time"
        )
        insert_map["_source_file_modification_time"] = (
            "s._source_file_modification_time"
        )

    # This protects against target/source schema mismatch.
    source_cols = set(good.columns)

    update_map = {
        k: v for k, v in update_map.items()
        if k in source_cols
    }

    insert_map = {
        k: v for k, v in insert_map.items()
        if k in source_cols
    }

    merge_explicit(
        good,
        SILVER_DEPOSIT,
        "t.deposit_id = s.deposit_id",
        update_map,
        {
            "deposit_id": "s.deposit_id",
            **insert_map,
        },
    )


# =============================================================================
# 6. BATCH TRADES -> SILVER
# =============================================================================

def process_batch_trades():

    if not table_exists(BRONZE_TRADES):
        print(f"Skipping: {BRONZE_TRADES} does not exist")
        return

    df = spark.table(BRONZE_TRADES)

    if "trade_id" not in df.columns or "client_id" not in df.columns:
        raise ValueError(
            "Trades require trade_id and client_id"
        )

    df = ensure_columns(
        df,
        {
            "trade_ts": "timestamp",
            "instrument_id": "string",
            "quantity": "decimal(18,8)",
            "price": "decimal(18,8)",
            "notional_usd": "decimal(18,2)",
            "fee_usd": "decimal(18,2)",
            "side": "string",
            "status": "string",
            "_source_file": "string",
            "delta_created_ts": "timestamp",
            "delta_created_dt": "date",
        },
    )

    valid = (
        df
        .filter(
            F.col("trade_id").isNotNull()
            & F.col("client_id").isNotNull()
        )
        .dropDuplicates(["trade_id"])
        .select(
            "trade_id",
            "client_id",
            "trade_ts",
            "instrument_id",
            "quantity",
            "price",
            "notional_usd",
            "fee_usd",
            "side",
            "status",
            "_source_file",
            "delta_created_ts",
            "delta_created_dt",
        )
    )

    if table_exists(SILVER_SIGNUP):
        clients = spark.table(SILVER_SIGNUP).select("client_id").dropDuplicates()

        unknown = valid.join(clients, "client_id", "left_anti")
        valid = valid.join(clients, "client_id", "left_semi")

        write_quarantine(
            unknown,
            QUARANTINE_TRADE,
            reason="Unknown client_id"
        )

    if not table_exists(SILVER_TRADES):
        valid.write.format("delta").mode("overwrite").saveAsTable(
            SILVER_TRADES
        )
    else:
        merge_explicit(
            valid,
            SILVER_TRADES,
            "t.trade_id = s.trade_id",
            {
                "client_id": "s.client_id",
                "trade_ts": "s.trade_ts",
                "instrument_id": "s.instrument_id",
                "quantity": "s.quantity",
                "price": "s.price",
                "notional_usd": "s.notional_usd",
                "fee_usd": "s.fee_usd",
                "side": "s.side",
                "status": "s.status",
                "_source_file": "s._source_file",
                "delta_created_ts": "s.delta_created_ts",
                "delta_created_dt": "s.delta_created_dt",
            },
            {
                "trade_id": "s.trade_id",
                "client_id": "s.client_id",
                "trade_ts": "s.trade_ts",
                "instrument_id": "s.instrument_id",
                "quantity": "s.quantity",
                "price": "s.price",
                "notional_usd": "s.notional_usd",
                "fee_usd": "s.fee_usd",
                "side": "s.side",
                "status": "s.status",
                "_source_file": "s._source_file",
                "delta_created_ts": "s.delta_created_ts",
                "delta_created_dt": "s.delta_created_dt",
            },
        )


# =============================================================================
# 7. STREAMING VENDOR DEPOSITS -> SILVER
# =============================================================================

def process_stream_deposit_batch(
    batch_df: DataFrame,
    batch_id: int
):
    """
    Called once per streaming microbatch.

    The key fix for the current error:
    -----------------------------------
    We NEVER call:
        whenMatchedUpdateAll()
        whenNotMatchedInsertAll()

    because the Bronze stream contains:
        _corrupt_record
        _source_file_modification_time
        other technical columns

    while Silver has a controlled canonical schema.

    Explicit mappings make the MERGE resilient to this mismatch.
    """

    if batch_df.isEmpty():
        return

    # -------------------------------------------------------------------------
    # Normalize vendor schema drift
    # -------------------------------------------------------------------------

    df = canonicalize_deposit(batch_df)

    # -------------------------------------------------------------------------
    # Deterministic duplicate handling
    # -------------------------------------------------------------------------

    w = Window.partitionBy("deposit_id").orderBy(
        F.col("delta_created_ts").desc_nulls_last(),
        F.col("_source_file").desc_nulls_last(),
        F.col("_source_file_modification_time").desc_nulls_last(),
    )

    df = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # -------------------------------------------------------------------------
    # Data quality
    # -------------------------------------------------------------------------

    invalid_required = (
        F.col("deposit_id").isNull()
        | F.col("client_id").isNull()
        | F.col("amount_usd").isNull()
    )

    negative_amount = F.col("amount_usd") < 0

    bad_required = df.filter(invalid_required)
    bad_negative = df.filter(
        (~invalid_required) & negative_amount
    )

    good = df.filter(
        (~invalid_required) & (~negative_amount)
    )

    write_quarantine(
        bad_required,
        QUARANTINE_DEPOSIT,
        batch_id=batch_id,
        reason="Missing required deposit fields"
    )

    write_quarantine(
        bad_negative,
        QUARANTINE_DEPOSIT,
        batch_id=batch_id,
        reason="Negative deposit amount"
    )

    # -------------------------------------------------------------------------
    # Referential integrity
    # -------------------------------------------------------------------------

    if table_exists(SILVER_SIGNUP):

        clients = (
            spark.table(SILVER_SIGNUP)
            .select("client_id")
            .where(F.col("client_id").isNotNull())
            .dropDuplicates()
        )

        unknown = (
            good
            .join(clients, "client_id", "left_anti")
        )

        good = (
            good
            .join(clients, "client_id", "left_semi")
        )

        write_quarantine(
            unknown,
            QUARANTINE_DEPOSIT,
            batch_id=batch_id,
            reason="Unknown client_id"
        )

    if good.isEmpty():
        return

    # -------------------------------------------------------------------------
    # First execution: create canonical Silver table.
    # -------------------------------------------------------------------------

    if not table_exists(SILVER_DEPOSIT):

        # DO NOT WRITE Bronze-only fields.
        create_df = good.select(
            "deposit_id",
            "client_id",
            "deposit_date",
            "amount_usd",
            "payment_method",
            "currency_original",
            "exchange_rate",
            "status",
            "processing_days",
            "fee_usd",
            "_source_file",
            "delta_created_ts",
            "delta_created_dt",
        )

        (
            create_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_DEPOSIT)
        )

        return

    # -------------------------------------------------------------------------
    # Existing Silver target
    # -------------------------------------------------------------------------

    target_cols = set(
        spark.table(SILVER_DEPOSIT).columns
    )

    source_cols = set(good.columns)

    # -------------------------------------------------------------------------
    # Explicit update/insert mappings.
    #
    # These are the ONLY business columns allowed to flow into Silver.
    # -------------------------------------------------------------------------

    business_cols = [
        "client_id",
        "deposit_date",
        "amount_usd",
        "payment_method",
        "currency_original",
        "exchange_rate",
        "status",
        "processing_days",
        "fee_usd",
        "_source_file",
        "delta_created_ts",
        "delta_created_dt",
    ]

    update_map = {}
    insert_map = {
        "deposit_id": "s.deposit_id"
    }

    for col_name in business_cols:

        if (
            col_name in target_cols
            and col_name in source_cols
        ):
            update_map[col_name] = f"s.{col_name}"
            insert_map[col_name] = f"s.{col_name}"

    # -------------------------------------------------------------------------
    # OPTIONAL metadata
    #
    # This line is the important fix for the error you received.
    #
    # If target has _source_file_modification_time, map it explicitly.
    # If target does not have it, DO NOT reference it.
    # -------------------------------------------------------------------------

    if (
        "_source_file_modification_time" in target_cols
        and "_source_file_modification_time" in source_cols
    ):
        update_map[
            "_source_file_modification_time"
        ] = "s._source_file_modification_time"

        insert_map[
            "_source_file_modification_time"
        ] = "s._source_file_modification_time"

    # -------------------------------------------------------------------------
    # MERGE
    # -------------------------------------------------------------------------

    merge_explicit(
        good,
        SILVER_DEPOSIT,
        "t.deposit_id = s.deposit_id",
        update_map,
        insert_map,
    )

    # -------------------------------------------------------------------------
    # Audit
    # -------------------------------------------------------------------------

    audit = spark.createDataFrame(
        [
            (
                "stream_client_deposits",
                int(batch_id),
                datetime.utcnow(),
                "SUCCESS",
            )
        ],
        [
            "pipeline_name",
            "batch_id",
            "processed_ts",
            "status",
        ],
    )

    (
        audit.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(SILVER_LOAD_AUDIT)
    )


# =============================================================================
# 8. STREAMING CDC -> SCD2
# =============================================================================

def create_cdc_tables():

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CDC_APPLIED_EVENTS} (
            lsn BIGINT,
            client_id STRING,
            applied_ts TIMESTAMP
        )
        USING DELTA
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_PROFILE_SCD2} (
            client_profile_sk BIGINT,
            client_id STRING,
            account_status STRING,
            risk_level STRING,
            profile_segment STRING,
            email STRING,
            phone STRING,
            effective_from_ts TIMESTAMP,
            effective_to_ts TIMESTAMP,
            is_current BOOLEAN,
            is_deleted BOOLEAN,
            source_lsn BIGINT,
            created_ts TIMESTAMP,
            updated_ts TIMESTAMP
        )
        USING DELTA
        """
    )


def process_profile_cdc_batch(
    batch_df: DataFrame,
    batch_id: int
):

    if batch_df.isEmpty():
        return

    create_cdc_tables()

    # Required CDC columns
    required = [
        "lsn",
        "commit_ts",
        "op",
        "client_id",
    ]

    missing = [
        c for c in required
        if c not in batch_df.columns
    ]

    if missing:
        raise ValueError(
            f"CDC required columns missing: {missing}"
        )

    df = ensure_columns(
        batch_df,
        {
            "before": "string",
            "after": "string",
        }
    )

    # -------------------------------------------------------------------------
    # Remove already applied LSNs.
    # -------------------------------------------------------------------------

    applied = (
        spark.table(CDC_APPLIED_EVENTS)
        .select("lsn")
        .dropDuplicates()
    )

    pending = (
        df
        .join(applied, "lsn", "left_anti")
        .orderBy("client_id", "lsn")
    )

    if pending.isEmpty():
        return

    # -------------------------------------------------------------------------
    # The assessment dataset is small. We process events in LSN order.
    #
    # Production note:
    # For very high CDC volume, replace driver-side sequential processing with
    # scalable stateful processing / a CDC framework. This keeps the assessment
    # implementation deterministic and easy to audit.
    # -------------------------------------------------------------------------

    rows = (
        pending
        .select(
            "lsn",
            "commit_ts",
            "op",
            "client_id",
            "before",
            "after",
        )
        .orderBy("client_id", "lsn")
        .collect()
    )

    target = DeltaTable.forName(
        spark,
        SILVER_PROFILE_SCD2
    )

    high_ts = "9999-12-31 23:59:59"

    for row in rows:

        lsn = int(row["lsn"])
        client_id = row["client_id"]
        op = str(row["op"]).lower()
        commit_ts = row["commit_ts"]

        # ---------------------------------------------------------------------
        # Current version
        # ---------------------------------------------------------------------

        current_rows = (
            spark.table(SILVER_PROFILE_SCD2)
            .filter(
                (F.col("client_id") == client_id)
                & (F.col("is_current") == True)
            )
            .orderBy(F.col("source_lsn").desc_nulls_last())
            .limit(1)
            .collect()
        )

        current = current_rows[0] if current_rows else None

        # Reject stale LSN.
        if current is not None:
            current_lsn = current["source_lsn"]

            if (
                current_lsn is not None
                and lsn <= current_lsn
            ):
                write_quarantine(
                    spark.createDataFrame(
                        [row.asDict()],
                        pending.schema
                    ),
                    QUARANTINE_PROFILE_CDC,
                    batch_id=batch_id,
                    reason="Stale or out-of-order LSN"
                )
                continue

        # ---------------------------------------------------------------------
        # Parse after image
        # ---------------------------------------------------------------------

        after_json = row["after"]

        # We use a small JSON parser based on schema inference for the
        # assessment. A production pipeline should use an explicit CDC schema.
        after_values = {}

        if after_json:
            after_df = spark.read.json(
                spark.sparkContext.parallelize([after_json])
            )

            if after_df.limit(1).count() > 0:
                after_values = after_df.first().asDict()

        def value(name, default=None):
            return after_values.get(name, default)

        # ---------------------------------------------------------------------
        # INSERT / UPDATE
        # ---------------------------------------------------------------------

        if op in ("insert", "update"):

            # Close current version.
            if current is not None:

                (
                    target.alias("t")
                    .merge(
                        spark.createDataFrame(
                            [(client_id,)],
                            ["client_id"]
                        ).alias("s"),
                        "t.client_id = s.client_id"
                        " AND t.is_current = true"
                    )
                    .whenMatchedUpdate(
                        set={
                            "effective_to_ts": F.lit(commit_ts),
                            "is_current": F.lit(False),
                            "updated_ts": F.current_timestamp(),
                        }
                    )
                    .execute()
                )

            new_sk = hash(
                (
                    client_id,
                    str(commit_ts),
                    lsn,
                )
            )

            new_row = spark.createDataFrame(
                [
                    (
                        new_sk,
                        client_id,
                        value("account_status"),
                        value("risk_level"),
                        value("profile_segment"),
                        value("email"),
                        value("phone"),
                        commit_ts,
                        datetime(9999, 12, 31, 23, 59, 59),
                        True,
                        False,
                        lsn,
                        datetime.utcnow(),
                        datetime.utcnow(),
                    )
                ],
                [
                    "client_profile_sk",
                    "client_id",
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
                ],
            )

            (
                new_row.write
                .format("delta")
                .mode("append")
                .saveAsTable(SILVER_PROFILE_SCD2)
            )

        # ---------------------------------------------------------------------
        # DELETE = soft delete
        # ---------------------------------------------------------------------

        elif op == "delete":

            if current is not None:

                (
                    target.alias("t")
                    .merge(
                        spark.createDataFrame(
                            [(client_id,)],
                            ["client_id"]
                        ).alias("s"),
                        "t.client_id = s.client_id"
                        " AND t.is_current = true"
                    )
                    .whenMatchedUpdate(
                        set={
                            "effective_to_ts": F.lit(commit_ts),
                            "is_current": F.lit(False),
                            "updated_ts": F.current_timestamp(),
                        }
                    )
                    .execute()
                )

            new_sk = hash(
                (
                    client_id,
                    str(commit_ts),
                    lsn,
                    "DELETE",
                )
            )

            delete_row = spark.createDataFrame(
                [
                    (
                        new_sk,
                        client_id,
                        "deleted",
                        None,
                        None,
                        None,
                        None,
                        commit_ts,
                        datetime(9999, 12, 31, 23, 59, 59),
                        True,
                        True,
                        lsn,
                        datetime.utcnow(),
                        datetime.utcnow(),
                    )
                ],
                [
                    "client_profile_sk",
                    "client_id",
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
                ],
            )

            (
                delete_row.write
                .format("delta")
                .mode("append")
                .saveAsTable(SILVER_PROFILE_SCD2)
            )

        else:
            write_quarantine(
                spark.createDataFrame(
                    [row.asDict()],
                    pending.schema
                ),
                QUARANTINE_PROFILE_CDC,
                batch_id=batch_id,
                reason=f"Unsupported CDC operation: {op}"
            )
            continue

        # ---------------------------------------------------------------------
        # Record applied LSN.
        # ---------------------------------------------------------------------

        spark.createDataFrame(
            [
                (
                    lsn,
                    client_id,
                    datetime.utcnow(),
                )
            ],
            [
                "lsn",
                "client_id",
                "applied_ts",
            ],
        ).write.format("delta").mode("append").saveAsTable(
            CDC_APPLIED_EVENTS
        )

    print(
        f"CDC batch {batch_id}: processed {len(rows)} pending events"
    )


# =============================================================================
# 9. RUN BATCH PIPELINES
# =============================================================================
#
# Uncomment when you want to run batch Silver processing.
#
# process_batch_signup()
# process_batch_profile()
# process_batch_deposits()
# process_batch_trades()


# =============================================================================
# 10. RUN STREAMING VENDOR DEPOSITS
# =============================================================================
#
# Keep this section at the bottom of the .py file.
#
# The important part is that process_stream_deposit_batch() now uses explicit
# MERGE mappings, so _source_file_modification_time is referenced only when
# that column actually exists in the Silver target.
#
# If your current failed query has an old checkpoint, normally keep the same
# checkpoint when only correcting application code. Structured Streaming can
# retry the failed microbatch. If the source/target contract itself has been
# deliberately changed, use a new checkpoint after evaluating replay impact.

deposit_silver_query = (
    spark.readStream
    .table(STREAM_DEPOSIT_BRONZE)
    .writeStream
    .foreachBatch(process_stream_deposit_batch)
    .option("checkpointLocation", SILVER_STREAM_CHECKPOINT)
    .queryName("deriv_silver_vendor_deposits")
    .trigger(availableNow=True)
    .start()
)

deposit_silver_query.awaitTermination()


# =============================================================================
# 11. RUN STREAMING CDC
# =============================================================================
#
# Uncomment only when the CDC Bronze stream is ready.
#
# cdc_query = (
#     spark.readStream
#     .table(STREAM_CDC_BRONZE)
#     .writeStream
#     .foreachBatch(process_profile_cdc_batch)
#     .option("checkpointLocation", CDC_STREAM_CHECKPOINT)
#     .queryName("deriv_silver_client_profile_cdc")
#     .trigger(availableNow=True)
#     .start()
# )
#
# cdc_query.awaitTermination()
