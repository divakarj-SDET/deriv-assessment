# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Deriv Assessment - Batch JSON to Bronze
# MAGIC
# MAGIC Loads batch JSON files from Unity Catalog Volumes into Bronze Delta tables.\n# MAGIC\n# MAGIC Source folders: `client_signup`, `client_profile`, `client_deposit`, and `client_trades`.
# MAGIC
# MAGIC Features:
# MAGIC - Adds `delta_created_ts` and `delta_created_dt`
# MAGIC - Partitions Bronze tables by both requested columns
# MAGIC - Maintains one latest-successful-load row per Bronze table in a master table
# MAGIC - Maintains an append-only load history table for every load attempt
# MAGIC - Captures source file, record count, load status, timestamps, and errors
# MAGIC - Master timestamp advances only after a successful Bronze write\n# MAGIC - Uses `_metadata.file_path` for Unity Catalog-compatible file lineage

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
import uuid
import traceback

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "workspace"
BRONZE_SCHEMA = "deriv_assement_bronze"

BASE_PATH = "/Volumes/workspace/deriv_assement/data/batch"

MASTER_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_load_master"
HISTORY_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_load_history"

TABLE_CONFIG = [
    {
        "table_name": "client_signup",
        "source_path": f"{BASE_PATH}/client_signup/*.json",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.client_signup",
    },
    {
        "table_name": "client_profile",
        "source_path": f"{BASE_PATH}/client_profile/*.json",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.client_profile",
    },
    {
        "table_name": "client_deposit",
        "source_path": f"{BASE_PATH}/client_deposit/*.json",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.client_deposit",
    },
    {
        "table_name": "client_trades",
        "source_path": f"{BASE_PATH}/client_trades/*.json",
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.client_trades",
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze schema and control tables

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MASTER_TABLE} (
    table_name STRING,
    source_path STRING,
    latest_loaded_ts TIMESTAMP,
    latest_loaded_dt DATE,
    last_load_status STRING,
    records_loaded BIGINT,
    last_source_file STRING,
    load_start_ts TIMESTAMP,
    load_end_ts TIMESTAMP,
    error_message STRING
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
    load_id STRING,
    table_name STRING,
    source_path STRING,
    load_start_ts TIMESTAMP,
    load_end_ts TIMESTAMP,
    delta_created_ts TIMESTAMP,
    delta_created_dt DATE,
    records_loaded BIGINT,
    source_files STRING,
    load_status STRING,
    error_message STRING
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper functions

# COMMAND ----------

def append_load_history(
    load_id,
    table_name,
    source_path,
    load_start_ts,
    load_end_ts,
    delta_created_ts,
    records_loaded,
    source_files,
    load_status,
    error_message=None,
):
    """Append one immutable audit row for a load attempt."""

    history_df = spark.createDataFrame(
        [(
            load_id,
            table_name,
            source_path,
            load_start_ts,
            load_end_ts,
            delta_created_ts,
            records_loaded,
            source_files,
            load_status,
            error_message,
        )],
        """
        load_id string,
        table_name string,
        source_path string,
        load_start_ts timestamp,
        load_end_ts timestamp,
        delta_created_ts timestamp,
        records_loaded long,
        source_files string,
        load_status string,
        error_message string
        """
    ).withColumn(
        "delta_created_dt",
        F.to_date("delta_created_ts")
    ).select(
        "load_id",
        "table_name",
        "source_path",
        "load_start_ts",
        "load_end_ts",
        "delta_created_ts",
        "delta_created_dt",
        "records_loaded",
        "source_files",
        "load_status",
        "error_message",
    )

    history_df.write.format("delta").mode("append").saveAsTable(HISTORY_TABLE)


def update_master_success(
    table_name,
    source_path,
    delta_created_ts,
    records_loaded,
    last_source_file,
    load_start_ts,
    load_end_ts,
):
    """
    Maintain exactly one latest-successful-load row per Bronze table.
    The latest_loaded_ts is updated only after a successful Bronze write.
    """

    master_update_df = spark.createDataFrame(
        [(
            table_name,
            source_path,
            delta_created_ts,
            "SUCCESS",
            records_loaded,
            last_source_file,
            load_start_ts,
            load_end_ts,
            None,
        )],
        """
        table_name string,
        source_path string,
        latest_loaded_ts timestamp,
        last_load_status string,
        records_loaded long,
        last_source_file string,
        load_start_ts timestamp,
        load_end_ts timestamp,
        error_message string
        """
    ).withColumn(
        "latest_loaded_dt",
        F.to_date("latest_loaded_ts")
    )

    master_delta = DeltaTable.forName(spark, MASTER_TABLE)

    (
        master_delta.alias("t")
        .merge(
            master_update_df.alias("s"),
            "t.table_name = s.table_name"
        )
        .whenMatchedUpdate(set={
            "source_path": "s.source_path",
            "latest_loaded_ts": "s.latest_loaded_ts",
            "latest_loaded_dt": "s.latest_loaded_dt",
            "last_load_status": "s.last_load_status",
            "records_loaded": "s.records_loaded",
            "last_source_file": "s.last_source_file",
            "load_start_ts": "s.load_start_ts",
            "load_end_ts": "s.load_end_ts",
            "error_message": "s.error_message",
        })
        .whenNotMatchedInsert(values={
            "table_name": "s.table_name",
            "source_path": "s.source_path",
            "latest_loaded_ts": "s.latest_loaded_ts",
            "latest_loaded_dt": "s.latest_loaded_dt",
            "last_load_status": "s.last_load_status",
            "records_loaded": "s.records_loaded",
            "last_source_file": "s.last_source_file",
            "load_start_ts": "s.load_start_ts",
            "load_end_ts": "s.load_end_ts",
            "error_message": "s.error_message",
        })
        .execute()
    )


def load_json_to_bronze(table_name, source_path, target_table):
    load_id = str(uuid.uuid4())

    # Use Spark timestamps so that load audit timestamps are generated consistently.
    load_start_ts = spark.sql(
        "SELECT current_timestamp() AS ts"
    ).first()["ts"]

    delta_created_ts = load_start_ts

    try:
        print("=" * 100)
        print(f"Load ID      : {load_id}")
        print(f"Table        : {table_name}")
        print(f"Source       : {source_path}")
        print(f"Target       : {target_table}")

        # Read all JSON files matching the configured path.
        source_df = (
            spark.read
            .option("multiLine", "false")
            .json(source_path)
        )

        # Capture source file before adding Bronze metadata.
        source_df = source_df.withColumn(
            "_source_file",
            F.col("_metadata.file_path")
        )

        # Materialize once for audit information.
        source_files = [
            row["_source_file"]
            for row in (
                source_df
                .select("_source_file")
                .distinct()
                .collect()
            )
        ]

        source_files_string = ",".join(sorted(source_files))
        last_source_file = max(source_files) if source_files else None

        bronze_df = (
            source_df
            .withColumn(
                "delta_created_ts",
                F.lit(delta_created_ts).cast("timestamp")
            )
            .withColumn(
                "delta_created_dt",
                F.to_date("delta_created_ts")
            )
        )

        records_loaded = bronze_df.count()

        # Append to Bronze.
        #
        # NOTE:
        # Partitioning on a timestamp is normally not recommended because
        # delta_created_ts can have very high cardinality. It is included
        # here because it was explicitly requested for the assessment.
        (
            bronze_df.write
            .format("delta")
            .mode("append")
            .partitionBy("delta_created_dt", "delta_created_ts")
            .saveAsTable(target_table)
        )

        load_end_ts = spark.sql(
            "SELECT current_timestamp() AS ts"
        ).first()["ts"]

        # Update latest-load master ONLY after the Bronze write succeeds.
        update_master_success(
            table_name=table_name,
            source_path=source_path,
            delta_created_ts=delta_created_ts,
            records_loaded=records_loaded,
            last_source_file=last_source_file,
            load_start_ts=load_start_ts,
            load_end_ts=load_end_ts,
        )

        # Preserve a permanent execution history.
        append_load_history(
            load_id=load_id,
            table_name=table_name,
            source_path=source_path,
            load_start_ts=load_start_ts,
            load_end_ts=load_end_ts,
            delta_created_ts=delta_created_ts,
            records_loaded=records_loaded,
            source_files=source_files_string,
            load_status="SUCCESS",
            error_message=None,
        )

        print(f"Status       : SUCCESS")
        print(f"Rows loaded  : {records_loaded}")
        print(f"Created TS   : {delta_created_ts}")
        print(f"Files        : {source_files_string}")

        return {
            "load_id": load_id,
            "table_name": table_name,
            "status": "SUCCESS",
            "records_loaded": records_loaded,
        }

    except Exception as exc:
        load_end_ts = spark.sql(
            "SELECT current_timestamp() AS ts"
        ).first()["ts"]

        error_message = str(exc)
        error_details = traceback.format_exc()

        # Failed executions are retained in history.
        # The master table is intentionally NOT advanced because it represents
        # the latest successful Bronze load.
        append_load_history(
            load_id=load_id,
            table_name=table_name,
            source_path=source_path,
            load_start_ts=load_start_ts,
            load_end_ts=load_end_ts,
            delta_created_ts=delta_created_ts,
            records_loaded=0,
            source_files=None,
            load_status="FAILED",
            error_message=error_message,
        )

        print(f"Status       : FAILED")
        print(f"Error        : {error_message}")
        print(error_details)

        return {
            "load_id": load_id,
            "table_name": table_name,
            "status": "FAILED",
            "records_loaded": 0,
            "error_message": error_message,
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute all configured batch loads

# COMMAND ----------

load_results = []

for config in TABLE_CONFIG:
    result = load_json_to_bronze(
        table_name=config["table_name"],
        source_path=config["source_path"],
        target_table=config["target_table"],
    )
    load_results.append(result)

display(spark.createDataFrame(load_results))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate latest successful load per Bronze table

# COMMAND ----------

display(
    spark.table(MASTER_TABLE)
    .orderBy("table_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate load execution history

# COMMAND ----------

display(
    spark.table(HISTORY_TABLE)
    .orderBy(F.col("load_start_ts").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Bronze tables

# COMMAND ----------

for config in TABLE_CONFIG:
    print(f"\nTable: {config['target_table']}")
    display(
        spark.table(config["target_table"])
        .orderBy(F.col("delta_created_ts").desc())
        .limit(20)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production notes
# MAGIC
# MAGIC 1. `bronze_load_master` contains one row per Bronze table and represents
# MAGIC    the latest successful load.
# MAGIC 2. `bronze_load_history` is append-only and records every successful or
# MAGIC    failed load execution.
# MAGIC 3. A failed execution does not advance `latest_loaded_ts`.
# MAGIC 4. `_source_file` provides source-file lineage for every Bronze record.
# MAGIC 5. `delta_created_ts` identifies the ingestion batch timestamp.
# MAGIC 6. `delta_created_dt` is useful for date-level pruning.
# MAGIC 7. Partitioning by a high-cardinality timestamp can create many small
# MAGIC    partitions. In a production design, normally partition only by
# MAGIC    `delta_created_dt` and retain `delta_created_ts` as a regular column.
# MAGIC 8. This notebook intentionally performs append-style Bronze ingestion.
# MAGIC    Business-key deduplication and idempotent MERGE logic should normally
# MAGIC    be implemented in Silver.