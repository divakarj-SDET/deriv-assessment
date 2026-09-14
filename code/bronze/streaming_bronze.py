# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Deriv Assessment - Streaming Bronze Ingestion
# MAGIC
# MAGIC This notebook implements file-based micro-batch streaming into Bronze
# MAGIC using Databricks Auto Loader and Structured Streaming.
# MAGIC
# MAGIC Sources:
# MAGIC 1. Vendor deposit CSV files
# MAGIC 2. Client profile CDC JSONL files
# MAGIC
# MAGIC Landing Volume:
# MAGIC `/Volumes/workspace/deriv_assement/data/stream/`
# MAGIC
# MAGIC Bronze schema:
# MAGIC `workspace.deriv_assement_bronze`
# MAGIC
# MAGIC Important:
# MAGIC - This is file-based streaming/micro-batch streaming.
# MAGIC - Auto Loader incrementally detects new files.
# MAGIC - Bronze preserves the source events with minimal transformation.
# MAGIC - Business deduplication and CDC application belong in Silver.
# MAGIC - `_metadata.file_path` is used for Unity Catalog compatibility.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "workspace"
BRONZE_SCHEMA = "deriv_assement_bronze"

STREAM_BASE_PATH = "/Volumes/workspace/deriv_assement/data/stream/"

# Expected landing folders.
DEPOSIT_SOURCE_PATH = f"{STREAM_BASE_PATH}deposits_vendor/"
CDC_SOURCE_PATH = f"{STREAM_BASE_PATH}client_profile_changes/"

DEPOSIT_TARGET = f"{CATALOG}.{BRONZE_SCHEMA}.stream_client_deposits"
CDC_TARGET = f"{CATALOG}.{BRONZE_SCHEMA}.stream_client_profile_changes"

CHECKPOINT_BASE = f"{STREAM_BASE_PATH}_checkpoints/"

DEPOSIT_CHECKPOINT = f"{CHECKPOINT_BASE}client_deposits/"
CDC_CHECKPOINT = f"{CHECKPOINT_BASE}client_profile_changes/"

# Auto Loader schema locations are separate from streaming checkpoints.
# Required when schema inference/evolution is enabled.
DEPOSIT_SCHEMA_LOCATION = f"{CHECKPOINT_BASE}schemas/client_deposits/"
CDC_SCHEMA_LOCATION = f"{CHECKPOINT_BASE}schemas/client_profile_changes/"

STREAM_QUERY_PREFIX = "deriv_bronze_"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze schema

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create streaming audit table
# MAGIC
# MAGIC One row is written for every micro-batch processed by each streaming query.

# COMMAND ----------

STREAM_AUDIT_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.streaming_load_history"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {STREAM_AUDIT_TABLE} (
    batch_id BIGINT,
    query_name STRING,
    target_table STRING,
    batch_start_ts TIMESTAMP,
    batch_end_ts TIMESTAMP,
    records_processed BIGINT,
    status STRING,
    error_message STRING
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming audit helper

# COMMAND ----------

def write_stream_audit(
    batch_id,
    query_name,
    target_table,
    batch_start_ts,
    batch_end_ts,
    records_processed,
    status,
    error_message=None
):
    audit_df = spark.createDataFrame(
        [(
            int(batch_id),
            query_name,
            target_table,
            batch_start_ts,
            batch_end_ts,
            int(records_processed),
            status,
            error_message
        )],
        """
        batch_id long,
        query_name string,
        target_table string,
        batch_start_ts timestamp,
        batch_end_ts timestamp,
        records_processed long,
        status string,
        error_message string
        """
    )

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(STREAM_AUDIT_TABLE)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Vendor Deposit Streaming
# MAGIC
# MAGIC Expected files:
# MAGIC
# MAGIC ```text
# MAGIC deposits_vendor_*.csv
# MAGIC ```
# MAGIC
# MAGIC Auto Loader processes newly arriving files incrementally.

# COMMAND ----------

deposit_query_name = f"{STREAM_QUERY_PREFIX}deposits"

deposit_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", DEPOSIT_SCHEMA_LOCATION)
    .option("header", "true")
    .load(DEPOSIT_SOURCE_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Bronze metadata to deposit stream

# COMMAND ----------

deposit_bronze_df = (
    deposit_stream_df
    .withColumn(
        "_source_file",
        F.col("_metadata.file_path")
    )
    .withColumn(
        "_source_file_modification_time",
        F.col("_metadata.file_modification_time")
    )
    .withColumn(
        "delta_created_ts",
        F.current_timestamp()
    )
    .withColumn(
        "delta_created_dt",
        F.to_date("delta_created_ts")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deposit streaming write
# MAGIC
# MAGIC `availableNow=True` is useful for the assessment because it processes
# MAGIC all currently available files and then stops.
# MAGIC
# MAGIC For continuously arriving files, replace `availableNow=True` with:
# MAGIC
# MAGIC ```python
# MAGIC .trigger(processingTime="1 minute")
# MAGIC ```

# COMMAND ----------

deposit_query = (
    deposit_bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", DEPOSIT_CHECKPOINT)
    .queryName(deposit_query_name)
    .partitionBy("delta_created_dt")
    .trigger(availableNow=True)
    .toTable(DEPOSIT_TARGET)
)

deposit_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Client Profile CDC Streaming
# MAGIC
# MAGIC Expected source:
# MAGIC
# MAGIC ```text
# MAGIC client_profile_changes.jsonl
# MAGIC ```
# MAGIC
# MAGIC Each record represents an INSERT, UPDATE, or DELETE event.
# MAGIC
# MAGIC Bronze intentionally does not apply the CDC operation.
# MAGIC The raw CDC event is preserved for Silver processing.

# COMMAND ----------

cdc_query_name = f"{STREAM_QUERY_PREFIX}client_profile_cdc"

cdc_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", CDC_SCHEMA_LOCATION)
    .load(CDC_SOURCE_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Bronze metadata to CDC stream

# COMMAND ----------

cdc_bronze_df = (
    cdc_stream_df
    .withColumn(
        "_source_file",
        F.col("_metadata.file_path")
    )
    .withColumn(
        "_source_file_modification_time",
        F.col("_metadata.file_modification_time")
    )
    .withColumn(
        "delta_created_ts",
        F.current_timestamp()
    )
    .withColumn(
        "delta_created_dt",
        F.to_date("delta_created_ts")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC streaming write

# COMMAND ----------

cdc_query = (
    cdc_bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CDC_CHECKPOINT)
    .queryName(cdc_query_name)
    .partitionBy("delta_created_dt")
    .trigger(availableNow=True)
    .toTable(CDC_TARGET)
)

cdc_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Validate Streaming Bronze Tables

# COMMAND ----------

print("Vendor Deposit Bronze")
display(
    spark.table(DEPOSIT_TARGET)
    .orderBy(F.col("delta_created_ts").desc())
    .limit(50)
)

print("Client Profile CDC Bronze")
display(
    spark.table(CDC_TARGET)
    .orderBy(F.col("delta_created_ts").desc())
    .limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Streaming Query Status

# COMMAND ----------

for q in spark.streams.active:
    print("=" * 80)
    print(f"Query name : {q.name}")
    print(f"Query ID   : {q.id}")
    print(f"Active     : {q.isActive}")
    print(f"Status     : {q.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Bronze Streaming Architecture
# MAGIC
# MAGIC ```text
# MAGIC                    Unity Catalog Volume
# MAGIC                            |
# MAGIC             +--------------+--------------+
# MAGIC             |                             |
# MAGIC             v                             v
# MAGIC      deposits_vendor/            client_profile_changes/
# MAGIC             |                             |
# MAGIC             v                             v
# MAGIC       Auto Loader                  Auto Loader
# MAGIC          CSV                         JSONL
# MAGIC             |                             |
# MAGIC             v                             v
# MAGIC      Structured Streaming       Structured Streaming
# MAGIC             |                             |
# MAGIC             v                             v
# MAGIC     stream_client_deposits    stream_client_profile_changes
# MAGIC             |                             |
# MAGIC             +--------------+--------------+
# MAGIC                            |
# MAGIC                            v
# MAGIC                          Silver
# MAGIC ```
# MAGIC
# MAGIC ## Silver responsibilities
# MAGIC
# MAGIC Vendor deposits:
# MAGIC - Deduplicate by business key such as `deposit_id`
# MAGIC - Normalize schema drift (`payment_method` vs `method`)
# MAGIC - Validate deposit amount
# MAGIC - Handle unknown clients
# MAGIC - Reconcile source records
# MAGIC
# MAGIC CDC:
# MAGIC - Order events using LSN / commit timestamp
# MAGIC - Handle multiple changes to the same client
# MAGIC - Apply INSERT / UPDATE / DELETE
# MAGIC - Build SCD Type 2 history
# MAGIC - Represent DELETE as a soft delete / end-dated record
# MAGIC
# MAGIC ## Auto Loader state management
# MAGIC
# MAGIC Auto Loader uses two different types of state:
# MAGIC
# MAGIC 1. **Schema location** - stores inferred/evolving source schema information.
# MAGIC 2. **Checkpoint location** - stores Structured Streaming progress/state.
# MAGIC
# MAGIC They are intentionally configured separately:
# MAGIC
# MAGIC ```text
# MAGIC stream/
# MAGIC └── _checkpoints/
# MAGIC     ├── client_deposits/
# MAGIC     ├── client_profile_changes/
# MAGIC     └── schemas/
# MAGIC         ├── client_deposits/
# MAGIC         └── client_profile_changes/
# MAGIC ```
# MAGIC
# MAGIC This is required when Auto Loader schema inference/evolution is enabled.
# MAGIC
# MAGIC # MAGIC ## Important design point
# MAGIC
# MAGIC Auto Loader provides incremental file discovery and checkpoint-based
# MAGIC stream processing. It does not by itself guarantee business-level
# MAGIC deduplication. That responsibility belongs to the downstream data
# MAGIC processing layer.