-- ============================================================================
-- 01_silver_ddl.sql - Silver layer DDL (Databricks / Unity Catalog / Delta)
--
-- Bronze stays raw and append-only so any Silver defect can be re-derived
-- without re-contacting the vendor. Every Silver table below is a MERGE target
-- keyed on a business key, which is what makes the pipeline replay-safe.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS workspace.deriv_assement_silver;

-- ----------------------------------------------------------------------------
-- silver_deposit: conformed deposits from BOTH sources.
-- source_system is part of the identity, not a label: the warehouse feed and the
-- vendor feed share no id namespace (DEP* vs VDEP*), and a third processor must
-- onboard without colliding with either.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workspace.deriv_assement_silver.silver_deposit (
    deposit_id          STRING NOT NULL COMMENT 'Natural key from the source system',
    client_id           STRING NOT NULL,
    deposit_date        DATE    COMMENT 'EVENT date. Drives partitioning, never the file label date',
    amount_usd          DECIMAL(18,2),
    payment_method      STRING  COMMENT 'Canonical name; vendor alias "method" mapped at load',
    currency_original   STRING,
    exchange_rate       DECIMAL(18,6),
    status              STRING,
    processing_days     INT,
    fee_usd             DECIMAL(18,2),
    source_system       STRING NOT NULL COMMENT 'WAREHOUSE | VENDOR',
    source_file         STRING  COMMENT 'Lineage back to the exact delivered file',
    row_hash            STRING  COMMENT 'Change-detection hash; makes MERGE a no-op on identical replay',
    is_quarantined      BOOLEAN DEFAULT FALSE,
    effective_from      TIMESTAMP,
    updated_at          TIMESTAMP
)
USING DELTA
PARTITIONED BY (deposit_date)  -- partition on EVENT date so backdated files restate the right window
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ----------------------------------------------------------------------------
-- File manifest: primary idempotency guard for file ingestion.
-- Hash, not filename: the vendor reuses filenames for corrections.
-- lag_days is the late-delivery detector (deposits_vendor_20240303.csv = 4 days).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workspace.deriv_assement_bronze.ingest_file_manifest (
    source_file         STRING,
    file_hash           STRING  COMMENT 'SHA-256 of file bytes',
    row_count           INT,
    min_event_date      DATE,
    max_event_date      DATE,
    file_label_date     DATE    COMMENT 'Date parsed from the filename',
    lag_days            INT     COMMENT 'file_label_date - max_event_date; >1 means backdated delivery',
    first_ingested_at   TIMESTAMP,
    last_seen_at        TIMESTAMP,
    ingest_count        INT,
    status              STRING  COMMENT 'INGESTED | SKIPPED_DUPLICATE | RE_INGESTED_CORRECTION'
) USING DELTA;

-- ----------------------------------------------------------------------------
-- Quarantine: rows withheld from Silver but NEVER discarded. Re-driven every
-- run, so a late-arriving dimension row releases the deposit automatically.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workspace.deriv_assement_silver.quarantine_deposit (
    run_id              STRING,
    deposit_id          STRING,
    client_id           STRING,
    source_file         STRING,
    raw_payload         STRING  COMMENT 'Full original row, for replay once the defect is fixed',
    failed_checks       STRING,
    quarantined_at      TIMESTAMP,
    resolved_at         TIMESTAMP COMMENT 'NULL while blocked; set when released into Silver'
) USING DELTA;

-- ----------------------------------------------------------------------------
-- DQ evidence. Append-only: every run leaves its own trail keyed by run_id.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workspace.deriv_assement_silver.dq_result (
    run_id              STRING,
    check_name          STRING,
    severity            STRING  COMMENT 'BLOCK | QUARANTINE | WARN',
    entity              STRING,
    record_key          STRING,
    source_file         STRING,
    detail              STRING,
    on_failure          STRING  COMMENT 'The concrete action taken, not a generic label',
    detected_at         TIMESTAMP
) USING DELTA;

-- ----------------------------------------------------------------------------
-- CDC watermark. Written in the SAME transaction as the dimension change, so the
-- watermark cannot advance without the data landing, or vice versa.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workspace.deriv_assement_silver.cdc_apply_log (
    lsn                 BIGINT,
    client_id           STRING,
    op                  STRING,
    commit_ts           TIMESTAMP,
    applied_at          TIMESTAMP,
    action_taken        STRING  COMMENT 'new_version | no_change_noop | soft_delete_tombstone'
) USING DELTA;
