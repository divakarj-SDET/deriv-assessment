-- ============================================================================
-- 05_gold_star_schema.sql - Kimball star schema.
-- Grain is stated on every fact table. A fact table without a declared grain is
-- the single most common cause of double-counting in a warehouse.
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS workspace.deriv_assement_gold;

-- ---------------------------------------------------------------------------
-- dim_client - SCD2. GRAIN: one row per client per version of tracked attributes.
-- Merges client_signup (1:1) and client_profile (1:1); splitting them would
-- force every query into a two-dimension join for no benefit.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workspace.deriv_assement_gold.dim_client (
    client_sk           STRING NOT NULL COMMENT 'sha2(client_id, valid_from). Stable across stub upgrade',
    client_id           STRING NOT NULL COMMENT 'Business key',
    full_name           STRING,
    date_of_birth       DATE,
    country             STRING,
    nationality         STRING,
    account_type        STRING,
    kyc_status          STRING,
    referral_source     STRING,
    signup_platform     STRING,
    signup_date         DATE,
    risk_category       STRING  COMMENT 'SCD2 TRACKED - drives margin and regulatory treatment',
    account_status      STRING  COMMENT 'SCD2 TRACKED - drives trading permission',
    account_balance_usd DECIMAL(18,2) COMMENT 'Derived; see currency note below',
    account_balance_original DECIMAL(18,2) COMMENT 'Value as held, in currency',
    currency            STRING  COMMENT '13 of 30 clients are non-USD - never SUM the _usd column blindly',
    fx_rate_to_usd      DECIMAL(18,6),
    valid_from          TIMESTAMP,
    valid_to            TIMESTAMP,
    is_current          BOOLEAN,
    is_deleted          BOOLEAN COMMENT 'Soft delete tombstone; never physically removed',
    is_inferred         BOOLEAN COMMENT 'Late-arriving stub, upgraded in place when the real row lands',
    source_lsn          BIGINT,
    record_hash         STRING,
    updated_at          TIMESTAMP
) USING DELTA;

-- dim_date - conformed across both facts.
CREATE TABLE IF NOT EXISTS workspace.deriv_assement_gold.dim_date (
    date_key INT NOT NULL, full_date DATE, year INT, quarter INT, month INT,
    day INT, month_name STRING, day_of_week STRING, is_weekend BOOLEAN
) USING DELTA;

-- dim_instrument - SCD1. contract_size is what makes the TRD012 PnL check possible.
CREATE TABLE IF NOT EXISTS workspace.deriv_assement_gold.dim_instrument (
    instrument_key STRING NOT NULL, instrument STRING,
    asset_class    STRING COMMENT 'FX | metals | crypto | index',
    contract_size  DECIMAL(18,4) COMMENT 'Units per lot; used to re-derive PnL as a control',
    is_leveraged   BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.deriv_assement_gold.dim_payment_method (
    payment_method_key STRING NOT NULL, payment_method STRING,
    method_category STRING, is_instant BOOLEAN
) USING DELTA;

-- ---------------------------------------------------------------------------
-- fact_deposit
-- GRAIN: ONE ROW PER DEPOSIT EVENT PER SOURCE SYSTEM.
-- source_system is in the grain because the vendor and warehouse feeds share no
-- id namespace (DEP* vs VDEP*) - see part1 section 1b.
-- Additive: amount_usd, fee_usd, net_amount_usd.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workspace.deriv_assement_gold.fact_deposit (
    deposit_sk         STRING NOT NULL,
    deposit_id         STRING COMMENT 'Degenerate dimension - natural key kept on the fact',
    client_sk          STRING COMMENT 'FK to dim_client. Surrogate, so a stub upgrade needs no fact restatement',
    client_id          STRING,
    date_key           INT,
    payment_method_key STRING,
    source_system      STRING,
    amount_usd         DECIMAL(18,2),
    fee_usd            DECIMAL(18,2),
    net_amount_usd     DECIMAL(18,2),
    processing_days    INT,
    status             STRING,
    updated_at         TIMESTAMP
) USING DELTA PARTITIONED BY (date_key);

-- ---------------------------------------------------------------------------
-- fact_trade
-- GRAIN: ONE ROW PER TRADE.
-- Additive: pnl_usd. Semi-additive: volume_lots. NON-additive: open/close price
-- - summing a price is meaningless and the model makes that explicit.
-- pnl_usd_derived and pnl_variance_usd are CONTROL columns: the reported figure
-- is never overwritten, because the trading book is the system of record.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workspace.deriv_assement_gold.fact_trade (
    trade_sk          STRING NOT NULL,
    trade_id          STRING,
    client_sk         STRING,
    client_id         STRING,
    date_key          INT,
    instrument_key    STRING,
    direction         STRING,
    volume_lots       DECIMAL(18,4),
    open_price        DECIMAL(18,4) COMMENT 'NON-ADDITIVE',
    close_price       DECIMAL(18,4) COMMENT 'NON-ADDITIVE',
    pnl_usd_reported  DECIMAL(18,2) COMMENT 'As supplied by the source. Never modified',
    pnl_usd_derived   DECIMAL(18,2) COMMENT 'Recomputed from prices x contract_size x lots',
    pnl_variance_usd  DECIMAL(18,2) COMMENT 'Control. Non-zero on TRD012 (245.00 vs 0.00)',
    trade_status      STRING,
    updated_at        TIMESTAMP
) USING DELTA PARTITIONED BY (date_key);

-- ---------------------------------------------------------------------------
-- fact_client_balance_snapshot
-- GRAIN: ONE ROW PER CLIENT PER DAY.
-- Exists so that "total AUM by country per day" is a trivial aggregate instead
-- of a range-join of every client's SCD2 interval against a date spine.
-- Derived FROM dim_client, so there is one source of truth and no drift.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workspace.deriv_assement_gold.fact_client_balance_snapshot (
    date_key INT NOT NULL, client_sk STRING NOT NULL, client_id STRING,
    account_balance_usd DECIMAL(18,2), risk_category STRING, account_status STRING,
    is_deleted BOOLEAN
) USING DELTA PARTITIONED BY (date_key);

-- ---------------------------------------------------------------------------
-- Late-arriving dimension: create an INFERRED member rather than dropping the
-- fact or nulling the FK. Fires on CL031 (DEP020) and CL099 (VDEP020).
-- valid_from is 1900-01-01 so the stub covers ANY historical fact date.
-- ---------------------------------------------------------------------------
MERGE INTO workspace.deriv_assement_gold.dim_client AS t
USING (SELECT DISTINCT f.client_id
       FROM workspace.deriv_assement_silver.silver_deposit f
       LEFT JOIN workspace.deriv_assement_gold.dim_client d ON d.client_id = f.client_id
       WHERE d.client_id IS NULL) AS s
   ON t.client_id = s.client_id
WHEN NOT MATCHED THEN INSERT (client_sk, client_id, full_name, risk_category,
       account_status, valid_from, valid_to, is_current, is_deleted, is_inferred, updated_at)
VALUES (sha2(concat(s.client_id, 'inferred'), 256), s.client_id, 'UNKNOWN', 'unknown',
       'unknown', TIMESTAMP'1900-01-01', TIMESTAMP'9999-12-31', TRUE, FALSE, TRUE, current_timestamp());

-- When the real record arrives the stub is UPGRADED IN PLACE. client_sk does not
-- change, so facts already loaded need no restatement - the entire reason the
-- fact carries a surrogate key rather than the natural key.
MERGE INTO workspace.deriv_assement_gold.dim_client AS t
USING workspace.deriv_assement_silver.dim_client_current AS s
   ON t.client_id = s.client_id AND t.is_inferred = TRUE
WHEN MATCHED THEN UPDATE SET
    t.full_name = s.full_name, t.date_of_birth = s.date_of_birth,
    t.risk_category = s.risk_category, t.account_status = s.account_status,
    t.account_balance_usd = s.account_balance_usd,
    t.is_inferred = FALSE, t.updated_at = current_timestamp();

-- Monitoring: an inferred member older than 7 days means the dimension feed is
-- BROKEN, not merely late. Without this alert the stub mechanism quietly hides it.
SELECT client_id, updated_at, datediff(current_date(), CAST(updated_at AS DATE)) AS age_days
FROM workspace.deriv_assement_gold.dim_client
WHERE is_inferred = TRUE AND datediff(current_date(), CAST(updated_at AS DATE)) > 7;
