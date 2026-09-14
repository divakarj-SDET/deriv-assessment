-- ============================================================================
-- 03_dq_checks.sql - Data quality rule set.
--
-- Severity drives a DISTINCT action. Nothing is "log and continue":
--   BLOCK      -> abort the batch before any Silver write, page on-call.
--                 Nothing lands. A partial financial load is worse than none.
--   QUARANTINE -> withhold the row, persist it, re-drive automatically next run.
--   WARN       -> load the row, record evidence for stewardship.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- BLOCK 1 - null business key. Without deposit_id the MERGE key is undefined
-- and idempotency is lost, so this must stop the batch.
-- ---------------------------------------------------------------------------
SELECT 'deposit_id_not_null' AS check_name, 'BLOCK' AS severity,
       'abort batch, page on-call' AS on_failure, COUNT(*) AS failures
FROM vendor_normalised WHERE deposit_id IS NULL OR trim(deposit_id) = ''
HAVING COUNT(*) > 0;

-- ---------------------------------------------------------------------------
-- BLOCK 2 - unmapped column in the delivered file.
-- The vendor renamed payment_method -> method on day 2. That rename is handled
-- by a DECLARED alias. Any column we have NOT declared is a contract change we
-- do not understand, and silently dropping it is how a feed quietly loses a
-- field for months. Fail loudly instead.
-- ---------------------------------------------------------------------------
-- (evaluated in Python against the file header; see code/databricks/03_silver_vendor_deposits.py)

-- ---------------------------------------------------------------------------
-- QUARANTINE 1 - non-positive amount.
-- Fires on VDEP001 = -250.00. Quarantine rather than BLOCK: this is most likely
-- a refund or chargeback the vendor encoded in the same feed, and halting 23
-- good rows for it would be the wrong trade. If refunds prove to be legitimate
-- traffic, the fix is a transaction_type column in the contract, not a weaker rule.
-- ---------------------------------------------------------------------------
SELECT 'amount_positive' AS check_name, 'QUARANTINE' AS severity,
       deposit_id, amount_usd,
       'withhold row, raise vendor ticket' AS on_failure
FROM vendor_normalised WHERE amount_usd IS NULL OR amount_usd <= 0;

-- ---------------------------------------------------------------------------
-- QUARANTINE 2 - orphan client_id.
-- Fires on VDEP020 -> CL099 and DEP020 -> CL031 (signup ends at CL030).
-- Quarantined, NOT dropped: if the dimension row arrives later the deposit is
-- released automatically (see 02_silver_vendor_deposits_merge.sql step 4).
-- ---------------------------------------------------------------------------
SELECT 'client_exists' AS check_name, 'QUARANTINE' AS severity,
       v.deposit_id, v.client_id,
       'withhold row, park for late-arriving dimension' AS on_failure
FROM vendor_normalised v
LEFT JOIN workspace.deriv_assement_bronze.client_signup s ON s.client_id = v.client_id
WHERE s.client_id IS NULL;

-- ---------------------------------------------------------------------------
-- WARN 1 - deposit dated before the client signed up.
-- Fires on 14 of 24 vendor rows, e.g. VDEP019 (CL022, deposit 2024-02-25,
-- signup 2024-04-20).
--
-- Deliberately WARN, not QUARANTINE. A rule failing 58% of a feed is describing
-- the business, not catching a defect - the vendor's client_id namespace clearly
-- does not align with warehouse signup dates. Quarantining 14 rows would destroy
-- the feed's usefulness and train the team to ignore the queue. Flag loudly,
-- escalate as a contract question.
-- ---------------------------------------------------------------------------
SELECT 'deposit_not_before_signup' AS check_name, 'WARN' AS severity,
       v.deposit_id, v.client_id, v.deposit_date, s.signup_date,
       'load, flag for stewardship review' AS on_failure
FROM vendor_normalised v
JOIN workspace.deriv_assement_bronze.client_signup s ON s.client_id = v.client_id
WHERE v.deposit_date < s.signup_date;

-- ---------------------------------------------------------------------------
-- WARN 2 - fee outside tolerance. Observed norm is 1.00% of amount.
-- Fires on VDEP012 (1.60%) and VDEP021 (1.43%).
-- ---------------------------------------------------------------------------
SELECT 'fee_within_tolerance' AS check_name, 'WARN' AS severity,
       deposit_id, amount_usd, fee_usd, ROUND(fee_usd / amount_usd * 100, 2) AS fee_pct,
       'load, flag for finance review' AS on_failure
FROM vendor_normalised
WHERE amount_usd > 0 AND fee_usd > 0 AND ABS(fee_usd / amount_usd - 0.01) > 0.002;

-- ---------------------------------------------------------------------------
-- WARN 3 - deposit by a client whose KYC is not approved.
-- Fires on VDEP004 (CL012 = rejected) and VDEP009 (CL026 = pending).
-- Never silently dropped: this is a compliance signal, and suppressing it would
-- hide exactly the activity compliance needs to see.
-- ---------------------------------------------------------------------------
SELECT 'kyc_approved_for_deposit' AS check_name, 'WARN' AS severity,
       v.deposit_id, v.client_id, s.kyc_status,
       'load, route to compliance queue' AS on_failure
FROM vendor_normalised v
JOIN workspace.deriv_assement_bronze.client_signup s ON s.client_id = v.client_id
WHERE s.kyc_status <> 'approved';

-- ---------------------------------------------------------------------------
-- WARN 4 - reported PnL does not recompute from prices.
-- Fires on TRD012: Gold, buy, 5.0 lots, open 2320.00 = close 2320.00, so derived
-- PnL is 0.00, but 245.00 is reported.
-- Both values are loaded and the variance is exposed as a control column. We do
-- NOT overwrite the reported figure - the trading book is the system of record
-- and a pipeline silently "correcting" it would be a serious error.
-- ---------------------------------------------------------------------------
SELECT 'pnl_recomputes' AS check_name, 'WARN' AS severity,
       t.trade_id, t.pnl_usd AS reported,
       ROUND((t.close_price - t.open_price)
             * CASE WHEN t.direction = 'buy' THEN 1 ELSE -1 END
             * i.contract_size * t.volume_lots, 2) AS derived,
       'load both values, flag to trading ops' AS on_failure
FROM workspace.deriv_assement_bronze.client_trades t
JOIN workspace.deriv_assement_gold.dim_instrument i ON i.instrument = t.instrument
WHERE ABS(t.pnl_usd - ROUND((t.close_price - t.open_price)
          * CASE WHEN t.direction = 'buy' THEN 1 ELSE -1 END
          * i.contract_size * t.volume_lots, 2)) > 0.01;

-- ---------------------------------------------------------------------------
-- WARN 5 - implausible date of birth. Fires on CL025 = 1888-12-19 (age 136).
-- Routed to KYC re-verification rather than blocking, because the client has
-- real money (18,200.00) and real trades behind them.
-- ---------------------------------------------------------------------------
SELECT 'dob_plausible' AS check_name, 'WARN' AS severity, client_id, date_of_birth,
       'load, route to KYC re-verification' AS on_failure
FROM workspace.deriv_assement_bronze.client_profile
WHERE date_of_birth < '1920-01-01' OR date_of_birth > current_date();

-- ---------------------------------------------------------------------------
-- SCD2 structural invariants. Asserted after EVERY run and every backfill.
-- All three must return zero rows.
-- ---------------------------------------------------------------------------
-- (a) exactly one current version per client
SELECT 'scd2_single_current' AS check_name, client_id, COUNT(*) AS versions
FROM workspace.deriv_assement_silver.dim_client
WHERE is_current = TRUE GROUP BY client_id HAVING COUNT(*) > 1;

-- (b) no gaps or overlaps in any client's timeline
SELECT 'scd2_contiguous_timeline' AS check_name, client_id, valid_to, next_valid_from
FROM (SELECT client_id, valid_to,
             LEAD(valid_from) OVER (PARTITION BY client_id ORDER BY valid_from) AS next_valid_from
      FROM workspace.deriv_assement_silver.dim_client)
WHERE next_valid_from IS NOT NULL AND next_valid_from <> valid_to;

-- (c) no version opens after it closes
SELECT 'scd2_valid_interval' AS check_name, client_id, valid_from, valid_to
FROM workspace.deriv_assement_silver.dim_client WHERE valid_from >= valid_to;
