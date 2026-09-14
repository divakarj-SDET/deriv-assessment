-- ============================================================================
-- 06_reconciliation.sql - vendor feed vs warehouse client_deposit.
--
-- TWO-TIER by design. A single matching strategy is fragile: identifier-only
-- matching reports false breaks whenever two systems use different id
-- namespaces, which is EXACTLY what happens here.
-- ============================================================================

-- Recon window is derived from the EVENT dates, never from the file label.
-- deposits_vendor_20240303.csv is labelled 3 March but contains 24-28 February.
-- A label-based window would silently reconcile zero of those 6 rows.
CREATE OR REPLACE TEMP VIEW recon_window AS
SELECT MIN(deposit_date) AS d_from, MAX(deposit_date) AS d_to
FROM workspace.deriv_assement_silver.silver_deposit WHERE source_system = 'VENDOR';

-- Tier 1 - exact deposit_id match. Cheap and unambiguous.
CREATE OR REPLACE TEMP VIEW recon_tier1 AS
SELECT 'TIER1_ID' AS match_tier, 'MATCHED' AS break_type,
       v.deposit_id AS deposit_id_vendor, w.deposit_id AS deposit_id_warehouse,
       v.client_id, v.deposit_date, v.amount_usd AS amount_vendor,
       w.amount_usd AS amount_warehouse, v.amount_usd - w.amount_usd AS variance_usd
FROM workspace.deriv_assement_silver.silver_deposit v
JOIN workspace.deriv_assement_silver.silver_deposit w
  ON v.deposit_id = w.deposit_id
WHERE v.source_system = 'VENDOR' AND w.source_system = 'WAREHOUSE';

-- Tier 2 - composite business key, for feeds that do not share an id namespace.
-- Catches genuine economic matches where the identifiers differ.
CREATE OR REPLACE TEMP VIEW recon_tier2 AS
SELECT 'TIER2_BUSINESS_KEY' AS match_tier, 'MATCHED' AS break_type,
       v.deposit_id, w.deposit_id, v.client_id, v.deposit_date,
       v.amount_usd, w.amount_usd, v.amount_usd - w.amount_usd
FROM workspace.deriv_assement_silver.silver_deposit v
JOIN workspace.deriv_assement_silver.silver_deposit w
  ON  v.client_id    = w.client_id
  AND v.deposit_date = w.deposit_date
  AND ABS(v.amount_usd - w.amount_usd) < 0.01      -- tolerance absorbs float noise
WHERE v.source_system = 'VENDOR' AND w.source_system = 'WAREHOUSE'
  AND v.deposit_id NOT IN (SELECT deposit_id_vendor FROM recon_tier1);

-- Breaks: vendor-only.
CREATE OR REPLACE TEMP VIEW recon_vendor_only AS
SELECT 'UNMATCHED' AS match_tier, 'IN_VENDOR_NOT_IN_WAREHOUSE' AS break_type,
       v.deposit_id, CAST(NULL AS STRING), v.client_id, v.deposit_date,
       v.amount_usd, CAST(NULL AS DECIMAL(18,2)), v.amount_usd
FROM workspace.deriv_assement_silver.silver_deposit v
WHERE v.source_system = 'VENDOR'
  AND v.deposit_id NOT IN (SELECT deposit_id_vendor FROM recon_tier1)
  AND v.deposit_id NOT IN (SELECT deposit_id FROM recon_tier2);

-- Breaks: warehouse-only, scoped to the vendor's own event window.
-- Without the window this would report every warehouse deposit ever loaded.
CREATE OR REPLACE TEMP VIEW recon_warehouse_only AS
SELECT 'UNMATCHED', 'IN_WAREHOUSE_NOT_IN_VENDOR',
       CAST(NULL AS STRING), w.deposit_id, w.client_id, w.deposit_date,
       CAST(NULL AS DECIMAL(18,2)), w.amount_usd, -w.amount_usd
FROM workspace.deriv_assement_silver.silver_deposit w, recon_window r
WHERE w.source_system = 'WAREHOUSE'
  AND w.deposit_date BETWEEN r.d_from AND r.d_to
  AND w.deposit_id NOT IN (SELECT deposit_id_warehouse FROM recon_tier1);

INSERT INTO workspace.deriv_assement_silver.reconciliation_result
SELECT current_timestamp(), * FROM (
    SELECT * FROM recon_tier1 UNION ALL SELECT * FROM recon_tier2
    UNION ALL SELECT * FROM recon_vendor_only UNION ALL SELECT * FROM recon_warehouse_only);

-- ---------------------------------------------------------------------------
-- Control totals per day per source. This is the check that actually catches a
-- silently truncated file, which row-level matching alone will not.
-- ---------------------------------------------------------------------------
SELECT deposit_date, source_system, COUNT(*) AS row_count,
       SUM(amount_usd) AS total_usd, SUM(fee_usd) AS total_fee
FROM workspace.deriv_assement_silver.silver_deposit
GROUP BY deposit_date, source_system ORDER BY deposit_date, source_system;

-- ---------------------------------------------------------------------------
-- Break ageing and materiality. Both matter: a small break open for a week and
-- a large break opened today are different problems with different escalations.
-- DEP013 alone is 75,000.00 - one row that moves a weekly number.
-- ---------------------------------------------------------------------------
SELECT break_type, COUNT(*) AS breaks, SUM(ABS(variance_usd)) AS abs_variance,
       MAX(datediff(current_date(), CAST(created_at AS DATE))) AS oldest_days,
       CASE WHEN SUM(ABS(variance_usd)) > 10000 THEN 'PAGE_IMMEDIATELY'
            WHEN MAX(datediff(current_date(), CAST(created_at AS DATE))) > 3 THEN 'ESCALATE_TO_VENDOR'
            ELSE 'MONITOR' END AS action
FROM workspace.deriv_assement_silver.reconciliation_result
WHERE break_type <> 'MATCHED' GROUP BY break_type;

-- ---------------------------------------------------------------------------
-- RESULT ON THE DELIVERED DATA:
--   Tier 1: 0 matches   Tier 2: 0 matches
--   20 vendor-only, 1 warehouse-only (DEP008, CL012, 2024-02-25, 350.00)
--   Vendor control total: 20 rows / 28,525.00 USD
--
-- Zero matches at BOTH tiers is the finding, and it is not a pipeline failure:
-- vendor ids are VDEP001-022, warehouse ids are DEP001-020, no overlap, and
-- Tier 2 confirms no economic duplicates either. This feed is NET-NEW deposit
-- traffic, not a mirror of client_deposit. Reconciliation here is therefore a
-- COMPLETENESS and CONTROL-TOTAL check, not a row-for-row tie-out. Reporting
-- "100% break rate" would be technically true and completely misleading.
-- ---------------------------------------------------------------------------
