# Deriv — Data Engineering Assessment

Production-grade pipeline design for a financial trading platform: vendor deposit
reconciliation, CDC historization, dimensional modelling, and real-time/batch architecture.

Platform: **Databricks** (Unity Catalog, Delta Lake, Auto Loader, Lakeflow Jobs), with a
runnable local prototype in DuckDB so every claim can be verified without a cluster.

---

## Navigation

| File | Contents |
|---|---|
| **[`part1_pipeline.md`](part1_pipeline.md)** | Architecture, idempotency, late/missing data, source deletes, 5 edge cases, DQ rule register, orchestration, reconciliation |
| **[`part2_data_model.md`](part2_data_model.md)** | Dimensional model + ERD, Kimball vs Data Vault, late-arriving dimensions, SCD choice, merge logic, historical backfill |
| **[`part3_architecture.md`](part3_architecture.md)** | Unified real-time + batch architecture, latency vs consistency, external API, build vs buy |
| **[`PROMPTS.md`](PROMPTS.md)** | AI prompts by part, with what I changed, corrected or rejected |
| `sql/` | DDL, MERGE logic, DQ checks, reconciliation, backfill — all commented |
| `code/databricks/` | PySpark notebooks, bronze → silver → gold, plus the two Lakeflow job definitions |
| `code/prototype/` | Runnable end-to-end prototype (DuckDB) |
| `data/` | The eight source files |

## Repository layout

```
├── README.md
├── part1_pipeline.md            Pipeline design & reconciliation
├── part2_data_model.md          Data model & historization
├── part3_architecture.md        TL extension
├── PROMPTS.md
├── sql/
│   ├── 01_silver_ddl.sql              Silver DDL, manifest, quarantine, DQ, watermark
│   ├── 02_silver_vendor_deposits_merge.sql   Drift fix, dedupe, idempotent MERGE
│   ├── 03_dq_checks.sql               Severity-tiered rules + SCD2 invariants
│   ├── 04_scd2_dim_client_merge.sql   CDC → SCD2, soft delete, AUTO CDC alternative
│   ├── 05_gold_star_schema.sql        Star schema + late-arriving dimension handling
│   ├── 06_reconciliation.sql          Two-tier match, control totals, break ageing
│   └── 08_backfill_restatement.sql    Reload a date range without corrupting history
├── code/
│   ├── databricks/
│   │   ├── 01_bronze_batch_json.py            JSON → Bronze, watermark + archive drain
│   │   ├── 02_bronze_streaming_autoloader.py  Auto Loader → Bronze, trigger_mode widget
│   │   ├── 03_silver_vendor_deposits.py       DQ gate, dedupe, MERGE, quarantine release
│   │   ├── 04_silver_cdc_scd2.py              LSN-ordered SCD2, soft deletes, current view
│   │   ├── 05_gold_dimensional.py             Star schema, inferred members, PnL control
│   │   ├── 06_reconciliation.py               Two-tier reconciliation
│   │   ├── create_job_streaming.json          Continuous job — 02 at trigger_mode=continuous
│   │   └── create_job_batch.json              File-arrival job — 01 → 03 → 04 → 05 → 06
│   └── prototype/
│       └── run_pipeline.py                    Runnable, idempotent, end-to-end
└── data/                                      Eight source files
```

## Running the prototype

```bash
pip install duckdb
cd code/prototype
python3 run_pipeline.py --reset     # build from scratch
python3 run_pipeline.py             # run again — proves idempotency
```

The second run reports `SKIPPED - byte-identical redelivery` for all three vendor files,
`MERGE: 0 inserted, 0 updated, 20 unchanged`, and `Applied 0 CDC events, skipped 12`.
All eight STATE tables are byte-identical across runs.

---

## What the data actually contains

The design is driven by defects found by profiling the files, not by assumption. Every one
is traceable to a specific record.

| Finding | Record(s) | Handling |
|---|---|---|
| Column renamed mid-feed (`payment_method` → `method`) | `deposits_vendor_20240302.csv` | Declared alias map; unmapped column = BLOCK |
| File delivered 4 days after its newest event | `deposits_vendor_20240303.csv` (events 24–28 Feb) | Event-time windowing + partition restatement |
| Cross-file duplicate redelivery | `VDEP002`, `VDEP005` | Dedupe on `deposit_id`, hash-guarded MERGE |
| Negative deposit amount | `VDEP001` = -250.00 | QUARANTINE, vendor ticket |
| Orphan foreign keys | `VDEP020`→`CL099`, `DEP020`→`CL031` | QUARANTINE, auto-released when the client arrives |
| Malformed JSON key | `DEP012` has `"credit_card"` instead of `payment_method` | Value recovered, source fix raised |
| CDC out of arrival order | 12 events; `CL001` has 3 same-day changes | Sort by `lsn`, never `commit_ts` |
| CDC delete | `lsn 1010` → `CL012` | Soft delete: end-date + tombstone |
| Insert for an existing key | `lsn 1001` → `CL030` | Hash-compared, no-op |
| PnL does not recompute | `TRD012`: open = close = 2320.00, yet 245.00 reported | Both values loaded, variance exposed |
| Activity predating signup | `TRD005` (`CL007`), 14 vendor deposits | WARN + stewardship escalation |
| Impossible date of birth | `CL025` = 1888-12-19 | WARN → KYC re-verification |
| Balance column named USD, 13 clients non-USD | `CL003` THB, `CL009` EUR, … | Split into original + currency + derived USD |

## Headline results

```
Vendor files       3 files, 24 raw rows → 22 clean → 20 loaded
DQ                 3 quarantined, 21 warnings, 0 blocking
silver_deposit     39 rows = 20 vendor ($28,525.00) + 19 warehouse ($121,800.00)
CDC                12 events applied in LSN order; 1 soft delete, 1 no-op
dim_client         41 versions across 30 clients · 0 inferred members
Reconciliation     Tier 1: 0 matches · Tier 2: 0 matches
                   20 vendor-only, 1 warehouse-only ($350.00, DEP008)
                   Vendor control total: $28,525.00
```

`0 inferred members` is the correct outcome, not a gap. Both orphan deposits are stopped
at the Silver DQ gate, so neither ever reaches Gold needing a stub. The inferred-member
path is the *second* line of defence — it fires only for an orphan that gets past
quarantine. Both mechanisms are implemented; on this data only the first one has work to
do. Detail in [`part1`](part1_pipeline.md) EC-4 and [`part2`](part2_data_model.md) §2a.

**The reconciliation result is the finding, not a failure.** Vendor ids (`VDEP*`) and
warehouse ids (`DEP*`) share no namespace, and the composite business key produces no
matches either. This feed is *net-new deposit traffic*, not a mirror of `client_deposit`,
so reconciliation is a completeness and control-total check rather than a row-for-row
tie-out. A single-tier design would have reported a 100% break rate on day one — technically
true, and completely misleading. Reasoning in [`part1_pipeline.md`](part1_pipeline.md) §1b.

## Three decisions I expect to be challenged on

1. **`account_balance_usd` is not SCD Type 2.** The obvious answer is Type 2 on all three
   CDC attributes. Balance changes on every deposit and closed trade, so that makes the
   dimension no longer slowly changing and pollutes the risk history with versions carrying
   no risk information. Split to Type 4. — [`part2`](part2_data_model.md) §2b.1
2. **`deposit_not_before_signup` is WARN, despite failing 14 of 24 rows.** A rule that fails
   58% of a feed is describing the business, not catching a defect. Quarantining would
   destroy the feed and train the team to ignore the queue. — [`part1`](part1_pipeline.md)
3. **Buy the transport, build the transformation.** The hard problems here — backdated file,
   renamed column, duplicates, id-namespace mismatch — are ones no integration platform
   solves. — [`part3`](part3_architecture.md) §3b
