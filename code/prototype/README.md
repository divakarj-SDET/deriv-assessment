# Runnable prototype

End-to-end implementation of the design, in DuckDB, on the real files in `../../data/`.
It exists so the design documents can quote verified numbers rather than assertions.

## Run

```bash
pip install duckdb
python3 run_pipeline.py --reset     # build from scratch
python3 run_pipeline.py             # run again — idempotency proof
python3 run_pipeline.py --replay 20240302   # simulate one file being redelivered
```

## What it implements

| Stage | Mirrors | Demonstrates |
|---|---|---|
| Bronze | `01_bronze_batch_json.py`, `02_bronze_streaming_autoloader.py` | File manifest with content hash; late-delivery detection |
| Silver | `03_silver_vendor_deposits.py`, `sql/02`, `sql/03` | Alias mapping, severity-tiered DQ, quarantine, dedupe, MERGE |
| SCD2 | `04_silver_cdc_scd2.py`, `sql/04` | LSN reordering, partial after-images, soft delete, LSN watermark |
| Recon | `06_reconciliation.py`, `sql/06` | Two-tier matching, break classification, control totals |
| Gold | `05_gold_dimensional.py`, `sql/05` | Star schema, inferred members, PnL recomputation |

Not mirrored: the Databricks-only control mechanisms — the `bronze_load_master` watermark,
the archive drain, and the Auto Loader checkpoints. There is no volume to archive into and
no Bronze table accumulating historical loads, so the prototype has nothing for them to
guard. `part1_pipeline.md` §1a.2 mechanism (5) covers them.

## What a clean run reports

```
deposits_vendor_20240302.csv   DRIFT ['method'] mapped to canonical names
deposits_vendor_20240303.csv   LATE: newest event is 4d older than the file label
DQ summary: 24 rows in, 22 clean, 2 quarantined, 19 warnings raised
Deduplicated 22 -> 20 on deposit_id (2 cross-file duplicates collapsed)
MERGE: 20 inserted, 0 updated, 0 unchanged
RECOVERED DEP012: payment_method from malformed key 'credit_card'
QUARANTINED DEP020: orphan client_id CL031 (warehouse feed)
Applied order : [1001, 1003, 1004, 1005, 1006, 1008, 1009, 1010, 1012, 1015, 1018, 1020]
lsn 1001 INSERT CL030: tracked attributes unchanged -> no new version
lsn 1010 DELETE CL012: tombstone inserted (is_deleted=TRUE). History preserved.
Tier 1: 0 matches · Tier 2: 0 matches · 20 vendor-only, 1 warehouse-only
Vendor control totals: 20 rows, 28525.00 USD
PnL VARIANCE TRD012: reported 245.0 vs derived 0.0
```

Two counts that look inconsistent and are not:

- **`19 warnings raised` here, 21 in the documents.** That counter covers only the vendor
  DQ gate. Two further WARN rows are raised downstream — `malformed_key_recovered` on
  `DEP012` and `pnl_recomputes` on `TRD012` — for 21 in `dq_result`.
- **No `Inferred dimension member created` line.** Both orphan deposits are quarantined at
  the Silver gate, so nothing reaches Gold needing a stub: `dim_client` holds 41 versions
  across 30 clients and **0 inferred members**. That is the designed precedence, explained
  in `part1_pipeline.md` EC-4.

## Idempotency proof

Run it twice. STATE tables are byte-identical:

```
bronze_vendor_deposit   24    ingest_file_manifest   3
silver_deposit          39    quarantine_deposit     3
dim_client              41    cdc_apply_log         12
fact_deposit            39    fact_trade            20
```

`dq_result` and `reconciliation_result` are AUDIT tables and grow by design — every run
leaves its own evidence trail keyed by `run_id`: 24 and 21 rows after the first run, 48
and 42 after the second.

## Notes

- `warehouse.duckdb` is generated output, not source. Delete it, or pass `--reset`, to
  rebuild from scratch; it is not committed.
- The contract sizes in `CONTRACT_SIZE` were **inferred from the trades that are internally
  consistent**, not taken from market convention. That matters: assuming the standard
  100 oz/lot for gold produces a false positive on `TRD016` and misses the real defect on
  `TRD012`.
