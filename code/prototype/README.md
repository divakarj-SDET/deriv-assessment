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
| Bronze | `03_silver_vendor_deposits.py` | File manifest with content hash; late-delivery detection |
| Silver | `sql/02`, `sql/03` | Alias mapping, severity-tiered DQ, quarantine, dedupe, MERGE |
| SCD2 | `sql/04` | LSN reordering, partial after-images, soft delete, LSN watermark |
| Recon | `sql/06` | Two-tier matching, break classification, control totals |
| Gold | `sql/05` | Star schema, inferred members, PnL recomputation |

## Idempotency proof

Run it twice. STATE tables are byte-identical:

```
bronze_vendor_deposit   24    ingest_file_manifest   3
silver_deposit          39    quarantine_deposit     3
dim_client              41    cdc_apply_log         12
fact_deposit            39    fact_trade            20
```

`dq_result` and `reconciliation_result` are AUDIT tables and grow by design — every run
leaves its own evidence trail keyed by `run_id`.

## Notes

- `warehouse.duckdb` is generated output, not source. It is gitignored.
- The contract sizes in `CONTRACT_SIZE` were **inferred from the trades that are internally
  consistent**, not taken from market convention. That matters: assuming the standard
  100 oz/lot for gold produces a false positive on `TRD016` and misses the real defect on
  `TRD012`.
