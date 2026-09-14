# Part 1 --- Pipeline Design & Reconciliation

## 1. Purpose and Scope

This document describes the production-grade ingestion and
reconciliation design for:

1.  The third-party vendor deposit CSV feed.
2.  The `client_profile_changes.jsonl` CDC change-log.

The design is grounded in the assessment inputs, including duplicate
vendor deposits, schema drift, late delivery, unknown clients, negative
amounts, and CDC events arriving out of LSN order.

The target platform is a Databricks Lakehouse using Delta Lake and Unity
Catalog.

------------------------------------------------------------------------

# 2. Architecture Overview

## 2.1 End-to-End Flow

``` mermaid
flowchart LR
    A[Vendor Deposit CSV] --> B[Landing / UC Volume]
    C[CDC JSONL] --> D[Landing / UC Volume]

    B --> E[Auto Loader]
    D --> F[Auto Loader]

    E --> G[Bronze Vendor Deposits]
    F --> H[Bronze CDC Events]

    G --> I[Silver Deposit Processing]
    H --> J[Silver CDC / SCD2 Processing]

    I --> K[Reconciliation + DQ]
    J --> K

    I --> L[Gold Facts / Dimensions]
    J --> L

    K --> M[Audit / Quarantine / Alerts]
    L --> N[Analytics / BI / Risk / Compliance]
```

## 2.2 Landing Layer

The landing layer is the immutable file-arrival zone.

Example locations:

``` text
/Volumes/workspace/deriv_assement/data/stream/
```

Files are not transformed at this stage.

The landing layer preserves the source payload and provides the basis
for replay.

For every file, operational metadata should be captured:

-   source file name/path
-   file modification time
-   ingestion timestamp
-   source system
-   batch/run identifier
-   file size
-   optional file checksum/hash

The important design principle is:

> Landing preserves what arrived; downstream layers determine what is
> trusted.

------------------------------------------------------------------------

# 3. Vendor CSV Pipeline

## 3.1 Ingestion

Vendor CSV files are discovered incrementally using Databricks Auto
Loader.

``` text
Vendor CSV
    |
    v
Landing Volume
    |
    v
Auto Loader
    |
    v
Bronze Delta
```

Auto Loader provides incremental file discovery and checkpoint-based
progress tracking.

The vendor feed is file-based micro-batch ingestion, not true event
streaming.

The following files are explicitly part of the assessment:

``` text
deposits_vendor_20240301.csv
deposits_vendor_20240302.csv
deposits_vendor_20240303.csv
```

The second file introduces a schema change:

``` text
payment_method -> method
```

The third file is delivered late and contains records whose business
dates precede the delivery date.

------------------------------------------------------------------------

## 3.2 Bronze Processing

Bronze stores source-faithful records plus technical metadata.

Typical metadata:

``` text
_source_file
_source_file_modification_time
_ingestion_ts
batch_id
```

Bronze should avoid business transformations.

Schema evolution is enabled for additive changes where appropriate,
while source-specific fields are retained so the original payload can be
reconstructed.

Example:

``` text
Bronze
-----
deposit_id
client_id
deposit_date
amount_usd
payment_method / method
currency_original
exchange_rate
status
processing_days
fee_usd
_source_file
_source_file_modification_time
delta_created_ts
delta_created_dt
```

Bronze ingestion metadata is deliberately not treated as part of the
Silver business contract.

------------------------------------------------------------------------

# 4. Silver Vendor Deposit Processing

Silver establishes the canonical business schema.

Processing flow:

``` text
Bronze
  |
  +--> Normalize schema
  |
  +--> Validate mandatory fields
  |
  +--> Detect duplicates
  |
  +--> Validate business rules
  |
  +--> Validate client relationship
  |
  +--> Quarantine invalid records
  |
  +--> MERGE valid records
  |
  v
Silver client_deposit
```

## 4.1 Schema Normalization

The vendor schema drift is normalized:

``` text
payment_method -> payment_method
method         -> payment_method
```

Therefore downstream consumers see one canonical column.

------------------------------------------------------------------------

# 5. Idempotency Strategy

Idempotency is implemented at multiple levels because file-level
exactly-once processing alone does not prevent business duplicates.

## 5.1 File-Level Idempotency

Auto Loader maintains checkpoint state for files already discovered and
processed.

A persistent checkpoint is used rather than an ephemeral checkpoint.

Example:

``` text
/Volumes/workspace/deriv_assement/data/stream/
_silver_checkpoints/client_deposits/
```

If the streaming job restarts, the checkpoint allows processing to
resume without re-reading already committed input as new work.

## 5.2 Business-Level Idempotency

The Silver deposit table uses:

``` text
deposit_id
```

as the business key.

Valid records are written using Delta `MERGE`:

``` sql
MERGE INTO silver.client_deposit t
USING valid_deposits s
ON t.deposit_id = s.deposit_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...);
```

The final implementation intentionally uses explicit column mappings
instead of `UPDATE ALL` / `INSERT ALL`.

This prevents Bronze-only columns such as `_corrupt_record` or
`_rescued_data` from becoming accidental Silver columns or causing MERGE
failures.

## 5.3 Deterministic Deduplication

Duplicates within a batch are reduced before MERGE.

Conceptually:

``` text
partition by deposit_id
order by ingestion metadata descending
```

The deterministic winning record is retained.

This is necessary because the assessment contains duplicate vendor
records across files.

------------------------------------------------------------------------

# 6. Vendor Reconciliation

The pipeline does not assume that the filename date equals the business
date.

Reconciliation uses business keys and business dates.

For each vendor record:

``` text
vendor.deposit_id
        |
        v
warehouse.deposit_id
```

The pipeline identifies:

-   vendor-only deposits
-   warehouse-only deposits
-   amount mismatches
-   client mismatches
-   status mismatches
-   duplicate vendor records

A reconciliation result should contain:

``` text
reconciliation_date
deposit_id
vendor_present
warehouse_present
amount_match
client_match
status_match
reconciliation_status
severity
```

Recommended statuses:

``` text
MATCHED
VENDOR_ONLY
WAREHOUSE_ONLY
FIELD_MISMATCH
DUPLICATE_SOURCE
INVALID_SOURCE
```

Critical reconciliation failures are retained in the
audit/reconciliation layer and can trigger alerts.

------------------------------------------------------------------------

# 7. Late and Missing Data

Late delivery and late-arriving business records are different concepts.

## 7.1 Late File

Example:

``` text
deposits_vendor_20240303.csv
```

arrives after the expected delivery window.

The pipeline does not reject it.

The file is processed when it arrives.

## 7.2 Late Business Event

A record can have:

``` text
deposit_date < delivery_date
```

This is not automatically an error.

The pipeline separates:

``` text
business_date
delivery/ingestion_date
```

Therefore historical deposits can be inserted or corrected based on
`deposit_id`.

## 7.3 Missing File Detection

Expected vendor files should be tracked in a control table/manifest.

Example:

``` text
expected_business_date
expected_file_name
arrival_ts
processing_status
record_count
reconciliation_status
```

A scheduled reconciliation job evaluates the expected delivery calendar.

Example:

``` text
Expected:
2024-03-01 -> received
2024-03-02 -> received
2024-03-03 -> received late
2024-03-04 -> missing
```

The missing date becomes:

``` text
MISSING
```

and remains eligible for subsequent reconciliation.

## 7.4 Self-Reconciliation

The pipeline periodically scans the landing/bronze layer for newly
arrived files.

When a missing file subsequently arrives:

``` text
MISSING
   |
   v
file arrives
   |
   v
Auto Loader discovers it
   |
   v
Bronze
   |
   v
Silver MERGE
   |
   v
reconciliation status updated
```

No manual database correction is required.

The combination of:

-   expected-file control table
-   Auto Loader discovery
-   business-key MERGE
-   source-vs-target reconciliation

allows late data to self-heal.

------------------------------------------------------------------------

# 8. CDC Pipeline

## 8.1 CDC Ingestion

The CDC file contains:

``` text
lsn
commit_ts
op
client_id
before
after
```

The source explicitly states that arrival order is not guaranteed to
match LSN order.

Therefore:

> Arrival order must never be treated as source transaction order.

Flow:

``` text
CDC JSONL
    |
    v
Landing
    |
    v
Auto Loader
    |
    v
Bronze CDC
    |
    v
Validate
    |
    v
Order by LSN
    |
    v
Replay check
    |
    v
SCD2
```

------------------------------------------------------------------------

# 9. CDC Idempotency and Ordering

## 9.1 LSN as the Event Identity

The CDC event is identified using:

``` text
lsn
```

A control table records successfully applied events:

``` text
silver.cdc_applied_events
```

Columns include:

``` text
lsn
client_id
op
commit_ts
applied_ts
source_file
```

Before applying an event:

``` text
Is LSN already applied?
       |
   +---+---+
   |       |
  YES      NO
   |       |
  skip    validate
           |
           v
         apply
           |
           v
      record LSN
```

A replay therefore does not create another SCD2 version.

## 9.2 Out-of-Order Events

CDC events are sorted by LSN before applying them.

Example:

``` text
Arrival:
100
102
101
```

Processing:

``` text
100
101
102
```

For an affected client, an event with an LSN less than or equal to the
currently applied LSN is treated as stale/out-of-order and quarantined
for investigation rather than silently overwriting newer state.

This protects the historical timeline.

------------------------------------------------------------------------

# 10. Source Delete Handling

Hard deletes are not performed in the warehouse.

For:

``` text
op = DELETE
```

the current SCD2 record is end-dated:

``` text
effective_to = delete.commit_ts
is_current = false
```

A new soft-delete version is inserted:

``` text
is_current = true
is_deleted = true
account_status = deleted
```

The CDC event is also retained in the audit/control layer.

Result:

``` text
Client CL001

Version 1
effective_from = 2024-01-01
effective_to   = 2024-11-30
is_current     = false
is_deleted     = false

Version 2
effective_from = 2024-11-30
effective_to   = high_date
is_current     = true
is_deleted     = true
```

## Trade-offs

### Advantages

-   Complete historical auditability.
-   No loss of regulatory history.
-   Historical reports remain reproducible.
-   Current consumers can easily filter `is_deleted = false`.

### Trade-offs

-   Additional storage.
-   Queries must understand current vs historical rows.
-   Downstream consumers must explicitly exclude deleted current records
    where appropriate.

For a financial trading platform, preserving history is preferred over
physical deletion.

------------------------------------------------------------------------

# 11. Data Quality and Edge Cases

The design explicitly handles the following assessment-specific cases.

  -----------------------------------------------------------------------
  Edge case               Severity                Handling
  ----------------------- ----------------------- -----------------------
  Duplicate vendor        Medium                  Deterministic
  deposit ID                                      deduplication by
                                                  `deposit_id`; audit
                                                  duplicate

  `payment_method`        Medium                  Canonical schema
  renamed to `method`                             mapping to
                                                  `payment_method`

  Late vendor file        Medium                  Accept late arrival;
                                                  process by business
                                                  key; reconcile
                                                  expected-file manifest

  Unknown client such as  High                    Quarantine deposit;
  `CL099`                                         reconcile after client
                                                  becomes available

  Negative deposit amount High                    Quarantine; do not
                                                  publish to trusted
                                                  Silver

  Out-of-order CDC LSN    High                    Process by LSN; stale
                                                  events quarantined

  Duplicate/replayed CDC  Medium                  Skip using
  LSN                                             `cdc_applied_events`

  CDC DELETE              Critical/business event End-date current row
                                                  and create soft-delete
                                                  version; retain audit
  -----------------------------------------------------------------------

The assessment requires 2--5 named edge cases; the design deliberately
provides more than the minimum because they are directly present in the
supplied data.

------------------------------------------------------------------------

# 12. Quarantine Strategy

Invalid records are not silently dropped.

Separate quarantine tables are maintained, for example:

``` text
silver.quarantine_deposits
silver.quarantine_profile_cdc
```

A quarantine record should contain:

``` text
quarantine_id
source_file
business_key
reason
severity
raw/derived payload
detected_ts
processing_batch_id
```

This provides a recoverable path:

``` text
Invalid
   |
   v
Quarantine
   |
   +--> investigation
   |
   +--> source correction
   |
   +--> replay
   |
   v
Silver
```

------------------------------------------------------------------------

# 13. DQ Severity Model

The pipeline distinguishes failures by business impact.

  Rule                      Severity                  Action
  ------------------------- ------------------------- -----------------------------------
  Missing deposit ID        Critical                  Quarantine
  Missing client ID         Critical                  Quarantine
  Negative deposit amount   High                      Quarantine
  Unknown client            High                      Quarantine + later reconciliation
  Duplicate deposit ID      Medium                    Deduplicate + audit
  Schema rename             Medium                    Normalize
  Duplicate CDC LSN         Medium                    Ignore replay + audit
  Stale CDC LSN             High                      Quarantine
  CDC delete                Critical/business event   Soft delete + audit

Critical failures affecting the trusted target should not be silently
published.

------------------------------------------------------------------------

# 14. Operational Monitoring

The pipeline maintains operational metadata including:

``` text
process_name
batch_id
process_start_ts
process_end_ts
records_received
records_processed
records_quarantined
duplicate_count
status
error_message
```

Monitoring should expose:

-   ingestion freshness
-   expected vs received files
-   record counts
-   duplicate counts
-   quarantine counts
-   reconciliation mismatches
-   CDC lag
-   processing failures
-   schema changes

Alerts should be triggered for critical failures and SLA breaches.

------------------------------------------------------------------------

# 15. Recovery and Replay

Recovery follows a layered approach.

## File replay

If a file must be replayed:

``` text
Landing
  |
  v
Bronze
  |
  v
Silver MERGE
```

The business key prevents duplicate target records.

## CDC replay

If a CDC file is replayed:

``` text
LSN
 |
 v
cdc_applied_events
 |
 +--> already applied -> skip
 |
 +--> not applied -> process
```

## Failed micro-batch

Streaming checkpoints allow the query to resume from its last committed
progress.

Quarantined records remain available for controlled remediation and
replay.

------------------------------------------------------------------------

# 16. Reconciliation Control Flow

``` mermaid
flowchart TD
    A[Expected File Calendar] --> B{File Received?}
    B -->|No| C[Mark Missing]
    C --> D[Continue Monitoring]
    D --> B

    B -->|Yes| E[Bronze]
    E --> F[Silver Validation]
    F --> G[Business Key MERGE]
    G --> H[Source vs Warehouse Reconciliation]

    H --> I{Match?}
    I -->|Yes| J[MATCHED]
    I -->|No| K[MISMATCH / QUARANTINE]
    K --> L[Audit + Alert]
```

------------------------------------------------------------------------

# 17. Target State

The final architecture separates responsibilities:

``` text
LANDING
  Preserve source files
       |
       v
BRONZE
  Source-faithful data
  + ingestion metadata
       |
       v
SILVER
  Canonical schema
  + validation
  + deduplication
  + referential integrity
  + quarantine
  + CDC SCD2
  + idempotent MERGE
       |
       v
RECONCILIATION
  Source vs target controls
  + missing/late file detection
  + DQ metrics
       |
       v
GOLD
  Trusted dimensional/business data
```

The key principle is:

> **Bronze preserves what the source sent; Silver decides what the
> platform can trust; reconciliation proves that the trusted data
> remains complete and consistent.**

------------------------------------------------------------------------

# 18. Implementation References

The accompanying prototype implements the described Bronze/Silver
processing in Databricks.

Important implementation decisions include:

-   Auto Loader for incremental file discovery.
-   Unity Catalog Volume paths for landing/checkpoints.
-   `_metadata.file_path` for source-file metadata in Unity Catalog
    environments.
-   Delta MERGE for business-key idempotency.
-   Explicit Silver MERGE mappings to prevent Bronze-only schema fields
    from breaking the target contract.
-   `foreachBatch` for file-based vendor-to-Silver processing.
-   CDC LSN tracking through `cdc_applied_events`.
-   SCD2 history with soft-delete handling.
-   Audit and quarantine tables.

For the assessment-sized CDC dataset, sequential LSN application is
acceptable as a prototype. A high-volume production implementation
should use scalable stateful/event-time processing rather than
collecting an entire micro-batch to the driver.

------------------------------------------------------------------------

# 19. Part 1 Completion Checklist

  Requirement                        Status
  ---------------------------------- ----------
  Architecture overview              Complete
  Vendor CSV source-to-target flow   Complete
  CDC source-to-target flow          Complete
  Idempotency mechanism              Complete
  File/checkpoint strategy           Complete
  Business-key MERGE strategy        Complete
  Late data handling                 Complete
  Missing file detection             Complete
  Self-reconciliation                Complete
  Source-delete handling             Complete
  Delete trade-offs                  Complete
  2--5 explicit edge cases           Complete
  Data-quality severity/actions      Complete
  Recovery/replay strategy           Complete
  Operational monitoring             Complete

**Part 1a is therefore complete and submission-ready.**
