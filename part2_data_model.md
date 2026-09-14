# Part 2 --- Dimensional Data Model

## 1. Objective

The objective of Part 2 is to define a production-grade dimensional
model for the trading platform that supports:

-   client-level analytics
-   deposit and trading analytics
-   historical reporting
-   SCD Type 2 client-profile history
-   source updates and deletes without physically deleting analytical
    history
-   late-arriving dimensions
-   historical reloads without corrupting previously established history

The model follows a Kimball-style star-schema approach while keeping the
detailed SCD2 history in a dedicated client-profile dimension.

------------------------------------------------------------------------

## 2. Modeling Principles

### 2.1 Facts represent business events

The fact tables represent measurable business events at a clearly
defined grain.

  Fact             Grain
  ---------------- ------------------------------
  `fact_deposit`   One row per accepted deposit
  `fact_trade`     One row per accepted trade

A deposit or trade must not be duplicated because of joins to
dimensions.

### 2.2 Dimensions describe business entities

Dimensions contain descriptive attributes used to slice and analyze
facts.

Core dimensions:

-   `dim_client`
-   `dim_client_profile_scd2`
-   `dim_date`
-   `dim_instrument`
-   `dim_payment_method`

### 2.3 Surrogate keys

Facts use surrogate dimension keys rather than relying exclusively on
source identifiers.

Example:

``` text
client_id = source/business key
client_sk = warehouse surrogate key
```

This is important because a client can have multiple historical profile
versions while retaining the same business `client_id`.

------------------------------------------------------------------------

# 3. Star Schema

``` mermaid
erDiagram

    DIM_CLIENT {
        BIGINT client_sk PK
        STRING client_id NK
        STRING signup_channel
        STRING country
        DATE signup_date
        BOOLEAN is_current
        TIMESTAMP created_ts
        TIMESTAMP updated_ts
    }

    DIM_CLIENT_PROFILE_SCD2 {
        BIGINT client_profile_sk PK
        STRING client_id FK
        STRING account_status
        STRING risk_level
        STRING profile_segment
        STRING email
        STRING phone
        TIMESTAMP effective_from_ts
        TIMESTAMP effective_to_ts
        BOOLEAN is_current
        BOOLEAN is_deleted
        BIGINT source_lsn
        TIMESTAMP created_ts
        TIMESTAMP updated_ts
    }

    DIM_DATE {
        INT date_sk PK
        DATE calendar_date
        INT calendar_year
        INT quarter_num
        INT month_num
        STRING month_name
        INT week_num
        STRING day_name
        BOOLEAN is_weekend
    }

    DIM_INSTRUMENT {
        BIGINT instrument_sk PK
        STRING instrument_id NK
        STRING instrument_name
        STRING instrument_type
        STRING base_currency
        STRING quote_currency
        BOOLEAN is_active
    }

    DIM_PAYMENT_METHOD {
        BIGINT payment_method_sk PK
        STRING payment_method_code NK
        STRING payment_method_name
        STRING provider
        BOOLEAN is_active
    }

    FACT_DEPOSIT {
        BIGINT deposit_sk PK
        STRING deposit_id NK
        BIGINT client_sk FK
        BIGINT profile_sk FK
        INT deposit_date_sk FK
        BIGINT payment_method_sk FK
        DECIMAL amount_usd
        DECIMAL amount_original
        DECIMAL exchange_rate
        DECIMAL fee_usd
        STRING currency_original
        STRING status
        INT processing_days
        TIMESTAMP transaction_ts
        TIMESTAMP loaded_ts
    }

    FACT_TRADE {
        BIGINT trade_sk PK
        STRING trade_id NK
        BIGINT client_sk FK
        BIGINT profile_sk FK
        BIGINT instrument_sk FK
        INT trade_date_sk FK
        DECIMAL quantity
        DECIMAL price
        DECIMAL notional_usd
        DECIMAL fee_usd
        STRING side
        STRING status
        TIMESTAMP trade_ts
        TIMESTAMP loaded_ts
    }

    DIM_CLIENT ||--o{ FACT_DEPOSIT : "client_sk"
    DIM_CLIENT_PROFILE_SCD2 ||--o{ FACT_DEPOSIT : "profile_sk"
    DIM_DATE ||--o{ FACT_DEPOSIT : "deposit_date_sk"
    DIM_PAYMENT_METHOD ||--o{ FACT_DEPOSIT : "payment_method_sk"

    DIM_CLIENT ||--o{ FACT_TRADE : "client_sk"
    DIM_CLIENT_PROFILE_SCD2 ||--o{ FACT_TRADE : "profile_sk"
    DIM_INSTRUMENT ||--o{ FACT_TRADE : "instrument_sk"
    DIM_DATE ||--o{ FACT_TRADE : "trade_date_sk"
```

------------------------------------------------------------------------

# 4. Dimension Design

## 4.1 `dim_client`

### Grain

One row per logical client.

### Purpose

Contains relatively stable client-level attributes originating from
signup and other master-data sources.

Recommended columns:

  Column             Type        Purpose
  ------------------ ----------- ---------------------------------
  `client_sk`        BIGINT      Surrogate key
  `client_id`        STRING      Source/business key
  `signup_channel`   STRING      Acquisition/signup channel
  `country`          STRING      Client country
  `signup_date`      DATE        Signup date
  `is_current`       BOOLEAN     Current master record
  `created_ts`       TIMESTAMP   Warehouse creation timestamp
  `updated_ts`       TIMESTAMP   Last warehouse update timestamp

`client_id` remains the stable natural/business key.

------------------------------------------------------------------------

# 5. `dim_client_profile_scd2`

This is the key historical dimension required for the assessment.

## 5.1 Why SCD Type 2?

Client profile attributes can change over time.

For example:

``` text
Client CL001
risk_level = LOW
       |
       | profile update
       v
risk_level = HIGH
```

The analytical system must preserve both versions so that historical
reports can answer:

> What was the client's profile at the time of the transaction?

Therefore, Type 2 is preferred over Type 1 for attributes whose
historical values matter.

------------------------------------------------------------------------

## 5.2 SCD2 columns

  Column                Purpose
  --------------------- --------------------------------
  `client_profile_sk`   Surrogate key for each version
  `client_id`           Stable source/business key
  profile attributes    Historical descriptive values
  `effective_from_ts`   Start of validity
  `effective_to_ts`     End of validity
  `is_current`          Identifies current version
  `is_deleted`          Represents source deletion
  `source_lsn`          CDC ordering/audit information
  `created_ts`          Warehouse creation timestamp
  `updated_ts`          Warehouse update timestamp

Open-ended current records use a standard high timestamp such as:

``` text
9999-12-31 23:59:59
```

------------------------------------------------------------------------

# 6. SCD Type 2 Processing

## 6.1 Insert

When a new client profile arrives:

``` text
No existing client_id
        |
        v
Insert new SCD2 version
is_current = true
is_deleted = false
```

Example:

``` text
CL001 | LOW | 2024-03-01 | 9999-12-31 | true
```

------------------------------------------------------------------------

## 6.2 Update

Suppose CDC contains:

``` text
client_id = CL001
op = update
lsn = 105
risk_level = HIGH
```

Existing version:

``` text
CL001 | LOW  | 2024-01-01 | 9999-12-31 | true
```

Processing creates:

``` text
CL001 | LOW  | 2024-01-01 | 2024-03-10 | false
CL001 | HIGH | 2024-03-10 | 9999-12-31 | true
```

The previous version is never overwritten.

### Transactional approach

The implementation should:

1.  identify the latest valid CDC event per client
2.  validate LSN ordering
3.  close the existing current version
4.  insert the new version
5.  record the applied LSN
6.  write an audit record

These operations should be executed as one logical Delta transaction
where possible.

------------------------------------------------------------------------

# 7. Delete Handling

Physical deletion is explicitly prohibited for this solution.

When a CDC delete arrives:

``` text
op = delete
```

the current record is closed and a terminal soft-deleted version is
inserted.

Example:

Before:

``` text
CL001 | ACTIVE | 2024-01-01 | 9999-12-31 | true | false
```

After:

``` text
CL001 | ACTIVE  | 2024-01-01 | 2024-03-20 | false | false
CL001 | DELETED | 2024-03-20 | 9999-12-31 | true  | true
```

The delete event itself is retained in the CDC/audit history.

This provides:

-   historical traceability
-   regulatory/audit support
-   reproducibility
-   protection against accidental data loss

Hard delete is not used.

------------------------------------------------------------------------

# 8. Multiple Updates and Out-of-Order CDC

CDC events are not guaranteed to arrive in LSN order.

Example:

``` text
arrival order:
LSN 108
LSN 106
LSN 107
```

The solution must not simply apply records in arrival order.

## Processing strategy

For each microbatch:

1.  read CDC events
2.  validate required fields
3.  remove already-applied LSNs
4.  order events by `client_id, lsn`
5.  detect stale events
6.  apply valid events in LSN order
7.  record successfully applied LSNs
8.  quarantine invalid/stale events

This protects the SCD2 timeline from moving backwards.

### Important production consideration

The current assessment implementation may process a microbatch
sequentially for deterministic behavior. For a very high-volume
production CDC workload, driver-side `collect()` should not be used. A
scalable implementation should use stateful/event-time processing or a
CDC framework capable of maintaining ordering and deduplication at
scale.

------------------------------------------------------------------------

# 9. Fact Table Design

## 9.1 `fact_deposit`

### Grain

**One row per accepted deposit transaction.**

Business key:

``` text
deposit_id
```

Measures:

-   `amount_usd`
-   `amount_original`
-   `exchange_rate`
-   `fee_usd`
-   `processing_days`

Dimensions:

-   client
-   client profile
-   date
-   payment method

Example analytical query:

``` sql
SELECT
    d.calendar_year,
    d.month_num,
    SUM(f.amount_usd) AS total_deposits
FROM fact_deposit f
JOIN dim_date d
  ON f.deposit_date_sk = d.date_sk
GROUP BY d.calendar_year, d.month_num;
```

------------------------------------------------------------------------

# 10. `fact_trade`

### Grain

**One row per accepted trade.**

Business key:

``` text
trade_id
```

Typical measures:

-   `quantity`
-   `price`
-   `notional_usd`
-   `fee_usd`

Dimensions:

-   client
-   client profile
-   instrument
-   trade date

Example:

``` sql
SELECT
    i.instrument_name,
    SUM(f.notional_usd) AS traded_notional
FROM fact_trade f
JOIN dim_instrument i
  ON f.instrument_sk = i.instrument_sk
GROUP BY i.instrument_name;
```

------------------------------------------------------------------------

# 11. Why Store `profile_sk` in Facts?

This is important for historical correctness.

Suppose:

``` text
March 1:
Client CL001 risk_level = LOW

March 15:
Client CL001 risk_level = HIGH

March 10 trade:
trade_id = T100
```

The trade should resolve to the profile version valid on March 10.

Therefore:

``` text
T100 -> client_sk -> CL001
     -> profile_sk -> LOW profile version
```

A future query should not accidentally attach the March 10 trade to the
client's current HIGH-risk profile.

This is a major reason to use a surrogate SCD2 key in the fact table.

------------------------------------------------------------------------

# 12. Late-Arriving Dimensions

A transaction may arrive before its dimension record.

Example:

``` text
Trade T100 arrives
client_id = CL099

Client dimension CL099 not yet available
```

The fact should not be silently dropped.

## Strategy

Create an inferred/unknown dimension member:

``` text
client_sk = -1
client_id = CL099
is_inferred = true
```

The fact can then be loaded:

``` text
T100 -> client_sk = -1
```

When the real client dimension arrives:

1.  update the inferred dimension record
2.  assign the real client attributes
3.  mark `is_inferred = false`
4.  update affected fact foreign keys if required by the serving model
5.  reconcile the affected records

For SCD2 dimensions, the effective timestamp of the actual dimension
record must be respected when resolving the fact.

------------------------------------------------------------------------

# 13. Unknown Member

A permanent unknown member should also exist.

Example:

``` text
client_sk = 0
client_id = UNKNOWN
```

Use cases include:

-   missing client ID
-   invalid source reference
-   data-quality fallback
-   historical records where the source dimension genuinely cannot be
    resolved

The unknown member prevents nullable foreign keys from spreading through
the star schema.

------------------------------------------------------------------------

# 14. Historical Reload Strategy

Historical reloads are dangerous because a naive overwrite can destroy
SCD2 history.

## Incorrect approach

``` text
DROP TABLE
RELOAD CURRENT DATA
```

This destroys:

-   old profile versions
-   historical auditability
-   transaction-to-profile relationships

## Recommended approach

### Step 1 --- Isolate the reload

Load source data into a temporary/staging Delta table.

``` text
source
  |
  v
staging_reload
```

### Step 2 --- Validate

Run:

-   row-count checks
-   duplicate checks
-   null checks
-   referential-integrity checks
-   amount/business-rule checks
-   source-to-target reconciliation

### Step 3 --- Rebuild only affected scope

For fact data, identify affected business dates/keys and rebuild that
scope.

For dimensions, preserve existing SCD2 versions and reconstruct affected
versions based on source effective timestamps/CDC history.

### Step 4 --- Atomic publish

Use Delta transactional operations such as `MERGE` or controlled
partition replacement.

Do not expose a partially rebuilt table to consumers.

### Step 5 --- Reconcile

Compare:

``` text
source count
target count
inserted
updated
rejected
quarantined
```

and verify key-level totals.

------------------------------------------------------------------------

# 15. Historical Reload Example

Suppose the March 10 deposit file is reprocessed.

The process should:

``` text
March 10 source
      |
      v
staging_deposit_reload
      |
      +--> DQ validation
      |
      +--> deduplication
      |
      +--> reconciliation
      |
      v
affected fact_deposit scope
      |
      v
atomic MERGE/replacement
```

Existing client-profile SCD2 history should not be deleted as part of
the deposit reload.

If the reload also changes profile history, the CDC/source history must
be replayed in LSN/effective-time order and the resulting SCD2 timeline
reconciled.

------------------------------------------------------------------------

# 16. Source Key vs Surrogate Key

The model deliberately separates these concepts.

  Key                   Purpose
  --------------------- -----------------------------------
  `client_id`           Stable source/business identifier
  `client_sk`           Warehouse surrogate key
  `client_profile_sk`   Unique SCD2 profile version
  `deposit_id`          Deposit business key
  `trade_id`            Trade business key
  `instrument_id`       Instrument business key

This allows source systems to retain their identifiers while the
warehouse controls historical dimension relationships.

------------------------------------------------------------------------

# 17. Referential Integrity

Before facts become Gold/serving data:

``` text
fact_deposit.client_sk
        |
        +--> dim_client

fact_deposit.profile_sk
        |
        +--> dim_client_profile_scd2

fact_trade.instrument_sk
        |
        +--> dim_instrument
```

Any unresolved required relationship should either:

-   resolve to an unknown/inferred member, or
-   be quarantined when the business rule says the record is invalid.

The chosen behavior must be measurable and reconciled.

------------------------------------------------------------------------

# 18. SCD2 Quality Checks

The following checks should run as part of data-quality validation.

### More than one current profile

``` sql
SELECT
    client_id,
    COUNT(*) AS current_count
FROM dim_client_profile_scd2
WHERE is_current = true
GROUP BY client_id
HAVING COUNT(*) > 1;
```

Expected result:

``` text
0 rows
```

### Overlapping versions

``` sql
-- Conceptual validation:
-- for each client_id, the next effective_from_ts
-- must be >= the previous effective_to_ts.
```

### Deleted client validation

``` sql
SELECT *
FROM dim_client_profile_scd2
WHERE is_deleted = true
  AND is_current = true
  AND account_status <> 'deleted';
```

Expected result:

``` text
0 rows
```

### Duplicate facts

``` sql
SELECT deposit_id, COUNT(*)
FROM fact_deposit
GROUP BY deposit_id
HAVING COUNT(*) > 1;
```

Expected result:

``` text
0 rows
```

------------------------------------------------------------------------

# 19. Dimensional Model and Gold Layer

The Silver layer is responsible for:

-   canonical schemas
-   DQ
-   deduplication
-   CDC processing
-   SCD2 history
-   referential integrity

The Gold dimensional layer is responsible for:

-   analytics-ready facts
-   conformed dimensions
-   surrogate-key resolution
-   business metrics
-   BI and downstream consumption

This separation prevents analytical consumers from having to understand
raw source behavior.

------------------------------------------------------------------------

# 20. Recommended Gold Tables

``` text
workspace.deriv_assement_gold.dim_client
workspace.deriv_assement_gold.dim_client_profile_scd2
workspace.deriv_assement_gold.dim_date
workspace.deriv_assement_gold.dim_instrument
workspace.deriv_assement_gold.dim_payment_method

workspace.deriv_assement_gold.fact_deposit
workspace.deriv_assement_gold.fact_trade
```

The exact physical names can be adapted to the organization's
catalog/schema conventions.

------------------------------------------------------------------------

# 21. Design Decisions and Trade-offs

  Decision                          Reason
  --------------------------------- ---------------------------------------------
  Kimball star schema               Simple and efficient analytical consumption
  Separate SCD2 profile dimension   Preserves client-profile history
  Surrogate keys                    Correct historical relationships
  Business keys retained            Traceability to source
  Soft delete                       Required auditability and no hard deletion
  Unknown member                    Prevents broken dimensional relationships
  Inferred member                   Supports late-arriving dimensions
  Delta MERGE                       Idempotent incremental processing
  Atomic historical reload          Prevents partial/inconsistent publication
  CDC ordered by LSN                Protects temporal correctness
  Fact grain explicitly defined     Prevents accidental double counting

------------------------------------------------------------------------

# 22. End-to-End Historical Example

Consider:

``` text
01-Mar
CL001 profile = LOW

10-Mar
Trade T100 occurs

15-Mar
CL001 profile changes LOW -> HIGH

20-Mar
CL001 is deleted
```

The SCD2 dimension becomes conceptually:

``` text
client_id | risk | from       | to         | current | deleted
----------|------|------------|------------|---------|--------
CL001     | LOW  | 01-Mar     | 15-Mar     | false   | false
CL001     | HIGH | 15-Mar     | 20-Mar     | false   | false
CL001     | DEL  | 20-Mar     | 9999-12-31 | true   | true
```

Trade `T100`, occurring on March 10, points to the LOW profile surrogate
key.

Therefore historical analytics remain correct even after the client
becomes HIGH risk and is later deleted.

------------------------------------------------------------------------

# 23. Part 2 Completion Checklist

-   [x] Dimensional model defined
-   [x] Fact grains explicitly defined
-   [x] Client dimension defined
-   [x] Client Profile SCD2 dimension defined
-   [x] Date dimension defined
-   [x] Instrument dimension defined
-   [x] Payment Method dimension defined
-   [x] Deposit fact defined
-   [x] Trade fact defined
-   [x] Surrogate keys defined
-   [x] Business/source keys retained
-   [x] SCD2 update handling defined
-   [x] SCD2 delete handling defined
-   [x] Soft delete/end-dating defined
-   [x] Out-of-order CDC handling defined
-   [x] Late-arriving dimension strategy defined
-   [x] Unknown/inferred members defined
-   [x] Historical reload strategy defined
-   [x] SCD2 validation checks defined
-   [x] Historical example included

------------------------------------------------------------------------

## 24. Implementation Alignment

The Part 2 model is designed to align with the Part 1 pipeline:

``` text
Batch JSON
    |
    v
Bronze
    |
    v
Silver canonical entities
    |
    +--------------------+
    |                    |
    v                    v
Current dimensions     SCD2 profile
    |                    |
    +---------+----------+
              |
              v
        Gold dimensions
              |
              v
        Gold fact tables
```

The model therefore separates ingestion concerns from analytical
modeling concerns while preserving source traceability and historical
correctness.
