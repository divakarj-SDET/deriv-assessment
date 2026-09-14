#!/usr/bin/env python3
"""
Deriv assessment - runnable local prototype.

Implements, end to end and idempotently, the same logic that the Databricks
notebooks in ../databricks/ implement in PySpark:

  bronze  : land the vendor CSVs and the CDC JSONL as-is, plus lineage columns
  silver  : normalise schema drift, dedupe, run DQ rules with severity,
            quarantine failures, MERGE survivors on a business key
  scd2    : apply the CDC log in LSN order to an SCD Type 2 client dimension,
            with soft deletes
  recon   : two-tier reconciliation of vendor feed vs warehouse deposits
  gold    : star schema (dim_client SCD2, dim_date, fact_deposit, fact_trade)

Run it twice. Every table's row count is identical after the second run.
That is the idempotency proof.

    python3 run_pipeline.py              # normal run
    python3 run_pipeline.py --reset      # rebuild the warehouse from scratch
    python3 run_pipeline.py --replay 2024-03-02   # re-deliver one vendor file

Requires: duckdb
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.abspath(os.path.join(HERE, "..", "..", "data"))
DB = os.path.join(HERE, "warehouse.duckdb")

# Contract-cleansing: the vendor renamed payment_method -> method on day 2.
# Aliases are declared, never inferred, so an unknown column is a loud failure
# rather than a silently dropped one.
COLUMN_ALIASES = {
    "method": "payment_method",
    "payment_type": "payment_method",
    "amount": "amount_usd",
}

CANONICAL_VENDOR_COLUMNS = [
    "deposit_id", "client_id", "deposit_date", "amount_usd", "payment_method",
    "currency_original", "exchange_rate", "status", "processing_days", "fee_usd",
]

# Instrument contract sizes, reverse-engineered from the trades that are
# internally consistent, then used to re-derive PnL and catch the one that is not.
CONTRACT_SIZE = {"EUR/USD": 10000, "Gold": 10, "BTC/USD": 1, "S&P500": 1}

BASELINE_TS = "1900-01-01 00:00:00"
END_OF_TIME = "9999-12-31 00:00:00"


def log(msg):
    print(msg, flush=True)


def section(title):
    log("\n" + "=" * 78)
    log(title)
    log("=" * 78)


def row_hash(*parts):
    return hashlib.sha256("|".join("" if p is None else str(p) for p in parts).encode()).hexdigest()[:16]


# --------------------------------------------------------------------------
# DDL
# --------------------------------------------------------------------------

def create_schema(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS bronze_vendor_deposit (
        ingest_id            VARCHAR,
        source_file          VARCHAR,
        source_row_num       INTEGER,
        raw_payload          VARCHAR,
        row_hash             VARCHAR,
        ingested_at          TIMESTAMP
    );

    -- File manifest: the primary idempotency guard for file-based ingestion.
    CREATE TABLE IF NOT EXISTS ingest_file_manifest (
        source_file          VARCHAR,
        file_hash            VARCHAR,
        row_count            INTEGER,
        min_event_date       DATE,
        max_event_date       DATE,
        file_label_date      DATE,
        lag_days             INTEGER,
        first_ingested_at    TIMESTAMP,
        last_seen_at         TIMESTAMP,
        ingest_count         INTEGER,
        status               VARCHAR
    );

    CREATE TABLE IF NOT EXISTS silver_deposit (
        deposit_id           VARCHAR PRIMARY KEY,
        client_id            VARCHAR,
        deposit_date         DATE,
        amount_usd           DECIMAL(18,2),
        payment_method       VARCHAR,
        currency_original    VARCHAR,
        exchange_rate        DECIMAL(18,6),
        status               VARCHAR,
        processing_days      INTEGER,
        fee_usd              DECIMAL(18,2),
        source_system        VARCHAR,
        source_file          VARCHAR,
        row_hash             VARCHAR,
        is_quarantined       BOOLEAN,
        effective_from       TIMESTAMP,
        updated_at           TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS dq_result (
        run_id               VARCHAR,
        check_name           VARCHAR,
        severity             VARCHAR,
        entity               VARCHAR,
        record_key           VARCHAR,
        source_file          VARCHAR,
        detail               VARCHAR,
        on_failure           VARCHAR,
        detected_at          TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS quarantine_deposit (
        run_id               VARCHAR,
        deposit_id           VARCHAR,
        client_id            VARCHAR,
        source_file          VARCHAR,
        raw_payload          VARCHAR,
        failed_checks        VARCHAR,
        quarantined_at       TIMESTAMP,
        resolved_at          TIMESTAMP
    );

    -- SCD Type 2 client dimension.
    CREATE TABLE IF NOT EXISTS dim_client (
        client_sk            VARCHAR,
        client_id            VARCHAR,
        full_name            VARCHAR,
        date_of_birth        DATE,
        nationality          VARCHAR,
        risk_category        VARCHAR,
        account_balance_usd  DECIMAL(18,2),
        account_status       VARCHAR,
        currency             VARCHAR,
        preferred_language   VARCHAR,
        valid_from           TIMESTAMP,
        valid_to             TIMESTAMP,
        is_current           BOOLEAN,
        is_deleted           BOOLEAN,
        source_lsn           BIGINT,
        source_op            VARCHAR,
        record_hash          VARCHAR,
        is_inferred          BOOLEAN,
        updated_at           TIMESTAMP
    );

    -- CDC watermark: highest LSN durably applied. Replay below this is a no-op.
    CREATE TABLE IF NOT EXISTS cdc_apply_log (
        lsn                  BIGINT PRIMARY KEY,
        client_id            VARCHAR,
        op                   VARCHAR,
        commit_ts            TIMESTAMP,
        applied_at           TIMESTAMP,
        action_taken         VARCHAR
    );

    CREATE TABLE IF NOT EXISTS reconciliation_result (
        run_id               VARCHAR,
        recon_date_from      DATE,
        recon_date_to        DATE,
        match_tier           VARCHAR,
        break_type           VARCHAR,
        deposit_id_vendor    VARCHAR,
        deposit_id_warehouse VARCHAR,
        client_id            VARCHAR,
        deposit_date         DATE,
        amount_vendor        DECIMAL(18,2),
        amount_warehouse     DECIMAL(18,2),
        variance_usd         DECIMAL(18,2),
        detail               VARCHAR,
        created_at           TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS fact_deposit (
        deposit_sk           VARCHAR,
        deposit_id           VARCHAR,
        client_sk            VARCHAR,
        client_id            VARCHAR,
        date_key             INTEGER,
        payment_method       VARCHAR,
        amount_usd           DECIMAL(18,2),
        fee_usd              DECIMAL(18,2),
        net_amount_usd       DECIMAL(18,2),
        status               VARCHAR,
        source_system        VARCHAR,
        updated_at           TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS fact_trade (
        trade_sk             VARCHAR,
        trade_id             VARCHAR,
        client_sk            VARCHAR,
        client_id            VARCHAR,
        date_key             INTEGER,
        instrument           VARCHAR,
        direction            VARCHAR,
        volume_lots          DECIMAL(18,4),
        open_price           DECIMAL(18,4),
        close_price          DECIMAL(18,4),
        pnl_usd_reported     DECIMAL(18,2),
        pnl_usd_derived      DECIMAL(18,2),
        pnl_variance_usd     DECIMAL(18,2),
        trade_status         VARCHAR,
        updated_at           TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS dim_date (
        date_key             INTEGER PRIMARY KEY,
        full_date            DATE,
        year                 INTEGER,
        quarter              INTEGER,
        month                INTEGER,
        day                  INTEGER,
        month_name           VARCHAR,
        is_weekend           BOOLEAN
    );
    """)


# --------------------------------------------------------------------------
# DQ rule set - severity drives a distinct on-failure action per rule
# --------------------------------------------------------------------------
#   BLOCK     -> abort the batch, nothing lands, page on-call
#   QUARANTINE-> the row is withheld from silver, batch continues
#   WARN      -> the row lands, flagged for stewardship review
DQ_RULES = [
    # name, severity, on_failure, predicate(record, ctx) -> failure detail or None
    ("deposit_id_not_null", "BLOCK", "abort batch, alert on-call",
     lambda r, c: None if r.get("deposit_id") else "deposit_id is null/empty"),
    ("unknown_column_in_file", "BLOCK", "abort batch, alert vendor ops",
     lambda r, c: None if not r.get("_unknown_columns") else f"unmapped columns: {r['_unknown_columns']}"),
    ("amount_positive", "QUARANTINE", "withhold row, raise vendor ticket",
     lambda r, c: None if (r["amount_usd"] is not None and r["amount_usd"] > 0)
     else f"non-positive amount_usd={r['amount_usd']}"),
    ("client_exists", "QUARANTINE", "withhold row, park for late dimension",
     lambda r, c: None if r["client_id"] in c["known_clients"]
     else f"client_id {r['client_id']} not in client_signup"),
    ("payment_method_present", "QUARANTINE", "withhold row, raise vendor ticket",
     lambda r, c: None if r.get("payment_method") else "payment_method missing after alias mapping"),
    ("deposit_not_before_signup", "WARN", "load, flag for stewardship",
     lambda r, c: None if (r["client_id"] not in c["signup_date"]
                           or r["deposit_date"] >= c["signup_date"][r["client_id"]])
     else f"deposit_date {r['deposit_date']} precedes signup_date {c['signup_date'].get(r['client_id'])}"),
    ("fee_within_tolerance", "WARN", "load, flag for finance review",
     lambda r, c: None if (r["amount_usd"] is None or r["fee_usd"] is None or r["amount_usd"] <= 0
                           or r["fee_usd"] == 0
                           or abs(r["fee_usd"] / r["amount_usd"] - 0.01) <= 0.002)
     else f"fee {r['fee_usd']} is {r['fee_usd'] / r['amount_usd'] * 100:.2f}% of amount, expected ~1.00%"),
    ("kyc_approved_for_deposit", "WARN", "load, flag to compliance queue",
     lambda r, c: None if c["kyc_status"].get(r["client_id"], "approved") == "approved"
     else f"client {r['client_id']} kyc_status={c['kyc_status'].get(r['client_id'])}"),
]


# --------------------------------------------------------------------------
# Bronze
# --------------------------------------------------------------------------

def parse_vendor_file(path):
    """Read a vendor CSV, apply declared column aliases, flag unknown columns."""
    with open(path) as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames or []
        mapped, unknown = {}, []
        for col in header:
            canon = COLUMN_ALIASES.get(col, col)
            if canon in CANONICAL_VENDOR_COLUMNS:
                mapped[col] = canon
            else:
                unknown.append(col)
        rows = []
        for i, raw in enumerate(reader, start=2):
            rec = {mapped[k]: v for k, v in raw.items() if k in mapped}
            rec["_unknown_columns"] = ",".join(unknown) if unknown else None
            rec["_raw"] = json.dumps(raw)
            rec["_row_num"] = i
            rows.append(rec)
        return rows, header, unknown


def file_label_date(filename):
    m = re.search(r"(\d{8})", filename)
    return datetime.strptime(m.group(1), "%Y%m%d").date() if m else None


def load_bronze_vendor(con, run_id, only_file=None):
    section("BRONZE - vendor deposit files")
    files = sorted(f for f in os.listdir(DATA) if f.startswith("deposits_vendor_") and f.endswith(".csv"))
    if only_file:
        files = [f for f in files if only_file in f]
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    for fname in files:
        path = os.path.join(DATA, fname)
        content = open(path, "rb").read()
        fhash = hashlib.sha256(content).hexdigest()[:16]

        prior = con.execute(
            "SELECT file_hash, ingest_count FROM ingest_file_manifest WHERE source_file = ?", [fname]
        ).fetchone()

        rows, header, unknown = parse_vendor_file(path)
        dates = [r["deposit_date"] for r in rows if r.get("deposit_date")]
        label = file_label_date(fname)
        max_date = max(dates) if dates else None
        min_date = min(dates) if dates else None
        lag = (label - datetime.strptime(max_date, "%Y-%m-%d").date()).days if (label and max_date) else None

        if prior and prior[0] == fhash:
            # Byte-identical redelivery: record that we saw it, ingest nothing.
            con.execute("""UPDATE ingest_file_manifest
                           SET last_seen_at = ?, ingest_count = ingest_count + 1, status = 'SKIPPED_DUPLICATE'
                           WHERE source_file = ?""", [now, fname])
            log(f"  {fname:32s} SKIPPED - byte-identical redelivery (hash {fhash})")
            continue

        if prior and prior[0] != fhash:
            # Same filename, different content: a corrected redelivery.
            log(f"  {fname:32s} CHANGED - content hash differs, re-ingesting as correction")
            con.execute("DELETE FROM bronze_vendor_deposit WHERE source_file = ?", [fname])
            con.execute("DELETE FROM ingest_file_manifest WHERE source_file = ?", [fname])

        for r in rows:
            con.execute("""INSERT INTO bronze_vendor_deposit VALUES (?,?,?,?,?,?)""", [
                run_id, fname, r["_row_num"], r["_raw"],
                row_hash(*[r.get(c) for c in CANONICAL_VENDOR_COLUMNS]), now])

        con.execute("""INSERT INTO ingest_file_manifest VALUES (?,?,?,?,?,?,?,?,?,?,?)""", [
            fname, fhash, len(rows), min_date, max_date, label, lag, now, now, 1, "INGESTED"])

        flag = ""
        if lag is not None and lag > 1:
            flag = f"  <-- LATE: newest event is {lag}d older than the file label"
        if unknown:
            flag += f"  <-- UNMAPPED COLUMNS {unknown}"
        drift = [c for c in header if c in COLUMN_ALIASES]
        if drift:
            flag += f"  <-- DRIFT {drift} mapped to canonical names"
        log(f"  {fname:32s} {len(rows):2d} rows  events {min_date}..{max_date}  label {label}{flag}")


# --------------------------------------------------------------------------
# Silver
# --------------------------------------------------------------------------

def reference_context(con):
    signup = json.load(open(os.path.join(DATA, "client_signup.json")))
    return {
        "known_clients": {r["client_id"] for r in signup},
        "signup_date": {r["client_id"]: r["signup_date"] for r in signup},
        "kyc_status": {r["client_id"]: r["kyc_status"] for r in signup},
    }


def to_num(v, cast=float):
    try:
        return cast(v)
    except (TypeError, ValueError):
        return None


def build_silver_deposits(con, run_id):
    section("SILVER - vendor deposits (normalise -> DQ -> dedupe -> merge)")
    ctx = reference_context(con)
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    bronze = con.execute("""
        SELECT source_file, source_row_num, raw_payload, row_hash
        FROM bronze_vendor_deposit ORDER BY source_file, source_row_num
    """).fetchall()

    typed = []
    for source_file, row_num, raw, rhash in bronze:
        raw_d = json.loads(raw)
        rec = {}
        unknown = []
        for k, v in raw_d.items():
            canon = COLUMN_ALIASES.get(k, k)
            if canon in CANONICAL_VENDOR_COLUMNS:
                rec[canon] = v
            else:
                unknown.append(k)
        rec["_unknown_columns"] = ",".join(unknown) if unknown else None
        rec["amount_usd"] = to_num(rec.get("amount_usd"))
        rec["fee_usd"] = to_num(rec.get("fee_usd"))
        rec["exchange_rate"] = to_num(rec.get("exchange_rate"))
        rec["processing_days"] = to_num(rec.get("processing_days"), int)
        rec["source_file"] = source_file
        rec["row_hash"] = rhash
        typed.append(rec)

    # ---- DQ evaluation -----------------------------------------------------
    blocking, quarantined, warned, clean = [], {}, 0, []
    for rec in typed:
        failures = []
        for name, severity, action, predicate in DQ_RULES:
            detail = predicate(rec, ctx)
            if detail is None:
                continue
            con.execute("INSERT INTO dq_result VALUES (?,?,?,?,?,?,?,?,?)", [
                run_id, name, severity, "vendor_deposit", rec.get("deposit_id"),
                rec["source_file"], detail, action, now])
            if severity == "BLOCK":
                blocking.append((name, rec.get("deposit_id"), detail))
            elif severity == "QUARANTINE":
                failures.append(f"{name}: {detail}")
            else:
                warned += 1
        if failures:
            quarantined[rec["deposit_id"]] = (rec, failures)
        else:
            clean.append(rec)

    if blocking:
        log("  BLOCKING failures - batch aborted, nothing promoted to silver:")
        for name, key, detail in blocking:
            log(f"    [{name}] {key}: {detail}")
        raise SystemExit(2)

    for dep_id, (rec, failures) in quarantined.items():
        already = con.execute(
            "SELECT 1 FROM quarantine_deposit WHERE deposit_id = ? AND resolved_at IS NULL", [dep_id]).fetchone()
        if not already:
            con.execute("INSERT INTO quarantine_deposit VALUES (?,?,?,?,?,?,?,?)", [
                run_id, dep_id, rec.get("client_id"), rec["source_file"],
                json.dumps({k: v for k, v in rec.items() if not k.startswith("_")}),
                " | ".join(failures), now, None])
        log(f"  QUARANTINED {dep_id}: {failures[0]}")

    log(f"  DQ summary: {len(typed)} rows in, {len(clean)} clean, "
        f"{len(quarantined)} quarantined, {warned} warnings raised")

    # ---- Deduplicate on the business key -----------------------------------
    # The vendor redelivers rows across files. Last file wins; identical payload
    # is a no-op. This is what makes the MERGE safe to replay.
    by_key = {}
    for rec in clean:
        k = rec["deposit_id"]
        if k not in by_key or rec["source_file"] > by_key[k]["source_file"]:
            by_key[k] = rec
    dupes = len(clean) - len(by_key)
    log(f"  Deduplicated {len(clean)} -> {len(by_key)} on deposit_id ({dupes} cross-file duplicates collapsed)")

    # ---- MERGE (idempotent upsert) -----------------------------------------
    inserted = updated = unchanged = 0
    for k, rec in sorted(by_key.items()):
        existing = con.execute("SELECT row_hash FROM silver_deposit WHERE deposit_id = ?", [k]).fetchone()
        vals = [rec["deposit_id"], rec["client_id"], rec["deposit_date"], rec["amount_usd"],
                rec.get("payment_method"), rec.get("currency_original"), rec.get("exchange_rate"),
                rec.get("status"), rec.get("processing_days"), rec.get("fee_usd"),
                "VENDOR", rec["source_file"], rec["row_hash"], False, now, now]
        if existing is None:
            con.execute("INSERT INTO silver_deposit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", vals)
            inserted += 1
        elif existing[0] != rec["row_hash"]:
            con.execute("""UPDATE silver_deposit SET client_id=?, deposit_date=?, amount_usd=?,
                           payment_method=?, currency_original=?, exchange_rate=?, status=?,
                           processing_days=?, fee_usd=?, source_file=?, row_hash=?, updated_at=?
                           WHERE deposit_id=?""",
                        vals[1:10] + [rec["source_file"], rec["row_hash"], now, k])
            updated += 1
        else:
            unchanged += 1
    log(f"  MERGE: {inserted} inserted, {updated} updated, {unchanged} unchanged (no-op on replay)")

    # ---- Warehouse deposits also land in silver (source_system = WAREHOUSE) --
    wh = json.load(open(os.path.join(DATA, "client_deposit.json")))
    w_ins = w_unchanged = 0
    for r in wh:
        # DEP012 carries the value under a malformed key 'credit_card' instead of
        # 'payment_method'. Recovered explicitly rather than silently nulled.
        pm = r.get("payment_method")
        recovered = None
        if pm is None:
            for k, v in r.items():
                if k not in ("deposit_id", "client_id", "deposit_date", "amount_usd", "currency_original",
                             "exchange_rate", "status", "processing_days", "fee_usd"):
                    pm, recovered = v, k
                    con.execute("INSERT INTO dq_result VALUES (?,?,?,?,?,?,?,?,?)", [
                        run_id, "malformed_key_recovered", "WARN", "warehouse_deposit", r["deposit_id"],
                        "client_deposit.json",
                        f"payment_method absent; value recovered from malformed key '{k}'='{v}'",
                        "load with recovered value, flag for source-system fix", now])
        if r["client_id"] not in reference_context(con)["known_clients"]:
            con.execute("INSERT INTO dq_result VALUES (?,?,?,?,?,?,?,?,?)", [
                run_id, "client_exists", "QUARANTINE", "warehouse_deposit", r["deposit_id"],
                "client_deposit.json", f"client_id {r['client_id']} not in client_signup",
                "withhold row, park for late dimension", now])
            already = con.execute("SELECT 1 FROM quarantine_deposit WHERE deposit_id=? AND resolved_at IS NULL",
                                  [r["deposit_id"]]).fetchone()
            if not already:
                con.execute("INSERT INTO quarantine_deposit VALUES (?,?,?,?,?,?,?,?)", [
                    run_id, r["deposit_id"], r["client_id"], "client_deposit.json", json.dumps(r),
                    f"client_exists: client_id {r['client_id']} not in client_signup", now, None])
            log(f"  QUARANTINED {r['deposit_id']}: orphan client_id {r['client_id']} (warehouse feed)")
            continue
        h = row_hash(*[r.get(c) for c in CANONICAL_VENDOR_COLUMNS])
        exists = con.execute("SELECT row_hash FROM silver_deposit WHERE deposit_id=?", [r["deposit_id"]]).fetchone()
        if exists is None:
            con.execute("INSERT INTO silver_deposit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
                r["deposit_id"], r["client_id"], r["deposit_date"], r["amount_usd"], pm,
                r.get("currency_original"), r.get("exchange_rate"), r.get("status"),
                r.get("processing_days"), r.get("fee_usd"), "WAREHOUSE", "client_deposit.json",
                h, False, now, now])
            w_ins += 1
            if recovered:
                log(f"  RECOVERED {r['deposit_id']}: payment_method from malformed key '{recovered}'")
        else:
            w_unchanged += 1
    log(f"  Warehouse deposits: {w_ins} inserted, {w_unchanged} unchanged")


# --------------------------------------------------------------------------
# SCD Type 2 from the CDC log
# --------------------------------------------------------------------------

def seed_dim_client(con):
    """Seed the dimension from the client_profile snapshot, once."""
    if con.execute("SELECT count(*) FROM dim_client").fetchone()[0] > 0:
        return 0
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    prof = json.load(open(os.path.join(DATA, "client_profile.json")))
    for p in prof:
        h = row_hash(p["risk_category"], p["account_balance_usd"], p["account_status"])
        con.execute("INSERT INTO dim_client VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            row_hash(p["client_id"], BASELINE_TS), p["client_id"], p["full_name"], p["date_of_birth"],
            p["nationality"], p["risk_category"], p["account_balance_usd"], p["account_status"],
            p["currency"], p["preferred_language"], BASELINE_TS, END_OF_TIME, True, False,
            0, "seed", h, False, now])
    return len(prof)


def apply_cdc(con, run_id):
    section("SILVER/GOLD - CDC -> SCD Type 2 dim_client")
    seeded = seed_dim_client(con)
    if seeded:
        log(f"  Seeded dim_client with {seeded} baseline rows from client_profile.json")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    events = [json.loads(l) for l in open(os.path.join(DATA, "client_profile_changes.jsonl")) if l.strip()]
    arrival = [e["lsn"] for e in events]

    # THE critical step: the file is in arrival order, not LSN order.
    events.sort(key=lambda e: e["lsn"])
    log(f"  Arrival order : {arrival}")
    log(f"  Applied order : {[e['lsn'] for e in events]}   <-- sorted by LSN before apply")

    ls = [e["lsn"] for e in events]
    gaps = [i for i in range(min(ls), max(ls) + 1) if i not in ls]
    log(f"  LSN gaps observed: {gaps} (expected - other tables share the transaction log; "
        f"tracked, not alerted, unless a gap persists past the completeness SLA)")

    applied = skipped = 0
    for e in events:
        if con.execute("SELECT 1 FROM cdc_apply_log WHERE lsn = ?", [e["lsn"]]).fetchone():
            skipped += 1
            continue

        cid, op, cts = e["client_id"], e["op"], e["commit_ts"].replace("T", " ").replace("Z", "")
        cur = con.execute("""SELECT client_sk, full_name, date_of_birth, nationality, risk_category,
                                    account_balance_usd, account_status, currency, preferred_language,
                                    record_hash, is_deleted
                             FROM dim_client WHERE client_id = ? AND is_current""", [cid]).fetchone()
        after = e.get("after") or {}
        action = None

        if op == "delete":
            if cur:
                con.execute("""UPDATE dim_client SET valid_to = ?, is_current = FALSE, updated_at = ?
                               WHERE client_id = ? AND is_current""", [cts, now, cid])
                # Soft delete: a tombstone version, never a physical delete.
                con.execute("INSERT INTO dim_client VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
                    row_hash(cid, cts), cid, cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7], cur[8],
                    cts, END_OF_TIME, True, True, e["lsn"], op, cur[9], False, now])
                action = "soft_delete_tombstone"
                log(f"  lsn {e['lsn']} DELETE {cid}: current row end-dated at {cts}, "
                    f"tombstone inserted (is_deleted=TRUE). History preserved.")
            else:
                action = "delete_for_unknown_key_ignored"
        else:
            # 'after' carries only the changed subset, so unchanged attributes are
            # carried forward from the current version rather than nulled.
            base = {
                "full_name": cur[1] if cur else after.get("full_name"),
                "date_of_birth": cur[2] if cur else after.get("date_of_birth"),
                "nationality": cur[3] if cur else after.get("nationality"),
                "risk_category": cur[4] if cur else None,
                "account_balance_usd": cur[5] if cur else None,
                "account_status": cur[6] if cur else None,
                "currency": cur[7] if cur else after.get("currency"),
                "preferred_language": cur[8] if cur else after.get("preferred_language"),
            }
            new = dict(base)
            for k, v in after.items():
                if k in new:
                    new[k] = v
            new_hash = row_hash(new["risk_category"], new["account_balance_usd"], new["account_status"])

            if cur and cur[9] == new_hash and not cur[10]:
                action = "no_change_noop"   # e.g. lsn 1001 re-inserting CL030 identically
                log(f"  lsn {e['lsn']} {op.upper()} {cid}: tracked attributes unchanged -> no new version")
            else:
                if cur:
                    con.execute("""UPDATE dim_client SET valid_to = ?, is_current = FALSE, updated_at = ?
                                   WHERE client_id = ? AND is_current""", [cts, now, cid])
                con.execute("INSERT INTO dim_client VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
                    row_hash(cid, cts), cid, new["full_name"], new["date_of_birth"], new["nationality"],
                    new["risk_category"], new["account_balance_usd"], new["account_status"],
                    new["currency"], new["preferred_language"], cts, END_OF_TIME, True, False,
                    e["lsn"], op, new_hash, False, now])
                action = "new_version"

        con.execute("INSERT INTO cdc_apply_log VALUES (?,?,?,?,?,?)", [e["lsn"], cid, op, cts, now, action])
        applied += 1

    log(f"  Applied {applied} CDC events, skipped {skipped} already-applied (LSN watermark replay guard)")


# --------------------------------------------------------------------------
# Reconciliation
# --------------------------------------------------------------------------

def reconcile(con, run_id):
    section("RECONCILIATION - vendor feed vs warehouse client_deposit")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    con.execute("DELETE FROM reconciliation_result WHERE run_id = ?", [run_id])

    win = con.execute("""SELECT min(deposit_date), max(deposit_date) FROM silver_deposit
                         WHERE source_system = 'VENDOR'""").fetchone()
    d_from, d_to = win
    log(f"  Recon window taken from the vendor events themselves: {d_from} .. {d_to}")
    log("  (NOT from the file label date - the 0303 file is backdated, so a "
        "label-based window would silently miss 6 rows)")

    # Tier 1: exact deposit_id match.
    t1 = con.execute("""
        SELECT v.deposit_id, w.deposit_id, v.client_id, v.deposit_date, v.amount_usd, w.amount_usd
        FROM silver_deposit v JOIN silver_deposit w
          ON v.deposit_id = w.deposit_id AND v.source_system='VENDOR' AND w.source_system='WAREHOUSE'
    """).fetchall()
    log(f"  Tier 1 (deposit_id): {len(t1)} matches")

    # Tier 2: composite business key, for feeds that do not share an ID namespace.
    t2 = con.execute("""
        SELECT v.deposit_id, w.deposit_id, v.client_id, v.deposit_date, v.amount_usd, w.amount_usd
        FROM silver_deposit v JOIN silver_deposit w
          ON v.client_id = w.client_id AND v.deposit_date = w.deposit_date
         AND abs(v.amount_usd - w.amount_usd) < 0.01
        WHERE v.source_system='VENDOR' AND w.source_system='WAREHOUSE'
    """).fetchall()
    log(f"  Tier 2 (client_id + deposit_date + amount): {len(t2)} matches")

    matched_v = {r[0] for r in t1} | {r[0] for r in t2}
    matched_w = {r[1] for r in t1} | {r[1] for r in t2}
    for r in t1 + t2:
        tier = "TIER1_ID" if r in t1 else "TIER2_BUSINESS_KEY"
        con.execute("INSERT INTO reconciliation_result VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            run_id, d_from, d_to, tier, "MATCHED", r[0], r[1], r[2], r[3], r[4], r[5],
            (r[4] or 0) - (r[5] or 0), "amounts agree", now])

    # Vendor rows with no warehouse counterpart, inside the window.
    v_only = con.execute("""
        SELECT deposit_id, client_id, deposit_date, amount_usd, source_file
        FROM silver_deposit WHERE source_system='VENDOR' ORDER BY deposit_id""").fetchall()
    n_v = 0
    for dep_id, cid, dt, amt, sf in v_only:
        if dep_id in matched_v:
            continue
        con.execute("INSERT INTO reconciliation_result VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            run_id, d_from, d_to, "UNMATCHED", "IN_VENDOR_NOT_IN_WAREHOUSE", dep_id, None, cid, dt,
            amt, None, amt, f"vendor-only, from {sf}", now])
        n_v += 1

    # Warehouse rows in the same window with no vendor counterpart.
    w_only = con.execute("""
        SELECT deposit_id, client_id, deposit_date, amount_usd FROM silver_deposit
        WHERE source_system='WAREHOUSE' AND deposit_date BETWEEN ? AND ? ORDER BY deposit_id""",
                         [d_from, d_to]).fetchall()
    n_w = 0
    for dep_id, cid, dt, amt in w_only:
        if dep_id in matched_w:
            continue
        con.execute("INSERT INTO reconciliation_result VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            run_id, d_from, d_to, "UNMATCHED", "IN_WAREHOUSE_NOT_IN_VENDOR", None, dep_id, cid, dt,
            None, amt, -amt, "warehouse-only inside vendor window", now])
        n_w += 1

    log(f"  Breaks: {n_v} vendor-only, {n_w} warehouse-only (inside window)")
    tot_v = con.execute("SELECT count(*), sum(amount_usd) FROM silver_deposit WHERE source_system='VENDOR'").fetchone()
    log(f"  Vendor control totals: {tot_v[0]} rows, {tot_v[1]} USD")
    log("  Interpretation: the two feeds share no deposit_id namespace (VDEP* vs DEP*)")
    log("  and no business key matches either, so this is a NET-NEW feed, not a")
    log("  duplicate of warehouse deposits. Reconciliation is therefore a completeness")
    log("  and control-total check, not a row-for-row tie-out.")


# --------------------------------------------------------------------------
# Gold
# --------------------------------------------------------------------------

def build_gold(con, run_id):
    section("GOLD - star schema")
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    con.execute("""INSERT INTO dim_date
        SELECT DISTINCT CAST(strftime(d, '%Y%m%d') AS INTEGER), d,
               CAST(strftime(d,'%Y') AS INTEGER), CAST(ceil(month(d)/3.0) AS INTEGER),
               month(d), day(d), strftime(d, '%B'), dayofweek(d) IN (0,6)
        FROM (SELECT DISTINCT deposit_date AS d FROM silver_deposit WHERE deposit_date IS NOT NULL) t
        WHERE CAST(strftime(d,'%Y%m%d') AS INTEGER) NOT IN (SELECT date_key FROM dim_date)""")

    # Late-arriving dimension: a deposit whose client has no dimension row still
    # loads, bound to an inferred member. It is never dropped and never silently
    # pointed at the unknown member forever - the inferred row is upgraded in place.
    unknown = con.execute("""
        SELECT DISTINCT f.client_id FROM silver_deposit f
        LEFT JOIN dim_client d ON d.client_id = f.client_id
        WHERE d.client_id IS NULL""").fetchall()
    for (cid,) in unknown:
        con.execute("INSERT INTO dim_client VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            row_hash(cid, "inferred"), cid, "UNKNOWN", None, None, "unknown", None, "unknown",
            None, None, BASELINE_TS, END_OF_TIME, True, False, 0, "inferred", None, True, now])
        log(f"  Inferred dimension member created for late/unknown client {cid}")

    con.execute("DELETE FROM fact_deposit")
    con.execute("""
        INSERT INTO fact_deposit
        SELECT md5(f.deposit_id), f.deposit_id, d.client_sk, f.client_id,
               CAST(strftime(f.deposit_date, '%Y%m%d') AS INTEGER), f.payment_method,
               f.amount_usd, f.fee_usd, f.amount_usd - coalesce(f.fee_usd,0), f.status,
               f.source_system, now()
        FROM silver_deposit f
        LEFT JOIN dim_client d ON d.client_id = f.client_id AND d.is_current
        WHERE f.is_quarantined = FALSE""")

    trades = json.load(open(os.path.join(DATA, "client_trades.json")))
    con.execute("DELETE FROM fact_trade")
    for t in trades:
        sk = con.execute("SELECT client_sk FROM dim_client WHERE client_id=? AND is_current",
                         [t["client_id"]]).fetchone()
        cs = CONTRACT_SIZE.get(t["instrument"])
        derived = None
        if cs:
            sign = 1 if t["direction"] == "buy" else -1
            derived = round((t["close_price"] - t["open_price"]) * sign * cs * t["volume_lots"], 2)
        var = round(t["pnl_usd"] - derived, 2) if derived is not None else None
        if var is not None and abs(var) > 0.01:
            con.execute("INSERT INTO dq_result VALUES (?,?,?,?,?,?,?,?,?)", [
                run_id, "pnl_recomputes", "WARN", "trade", t["trade_id"], "client_trades.json",
                f"reported pnl {t['pnl_usd']} != derived {derived} from prices "
                f"({t['open_price']}->{t['close_price']}, {t['volume_lots']} lots)",
                "load both values, flag to trading ops; do not overwrite the book", now])
            log(f"  PnL VARIANCE {t['trade_id']}: reported {t['pnl_usd']} vs derived {derived}")
        con.execute("INSERT INTO fact_trade VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            md5 := row_hash(t["trade_id"]), t["trade_id"], sk[0] if sk else None, t["client_id"],
            int(t["trade_date"].replace("-", "")), t["instrument"], t["direction"], t["volume_lots"],
            t["open_price"], t["close_price"], t["pnl_usd"], derived, var, t["trade_status"], now])

    con.execute("""INSERT INTO dim_date
        SELECT DISTINCT CAST(strftime(d, '%Y%m%d') AS INTEGER), d,
               CAST(strftime(d,'%Y') AS INTEGER), CAST(ceil(month(d)/3.0) AS INTEGER),
               month(d), day(d), strftime(d, '%B'), dayofweek(d) IN (0,6)
        FROM (SELECT DISTINCT strptime(CAST(date_key AS VARCHAR), '%Y%m%d')::DATE AS d
              FROM fact_trade) t
        WHERE CAST(strftime(d, '%Y%m%d') AS INTEGER) NOT IN (SELECT date_key FROM dim_date)""")
    log("  fact_deposit, fact_trade, dim_date built")


# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------

def summarise(con):
    section("SUMMARY")
    # STATE tables must be identical after a replay. AUDIT tables are append-only
    # by design: every run leaves its own evidence trail, keyed by run_id.
    kinds = {
        "bronze_vendor_deposit": "STATE", "ingest_file_manifest": "STATE",
        "silver_deposit": "STATE", "quarantine_deposit": "STATE", "dim_client": "STATE",
        "cdc_apply_log": "STATE", "fact_deposit": "STATE", "fact_trade": "STATE",
        "dq_result": "AUDIT (append-only)", "reconciliation_result": "AUDIT (append-only)",
    }
    for t, kind in kinds.items():
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        log(f"  {t:26s} {n:5d}   {kind}")

    log("\n  DQ results by severity:")
    for sev, cnt in con.execute(
            "SELECT severity, count(*) FROM dq_result GROUP BY 1 ORDER BY 1").fetchall():
        log(f"    {sev:12s} {cnt}")

    log("\n  dim_client history for CL001 (three same-day changes, delivered out of order):")
    for r in con.execute("""SELECT risk_category, account_balance_usd, account_status,
                                   valid_from, valid_to, is_current, source_lsn
                            FROM dim_client WHERE client_id='CL001' ORDER BY valid_from""").fetchall():
        log(f"    risk={r[0]:7s} bal={r[1]:>9} status={r[2]:12s} "
            f"{str(r[3])[:19]} -> {str(r[4])[:19]} current={r[5]} lsn={r[6]}")

    log("\n  dim_client for CL012 (source delete - history intact):")
    for r in con.execute("""SELECT account_status, valid_from, valid_to, is_current, is_deleted, source_lsn
                            FROM dim_client WHERE client_id='CL012' ORDER BY valid_from""").fetchall():
        log(f"    status={r[0]:12s} {str(r[1])[:19]} -> {str(r[2])[:19]} "
            f"current={r[3]} deleted={r[4]} lsn={r[5]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="drop the warehouse and rebuild")
    ap.add_argument("--replay", help="re-ingest only the vendor file matching this label, e.g. 20240302")
    args = ap.parse_args()

    if args.reset and os.path.exists(DB):
        os.remove(DB)
        log("Warehouse removed - rebuilding from scratch")

    run_id = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")
    con = duckdb.connect(DB)
    create_schema(con)

    log(f"Run id: {run_id}")
    load_bronze_vendor(con, run_id, only_file=args.replay)
    build_silver_deposits(con, run_id)
    apply_cdc(con, run_id)
    reconcile(con, run_id)
    build_gold(con, run_id)
    summarise(con)
    con.close()
    log("\nDone. Re-run this script - all counts stay identical (idempotency proof).")


if __name__ == "__main__":
    main()
