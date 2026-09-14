# PROMPTS.md

AI assistance used on this submission, grouped by part. Each entry records the prompt, what
came back, and what I changed, kept or rejected.

I used Claude (Opus) as the primary assistant, plus targeted web searches to verify
Databricks API names that have changed recently. The general pattern: AI was useful for
generating structure and boilerplate quickly, and consistently weakest at *reading the
actual data* — most of the corrections below are cases where it produced a textbook answer
that the delivered files contradict.

---

## Setup — data profiling

**Prompt:**
> "Here is an assessment brief with eight data files embedded in markdown code blocks.
> Extract each file to disk, then write a profiling script that finds every data quality
> anomaly: orphan foreign keys, duplicate ids across files, schema differences between
> files, dates that violate expected ordering, impossible values, and internal arithmetic
> inconsistencies. Report exact record ids, not counts."

**Outcome — kept.** This was the highest-value prompt of the exercise. Rather than reason
about the data by eye, I extracted it and profiled it programmatically. That is how I found
the anomalies I would otherwise have missed: the `credit_card` malformed key on `DEP012`,
the 14 deposits predating signup, and the LSN gaps.

**What I corrected:** the first profiling pass reported `TRD016` as a PnL error because it
assumed a gold contract size of 100 oz/lot. I had it *infer* the contract size from the
trades that are internally consistent instead of assuming an industry standard. That
changed the answer: gold in this dataset uses a factor of 10, `TRD016` is correct, and
**`TRD012` is the only genuine PnL defect** (open price = close price = 2320.00, yet
245.00 reported). The assumed-constant version would have produced a false positive and a
missed true positive.

---

## Part 1a — Pipeline design

**Prompt:**
> "Design a medallion architecture on Databricks for ingesting a daily vendor CSV deposit
> feed and a CDC JSONL change log into a trading warehouse. Cover idempotency, late-arriving
> data, and source deletes. Be specific about the mechanism for each."

**What came back:** a competent generic medallion answer — bronze/silver/gold, Auto Loader,
merge on primary key, watermarking on a timestamp column.

**What I changed:**

1. **Rejected timestamp watermarking for the CDC path.** The suggestion was to watermark on
   `commit_ts`. That is wrong for this data: `CL001`'s `lsn` 1004, 1005 and 1006 all commit
   on 2024-11-15 and arrive out of order, so a `commit_ts` watermark cannot order them and
   risks skipping events committed in the same second. I replaced it with an LSN high-water
   mark in `cdc_apply_log`.

2. **Rejected filename-based file tracking.** The suggested manifest keyed on filename. The
   vendor reuses filenames for corrections, so I keyed on a SHA-256 content hash and added
   explicit handling for "same name, different hash" as a correction rather than a duplicate.

3. **Added the event-time vs label-time distinction**, which the AI did not raise at all. It
   only surfaced because profiling showed `deposits_vendor_20240303.csv` contains events 4
   days older than its filename. I asked a follow-up specifically about it:

   > "A vendor file is named with 2024-03-03 but every row inside it is dated 24–28
   > February. What breaks in a pipeline that derives its processing window from the
   > filename, and how should the window be derived instead?"

   That produced the partition-restatement approach I kept.

**Prompt (data quality):**
> "Propose a data quality rule set for this deposit feed with severity levels. For each
> rule give the specific on-failure action."

**What I rejected:** the first version made `deposit_date >= signup_date` a hard failure
that quarantines the row. Running it against the data shows it fires on **14 of 24 vendor
rows**. A rule that fails 58% of a feed is describing the business, not catching a defect —
and quarantining 14 rows would destroy the feed and train the team to ignore the queue. I
downgraded it to WARN with an escalation path to the vendor. This is in
`part1_pipeline.md` as an explicit judgement call, because I expect to be challenged on it.

I also downgraded the negative-amount rule from BLOCK to QUARANTINE: `VDEP001` at -250.00
is most likely a refund, and halting 23 good rows over it is the wrong trade.

---

## Part 1b — Reconciliation

**Prompt:**
> "Write a reconciliation between a vendor deposit feed and a warehouse deposit table.
> Match on deposit_id, classify breaks, and report control totals."

**What came back:** a single-tier `FULL OUTER JOIN` on `deposit_id`.

**What I changed — the most important correction in the submission.** Running that against
the data returns a **100% break rate**, because vendor ids are `VDEP001–022` and warehouse
ids are `DEP001–020` with zero overlap. Shipped as-is, that report says "every single row
failed reconciliation" on day one, which is both alarming and meaningless.

I added a second matching tier on the composite business key
(`client_id + deposit_date + amount_usd`) to test whether the feeds overlap *economically*
even though the identifiers differ. They do not — Tier 2 also returns zero. That is what
establishes the actual finding: **this feed is net-new deposit traffic, not a mirror of
`client_deposit`**, so reconciliation is a completeness and control-total check rather than
a row-for-row tie-out.

The AI's answer was not wrong as SQL. It was wrong as an *interpretation*, and the two-tier
design is what makes the distinction visible.

---

## Part 2a — Dimensional model

**Prompt:**
> "For a trading warehouse with client signup, profile, deposit and trade tables, compare
> Kimball star schema against Data Vault. Which fits this dataset and why? State the grain
> of every fact table."

**What came back:** a balanced comparison recommending Kimball. I agreed with the
conclusion but not the reasoning, which was generic ("Kimball is simpler, Data Vault is for
complex environments").

**What I changed:** I rewrote the justification around properties of *this* dataset — the
source topology is already star-shaped, there is one business key (`client_id`), and there
are only two source systems. I also added the condition that would flip the decision
(multiple processors requiring client mastering), because "why not the alternative" is the
part that gets challenged in a walkthrough.

**What I added that the AI missed:** the `account_balance_usd` currency trap. 13 of 30
clients hold a non-USD `currency` while the balance column is named `_usd`. The AI modelled
it as a plain additive measure. Summing it today produces a meaningless number that looks
plausible, so I split it into `account_balance_original` + `currency` + `fx_rate_to_usd`
with a derived USD figure.

**Prompt (late-arriving dimensions):**
> "How should a Kimball star handle a fact arriving before its dimension row? Compare
> dropping the fact, nulling the FK, and inferred members."

**Outcome — kept and extended.** The inferred-member recommendation was right. I added the
7-day ageing alert, because a stub mechanism with no monitoring silently hides a broken
dimension feed — the AI presented inferred members as a complete solution.

**What running it then corrected in my own writing.** I had documented the inferred member
as the handling for `CL099` and `CL031`, and quoted a log line asserting one was created.
The prototype creates **zero** — both orphans are quarantined at the Silver DQ gate and
never reach Gold. The mechanism is right and the precedence is right; the claim was simply
not what the code does. It is now documented as the second layer, with the zero count
stated explicitly, which is a stronger answer than the one I had written from the design
rather than from the run.

---

## Part 2b — Historization / SCD

**Prompt:**
> "How do I write a MERGE statement in Databricks to implement SCD Type 2 for a client
> dimension fed by a CDC log with insert, update and delete events?"

**What came back:** a standard two-phase merge (close current row, insert new version) and
`APPLY CHANGES INTO` as the managed alternative.

**Two corrections:**

1. **Partial after-images.** The generated `INSERT` took all columns from the `after` image.
   In this feed `after` carries only `risk_category`, `account_balance_usd` and
   `account_status` — so that merge nulls `full_name`, `nationality` and
   `preferred_language` on every single update. I added `COALESCE` against the prior
   version. This is the kind of bug that passes review and silently destroys a dimension.

2. **API name is out of date.** I verified against current Databricks docs rather than
   trusting the model: `APPLY CHANGES` has been superseded by the **AUTO CDC** APIs under
   Lakeflow Declarative Pipelines. `APPLY CHANGES` still works but is no longer the
   recommendation. The training data was stale, which is exactly why I searched.

**Where I disagreed with the AI outright:**

> Prompt: "Should all three CDC attributes — risk_category, account_balance_usd,
> account_status — be tracked as SCD Type 2?"

It said yes to all three. I do not agree, and `part2_data_model.md` argues the case:
`account_balance_usd` changes on every deposit, withdrawal and closed trade, so tracking it
in the same SCD2 row means a new dimension version per balance movement. At scale the
dimension stops being slowly changing, and the risk history gets polluted with versions
that carry no risk information. This dataset already demonstrates it — `lsn 1005` for
`CL001` is a pure balance move with `risk_category` and `account_status` unchanged.

I split it: Type 2 for `risk_category` and `account_status`, Type 4 (daily snapshot) for
the balance. I have also documented honestly that the prototype still hashes all three,
because at 30 clients the split is not yet worth the extra object — and noted the exact
one-line change that implements it.

**Prompt (backfill):**
> "How do I reload one month of data into an SCD2 dimension without corrupting the history
> that already exists?"

**What I corrected:** the suggested predicate was
`DELETE WHERE valid_from BETWEEN start AND end`. That misses versions which opened *before*
November and are still open *during* it. The correct predicate is interval overlap:
`valid_from < '2024-12-01' AND valid_to >= '2024-11-01'`. I also added the Delta `RESTORE`
snapshot step and the post-backfill invariant assertions, neither of which was suggested.

---

## Part 3 — Architecture and build vs buy

**Prompt:**
> "Design an architecture serving both a sub-second fraud detection signal on deposit events
> and a weekly batch analytics report, with an external partner API consuming the data.
> Address how the paths avoid blocking each other and where eventual consistency is
> acceptable."

**What came back:** a reasonable Lambda architecture with Kafka, Structured Streaming and
Delta.

**What I changed:** the answer treated "Lambda vs Kappa" as the interesting question. I
reframed around the failure mode that actually matters — logic drift between the streaming
and batch implementations of the same rule — and specified a shared, versioned scoring
library imported by both paths. I also made the consistency decision explicit *per consumer*
rather than as a single global stance, and stated plainly where I refuse eventual
consistency (anything financial reported to the board).

**Added:** serving the C-suite report from a *tagged* Delta version, so a Tuesday backfill
cannot silently change a number quoted in Monday's board meeting. The AI's design would
have served it from the live table.

**Prompt (build vs buy):**
> "Give me criteria for deciding between building a custom connector and buying an
> integration platform for onboarding a new payment processor."

**What I changed:** the generic answer weighed licence cost against build cost. I grounded
the recommendation in what this assessment's own data shows: the hard problems here are the
backdated file, the renamed column, the duplicate rows and the id-namespace mismatch — and
**no integration platform solves any of them**. That is what drives the split recommendation
(buy the transport, build the transformation) rather than a straight build-or-buy verdict.

---

## Orchestration — Lakeflow jobs and the streaming trigger

**Prompt:**
> "Write a Databricks job definition that runs an Auto Loader streaming notebook
> continuously and then runs the Silver, Gold and reconciliation notebooks after it."

**What came back:** a single job with the streaming notebook as task one and the batch
notebooks chained behind it with `depends_on`.

**Rejected — it cannot work.** A continuous task never reaches a terminal state, so the
dependent tasks would never start. The batch half of that DAG would sit pending forever
while looking, on the job page, like a healthy run. I split it into two definitions: a
single-task continuous job for Bronze ingestion, and a file-arrival job for
`01 → 03 → 04 → 05 → 06`.

**What I added that the prompt did not ask for:**

1. **A `trigger_mode` widget on the streaming notebook.** The same notebook has to drain
   and stop when run as a task, and run forever when run as the continuous job. Editing the
   trigger by hand between the two is how a debugging change gets committed by accident, so
   the job supplies the mode as a parameter.
2. **`awaitAnyTermination()` instead of awaiting each query.** Awaiting the deposit query
   at the point it is started deadlocks in continuous mode — the call never returns, so the
   CDC query never starts and that feed silently ingests nothing. `awaitAnyTermination`
   also fails the run when *either* query dies, rather than letting a healthy stream mask
   a dead one.
3. **Archive path outside the trigger's watched path.** The batch job triggers on file
   arrival in the landing volume and notebook `01` drains consumed files. Archiving inside
   the watched path would make the job re-trigger itself indefinitely.
4. **Per-task retry policy, not a uniform one.** Reconciliation is `max_retries: 0`,
   because it appends evidence keyed by `run_id` — a retry leaves two sets of break rows
   for one logical run and makes the audit trail lie. The AI's default gave every task the
   same retry count.

Both jobs ship `PAUSED`. A job definition in a repo should not start moving data because
someone imported it.

---

## Prototype

**Prompt:**
> "Write a runnable Python/DuckDB prototype implementing this pipeline end to end: file
> manifest, DQ gate with severity, dedupe, merge, SCD2 from CDC ordered by LSN with soft
> deletes, two-tier reconciliation, and a star schema. It must be idempotent — running it
> twice must leave every state table unchanged."

**Outcome — kept, after three rounds of debugging.** Building it was not decoration; it is
what let me verify claims instead of asserting them. Concretely, it caught:

- an ordering bug in my own first CDC implementation, where the `valid_to` of one version
  did not equal the `valid_from` of the next for `CL001`;
- a date-parsing failure when building `dim_date` from trade dates;
- the fact that my reconciliation returned zero matches — which I initially assumed was a
  bug in my join before confirming the id namespaces genuinely do not overlap.

Every number quoted in the three documents is copied from this prototype's output. Verified
idempotency: `python3 run_pipeline.py` twice leaves all eight STATE tables byte-identical.
