# ATP Bitemporal Match Warehouse — Project Spec

N.B. This is an AI generated document, defining the spec of things I should be aiming to achieve as part of this project. Note I will stay as close to this spec as possible, but I will always endeavour to make the strongest justified engineering decision which may come to light when building the project.

**Status:** Draft v0.1
**Owner:** Eamon
**Last updated:** 2026-08-22

---

## 1. Why this exists

### 1.1 The actual goal

This project exists to close two specific gaps I don't get to touch day-to-day:

1. **Tooling I don't touch at work.** Orchestration (Airflow), infrastructure-as-code
   (Terraform), CI for data, and API ingestion with real constraints.
2. **Something I can discuss without NDA hedging.** One system I can whiteboard end to end,
   including the parts I got wrong.

### 1.2 What "done" looks like

Done is **not** "all features built". Done is:

- A running system that ingests daily without manual intervention for 30 consecutive days.
- A README that a stranger could follow to stand it up.
- A five-minute demo that shows a data correction landing and being handled correctly.
- Honest documentation of what was cut and why.

### 1.3 Explicit non-goals

- Not a dashboard project. Visualisation is the last 5%, if at all.
- Not a prediction model. ML is a different interview.
- Not commercial. Source licences forbid it (see §3.3).
- Not "complete coverage" of tennis. Scope is ATP singles, tour level.

---

## 2. Success criteria

The project is worth the evenings if I can answer these in an interview without hesitating:

| # | Question | Where it's answered |
|---|---|---|
| 1 | What's the grain of this table? | §6.2 |
| 2 | What happens if the job runs twice? | §7.2 |
| 3 | How do you backfill without duplicating? | §7.4 |
| 4 | A source corrects a record from six months ago. What happens? | §6.4 |
| 5 | How do you stop bad data reaching consumers? | §9.2 |
| 6 | How do you know the pipeline ran correctly? | §9.3 |
| 7 | Why did you partition it that way? | §6.6 |
| 8 | How do you join two systems with no shared key? | §6.5 |

If a feature doesn't help answer one of the above, it's decoration. Cut it.

---

## 3. Data sources

### 3.1 TML-Database (primary, historical)

**Revised 2026-08-24** — the original GitHub transport went stale (see below); this
section now reflects the live source.

- **What:** ATP match results, player biographical data, rankings. Keyed on official ATP
  player IDs.
- **Transport:** JSON API at `https://stats.tennismylife.org/api/data-files`, returning
  metadata for every file the source publishes: `{count, files: [{name, url, size,
  mtime}]}`. Supersedes the original GitHub repo (`Tennismylife/TML-Database`), which is
  no longer being updated — the maintainer moved live delivery to their own site
  (`stats.tennismylife.org`). Tracked file for Phase 1: `2026.csv` (current year, ATP
  tour-level singles) — matches the `mtime` in the API response against the date `2026.csv`
  was last downloaded.
- **Cadence:** Updated daily or more frequently, following live results. Confirmed live —
  `2026.csv`'s `mtime` matched the day it was checked.
- **Why it matters here:** It **rewrites history**. The ATP issues corrections and
  retro-additions, and TML follows them. This is what makes the bitemporal model necessary
  rather than academic.
- **Change detection:** Compare the tracked file's `mtime` (from the API response) against
  the last-seen value before downloading anything. Do not re-download 80MB daily to find
  three changed rows. One API call returns both the metadata needed for this check and the
  `url` to download from — no separate "check" call needed, unlike the old SHA-check flow.
- **Also newly available from this source, explicitly out of scope for Phase 1:** WTA Tour
  files (new, self-described by the maintainer as not yet as reliable as ATP), ATP
  Challenger Tour, qualifying-round files, and `ongoing_tourneys.csv` (updates near-real-time
  during live tournaments — worth revisiting when planning §3.2/Phase 3, since it may
  partially cover the "live state" need already).

### 3.2 Live tennis API (secondary, current state)

- **What:** Fixtures, in-flight scores, completed results.
- **Constraint:** Free tier is **100 requests/day, 30/minute**. This is a hard cap and is
  the most interesting engineering constraint in the project.
- **Why it matters here:** Records that **change state** within a day
  (scheduled → live → completed → corrected). Forces genuine idempotent merge.
- **Risk:** Free tier terms could change or the service could disappear. The system must
  degrade gracefully to TML-only. See §13.

### 3.3 Licence and attribution

**Revised 2026-08-24:** the source moved from the GitHub repo (Jeff Sackmann-derived data
under CC BY-NC-SA 4.0) to `stats.tennismylife.org`, which declares its own licence in page
metadata as **MIT License** — a real change, and one to treat with some caution rather than
take at face value: MIT is a software licence, unusual to see applied to a dataset, so it's
unclear whether it's meant to cover the data itself or just the maintainer's own tooling.

Practical consequences, kept conservative until this is clearer:

- Attribution block in the repo README and in any public write-up — kept regardless of
  which licence turns out to actually apply. Costs nothing, removes the ambiguity as a risk.
- No monetisation, ever, including "free tier with paid upgrade" — unaffected either way,
  since §1.2 already rules this out as a project goal.
- If derived data is published, treat it as if share-alike still applies, pending
  confirmation — i.e. don't assume MIT's permissiveness extends to redistribution terms.
- Public GitHub repo is fine. A hosted public app is a grey area — keep it private or
  demo from a screen recording.

### 3.4 Known data quirks (design around these, don't discover them later)

- `tourney_date` is `YYYYMMDD` as an integer, not a date. Autodetect will type it INT64.
- Match stats only exist **1991-present** for tour level, **2008-present** for challengers.
  Everything earlier is winner/loser/score only.
- Davis Cup matches sit in the tour-level files but mostly lack stats.
- Some matches have stats deliberately deleted upstream for failing sanity checks — so a
  null is not always "missing", sometimes it's "rejected".
- Doubles files have the same columns in a **different order**.
- Seed and entry columns are sparse; schema autodetect types them inconsistently depending
  on which rows it samples.

---

## 4. Architecture

### 4.1 Layers

```
Sources          TML CSVs (GitHub)          Live API (rate-limited)
                        |                            |
                        v                            v
Landing          GCS — immutable, hive-partitioned, batch-tagged
                        |
                        v
Raw              BigQuery — all STRING, append-only, partitioned on ingest date
                        |
                        v
Snapshots        dbt snapshots — captures transaction time (when we learned it)
                        |
                        v
Staging          dbt — cast, rename, dedupe. One model per source entity.
                        |
                        v
Intermediate     dbt — entity resolution, bitemporal assembly
                        |
                        v
Marts            dbt — dim_player, dim_ranking, fct_match_version
```

Orchestrated by Airflow. **Airflow orchestrates, BigQuery computes.** No task should do
arithmetic on the worker. If pandas appears in a DAG, the design is wrong.

### 4.2 Layer contracts

| Layer | May do | May not do |
|---|---|---|
| Landing | Write bytes as received, add manifest | Parse, filter, reshape |
| Raw | Load, add `_meta` columns | Cast types, deduplicate, join |
| Snapshots | Detect change, version rows | Business logic |
| Staging | Cast, rename, dedupe within source | Join across sources |
| Intermediate | Join, resolve entities, assemble history | Aggregate for presentation |
| Marts | Aggregate, expose | Reference raw directly |

These rules exist so that "where does this logic go" is never a debate at 11pm.

### 4.3 Why immutable landing

Storage is pennies. Losing history is unrecoverable. When I discover in three months that
a field has been parsed wrong the whole time, I reparse from landing rather than losing the
data permanently. Re-running a batch writes a **new** `batch_id`; the old one stays.

---

## 5. Infrastructure

### 5.1 Compute

- **Airflow:** Docker Compose on a GCE VM. LocalExecutor. Postgres as a sibling container,
  volume on a separate persistent disk so a VM rebuild doesn't destroy DAG history.
- **Sizing:** Start `e2-small` (2 vCPU / 2GB) with `parallelism` and `max_active_tasks`
  turned down. Move to `e2-medium` if the scheduler OOMs. Expect £12–25/month.
- **Why not Composer:** ~£250/month for a hobby project is indefensible.
- **Why not Cloud Workflows:** "Airflow" is the word on job specs. That is a real reason.

### 5.2 Storage and compute costs

| Service | Expected | Notes |
|---|---|---|
| GCS | < £1/month | Dataset is a few hundred MB |
| BigQuery storage | < £1/month | Well inside free tier |
| BigQuery query | ~£0 | Inside 1TB/month free tier |
| GCE VM | £12–25/month | The only real cost |

**Honesty note for the write-up:** partitioning and clustering will not save meaningful
money at this data volume. Do not claim cost savings that didn't happen. Frame it as
"designed for this query pattern" and be able to explain what would change at 1000x.

### 5.3 Guardrails

- `maximum_bytes_billed` set on every query.
- Billing budget alert at £40/month.
- Separate `dev` and `prod` BigQuery datasets, separate service accounts.

---

## 6. Data model

### 6.1 The central idea

Two independent time axes, kept distinct everywhere:

| Axis | Meaning | Source |
|---|---|---|
| **Valid time** (event time) | when the fact was true in the world | `tourney_date`, ranking week |
| **Transaction time** (system time) | when we learned it | `_ingested_at` |

The system must separately answer:

- *"What do we know now about this match?"*
- *"What did we believe about this match on 15 March?"*

This is the hook. It's the exact class of bug that bites in telco and finance, it's hard to
get right, and almost no portfolio project does it.

### 6.2 Grain declarations

Every model declares its grain in the model description **and** enforces it with a
uniqueness test. Non-negotiable.

| Model | Grain |
|---|---|
| `stg_tml__matches` | one row per (tourney_id, match_num) per batch |
| `stg_api__matches` | one row per api_match_id per poll |
| `dim_player` | one row per player_sk (SCD2: player + valid range) |
| `dim_ranking` | one row per (player_sk, ranking_week) |
| `fct_match_version` | one row per (match_sk, known_from) |

### 6.3 Keys

- Surrogate keys as hashes of the natural key, generated in staging.
- Natural key for a match: `(tourney_id, match_num)` from TML.
- The API does **not** expose either. See §6.5.

### 6.4 Handling corrections

Transaction time comes free from dbt snapshots using the `check` strategy on a hash of the
payload — `dbt_valid_from` / `dbt_valid_to`. Valid time is modelled by hand.

The query that justifies the whole project:

```sql
-- what did we believe about this match on 15 March?
select *
from fct_match_version
where match_sk = @match_sk
  and known_from <= '2026-03-15'
  and known_to   >  '2026-03-15'
```

**Demo script:** pick one match. Show the record as scheduled, as live, as completed, and
after a correction lands months later. Show that no history was destroyed.

### 6.5 Entity resolution

The two sources share no keys and spell names differently. This is a genuine problem, not a
textbook exercise.

Approach:
1. Candidate match on `(tourney_date ± 1 day, unordered player pair, round)`.
2. Fuzzy name comparison on normalised surnames (strip diacritics, handle
   `Last F.` vs `First Last`).
3. Confidence score persisted, not discarded.
4. **Manual override table**, version controlled, for the ones that don't resolve.
5. Unresolved matches are surfaced in a report, not silently dropped.

A documented, testable override mechanism is more impressive than pretending the fuzzy match
works. Report the match rate honestly.

### 6.6 Partitioning and clustering

- Raw: partition on `_ingested_at` (DATE), cluster on source entity.
- Marts: partition on `tourney_date`, cluster on `player_sk`.
- Rationale: the dominant query pattern is "one player, over time" and "one tournament".

---

## 7. Ingestion contracts

### 7.1 Landing layout

```
gs://<bucket>/raw/{source}/{entity}/ingest_date=YYYY-MM-DD/batch_id=<uuid>/part-000.json.gz
```

Every batch writes a `_manifest.json`:

```json
{
  "batch_id": "uuid",
  "source": "tml",
  "entity": "matches",
  "extraction_window": {"from": "...", "to": "..."},
  "row_count": 1234,
  "source_version": "<source mtime timestamp, or git sha/etag for other sources>",
  "ingested_at": "2026-08-22T04:00:00Z"
}
```

The manifest is what makes replay possible. It is the thing that separates this from a
tutorial.

### 7.2 Idempotency

Pick per table, document the choice:

- **Delete-insert by batch:** `DELETE WHERE _batch_id = @batch_id` then insert. Simple,
  obviously correct, preferred for raw.
- **MERGE on natural key:** dbt incremental with `unique_key` + `merge` strategy. Used in
  marts.

Test: run any DAG twice with the same parameters. Row counts and content must be identical.
This should be an actual automated test, not a claim.

### 7.3 Watermarks

Table `_meta.ingest_watermarks`:

| Column | Purpose |
|---|---|
| source, entity | key |
| last_successful_watermark | how far we've got |
| last_batch_id | for replay |
| updated_at | audit |

Advance **only after** the load verifies its row count against the manifest. If a run dies
between landing and advancing, the next run re-lands the same window — harmless, because
loads are idempotent.

**Do not** derive the watermark from `max(date)` in the target. That silently skips data
whenever a partial load succeeds. This is the specific trap the question in §2 is probing.

### 7.4 Backfill

Backfill uses the **same task code** as the daily run, parameterised by date range. If
backfill has its own script, it will drift and be wrong when I need it.

### 7.5 Schema drift

New column upstream must not break the load. Because raw is all-STRING with
`ALLOW_FIELD_ADDITION`, new columns land silently and surface as a test warning rather than
a 3am failure.

### 7.6 API budget management

100/day is the constraint that makes this interesting.

**Priority order:**
1. Matches currently in flight
2. Today's fixtures
3. Reconciliation of yesterday's results

**Mechanism:**
- Budget state in a BigQuery table, decremented per call.
- Airflow Pool sized 1 for API tasks; token bucket for the 30/min ceiling.
- On exhaustion, raise `AirflowSkipException` — **not** a failure. Running out of quota is
  expected behaviour, not an incident. Don't page yourself at 11pm for a working system.
- Exponential backoff on 429 and 5xx, with a cap.

---

## 8. Orchestration

### 8.1 DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `tml_daily` | daily 04:00 | SHA check → extract changed → land → load raw |
| `api_poll` | hourly, budget-aware | Priority-driven extraction within quota |
| `dbt_build` | after both | snapshots → run → test |
| `backfill_tml` | manual, parameterised | Same task code, explicit date range |

### 8.2 Task design rules

- Every task idempotent — a retry is always safe.
- Real dependencies, not a cron of scripts hoping the previous one finished.
- Checkpointing so a run that dies halfway resumes rather than restarts.
- Retries with exponential backoff.

### 8.3 Alerting

Distinguish two failure classes, because they need different responses:

- **Source unavailable** — retry, alert only after N consecutive failures.
- **Source returned garbage** — alert immediately, halt downstream.

---

## 9. Data quality

### 9.1 Test severity policy

`severity: error` is reserved for tests where bad data is worse than no data:

- Grain uniqueness on every model.
- Referential integrity: every fact row resolves to a dim.
- The 1991 stats boundary: stats present where expected, absent where expected.
- Not-null on keys.

`severity: warn` for everything else. If everything is an error, nothing is.

### 9.2 Circuit breaker

`dbt build` provides this natively — a failing test on a staging model blocks every
downstream model. Marts are only rebuilt if tests pass. The discipline is the severity
policy above, not the mechanism.

### 9.3 Observability

- Run metadata logged to a queryable table: DAG, task, batch_id, row counts, duration.
- Freshness checks: alert if a source hasn't updated in N days.
- Row-count anomaly detection: today's volume vs trailing 7-day average.
- **The point:** be able to answer "why did this number move" from data, not from memory.

---

## 10. CI/CD

- GitHub Actions on every PR: create `dbt_ci_pr<N>` dataset, `dbt build --target ci`, drop
  on merge.
- `sqlfluff` lint.
- Auth via **Workload Identity Federation**, not a JSON key in GitHub secrets. This detail
  gets noticed.
- Terraform plan on PR, apply on merge to main.

---

## 11. Security

- Least-privilege service accounts, one per function. No `Owner`.
- API keys in Secret Manager, never in env files or code.
- Separate dev/prod projects or at minimum separate datasets and SAs.
- No secrets in Airflow Variables — use the Secret Manager backend.

---

## 12. Delivery phases

**The failure mode is building everything before shipping anything.** Each phase must be
independently demonstrable.

### Phase 1 — Walking skeleton
TML source only. Land → raw → three dbt models → one test. Manual trigger. Terraform for
buckets and datasets.
*Done when:* `terraform apply` plus one command produces a queryable table.

### Phase 2 — Orchestrated and tested
Airflow on the VM. `tml_daily` running on schedule. Watermarks. Idempotency test. CI on PR.
*Done when:* it runs for 7 days unattended.

### Phase 3 — Second source
API ingestion with budget management. Entity resolution and the crosswalk. Match-rate report.
*Done when:* both sources join and the unresolved rate is measured and documented.

### Phase 4 — Bitemporal
Snapshots, SCD2 dims, `fct_match_version`. The as-of query. The demo script.
*Done when:* the correction demo works on a real correction.

### Phase 5 — Polish
Observability, alerting, README, write-up.

**If time runs out, stop after Phase 4 and write it up properly.** Half the list done
properly beats all of it done shallowly.

---

## 13. Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| API free tier changes or disappears | Medium | Degrade to TML-only; don't hard-couple |
| TML repo goes dark (as the original Sackmann repo did) | **Materialised 2026-08-24** | GitHub repo went stale; maintainer moved live delivery to `stats.tennismylife.org` with a proper JSON API. Landing layer's immutable archive did its job — nothing was lost, just repointed the transport (§3.1) |
| Entity resolution match rate too low to be useful | Medium | Manual override table; report honestly rather than hide it |
| Scope creep into ML or dashboards | **High** | §1.3 non-goals; re-read before starting anything new |
| Loses to work/life priorities | High | Phases are independently shippable for this reason |

---

## 14. Decision log

Append here. Every non-obvious choice gets a line, because in an interview "why" matters
more than "what".

| Date | Decision | Rationale | Alternatives rejected |
|---|---|---|---|
| 2026-08-22 | Airflow in Docker on GCE | Airflow is the job-spec keyword; Composer is £250/mo | Composer, Cloud Workflows, Dagster |
| 2026-08-22 | All-STRING raw layer | Schema surprises in 1974 shouldn't fail a load | Typed load with explicit schema |
| 2026-08-22 | Bitemporal as the hook | Differentiates from tutorial projects; genuinely hard | Standard SCD2 only |
| 2026-08-24 | Switched TML transport from GitHub repo to `stats.tennismylife.org` JSON API | Original repo stopped updating; new source has a documented API with per-file `mtime`, arguably cleaner than the SHA-check it replaces | Scraping the GitHub repo's stale copy anyway (rejected — not live); scraping the website's HTML (rejected — the API makes this unnecessary) |
| | | | |

---

## 15. Glossary

- **Bitemporal** — modelling both when a fact was true and when it was recorded.
- **Circuit breaker** — failing tests halt the pipeline rather than logging a warning.
- **Grain** — what one row of a table represents. The most commonly failed interview question.
- **Idempotent** — running twice produces the same result as running once.
- **Late-arriving data** — a record about the past that shows up now.
- **SCD Type 2** — slowly changing dimension keeping full history via valid-from/valid-to.
- **Watermark** — durable marker of how far ingestion has progressed.
- **Walking skeleton** — thinnest possible end-to-end slice, built first.

---

## 16. Open questions

- [ ] VM: does `e2-small` actually hold Airflow, or straight to `e2-medium`?
- [ ] dbt Core on the Airflow VM, or a separate Cloud Run job?
- [ ] Snapshot cadence — daily is probably right, but does the API need finer?
- [ ] How much of the intermediate layer is genuinely needed vs staging → marts?
- [ ] Public repo (better for portfolio) vs private (safer on licence)?
