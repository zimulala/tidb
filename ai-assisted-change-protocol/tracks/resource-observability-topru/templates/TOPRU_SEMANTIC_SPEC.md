# TOPRU_SEMANTIC_SPEC.md

> Purpose: nail down TopRU semantics to avoid drift between design/implementation/review.
> This is a track-level spec; core semantic decisions must also be recorded in templates/SEMANTIC_SPEC.md.
> All S# below are Semantic GO Gate items: no GO ⇒ STOP.

## Semantic decisions (S1–S6) — MUST be explicit GO

### S1 — TopRU mode: finish-only vs in-flight sampling
- Maintainer GO:
  - [ ] Approved (name/date)
- Options:
  - Option A: finish-only
    - Meaning: RU is only attributed at execution finish.
    - Limitations: long-running statements contribute 0 until finish; weaker near-real-time triage.
  - Option B: in-flight 1s delta sampling
    - Meaning: sample RUDetails periodically during execution; accumulate deltaRU across ticks + finish.
    - Required: rules to avoid double count, handle nil/reset, and handle abnormal exit.
- Chosen (owner preference): Option B

### S2 — ExecCount semantics and coupling to RU/Duration
- Maintainer GO:
  - [ ] Approved (name/date)
- Options:
  - Option A: begin-based (align with TopSQL)
    - Meaning: exec_count counts starts; not completions.
    - Implication: within a report window, it is possible to have TotalRU/Duration > 0 while exec_count == 0.
  - Option B: finish-based (couple to RU/Duration)
    - Meaning: exec_count increments when RU/Duration increments are finalized.
    - Implication: easier user interpretation but may diverge from TopSQL.
- RU=0 counting:
  - Option A: do not count when deltaRU == 0
  - Option B: count anyway (must justify as signal vs noise)
- Chosen (owner preference): Option A (begin-based, TopSQL-consistent)

### S3 — Enable/Disable behavior (including drain+drop)
- Maintainer GO:
  - [ ] Approved (name/date)
- Options:
  - Option A: disabled means no collection + no reporting; state is cleared/bypassed (no drop accounting)
  - Option B: disabled allows drain+drop; MUST have user-visible signals (metrics/logs) for drops
- Chosen (owner preference): Option A

### S4 — Backpressure / drop policy
- Maintainer GO:
  - [ ] Approved (name/date)
- Options:
  - Option A: deterministic policy where feasible
    - Example: TopN + others semantics; keep-largest RU; explicit caps; predictable merges.
    - Unavoidable drops (e.g. channel full) must have signals and be documented.
  - Option B: allow random drops
    - Must be explicitly accepted and MUST be observable.
- Chosen (owner preference): Option A

### S5 — Report interval coupling (TopRU affects TopSQL cadence)
- Maintainer GO:
  - [ ] Approved (name/date)
- Options:
  - Option A: allow coupling
    - Example: effective interval = min(TopSQL, TopRU)
    - User-facing: enabling TopRU may increase TopSQL reporting frequency.
  - Option B: fully separated
    - Independent tickers/pipelines; merging happens at send/bundle layer.
- Chosen (owner preference): Option A

### S6 — Metadata availability (SQL/Plan meta when TopSQL is disabled)
- Maintainer GO:
  - [ ] Approved (name/date)
- Options:
  - Option A: TopProfilingEnabled (TopSQL OR TopRU) ⇒ best-effort register SQL/Plan meta
    - Fallback allowed: SQLMeta required; PlanMeta best-effort/limited with signals.
  - Option B: TopRU uses digests only; meta is optional
- Chosen (owner preference): Option A

---

## Detailed semantics (derived from S1–S6)

## 1) Correlation semantics (keys + time_window)
- Required correlation keys:
  - sql_digest + time_window (+ plan_digest if available)
- time_window definition:
  - source of time (event time / observation time):
  - rounding/alignment rule:
  - drift/jitter handling:
- plan_digest availability:
  - when can plan_digest be missing?
  - behavior if missing (drop/degrade/mark unknown) (must match S6):

## 2) Sampling semantics
- Data source for RU (what structure / where it is read):
- Delta definition:
  - deltaRU = current - last
  - invalid samples handling (nil / <=0 / reset):
- Cadence:
  - sampling interval (e.g. 1s):
  - finish-path behavior (final delta on execution finish?):
  - micro-batch merge interval (if any):
- Window alignment:
  - report interval options:
  - alignment rule (wall-clock aligned? sliding?):

## 3) exec_count semantics
- Definition (must match S2):
- How users should interpret exec_count vs RU/Duration:
- User-visible signals when semantics are partial/limited:

## 4) Interval semantics (sampling vs reporting)
- sampling interval:
- report interval:
- Behavior when interval changes at runtime (if supported):

## 5) Enable/Disable semantics
- Gates:
  - Resource Control disabled ⇒ behavior:
  - Feature flag disabled ⇒ behavior:
- On disable:
  - drain + drop? clear state? keep state until next cycle?
- On enable:
  - when does data start appearing?

## 6) Drop/backpressure semantics
- See: tracks/resource-observability-topru/templates/TOPRU_DROP_SPEC.md

## 7) Compatibility semantics
- See: tracks/resource-observability-topru/templates/TOPRU_COMPAT_SPEC.md

## Global maintainer sign-off (optional)
- [ ] Approved by maintainer: <name/date>
- Notes:
