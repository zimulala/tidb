# PHASE_3D_REVIEW_REPORT

## Summary
- scope: TopRU (TiDB kernel / observability) — strict review after initial semi-automation + follow-up review
- head_commit: TODO_COMMIT
- reviewer: TODO_NAME
- date: TODO_DATE
- overall_status: Open (pending closures)

## Inputs
- strict review notes (P0-1/2/3 + majors)
- design doc: docs/design/2026-01-19-topru-design.md (in-flight 1s delta sampling described)
- key implementation areas mentioned:
    - pkg/util/topsql/stmtstats/stmtstats.go
    - pkg/util/topsql/stmtstats/rustats.go
    - pkg/util/topsql/stmtstats/aggregator.go
    - pkg/util/topsql/reporter/reporter.go
    - pkg/executor/adapter.go

## Semantic decisions (recorded)
> These are maintainer decisions and must be mirrored in TOPRU_SEMANTIC_SPEC.md.
- S1 in-flight sampling: **Chosen B (implement execCtx + 1s delta sampling)**
- S2 ExecCount semantics: **begin-based (align with TopSQL semantics)**
- S3 SQL/Plan meta when TopRU enabled and TopSQL disabled: **Required**
- S4 disabled behavior: **No-op when disabled (do not record TopRU; no drop metrics)**
- S5 report interval: **Coupled (Option A) — enabling TopRU may affect TopSQL cadence**

---

## Findings (F#)
> Rules:
> - Status: Open | Closed | WontFix
> - Closed MUST include Evidence: E# (commit-bound) OR an explicit doc-only closure note when appropriate.
> - Every finding MUST include closure_requirement.

### F1 — Critical — In-flight 1s delta sampling missing (design mismatch / functional gap)
- severity: Critical
- issue:
    - Design doc describes in-flight sampling using ExecutionContext + LastRUSample to compute ruDelta = current - last (avoid double count) with local 1s sampling.
    - Current implementation appears finish-only for RU (reads RUDetails cumulative value only at OnExecutionFinished) and does not maintain execCtx/LastRUSample state in StatementStats; RU buffer is finishedRUBuffer only.
    - This contradicts the design expectation and under-represents long-running queries (no intermediate samples).
- references:
    - pkg/util/topsql/stmtstats/stmtstats.go (structure lacks execCtx/LastRUSample)
    - docs/design/2026-01-19-topru-design.md (in-flight sampling described)
- decision context:
    - Chosen: **B — implement execCtx + 1s delta sampling**
- closure_requirement:
    1) Implement per-execution sampling state (execCtx / LastRUSample / last sample time) and 1s local tick delta sampling.
    2) Ensure no double count:
        - delta must be monotonic and never negative; handle counter reset defensively.
    3) Handle edge cases:
        - SQL digest switch within session/execution context
        - abnormal exit / cancellation / panic path
        - partial final interval: ensure final delta is captured at finish without double counting
    4) Add tests:
        - long-running query (>1s): generates >= N delta samples; sum(delta) approximates final RU within acceptable tolerance
        - cancellation/abnormal exit does not leak state and does not double-count
- status: Closed
- evidence: E1
- closure_note:
    - Implemented single-session active `execCtx` lifecycle in `StatementStats` (begin bind, finish clear).
    - `MergeRUInto()` now performs in-flight delta sampling from active execution context.
    - Sampling cadence is aggregator-driven (nominal 1s tick), and finish path only flushes the final delta based on `LastRUSample` (no full-RU double count).

---

### F2 — Critical/Major — ExecCount semantics can de-couple from RU/Duration (statistical integrity)
- severity: Major (promote to Critical if product/consumer requires per-bucket consistency)
- issue:
    - ExecCount increment occurs at begin, while TotalRU/ExecDuration increments occur at finish.
    - Aggregator drains finishedRUBuffer each second; for long queries:
        - ExecCount may be drained early (tick 1) and RU/Duration arrives later (tick N), producing increments with TotalRU>0 but ExecCount=0.
    - Toggle changes mid-execution (TopRUEnabled check differs between begin/finish) can further desync.
    - RU=0 paths: begin may emit ExecCount but finish may not emit RU/Duration (totalRU<=0 returns), creating noisy records (ExecCount>0, RU=0).
- decision context:
    - ExecCount: **begin-based (align with TopSQL semantics)** — therefore we must define how to keep interpretation sound.
- closure_requirement (choose one approach and document it in TOPRU_SEMANTIC_SPEC.md):
    - Option X (recommended): maintain per-exec context so ExecCount and RU/Duration are published in a consistent semantic bucket; begin count should not be drained into a standalone record that can be interpreted as “completed”.
    - Option Y: allow decoupling but mark record types explicitly (e.g., begin_count_only) and ensure downstream/agent/UI does not interpret count-only as completed RU record.
    - Regardless of option:
        1) Define and document the semantics: "begin count" vs "completed count" and how consumers should interpret.
        2) Add tests for:
            - long query (>1s) does not produce misleading (ExecCount=0, RU>0) unless explicitly allowed and marked
            - toggle changes mid-execution behave consistently with the chosen semantics
            - RU=0 produces either no record or an explicitly marked record (to avoid noise)
- status: Closed
- evidence: E1
- closure_note:
    - Begin-based ExecCount is now attributed once on first positive RU delta (tick or finish), preventing count-only RU=0 noise.
    - Late-enable semantics are explicitly documented: RU>0 with ExecCount=0 is expected when TopRU is enabled after execution begins.
    - Added stmtstats tests for long-running multi-tick behavior, mid-exec toggle, RU=0 noise prevention, and same-tick bucket merge semantics.

---

### F3 — Major — TopRU enabled while TopSQL disabled yields missing SQL/Plan meta (usability regression)
- severity: Major
- issue:
    - TopRU records carry digests; without SQLMeta/PlanMeta, observability value is reduced.
    - Current begin path `observeStmtBeginForTopSQL` early-returns when TopSQL disabled, so RegisterSQL/RegisterPlan not called even if TopRU enabled.
    - PubSub/single_target may still send meta sets, leading to situations where only digests are available when TopRU alone is enabled.
- references:
    - pkg/executor/adapter.go (observeStmtBeginForTopSQL gate)
- decision context:
    - **Need SQL/Plan meta when TopRU enabled (TopProfilingEnabled).**
- closure_requirement:
    1) Change gating so that when TopProfilingEnabled() (TopSQL OR TopRU) is true, SQL/Plan meta registration is performed (possibly with lightweight/limited plan policy).
    2) If plan meta is expensive, define fallback:
        - register SQL meta always; plan meta subject to rate limit / size limit.
    3) Add tests:
        - TopSQL disabled + TopRU enabled still results in SQLMeta (and PlanMeta if required) being present in the reported payload.
- status: Closed
- evidence: E1
- closure_note:
    - Changed `observeStmtBeginForTopSQL` gate to use `TopProfilingEnabled()` for meta registration path.
    - Added executor unit test proving SQL/Plan meta registration when TopSQL is disabled and TopRU is enabled.

---

### F4 — Major — Disabled behavior: drain+drop semantics not documented (silent undercount) — **Chosen: No-op when disabled**
- severity: Major (documentation correctness)
- issue:
    - Current behavior drains/aggregates even when TopRU disabled (may silently discard).
    - Review concern: silent undercount if disabled drops are not explicit/observable.
- decision context:
    - **Chosen:** disabled => no-op (do not record TopRU; do not recommend metrics since it requires RU collection not requested).
- closure_requirement:
    1) Make disabled behavior explicit in TOPRU_SEMANTIC_SPEC.md:
        - When disabled, TopRU should not collect RU deltas; any buffers should be treated as non-authoritative.
    2) Align implementation with the “no-op” semantic:
        - If aggregator still runs for housekeeping, ensure it does not require RU collection and does not produce TopRU records.
    3) Add code comments near the gate(s) explaining why no drop metrics exist (explicit non-goal).
- status: Open
- evidence: Doc-only closure acceptable (no E# required), OR TODO_E# if you add a unit test confirming no TopRU output when disabled.

---

### F5 — Major — Report interval semantics mixed; TopRU enabling may change TopSQL cadence — **Chosen: Coupled (min interval)**
- severity: Major (semantic coupling / operational expectation)
- issue:
    - Applying min(TopSQL interval, TopRU interval) can change TopSQL reporting cadence when TopRU enabled.
    - This cross-feature coupling must be explicit and accepted; otherwise it looks like a bug.
- references:
    - pkg/util/topsql/reporter/reporter.go
- decision context:
    - **Chosen:** Option A (coupled). Enabling TopRU may affect TopSQL cadence.
- closure_requirement:
    1) Document explicitly in TOPRU_SEMANTIC_SPEC.md + (optional) reporter-level comment:
        - “TopRU can reduce effective report interval by taking min; this is expected.”
    2) Add minimal test or deterministic check:
        - verify the effective interval selection behavior is stable and matches the coupling policy.
- status: Open
- evidence: TODO_E# (E1 minimal unit test) or Doc-only closure if you consider the doc sufficient (recommended: add small test).

---

## Performance / Resource risks (tracked)
- R1: in-flight 1s sampling overhead under high QPS / high cardinality — must be measured (bench/profile)
    - closure evidence: TODO_E2 (benchmark or profiling report, commit-bound)
- R2: meta registration cost when TopSQL disabled but TopRU enabled — may need limits
    - closure evidence: TODO_E2 (bench) or explicit fallback acceptance

## Backward compatibility (tracked)
- Chosen semantics do not mention agent compatibility in this report; if RPC changes exist, add findings here.
- If any Unimplemented handling differs between paths, track as separate finding (F#) with closure requirement.

## Tests / Bench gaps (minimum set)
- T1: in-flight sampling long query produces N deltas and no double count (E1)
- T2: cancellation/abnormal exit does not leak execCtx or double count (E1)
- T3: TopSQL disabled + TopRU enabled registers SQL/Plan meta per policy (E1)
- T4: interval coupling behavior stable (E1)
- B1: sampling overhead (E2)
- B2: aggregator overhead under high cardinality (E2)

---

## Evidence Index linkage
- Evidence must be commit-bound (commit/time/env/command/artifact_path) in `topru-ai/EVIDENCE_INDEX.md`.
- Any code change invalidates evidence unless rerun and recorded with new commit.
