# Data Path Map (TopRU)

> Goal: end-to-end map from trigger → correlation → collection → aggregation → export.
> This is the concrete “pipeline view” used for review and SOP.

## Step 1 — Trigger / Source
- Source:
  - SQL execution begin/finish lifecycle events
  - 1s aggregator tick (sampling cadence)
  - Subscription enable/disable + report_interval changes (control plane)
- Key(s) produced:
  - RUKey = (user, sql_digest, plan_digest)
  - Sampling timestamp_sec
- Where in code:
  - executor hook(s) / stmtstats ExecutionContext lifecycle
  - stmtstats aggregator tick
  - reporter RU collecting/bucketing

## Step 2 — Correlation
- correlation key(s):
  - user + sql_digest + plan_digest (best-effort)
- time_window definition:
  - report_interval aligned window (wall-clock aligned)
  - item timestamp = sampling timestamp_sec (tick time)
- ambiguity handling:
  - plan_digest missing: per SEMANTIC_SPEC S6 (drop/degrade/mark unknown)
  - cross-window executions: RU deltas may span multiple windows (document user interpretation)

## Step 3 — Collection
- logs:
  - enable/disable transitions (optional)
  - abnormal delta samples (optional; rate-limited)
- metrics:
  - (optional) sampling count / invalid sample count / drop counters
- traces:
  - usually N/A (hot path); only if explicitly enabled
- configs / env:
  - enable_top_ru
  - report_interval (15s/30s/60s)
  - resource control enabled/disabled gate

## Step 4 — Aggregation / Processing
- aggregation points:
  - per-session: ExecutionContext + finished buffer
  - global 1s tick: merge increments across sessions
  - reporter: TopN + others (pre-filter and final report)
- caps / backpressure:
  - key caps (session/aggregator/reporter)
  - TopN limits + "others" semantics
- drop policy + metrics:
  - invalid delta (<=0 or nil) dropped
  - backpressure/channel full behavior (must be documented)
  - disabled behavior (must be documented)

## Step 5 — Build output / Bundle
- bundle schema:
  - tipb.TopRURecord + TopRURecordItem time-series
  - SQLMeta/PlanMeta (best-effort, required for usability per S6)
- required fields:
  - sql_digest + timestamp_sec + total_ru
  - plan_digest best-effort
- versioning:
  - protobuf additive-only
- bundle id / correlation fields:
  - digest keys + time window implied by timestamp and report interval

## Step 6 — Transport / Storage / Bundle export
- transport:
  - PubSub stream (TopSQLSubResponse oneof) and/or SingleTarget RPC
- storage:
  - downstream agent / TiDB Cloud ingestion pipeline
- retention:
  - downstream-defined
- export mechanism (where/how bundle is exported):
  - periodic send every report_interval

## Step 7 — Consumer / SOP
- consumer:
  - agent/collector side that turns digests into readable diagnostics
- SOP routing rules:
  - if meta missing: fallback path (digest-only) + user guidance
  - if drop/backpressure happens: expected signals + recommended actions
