# DATA_PATH_MAP

## Sources
- RUDetails is read from execution context:
    - via adapter GoCtx (a.GoCtx) value util.RUDetailsCtxKey
    - equivalently via ExecutionContext.Ctx (same underlying context)
- SQL/Plan digests and user identity are available in executor begin/finish path.

## Correlation keys / joins
- Key = (user, sql_digest, plan_digest)
- RUDetails delta is attributed to the current key in execCtx.

## Sampling points (cadence)
- In-flight delta sampling is performed in MergeRUInto(), driven by aggregator tick (nominal 1s).
- Finish path flushes final delta based on LastRUSample (no full-RU double count).

## Aggregation
- finishedRUIncrements (RUIncrementMap) accumulates RUIncrements per key per tick.
- MergeRUInto() drains finished buffer and merges active execCtx sampling delta into RUIncrementMap.

## Outputs / sinks
- Reporter collects RURecords from aggregated RUIncrementMap and sends them out via TopSQL reporter pipeline.
- Sinks:
    - PubSub sink: streams records to subscribers that opt into TopRU.
    - SingleTarget sink: sends batch records to the configured target.
- Metadata:
    - SQLMeta/PlanMeta are registered on executor begin path when TopProfilingEnabled (TopSQL or TopRU), and sent alongside RURecords by reporter.

## Notes
- Disabled behavior: TopRU disabled => no-op (no RU collection/output).
- Interval policy: TopSQL and TopRU use independent cadence (separate ticker semantics).
