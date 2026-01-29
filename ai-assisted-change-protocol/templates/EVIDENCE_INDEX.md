# Evidence Index (for TL4 claims)

> Every "Verified-by-test" statement must reference an evidence item here.
> Evidence is immutable during review:
> - must bind commit hash + time + environment + command/params
> - if the code diff changes, the evidence is stale; collect new evidence and use a new Evidence ID.

| Evidence ID | Type | What it proves | Where to find | Time | Commit | Environment | Command/params | Notes |
|------------:|------|----------------|---------------|------|--------|-------------|----------------|------|
| E1 | CI / metric | Diagnostics bundle success rate (per trigger) | link/log path | | | | | |
| E2 | Metric / report | SOP coverage rate (case2/3) + time-to-initial-triage | report/dashboard path | | | | | |
| E3 | Benchmark / profile | Overhead (CPU/memory/latency/IO) and regression risk | report path | | | | | |
