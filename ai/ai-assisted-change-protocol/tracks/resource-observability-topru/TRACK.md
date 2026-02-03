# TRACK: resource-observability-topru

# TRACK_FORMAT_VERSION=2
track_id=resource-observability-topru

pr_ready_must=[E_func,E_integ]
pr_ready_should=[E_perf]
pr_ready_conditional=[E_compat]

notes=PR-ready requires functional correctness + integration smoke; perf evidence recommended; compat evidence required if shipping across versions/agents.
