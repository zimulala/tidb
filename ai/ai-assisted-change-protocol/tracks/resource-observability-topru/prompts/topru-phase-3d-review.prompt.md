TL: TL3 (default) / TL4 (only with evidence artifacts)

Task:
Act as a review assistant for a TopRU-like change.
Focus on semantic correctness (sampling/interval/exec_count), compatibility, and drop/backpressure observability.
Produce PHASE_3D_REVIEW_REPORT.md.

Rules:
- Do not claim tests ran unless evidence is provided.
- Every conclusion must be epistemically labeled.
- Follow contracts/review-evidence-slots.md (closure requirement + evidence id + status for every finding).
- You must not output "PASS" / "MERGE APPROVED". Missing evidence ⇒ keep Open.

Known failure modes to explicitly check (must address in findings):
- Sampling cadence drift vs design (e.g. "1s delta sampling" missing or inconsistent).
- exec_count semantics decoupled from TotalRU/ExecDuration (user-facing confusion / correctness risk).
- TopSQL disabled but TopRU enabled causing SQLMeta/PlanMeta incompleteness.

Input:
1) PR diff / commit list / changed files summary
2) PHASE_2_DESIGN_RECORD.md
3) templates/SEMANTIC_SPEC.md
4) tracks/resource-observability-topru/templates/TOPRU_SEMANTIC_SPEC.md
5) tracks/resource-observability-topru/templates/TOPRU_COMPAT_SPEC.md
6) tracks/resource-observability-topru/templates/TOPRU_DROP_SPEC.md
7) FAILURE_ASSUMPTIONS.md
8) Evidence Index (optional unless making Verified-by-test claims)

Output:
<PHASE_3D_REVIEW_REPORT.md filled>
