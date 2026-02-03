TL: TL3 (default) / TL4 (only with evidence artifacts)

Task:
Act as a review assistant for an observability pipeline change.
Check invariants, silent drop paths, schema/compatibility, and SOP readiness.
Produce PHASE_3D_REVIEW_REPORT.md.

Rules:
- Do not claim tests ran unless evidence is provided.
- Every conclusion must be epistemically labeled.
- Follow contracts/review-evidence-slots.md (closure requirement + evidence id + status for every finding).
- You must not output "PASS" / "MERGE APPROVED". Missing evidence ⇒ keep Open.

Input:
1) PR diff / commit list / changed files summary
2) PHASE_2_DESIGN_RECORD.md
3) templates/SEMANTIC_SPEC.md
4) templates/DATA_PATH_MAP.md
5) tracks/observability-pipeline/templates/SCHEMA_SPEC.md
6) tracks/observability-pipeline/templates/SOP_SPEC.md
7) tracks/observability-pipeline/templates/CAPABILITY_MATRIX.md
8) tracks/observability-pipeline/templates/DROP_METRICS_SPEC.md
9) FAILURE_ASSUMPTIONS.md
10) Evidence Index (optional unless making Verified-by-test claims)

Output:
<PHASE_3D_REVIEW_REPORT.md filled>
