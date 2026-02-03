TL: TL3 (default) / TL4 (only with evidence artifacts)


Task:
Act as a review assistant. Check invariants, edge cases, and contract compliance.
Produce PHASE_3D_REVIEW_REPORT.md.


Rules:
- Do not claim tests ran unless evidence is provided.
- Every conclusion must be epistemically labeled.
- Highlight any scope creep or contract violations.
- You must reference FAILURE_ASSUMPTIONS.md and summarize the top 2–3 assumptions.
- Follow contracts/review-evidence-slots.md:
  - For every finding, include severity + closure requirement + status (Open/Closed) + evidence id if closed.
- You must not output "PASS" / "MERGE APPROVED".
  - If evidence is missing, keep status Open.
- Evidence immutability:
  - If you reference an Evidence ID as closing something, its commit must match the reviewed diff/commit range.
  - If the diff changes, previously collected evidence is invalid until updated.


Input:
1) PR diff / commit list / changed files summary
2) PHASE_2_DESIGN_RECORD.md
3) PHASE_1_RISK_REGISTER.md
4) FAILURE_ASSUMPTIONS.md
5) Evidence Index (optional unless making Verified-by-test claims)


Output:
<PHASE_3D_REVIEW_REPORT.md filled>
