# EVIDENCE_INDEX

rules:
- evidence is commit-bound and immutable for review purposes
- each E# MUST include: commit, time, env, command, artifact_path
- if commit != PROJECT_STATE.current_commit (when known), evidence is stale ⇒ COLLECT_EVIDENCE
- do not close findings with stale/missing evidence

<!-- NAVIGATOR:BEGIN AUTO_EVIDENCE -->
| ID | Type | What it proves | Commit | Time | Env | Command | Artifact path |
|----|------|----------------|--------|------|-----|---------|---------------|
| E_TBD | plan | Evidence plan placeholder (will be replaced by real test/bench artifacts). | e77f6de611501f8093e5df24ce58aa2c5a8be565 | 2026-01-30T07:21:16Z | local | N/A (plan) | ./artifacts/evidence/ |
| E4 | doc | Semantic coupling accepted: interval min policy documented; disabled=no-op documented. | e77f6de611501f8093e5df24ce58aa2c5a8be565 | 2026-01-30T07:21:16Z | local | edit docs/spec | ./TOPRU_SEMANTIC_SPEC.md |
| E1 | plan | (Planned) In-flight sampling correctness: >=N deltas, no double count, abnormal exit safe; begin-based ExecCount semantics tests; meta registration when TopSQL off. | e77f6de611501f8093e5df24ce58aa2c5a8be565 | 2026-01-30T07:21:16Z | local | TODO: go test ... | ./artifacts/evidence/E1/ |
| E2 | plan | (Planned) Same bucket accumulation correctness for same TopRU key; appears reliably in TopN output (no loss/overwrite/double-count). | e77f6de611501f8093e5df24ce58aa2c5a8be565 | 2026-01-30T07:21:16Z | local | TODO: go test ./... -run TestTopRUAccumulateSameBucketSameKey -count=1 | ./artifacts/evidence/E2/ |
| E3 | plan | (Planned) Overhead evaluation: 1s sampling + meta registration overhead under representative workload. | e77f6de611501f8093e5df24ce58aa2c5a8be565 | 2026-01-30T07:21:16Z | local | TODO: bench/profile command | ./artifacts/evidence/E3/ |
<!-- NAVIGATOR:END AUTO_EVIDENCE -->

## Notes (human)
- TODO
