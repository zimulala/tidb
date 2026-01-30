# EVIDENCE_INDEX

rules:
- evidence is commit-bound and immutable for review purposes
- each E# MUST include: commit, time, env, command, artifact_path
- if commit != PROJECT_STATE.current_commit (when known), evidence is stale ⇒ COLLECT_EVIDENCE
- do not close findings with stale/missing evidence

<!-- NAVIGATOR:BEGIN AUTO_EVIDENCE -->
| ID | Type  | What it proves                                                                                                                                                                                                                                                                                                              | Commit | Time | Env | Command | Artifact path |
|----|-------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|------|-----|---------|---------------|
| E1 | test  | In-flight sampling correctness: >=N deltas, no double count, abnormal exit safe; begin-based ExecCount semantics tests; meta registration when TopSQL off.                                                                                                                                                                  | TODO_COMMIT | TODO_TIME | TODO_ENV | TODO_CMD | TODO_PATH |
| E2 | test  | Proves that within the same timestamp bucket(aggregator tick（1s）), multiple finish events for the same TopRU key(user/sql_digest/plan_digest) produce RUIncrements that accumulate correctly (RU/duration/exec semantics per spec) and the aggregated key reliably appears in TopN output (no loss/overwrite/double-count). | TODO_COMMIT | TODO_TIME | TODO_ENV | go test ./... -run TestTopRUAccumulateSameBucketSameKey -count=1 | TODO_PATH |
| E3 | bench | Overhead evaluation: 1s sampling + meta registration overhead under representative workload.                                                                                                                                                                                                                                | TODO_COMMIT | TODO_TIME | TODO_ENV | TODO_CMD | TODO_PATH |
| E4 | doc   | Semantic coupling accepted: interval min policy documented; disabled=no-op documented.                                                                                                                                                                                                                                      | TODO_COMMIT | TODO_TIME | TODO_ENV | TODO_CMD | TODO_PATH |
| E_TBD | plan  | todo                                                                                                                                                                                                                                                                                                                        | | TODO_TIME | TODO_ENV | TODO_CMD | TODO_PATH |
<!-- NAVIGATOR:END AUTO_EVIDENCE -->

## Notes (human)
- TODO
