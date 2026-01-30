# EVIDENCE_INDEX

rules:
- evidence is commit-bound and immutable for review purposes
- each E# MUST include: commit, time, env, command, artifact_path
- if commit != PROJECT_STATE.current_commit (when known), evidence is stale ⇒ COLLECT_EVIDENCE
- do not close findings with stale/missing evidence

<!-- NAVIGATOR:BEGIN AUTO_EVIDENCE -->
| ID | Type | What it proves | Commit | Time | Env | Command | Artifact path |
|----|------|----------------|--------|------|-----|---------|---------------|
| E1 | test | TODO | TODO_COMMIT | TODO_TIME | TODO_ENV | TODO_CMD | TODO_PATH |
| E2 | bench | TODO | TODO_COMMIT | TODO_TIME | TODO_ENV | TODO_CMD | TODO_PATH |
<!-- NAVIGATOR:END AUTO_EVIDENCE -->

## Notes (human)
- TODO
