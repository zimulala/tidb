# Fail Codes Contract (v1)

Source of truth for automation is `fail_codes.json`.

Each code includes:
- `code`
- `title`
- `condition` (deterministic and script-checkable)
- `min_fix` (actionable minimum fix)
- `severity` (`blocker` or `warn`)

Current set:
- E_PATCH_SCOPE
- E_NO_RUN_RECORD
- E_BAD_RUN_RECORD
- E_TOO_MANY_ACTIONS
- E_OPEN_ASSUMPTION
- E_NO_EVIDENCE
- E_MISSING_LAST_RUN
- E_SCHEMA_INVALID
- W_RISK_NO_MITIGATION
