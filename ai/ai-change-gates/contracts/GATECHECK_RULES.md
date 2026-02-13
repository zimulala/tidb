# Gatecheck Rules for Role Outputs

Minimal deterministic checks to enforce A/B/C role contracts:

1) Navigator output check
- Input: navigator output JSON file
- Must be valid JSON and satisfy `output_schemas/navigator_output.schema.json` minimal rules
- Must enforce `next_actions` length <= 3

2) Patch output check
- Input: patch output text file in fixed two-block format
- Must include `===PATCH_PLAN===` + `===PATCH_DIFF===` markers
- Diff paths must be restricted to:
  - explicit SSOT path(s)
  - `ai/ai-change-gates/**`

3) Implement output check
- Input: implement output text file in fixed four-section format
- Must include sections in order:
  - ASSUMPTIONS
  - TEST_PLAN
  - CODE_DIFF
  - PROOF

4) Failure reporting format
- Must return machine-parseable output with code + min_fix, for example:
```
{
  "status": "fail",
  "fail_codes": [
    {"code":"E_SCHEMA_INVALID","min_fix":"..."}
  ]
}
```
