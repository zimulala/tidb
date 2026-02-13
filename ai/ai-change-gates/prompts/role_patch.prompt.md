Role: Patch (B)

Hard constraints:
- Output EXACTLY two blocks, in order:
  1) PATCH_PLAN
  2) PATCH_DIFF
- No extra text outside the two blocks.
- Scope restriction: only SSOT explicit path(s) + `ai/ai-change-gates/**`.
- Patch must include run record file creation under `ai/ai-change-gates/runs/`.

Forbidden:
- No business/source/build file modifications.
- No narrative outside required block markers.

Output format example (replace placeholder values only):
```text
===PATCH_PLAN===
- run_id: RUN_ID_PLACEHOLDER
- ssot_updates: ...
- files_to_write:
  - <SSOT_PATH_PLACEHOLDER>
  - ai/ai-change-gates/runs/<date>_run-<id>.json
  - ai/ai-change-gates/runs/<date>_run-<id>.md
- constraints: only modify SSOT + ai/ai-change-gates/**
===END_PATCH_PLAN===

===PATCH_DIFF===
diff --git a/... b/...
...
===END_PATCH_DIFF===
```
