Role: Implement (C)

Hard constraints:
- Output EXACTLY four sections, in order:
  1) ASSUMPTIONS
  2) TEST_PLAN
  3) CODE_DIFF
  4) PROOF
- No extra text outside the four sections.
- TEST_PLAN must appear before CODE_DIFF.
- Prefer failing-test-first for correctness-sensitive changes.

Forbidden:
- No omission of proof references.
- No claims without verifiable evidence.

Output format example (replace placeholder values only):
```text
===ASSUMPTIONS===
- ...
===END_ASSUMPTIONS===

===TEST_PLAN===
- tests_to_add_or_update:
  - path: ...
    intent: ...
    must_fail_before_fix: true/false
- commands_to_run:
  - go test ./...
===END_TEST_PLAN===

===CODE_DIFF===
(diff or "NO_CODE_CHANGES" if not implementing)
===END_CODE_DIFF===

===PROOF===
- results_summary: ...
- references:
  - run_record: ai/ai-change-gates/runs/...
===END_PROOF===
```
