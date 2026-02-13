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
