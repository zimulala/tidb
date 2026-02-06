TL: TL1/TL3

Task:
You run STRICT_REVIEW_FIX_ONECLICK for a given project. You MUST:
- collect commit-bound review evidence (diff + changed files + commits)
- output human-readable review.md (Chinese) AND machine-readable findings.yaml
- write artifacts to ai/projects/topru-ai/artifacts/review/<RUN_ID>/
- patch SSOT_V2 review section in ai/projects/topru-ai/PROJECT_STATE.md (when allowed)
- output next repair actions and a suggested incremental re-review command

Safety boundary (hard):
- No GitHub comments; no auto-merge; no issue creation.
- Do NOT modify production source code unless the user explicitly asks for a patch/commit.
- Default to: analysis + artifacts + SSOT updates + suggested fix tasks + verify commands.

Inputs:
- repo root: current working directory
- SSOT: ai/projects/topru-ai/PROJECT_STATE.md
- optional protocol root: ai/ai-change-gates
- review range:
  - prefer explicit BASE/HEAD, else:
      base = git merge-base origin/master HEAD (fallback origin/main, master, main)
      head = git rev-parse HEAD
- mode:
  - if SSOT contains review.baseline_commit => incremental (incr)
  - else first

Artifacts (must create):
- ai/projects/topru-ai/artifacts/review/<RUN_ID>/changed_files.txt
- ai/projects/topru-ai/artifacts/review/<RUN_ID>/diff.patch
- ai/projects/topru-ai/artifacts/review/<RUN_ID>/commits.txt
- ai/projects/topru-ai/artifacts/review/<RUN_ID>/summary.txt
- ai/projects/topru-ai/artifacts/review/<RUN_ID>/review.md
- ai/projects/topru-ai/artifacts/review/<RUN_ID>/findings.yaml
- ai/projects/topru-ai/artifacts/review/<RUN_ID>/run.log
- ai/projects/topru-ai/artifacts/review/<RUN_ID>/next_actions.md

Review output rules:
- review.md: Chinese; each finding MUST include fields:
  类型 / Must fix / 影响面 / 范围 / 概率 / 位置 / 建议 / 最小验证 / 推荐验证 / 清理
- findings.yaml schema:
  baseline: "<base>"
  head: "<head>"
  findings:
    - id: R1
      must_fix: true
      type: Risk
      impact: Compatibility
      scope: upgrade-path
      likelihood: Med
      location: "path/to/file.go:FuncName (hunk: ...)"
      title: "一句话问题"
      advice: "具体修复方向"
      verify_min: ["go test ..."]
      verify_opt: ["..."]
      cleanup: []
      status: open

Incremental rule (hard):
- incr/targeted: default ONLY re-check previous open findings.
- only add new findings if the new diff touches new areas not covered by previous findings.

SSOT patch rule:
- Update SSOT_V2 with:
  review:
    baseline_commit: "<head>"
    open: [R1, R3]
    fixed: [R2]
    partially_fixed: [R4]
    last_run:
      time: "<iso>"
      range: "<base>..<head>"
      artifacts:
        review_md: "ai/projects/topru-ai/artifacts/review/<RUN_ID>/review.md"
        findings_yaml: "ai/projects/topru-ai/artifacts/review/<RUN_ID>/findings.yaml"
        run_log: "ai/projects/topru-ai/artifacts/review/<RUN_ID>/run.log"

Output (short):
1) Mode + Range
2) Patched SSOT: yes/no
3) Open must-fix findings: [..]
4) Next actions (max 3): each must include a minimal verify command
5) Next re-review command

