# deconflict

Purpose: Resolve Git PR conflicts in a **worktree** created by `wt deconflict <prNum>` with **maximum determinism**, **minimum risk**, and **auditable outputs**.

This skill assumes:
- You are inside the deconflict worktree (for example `${WT_ROOT}/deconflict-pr-<pr>-<base>`).
- `wt deconflict <prNum>` already generated `_run/ai/pr_meta.json` and starter report.
- Push is done by `wt deconflict <prNum> --push` (this skill does not push).

## 0) TL;DR
1. Collect conflict files to `_run/ai/conflicts.txt`.
2. Generate/update PR policy at `_run/ai/pr_policy.md`.
3. Resolve conflicts with **HEAD-only by default** plus policy KEEP allowlist.
4. Verify (at least compile) and record evidence.
5. Continue rebase/merge and update `_run/ai/deconflict_report.md`.
6. Run ready-to-push checklist, then use `wt deconflict <prNum> --push`.

## 1) Scope & Safety

### In scope
- Resolve merge/rebase conflicts only.
- Apply minimal changes required to remove conflict markers.
- Preserve PR-required items defined by PR policy.
- Make code compile and pass minimal checks.

### Out of scope
- No unrelated refactors.
- No style-only edits beyond necessary `gofmt`.
- No direct push to remote (use `wt deconflict ... --push`).
- Do not decide rebase vs merge for user preference; report both command blocks.

### Safety defaults
- Default policy: keep `<<<<<<< HEAD` side.
- Introduce non-HEAD content only when explicitly allowed by PR policy KEEP.
- Introduce non-HEAD content only when required to satisfy compile checks.
- If non-HEAD content is needed, graft the smallest possible fragment.

## 2) Inputs

Required:
- `_run/ai/pr_meta.json`.
- Current Git conflict state (rebase/merge in progress).

Recommended:
- `_run/ai/pr_policy.md`.

## 3) Outputs

Always create/update:
- `_run/ai/conflicts.txt` (machine-readable conflict files).
- `_run/ai/commands.log` (audit trail of key commands and outputs).
- `_run/ai/deconflict_report.md` (human-readable report).

Optional:
- `_run/ai/patch.diff` (final diff snapshot).

## 4) Decision Framework

### 4.1 Conflict types
- Type A: text-only (docs/comments/log strings), low risk.
- Type B: syntax/structure (imports/fields/signatures/simple logic), medium risk.
- Type C: call-chain consistency/API changes across files, high risk.
- Type D: large refactor/move/many hunks, highest risk.

### 4.2 Three-layer strategy
- L1 Mechanical: resolve by HEAD-only.
- L2 Structural invariants: no conflict markers, `gofmt`, minimal compile check passes.
- L3 Minimal non-HEAD graft: only when needed by PR KEEP policy or compile.

### 4.3 Escalation thresholds
Stop and escalate when any is true:
- One file has more than 3 conflict hunks.
- Need to import more than 20 non-HEAD lines (or multiple functions).
- Cross-file API mismatch persists after minimal fixes.
- Conflict touches high-risk/denylisted paths from policy.
- Result cannot be explained in 2-3 sentences.

When escalating, write in report:
- What was tried.
- What is uncertain.
- Two candidate approaches.

## 5) PR Policy (per PR, never hardcoded)

### 5.1 Why
Required-keep behavior differs per PR. Do not hardcode special cases in skill.

### 5.2 Path
- `_run/ai/pr_policy.md`

### 5.3 Preferred generation flow
1. Read PR URL from `_run/ai/pr_meta.json`.
2. Extract `DECONFLICT_POLICY:` block from PR body (or commit messages).
3. If absent, initialize template and fill manually.

Suggested extraction command:
```bash
pr_url="$(jq -r '.url' _run/ai/pr_meta.json)"
pr_num="$(basename "$pr_url")"
repo="$(git remote get-url origin | sed -E 's#(git@github.com:|https://github.com/|\\.git)##g')"
gh pr view "$pr_num" --repo "$repo" --json body --jq '.body'
```

### 5.4 Policy fields
- `PreferHEAD`: default `true`.
- `KEEP` allowlist: symbols/behaviors that must exist in final result.
- `NOTOUCH` denylist: paths to avoid unless unavoidable.
- `ExtraChecks`: extra validation commands.

## 6) SOP

Log helper:
```bash
mkdir -p _run/ai _run/logs
: > _run/ai/commands.log
log() { echo "+ $*" | tee -a _run/ai/commands.log; "$@" 2>&1 | tee -a _run/ai/commands.log; }
```

### Step 1: collect conflict list
```bash
log git status
log git diff --name-only --diff-filter=U | tee _run/ai/conflicts.txt
```

### Step 2: create/update PR policy
If `_run/ai/pr_policy.md` is missing, create:
```bash
test -f _run/ai/pr_policy.md || cat > _run/ai/pr_policy.md <<'EOF'
# PR Deconflict Policy

## Metadata
- PR:
- Base:
- Head:
- Strategy preference: either

## Rules
- PreferHEAD: true

## KEEP (allowlist)
KEEP:
  - symbol:

## NOTOUCH (denylist)
NOTOUCH:
  - path:

## ExtraChecks
ExtraChecks:
  - cmd:

## Notes
- Context:
- Risk areas:
EOF
```

### Step 3: resolve each conflicted file
For each file in `_run/ai/conflicts.txt`:
1. Keep HEAD side of each conflict block.
2. Remove markers and non-HEAD side.
3. Run `gofmt -w <file>`.
4. Enforce KEEP items:
- Check symbol exists: `log rg -n "<KEEP_ITEM>" -S <file> || true`
- If missing, graft minimal non-HEAD fragment only.
- Re-run `gofmt -w <file>`.

### Step 4: global sanity
```bash
log rg -n "<<<<<<<|=======|>>>>>>>" -S . || true
```
If any markers remain, fix before proceeding.

For each KEEP item:
```bash
log rg -n "<KEEP_ITEM>" -S . || true
```

### Step 5: minimal verification
Default compile-only:
```bash
log go test ./... -run TestNonExistent
```

Run `ExtraChecks` from policy when provided.

### Step 6: continue rebase/merge
If rebase in progress:
```bash
log git add -A
log git rebase --continue
```

If merge in progress:
```bash
log git add -A
log git commit
```

If more conflicts appear, repeat steps 1-6.

### Step 7: optional patch snapshot
```bash
log git diff > _run/ai/patch.diff || true
```

## 7) Acceleration: rerere
Recommended once per machine:
```bash
git config --global rerere.enabled true
git config --global rerere.autoupdate true
```

Record rerere usage in report.

## 8) Report template
Use `_run/ai/deconflict_report.md`:
```markdown
# Deconflict Report

## PR
- URL:
- Title:
- Base:
- Head (owner/ref):
- Worktree path:
- Strategy chosen: rebase|merge|either

## Policy Summary
- PreferHEAD: true/false
- KEEP items:
- NOTOUCH:
- ExtraChecks:

## Conflicts
| File | Type (A/B/C/D) | Approach | Non-HEAD graft? | Notes |
|------|-----------------|----------|------------------|-------|
|      |                 | HEAD-only | yes/no          |       |

## Actions Taken
- Resolved markers by HEAD-only
- Applied KEEP items with minimal graft
- Applied gofmt

## Verification
- Marker scan: pass/fail
- KEEP scan: pass/fail
- Minimal compile: pass/fail
- ExtraChecks: pass/fail

## rerere
- enabled: yes/no
- used recorded resolution: yes/no/unknown

## Escalation (if any)
- What is uncertain:
- Two candidate approaches:
  1)
  2)

## Ready-to-push Checklist
- [ ] No conflict markers remain
- [ ] KEEP items present
- [ ] Minimal compile passed
- [ ] `_run/ai/commands.log` captured key outputs
- [ ] Changes limited to conflict-resolution scope

## Next Step
Run `wt deconflict <prNum> --push`
```

## 9) Fallback comment template
If `wt deconflict <prNum> --push` falls back to fork:
```text
I resolved merge conflicts for this PR in a deconflict worktree and produced an auditable report.
Because pushing to the original head branch was not possible (permissions / branch protection), I pushed a deconflict branch to my fork and opened a replacement PR:

- Replacement PR: <LINK>
- Deconflict report summary: <KEY POINTS>

Please consider merging the replacement PR, or advise if you prefer an alternative approach.
```

## 10) Reference command blocks

Rebase:
```bash
git fetch upstream <baseRefName>
git rebase upstream/<baseRefName>
git diff --name-only --diff-filter=U
git add -A
git rebase --continue
git rebase --abort
```

Merge:
```bash
git fetch upstream <baseRefName>
git merge --no-ff upstream/<baseRefName>
git add -A
git commit
git merge --abort
```
