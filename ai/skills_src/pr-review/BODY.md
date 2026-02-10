
###############################################################################
# EXECUTION ZONE (Normative, Enforced)
#
# - This section defines mandatory PR review behavior.
# - All rules here MUST be followed when the skill is active.
# - No historical notes, rationale, or changelog content is allowed here.
###############################################################################

## 0. Scope & Trigger

Apply this skill when:
- The user asks to review a PR / diff / commit(s)
- The user asks to re-review after new commits
- The user says “用 PR Review Skill review”

Default behavior:
- Output **Chinese review results for human approval**
- If the user explicitly requests a different output language, follow the user’s instruction
- **Do not post anything to GitHub** unless the user explicitly instructs which comments to push

## 1. Hard Workflow (Must Follow Every Round)

### 1.1 Define Scope for This Round

Classify the round as one of:
- **First review**: no prior review in this session
- **Incremental review**: PR has new commit(s) after the last round
- **Targeted verification**: verify specific previously raised comments only

If the user does not specify scope:
- If there is **no prior review baseline** (no previously reviewed commit/hash mentioned in this session), default to **First review**
- Otherwise, default to **Incremental review**:
  - Only review the diff since the last reviewed commit/hash mentioned in the conversation

### 1.2 Collect Evidence (Minimal but Sufficient)

Perform evidence-based review:
- Prefer using any provided PR artifacts (diff/summary/changed-files metadata) when available
- Read PR description and changed-files list
- For incremental review, focus on:
  - `git show <commit>`
  - `git diff <prev>..<new>`
  - touched files only
- Run **targeted** tests or compilation when feasible; avoid broad test runs
- If a test in this repository requires `--tags=intest`, use it
- If the target packages use failpoints, enable failpoints before running tests and ensure they are disabled afterward

### 1.3 Review Dimensions

Always check:
- **基本功能与正确性**
  - Feature completeness vs PR claim
  - Call-chain correctness
  - Obvious logic bugs
  - Signature mismatches
  - Missing `Close` / `Flush`
  - Error handling
- **工程风险**
  - Resource leaks (memory / goroutine / FD / connection / `resp.Body`)
  - Data race / concurrency safety
  - Security issues (secret logging, unbounded reads, unsafe defaults)

Conditionally check (only when behavior or contract may change):
- **边界与兼容性**
  - Defaults
  - Filesystem / object-store paths
  - System variables
  - HTTP behavior
  - GC behavior
  - Implicit ops scripts

- **Contract/Behavior Change Checklist**
  - **Defaults**: default value changes? backward compatibility impact?
  - **Units/Types**: parsing/matching units consistent across docs/examples/code?
  - **Upgrade Path**: does upgrade/downgrade change behavior or break configs?
  - **HTTP/CLI**: response codes, payload shape, timeouts, retries?
  - **Logging/Observability**: new high-frequency logs? sensitive data? log volume risk?
  - **Error Surface**: error codes/messages stable and diagnosable?

Optional (suggestion-only, non-blocking unless explicitly requested):
- **性能与工程优化**
  - Avoid unnecessary allocations or copies
  - Avoid `io.ReadAll` when streaming is sufficient
  - Avoid extra IO
  - Identify caching opportunities

### 1.3.1 Severity & Impact (Required)

For each finding, classify:
- **影响面**: Crash / DataLoss / Security / Compatibility / Perf / Observability / Noise
- **范围**: local / cluster / upgrade-path / multi-tenant
- **概率**: High / Med / Low

Use this classification to justify **Must fix** decisions and to order the output list by priority.

### 1.4 Produce Review Results (Chinese, for Human Approval)

Output a numbered list. Each item MUST include:
- **类型**: Bug / Risk / Security / Optimization / Suggestion
- **Must fix**: Yes / No
- **影响面**: Crash / DataLoss / Security / Compatibility / Perf / Observability / Noise
- **范围**: local / cluster / upgrade-path / multi-tenant
- **概率**: High / Med / Low
- **位置**: file + function (and line/hunk if known)
- **结论**: one-sentence problem statement
- **建议动作**: concrete fix direction
- **验证（最小，必须）**: how to confirm minimally (unit test, compile, manual steps)
  - Include minimal reproducible commands when suggesting tests or builds
  - Include required cleanup steps when verification changes workspace state (e.g. failpoint enable/disable)
- **验证（推荐，可选）**: heavier validation if appropriate (integration/intest/etc.)
- **清理**: required workspace cleanup steps (only if state changed)

Template:

- [N] **类型**: <Bug|Risk|Security|Optimization|Suggestion>  **Must fix**: <Yes|No>
  - **影响面**: <Crash|DataLoss|Security|Compatibility|Perf|Observability|Noise>
  - **范围**: <local|cluster|upgrade-path|multi-tenant>
  - **概率**: <High|Med|Low>
  - **位置**: `<path>` / `<func>`
  - **问题**: ...
  - **建议动作**: ...
  - **验证（最小，必须）**: ...
  - **验证（推荐，可选）**: ...
  - **清理**: ...

### 1.5 Stop and Wait

After producing Chinese review results:
- **Stop**
- Wait for explicit user instruction
- Do not assume any comment should be pushed to GitHub

## 2. Incremental Review Constraints (Strict)

When new commits appear:
- **Only validate whether previously reported issues are fixed correctly**
- Keep a brief internal list of previously reported items and their fix status:
  - open / fixed / partially fixed
- Do not re-review parts already confirmed OK, unless the new diff touches them
- Explicitly distinguish:
  - “问题是否被修复”
  - “仍可改进之处（非必须）”

### 2.1 Incremental Review Summary (Required)

At the beginning of an incremental review, include a short summary:
- **baseline**: <previous reviewed commit/hash>
- **new range**: <prev..new>
- **previous findings status**:
  - open: [R1,R3,...]
  - fixed: [R2,...]
  - partially fixed: [R4,...]

Only verify previously reported items unless new diff touches other areas.

## 3. GitHub Interaction Boundary (Cognitive Only)

The following rules define recognition of GitHub constraints.
They do NOT authorize execution.

- Generated GitHub review comments must be **English**
- Inline review comments must anchor to **diff-resolvable lines or hunks only**
- Do NOT post, submit, or simulate posting without explicit user approval
- Do not assume generated comments will be accepted by GitHub; anchoring may require adjustment
- Creating new GitHub issues is also a GitHub interaction:
  - Do NOT create issues unless the user explicitly instructs to do so
  - When instructed, follow the target repo’s issue template and language norms
- Any GitHub operation with side effects (e.g. commenting, creating issues, labeling, closing/reopening) requires explicit user instruction

### 3.1 Inline Anchoring Constraints

- Inline comments cannot use absolute file line numbers
- If the real problematic line is not in the diff:
  - Anchor to a nearby changed line in the same file
  - Explicitly mention the real issue location in the comment
- The same account can have only **one pending review** per PR

### 3.2 Practical “gh” Failure Modes (Knowledge Only)

- Pending review exists → cannot create a new review; use a PR comment, or wait for the user to submit/discard the pending review
- 400 JSON parse error → avoid manual quoting; prefer `gh api --jq` filtering or generate JSON via tooling
- 422 Line could not be resolved → anchor to diff-visible lines; submit comments one by one if needed
- Some `gh` subcommands may be unavailable (CLI build/version differences) → fall back to `gh api` and local artifacts
  - Example: no `gh label` command → check labels via `gh api repos/<owner>/<repo>/labels/<url-encoded-label>`
- Fetching raw template URLs may time out → use `gh api repos/<owner>/<repo>/contents/<path>` as a reliable fallback
  - Note: GitHub Contents API returns Base64 `content`; decode locally (e.g. `python3 -c 'import base64,sys;print(base64.b64decode(sys.stdin.read()).decode())'` or `base64 -d`)
- Label/template discovery without dedicated commands → use API endpoints
  - List templates: `gh api repos/<owner>/<repo>/contents/.github/ISSUE_TEMPLATE`
  - List labels: `gh api repos/<owner>/<repo>/labels?per_page=100` (filter client-side if needed)

###############################################################################
# METADATA ZONE (Non-Executable, Informational Only)
#
# - Content in this section MUST NOT override or weaken any EXECUTION ZONE rules.
# - This section provides non-normative guidance, traceability, and maintenance notes.
###############################################################################

## Skill Changelog

### v1.0
- Initial extraction from real TiDB PR review practice.
- Established incremental review as the default behavior.
- Enforced human-in-the-loop control for all GitHub interactions.

### v1.1
- Refined scope defaulting to avoid misclassifying first-round reviews.
- Strengthened evidence collection with artifact-first and failpoint-safe testing guidance.
- Improved verification guidance for reproducibility and cleanup.

### v1.2
- Added practical `gh` fallback notes (API-first) for environments with missing subcommands or network timeouts.

### v1.3
- Clarified issue creation boundary and added reusable guidance for deriving issues from reviews.

### v1.4
- Added Severity/Impact classification and incremental summary requirements.
- Split verification into minimal-required vs recommended-optional.
- Added contract/behavior change checklist for compatibility-critical PRs.

## Rule Justifications (Non-Normative)

The following notes justify **new or refined rules** introduced since v1.1 for traceability. They MUST NOT be treated as additional constraints beyond what is stated in the EXECUTION ZONE.

- PRR-SKILL-001 (Execution / 1.1 Scope defaulting)
  - Why: If a session has no prior reviewed baseline, defaulting to incremental review can unintentionally narrow scope and miss key changes; the refined rule preserves “incremental by default” only when a baseline exists.
- PRR-SKILL-006 (Execution / 1.2 Evidence: artifact-first)
  - Why: Review environments may already provide PR diffs/summaries; using them first reduces redundant fetching and improves speed and reliability under network/tooling constraints.
- PRR-SKILL-002 (Execution / 1.2 Evidence: failpoint-safe tests)
  - Why: In TiDB, some packages rely on failpoints; running tests without proper enable/disable can produce false results or leave the workspace in a modified state.
- PRR-SKILL-008 (Execution / 1.4 Verification: reproducibility + cleanup)
  - Why: Review conclusions are more actionable when verification steps are copy-pastable; any workspace-state changes (e.g. failpoint toggling) should be cleaned up to avoid impacting subsequent work.
- PRR-SKILL-009 (Execution / 3.2 gh failure modes: API fallback)
  - Why: In some environments, `gh` subcommands may be missing and raw URL fetches may time out; `gh api` plus local artifacts provide a robust fallback to keep review workflows unblocked.
- PRR-SKILL-010 (Execution / 3 GitHub interaction boundary: issue creation)
  - Why: Creating issues has side effects similar to posting review comments; requiring explicit user instruction and following templates prevents accidental actions and improves consistency.

## Review Heuristics & Pitfalls (Non-Normative)

The following heuristics are reusable reviewer experience notes. They MUST NOT override any EXECUTION ZONE rules.

- Scope control for “diff-external” findings
  - If you discover a likely issue outside the PR diff, avoid scope creep by default:
    - Prefer reporting it as **Suggestion (Must fix: No)** and recommend tracking via a separate issue/PR.
    - Escalate to **Must fix: Yes** only if the issue directly causes a regression/security risk/contract break introduced or surfaced by this PR.
- Contract consistency checks for config/variable/rule changes
  - When reviewing system variables, rules, parsers, or user-facing configuration:
    - **Units**: ensure docs/examples/parsing/matching use consistent units (avoid “same name, different unit” ambiguity).
    - **Type alignment**: parsing types and comparison types must align; mismatches can silently make conditions never match.
    - **Range strategy**: clarify whether min/max is enforced (parse-time rejection) or documented as guidance; ensure examples do not imply a different unit/range.
- Range validation strictness guidance
  - Treat “add per-field min/max validation” as **Optimization/Suggestion (non-blocking)** by default.
  - Promote to **Must fix** only when unbounded values can realistically cause stability/security risks (e.g. resource exhaustion, log flooding) or backward-compatibility breaks.
- Tooling fallback strategy
  - If an expected GitHub CLI subcommand or feature is unavailable, fall back to `gh api` or local artifacts instead of blocking progress (e.g., templates/labels via API, diff via artifacts).

## Issue Derivation from Review (Non-Normative)

- When to propose a new issue (instead of blocking the PR)
  - Use a separate issue when the finding is outside the PR diff/scope but worth tracking
  - Keep PR review item non-blocking unless it directly causes regression/security/contract break

- Issue content checklist (reusable structure)
  - Link back to the source PR (context)
  - Minimal reproducible steps / proof (code snippet, config, or exact commands)
  - Expected vs actual behavior
  - Impact (who/what is affected)
  - Root cause hypothesis (if evidence-backed; avoid guessing)
  - Proposed fix directions (options A/B)
  - Test plan (what test to add / run)
  - Suggested labels (if label permission/availability is uncertain, include as plain text)

- Template/label handling
  - Prefer using repo issue templates verbatim (don’t invent headings that conflict)
  - If label tooling is missing, still file the issue and include “Suggested labels: …” in the body

