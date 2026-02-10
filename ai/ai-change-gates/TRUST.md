# AI Skills Trust Model

## Goal
- Prevent silent project-level skill injection.
- Keep default behavior safe: global skills only.
- Allow project override only when local trust proof and local allowlist both pass.

## Default Policy
- Runtime should load global skills only (for example `~/.codex/skills`).
- Project skills are disabled by default.

## Optional Project Override
- Project override is allowed only when all checks pass:
1. Repo root contains `ai.trust` or `.ai/trust`.
2. Trust file has a non-empty token (first non-comment line).
3. The token equals repo root path or repo root SHA-256.
4. The same token exists in local allowlist.

## Local Allowlist
- File (line-based): `~/.config/ai-change-gates/trusted_roots`
  - One token per line.
  - Token can be:
    - absolute repo root path
    - SHA-256 hash of repo root path
- Or JSON file: `~/.config/ai-change-gates/trust.json`
  - Supported keys:
    - `trusted_roots` (array of strings)
    - `trusted_hashes` (array of strings)

## Tooling
- `bash ai/ai-change-gates/tools/check_trust.sh`
  - Prints decision and reason.
  - Optional `--json` for machine output.
  - Optional `--assert-project` to fail when project override is not allowed.

## Operational Rule
- Do not directly trust project skills by presence alone.
- Always run trust check before enabling project skill override in any runtime.
