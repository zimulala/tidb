# wt

`wt` is a worktree helper managed by `ai-change-gates` tool assets.

## Commands

```bash
wt open <branch> [--codex] [--cursor] [--dry-run] [-v]
wt pr <prNum> [--codex] [--cursor] [--dry-run] [-v]
wt --version
```

## Config

Create `~/.config/wt/config` from `config.example`.

Required keys:
- `REPO_ROOT`
- `DEFAULT_REMOTE`
- `BASE_BRANCH`
- `CURSOR_CMD`
- `CODEX_CMD`

Optional:
- `WT_ROOT` (default: sibling `<repo-name>-wt` directory)

Command templates support `%WORKTREE%` placeholder.

## Install

```bash
bash ai/ai-change-gates/tools/install_wt.sh
```

The installer links:
- source: `ai/ai-change-gates/tools/wt/wt`
- target: `~/workspace/bin/wt`

## Examples

```bash
wt open feature-x --dry-run -v
wt pr 12345 --dry-run
wt open feature-x --codex --cursor
```
