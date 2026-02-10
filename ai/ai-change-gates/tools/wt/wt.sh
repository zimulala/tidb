#!/usr/bin/env bash
set -euo pipefail

# wt: worktree helper for tidb + iTerm + codex
#
# Commands:
#   wt open <branch> [--codex] [--cursor]
#   wt pr   <prNum>  [--codex] [--cursor]
#
# Defaults:
#   remote = zimuxia
#   remote namespace = zimuxia/<branch>
#   worktree root = ../tidb-wt (relative to repo root parent)
#
# Behavior:
# - If remote branch does not exist: create local branch from current HEAD and push to remote
# - If --codex: open a new iTerm tab and run "cd <worktree> && codex"

REMOTE="${WT_REMOTE:-zimuxia}"
REMOTE_NS="${WT_REMOTE_NS:-zimuxia}"   # remote branch namespace: zimuxia/<branch>
WT_ROOT="${WT_ROOT:-}"                # auto computed if empty

usage() {
  cat <<EOF
Usage:
  wt open <branch> [--codex] [--cursor]
  wt pr   <prNum>  [--codex] [--cursor]

Env overrides:
  WT_REMOTE=zimuxia
  WT_REMOTE_NS=zimuxia
  WT_ROOT=/abs/path/to/tidb-wt
EOF
  exit 2
}

die(){ echo "ERROR: $*" >&2; exit 1; }

need_repo() {
  git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "run inside a git repo"
}

repo_root() { git rev-parse --show-toplevel; }

default_wt_root() {
  local root; root="$(repo_root)"
  local parent; parent="$(cd "${root%/*}" && pwd)"
  echo "$parent/tidb-wt"
}

ensure_remote() {
  local root; root="$(repo_root)"
  git -C "$root" remote get-url "$REMOTE" >/dev/null 2>&1 || die "remote '$REMOTE' not found (git remote -v)"
}

wt_path_for() {
  local name="$1"
  [[ -n "${WT_ROOT}" ]] || WT_ROOT="$(default_wt_root)"
  echo "${WT_ROOT}/${name}"
}

init_run_dirs() {
  local wt="$1"
  mkdir -p "$wt/_run/"{logs,tmp,data,ai}
  cat > "$wt/AI_WORKSPACE.md" <<EOF
# AI Workspace
- worktree: $(basename "$wt")
- run_dir:  $wt/_run
- logs:     $wt/_run/logs
- tmp:      $wt/_run/tmp
- data:     $wt/_run/data
- ai:       $wt/_run/ai
EOF
}

open_iterm_and_run() {
  local cmd="$1"
  # iTerm2 AppleScript: create a new tab and write command to session. :contentReference[oaicite:0]{index=0}
  osascript <<OSA
tell application "iTerm2"
  activate
  if (count of windows) = 0 then
    create window with default profile
  end if
  tell current window
    set newTab to (create tab with default profile)
    tell current session of newTab
      write text "$cmd"
    end tell
  end tell
end tell
OSA
}

open_cursor() {
  local path="$1"
  # Best-effort: open folder in Cursor (Cursor is a macOS app; enterprises may deploy a CLI too). :contentReference[oaicite:1]{index=1}
  # If Cursor isn't installed / app name differs, fallback to `open <path>`.
  if open -na "Cursor" --args "$path" >/dev/null 2>&1; then
    return 0
  fi
  if open -a "Cursor" "$path" >/dev/null 2>&1; then
    return 0
  fi
  open "$path" >/dev/null 2>&1 || true
}

ensure_branch_worktree() {
  local branch="$1"
  local wt_name="$2"            # usually same as branch, but PR uses pr-123
  local want_codex="$3"
  local want_cursor="$4"

  need_repo
  ensure_remote

  local root; root="$(repo_root)"
  [[ -n "${WT_ROOT}" ]] || WT_ROOT="$(default_wt_root)"
  mkdir -p "$WT_ROOT"

  local worktree; worktree="$(wt_path_for "$wt_name")"
  local local_branch="$wt_name"

  local remote_branch="${REMOTE_NS}/${branch}"     # zimuxia/<branch>
  local remote_ref="${REMOTE}/${remote_branch}"    # zimuxia/zimuxia/<branch>

  echo "==> Repo: $root"
  echo "==> Current branch (kept as-is): $(git -C "$root" branch --show-current || true)"
  echo "==> Worktree: $worktree"
  echo "==> Local branch: $local_branch"
  echo "==> Remote ref: $remote_ref"
  echo

  echo "==> Fetching $REMOTE ..."
  git -C "$root" fetch "$REMOTE" --prune

  if git -C "$root" show-ref --verify --quiet "refs/remotes/$remote_ref"; then
    echo "==> Remote branch exists; updating local branch '$local_branch' -> $remote_ref"
    git -C "$root" branch -f "$local_branch" "$remote_ref"
  else
    echo "==> Remote branch missing; creating local branch '$local_branch' from HEAD and pushing to $REMOTE as $remote_branch"
    git -C "$root" branch -f "$local_branch" HEAD
    git -C "$root" push -u "$REMOTE" "$local_branch:$remote_branch"
  fi

  if [[ ! -d "$worktree" ]]; then
    echo "==> Adding worktree ..."
    git -C "$root" worktree add "$worktree" "$local_branch"
  else
    echo "==> Worktree already exists: $worktree"
  fi

  init_run_dirs "$worktree"

  if [[ "$want_cursor" == "1" ]]; then
    echo "==> Opening in Cursor..."
    open_cursor "$worktree"
  fi

  if [[ "$want_codex" == "1" ]]; then
    echo "==> Starting codex in new iTerm tab..."
    # Use a single line command; keep it simple.
    open_iterm_and_run "cd '$worktree' && echo '[wt] ' \$(pwd) && echo '[branch] ' \$(git branch --show-current 2>/dev/null) && codex"
  fi

  echo "==> OK."
}

cmd="${1:-}"; shift || true
[[ -n "$cmd" ]] || usage

case "$cmd" in
  open)
    branch="${1:-}"; shift || true
    [[ -n "$branch" ]] || usage

    want_codex=0
    want_cursor=0
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --codex) want_codex=1 ;;
        --cursor) want_cursor=1 ;;
        *) die "unknown flag: $1" ;;
      esac
      shift
    done

    # open <branch> => remote branch zimuxia/<branch>, local branch <branch>, worktree name <branch>
    ensure_branch_worktree "$branch" "$branch" "$want_codex" "$want_cursor"
    ;;

  pr)
    pr="${1:-}"; shift || true
    [[ -n "$pr" ]] || usage
    [[ "$pr" =~ ^[0-9]+$ ]] || die "prNum must be a number"

    want_codex=0
    want_cursor=0
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --codex) want_codex=1 ;;
        --cursor) want_cursor=1 ;;
        *) die "unknown flag: $1" ;;
      esac
      shift
    done

    need_repo
    ensure_remote
    command -v gh >/dev/null 2>&1 || die "gh not found; install GitHub CLI (gh)"

    root="$(repo_root)"
    wt_name="pr-$pr"

    echo "==> Checking out PR #$pr into local branch '$wt_name' ..."
    # Prefer explicit branch name; if unsupported, fallback.
    if gh pr checkout "$pr" -b "$wt_name" >/dev/null 2>&1; then
      :
    else
      gh pr checkout "$pr"
      # After checkout, rename current branch to pr-<num> (force if exists)
      cur="$(git -C "$root" branch --show-current || true)"
      [[ -n "$cur" ]] || die "failed to detect current branch after gh pr checkout"
      git -C "$root" branch -f "$wt_name" "$cur"
    fi

    # PR branches are local-only; we don’t push them by default.
    # Create worktree directly from the local branch.
    worktree="$(wt_path_for "$wt_name")"
    mkdir -p "$(dirname "$worktree")"

    if [[ ! -d "$worktree" ]]; then
      git -C "$root" worktree add "$worktree" "$wt_name"
    fi
    init_run_dirs "$worktree"

    if [[ "$want_cursor" == "1" ]]; then
      echo "==> Opening in Cursor..."
      open_cursor "$worktree"
    fi
    if [[ "$want_codex" == "1" ]]; then
      echo "==> Starting codex in new iTerm tab..."
      open_iterm_and_run "cd '$worktree' && echo '[wt] ' \$(pwd) && echo '[branch] ' \$(git branch --show-current 2>/dev/null) && codex"
    fi

    echo "==> OK: $worktree"
    ;;

  *)
    usage
    ;;
esac

