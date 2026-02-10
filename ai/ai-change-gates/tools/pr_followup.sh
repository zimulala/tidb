#!/usr/bin/env bash
set -uo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash ai/ai-change-gates/tools/pr_followup.sh --pr <number> --mode <plan|exec> [--apply] [--reviewer <user>] [--out <dir>] [--include-resolved=1]

Examples:
  bash ai/ai-change-gates/tools/pr_followup.sh --pr 66064 --mode plan
  bash ai/ai-change-gates/tools/pr_followup.sh --pr 66064 --reviewer XuHuaiyu --mode plan
  bash ai/ai-change-gates/tools/pr_followup.sh --pr 66064 --mode exec
  bash ai/ai-change-gates/tools/pr_followup.sh --pr 66064 --mode exec --apply
  bash ai/ai-change-gates/tools/pr_followup.sh --pr 66064 --mode plan --out ai/projects/topru-ai/artifacts/pr_followup
EOF
}

die() {
  echo "[pr_followup][ERROR] $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

now_iso() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

json_escape() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  s="${s//$'\r'/\\r}"
  s="${s//$'\t'/\\t}"
  printf '%s' "${s}"
}

json_quote() {
  printf '"%s"' "$(json_escape "$1")"
}

yaml_quote() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/ }"
  s="${s//$'\r'/ }"
  s="${s//$'\t'/ }"
  printf '"%s"' "${s}"
}

single_line() {
  local s="$1"
  s="${s//$'\n'/ }"
  s="${s//$'\r'/ }"
  s="${s//$'\t'/ }"
  echo "${s}" | sed -E 's/[[:space:]]+/ /g; s/^ //; s/ $//'
}

trace() {
  # trace <stage> <status> <msg> [json-details]
  local stage="$1"
  local status="$2"
  local msg="$3"
  local details="${4:-{}}"
  local t
  t="$(now_iso)"
  mkdir -p "$(dirname "${TRACE_FILE}")" >/dev/null 2>&1 || true
  echo "{\"time\":\"${t}\",\"stage\":\"${stage}\",\"status\":\"${status}\",\"msg\":$(json_quote "${msg}"),\"details\":${details}}" >> "${TRACE_FILE}" 2>/dev/null || true
}

repo_from_remote_url() {
  local url="$1"
  local trimmed
  trimmed="${url%.git}"
  if echo "${trimmed}" | grep -Eq '^git@github.com:'; then
    echo "${trimmed}" | sed -E 's#^git@github.com:##'
    return 0
  fi
  if echo "${trimmed}" | grep -Eq '^https://github.com/'; then
    echo "${trimmed}" | sed -E 's#^https://github.com/##'
    return 0
  fi
  if echo "${trimmed}" | grep -Eq '^ssh://git@github.com/'; then
    echo "${trimmed}" | sed -E 's#^ssh://git@github.com/##'
    return 0
  fi
  return 1
}

choose_action() {
  # choose_action <body> <path> <already_addressed(true|false)>
  local body_lower path="$2" already="$3"
  body_lower="$(echo "$1" | tr '[:upper:]' '[:lower:]')"
  if [[ "${already}" == "true" ]]; then
    echo "validate_noop"
    return 0
  fi
  if echo "${body_lower}" | grep -Eq '\b(test|unit test|test case|regression)\b'; then
    echo "add_or_update_tests"
    return 0
  fi
  if echo "${body_lower}" | grep -Eq '\b(doc|docs|comment|clarify|explain)\b'; then
    echo "update_docs_or_comments"
    return 0
  fi
  if echo "${body_lower}" | grep -Eq '\b(rename|naming|typo)\b'; then
    echo "rename_or_cleanup"
    return 0
  fi
  if [[ -n "${path}" ]]; then
    echo "update_code_in_path"
    return 0
  fi
  echo "manual_followup"
}

verify_cmd_for_path() {
  # verify_cmd_for_path <path>
  local path="$1"
  local dir
  if [[ "${path}" == pkg/* ]]; then
    dir="$(dirname "${path}")"
    echo "go test --tags=intest -run TestNonExistent ./${dir}/..."
    return 0
  fi
  if [[ "${path}" == tests/realtikvtest/* ]]; then
    echo "go test --tags=intest -run TestNonExistent ./tests/realtikvtest/..."
    return 0
  fi
  echo "echo \"manual verify required for ${path:-conversation}\""
}

build_next_cmd() {
  if [[ "${MODE}" == "plan" ]]; then
    local cmd="bash ai/ai-change-gates/tools/pr_followup.sh --pr ${PR_NUM} --mode exec"
    if [[ -n "${REVIEWER}" ]]; then
      cmd+=" --reviewer ${REVIEWER}"
    fi
    if [[ "${INCLUDE_RESOLVED}" == "1" ]]; then
      cmd+=" --include-resolved=1"
    fi
    if [[ -n "${OUT_BASE_ARG}" ]]; then
      cmd+=" --out ${OUT_BASE_ARG}"
    fi
    echo "${cmd}"
    return 0
  fi

  if [[ "${RESULT_WORD}" == "PASS" ]]; then
    echo "gh pr comment ${PR_NUM} --body-file ${REPLY_DRAFT_FILE}"
    return 0
  fi
  local cmd="bash ai/ai-change-gates/tools/pr_followup.sh --pr ${PR_NUM} --mode plan"
  if [[ -n "${REVIEWER}" ]]; then
    cmd+=" --reviewer ${REVIEWER}"
  fi
  if [[ "${INCLUDE_RESOLVED}" == "1" ]]; then
    cmd+=" --include-resolved=1"
  fi
  if [[ -n "${OUT_BASE_ARG}" ]]; then
    cmd+=" --out ${OUT_BASE_ARG}"
  fi
  echo "${cmd}"
}

write_manifest() {
  local head_sha patch_id cmdline env_str git_cfg_sig
  head_sha="$(git rev-parse HEAD 2>/dev/null || true)"
  patch_id="$( (git show HEAD | git patch-id --stable | awk '{print $1}') 2>/dev/null || echo UNKNOWN )"
  env_str="$(uname -s)/$(uname -m)"
  git_cfg_sig="$(cksum .git/config 2>/dev/null | awk '{print $1 ":" $2}' || true)"

  cat > "${MANIFEST_FILE}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${RUN_ID}"),
  "time": $(json_quote "$(now_iso)"),
  "repo_root": $(json_quote "${ROOT}"),
  "repo": $(json_quote "${REPO_FULL_NAME}"),
  "pr": ${PR_NUM},
  "mode": $(json_quote "${MODE}"),
  "reviewer": $(json_quote "${REVIEWER}"),
  "include_resolved": ${INCLUDE_RESOLVED},
  "apply": ${APPLY},
  "head_commit": $(json_quote "${head_sha}"),
  "head_patch_id": $(json_quote "${patch_id}"),
  "env": $(json_quote "${env_str}"),
  "command": $(json_quote "${INVOCATION}"),
  "git_config_sig": $(json_quote "${git_cfg_sig}"),
  "artifacts": {
    "comments_raw": $(json_quote "${COMMENTS_RAW_FILE}"),
    "comments_json": $(json_quote "${COMMENTS_JSON_FILE}"),
    "comments_md": $(json_quote "${COMMENTS_MD_FILE}"),
    "comment_actions_yaml": $(json_quote "${ACTIONS_FILE}"),
    "reply_draft_md": $(json_quote "${REPLY_DRAFT_FILE}"),
    "trace": $(json_quote "${TRACE_FILE}"),
    "manifest": $(json_quote "${MANIFEST_FILE}"),
    "result": $(json_quote "${RESULT_FILE}"),
    "patch": $(json_quote "${PATCH_FILE}"),
    "verify_log": $(json_quote "${VERIFY_LOG_FILE}"),
    "touched_files": $(json_quote "${TOUCHED_FILES_FILE}")
  }
}
EOF
}

write_result_json() {
  cat > "${RESULT_FILE}" <<EOF
{
  "schema_version": "v1",
  "run_id": $(json_quote "${RUN_ID}"),
  "repo": $(json_quote "${REPO_FULL_NAME}"),
  "pr": ${PR_NUM},
  "mode": $(json_quote "${MODE}"),
  "reviewer": $(json_quote "${REVIEWER}"),
  "include_resolved": ${INCLUDE_RESOLVED},
  "apply": ${APPLY},
  "result": $(json_quote "${RESULT_WORD}"),
  "reason": $(json_quote "${RESULT_REASON}"),
  "selected_total": ${SELECTED_TOTAL},
  "patch_bytes": ${PATCH_BYTES},
  "verify_rc": ${VERIFY_RC},
  "apply_rc": ${APPLY_RC},
  "next": $(json_quote "${NEXT_CMD}"),
  "artifacts": {
    "comments_raw": $(json_quote "${COMMENTS_RAW_FILE}"),
    "comments_json": $(json_quote "${COMMENTS_JSON_FILE}"),
    "comments_md": $(json_quote "${COMMENTS_MD_FILE}"),
    "comment_actions_yaml": $(json_quote "${ACTIONS_FILE}"),
    "reply_draft_md": $(json_quote "${REPLY_DRAFT_FILE}"),
    "patch": $(json_quote "${PATCH_FILE}"),
    "verify_log": $(json_quote "${VERIFY_LOG_FILE}"),
    "touched_files": $(json_quote "${TOUCHED_FILES_FILE}"),
    "trace": $(json_quote "${TRACE_FILE}"),
    "manifest": $(json_quote "${MANIFEST_FILE}"),
    "result": $(json_quote "${RESULT_FILE}")
  }
}
EOF
}

PR_NUM=""
MODE=""
REVIEWER=""
OUT_BASE_ARG=""
OUT_BASE=""
INCLUDE_RESOLVED=0
APPLY=0
REPO_ARG_IGNORED=""
INVOCATION="bash ai/ai-change-gates/tools/pr_followup.sh $*"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pr)
      PR_NUM="${2:-}"
      shift 2
      ;;
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --reviewer)
      REVIEWER="${2:-}"
      shift 2
      ;;
    --out)
      OUT_BASE_ARG="${2:-}"
      shift 2
      ;;
    --apply)
      APPLY=1
      shift
      ;;
    --include-resolved)
      INCLUDE_RESOLVED="${2:-0}"
      shift 2
      ;;
    --include-resolved=*)
      INCLUDE_RESOLVED="${1#*=}"
      shift
      ;;
    --repo)
      REPO_ARG_IGNORED="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown arg: $1"
      ;;
  esac
done

[[ -n "${PR_NUM}" ]] || die "--pr is required"
echo "${PR_NUM}" | grep -Eq '^[0-9]+$' || die "--pr must be numeric"
[[ "${MODE}" == "plan" || "${MODE}" == "exec" ]] || die "--mode must be plan or exec"
[[ "${INCLUDE_RESOLVED}" == "0" || "${INCLUDE_RESOLVED}" == "1" ]] || die "--include-resolved must be 0 or 1"

if [[ -n "${REPO_ARG_IGNORED}" ]]; then
  echo "[pr_followup][NOTICE] --repo is ignored; repo is inferred from current git path" >&2
fi

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[[ -n "${ROOT}" ]] || die "not in a git repository"
cd "${ROOT}"

need_cmd git
need_cmd gh
need_cmd jq
need_cmd rg

SHORT_HEAD="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)_pr${PR_NUM}_${SHORT_HEAD}_${MODE}"
if [[ -n "${OUT_BASE_ARG}" ]]; then
  if [[ "${OUT_BASE_ARG}" == /* ]]; then
    OUT_BASE="${OUT_BASE_ARG}"
  else
    OUT_BASE="${ROOT}/${OUT_BASE_ARG}"
  fi
else
  OUT_BASE="${ROOT}/ai/artifacts/pr_followup"
fi
ART_DIR="${OUT_BASE}/${RUN_ID}"

COMMENTS_RAW_FILE="${ART_DIR}/comments.raw.json"
COMMENTS_JSON_FILE="${ART_DIR}/comments.json"
COMMENTS_MD_FILE="${ART_DIR}/comments.md"
ACTIONS_FILE="${ART_DIR}/comment_actions.yaml"
REPLY_DRAFT_FILE="${ART_DIR}/reply_draft.md"
TRACE_FILE="${ART_DIR}/trace.jsonl"
MANIFEST_FILE="${ART_DIR}/manifest.json"
RESULT_FILE="${ART_DIR}/result.json"
PATCH_FILE="${ART_DIR}/patch.diff"
VERIFY_LOG_FILE="${ART_DIR}/verify.log"
TOUCHED_FILES_FILE="${ART_DIR}/touched_files.txt"
VERIFY_CANDIDATES_FILE="${ART_DIR}/verify_candidates.txt"

mkdir -p "${ART_DIR}" || die "failed to create output dir: ${ART_DIR}"
: > "${TRACE_FILE}"
: > "${VERIFY_CANDIDATES_FILE}"
if [[ "${MODE}" == "exec" ]]; then
  : > "${PATCH_FILE}"
  : > "${VERIFY_LOG_FILE}"
  : > "${TOUCHED_FILES_FILE}"
fi

trace "init" "ok" "start pr_followup" "{\"pr\":${PR_NUM},\"mode\":$(json_quote "${MODE}"),\"run_id\":$(json_quote "${RUN_ID}")}"

REPO_URL="$(git remote get-url origin 2>/dev/null || true)"
REPO_FULL_NAME="$(repo_from_remote_url "${REPO_URL}" || true)"
if [[ -z "${REPO_FULL_NAME}" ]]; then
  REPO_FULL_NAME="$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)"
fi
[[ -n "${REPO_FULL_NAME}" ]] || die "failed to infer GitHub repo from current path"
REPO_OWNER="${REPO_FULL_NAME%%/*}"
REPO_NAME="${REPO_FULL_NAME##*/}"
trace "detect_repo" "ok" "detected repo" "{\"repo\":$(json_quote "${REPO_FULL_NAME}")}"

THREADS_QUERY='query($owner:String!,$name:String!,$number:Int!,$cursor:String){repository(owner:$owner,name:$name){pullRequest(number:$number){reviewThreads(first:100,after:$cursor){nodes{id isResolved isOutdated path line originalLine comments(first:100){nodes{id databaseId body createdAt url author{login}}}}pageInfo{hasNextPage endCursor}}}}}'
ISSUE_COMMENTS_QUERY='query($owner:String!,$name:String!,$number:Int!,$cursor:String){repository(owner:$owner,name:$name){pullRequest(number:$number){comments(first:100,after:$cursor){nodes{id databaseId body createdAt url author{login}}pageInfo{hasNextPage endCursor}}}}}'

THREADS_ACC_FILE="${ART_DIR}/.threads.acc.json"
COMMENTS_ACC_FILE="${ART_DIR}/.comments.acc.json"
THREADS_NODE_FILE="${ART_DIR}/.threads.node.json"
COMMENTS_NODE_FILE="${ART_DIR}/.comments.node.json"
: > "${THREADS_ACC_FILE}"
: > "${COMMENTS_ACC_FILE}"
echo "[]" > "${THREADS_ACC_FILE}"
echo "[]" > "${COMMENTS_ACC_FILE}"

trace "fetch" "ok" "fetch review threads via GraphQL"
cursor=""
while true; do
  if [[ -n "${cursor}" ]]; then
    resp="$(gh api graphql -f query="${THREADS_QUERY}" -F owner="${REPO_OWNER}" -F name="${REPO_NAME}" -F number="${PR_NUM}" -F cursor="${cursor}" 2>/dev/null)"
  else
    resp="$(gh api graphql -f query="${THREADS_QUERY}" -F owner="${REPO_OWNER}" -F name="${REPO_NAME}" -F number="${PR_NUM}" 2>/dev/null)"
  fi
  rc=$?
  if [[ ${rc} -ne 0 ]]; then
    trace "fetch" "error" "failed to fetch review threads" "{\"rc\":${rc}}"
    die "failed to fetch review threads for PR ${PR_NUM} (repo ${REPO_FULL_NAME})"
  fi
  echo "${resp}" | jq '.data.repository.pullRequest.reviewThreads.nodes // []' > "${THREADS_NODE_FILE}" || die "invalid threads response JSON"
  jq -s '.[0] + .[1]' "${THREADS_ACC_FILE}" "${THREADS_NODE_FILE}" > "${THREADS_ACC_FILE}.new" || die "failed to merge thread pages"
  mv "${THREADS_ACC_FILE}.new" "${THREADS_ACC_FILE}"
  has_next="$(echo "${resp}" | jq -r '.data.repository.pullRequest.reviewThreads.pageInfo.hasNextPage // false')"
  cursor="$(echo "${resp}" | jq -r '.data.repository.pullRequest.reviewThreads.pageInfo.endCursor // empty')"
  [[ "${has_next}" == "true" ]] || break
  [[ -n "${cursor}" ]] || break
done

trace "fetch" "ok" "fetch issue comments via GraphQL"
cursor=""
while true; do
  if [[ -n "${cursor}" ]]; then
    resp="$(gh api graphql -f query="${ISSUE_COMMENTS_QUERY}" -F owner="${REPO_OWNER}" -F name="${REPO_NAME}" -F number="${PR_NUM}" -F cursor="${cursor}" 2>/dev/null)"
  else
    resp="$(gh api graphql -f query="${ISSUE_COMMENTS_QUERY}" -F owner="${REPO_OWNER}" -F name="${REPO_NAME}" -F number="${PR_NUM}" 2>/dev/null)"
  fi
  rc=$?
  if [[ ${rc} -ne 0 ]]; then
    trace "fetch" "error" "failed to fetch issue comments" "{\"rc\":${rc}}"
    die "failed to fetch issue comments for PR ${PR_NUM} (repo ${REPO_FULL_NAME})"
  fi
  echo "${resp}" | jq '.data.repository.pullRequest.comments.nodes // []' > "${COMMENTS_NODE_FILE}" || die "invalid issue comments response JSON"
  jq -s '.[0] + .[1]' "${COMMENTS_ACC_FILE}" "${COMMENTS_NODE_FILE}" > "${COMMENTS_ACC_FILE}.new" || die "failed to merge issue comment pages"
  mv "${COMMENTS_ACC_FILE}.new" "${COMMENTS_ACC_FILE}"
  has_next="$(echo "${resp}" | jq -r '.data.repository.pullRequest.comments.pageInfo.hasNextPage // false')"
  cursor="$(echo "${resp}" | jq -r '.data.repository.pullRequest.comments.pageInfo.endCursor // empty')"
  [[ "${has_next}" == "true" ]] || break
  [[ -n "${cursor}" ]] || break
done

PR_META_RAW="$(gh api "repos/${REPO_FULL_NAME}/pulls/${PR_NUM}" 2>/dev/null || true)"
if [[ -n "${PR_META_RAW}" ]]; then
  PR_URL="$(echo "${PR_META_RAW}" | jq -r '.html_url // ""')"
  PR_TITLE="$(echo "${PR_META_RAW}" | jq -r '.title // ""')"
  PR_BASE_SHA="$(echo "${PR_META_RAW}" | jq -r '.base.sha // ""')"
  PR_HEAD_SHA="$(echo "${PR_META_RAW}" | jq -r '.head.sha // ""')"
else
  PR_URL=""
  PR_TITLE=""
  PR_BASE_SHA=""
  PR_HEAD_SHA=""
fi

cat > "${COMMENTS_RAW_FILE}" <<EOF
{
  "schema_version": "v1",
  "fetched_at": $(json_quote "$(now_iso)"),
  "repo": $(json_quote "${REPO_FULL_NAME}"),
  "pr": ${PR_NUM},
  "include_resolved": ${INCLUDE_RESOLVED},
  "reviewer": $(json_quote "${REVIEWER}"),
  "pr_meta": {
    "url": $(json_quote "${PR_URL}"),
    "title": $(json_quote "${PR_TITLE}"),
    "base": $(json_quote "${PR_BASE_SHA}"),
    "head": $(json_quote "${PR_HEAD_SHA}")
  },
  "review_threads": $(cat "${THREADS_ACC_FILE}"),
  "issue_comments": $(cat "${COMMENTS_ACC_FILE}")
}
EOF
trace "fetch" "ok" "wrote comments.raw.json" "{\"path\":$(json_quote "${COMMENTS_RAW_FILE}")}"

THREADS_NORM_FILE="${ART_DIR}/.threads.norm.json"
ISSUE_NORM_FILE="${ART_DIR}/.issue.norm.json"

jq -c --arg reviewer "${REVIEWER}" --argjson include_resolved "${INCLUDE_RESOLVED}" '
[
  .[] as $t
  | ($t.comments.nodes // []) as $cs
  | ($cs | map(.author.login // "") | map(select(. != "")) | unique) as $authors
  | {
      source: "thread",
      id: ($t.id | tostring),
      thread_id: ($t.id | tostring),
      resolved: ($t.isResolved // false),
      outdated: ($t.isOutdated // false),
      path: ($t.path // ""),
      line: ($t.line // $t.originalLine // null),
      url: (($cs | last | .url) // ""),
      author: (($cs | last | .author.login) // ""),
      created_at: (($cs | first | .createdAt) // ""),
      body: (($cs | last | .body) // ""),
      comments: ($cs | map({
        id: ((.databaseId // .id) | tostring),
        author: (.author.login // ""),
        body: (.body // ""),
        created_at: (.createdAt // ""),
        url: (.url // "")
      })),
      authors: $authors
    }
  | .reviewer_match = (if $reviewer == "" then true else (.authors | index($reviewer) != null) end)
  | .resolved_match = (if $include_resolved == 1 then true else (.resolved | not) end)
  | select(.reviewer_match and .resolved_match)
  | del(.reviewer_match, .resolved_match)
]
' "${THREADS_ACC_FILE}" > "${THREADS_NORM_FILE}" || die "failed to normalize threads"

jq -c --arg reviewer "${REVIEWER}" '
[
  .[]
  | {
      source: "issue_comment",
      id: (.id | tostring),
      comment_id: ((.databaseId // .id) | tostring),
      author: (.author.login // ""),
      path: "",
      line: null,
      resolved: false,
      created_at: (.createdAt // ""),
      url: (.url // ""),
      body: (.body // ""),
      actionable: ((.body // "") | test("(suggest|fix|please|should|consider|could you|can you|need to|required|nit|typo|rename|refactor|optimi[sz]e)"; "i")),
      already_addressed: ((.body // "") | test("(^|\\b)(done|fixed|resolved|addressed|handled|updated|already)\\b"; "i"))
    }
  | .reviewer_match = (if $reviewer == "" then true else (.author == $reviewer) end)
  | select(.reviewer_match and .actionable)
  | del(.reviewer_match)
]
' "${COMMENTS_ACC_FILE}" > "${ISSUE_NORM_FILE}" || die "failed to normalize issue comments"

cat > "${COMMENTS_JSON_FILE}" <<EOF
{
  "schema_version": "v1",
  "generated_at": $(json_quote "$(now_iso)"),
  "repo": $(json_quote "${REPO_FULL_NAME}"),
  "pr": ${PR_NUM},
  "reviewer_filter": $(json_quote "${REVIEWER}"),
  "include_resolved": ${INCLUDE_RESOLVED},
  "threads_total_raw": $(jq 'length' "${THREADS_ACC_FILE}"),
  "threads_selected": $(jq 'length' "${THREADS_NORM_FILE}"),
  "issue_comments_total_raw": $(jq 'length' "${COMMENTS_ACC_FILE}"),
  "issue_comments_selected": $(jq 'length' "${ISSUE_NORM_FILE}"),
  "threads": $(cat "${THREADS_NORM_FILE}"),
  "issue_comments": $(cat "${ISSUE_NORM_FILE}"),
  "selected": $(jq -s '.[0] + .[1]' "${THREADS_NORM_FILE}" "${ISSUE_NORM_FILE}")
}
EOF
trace "classify" "ok" "wrote comments.json" "{\"path\":$(json_quote "${COMMENTS_JSON_FILE}")}"

THREADS_SELECTED="$(jq 'length' "${THREADS_NORM_FILE}")"
ISSUE_SELECTED="$(jq 'length' "${ISSUE_NORM_FILE}")"
SELECTED_TOTAL="$(jq '.selected | length' "${COMMENTS_JSON_FILE}")"

{
  echo "# PR Follow-up Summary"
  echo
  echo "- run_id: \`${RUN_ID}\`"
  echo "- repo: \`${REPO_FULL_NAME}\`"
  echo "- pr: #${PR_NUM}"
  if [[ -n "${PR_URL}" ]]; then
    echo "- pr_url: ${PR_URL}"
  fi
  echo "- mode: \`${MODE}\`"
  echo "- reviewer_filter: \`${REVIEWER:-<all-reviewers>}\`"
  echo "- include_resolved: \`${INCLUDE_RESOLVED}\`"
  echo "- selected_threads: ${THREADS_SELECTED}"
  echo "- selected_issue_comments: ${ISSUE_SELECTED}"
  echo "- selected_total: ${SELECTED_TOTAL}"
  echo
  echo "## Unresolved Review Threads"
  if [[ "${THREADS_SELECTED}" -eq 0 ]]; then
    echo "- none"
  else
    while IFS= read -r item; do
      path="$(echo "${item}" | jq -r '.path // ""')"
      line="$(echo "${item}" | jq -r '.line // ""')"
      url="$(echo "${item}" | jq -r '.url // ""')"
      author="$(echo "${item}" | jq -r '.author // ""')"
      resolved="$(echo "${item}" | jq -r '.resolved')"
      body="$(echo "${item}" | jq -r '.body // ""' | head -n 1)"
      body="$(single_line "${body}")"
      echo "- [thread] ${author} @ ${path:-<conversation>}${line:+:${line}} resolved=${resolved} url=${url}"
      echo "  - ${body}"
    done < <(jq -c '.[]' "${THREADS_NORM_FILE}")
  fi
  echo
  echo "## Actionable Issue Comments (best-effort)"
  if [[ "${ISSUE_SELECTED}" -eq 0 ]]; then
    echo "- none"
  else
    while IFS= read -r item; do
      author="$(echo "${item}" | jq -r '.author // ""')"
      url="$(echo "${item}" | jq -r '.url // ""')"
      already="$(echo "${item}" | jq -r '.already_addressed')"
      body="$(echo "${item}" | jq -r '.body // ""' | head -n 1)"
      body="$(single_line "${body}")"
      echo "- [comment] ${author} already_addressed=${already} url=${url}"
      echo "  - ${body}"
    done < <(jq -c '.[]' "${ISSUE_NORM_FILE}")
  fi
} > "${COMMENTS_MD_FILE}"

echo "schema_version: v1" > "${ACTIONS_FILE}"
echo "run_id: ${RUN_ID}" >> "${ACTIONS_FILE}"
echo "repo: ${REPO_FULL_NAME}" >> "${ACTIONS_FILE}"
echo "pr: ${PR_NUM}" >> "${ACTIONS_FILE}"
echo "mode: ${MODE}" >> "${ACTIONS_FILE}"
echo "reviewer_filter: ${REVIEWER:-<all-reviewers>}" >> "${ACTIONS_FILE}"
echo "actions:" >> "${ACTIONS_FILE}"

{
  echo "# PR Follow-up Reply Draft"
  echo
  echo "Thanks for the review. I prepared a follow-up plan for unresolved/actionable comments."
  echo
  echo "## Planned Actions"
} > "${REPLY_DRAFT_FILE}"

action_idx=0
while IFS= read -r item; do
  action_idx=$((action_idx + 1))
  action_id="A${action_idx}"
  src="$(echo "${item}" | jq -r '.source')"
  item_id="$(echo "${item}" | jq -r '.id')"
  author="$(echo "${item}" | jq -r '.author // ""')"
  path="$(echo "${item}" | jq -r '.path // ""')"
  line="$(echo "${item}" | jq -r '.line // empty')"
  url="$(echo "${item}" | jq -r '.url // ""')"
  body="$(echo "${item}" | jq -r '.body // ""')"
  already="$(echo "${item}" | jq -r '.already_addressed // false')"

  chosen_action="$(choose_action "${body}" "${path}" "${already}")"
  verify_min="$(verify_cmd_for_path "${path}")"
  reason="from ${src} ${item_id}; author=${author}; location=${path:-conversation}${line:+:${line}}"
  summary="$(single_line "$(echo "${body}" | head -n 1)")"

  echo "  - id: ${action_id}" >> "${ACTIONS_FILE}"
  echo "    source: ${src}" >> "${ACTIONS_FILE}"
  echo "    source_id: ${item_id}" >> "${ACTIONS_FILE}"
  echo "    url: $(yaml_quote "${url}")" >> "${ACTIONS_FILE}"
  echo "    location: $(yaml_quote "${path:-<conversation>}${line:+:${line}}")" >> "${ACTIONS_FILE}"
  echo "    author: $(yaml_quote "${author}")" >> "${ACTIONS_FILE}"
  echo "    chosen_action: ${chosen_action}" >> "${ACTIONS_FILE}"
  echo "    reason: $(yaml_quote "${reason}")" >> "${ACTIONS_FILE}"
  echo "    verify_min: $(yaml_quote "${verify_min}")" >> "${ACTIONS_FILE}"
  echo "    already_addressed: ${already}" >> "${ACTIONS_FILE}"

  if echo "${verify_min}" | grep -Eq '^go test '; then
    if ! grep -Fxq "${verify_min}" "${VERIFY_CANDIDATES_FILE}"; then
      echo "${verify_min}" >> "${VERIFY_CANDIDATES_FILE}"
    fi
  fi

  {
    echo "- [ ] ${action_id} (${src}) ${author} @ ${path:-<conversation>}${line:+:${line}}"
    echo "  - context: ${summary}"
    echo "  - action: \`${chosen_action}\`"
    echo "  - verify: \`${verify_min}\`"
    echo "  - link: ${url}"
  } >> "${REPLY_DRAFT_FILE}"
done < <(jq -c '.selected[]' "${COMMENTS_JSON_FILE}")

if [[ "${SELECTED_TOTAL}" -eq 0 ]]; then
  echo "  - id: A0" >> "${ACTIONS_FILE}"
  echo "    source: none" >> "${ACTIONS_FILE}"
  echo "    source_id: none" >> "${ACTIONS_FILE}"
  echo "    url: \"\"" >> "${ACTIONS_FILE}"
  echo "    location: \"<none>\"" >> "${ACTIONS_FILE}"
  echo "    author: \"\"" >> "${ACTIONS_FILE}"
  echo "    chosen_action: no_action" >> "${ACTIONS_FILE}"
  echo "    reason: \"No unresolved/actionable comments selected.\"" >> "${ACTIONS_FILE}"
  echo "    verify_min: \"echo \\\"no verify needed\\\"\"" >> "${ACTIONS_FILE}"
  echo "    already_addressed: false" >> "${ACTIONS_FILE}"
  {
    echo
    echo "No unresolved/actionable comments were selected."
  } >> "${REPLY_DRAFT_FILE}"
fi

trace "plan" "ok" "wrote planning artifacts" "{\"comments_md\":$(json_quote "${COMMENTS_MD_FILE}"),\"reply_draft\":$(json_quote "${REPLY_DRAFT_FILE}")}"

PATCH_BYTES=0
VERIFY_RC=0
APPLY_RC=0
RESULT_WORD="PASS"
RESULT_REASON=""
NEXT_CMD=""

if [[ "${MODE}" == "exec" ]]; then
  trace "exec" "ok" "start exec stage"
  git diff --binary HEAD > "${PATCH_FILE}" || die "failed to build patch.diff"
  PATCH_BYTES="$(wc -c < "${PATCH_FILE}" | tr -d ' ')"

  grep -E '^\+\+\+ b/' "${PATCH_FILE}" | sed 's#^\+\+\+ b/##' | grep -v '^/dev/null$' | sort -u > "${TOUCHED_FILES_FILE}" || true
  if [[ ! -s "${TOUCHED_FILES_FILE}" ]]; then
    echo "no changes" > "${TOUCHED_FILES_FILE}"
  fi

  {
    echo "[verify] pr_followup exec"
    echo "run_id=${RUN_ID}"
    echo "patch_bytes=${PATCH_BYTES}"
  } > "${VERIFY_LOG_FILE}"

  verify_cmd=""
  if [[ -s "${VERIFY_CANDIDATES_FILE}" ]]; then
    verify_cmd="$(head -n 1 "${VERIFY_CANDIDATES_FILE}")"
  else
    touched_first="$(head -n 1 "${TOUCHED_FILES_FILE}")"
    if [[ "${touched_first}" == pkg/* ]]; then
      verify_cmd="$(verify_cmd_for_path "${touched_first}")"
    fi
  fi

  if [[ -n "${verify_cmd}" ]] && echo "${verify_cmd}" | grep -Eq '^go test '; then
    mkdir -p "${ART_DIR}/.gocache" "${ART_DIR}/.gotmp" || true
    {
      echo "[verify] run: ${verify_cmd}"
      echo "[verify] env: GOCACHE=${ART_DIR}/.gocache GOTMPDIR=${ART_DIR}/.gotmp"
      set +e
      GOCACHE="${ART_DIR}/.gocache" GOTMPDIR="${ART_DIR}/.gotmp" bash -lc "${verify_cmd}"
      VERIFY_RC=$?
      set -e
      echo "[verify] rc=${VERIFY_RC}"
    } >> "${VERIFY_LOG_FILE}" 2>&1
  else
    echo "[verify] no package-scoped go test command derived; skipped" >> "${VERIFY_LOG_FILE}"
    VERIFY_RC=0
  fi

  if [[ "${APPLY}" == "1" ]]; then
    cfg_before="$(cksum .git/config 2>/dev/null | awk '{print $1 ":" $2}' || true)"
    if rg -q '^diff --git ' "${PATCH_FILE}"; then
      invalid_path="$(awk '/^\+\+\+ b\//{print substr($0, 7)}' "${PATCH_FILE}" | grep -E '^/|(^|/)\.\.(/|$)' | head -n 1 || true)"
      if [[ -n "${invalid_path}" ]]; then
        APPLY_RC=1
        echo "[safety] patch contains out-of-repo path: ${invalid_path}" >> "${VERIFY_LOG_FILE}"
      elif git diff --quiet && git diff --cached --quiet; then
        if ! git apply --check "${PATCH_FILE}" >> "${VERIFY_LOG_FILE}" 2>&1; then
          APPLY_RC=1
          echo "[apply] git apply --check failed" >> "${VERIFY_LOG_FILE}"
        else
          set +e
          git apply "${PATCH_FILE}" >> "${VERIFY_LOG_FILE}" 2>&1
          APPLY_RC=$?
          set -e
        fi
      else
        APPLY_RC=0
        echo "[apply] worktree already has local changes; treat as already-applied and skip git apply" >> "${VERIFY_LOG_FILE}"
      fi
    else
      APPLY_RC=0
      echo "[apply] patch.diff has no changes; nothing to apply" >> "${VERIFY_LOG_FILE}"
    fi
    cfg_after="$(cksum .git/config 2>/dev/null | awk '{print $1 ":" $2}' || true)"
    if [[ "${cfg_before}" != "${cfg_after}" ]]; then
      APPLY_RC=1
      echo "[safety] .git/config changed unexpectedly" >> "${VERIFY_LOG_FILE}"
    fi
  fi

  if [[ "${PATCH_BYTES}" == "0" ]]; then
    echo "[exec] no changes in patch.diff" >> "${VERIFY_LOG_FILE}"
  fi
  trace "exec" "ok" "exec artifacts generated" "{\"patch\":$(json_quote "${PATCH_FILE}"),\"verify\":$(json_quote "${VERIFY_LOG_FILE}")}"
fi

if [[ "${MODE}" == "plan" ]]; then
  RESULT_WORD="PASS"
  RESULT_REASON="plan artifacts generated"
else
  if [[ "${VERIFY_RC}" != "0" || "${APPLY_RC}" != "0" ]]; then
    RESULT_WORD="FAIL"
    RESULT_REASON="verify/apply failed (verify_rc=${VERIFY_RC}, apply_rc=${APPLY_RC})"
  elif [[ "${SELECTED_TOTAL}" -gt 0 && "${PATCH_BYTES}" == "0" ]]; then
    RESULT_WORD="FAIL"
    RESULT_REASON="selected comments exist but patch.diff is empty"
  elif [[ "${SELECTED_TOTAL}" -eq 0 && "${PATCH_BYTES}" == "0" ]]; then
    RESULT_WORD="PASS"
    RESULT_REASON="no selected comments and no patch change"
  else
    RESULT_WORD="PASS"
    RESULT_REASON="verify passed"
  fi
fi

NEXT_CMD="$(build_next_cmd)"
write_manifest
write_result_json

trace "finalize" "ok" "finish pr_followup" "{\"result\":$(json_quote "${RESULT_WORD}"),\"reason\":$(json_quote "${RESULT_REASON}"),\"next\":$(json_quote "${NEXT_CMD}")}"

echo "RESULT=${RESULT_WORD} PR=${PR_NUM} MODE=${MODE} RUN_ID=${RUN_ID}"
echo "NEXT=${NEXT_CMD}"
echo "DETAILS=comments_md=${COMMENTS_MD_FILE} reply_draft=${REPLY_DRAFT_FILE} patch=${PATCH_FILE:-<none>} verify=${VERIFY_LOG_FILE:-<none>} result=${RESULT_FILE} trace=${TRACE_FILE}"

if [[ "${RESULT_WORD}" == "PASS" ]]; then
  exit 0
fi
exit 1
