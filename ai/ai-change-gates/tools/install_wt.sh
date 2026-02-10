#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
SRC="${ROOT}/ai/ai-change-gates/tools/wt/wt"
BIN_DIR="${HOME}/workspace/bin"
DEST="${BIN_DIR}/wt"

[[ -f "${SRC}" ]] || {
  echo "[install_wt][ERROR] source script not found: ${SRC}" >&2
  exit 1
}

mkdir -p "${BIN_DIR}"
chmod +x "${SRC}"
ln -sfn "${SRC}" "${DEST}"

echo "[install_wt] linked ${DEST} -> ${SRC}"
if [[ ":${PATH}:" != *":${BIN_DIR}:"* ]]; then
  echo "[install_wt][WARN] ${BIN_DIR} is not in PATH"
fi
