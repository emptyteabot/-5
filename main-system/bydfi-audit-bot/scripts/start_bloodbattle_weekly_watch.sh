#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -f "${REPO_ROOT}/.env.server" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env.server"
  set +a
fi

PYTHON_EXE="${BYDFI_OPS_PYTHON_EXE:-python3}"
LOGS_DIR="${REPO_ROOT}/output/logs"
mkdir -p "${LOGS_DIR}"
STAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_PATH="${LOGS_DIR}/bloodbattle_weekly_watch_${STAMP}.log"

(
  cd "${REPO_ROOT}"
  "${PYTHON_EXE}" -X utf8 scripts/check_bloodbattle_weekly_trigger.py "$@"
) 2>&1 | tee "${LOG_PATH}"

exit "${PIPESTATUS[0]}"
