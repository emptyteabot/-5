#!/usr/bin/env bash
set -euo pipefail

RUNNER="${1:-daily}"
shift || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ "${RUNNER}" != "daily" && "${RUNNER}" != "weekly" ]]; then
  echo "usage: bash scripts/start_ops_cycle.sh [daily|weekly]" >&2
  exit 2
fi

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
LOG_PATH="${LOGS_DIR}/${RUNNER}_ops_cycle_${STAMP}.log"

if [[ "${RUNNER}" == "daily" ]]; then
  RUNNER_PATH="${REPO_ROOT}/run_daily_ops_cycle.py"
else
  RUNNER_PATH="${REPO_ROOT}/run_weekly_ops_cycle.py"
fi

ARGS=("-X" "utf8" "${RUNNER_PATH}")

SKIP_DISCOVER_VALUE="${SKIP_DISCOVER:-}"
if [[ -z "${SKIP_DISCOVER_VALUE}" && "${RUNNER}" == "daily" ]]; then
  SKIP_DISCOVER_VALUE="1"
fi

if [[ "${SKIP_DISCOVER_VALUE}" == "1" ]]; then
  ARGS+=("--skip-discover")
fi
if [[ "${SKIP_AUTO_COLLECT:-}" == "1" ]]; then
  ARGS+=("--skip-auto-collect")
fi
if [[ "${SKIP_DIGEST_PDF:-}" != "1" ]]; then
  ARGS+=("--render-digest-pdf")
fi
if [[ "${SKIP_DELIVERY:-}" != "1" ]]; then
  ARGS+=("--deliver")
fi
if [[ "${RENDER_CEO:-}" == "1" ]]; then
  ARGS+=("--render-ceo")
fi

(
  cd "${REPO_ROOT}"
  "${PYTHON_EXE}" "${ARGS[@]}" "$@"
) 2>&1 | tee "${LOG_PATH}"

exit "${PIPESTATUS[0]}"
