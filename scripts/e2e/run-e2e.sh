#!/usr/bin/env bash
# ============================================================================
# run-e2e.sh — one-shot driver for the WP9 black-box e2e suite (milestone M9).
#
#   up (build adapter) -> wait for health + readiness -> ./mvnw verify -Pe2e
#     -> collect compose logs into docs/e2e/report/ -> down -v
#
# Flags:
#   --load-profile ci|full   ThroughputLatencyE2EIT window (default: ci).
#                            ci   ~70s @ 100 msg/s + ~15s x3 burst
#                            full 10 min @ 100 msg/s + 5 min x3 burst (nfr.md)
#   --keep-up                do NOT `docker compose down` at the end (debug)
#   -h | --help
#
# Requires Docker. Run from the repo root or anywhere — it cd's to the repo root.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." >/dev/null 2>&1 && pwd)"
cd "${REPO_ROOT}"

COMPOSE_FILE="compose.e2e.yaml"
COMPOSE=(docker compose -f "${COMPOSE_FILE}")
LOAD_PROFILE="ci"
KEEP_UP=0
TS="$(date +%Y%m%d-%H%M%S)"
REPORT_DIR="docs/e2e/report"
LOG_FILE="${REPORT_DIR}/run-${TS}-compose.log"
SUMMARY_FILE="${REPORT_DIR}/run-${TS}-summary.txt"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --load-profile) LOAD_PROFILE="${2:?--load-profile needs ci|full}"; shift 2 ;;
    --load-profile=*) LOAD_PROFILE="${1#*=}"; shift ;;
    --keep-up) KEEP_UP=1; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done
if [[ "${LOAD_PROFILE}" != "ci" && "${LOAD_PROFILE}" != "full" ]]; then
  echo "--load-profile must be ci|full (got '${LOAD_PROFILE}')" >&2; exit 2
fi

mkdir -p "${REPORT_DIR}"

log()  { printf '\n\033[1;34m[e2e]\033[0m %s\n' "$*"; }
warn() { printf '\n\033[1;33m[e2e]\033[0m %s\n' "$*"; }

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not available — cannot run the e2e suite." >&2
  exit 3
fi

cleanup() {
  local rc=$?
  log "collecting compose logs -> ${LOG_FILE}"
  "${COMPOSE[@]}" logs --no-color --timestamps > "${LOG_FILE}" 2>&1 || true
  if [[ "${KEEP_UP}" -eq 1 ]]; then
    warn "--keep-up: leaving the stack running. Tear down with: ${COMPOSE[*]} down -v"
  else
    log "tearing the stack down (down -v)"
    "${COMPOSE[@]}" down -v --remove-orphans || true
  fi
  exit "${rc}"
}
trap cleanup EXIT

# --- build ---------------------------------------------------------------------
log "building the adapter image (cold build ~10 min; cached afterwards)"
"${COMPOSE[@]}" build adapter

# --- up ----------------------------------------------------------------------
log "starting the stack (up -d)"
"${COMPOSE[@]}" up -d

# --- wait for health --------------------------------------------------------
wait_healthy() {
  local deadline=$(( SECONDS + 420 ))
  local services
  services="$("${COMPOSE[@]}" config --services)"
  while (( SECONDS < deadline )); do
    local all_ok=1
    for svc in ${services}; do
      local cid state health
      cid="$("${COMPOSE[@]}" ps -q "${svc}" 2>/dev/null || true)"
      if [[ -z "${cid}" ]]; then all_ok=0; break; fi
      state="$(docker inspect -f '{{.State.Status}}' "${cid}" 2>/dev/null || echo missing)"
      health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${cid}" 2>/dev/null || echo none)"
      if [[ "${state}" != "running" ]]; then all_ok=0; break; fi
      if [[ "${health}" != "healthy" && "${health}" != "none" ]]; then all_ok=0; break; fi
    done
    if (( all_ok == 1 )); then log "all compose services healthy"; return 0; fi
    sleep 5
  done
  warn "timed out waiting for compose health; current state:"
  "${COMPOSE[@]}" ps
  return 1
}
wait_healthy

wait_readiness() {
  local deadline=$(( SECONDS + 240 ))
  while (( SECONDS < deadline )); do
    if curl -fsS "http://localhost:18080/actuator/health/readiness" 2>/dev/null | grep -q '"status":"UP"'; then
      log "adapter readiness UP"
      return 0
    fi
    sleep 3
  done
  warn "adapter readiness did not turn UP in time"
  curl -s "http://localhost:18080/actuator/health" || true
  return 1
}
wait_readiness

# --- run the suite ----------------------------------------------------------
log "running: ./mvnw verify -Pe2e -De2e.load.profile=${LOAD_PROFILE}"
set +e
./mvnw -q verify -Pe2e -De2e.load.profile="${LOAD_PROFILE}"
MVN_RC=$?
set -e

# --- summarise ------------------------------------------------------------
{
  echo "WP9 e2e run ${TS} — load profile: ${LOAD_PROFILE}"
  echo "mvn exit code: ${MVN_RC}"
  echo
  echo "=== Failsafe summary (target/failsafe-reports/*.txt) ==="
  if compgen -G "target/failsafe-reports/*E2EIT.txt" > /dev/null; then
    for f in target/failsafe-reports/*E2EIT.txt; do
      grep -H -E 'Tests run|<<< (FAIL|ERROR)|Time elapsed' "$f" | sed 's/^/  /'
    done
  else
    echo "  (no failsafe report files found)"
  fi
  echo
  if [[ -f target/failsafe-reports/failsafe-summary.xml ]]; then
    echo "=== failsafe-summary.xml ==="
    cat target/failsafe-reports/failsafe-summary.xml
  fi
} | tee "${SUMMARY_FILE}"

log "summary -> ${SUMMARY_FILE}"
[[ ${MVN_RC} -eq 0 ]] && log "e2e suite PASSED" || warn "e2e suite FAILED (exit ${MVN_RC})"
exit "${MVN_RC}"
