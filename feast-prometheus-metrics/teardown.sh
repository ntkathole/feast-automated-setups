#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${SCRIPT_DIR}/workspace"
PID_FILE="${WORK_DIR}/.feast_serve.pid"

CONTAINER_NAME_PROMETHEUS="feast-prometheus"
CONTAINER_NAME_GRAFANA="feast-grafana"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Stop the Feast feature server, Prometheus, and Grafana, and clean up the workspace.

Options:
  --keep-workspace    Keep the workspace directory (feature repo, logs, configs)
  --keep-containers   Keep Docker containers (stop but don't remove)
  -h, --help          Show this help message

Examples:
  # Full cleanup
  ./teardown.sh

  # Keep workspace for inspection
  ./teardown.sh --keep-workspace
EOF
    exit 0
}

KEEP_WORKSPACE=false
KEEP_CONTAINERS=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep-workspace)   KEEP_WORKSPACE=true; shift ;;
        --keep-containers)  KEEP_CONTAINERS=true; shift ;;
        -h|--help)          usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }

# ── 1. Stop Feast feature server ────────────────────────────────────
info "Stopping Feast feature server …"
if [[ -f "$PID_FILE" ]]; then
    FEAST_PID=$(cat "$PID_FILE")
    if kill -0 "$FEAST_PID" 2>/dev/null; then
        kill "$FEAST_PID" 2>/dev/null || true
        sleep 2
        kill -0 "$FEAST_PID" 2>/dev/null && kill -9 "$FEAST_PID" 2>/dev/null || true
        success "Feast serve stopped (PID ${FEAST_PID})"
    else
        warn "Feast serve was not running (PID ${FEAST_PID})"
    fi
    rm -f "$PID_FILE"
else
    warn "No PID file found — Feast serve may not have been started by setup.sh"
    # Try to find and kill any feast serve on the known port
    PIDS=$(lsof -ti :6566 2>/dev/null || true)
    if [[ -n "$PIDS" ]]; then
        echo "$PIDS" | xargs kill 2>/dev/null || true
        success "Killed process(es) on port 6566"
    fi
fi

# ── 2. Stop Docker containers ───────────────────────────────────────
info "Stopping Docker containers …"

for CONTAINER in "$CONTAINER_NAME_GRAFANA" "$CONTAINER_NAME_PROMETHEUS"; do
    if docker ps -q -f name="$CONTAINER" 2>/dev/null | grep -q .; then
        if [[ "$KEEP_CONTAINERS" == "true" ]]; then
            docker stop "$CONTAINER" > /dev/null 2>&1 || true
            success "Stopped $CONTAINER (kept)"
        else
            docker rm -f -v "$CONTAINER" > /dev/null 2>&1 || true
            success "Removed $CONTAINER"
        fi
    elif docker ps -aq -f name="$CONTAINER" 2>/dev/null | grep -q .; then
        if [[ "$KEEP_CONTAINERS" == "false" ]]; then
            docker rm -f -v "$CONTAINER" > /dev/null 2>&1 || true
            success "Removed stopped container $CONTAINER"
        fi
    else
        warn "$CONTAINER was not running"
    fi
done

# ── 3. Clean up workspace ───────────────────────────────────────────
if [[ "$KEEP_WORKSPACE" == "false" ]]; then
    info "Removing workspace …"
    rm -rf "$WORK_DIR"
    success "Workspace removed"
else
    info "Workspace kept at: $WORK_DIR"
fi

echo ""
echo -e "${GREEN}Teardown complete.${NC}"
echo ""
