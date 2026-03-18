#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${SCRIPT_DIR}/workspace"
PID_FILE="${WORK_DIR}/.feast_serve.pid"
LOG_DIR="${WORK_DIR}/logs"

PROMETHEUS_PORT="${PROMETHEUS_PORT:-9090}"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"
METRICS_PORT=8000  # Fixed — hardcoded in feast metrics server
FEATURE_SERVER_PORT="${FEATURE_SERVER_PORT:-6566}"
TRAFFIC_DURATION="${TRAFFIC_DURATION:-60}"

CONTAINER_NAME_PROMETHEUS="feast-prometheus"
CONTAINER_NAME_GRAFANA="feast-grafana"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Set up a local Feast feature server with Prometheus metrics collection
and a Grafana dashboard for visualization.

Options:
  --feature-server-port PORT  Feast feature server port (default: ${FEATURE_SERVER_PORT})
  --prometheus-port PORT      Prometheus web UI port (default: ${PROMETHEUS_PORT})
  --grafana-port PORT         Grafana web UI port (default: ${GRAFANA_PORT})
  --traffic-duration SECS     Seconds to run traffic generator (default: ${TRAFFIC_DURATION})
  --skip-traffic              Skip traffic generation step
  --skip-grafana              Skip Grafana deployment (Prometheus only)
  -h, --help                  Show this help message

Note: The Feast metrics endpoint is always on port 8000 (hardcoded in Feast).

Prerequisites:
  - Python 3 with 'feast' installed (pip install feast)
  - Docker (for Prometheus and Grafana containers)

Examples:
  # Default setup — starts everything and generates 60s of traffic
  ./setup.sh

  # Custom ports
  ./setup.sh --prometheus-port 9091 --grafana-port 3001

  # Skip traffic generation (manual testing)
  ./setup.sh --skip-traffic
EOF
    exit 0
}

SKIP_TRAFFIC=false
SKIP_GRAFANA=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --feature-server-port) FEATURE_SERVER_PORT="$2"; shift 2 ;;
        --prometheus-port)     PROMETHEUS_PORT="$2"; shift 2 ;;
        --grafana-port)        GRAFANA_PORT="$2"; shift 2 ;;
        --traffic-duration)    TRAFFIC_DURATION="$2"; shift 2 ;;
        --skip-traffic)        SKIP_TRAFFIC=true; shift ;;
        --skip-grafana)        SKIP_GRAFANA=true; shift ;;
        -h|--help)             usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# ── Colours ──────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()    { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }

# ── 1. Prerequisites ────────────────────────────────────────────────
info "Checking prerequisites …"

command -v python3 >/dev/null 2>&1 || fail "python3 is required but not found"
python3 -c "import feast" 2>/dev/null  || fail "'feast' Python package is required — pip install feast"
command -v docker  >/dev/null 2>&1 || fail "docker is required but not found"
docker info >/dev/null 2>&1           || fail "Docker daemon is not running"

success "All prerequisites satisfied"

# On Linux, containers need --add-host to reach the host; on macOS Docker
# Desktop, host.docker.internal resolves natively.
DOCKER_HOST_FLAG=""
if [[ "$(uname -s)" == "Linux" ]]; then
    DOCKER_HOST_FLAG="--add-host=host.docker.internal:host-gateway"
fi

# ── 2. Clean previous run ───────────────────────────────────────────
if [[ -f "$PID_FILE" ]]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        warn "Killing previous feast serve (PID $OLD_PID)"
        kill "$OLD_PID" 2>/dev/null || true
        sleep 1
    fi
    rm -f "$PID_FILE"
fi

docker rm -f -v "$CONTAINER_NAME_PROMETHEUS" 2>/dev/null || true
docker rm -f -v "$CONTAINER_NAME_GRAFANA"    2>/dev/null || true

# ── 3. Create workspace ─────────────────────────────────────────────
info "Initialising Feast feature repository …"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR" "$LOG_DIR"

pushd "$WORK_DIR" > /dev/null
feast init feast_demo
cd feast_demo/feature_repo

# ── 4. Configure metrics in feature_store.yaml ──────────────────────
info "Enabling Prometheus metrics in feature_store.yaml …"

python3 - <<'PYEOF'
import yaml, pathlib

p = pathlib.Path("feature_store.yaml")
cfg = yaml.safe_load(p.read_text())

if "feature_server" not in cfg:
    cfg["feature_server"] = {}

cfg["feature_server"]["metrics"] = {
    "enabled": True,
    "resource": True,
    "request": True,
    "online_features": True,
    "push": True,
    "materialization": True,
    "freshness": True,
}

p.write_text(yaml.dump(cfg, default_flow_style=False, sort_keys=False))
PYEOF

success "feature_store.yaml updated"

# ── 5. Copy feature definitions ─────────────────────────────────────
info "Installing feature definitions …"
cp "${SCRIPT_DIR}/feature_definitions.py" ./feature_definitions.py
success "Feature definitions copied"

# ── 6. Apply & materialise ──────────────────────────────────────────
info "Running feast apply …"
feast apply > "${LOG_DIR}/feast_apply.log" 2>&1
success "feast apply completed"

info "Materialising features …"
END_TS=$(python3 -c "from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat())")
START_TS=$(python3 -c "from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc) - timedelta(hours=1)).isoformat())")
feast materialize -v driver_hourly_stats -v driver_hourly_stats_fresh "$START_TS" "$END_TS" > "${LOG_DIR}/feast_materialize.log" 2>&1
success "Materialisation completed"

# Bootstrap the write-path ODFV table in the online store.
# feast apply doesn't create the SQLite table for write_to_online_store ODFVs,
# and feast materialize fails due to entity-name vs join-key mismatch.
# Create the table directly and seed one row via the SDK.
info "Bootstrapping trip_score ODFV online store table …"
python3 -c "
import sqlite3, os
from feast import FeatureStore
import pandas as pd
from datetime import datetime, timezone

store = FeatureStore(repo_path='.')
db_path = os.path.join('data', 'online_store.db')
project = store.project

conn = sqlite3.connect(db_path)
table_name = f'{project}_trip_score'
conn.execute(
    f'CREATE TABLE IF NOT EXISTS \"{table_name}\" '
    f'(entity_key BLOB, feature_name TEXT, value BLOB, vector_value BLOB, '
    f'event_ts timestamp, created_ts timestamp, '
    f'PRIMARY KEY(entity_key, feature_name))'
)
conn.execute(
    f'CREATE INDEX IF NOT EXISTS \"{table_name}_ek\" ON \"{table_name}\" (entity_key)'
)
conn.commit()
conn.close()

df = pd.DataFrame({
    'driver_id': [1001],
    'conv_rate': [0.5],
    'acc_rate': [0.8],
    'avg_daily_trips': [10],
    'event_timestamp': [datetime.now(timezone.utc)],
    'created': [datetime.now(timezone.utc)],
})
store.write_to_online_store('trip_score', df)
" > "${LOG_DIR}/bootstrap_trip_score.log" 2>&1
success "trip_score ODFV table bootstrapped"

# ── 7. Start feature server ─────────────────────────────────────────
info "Starting Feast feature server (port ${FEATURE_SERVER_PORT}, metrics on ${METRICS_PORT}) …"
feast serve \
    --host 0.0.0.0 \
    --port "$FEATURE_SERVER_PORT" \
    --metrics \
    > "${LOG_DIR}/feast_serve.log" 2>&1 &
FEAST_PID=$!
echo "$FEAST_PID" > "$PID_FILE"

# Wait for readiness
MAX_WAIT=30
for i in $(seq 1 $MAX_WAIT); do
    if curl -sf "http://localhost:${METRICS_PORT}/metrics" > /dev/null 2>&1; then
        break
    fi
    if ! kill -0 "$FEAST_PID" 2>/dev/null; then
        fail "Feast serve exited unexpectedly. Check ${LOG_DIR}/feast_serve.log"
    fi
    sleep 1
done

if ! curl -sf "http://localhost:${METRICS_PORT}/metrics" > /dev/null 2>&1; then
    fail "Feast metrics endpoint did not become ready within ${MAX_WAIT}s"
fi
success "Feature server running (PID ${FEAST_PID})"

popd > /dev/null

# ── 8. Start Prometheus ──────────────────────────────────────────────
info "Starting Prometheus (port ${PROMETHEUS_PORT}) …"

PROM_CONFIG="${WORK_DIR}/prometheus.yml"
sed "s/host.docker.internal:8000/host.docker.internal:${METRICS_PORT}/" \
    "${SCRIPT_DIR}/prometheus.yml" > "$PROM_CONFIG"

docker run -d \
    --name "$CONTAINER_NAME_PROMETHEUS" \
    -p "${PROMETHEUS_PORT}:9090" \
    -v "${PROM_CONFIG}:/etc/prometheus/prometheus.yml:ro" \
    ${DOCKER_HOST_FLAG:+"$DOCKER_HOST_FLAG"} \
    prom/prometheus:latest \
    --config.file=/etc/prometheus/prometheus.yml \
    --web.enable-lifecycle \
    > /dev/null

# Wait for Prometheus
for i in $(seq 1 20); do
    if curl -sf "http://localhost:${PROMETHEUS_PORT}/-/ready" > /dev/null 2>&1; then break; fi
    sleep 1
done
curl -sf "http://localhost:${PROMETHEUS_PORT}/-/ready" > /dev/null 2>&1 \
    || fail "Prometheus did not become ready"
success "Prometheus running at http://localhost:${PROMETHEUS_PORT}"

# ── 9. Start Grafana ────────────────────────────────────────────────
if [[ "$SKIP_GRAFANA" == "false" ]]; then
    info "Starting Grafana (port ${GRAFANA_PORT}) …"

    GRAFANA_PROV_DIR="${WORK_DIR}/grafana/provisioning"
    GRAFANA_DASH_DIR="${WORK_DIR}/grafana/dashboards"
    mkdir -p "${GRAFANA_PROV_DIR}/datasources" "${GRAFANA_PROV_DIR}/dashboards" "$GRAFANA_DASH_DIR"

    sed "s/__PROMETHEUS_PORT__/${PROMETHEUS_PORT}/" \
        "${SCRIPT_DIR}/grafana_datasource.yml" > "${GRAFANA_PROV_DIR}/datasources/datasource.yml"
    cp "${SCRIPT_DIR}/grafana_dashboard_provider.yml" "${GRAFANA_PROV_DIR}/dashboards/provider.yml"
    cp "${SCRIPT_DIR}/grafana_dashboard.json" "${GRAFANA_DASH_DIR}/feast.json"

    docker run -d \
        --name "$CONTAINER_NAME_GRAFANA" \
        -p "${GRAFANA_PORT}:3000" \
        -e GF_SECURITY_ADMIN_USER=admin \
        -e GF_SECURITY_ADMIN_PASSWORD=feast \
        -e GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/feast.json \
        -v "${GRAFANA_PROV_DIR}:/etc/grafana/provisioning:ro" \
        -v "${GRAFANA_DASH_DIR}:/var/lib/grafana/dashboards:ro" \
        ${DOCKER_HOST_FLAG:+"$DOCKER_HOST_FLAG"} \
        grafana/grafana:latest \
        > /dev/null

    for i in $(seq 1 20); do
        if curl -sf "http://localhost:${GRAFANA_PORT}/api/health" > /dev/null 2>&1; then break; fi
        sleep 1
    done
    curl -sf "http://localhost:${GRAFANA_PORT}/api/health" > /dev/null 2>&1 \
        || fail "Grafana did not become ready"
    success "Grafana running at http://localhost:${GRAFANA_PORT}"
fi

# ── 10. Generate traffic ────────────────────────────────────────────
if [[ "$SKIP_TRAFFIC" == "false" ]]; then
    info "Generating traffic for ${TRAFFIC_DURATION}s to populate metrics …"
    info "(Write-path ODFV metrics are served on :8001 — traffic generator runs in background)"
    python3 "${SCRIPT_DIR}/generate_traffic.py" \
        --url "http://localhost:${FEATURE_SERVER_PORT}" \
        --duration "$TRAFFIC_DURATION" \
        --repo-path "${WORK_DIR}/feast_demo/feature_repo" \
        > "${LOG_DIR}/traffic_gen.log" 2>&1 &
    TRAFFIC_PID=$!
    echo "$TRAFFIC_PID" > "${WORK_DIR}/.traffic_gen.pid"

    # Wait for traffic generation to finish
    wait "$TRAFFIC_PID" 2>/dev/null || true
    success "Traffic generation completed"

    # Restart traffic generator with long duration to keep SDK metrics server alive
    info "Keeping SDK metrics server alive (background traffic at ~1 req/s) …"
    python3 "${SCRIPT_DIR}/generate_traffic.py" \
        --url "http://localhost:${FEATURE_SERVER_PORT}" \
        --duration 86400 \
        --rps 1 \
        --repo-path "${WORK_DIR}/feast_demo/feature_repo" \
        > "${LOG_DIR}/traffic_gen_bg.log" 2>&1 &
    TRAFFIC_BG_PID=$!
    echo "$TRAFFIC_BG_PID" > "${WORK_DIR}/.traffic_gen_bg.pid"
    success "Background traffic running (PID ${TRAFFIC_BG_PID}, SDK metrics on :8001)"
fi

# ── 11. Summary ─────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Feast Prometheus Metrics Setup — Complete${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Feature Server:    ${BLUE}http://localhost:${FEATURE_SERVER_PORT}${NC}"
echo -e "  Metrics Endpoint:  ${BLUE}http://localhost:${METRICS_PORT}/metrics${NC}"
echo -e "  Prometheus UI:     ${BLUE}http://localhost:${PROMETHEUS_PORT}${NC}"
if [[ "$SKIP_GRAFANA" == "false" ]]; then
echo -e "  Grafana Dashboard: ${BLUE}http://localhost:${GRAFANA_PORT}${NC}"
echo -e "  Grafana Login:     admin / feast"
fi
echo ""
echo -e "  Logs:              ${WORK_DIR}/logs/"
echo -e "  Workspace:         ${WORK_DIR}/feast_demo/"
echo ""
if [[ "$SKIP_GRAFANA" == "false" ]]; then
echo -e "  Grafana dashboard: ${BLUE}http://localhost:${GRAFANA_PORT}/d/feast-metrics${NC}"
echo ""
fi
echo -e "${GREEN}  PromQL queries to try in Prometheus (http://localhost:${PROMETHEUS_PORT}):${NC}"
echo ""
echo -e "  ${YELLOW}# Request rate by endpoint${NC}"
echo    "  rate(feast_feature_server_request_total[1m])"
echo ""
echo -e "  ${YELLOW}# p95 request latency${NC}"
echo    "  histogram_quantile(0.95, sum(rate(feast_feature_server_request_latency_seconds_bucket[1m])) by (le))"
echo ""
echo -e "  ${YELLOW}# p95 latency broken down by feature count${NC}"
echo    "  histogram_quantile(0.95, sum(rate(feast_feature_server_request_latency_seconds_bucket[1m])) by (le, feature_count))"
echo ""
echo -e "  ${YELLOW}# CPU and memory usage${NC}"
echo    "  feast_feature_server_cpu_usage"
echo    "  feast_feature_server_memory_usage"
echo ""
echo -e "  ${YELLOW}# Feature freshness (staleness per feature view)${NC}"
echo    "  feast_feature_freshness_seconds"
echo ""
echo -e "  ${YELLOW}# Materialization success/failure count${NC}"
echo    "  feast_materialization_total"
echo ""
echo -e "  ${YELLOW}# Online feature request rate${NC}"
echo    "  rate(feast_online_features_request_total[1m])"
echo ""
echo -e "  ${YELLOW}# Average entities per request${NC}"
echo    "  feast_online_features_entity_count_sum / feast_online_features_entity_count_count"
echo ""
echo -e "  ${YELLOW}# Push requests by source and mode${NC}"
echo    "  feast_push_request_total"
echo ""
echo -e "  To stop everything: ${YELLOW}./teardown.sh${NC}"
echo ""
