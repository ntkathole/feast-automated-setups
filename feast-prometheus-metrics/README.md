# Feast Prometheus Metrics — Automated Setup

Automated local setup that deploys a Feast feature server with Prometheus metrics collection, a Prometheus instance, and a Grafana dashboard — all with a single command.

## Prerequisites

- **Python 3** with `feast` installed (`pip install feast`)
- **Docker** (for Prometheus and Grafana containers)
- **curl** (for health checks)

## Directory Structure

```
feast-prometheus-metrics/
├── setup.sh                        # Deploy everything
├── teardown.sh                     # Stop and clean up everything
├── generate_traffic.py             # Traffic generator to populate metrics
├── feature_definitions.py          # Simplified feature definitions (copied into workspace)
├── prometheus.yml                  # Prometheus scrape config template
├── grafana_datasource.yml          # Grafana datasource provisioning template
├── grafana_dashboard_provider.yml  # Grafana dashboard provisioning config
├── grafana_dashboard.json          # Pre-built Grafana dashboard
├── workspace/                      # (auto-created) Runtime workspace
└── README.md
```

## Quick Start

```bash
# Default setup — starts everything and generates 60s of traffic
./setup.sh

# Custom ports
./setup.sh --prometheus-port 9091 --grafana-port 3001

# Skip traffic generation (generate manually later)
./setup.sh --skip-traffic

# Prometheus only (no Grafana)
./setup.sh --skip-grafana
```

## What It Does

1. **Prerequisite check** — verifies `python3`, `feast`, `docker` are available
2. **Feature repository** — runs `feast init`, replaces feature definitions with a simplified version, enables all metric categories in `feature_store.yaml`
3. **Apply & materialize** — runs `feast apply` and `feast materialize` to seed data
4. **Feature server** — starts `feast serve --metrics` in the background (port 6566, metrics on 8000)
5. **Prometheus** — starts a Prometheus container scraping the Feast metrics endpoint
6. **Grafana** — starts a Grafana container with a pre-provisioned Feast dashboard
7. **Traffic generation** — sends varied requests (online features, push, materialize) to populate all metric types

## Setup Options

| Flag | Description |
|------|-------------|
| `--feature-server-port PORT` | Feast feature server port (default: `6566`) |
| `--prometheus-port PORT` | Prometheus web UI port (default: `9090`) |
| `--grafana-port PORT` | Grafana web UI port (default: `3000`) |
| `--traffic-duration SECS` | Seconds to run traffic generator (default: `60`) |
| `--skip-traffic` | Skip automated traffic generation |
| `--skip-grafana` | Skip Grafana deployment |

> **Note:** The Feast metrics endpoint is always on port `8000` (hardcoded in Feast).

## URLs After Setup

| Service | URL |
|---------|-----|
| Feature Server | http://localhost:6566 |
| Metrics Endpoint | http://localhost:8000/metrics |
| Prometheus UI | http://localhost:9090 |
| Grafana Dashboard | http://localhost:3000 |

**Grafana credentials:** `admin` / `feast`

## Teardown

```bash
# Full cleanup — stops everything and removes workspace
./teardown.sh

# Keep workspace for inspection
./teardown.sh --keep-workspace
```

| Flag | Description |
|------|-------------|
| `--keep-workspace` | Keep the workspace directory (feature repo, logs, configs) |
| `--keep-containers` | Stop but don't remove Docker containers |

## Metrics Collected

The Grafana dashboard visualises the following Feast metrics:

### Resource Metrics
- `feast_feature_server_cpu_usage` — CPU usage per worker process
- `feast_feature_server_memory_usage` — Memory usage per worker process

### Request Metrics
- `feast_feature_server_request_total` — Request count by endpoint and status
- `feast_feature_server_request_latency_seconds` — Request latency histogram with `feature_count` and `feature_view_count` dimensions

### Online Features
- `feast_online_features_request_total` — Online feature retrieval count
- `feast_online_features_entity_count` — Entities per request histogram

### Push Metrics
- `feast_push_request_total` — Push request count by source and mode

### Materialization Metrics
- `feast_materialization_total` — Materialization runs by feature view and status
- `feast_materialization_duration_seconds` — Materialization duration histogram

### Freshness Metrics
- `feast_feature_freshness_seconds` — Feature staleness per feature view

## Manual Traffic Generation

If you used `--skip-traffic`, you can generate traffic manually:

```bash
# Generate 120 seconds of traffic at ~5 req/s
python3 generate_traffic.py --url http://localhost:6566 --duration 120

# Higher request rate
python3 generate_traffic.py --url http://localhost:6566 --duration 60 --rps 10
```

Or send individual requests:

```bash
# Online features
curl -X POST http://localhost:6566/get-online-features \
  -H "Content-Type: application/json" \
  -d '{"features": ["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"], "entities": {"driver_id": [1001, 1002]}}'

# Push
curl -X POST http://localhost:6566/push \
  -H "Content-Type: application/json" \
  -d '{"push_source_name": "driver_stats_push_source", "df": {"driver_id": [1001], "conv_rate": [0.5], "acc_rate": [0.8], "avg_daily_trips": [10], "event_timestamp": ["2025-01-01T00:00:00+00:00"], "created": ["2025-01-01T00:00:00+00:00"]}, "to": "online"}'

# Materialize incremental
curl -X POST http://localhost:6566/materialize-incremental \
  -H "Content-Type: application/json" \
  -d '{"end_ts": "2025-01-01T00:00:00+00:00"}'
```

## Metrics Configuration

The setup enables all metric categories via `feature_store.yaml`:

```yaml
feature_server:
  metrics:
    enabled: true
    resource: true
    request: true
    online_features: true
    push: true
    materialization: true
    freshness: true
```

Each category can be individually toggled. See the [Feast metrics documentation](../../sdk/python/feast/infra/feature_servers/base_config.py) for details.
