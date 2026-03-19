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
3. **Apply & materialize** — runs `feast apply` and `feast materialize` (batch FVs) to seed data
4. **Feature server** — starts `feast serve --metrics` in the background (port 6566, metrics on 8000)
5. **Prometheus** — starts a Prometheus container scraping the Feast metrics endpoint
6. **Grafana** — starts a Grafana container with a pre-provisioned Feast dashboard
7. **Traffic generation** — sends varied requests including Pandas ODFV, Python ODFV, push, materialize, and SDK `write_to_online_store` for write-path ODFV transforms

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

### Online Store Read
- `feast_feature_server_online_store_read_duration_seconds` — Time spent reading from the online store (covers all table reads including parallel async)

### ODFV Transformation (Read Path)
- `feast_feature_server_transformation_duration_seconds` — Duration of on-demand feature view transformations during online serving, with `odfv_name` and `mode` labels. Only emitted for ODFVs with `track_metrics=True`.

### ODFV Transformation (Write Path)
- `feast_feature_server_write_transformation_duration_seconds` — Duration of on-demand feature view transformations during push/materialize (`write_to_online_store=True`), with `odfv_name` and `mode` labels. Only emitted for ODFVs with `track_metrics=True`.

### Push Metrics
- `feast_push_request_total` — Push request count by source and mode

### Materialization Metrics
- `feast_materialization_result_total` — Materialization runs by feature view and status
- `feast_materialization_duration_seconds` — Materialization duration histogram

### Freshness Metrics
- `feast_feature_freshness_seconds` — Feature staleness per feature view

## Per-ODFV Metrics Opt-In

ODFV transformation metrics are opt-in at the definition level via `track_metrics=True`:

```python
# Pandas-mode read-path ODFV
@on_demand_feature_view(
    sources=[driver_stats_fv, input_request],
    schema=[Field(name="output", dtype=Float64)],
    track_metrics=True,
)
def my_pandas_transform(inputs: pd.DataFrame) -> pd.DataFrame:
    ...

# Python-mode read-path ODFV
@on_demand_feature_view(
    sources=[driver_stats_fv, input_request],
    schema=[Field(name="output_py", dtype=Float64)],
    mode="python",
    track_metrics=True,
)
def my_python_transform(inputs: Dict[str, Any]) -> Dict[str, Any]:
    ...

# Write-path ODFV (transformed during write_to_online_store)
trip_score = OnDemandFeatureView(
    name="trip_score",
    entities=[driver],
    sources=[driver_stats_fv],
    schema=[Field(name="trip_score", dtype=Float64)],
    feature_transformation=PythonTransformation(udf=udf_fn, udf_string="..."),
    mode="python",
    write_to_online_store=True,
    track_metrics=True,
)
```

When `track_metrics=False` (the default), zero metrics code runs for that ODFV — no timing, no Prometheus recording.

## Pandas vs Python ODFV Comparison

This demo includes two read-path ODFVs performing the same computation, one using Pandas mode and the other Python mode. The Grafana dashboard includes a dedicated **"Pandas vs Python ODFV Read Latency"** panel that overlays p50/p95 latencies side by side, making it easy to compare the performance characteristics of each transformation mode.

## Manual Traffic Generation

If you used `--skip-traffic`, you can generate traffic manually:

```bash
# Generate 120 seconds of traffic at ~5 req/s (including write-path transforms)
python3 generate_traffic.py --url http://localhost:6566 --duration 120 \
    --repo-path workspace/feast_demo/feature_repo

# Without write-path transforms (REST-only traffic)
python3 generate_traffic.py --url http://localhost:6566 --duration 60 --rps 10

# Higher request rate
python3 generate_traffic.py --url http://localhost:6566 --duration 60 --rps 15 \
    --repo-path workspace/feast_demo/feature_repo
```

Or send individual requests:

```bash
# Online features (basic — no ODFV)
curl -X POST http://localhost:6566/get-online-features \
  -H "Content-Type: application/json" \
  -d '{"features": ["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"], "entities": {"driver_id": [1001, 1002]}}'

# Online features with Pandas ODFV transform
curl -X POST http://localhost:6566/get-online-features \
  -H "Content-Type: application/json" \
  -d '{"features": ["driver_hourly_stats:conv_rate", "transformed_conv_rate:conv_rate_plus_val1", "transformed_conv_rate:conv_rate_plus_val2"], "entities": {"driver_id": [1001, 1002], "val_to_add": [5, 10], "val_to_add_2": [3, 7]}}'

# Online features with Python ODFV transform
curl -X POST http://localhost:6566/get-online-features \
  -H "Content-Type: application/json" \
  -d '{"features": ["driver_hourly_stats:conv_rate", "transformed_conv_rate_python:conv_rate_plus_val1_py", "transformed_conv_rate_python:conv_rate_plus_val2_py"], "entities": {"driver_id": [1001, 1002], "val_to_add": [5, 10], "val_to_add_2": [3, 7]}}'

# Push
curl -X POST http://localhost:6566/push \
  -H "Content-Type: application/json" \
  -d '{"push_source_name": "driver_stats_push_source", "df": {"driver_id": [1001], "conv_rate": [0.5], "acc_rate": [0.8], "avg_daily_trips": [10], "event_timestamp": ["2025-01-01T00:00:00+00:00"], "created": ["2025-01-01T00:00:00+00:00"]}, "to": "online"}'

# Materialize incremental
curl -X POST http://localhost:6566/materialize-incremental \
  -H "Content-Type: application/json" \
  -d '{"end_ts": "2025-01-01T00:00:00+00:00"}'
```

## Example PromQL Queries

```promql
# Online store read p95 latency
histogram_quantile(0.95, sum(rate(feast_feature_server_online_store_read_duration_seconds_bucket[1m])) by (le))

# ODFV read-path transform p95 by ODFV name
histogram_quantile(0.95, sum(rate(feast_feature_server_transformation_duration_seconds_bucket[1m])) by (le, odfv_name))

# ODFV write-path transform p95 by ODFV name
histogram_quantile(0.95, sum(rate(feast_feature_server_write_transformation_duration_seconds_bucket[1m])) by (le, odfv_name))

# Latency breakdown: avg total vs avg store read vs avg transform
rate(feast_feature_server_request_latency_seconds_sum[1m]) / rate(feast_feature_server_request_latency_seconds_count[1m])
rate(feast_feature_server_online_store_read_duration_seconds_sum[1m]) / rate(feast_feature_server_online_store_read_duration_seconds_count[1m])
rate(feast_feature_server_transformation_duration_seconds_sum[1m]) / rate(feast_feature_server_transformation_duration_seconds_count[1m])

# Pandas vs Python p95 comparison
histogram_quantile(0.95, sum(rate(feast_feature_server_transformation_duration_seconds_bucket{mode="pandas"}[1m])) by (le))
histogram_quantile(0.95, sum(rate(feast_feature_server_transformation_duration_seconds_bucket{mode="python"}[1m])) by (le))

# Write-path transform rate by ODFV
rate(feast_feature_server_write_transformation_duration_seconds_count[1m])
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
