#!/usr/bin/env python3
"""
Traffic generator for Feast feature server metrics demo.

Sends varied requests to the feature server to populate all metric types:
  - /get-online-features  (request latency, online features, entity counts)
  - /get-online-features with Pandas ODFV  (read-path transform, pandas mode)
  - /get-online-features with Python ODFV  (read-path transform, python mode)
  - /push                 (push counter)
  - /materialize-incremental (materialization counter + duration)
  - SDK write_to_online_store  (write-path transform via trip_score ODFV)

Usage:
    python3 generate_traffic.py --url http://localhost:6566 --duration 60
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.error import URLError
from urllib.request import Request, urlopen


def post_json(url: str, path: str, payload: dict) -> int:
    """POST JSON to url+path and return the HTTP status code."""
    data = json.dumps(payload).encode()
    req = Request(
        f"{url}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req) as resp:
            resp.read()
            return resp.status
    except URLError as e:
        if hasattr(e, "code"):
            return e.code
        return 0


def send_online_features_plain(url: str) -> None:
    """Send /get-online-features with batch features only (no ODFV)."""
    entity_count = random.choice([1, 2, 3, 5, 10])
    entity_ids = [random.randint(1001, 1010) for _ in range(entity_count)]

    payload = {
        "features": [
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
        ],
        "entities": {"driver_id": entity_ids},
    }
    status = post_json(url, "/get-online-features", payload)
    sys.stdout.write("." if status == 200 else "x")
    sys.stdout.flush()


def send_online_features_pandas_odfv(url: str) -> None:
    """Send /get-online-features triggering the Pandas-mode ODFV."""
    entity_count = random.choice([1, 2, 3, 5, 10])
    entity_ids = [random.randint(1001, 1010) for _ in range(entity_count)]

    payload = {
        "features": [
            "driver_hourly_stats:conv_rate",
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2",
        ],
        "entities": {
            "driver_id": entity_ids,
            "val_to_add": [random.randint(1, 10) for _ in range(entity_count)],
            "val_to_add_2": [random.randint(1, 10) for _ in range(entity_count)],
        },
    }
    status = post_json(url, "/get-online-features", payload)
    sys.stdout.write("D" if status == 200 else "d")
    sys.stdout.flush()


def send_online_features_python_odfv(url: str) -> None:
    """Send /get-online-features triggering the Python-mode ODFV."""
    entity_count = random.choice([1, 2, 3, 5, 10])
    entity_ids = [random.randint(1001, 1010) for _ in range(entity_count)]

    payload = {
        "features": [
            "driver_hourly_stats:conv_rate",
            "transformed_conv_rate_python:conv_rate_plus_val1_py",
            "transformed_conv_rate_python:conv_rate_plus_val2_py",
        ],
        "entities": {
            "driver_id": entity_ids,
            "val_to_add": [random.randint(1, 10) for _ in range(entity_count)],
            "val_to_add_2": [random.randint(1, 10) for _ in range(entity_count)],
        },
    }
    status = post_json(url, "/get-online-features", payload)
    sys.stdout.write("Y" if status == 200 else "y")
    sys.stdout.flush()


def send_push(url: str) -> None:
    """Send a /push request."""
    now = datetime.now(timezone.utc)
    payload = {
        "push_source_name": "driver_stats_push_source",
        "df": {
            "driver_id": [random.randint(1001, 1010)],
            "conv_rate": [round(random.uniform(0.0, 1.0), 4)],
            "acc_rate": [round(random.uniform(0.0, 1.0), 4)],
            "avg_daily_trips": [random.randint(0, 100)],
            "event_timestamp": [now.isoformat()],
            "created": [now.isoformat()],
        },
        "to": "online",
    }
    status = post_json(url, "/push", payload)
    sys.stdout.write("P" if status == 200 else "p")
    sys.stdout.flush()


def send_materialize_incremental(url: str) -> None:
    """Send a /materialize-incremental request."""
    end_ts = datetime.now(timezone.utc).isoformat()
    payload = {"end_ts": end_ts}
    status = post_json(url, "/materialize-incremental", payload)
    sys.stdout.write("M" if status == 200 else "m")
    sys.stdout.flush()


def start_sdk_metrics_server(port: int = 8001) -> None:
    """Start a Prometheus HTTP server to expose metrics from SDK operations.

    Write-path ODFV transforms happen in this process (not the feature
    server), so we need our own metrics endpoint for Prometheus to scrape.
    """
    from feast import metrics as feast_metrics

    feast_metrics._config.online_features = True
    try:
        from prometheus_client import start_http_server

        start_http_server(port)
        print(f"SDK metrics server started on :{port}")
    except Exception as e:
        print(f"Warning: could not start SDK metrics server on :{port}: {e}")


def send_write_path_transform(store) -> None:
    """Trigger write-path ODFV transform via SDK write_to_online_store.

    This writes to the trip_score ODFV (write_to_online_store=True),
    triggering _transform_on_demand_feature_view_df and emitting
    feast_feature_server_write_transformation_duration_seconds.
    """
    import pandas as pd

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "driver_id": [random.randint(1001, 1010)],
            "conv_rate": [round(random.uniform(0.0, 1.0), 4)],
            "acc_rate": [round(random.uniform(0.0, 1.0), 4)],
            "avg_daily_trips": [random.randint(0, 100)],
            "event_timestamp": [now],
            "created": [now],
        }
    )
    try:
        store.write_to_online_store("trip_score", df)
        sys.stdout.write("W")
    except Exception:
        sys.stdout.write("w")
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description="Feast traffic generator")
    parser.add_argument(
        "--url", default="http://localhost:6566", help="Feature server URL"
    )
    parser.add_argument(
        "--duration", type=int, default=60, help="Duration in seconds"
    )
    parser.add_argument(
        "--rps", type=float, default=5.0, help="Approximate requests per second"
    )
    parser.add_argument(
        "--repo-path",
        default=None,
        help="Path to the Feast feature repo (for write-path transforms via SDK). "
        "If not given, write-path transforms are skipped.",
    )
    args = parser.parse_args()

    store = None
    if args.repo_path:
        try:
            from feast import FeatureStore

            store = FeatureStore(repo_path=args.repo_path)
            print(f"SDK loaded — write-path transforms enabled (repo: {args.repo_path})")
            start_sdk_metrics_server(port=8001)
        except Exception as e:
            print(f"Warning: could not load FeatureStore from {args.repo_path}: {e}")
            print("Write-path transforms will be skipped.")

    print(f"Generating traffic to {args.url} for {args.duration}s (~{args.rps} req/s)")
    print(
        "Legend:  . = plain online   D/d = pandas ODFV ok/fail   "
        "Y/y = python ODFV ok/fail"
    )
    print(
        "        P/p = push ok/fail  M/m = materialize ok/fail   "
        "W/w = write transform ok/fail"
    )
    print()

    start = time.monotonic()
    request_count = 0
    interval = 1.0 / args.rps

    while time.monotonic() - start < args.duration:
        roll = random.random()
        if roll < 0.25:
            send_online_features_plain(args.url)
        elif roll < 0.45:
            send_online_features_pandas_odfv(args.url)
        elif roll < 0.65:
            send_online_features_python_odfv(args.url)
        elif roll < 0.75:
            send_push(args.url)
        elif roll < 0.85:
            send_materialize_incremental(args.url)
        elif store is not None:
            send_write_path_transform(store)
        else:
            send_online_features_plain(args.url)

        request_count += 1

        if request_count % 50 == 0:
            elapsed = time.monotonic() - start
            print(f"  [{elapsed:.0f}s] {request_count} requests sent")

        jitter = interval * random.uniform(0.5, 1.5)
        time.sleep(jitter)

    elapsed = time.monotonic() - start
    print(
        f"\n\nDone: {request_count} requests in {elapsed:.1f}s "
        f"({request_count / elapsed:.1f} req/s)"
    )


if __name__ == "__main__":
    main()
