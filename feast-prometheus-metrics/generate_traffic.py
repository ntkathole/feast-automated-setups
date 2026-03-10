#!/usr/bin/env python3
"""
Traffic generator for Feast feature server metrics demo.

Sends varied requests to the feature server to populate all metric types:
  - /get-online-features  (request latency, online features, entity counts)
  - /push                 (push counter)
  - /materialize-incremental (materialization counter + duration)

Usage:
    python3 generate_traffic.py --url http://localhost:6566 --duration 60
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError


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


def send_online_features(url: str) -> None:
    """Send /get-online-features with varying entity counts.

    Alternates between plain batch features and requests that include
    the on-demand feature view (transformed_conv_rate), which requires
    request-time data (val_to_add, val_to_add_2) and triggers read-path
    ODFV transformation.
    """
    entity_count = random.choice([1, 2, 3, 5, 10])
    entity_ids = [random.randint(1001, 1010) for _ in range(entity_count)]

    use_odfv = random.random() < 0.5
    if use_odfv:
        payload = {
            "features": [
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "transformed_conv_rate:conv_rate_plus_val1",
                "transformed_conv_rate:conv_rate_plus_val2",
            ],
            "entities": {
                "driver_id": entity_ids,
                "val_to_add": [random.randint(1, 10) for _ in range(entity_count)],
                "val_to_add_2": [random.randint(1, 10) for _ in range(entity_count)],
            },
        }
    else:
        payload = {
            "features": [
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "driver_hourly_stats:avg_daily_trips",
            ],
            "entities": {"driver_id": entity_ids},
        }
    status = post_json(url, "/get-online-features", payload)
    if status == 200:
        sys.stdout.write("T" if use_odfv else ".")
    else:
        sys.stdout.write("t" if use_odfv else "x")
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
    if status == 200:
        sys.stdout.write("P")
    else:
        sys.stdout.write("p")
    sys.stdout.flush()


def send_materialize_incremental(url: str) -> None:
    """Send a /materialize-incremental request."""
    end_ts = datetime.now(timezone.utc).isoformat()
    payload = {"end_ts": end_ts}
    status = post_json(url, "/materialize-incremental", payload)
    if status == 200:
        sys.stdout.write("M")
    else:
        sys.stdout.write("m")
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description="Feast traffic generator")
    parser.add_argument("--url", default="http://localhost:6566", help="Feature server URL")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--rps", type=float, default=5.0, help="Approximate requests per second")
    args = parser.parse_args()

    print(f"Generating traffic to {args.url} for {args.duration}s (~{args.rps} req/s)")
    print("Legend:  . = online features   T/t = online+ODFV ok/fail   P/p = push ok/fail   M/m = materialize ok/fail")
    print()

    start = time.monotonic()
    request_count = 0
    interval = 1.0 / args.rps

    while time.monotonic() - start < args.duration:
        roll = random.random()
        if roll < 0.70:
            send_online_features(args.url)
        elif roll < 0.85:
            send_push(args.url)
        else:
            send_materialize_incremental(args.url)

        request_count += 1

        if request_count % 50 == 0:
            elapsed = time.monotonic() - start
            print(f"  [{elapsed:.0f}s] {request_count} requests sent")

        jitter = interval * random.uniform(0.5, 1.5)
        time.sleep(jitter)

    elapsed = time.monotonic() - start
    print(f"\n\nDone: {request_count} requests in {elapsed:.1f}s ({request_count/elapsed:.1f} req/s)")


if __name__ == "__main__":
    main()
