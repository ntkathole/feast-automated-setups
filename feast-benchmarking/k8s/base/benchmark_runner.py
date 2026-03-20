#!/usr/bin/env python3
"""
Unified Feast Benchmark Runner - Runs benchmarks for any online store.

Environment Variables:
    STORE: Online store type (sqlite, redis, postgres, dynamodb)
    FEAST_REF: Feast reference for results directory
    SCENARIOS: Comma-separated scenarios (entity_scaling, feature_scaling)
    ITERATIONS: Number of benchmark iterations
    WARMUP: Warmup iterations
    SLA_P99_MS: SLA target in milliseconds
    FIXED_FEATURES: Fixed feature count for entity scaling
    FIXED_ENTITIES: Fixed entity count for feature scaling
    ENTITY_COUNTS: Comma-separated entity counts
    FEATURE_COUNTS: Comma-separated feature counts
    
Store-specific env vars:
    REDIS_HOST, REDIS_PORT
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE, POSTGRES_USER, POSTGRES_PASSWORD
    AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
"""
import cProfile
import json
import os
import pstats
import shutil
import statistics
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
from feast import Entity, FeatureStore, FeatureView, Field, FileSource
from feast.types import Float64

try:
    import feast
    FEAST_VERSION = feast.__version__
except:
    FEAST_VERSION = "unknown"

STORE = os.environ.get("STORE", "sqlite")
REF = os.environ.get("FEAST_REF", "master")

STORE_DEFAULTS = {
    "sqlite": {"iterations": 100, "warmup": 10},
    "redis": {"iterations": 300, "warmup": 20},
    "postgres": {"iterations": 300, "warmup": 20},
    "dynamodb": {"iterations": 500, "warmup": 30},
}


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


@dataclass
class Result:
    store: str
    scenario: str
    num_features: int
    num_entities: int
    p50: float
    p95: float
    p99: float
    mean: float
    min_val: float
    max_val: float
    std_dev: float
    rps: float
    online_read_pct: float = 0.0
    protobuf_convert_pct: float = 0.0
    entity_serial_pct: float = 0.0
    other_pct: float = 100.0
    sla_pass: bool = False
    feast_version: str = ""
    optimizations: List[str] = field(default_factory=list)


def get_store_config() -> str:
    """Generate online_store YAML config based on STORE type."""
    if STORE == "sqlite":
        return """type: sqlite
  path: {repo}/online.db"""
    
    elif STORE == "redis":
        host = os.environ.get("REDIS_HOST", "redis")
        port = os.environ.get("REDIS_PORT", "6379")
        return f"""type: redis
  connection_string: {host}:{port}"""
    
    elif STORE == "postgres":
        return f"""type: postgres
  host: {os.environ.get("POSTGRES_HOST", "postgres")}
  port: {os.environ.get("POSTGRES_PORT", "5432")}
  database: {os.environ.get("POSTGRES_DATABASE", "feast")}
  user: {os.environ.get("POSTGRES_USER", "feast")}
  password: {os.environ.get("POSTGRES_PASSWORD", "feast")}"""
    
    elif STORE == "dynamodb":
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        use_dax = os.environ.get("USE_DAX", "false").lower() == "true"
        dax_endpoint = os.environ.get("DAX_ENDPOINT", "")
        
        if use_dax and dax_endpoint:
            log(f"DAX enabled: {dax_endpoint}")
            # Use native DAX support (use_dax + dax_endpoint)
            # This triggers the amazon-dax-client in Feast
            return f"""type: dynamodb
  region: {region}
  use_dax: true
  dax_endpoint: "{dax_endpoint}"
  batch_size: 100
  consistent_reads: false"""
        else:
            return f"""type: dynamodb
  region: {region}
  batch_size: 100
  consistent_reads: false
  max_pool_connections: 200
  keepalive_timeout: 60.0
  connect_timeout: 2
  read_timeout: 5
  retry_mode: adaptive
  total_max_retry_attempts: 3"""
    
    else:
        raise ValueError(f"Unknown store: {STORE}")


def profile_request(fs, features, entity_rows) -> Tuple[float, Dict[str, float]]:
    """Profile a single request and extract timing breakdown."""
    profiler = cProfile.Profile()
    start = time.perf_counter()
    profiler.enable()
    fs.get_online_features(features=features, entity_rows=entity_rows)
    profiler.disable()
    total_ms = (time.perf_counter() - start) * 1000
    
    stats = pstats.Stats(profiler)
    function_times = {}
    for func, (cc, nc, tt, ct, callers) in stats.stats.items():
        _, _, funcname = func
        function_times[funcname] = function_times.get(funcname, 0) + tt * 1000
    
    online_read = function_times.get('online_read', 0) + function_times.get('_get_online_features_from_online_store', 0)
    protobuf = function_times.get('_convert_rows_to_protobuf', 0) + function_times.get('construct_response_feature_vector', 0)
    entity_serial = function_times.get('serialize_entity_key', 0)
    
    breakdown = {
        'online_read_pct': (online_read / total_ms * 100) if total_ms > 0 else 0,
        'protobuf_convert_pct': (protobuf / total_ms * 100) if total_ms > 0 else 0,
        'entity_serial_pct': (entity_serial / total_ms * 100) if total_ms > 0 else 0,
    }
    breakdown['other_pct'] = max(0, 100 - sum(breakdown.values()))
    return total_ms, breakdown


def setup_feast(repo: str, num_features: int, max_entities: int) -> FeatureStore:
    """Setup Feast repo with configured online store."""
    shutil.rmtree(repo, ignore_errors=True)
    os.makedirs(repo, exist_ok=True)
    
    store_config = get_store_config().format(repo=repo)
    
    config = f"""project: benchmark
provider: local
registry:
  path: {repo}/registry.db
  cache_ttl_seconds: 0
online_store:
  {store_config}
entity_key_serialization_version: 3
"""
    with open(f"{repo}/feature_store.yaml", "w") as f:
        f.write(config)
    
    data = {"user_id": [f"user_{i}" for i in range(max_entities)]}
    for i in range(num_features):
        data[f"f{i}"] = [float(i * j) for j in range(max_entities)]
    df = pd.DataFrame(data)
    df["event_timestamp"] = pd.Timestamp.now()
    df.to_parquet(f"{repo}/data.parquet")
    
    source = FileSource(path=f"{repo}/data.parquet", timestamp_field="event_timestamp")
    entity = Entity(name="user_id", join_keys=["user_id"])
    schema = [Field(name=f"f{i}", dtype=Float64) for i in range(num_features)]
    fv = FeatureView(name="fv", entities=[entity], schema=schema, source=source, ttl=timedelta(days=1))
    
    fs = FeatureStore(repo_path=repo)
    fs.apply([entity, fv])
    fs.materialize(start_date=datetime.now() - timedelta(days=1), end_date=datetime.now())
    fs.refresh_registry()
    return fs


def run_benchmark(fs, num_features: int, num_entities: int, iterations: int, 
                  warmup: int, sla: float, scenario: str, optimizations: List[str]) -> Result:
    """Run benchmark with profiling."""
    features = [f"fv:f{i}" for i in range(num_features)]
    rows = [{"user_id": f"user_{i}"} for i in range(num_entities)]
    
    for _ in range(warmup):
        fs.get_online_features(features=features, entity_rows=rows)
    
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        fs.get_online_features(features=features, entity_rows=rows)
        times.append((time.perf_counter() - start) * 1000)
    times.sort()
    
    breakdowns = []
    for _ in range(5):
        _, bd = profile_request(fs, features, rows)
        breakdowns.append(bd)
    avg_breakdown = {k: statistics.mean([b[k] for b in breakdowns]) for k in breakdowns[0]}
    
    p99_idx = int(len(times) * 0.99)
    mean_time = statistics.mean(times)
    
    return Result(
        store=STORE,
        scenario=scenario,
        num_features=num_features,
        num_entities=num_entities,
        p50=times[len(times) // 2],
        p95=times[int(len(times) * 0.95)],
        p99=times[p99_idx],
        mean=mean_time,
        min_val=min(times),
        max_val=max(times),
        std_dev=statistics.stdev(times) if len(times) > 1 else 0,
        rps=1000 / mean_time if mean_time > 0 else 0,
        online_read_pct=avg_breakdown['online_read_pct'],
        protobuf_convert_pct=avg_breakdown['protobuf_convert_pct'],
        entity_serial_pct=avg_breakdown['entity_serial_pct'],
        other_pct=avg_breakdown['other_pct'],
        sla_pass=times[p99_idx] < sla,
        feast_version=FEAST_VERSION,
        optimizations=optimizations
    )


def run_entity_scaling():
    """Scenario 1: Fixed features, vary entities."""
    repo = "/tmp/feast_repo"
    defaults = STORE_DEFAULTS.get(STORE, STORE_DEFAULTS["sqlite"])
    
    ITERATIONS = int(os.environ.get("ITERATIONS", defaults["iterations"]))
    WARMUP = int(os.environ.get("WARMUP", defaults["warmup"]))
    SLA = float(os.environ.get("SLA_P99_MS", "60"))
    FIXED_FEATURES = int(os.environ.get("FIXED_FEATURES", "200"))
    ENTITY_COUNTS = [int(x) for x in os.environ.get("ENTITY_COUNTS", "1,10,50,100,200").split(",")]
    
    optimizations = ["cache_ttl=0", "entity_key_v3", f"iter={ITERATIONS}"]
    if STORE == "dynamodb":
        optimizations.append("adaptive_retry")
    
    max_entities = max(ENTITY_COUNTS) + 100
    
    print("\n" + "=" * 70)
    print(f"{STORE.upper()} - ENTITY SCALING")
    print(f"Fixed Features: {FIXED_FEATURES} | Varying Entities: {ENTITY_COUNTS}")
    print(f"Iterations: {ITERATIONS} | Warmup: {WARMUP} | SLA: {SLA}ms")
    print("=" * 70)
    
    log(f"Setting up Feast with {STORE}...")
    fs = setup_feast(repo, FIXED_FEATURES, max_entities)
    results = []
    
    for n_entities in ENTITY_COUNTS:
        log(f"Benchmarking {n_entities} entities...")
        r = run_benchmark(fs, FIXED_FEATURES, n_entities, ITERATIONS, WARMUP, SLA, "entity_scaling", optimizations)
        results.append(r)
        status = "PASS" if r.sla_pass else "FAIL"
        print(f"{n_entities:4d}e: p50={r.p50:7.1f}ms | p95={r.p95:7.1f}ms | p99={r.p99:7.1f}ms | std={r.std_dev:5.1f}ms [{status}]")
    
    out_dir = f"/results/{REF}/{STORE}/entity_scaling"
    os.makedirs(out_dir, exist_ok=True)
    output = {
        "store": STORE,
        "scenario": "entity_scaling",
        "timestamp": datetime.now().isoformat(),
        "feast_version": FEAST_VERSION,
        "config": {
            "fixed_features": FIXED_FEATURES,
            "entity_counts": ENTITY_COUNTS,
            "iterations": ITERATIONS,
            "warmup": WARMUP,
            "sla_ms": SLA,
            "optimizations": optimizations
        },
        "latency": [asdict(r) for r in results]
    }
    with open(f"{out_dir}/benchmark_results.json", "w") as f:
        json.dump(output, f, indent=2)
    
    passed = sum(1 for r in results if r.sla_pass)
    print(f"Saved: {out_dir}/benchmark_results.json | SLA: {passed}/{len(results)} ({100 * passed / len(results):.0f}%)")
    return results


def run_feature_scaling():
    """Scenario 2: Fixed entities, vary features."""
    repo = "/tmp/feast_repo"
    defaults = STORE_DEFAULTS.get(STORE, STORE_DEFAULTS["sqlite"])
    
    ITERATIONS = int(os.environ.get("ITERATIONS", defaults["iterations"]))
    WARMUP = int(os.environ.get("WARMUP", defaults["warmup"]))
    SLA = float(os.environ.get("SLA_P99_MS", "60"))
    FIXED_ENTITIES = int(os.environ.get("FIXED_ENTITIES", "50"))
    FEATURE_COUNTS = [int(x) for x in os.environ.get("FEATURE_COUNTS", "5,25,50,100,150,200").split(",")]
    
    optimizations = ["cache_ttl=0", "entity_key_v3", f"iter={ITERATIONS}"]
    if STORE == "dynamodb":
        optimizations.append("adaptive_retry")
    
    print("\n" + "=" * 70)
    print(f"{STORE.upper()} - FEATURE SCALING")
    print(f"Fixed Entities: {FIXED_ENTITIES} | Varying Features: {FEATURE_COUNTS}")
    print(f"Iterations: {ITERATIONS} | Warmup: {WARMUP} | SLA: {SLA}ms")
    print("=" * 70)
    
    results = []
    for n_features in FEATURE_COUNTS:
        log(f"Setting up Feast with {n_features} features...")
        fs = setup_feast(repo, n_features, FIXED_ENTITIES + 100)
        r = run_benchmark(fs, n_features, FIXED_ENTITIES, ITERATIONS, WARMUP, SLA, "feature_scaling", optimizations)
        results.append(r)
        status = "PASS" if r.sla_pass else "FAIL"
        print(f"{n_features:4d}f: p50={r.p50:7.1f}ms | p95={r.p95:7.1f}ms | p99={r.p99:7.1f}ms | std={r.std_dev:5.1f}ms [{status}]")
    
    out_dir = f"/results/{REF}/{STORE}/feature_scaling"
    os.makedirs(out_dir, exist_ok=True)
    output = {
        "store": STORE,
        "scenario": "feature_scaling",
        "timestamp": datetime.now().isoformat(),
        "feast_version": FEAST_VERSION,
        "config": {
            "fixed_entities": FIXED_ENTITIES,
            "feature_counts": FEATURE_COUNTS,
            "iterations": ITERATIONS,
            "warmup": WARMUP,
            "sla_ms": SLA,
            "optimizations": optimizations
        },
        "latency": [asdict(r) for r in results]
    }
    with open(f"{out_dir}/benchmark_results.json", "w") as f:
        json.dump(output, f, indent=2)
    
    passed = sum(1 for r in results if r.sla_pass)
    print(f"Saved: {out_dir}/benchmark_results.json | SLA: {passed}/{len(results)} ({100 * passed / len(results):.0f}%)")
    return results


if __name__ == "__main__":
    SCENARIOS = os.environ.get("SCENARIOS", "entity_scaling,feature_scaling").split(",")
    
    print(f"\n{'#' * 70}")
    print(f"# {STORE.upper()} BENCHMARK - Running scenarios: {SCENARIOS}")
    print(f"# Feast version: {FEAST_VERSION}")
    print(f"# Results directory: /results/{REF}/{STORE}/")
    print(f"{'#' * 70}")
    
    if "entity_scaling" in SCENARIOS:
        run_entity_scaling()
    if "feature_scaling" in SCENARIOS:
        run_feature_scaling()
    
    print(f"\n{'#' * 70}")
    print(f"# {STORE.upper()} BENCHMARK COMPLETE")
    print(f"{'#' * 70}\n")
