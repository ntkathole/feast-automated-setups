#!/usr/bin/env python3
"""
Feast Bottleneck Analyzer - Detailed function-level profiling.

Generates clear breakdown of where time is spent in get_online_features().
Output formats: table, JSON, and flamegraph-compatible.

Usage:
    python analyze_bottlenecks.py --store sqlite --entities 50 --features 200
    python analyze_bottlenecks.py --store redis --redis-host localhost
"""
import argparse
import cProfile
import json
import os
import pstats
import shutil
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from io import StringIO
from typing import Dict, List, Tuple

import pandas as pd


def setup_feast_repo(repo_path: str, store_type: str, store_config: Dict) -> str:
    """Create a minimal Feast repo for profiling."""
    shutil.rmtree(repo_path, ignore_errors=True)
    os.makedirs(repo_path, exist_ok=True)
    
    online_store_yaml = {
        'sqlite': f"type: sqlite\n  path: {repo_path}/online.db",
        'redis': f"type: redis\n  connection_string: {store_config.get('redis_host', 'localhost')}:{store_config.get('redis_port', 6379)}",
        'postgres': f"""type: postgres
  host: {store_config.get('pg_host', 'localhost')}
  port: {store_config.get('pg_port', 5432)}
  database: {store_config.get('pg_db', 'feast')}
  user: {store_config.get('pg_user', 'feast')}
  password: {store_config.get('pg_pass', 'feast')}""",
        'dynamodb': f"""type: dynamodb
  region: {store_config.get('region', 'us-east-1')}
  batch_size: 100
  max_pool_connections: 200"""
    }
    
    config = f"""project: profile_test
provider: local
registry:
  path: {repo_path}/registry.db
  cache_ttl_seconds: 0
online_store:
  {online_store_yaml.get(store_type, online_store_yaml['sqlite'])}
entity_key_serialization_version: 3
"""
    with open(f"{repo_path}/feature_store.yaml", "w") as f:
        f.write(config)
    
    return repo_path


def create_test_data(repo_path: str, num_features: int, num_entities: int):
    """Create test data and feature definitions."""
    from feast import Entity, FeatureStore, FeatureView, Field, FileSource
    from feast.types import Float64
    
    # Create data
    data = {"user_id": [f"user_{i}" for i in range(num_entities + 100)]}
    for i in range(num_features):
        data[f"f{i}"] = [float(i * j) for j in range(num_entities + 100)]
    df = pd.DataFrame(data)
    df["event_timestamp"] = pd.Timestamp.now()
    df.to_parquet(f"{repo_path}/data.parquet")
    
    # Define features
    source = FileSource(path=f"{repo_path}/data.parquet", timestamp_field="event_timestamp")
    entity = Entity(name="user_id", join_keys=["user_id"])
    schema = [Field(name=f"f{i}", dtype=Float64) for i in range(num_features)]
    fv = FeatureView(name="fv", entities=[entity], schema=schema, source=source, ttl=timedelta(days=1))
    
    fs = FeatureStore(repo_path=repo_path)
    fs.apply([entity, fv])
    fs.materialize(start_date=datetime.now() - timedelta(days=1), end_date=datetime.now())
    fs.refresh_registry()
    
    return fs, [f"fv:f{i}" for i in range(num_features)]


def profile_single_call(fs, features: List[str], entity_rows: List[Dict[str, str]]) -> pstats.Stats:
    """Profile a single get_online_features call."""
    profiler = cProfile.Profile()
    profiler.enable()
    fs.get_online_features(features=features, entity_rows=entity_rows)
    profiler.disable()
    return pstats.Stats(profiler)


def extract_function_times(stats: pstats.Stats) -> Dict[str, Dict]:
    """Extract detailed timing for each function."""
    results = {}
    for (filename, lineno, funcname), (cc, nc, tt, ct, callers) in stats.stats.items():
        # tt = total time in this function (excluding subcalls)
        # ct = cumulative time (including subcalls)
        key = funcname
        if key not in results:
            results[key] = {
                'calls': 0,
                'total_time_ms': 0,
                'cumulative_time_ms': 0,
                'file': filename.split('/')[-1] if '/' in filename else filename,
                'line': lineno
            }
        results[key]['calls'] += nc
        results[key]['total_time_ms'] += tt * 1000
        results[key]['cumulative_time_ms'] += ct * 1000
    
    return results


def categorize_functions(func_times: Dict) -> Dict[str, List]:
    """Group functions by category for clearer analysis."""
    categories = {
        'DB/Store Read': [
            # Generic
            'online_read', '_get_online_features_from_online_store',
            'execute', 'fetchall', '_make_request', 'send', 'recv',
            # SQLite
            'sqlite3.Cursor', 
            # Redis - network I/O and pipeline
            '_read_response', 'read_response', 'readline', '_execute_pipeline',
            'execute_command', 'pipeline_execute_command', 'hmget', 'mget',
            '_get_features_for_entity', 'pack',
            # Postgres
            '_execute_gen', '_execute_send', 'wait',
            # DynamoDB - batch operations and response parsing
            'batch_get_item', '_process_batch_get_response', '_handle_structure',
            '_parse_shape', '_handle_map', 'deserialize', '_deserialize',
            '_SSLSocket', 'ssl.read', '_bytes_from_decode_data', 'b64decode',
        ],
        'Protobuf/Serialization': [
            '_convert_rows_to_protobuf', 'construct_response_feature_vector',
            'SerializeToString', 'ParseFromString', 'MessageToDict',
            'to_proto', 'from_proto', '_serialized_to_proto',
            '_convert_redis_values_to_protobuf',
        ],
        'Entity Processing': [
            'serialize_entity_key', '_prepare_entities_to_read_from_online_store',
            '_get_entity_key', '_get_unique_entities', '_get_entity_key_protos',
        ],
        'Timestamp Handling': [
            'FromDatetime', 'convert_timestamp', '_CheckTimestampValid',
            'ToDatetime', 'FromSeconds', 'ToSeconds',
            # Also include datetime operations often used in timestamp conversion
            'timegm', 'utctimetuple', 'fromtimestamp', 'datetime.replace',
        ],
        'Registry/Metadata': [
            'refresh_registry', '_get_online_request_context',
            'get_feature_view', '_get_feature_views_to_use'
        ],
        'Type Checking': [
            'check_type_internal', 'builtin_checker_lookup', 'get_origin',
        ],
    }
    
    categorized = defaultdict(list)
    uncategorized = []
    
    for func_name, data in func_times.items():
        found = False
        for category, patterns in categories.items():
            if any(p in func_name for p in patterns):
                categorized[category].append((func_name, data))
                found = True
                break
        if not found and data['total_time_ms'] > 0.1:  # Only track significant funcs
            uncategorized.append((func_name, data))
    
    categorized['Other (Significant)'] = uncategorized
    return dict(categorized)


def run_profiling(
    store_type: str,
    num_entities: int,
    num_features: int,
    iterations: int,
    store_config: Dict
) -> Dict:
    """Run profiling and return detailed breakdown."""
    print(f"\n{'='*70}")
    print(f"FEAST BOTTLENECK ANALYSIS")
    print(f"{'='*70}")
    print(f"Store: {store_type} | Entities: {num_entities} | Features: {num_features}")
    print(f"Iterations: {iterations}")
    print(f"{'='*70}\n")
    
    # Setup
    repo_path = "/tmp/feast_profile_test"
    print("Setting up test environment...")
    setup_feast_repo(repo_path, store_type, store_config)
    fs, features = create_test_data(repo_path, num_features, num_entities)
    entity_rows = [{"user_id": f"user_{i}"} for i in range(num_entities)]
    
    # Warmup
    print("Warming up (3 iterations)...")
    for _ in range(3):
        fs.get_online_features(features=features, entity_rows=entity_rows)
    
    # Profile multiple iterations
    print(f"Profiling {iterations} iterations...")
    all_func_times = defaultdict(lambda: {'calls': 0, 'total_time_ms': 0, 'cumulative_time_ms': 0})
    total_times = []
    
    for i in range(iterations):
        start = time.perf_counter()
        stats = profile_single_call(fs, features, entity_rows)
        elapsed = (time.perf_counter() - start) * 1000
        total_times.append(elapsed)
        
        func_times = extract_function_times(stats)
        for func, data in func_times.items():
            all_func_times[func]['calls'] += data['calls']
            all_func_times[func]['total_time_ms'] += data['total_time_ms']
            all_func_times[func]['cumulative_time_ms'] += data['cumulative_time_ms']
            all_func_times[func]['file'] = data['file']
        
        if (i + 1) % 5 == 0:
            print(f"  Progress: {i+1}/{iterations}")
    
    # Average the times
    for func in all_func_times:
        all_func_times[func]['total_time_ms'] /= iterations
        all_func_times[func]['cumulative_time_ms'] /= iterations
        all_func_times[func]['calls'] /= iterations
    
    avg_total = statistics.mean(total_times)
    
    # Categorize and sort
    categorized = categorize_functions(dict(all_func_times))
    
    return {
        'config': {
            'store': store_type,
            'entities': num_entities,
            'features': num_features,
            'iterations': iterations
        },
        'summary': {
            'avg_total_ms': avg_total,
            'p50_ms': sorted(total_times)[len(total_times)//2],
            'p99_ms': sorted(total_times)[-1],
            'min_ms': min(total_times),
            'max_ms': max(total_times)
        },
        'categorized_breakdown': categorized,
        'all_functions': dict(all_func_times)
    }


def print_results(results: Dict):
    """Print formatted results."""
    print(f"\n{'='*70}")
    print("RESULTS SUMMARY")
    print(f"{'='*70}")
    
    summary = results['summary']
    print(f"\nTotal Latency:")
    print(f"  Average: {summary['avg_total_ms']:.2f} ms")
    print(f"  P50:     {summary['p50_ms']:.2f} ms")
    print(f"  P99:     {summary['p99_ms']:.2f} ms")
    print(f"  Range:   {summary['min_ms']:.2f} - {summary['max_ms']:.2f} ms")
    
    avg_total = summary['avg_total_ms']
    
    print(f"\n{'='*70}")
    print("BREAKDOWN BY CATEGORY")
    print(f"{'='*70}")
    
    category_totals = []
    for category, funcs in results['categorized_breakdown'].items():
        cat_total = sum(f[1]['total_time_ms'] for f in funcs)
        cat_pct = (cat_total / avg_total * 100) if avg_total > 0 else 0
        category_totals.append((category, cat_total, cat_pct, funcs))
    
    # Sort by time
    category_totals.sort(key=lambda x: x[1], reverse=True)
    
    for category, total_ms, pct, funcs in category_totals:
        if total_ms < 0.1:
            continue
        print(f"\n{category}: {total_ms:.2f} ms ({pct:.1f}%)")
        print("-" * 50)
        
        # Sort functions within category
        sorted_funcs = sorted(funcs, key=lambda x: x[1]['total_time_ms'], reverse=True)[:10]
        for func_name, data in sorted_funcs:
            if data['total_time_ms'] < 0.05:
                continue
            func_pct = (data['total_time_ms'] / avg_total * 100) if avg_total > 0 else 0
            print(f"  {func_name[:40]:<40} {data['total_time_ms']:>8.2f} ms ({func_pct:>5.1f}%) [{data['calls']:.0f} calls]")
    
    # Top functions overall
    print(f"\n{'='*70}")
    print("TOP 20 FUNCTIONS BY TIME")
    print(f"{'='*70}")
    
    all_funcs = [(k, v) for k, v in results['all_functions'].items()]
    all_funcs.sort(key=lambda x: x[1]['total_time_ms'], reverse=True)
    
    print(f"\n{'Function':<45} {'Time (ms)':>10} {'%':>7} {'Calls':>8}")
    print("-" * 75)
    for func_name, data in all_funcs[:20]:
        pct = (data['total_time_ms'] / avg_total * 100) if avg_total > 0 else 0
        print(f"{func_name[:45]:<45} {data['total_time_ms']:>10.2f} {pct:>6.1f}% {data['calls']:>8.0f}")
    
    # Optimization recommendations
    print(f"\n{'='*70}")
    print("OPTIMIZATION TARGETS")
    print(f"{'='*70}")
    
    for category, total_ms, pct, _ in category_totals[:3]:
        if pct > 10:
            print(f"\n→ {category} ({pct:.1f}% of total)")
            if 'Protobuf' in category:
                print("  - Consider: Batch protobuf operations, lazy deserialization")
                print("  - PR: https://github.com/feast-dev/feast/pull/4613")
            elif 'Timestamp' in category:
                print("  - Consider: Cache timestamp conversions, reduce validation")
                print("  - PR: https://github.com/feast-dev/feast/pull/5006")
            elif 'DB' in category or 'Store' in category:
                print("  - Consider: Connection pooling, batch reads, async I/O")
            elif 'Entity' in category:
                print("  - Consider: entity_key_serialization_version: 3")


def main():
    parser = argparse.ArgumentParser(description='Feast Bottleneck Analyzer')
    parser.add_argument('--store', choices=['sqlite', 'redis', 'postgres', 'dynamodb'],
                       default='sqlite', help='Online store type')
    parser.add_argument('--entities', type=int, default=50, help='Number of entities')
    parser.add_argument('--features', type=int, default=200, help='Number of features')
    parser.add_argument('--iterations', type=int, default=10, help='Profiling iterations')
    parser.add_argument('--output', type=str, help='Output JSON file path')
    
    # Store-specific options
    parser.add_argument('--redis-host', default='localhost')
    parser.add_argument('--redis-port', type=int, default=6379)
    parser.add_argument('--pg-host', default='localhost')
    parser.add_argument('--pg-port', type=int, default=5432)
    parser.add_argument('--pg-db', default='feast')
    parser.add_argument('--pg-user', default='feast')
    parser.add_argument('--pg-pass', default='feast')
    parser.add_argument('--aws-region', default='us-east-1')
    
    args = parser.parse_args()
    
    store_config = {
        'redis_host': args.redis_host,
        'redis_port': args.redis_port,
        'pg_host': args.pg_host,
        'pg_port': args.pg_port,
        'pg_db': args.pg_db,
        'pg_user': args.pg_user,
        'pg_pass': args.pg_pass,
        'region': args.aws_region
    }
    
    results = run_profiling(
        store_type=args.store,
        num_entities=args.entities,
        num_features=args.features,
        iterations=args.iterations,
        store_config=store_config
    )
    
    print_results(results)
    
    if args.output:
        # Convert for JSON serialization
        output_data = {
            'config': results['config'],
            'summary': results['summary'],
            'category_breakdown': {
                cat: [(f[0], f[1]) for f in funcs]
                for cat, funcs in results['categorized_breakdown'].items()
            },
            'top_functions': [
                {'name': k, **v}
                for k, v in sorted(
                    results['all_functions'].items(),
                    key=lambda x: x[1]['total_time_ms'],
                    reverse=True
                )[:50]
            ]
        }
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults saved to: {args.output}")


if __name__ == '__main__':
    main()
