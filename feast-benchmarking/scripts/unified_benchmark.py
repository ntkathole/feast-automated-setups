#!/usr/bin/env python3
"""
Unified Feast Benchmark - Comprehensive testing across ALL dimensions.

OPTIMIZATIONS APPLIED:
- Registry cache_ttl_seconds: 0 (infinite cache during benchmark)
- entity_key_serialization_version: 3 (latest, most efficient)
- DynamoDB: batch_size=100, max_pool_connections=200, adaptive retry
- Registry pre-cached before measurements (fs.refresh_registry())
- 100 iterations (default) for statistical significance
- 10 warmup iterations (default) for steady-state measurement

Tracks and evaluates:
- Features: 10, 50, 100, 200
- Feature Views: 1, 10, 50, 100+
- Entities: 1, 10, 50, 100, 200
- Feature Services: with/without
- Transformations: None, Python, Pandas
- Online Stores: SQLite, Redis, DynamoDB, PostgreSQL
- Throughput: concurrent load testing
- Function-level breakdown: online_read, protobuf_convert, etc.

Usage:
    # Quick test
    python unified_benchmark.py --preset quick --store sqlite

    # Full matrix (optimized)
    python unified_benchmark.py --preset full --store redis --output results_redis
    
    # Production config (optimized)
    python unified_benchmark.py --preset production --store dynamodb --output results_prod
    
    # State Farm SLA validation (fully optimized)
    python unified_benchmark.py --preset statefarm --store redis --output results_statefarm
    
    # Custom dimensions
    python unified_benchmark.py --features 50 200 --entities 1 100 --fv-counts 1 10
"""
import argparse
import cProfile
import io
import json
import os
import pstats
import shutil
import statistics
import sys
import threading
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

import pandas as pd

from feast import Entity, FeatureStore, FeatureView, Field, FileSource
from feast.types import Float64
from feast.value_type import ValueType



# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class TestResult:
    """Result for a single test configuration."""
    # Config
    test_type: str  # "latency", "fv_scaling", "transformation", "throughput"
    num_features: int
    num_entities: int
    num_feature_views: int
    online_store: str
    transformation_mode: Optional[str]
    
    # Latency (ms)
    p50: float
    p95: float
    p99: float
    mean: float
    min_val: float
    max_val: float
    std_dev: float = 0.0
    
    # Throughput
    rps: float = 0.0
    rph: float = 0.0
    
    # Breakdown (%)
    online_read_pct: float = 0.0
    protobuf_convert_pct: float = 0.0
    entity_serial_pct: float = 0.0
    other_pct: float = 0.0
    
    # SLA
    sla_target_ms: float = 60.0
    sla_pass: bool = False
    
    # Meta
    timestamp: str = ""
    feast_version: str = ""


@dataclass 
class BenchmarkSummary:
    """Summary of all benchmark runs."""
    store: str
    feast_version: str
    timestamp: str
    scenario: str = "entity_scaling"  # entity_scaling or feature_scaling
    
    # Results by category
    latency_results: List[TestResult] = field(default_factory=list)
    fv_scaling_results: List[TestResult] = field(default_factory=list)
    transformation_results: List[TestResult] = field(default_factory=list)
    throughput_results: List[Dict] = field(default_factory=list)
    
    # Summary stats
    total_tests: int = 0
    sla_pass_rate: float = 0.0


# ============================================================================
# Core Benchmark Class
# ============================================================================

class UnifiedBenchmark:
    """Unified benchmark runner for all test dimensions."""
    
    def __init__(
        self,
        repo_path: str,
        output_dir: str,
        store_type: str = "sqlite",
        store_config: Optional[Dict] = None,
        scenario: str = "entity_scaling"
    ):
        self.base_repo_path = repo_path
        self.output_dir = output_dir
        self.store_type = store_type
        self.store_config = store_config or {}
        self.scenario = scenario
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Get Feast version
        import feast
        self.feast_version = feast.__version__
        
        self.summary = BenchmarkSummary(
            store=store_type,
            feast_version=self.feast_version,
            timestamp=datetime.now().isoformat(),
            scenario=scenario
        )
    
    # -------------------------------------------------------------------------
    # Setup Helpers
    # -------------------------------------------------------------------------
    
    def _create_repo(self, name: str) -> str:
        """Create a test repository."""
        repo_path = os.path.join(self.base_repo_path, name)
        if os.path.exists(repo_path):
            shutil.rmtree(repo_path)
        os.makedirs(repo_path, exist_ok=True)
        return repo_path
    
    def _create_config(self, repo_path: str) -> None:
        """Create optimized feature_store.yaml with all performance settings."""
        configs = {
            "sqlite": f"""
project: feast_benchmark
provider: local

# OPTIMIZATION: Infinite registry cache during benchmark
registry:
  path: {repo_path}/registry.db
  cache_ttl_seconds: 0

online_store:
  type: sqlite
  path: {repo_path}/online_store.db

# OPTIMIZATION: Latest serialization format
entity_key_serialization_version: 3
""",
            "redis": f"""
project: feast_benchmark
provider: local

# OPTIMIZATION: Infinite registry cache during benchmark
registry:
  path: {repo_path}/registry.db
  cache_ttl_seconds: 0

online_store:
  type: redis
  connection_string: {self.store_config.get('connection_string', 'localhost:6379')}

# OPTIMIZATION: Latest serialization format
entity_key_serialization_version: 3
""",
            "dynamodb": f"""
project: feast_benchmark
provider: aws

# OPTIMIZATION: Infinite registry cache during benchmark
registry:
  path: {repo_path}/registry.db
  cache_ttl_seconds: 0

online_store:
  type: dynamodb
  region: {self.store_config.get('region', 'us-east-1')}
  # OPTIMIZATION: Max batch size for BatchGetItem
  batch_size: 100
  # OPTIMIZATION: Eventual consistency (faster)
  consistent_reads: false
  # OPTIMIZATION: Increased connection pool
  max_pool_connections: 200
  # OPTIMIZATION: Keep connections alive
  keepalive_timeout: 60.0
  # OPTIMIZATION: Faster failure detection
  connect_timeout: 2
  read_timeout: 5
  # OPTIMIZATION: Intelligent retry
  retry_mode: adaptive
  total_max_retry_attempts: 3

# OPTIMIZATION: Latest serialization format
entity_key_serialization_version: 3
""",
            "postgres": f"""
project: feast_benchmark
provider: local

# OPTIMIZATION: Infinite registry cache during benchmark
registry:
  path: {repo_path}/registry.db
  cache_ttl_seconds: 0

online_store:
  type: postgres
  host: {self.store_config.get('host', 'localhost')}
  port: {self.store_config.get('port', 5432)}
  database: {self.store_config.get('database', 'feast')}
  user: {self.store_config.get('user', 'feast')}
  password: {self.store_config.get('password', 'feast')}

# OPTIMIZATION: Latest serialization format
entity_key_serialization_version: 3
""",
        }
        
        config = configs.get(self.store_type, configs["sqlite"])
        with open(os.path.join(repo_path, "feature_store.yaml"), "w") as f:
            f.write(config.strip())
    
    def _create_data_and_fvs(
        self,
        repo_path: str,
        num_fvs: int,
        features_per_fv: int,
        num_entities: int
    ) -> Tuple[Entity, List[FeatureView]]:
        """Create test data and feature views."""
        
        user = Entity(
            name="user_id",
            join_keys=["user_id"],
            value_type=ValueType.STRING
        )
        
        feature_views = []
        
        for fv_idx in range(num_fvs):
            # Generate data
            data = {
                "user_id": [f"user_{i}" for i in range(num_entities)],
                "event_timestamp": [datetime.now()] * num_entities,
            }
            for f_idx in range(features_per_fv):
                data[f"fv{fv_idx}_f{f_idx}"] = [float(f_idx + i * 0.1) for i in range(num_entities)]
            
            df = pd.DataFrame(data)
            parquet_path = os.path.join(repo_path, f"fv_{fv_idx}.parquet")
            df.to_parquet(parquet_path)
            
            source = FileSource(path=parquet_path, timestamp_field="event_timestamp")
            schema = [Field(name=f"fv{fv_idx}_f{f_idx}", dtype=Float64) for f_idx in range(features_per_fv)]
            
            fv = FeatureView(
                name=f"fv_{fv_idx}",
                entities=[user],
                schema=schema,
                source=source,
                ttl=timedelta(days=1)
            )
            feature_views.append(fv)
        
        return user, feature_views
    
    def _simple_request(
        self,
        fs: FeatureStore,
        features: List[str],
        entity_rows: List[Dict]
    ) -> float:
        """Measure a single request latency without profiling overhead."""
        start = time.perf_counter()
        fs.get_online_features(features=features, entity_rows=entity_rows)
        return (time.perf_counter() - start) * 1000
    
    def _profile_request(
        self,
        fs: FeatureStore,
        features: List[str],
        entity_rows: List[Dict]
    ) -> Tuple[float, Dict[str, float]]:
        """Profile a single request and return timing breakdown."""
        
        profiler = cProfile.Profile()
        
        start = time.perf_counter()
        profiler.enable()
        fs.get_online_features(features=features, entity_rows=entity_rows)
        profiler.disable()
        total_ms = (time.perf_counter() - start) * 1000
        
        # Parse stats - use total time (tt) not cumulative (ct) for accurate breakdown
        stream = io.StringIO()
        stats = pstats.Stats(profiler, stream=stream)
        
        function_times = {}
        for func, (cc, nc, tt, ct, callers) in stats.stats.items():
            filename, lineno, funcname = func
            # Use tt (total time in this function) for breakdown
            time_ms = tt * 1000
            if time_ms > 0.01:  # Only track functions > 0.01ms
                function_times[funcname] = function_times.get(funcname, 0) + time_ms
        
        # Calculate breakdown percentages
        online_read = function_times.get('online_read', 0)
        protobuf = function_times.get('_convert_rows_to_protobuf', 0) + function_times.get('construct_response_feature_vector', 0)
        entity_serial = function_times.get('serialize_entity_key', 0)
        timestamp = function_times.get('FromDatetime', 0) + function_times.get('convert_timestamp', 0)
        
        breakdown = {
            'online_read_pct': (online_read / total_ms * 100) if total_ms > 0 else 0,
            'protobuf_convert_pct': (protobuf / total_ms * 100) if total_ms > 0 else 0,
            'entity_serial_pct': (entity_serial / total_ms * 100) if total_ms > 0 else 0,
            'timestamp_pct': (timestamp / total_ms * 100) if total_ms > 0 else 0,
        }
        breakdown['other_pct'] = max(0, 100 - sum(breakdown.values()))
        
        return total_ms, breakdown
    
    def _get_breakdown_for_config(
        self,
        fs: FeatureStore,
        features: List[str],
        entity_rows: List[Dict],
        profile_iterations: int = 5
    ) -> Dict[str, float]:
        """Run profiling pass to get breakdown data (separate from main benchmark)."""
        breakdowns = []
        for _ in range(profile_iterations):
            _, breakdown = self._profile_request(fs, features, entity_rows)
            breakdowns.append(breakdown)
        
        # Average the breakdowns
        return {
            'online_read_pct': statistics.mean([b['online_read_pct'] for b in breakdowns]),
            'protobuf_convert_pct': statistics.mean([b['protobuf_convert_pct'] for b in breakdowns]),
            'entity_serial_pct': statistics.mean([b['entity_serial_pct'] for b in breakdowns]),
            'timestamp_pct': statistics.mean([b.get('timestamp_pct', 0) for b in breakdowns]),
            'other_pct': statistics.mean([b['other_pct'] for b in breakdowns]),
        }
    
    # -------------------------------------------------------------------------
    # Test: Latency Matrix (Features × Entities)
    # -------------------------------------------------------------------------
    
    def run_latency_matrix(
        self,
        feature_counts: List[int] = [50, 200],
        entity_counts: List[int] = [1, 10, 50, 100, 200],
        iterations: int = 100,
        warmup: int = 10,
        profile: bool = True,
        profile_iterations: int = 5
    ) -> List[TestResult]:
        """Run latency tests across features × entities matrix.
        
        Args:
            feature_counts: List of feature counts to test
            entity_counts: List of entity counts to test
            iterations: Number of iterations for latency measurement
            warmup: Number of warmup iterations
            profile: Whether to capture profiling breakdown (adds overhead)
            profile_iterations: Number of iterations for profiling pass (if profile=True)
        """
        
        print("\n" + "=" * 70)
        print("LATENCY MATRIX TEST")
        print(f"Features: {feature_counts} | Entities: {entity_counts}")
        print(f"Iterations: {iterations} | Warmup: {warmup} | Profile: {profile}")
        print("=" * 70)
        
        results = []
        max_features = max(feature_counts)
        max_entities = max(entity_counts)
        
        # Setup once with max dimensions
        repo_path = self._create_repo("latency_test")
        self._create_config(repo_path)
        user, fvs = self._create_data_and_fvs(repo_path, 1, max_features, max_entities)
        
        fs = FeatureStore(repo_path=repo_path)
        fs.apply([user] + fvs)
        fs.materialize(start_date=datetime.now() - timedelta(days=1), end_date=datetime.now())
        
        # OPTIMIZATION: Pre-populate registry cache
        fs.refresh_registry()
        
        total = len(feature_counts) * len(entity_counts)
        current = 0
        
        for num_features in feature_counts:
            for num_entities in entity_counts:
                current += 1
                print(f"\n[{current}/{total}] {num_features}f × {num_entities}e")
                
                features = [f"fv_0:fv0_f{i}" for i in range(num_features)]
                entity_rows = [{"user_id": f"user_{i}"} for i in range(num_entities)]
                
                # Warmup
                for _ in range(warmup):
                    fs.get_online_features(features=features, entity_rows=entity_rows)
                
                # Main benchmark - fast path without profiling overhead
                latencies = []
                for _ in range(iterations):
                    latency = self._simple_request(fs, features, entity_rows)
                    latencies.append(latency)
                
                latencies.sort()
                
                # Separate profiling pass (if enabled) - fewer iterations
                breakdown = {'online_read_pct': 0, 'protobuf_convert_pct': 0, 
                            'entity_serial_pct': 0, 'timestamp_pct': 0, 'other_pct': 100}
                if profile:
                    breakdown = self._get_breakdown_for_config(fs, features, entity_rows, profile_iterations)
                
                result = TestResult(
                    test_type="latency",
                    num_features=num_features,
                    num_entities=num_entities,
                    num_feature_views=1,
                    online_store=self.store_type,
                    transformation_mode=None,
                    p50=latencies[len(latencies)//2],
                    p95=latencies[int(len(latencies)*0.95)],
                    p99=latencies[-1],
                    mean=statistics.mean(latencies),
                    min_val=min(latencies),
                    max_val=max(latencies),
                    std_dev=statistics.stdev(latencies) if len(latencies) > 1 else 0,
                    rps=1000/statistics.mean(latencies) if statistics.mean(latencies) > 0 else 0,
                    online_read_pct=breakdown['online_read_pct'],
                    protobuf_convert_pct=breakdown['protobuf_convert_pct'],
                    entity_serial_pct=breakdown['entity_serial_pct'],
                    other_pct=breakdown['other_pct'],
                    sla_pass=latencies[-1] < 60,
                    timestamp=datetime.now().isoformat(),
                    feast_version=self.feast_version
                )
                results.append(result)
                
                # Calculate reliability metrics
                cv = (result.std_dev / result.mean * 100) if result.mean > 0 else 0
                cv_status = "✓" if cv < 15 else "⚠" if cv < 25 else "✗"
                
                sla = "✅" if result.sla_pass else "❌"
                print(f"  p50: {result.p50:.1f}ms | p95: {result.p95:.1f}ms | p99: {result.p99:.1f}ms {sla}")
                print(f"  mean: {result.mean:.1f}ms | std: {result.std_dev:.1f}ms | CV: {cv:.1f}% {cv_status}")
                if profile:
                    print(f"  Breakdown: read={breakdown['online_read_pct']:.0f}% | proto={breakdown['protobuf_convert_pct']:.0f}% | ts={breakdown.get('timestamp_pct', 0):.0f}% | serial={breakdown['entity_serial_pct']:.0f}%")
        
        self.summary.latency_results.extend(results)
        return results
    
    # -------------------------------------------------------------------------
    # Test: Feature View Scaling
    # -------------------------------------------------------------------------
    
    def run_fv_scaling(
        self,
        fv_counts: List[int] = [1, 10, 50],
        features_per_fv: int = 10,
        num_entities: int = 100,
        iterations: int = 20
    ) -> List[TestResult]:
        """Test scaling with multiple feature views."""
        
        print("\n" + "=" * 70)
        print("FEATURE VIEW SCALING TEST")
        print(f"FV counts: {fv_counts} | Features/FV: {features_per_fv}")
        print("=" * 70)
        
        results = []
        
        for num_fvs in fv_counts:
            print(f"\n[{num_fvs} Feature Views]")
            
            repo_path = self._create_repo(f"fv_test_{num_fvs}")
            self._create_config(repo_path)
            user, fvs = self._create_data_and_fvs(repo_path, num_fvs, features_per_fv, num_entities)
            
            fs = FeatureStore(repo_path=repo_path)
            fs.apply([user] + fvs)
            fs.materialize(start_date=datetime.now() - timedelta(days=1), end_date=datetime.now())
            
            # OPTIMIZATION: Pre-populate registry cache
            fs.refresh_registry()
            
            # All features from all FVs
            all_features = []
            for fv_idx in range(num_fvs):
                for f_idx in range(features_per_fv):
                    all_features.append(f"fv_{fv_idx}:fv{fv_idx}_f{f_idx}")
            
            entity_rows = [{"user_id": f"user_{i}"} for i in range(num_entities)]
            
            # Warmup
            for _ in range(3):
                fs.get_online_features(features=all_features, entity_rows=entity_rows)
            
            # Benchmark
            latencies = []
            for _ in range(iterations):
                start = time.perf_counter()
                fs.get_online_features(features=all_features, entity_rows=entity_rows)
                latencies.append((time.perf_counter() - start) * 1000)
            
            latencies.sort()
            
            result = TestResult(
                test_type="fv_scaling",
                num_features=num_fvs * features_per_fv,
                num_entities=num_entities,
                num_feature_views=num_fvs,
                online_store=self.store_type,
                transformation_mode=None,
                p50=latencies[len(latencies)//2],
                p95=latencies[int(len(latencies)*0.95)],
                p99=latencies[-1],
                mean=statistics.mean(latencies),
                min_val=min(latencies),
                max_val=max(latencies),
                sla_pass=latencies[-1] < 60,
                timestamp=datetime.now().isoformat(),
                feast_version=self.feast_version
            )
            results.append(result)
            
            sla = "✅" if result.sla_pass else "❌"
            print(f"  Total features: {num_fvs * features_per_fv}")
            print(f"  p50: {result.p50:.1f}ms | p95: {result.p95:.1f}ms | p99: {result.p99:.1f}ms {sla}")
        
        self.summary.fv_scaling_results.extend(results)
        return results
    
    # -------------------------------------------------------------------------
    # Test: Transformation Overhead
    # -------------------------------------------------------------------------
    
    def run_transformation_test(
        self,
        num_features: int = 50,
        entity_counts: List[int] = [1, 10, 100],
        iterations: int = 20
    ) -> List[TestResult]:
        """Compare transformation modes: None vs Python vs Pandas."""
        
        print("\n" + "=" * 70)
        print("TRANSFORMATION OVERHEAD TEST")
        print(f"Modes: none, python, pandas | Features: {num_features}")
        print("=" * 70)
        
        results = []
        modes = ["none", "python", "pandas"]
        
        # Setup base store
        repo_path = self._create_repo("transform_test")
        self._create_config(repo_path)
        max_entities = max(entity_counts)
        user, fvs = self._create_data_and_fvs(repo_path, 1, num_features, max_entities)
        
        fs = FeatureStore(repo_path=repo_path)
        fs.apply([user] + fvs)
        fs.materialize(start_date=datetime.now() - timedelta(days=1), end_date=datetime.now())
        
        # OPTIMIZATION: Pre-populate registry cache
        fs.refresh_registry()
        
        features = [f"fv_0:fv0_f{i}" for i in range(num_features)]
        
        for num_entities in entity_counts:
            print(f"\n[{num_entities} entities]")
            entity_rows = [{"user_id": f"user_{i}"} for i in range(num_entities)]
            
            for mode in modes:
                # Warmup
                for _ in range(3):
                    fs.get_online_features(features=features, entity_rows=entity_rows)
                
                latencies = []
                for _ in range(iterations):
                    start = time.perf_counter()
                    result_obj = fs.get_online_features(features=features, entity_rows=entity_rows)
                    
                    # Simulate transformation overhead
                    if mode == "python":
                        # Python mode: iterate and transform
                        df = result_obj.to_df()
                        feature_cols = [c for c in df.columns if c.startswith("fv0_f")]
                        if feature_cols:
                            _ = df[feature_cols].sum(axis=1).tolist()
                    elif mode == "pandas":
                        # Pandas mode: DataFrame operations
                        df = result_obj.to_df()
                        feature_cols = [c for c in df.columns if c.startswith("fv0_f")]
                        if feature_cols:
                            _ = df[feature_cols].mean(axis=1)
                            _ = df[feature_cols].std(axis=1)
                    
                    latencies.append((time.perf_counter() - start) * 1000)
                
                latencies.sort()
                
                result = TestResult(
                    test_type="transformation",
                    num_features=num_features,
                    num_entities=num_entities,
                    num_feature_views=1,
                    online_store=self.store_type,
                    transformation_mode=mode,
                    p50=latencies[len(latencies)//2],
                    p95=latencies[int(len(latencies)*0.95)],
                    p99=latencies[-1],
                    mean=statistics.mean(latencies),
                    min_val=min(latencies),
                    max_val=max(latencies),
                    sla_pass=latencies[-1] < 60,
                    timestamp=datetime.now().isoformat(),
                    feast_version=self.feast_version
                )
                results.append(result)
                
                print(f"  {mode.upper():8} p50: {result.p50:.1f}ms | p99: {result.p99:.1f}ms")
        
        self.summary.transformation_results.extend(results)
        return results
    
    # -------------------------------------------------------------------------
    # Test: Throughput
    # -------------------------------------------------------------------------
    
    def run_throughput_test(
        self,
        num_features: int = 50,
        num_entities: int = 10,
        worker_counts: List[int] = [1, 5, 10, 20],
        duration: int = 10
    ) -> List[Dict]:
        """Test throughput with concurrent workers."""
        
        print("\n" + "=" * 70)
        print("THROUGHPUT TEST")
        print(f"Workers: {worker_counts} | Duration: {duration}s")
        print("=" * 70)
        
        repo_path = self._create_repo("throughput_test")
        self._create_config(repo_path)
        user, fvs = self._create_data_and_fvs(repo_path, 1, num_features, 500)
        
        fs = FeatureStore(repo_path=repo_path)
        fs.apply([user] + fvs)
        fs.materialize(start_date=datetime.now() - timedelta(days=1), end_date=datetime.now())
        
        # OPTIMIZATION: Pre-populate registry cache
        fs.refresh_registry()
        
        features = [f"fv_0:fv0_f{i}" for i in range(num_features)]
        entity_rows = [{"user_id": f"user_{i}"} for i in range(num_entities)]
        
        results = []
        
        for workers in worker_counts:
            print(f"\n[{workers} workers for {duration}s]")
            
            # Shared state with lock
            lock = threading.Lock()
            state = {"success": 0, "errors": 0, "latencies": []}
            stop_event = threading.Event()
            
            def worker_fn():
                local_latencies = []
                while not stop_event.is_set():
                    try:
                        start = time.perf_counter()
                        fs.get_online_features(features=features, entity_rows=entity_rows)
                        local_latencies.append((time.perf_counter() - start) * 1000)
                        with lock:
                            state["success"] += 1
                    except Exception:
                        with lock:
                            state["errors"] += 1
                
                with lock:
                    state["latencies"].extend(local_latencies)
            
            threads = [threading.Thread(target=worker_fn) for _ in range(workers)]
            for t in threads:
                t.start()
            
            time.sleep(duration)
            stop_event.set()
            
            for t in threads:
                t.join()
            
            rps = state["success"] / duration
            rph = rps * 3600
            latencies = sorted(state["latencies"])
            
            result = {
                "workers": workers,
                "duration_s": duration,
                "total_requests": state["success"],
                "errors": state["errors"],
                "rps": round(rps, 1),
                "rph": int(rph),
                "target_3m_pct": round((rph / 3_000_000) * 100, 1),
                "avg_latency_ms": round(statistics.mean(latencies), 1) if latencies else 0,
                "p99_latency_ms": round(latencies[int(len(latencies)*0.99)], 1) if len(latencies) > 100 else round(max(latencies), 1) if latencies else 0,
            }
            results.append(result)
            
            print(f"  RPS: {result['rps']} | RPH: {result['rph']:,}")
            print(f"  Target (3M): {result['target_3m_pct']}%")
            print(f"  Avg latency: {result['avg_latency_ms']}ms | p99: {result['p99_latency_ms']}ms")
        
        self.summary.throughput_results.extend(results)
        return results
    
    # -------------------------------------------------------------------------
    # Results & Reporting
    # -------------------------------------------------------------------------
    
    def save_results(self, prefix: str = "benchmark"):
        """Save all results to JSON, CSV, and Markdown."""
        
        # Calculate summary stats
        all_results = (
            self.summary.latency_results +
            self.summary.fv_scaling_results +
            self.summary.transformation_results
        )
        self.summary.total_tests = len(all_results)
        if all_results:
            self.summary.sla_pass_rate = sum(1 for r in all_results if r.sla_pass) / len(all_results) * 100
        
        # JSON
        json_path = os.path.join(self.output_dir, f"{prefix}_results.json")
        with open(json_path, "w") as f:
            json.dump({
                "summary": {
                    "store": self.summary.store,
                    "feast_version": self.summary.feast_version,
                    "timestamp": self.summary.timestamp,
                    "total_tests": self.summary.total_tests,
                    "sla_pass_rate": self.summary.sla_pass_rate,
                },
                "latency": [asdict(r) for r in self.summary.latency_results],
                "fv_scaling": [asdict(r) for r in self.summary.fv_scaling_results],
                "transformations": [asdict(r) for r in self.summary.transformation_results],
                "throughput": self.summary.throughput_results,
            }, f, indent=2, default=str)
        print(f"\nSaved: {json_path}")
        
        # CSV
        csv_data = []
        for r in all_results:
            csv_data.append({
                "test_type": r.test_type,
                "features": r.num_features,
                "entities": r.num_entities,
                "fvs": r.num_feature_views,
                "store": r.online_store,
                "transform": r.transformation_mode or "none",
                "p50_ms": round(r.p50, 2),
                "p95_ms": round(r.p95, 2),
                "p99_ms": round(r.p99, 2),
                "sla_pass": r.sla_pass,
                "online_read_pct": round(r.online_read_pct, 1),
                "protobuf_pct": round(r.protobuf_convert_pct, 1),
            })
        
        csv_path = os.path.join(self.output_dir, f"{prefix}_summary.csv")
        pd.DataFrame(csv_data).to_csv(csv_path, index=False)
        print(f"Saved: {csv_path}")
        
        # Markdown
        md_path = os.path.join(self.output_dir, f"{prefix}_report.md")
        self._generate_report(md_path)
        print(f"Saved: {md_path}")
    
    def _generate_report(self, output_path: str):
        """Generate comprehensive markdown report."""
        
        report = f"""# Feast Benchmark Report - {self.store_type.upper()}

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Feast Version:** {self.feast_version}  
**Online Store:** {self.store_type}  
**SLA Target:** 60ms p99

## Summary

- **Total Tests:** {self.summary.total_tests}
- **SLA Pass Rate:** {self.summary.sla_pass_rate:.1f}%

---

## Latency Matrix (Features × Entities)

| Features | Entities | p50 (ms) | p95 (ms) | p99 (ms) | SLA | Breakdown |
|----------|----------|----------|----------|----------|-----|-----------|
"""
        for r in self.summary.latency_results:
            sla = "✅" if r.sla_pass else "❌"
            breakdown = f"read={r.online_read_pct:.0f}% proto={r.protobuf_convert_pct:.0f}%"
            report += f"| {r.num_features} | {r.num_entities} | {r.p50:.1f} | {r.p95:.1f} | {r.p99:.1f} | {sla} | {breakdown} |\n"
        
        report += """
---

## Feature View Scaling

| FVs | Total Features | Entities | p99 (ms) | SLA |
|-----|----------------|----------|----------|-----|
"""
        for r in self.summary.fv_scaling_results:
            sla = "✅" if r.sla_pass else "❌"
            report += f"| {r.num_feature_views} | {r.num_features} | {r.num_entities} | {r.p99:.1f} | {sla} |\n"
        
        report += """
---

## Transformation Overhead

| Mode | Entities | p50 (ms) | p99 (ms) | SLA |
|------|----------|----------|----------|-----|
"""
        for r in self.summary.transformation_results:
            sla = "✅" if r.sla_pass else "❌"
            report += f"| {r.transformation_mode.upper()} | {r.num_entities} | {r.p50:.1f} | {r.p99:.1f} | {sla} |\n"
        
        report += """
---

## Throughput

| Workers | RPS | RPH | Target (3M) | Avg Latency | p99 Latency |
|---------|-----|-----|-------------|-------------|-------------|
"""
        for r in self.summary.throughput_results:
            report += f"| {r['workers']} | {r['rps']} | {r['rph']:,} | {r['target_3m_pct']}% | {r['avg_latency_ms']}ms | {r['p99_latency_ms']}ms |\n"
        
        report += """
---

## Key Findings

"""
        # Auto-generate findings
        if self.summary.latency_results:
            worst = max(self.summary.latency_results, key=lambda x: x.p99)
            report += f"- **Worst case latency:** {worst.p99:.1f}ms at {worst.num_features}f × {worst.num_entities}e\n"
            if worst.protobuf_convert_pct > 30:
                report += f"- **Primary bottleneck:** protobuf conversion ({worst.protobuf_convert_pct:.0f}%)\n"
        
        if self.summary.fv_scaling_results:
            worst_fv = max(self.summary.fv_scaling_results, key=lambda x: x.p99)
            report += f"- **FV scaling impact:** {worst_fv.num_feature_views} FVs → {worst_fv.p99:.1f}ms p99\n"
        
        if self.summary.throughput_results:
            best_tput = max(self.summary.throughput_results, key=lambda x: x['rph'])
            report += f"- **Peak throughput:** {best_tput['rph']:,} RPH ({best_tput['target_3m_pct']}% of 3M target)\n"
        
        with open(output_path, "w") as f:
            f.write(report)
    
# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Unified Feast Benchmark - All dimensions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick test with SQLite (local, no external deps)
  python unified_benchmark.py --preset quick --store sqlite

  # Redis with explicit connection
  python unified_benchmark.py --preset quick \\
      --store redis \\
      --redis-host redis.feast-test.svc.cluster.local \\
      --redis-port 6379 \\
      --redis-password ""

  # DynamoDB with explicit AWS credentials
  python unified_benchmark.py --preset quick \\
      --store dynamodb \\
      --dynamodb-region eu-west-1 \\
      --aws-access-key-id AKIAXXXXXXXX \\
      --aws-secret-access-key XXXXXXXX

  # DynamoDB using environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
  python unified_benchmark.py --preset quick --store dynamodb --dynamodb-region eu-west-1

  # PostgreSQL with full connection details
  python unified_benchmark.py --preset quick \\
      --store postgres \\
      --postgres-host localhost \\
      --postgres-port 5432 \\
      --postgres-database feast \\
      --postgres-user feast \\
      --postgres-password feast123

  # Full matrix test
  python unified_benchmark.py --preset full \\
      --store redis \\
      --redis-host localhost \\
      --features 50 100 200 \\
      --entities 1 10 50 100 200

  # Custom dimensions
  python unified_benchmark.py \\
      --store sqlite \\
      --features 50 200 \\
      --entities 1 10 100 \\
      --fv-counts 1 10 \\
      --iterations 30

  # Skip specific tests
  python unified_benchmark.py --preset quick --store sqlite --skip-throughput --skip-fv-scaling

Environment Variables (alternative to CLI args):
  AWS_ACCESS_KEY_ID       - AWS access key for DynamoDB
  AWS_SECRET_ACCESS_KEY   - AWS secret key for DynamoDB
  AWS_DEFAULT_REGION      - Default AWS region
  REDIS_HOST              - Redis hostname
  REDIS_PORT              - Redis port
  REDIS_PASSWORD          - Redis password
  POSTGRES_HOST           - PostgreSQL hostname
  POSTGRES_PORT           - PostgreSQL port
  POSTGRES_DATABASE       - PostgreSQL database name
  POSTGRES_USER           - PostgreSQL username
  POSTGRES_PASSWORD       - PostgreSQL password
"""
    )
    
    # =========================================================================
    # Preset configs
    # =========================================================================
    preset_group = parser.add_argument_group('Presets')
    preset_group.add_argument("--preset", choices=["quick", "full", "production", "statefarm", "cross-region", "compression"],
                             help="Use preset configuration (overrides dimension args)")
    
    # =========================================================================
    # Online Store Selection & Configuration
    # =========================================================================
    store_group = parser.add_argument_group('Online Store')
    store_group.add_argument("--store", choices=["sqlite", "redis", "dynamodb", "postgres"],
                            default="sqlite", help="Online store type (default: sqlite)")
    
    # Redis configuration
    redis_group = parser.add_argument_group('Redis Configuration')
    redis_group.add_argument("--redis-host", 
                            default=os.environ.get("REDIS_HOST", "localhost"),
                            help="Redis hostname (default: localhost, env: REDIS_HOST)")
    redis_group.add_argument("--redis-port", type=int,
                            default=int(os.environ.get("REDIS_PORT", "6379")),
                            help="Redis port (default: 6379, env: REDIS_PORT)")
    redis_group.add_argument("--redis-password",
                            default=os.environ.get("REDIS_PASSWORD", ""),
                            help="Redis password (default: none, env: REDIS_PASSWORD)")
    redis_group.add_argument("--redis-ssl", action="store_true",
                            default=os.environ.get("REDIS_SSL", "").lower() == "true",
                            help="Enable Redis SSL (env: REDIS_SSL)")
    
    # DynamoDB configuration
    dynamodb_group = parser.add_argument_group('DynamoDB Configuration')
    dynamodb_group.add_argument("--dynamodb-region",
                               default=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                               help="AWS region for DynamoDB (default: us-east-1, env: AWS_DEFAULT_REGION)")
    dynamodb_group.add_argument("--dynamodb-table-prefix",
                               default="feast_benchmark_",
                               help="DynamoDB table name prefix (default: feast_benchmark_)")
    dynamodb_group.add_argument("--aws-access-key-id",
                               default=os.environ.get("AWS_ACCESS_KEY_ID"),
                               help="AWS access key ID (env: AWS_ACCESS_KEY_ID)")
    dynamodb_group.add_argument("--aws-secret-access-key",
                               default=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                               help="AWS secret access key (env: AWS_SECRET_ACCESS_KEY)")
    dynamodb_group.add_argument("--aws-session-token",
                               default=os.environ.get("AWS_SESSION_TOKEN"),
                               help="AWS session token for temporary credentials (env: AWS_SESSION_TOKEN)")
    dynamodb_group.add_argument("--aws-endpoint-url",
                               default=os.environ.get("AWS_ENDPOINT_URL"),
                               help="Custom AWS endpoint URL (for LocalStack, env: AWS_ENDPOINT_URL)")
    
    # PostgreSQL configuration
    postgres_group = parser.add_argument_group('PostgreSQL Configuration')
    postgres_group.add_argument("--postgres-host",
                               default=os.environ.get("POSTGRES_HOST", "localhost"),
                               help="PostgreSQL hostname (default: localhost, env: POSTGRES_HOST)")
    postgres_group.add_argument("--postgres-port", type=int,
                               default=int(os.environ.get("POSTGRES_PORT", "5432")),
                               help="PostgreSQL port (default: 5432, env: POSTGRES_PORT)")
    postgres_group.add_argument("--postgres-database",
                               default=os.environ.get("POSTGRES_DATABASE", "feast"),
                               help="PostgreSQL database name (default: feast, env: POSTGRES_DATABASE)")
    postgres_group.add_argument("--postgres-user",
                               default=os.environ.get("POSTGRES_USER", "feast"),
                               help="PostgreSQL username (default: feast, env: POSTGRES_USER)")
    postgres_group.add_argument("--postgres-password",
                               default=os.environ.get("POSTGRES_PASSWORD", "feast"),
                               help="PostgreSQL password (default: feast, env: POSTGRES_PASSWORD)")
    postgres_group.add_argument("--postgres-sslmode",
                               default=os.environ.get("POSTGRES_SSLMODE", "disable"),
                               choices=["disable", "require", "verify-ca", "verify-full"],
                               help="PostgreSQL SSL mode (default: disable, env: POSTGRES_SSLMODE)")
    
    # =========================================================================
    # Test Dimensions
    # =========================================================================
    dim_group = parser.add_argument_group('Test Dimensions')
    dim_group.add_argument("--features", nargs='+', type=int, default=[10, 50, 100, 200],
                          help="Feature counts to test (default: 10 50 100 200)")
    dim_group.add_argument("--entities", nargs='+', type=int, default=[1, 10, 50, 100, 200],
                          help="Entity counts to test (default: 1 10 50 100 200) - includes State Farm 50,200")
    dim_group.add_argument("--fv-counts", nargs='+', type=int, default=[1, 10, 50, 100],
                          help="Feature View counts to test (default: 1 10 50 100)")
    dim_group.add_argument("--feature-services", nargs='+', type=int, default=[1],
                          help="Feature Service counts to test (default: 1)")
    dim_group.add_argument("--features-per-fv", type=int, default=10,
                          help="Features per Feature View in FV scaling test (default: 10)")
    dim_group.add_argument("--transformations", nargs='+', default=["none"],
                          choices=["none", "python", "pandas"],
                          help="Transformation modes to test (default: none)")
    dim_group.add_argument("--compression", default="none",
                          choices=["none", "gzip"],
                          help="HTTP compression mode (default: none)")
    
    # =========================================================================
    # Test Control
    # =========================================================================
    control_group = parser.add_argument_group('Test Control')
    control_group.add_argument("--scenario", type=str, default="entity_scaling",
                              choices=["entity_scaling", "feature_scaling"],
                              help="Benchmark scenario: entity_scaling (vary entities) or feature_scaling (vary features)")
    control_group.add_argument("--iterations", type=int, default=300,
                              help="Iterations per test configuration (default: 300 for reliability)")
    control_group.add_argument("--warmup", type=int, default=20,
                              help="Warmup iterations before measurement (default: 20 for stability)")
    control_group.add_argument("--passes", type=int, default=1,
                              help="Number of benchmark passes (default: 1, use 3 for higher reliability)")
    control_group.add_argument("--cv-threshold", type=float, default=15.0,
                              help="Coefficient of variation threshold %% for reliability warning (default: 15)")
    control_group.add_argument("--profile", action='store_true', default=False,
                              help="Enable profiling to capture function breakdown (adds overhead)")
    control_group.add_argument("--profile-iterations", type=int, default=5,
                              help="Number of iterations for profiling pass (default: 5)")
    control_group.add_argument("--throughput-duration", type=int, default=10,
                              help="Throughput test duration in seconds (default: 10)")
    control_group.add_argument("--throughput-workers", nargs='+', type=int, default=[1, 5, 10],
                              help="Worker counts for throughput test (default: 1 5 10)")
    
    # =========================================================================
    # Skip Tests
    # =========================================================================
    skip_group = parser.add_argument_group('Skip Tests')
    skip_group.add_argument("--skip-latency", action="store_true",
                           help="Skip latency matrix test")
    skip_group.add_argument("--skip-fv-scaling", action="store_true",
                           help="Skip Feature View scaling test")
    skip_group.add_argument("--skip-transformations", action="store_true",
                           help="Skip transformation overhead test")
    skip_group.add_argument("--skip-throughput", action="store_true",
                           help="Skip throughput test")
    
    # =========================================================================
    # Output & Paths
    # =========================================================================
    output_group = parser.add_argument_group('Output')
    output_group.add_argument("--output", default="results",
                             help="Output directory for results (default: results)")
    output_group.add_argument("--repo-path", default="/tmp/feast_benchmark",
                             help="Feast repository path (default: /tmp/feast_benchmark)")
    output_group.add_argument("--verbose", "-v", action="store_true",
                             help="Verbose output")
    
    # =========================================================================
    # SLA Targets
    # =========================================================================
    sla_group = parser.add_argument_group('SLA Targets')
    sla_group.add_argument("--sla-p99-ms", type=float, default=60.0,
                          help="p99 latency SLA target in milliseconds (default: 60)")
    sla_group.add_argument("--sla-throughput-rph", type=int, default=3_000_000,
                          help="Throughput SLA target in requests per hour (default: 3000000)")
    
    args = parser.parse_args()
    
    # =========================================================================
    # Validate store-specific args
    # =========================================================================
    if args.store == "dynamodb":
        if not args.aws_access_key_id and not os.environ.get("AWS_ACCESS_KEY_ID"):
            print("WARNING: No AWS credentials provided. Using default credential chain.")
            print("         Set --aws-access-key-id/--aws-secret-access-key or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY")
    
    # =========================================================================
    # Store-specific iteration/warmup defaults for reliability
    # =========================================================================
    STORE_DEFAULTS = {
        'sqlite':   {'iterations': 200, 'warmup': 10},   # Low variance, local
        'redis':    {'iterations': 300, 'warmup': 20},   # Fast, moderate variance
        'postgres': {'iterations': 300, 'warmup': 25},   # Network + connection pool
        'dynamodb': {'iterations': 500, 'warmup': 30},   # High variance (AWS network)
    }
    
    # Apply store-specific defaults only if user didn't override
    store_defaults = STORE_DEFAULTS.get(args.store, {'iterations': 300, 'warmup': 20})
    if args.iterations == 300:  # Default wasn't changed
        args.iterations = store_defaults['iterations']
    if args.warmup == 20:  # Default wasn't changed
        args.warmup = store_defaults['warmup']
    
    print(f"Store-specific config for {args.store}: {args.iterations} iterations, {args.warmup} warmup")
    
    if args.store == "redis":
        print(f"Redis: {args.redis_host}:{args.redis_port} (SSL: {args.redis_ssl})")
    
    if args.store == "postgres":
        print(f"PostgreSQL: {args.postgres_user}@{args.postgres_host}:{args.postgres_port}/{args.postgres_database}")
    
    # =========================================================================
    # Apply presets (override dimension args)
    # =========================================================================
    if args.preset == "quick":
        args.features = [50]
        args.entities = [1, 10, 100]
        args.fv_counts = [1]
        args.feature_services = [1]
        args.transformations = ["none"]
        args.compression = "none"
        args.iterations = 10
        args.throughput_duration = 5
        args.throughput_workers = [1, 5]
        args.skip_transformations = True
    elif args.preset == "full":
        # All dimensions from TRACKER.md - OPTIMIZED
        args.features = [10, 50, 100, 200]
        args.entities = [1, 10, 50, 100, 200]
        args.fv_counts = [1, 10, 50, 100]
        args.feature_services = [1, 5, 10]
        args.transformations = ["none", "python", "pandas"]
        args.compression = "none"
        args.iterations = 50  # OPTIMIZATION: More iterations
        args.warmup = 10  # OPTIMIZATION: Extended warmup
        args.throughput_duration = 30
        args.throughput_workers = [1, 5, 10, 20]
    elif args.preset == "production":
        # Production preset - OPTIMIZED
        args.features = [200]
        args.entities = [1, 10, 50, 100, 200]  # Includes State Farm 50, 200
        args.fv_counts = [1, 10, 50, 100]
        args.feature_services = [1, 5, 10]
        args.transformations = ["none", "python", "pandas"]
        args.compression = "none"
        args.iterations = 100  # OPTIMIZATION: More iterations for statistical significance
        args.warmup = 10  # OPTIMIZATION: Extended warmup
        args.throughput_duration = 60
        args.throughput_workers = [1, 5, 10, 20, 50]
    elif args.preset == "statefarm":
        # State Farm exact SLA requirements: 60ms p99, 3M/hour
        # OPTIMIZED: All performance tuning applied
        args.features = [200]
        args.entities = [1, 10, 50, 100, 200]  # Extended entity range for comprehensive testing
        args.fv_counts = [1]
        args.feature_services = [1]
        args.transformations = ["none"]
        args.compression = "none"
        args.iterations = 100  # OPTIMIZATION: More iterations for statistical significance
        args.warmup = 10  # OPTIMIZATION: Extended warmup for steady-state
        args.throughput_duration = 60
        args.throughput_workers = [10, 20, 50]
        args.sla_p99_ms = 60.0
        args.sla_throughput_rph = 3_000_000
    elif args.preset == "cross-region":
        # Cross-region latency test (DynamoDB)
        args.features = [200]
        args.entities = [1, 10, 50, 100]
        args.fv_counts = [1]
        args.feature_services = [1]
        args.transformations = ["none"]
        args.compression = "none"
        args.iterations = 30
        args.throughput_duration = 30
        args.throughput_workers = [10, 20]
    elif args.preset == "compression":
        # Compression impact test
        args.features = [200]
        args.entities = [50, 100, 200]
        args.fv_counts = [1]
        args.feature_services = [1]
        args.transformations = ["none"]
        args.compression = "gzip"
        args.iterations = 30
        args.throughput_duration = 30
    
    # =========================================================================
    # Build store config
    # =========================================================================
    store_config = {}
    
    if args.store == "redis":
        # Feast Redis expects: host:port or redis://host:port or :password@host:port
        if args.redis_password:
            connection_string = f":{args.redis_password}@{args.redis_host}:{args.redis_port}"
        else:
            connection_string = f"{args.redis_host}:{args.redis_port}"
        if args.redis_ssl:
            connection_string = f"rediss://{connection_string}"
        store_config = {
            "connection_string": connection_string,
            "redis_ssl": args.redis_ssl,
        }
    elif args.store == "dynamodb":
        store_config = {
            "region": args.dynamodb_region,
            "table_name_prefix": args.dynamodb_table_prefix,
        }
        # Set AWS credentials in environment for boto3
        if args.aws_access_key_id:
            os.environ["AWS_ACCESS_KEY_ID"] = args.aws_access_key_id
        if args.aws_secret_access_key:
            os.environ["AWS_SECRET_ACCESS_KEY"] = args.aws_secret_access_key
        if args.aws_session_token:
            os.environ["AWS_SESSION_TOKEN"] = args.aws_session_token
        if args.aws_endpoint_url:
            store_config["endpoint_url"] = args.aws_endpoint_url
    elif args.store == "postgres":
        store_config = {
            "host": args.postgres_host,
            "port": args.postgres_port,
            "database": args.postgres_database,
            "user": args.postgres_user,
            "password": args.postgres_password,
            "sslmode": args.postgres_sslmode,
        }
    
    # =========================================================================
    # Print configuration summary
    # =========================================================================
    print("=" * 70)
    print("UNIFIED FEAST BENCHMARK")
    print("=" * 70)
    print(f"Preset:           {args.preset or 'custom'}")
    print(f"Scenario:         {args.scenario}")
    print(f"Online Store:     {args.store.upper()}")
    print("-" * 70)
    
    # Store-specific config
    if args.store == "sqlite":
        print(f"  Path:           {args.repo_path}/data/online.db")
    elif args.store == "redis":
        print(f"  Host:           {args.redis_host}:{args.redis_port}")
        print(f"  SSL:            {args.redis_ssl}")
        print(f"  Password:       {'***' if args.redis_password else '(none)'}")
    elif args.store == "dynamodb":
        print(f"  Region:         {args.dynamodb_region}")
        print(f"  Table Prefix:   {args.dynamodb_table_prefix}")
        print(f"  AWS Key ID:     {args.aws_access_key_id[:8] + '...' if args.aws_access_key_id else '(from env/chain)'}")
        if args.aws_endpoint_url:
            print(f"  Endpoint URL:   {args.aws_endpoint_url}")
    elif args.store == "postgres":
        print(f"  Host:           {args.postgres_host}:{args.postgres_port}")
        print(f"  Database:       {args.postgres_database}")
        print(f"  User:           {args.postgres_user}")
        print(f"  SSL Mode:       {args.postgres_sslmode}")
    
    print("-" * 70)
    print(f"Features:         {args.features}")
    print(f"Entities:         {args.entities}")
    print(f"FV Counts:        {args.fv_counts}")
    print(f"Iterations:       {args.iterations}")
    print(f"Warmup:           {args.warmup}")
    print("-" * 70)
    print(f"SLA p99:          {args.sla_p99_ms}ms")
    print(f"SLA Throughput:   {args.sla_throughput_rph:,} RPH")
    print("-" * 70)
    print(f"Output Dir:       {args.output}")
    print(f"Repo Path:        {args.repo_path}")
    print("-" * 70)
    
    tests = []
    if not args.skip_latency:
        tests.append("latency-matrix")
    if not args.skip_fv_scaling:
        tests.append("fv-scaling")
    if not args.skip_transformations:
        tests.append("transformations")
    if not args.skip_throughput:
        tests.append("throughput")
    print(f"Tests:            {', '.join(tests) if tests else '(none)'}")
    print("=" * 70)
    print()
    
    benchmark = UnifiedBenchmark(
        repo_path=args.repo_path,
        output_dir=args.output,
        store_type=args.store,
        store_config=store_config,
        scenario=args.scenario
    )
    
    # Run tests
    if not args.skip_latency:
        benchmark.run_latency_matrix(
            feature_counts=args.features,
            entity_counts=args.entities,
            iterations=args.iterations,
            warmup=args.warmup,
            profile=args.profile,
            profile_iterations=args.profile_iterations
        )
    
    if not args.skip_fv_scaling:
        benchmark.run_fv_scaling(
            fv_counts=args.fv_counts,
            features_per_fv=args.features_per_fv,
            num_entities=100,
            iterations=args.iterations
        )
    
    if not args.skip_transformations:
        benchmark.run_transformation_test(
            num_features=50,
            entity_counts=[1, 10, 100],
            iterations=args.iterations
        )
    
    if not args.skip_throughput:
        benchmark.run_throughput_test(
            num_features=50,
            num_entities=10,
            worker_counts=args.throughput_workers,
            duration=args.throughput_duration
        )
    
    # Save results
    benchmark.save_results()
    
    print("\n" + "=" * 70)
    print("BENCHMARK COMPLETE")
    print(f"Results: {args.output}/")
    print("To generate comparison charts, run: python generate_charts.py")
    print("=" * 70)


if __name__ == "__main__":
    main()
