"""
Feature definitions for the Feast Prometheus metrics demo.

Provides:
  - Two batch feature views (one from file, one from push source)
  - A Pandas-mode read-path ODFV with track_metrics=True
  - A Python-mode read-path ODFV with track_metrics=True
  - A Python-mode write-path ODFV with track_metrics=True and write_to_online_store=True
  - Feature services exercising different combinations

This exercises all metric categories including Pandas vs Python ODFV
transformation timing comparison and write-path transform timing.
"""

from datetime import timedelta
from typing import Any, Dict

import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
)
from feast.on_demand_feature_view import OnDemandFeatureView, on_demand_feature_view
from feast.transformation.python_transformation import PythonTransformation
from feast.types import Float32, Float64, Int64

driver = Entity(name="driver", join_keys=["driver_id"])

driver_stats_source = FileSource(
    name="driver_hourly_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_stats_source,
    tags={"team": "driver_performance"},
)

input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name="val_to_add", dtype=Int64),
        Field(name="val_to_add_2", dtype=Int64),
    ],
)


# ── Read-path ODFV: Pandas mode ──────────────────────────────────────
@on_demand_feature_view(
    sources=[driver_stats_fv, input_request],
    schema=[
        Field(name="conv_rate_plus_val1", dtype=Float64),
        Field(name="conv_rate_plus_val2", dtype=Float64),
    ],
    track_metrics=True,
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]
    return df


# ── Read-path ODFV: Python mode ──────────────────────────────────────
@on_demand_feature_view(
    sources=[driver_stats_fv, input_request],
    schema=[
        Field(name="conv_rate_plus_val1_py", dtype=Float64),
        Field(name="conv_rate_plus_val2_py", dtype=Float64),
    ],
    mode="python",
    track_metrics=True,
)
def transformed_conv_rate_python(inputs: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "conv_rate_plus_val1_py": inputs["conv_rate"] + inputs["val_to_add"],
        "conv_rate_plus_val2_py": inputs["conv_rate"] + inputs["val_to_add_2"],
    }


# ── Write-path ODFV: Python mode ─────────────────────────────────────
# Computes trip_score = conv_rate * acc_rate * avg_daily_trips.
# write_to_online_store=True means this is transformed during
# write_to_online_store() calls, and the transform time is captured by
# feast_feature_server_write_transformation_duration_seconds.
def _trip_score_udf(inputs: Dict[str, Any]) -> Dict[str, Any]:
    conv = inputs["conv_rate"]
    acc = inputs["acc_rate"]
    trips = inputs["avg_daily_trips"]
    return {"trip_score": [float(c) * float(a) * float(t) for c, a, t in zip(conv, acc, trips)]}


trip_score_odfv = OnDemandFeatureView(
    name="trip_score",
    entities=[driver],
    sources=[driver_stats_fv],
    schema=[Field(name="trip_score", dtype=Float64)],
    feature_transformation=PythonTransformation(
        udf=_trip_score_udf, udf_string="trip_score_udf"
    ),
    mode="python",
    write_to_online_store=True,
    track_metrics=True,
)


driver_stats_push_source = PushSource(
    name="driver_stats_push_source",
    batch_source=driver_stats_source,
)

driver_stats_fresh_fv = FeatureView(
    name="driver_hourly_stats_fresh",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_stats_push_source,
    tags={"team": "driver_performance"},
)

driver_activity_v1 = FeatureService(
    name="driver_activity_v1",
    features=[
        driver_stats_fv[["conv_rate"]],
        transformed_conv_rate,
    ],
)

driver_activity_v2 = FeatureService(
    name="driver_activity_v2",
    features=[driver_stats_fv, transformed_conv_rate],
)

driver_activity_v3 = FeatureService(
    name="driver_activity_v3",
    features=[driver_stats_fresh_fv],
)
