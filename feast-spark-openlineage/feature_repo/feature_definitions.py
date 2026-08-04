# Minimal postgres driver feature definitions for Spark materialize + OpenLineage.

from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.types import Float32, Int64, ValueType

driver = Entity(
    name="driver",
    join_keys=["driver_id"],
    value_type=ValueType.INT64,
)

driver_stats_source = PostgreSQLSource(
    name="driver_hourly_stats_source",
    query="SELECT * FROM feast_driver_hourly_stats",
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

driver_activity_v1 = FeatureService(
    name="driver_activity_v1",
    features=[driver_stats_fv],
)
