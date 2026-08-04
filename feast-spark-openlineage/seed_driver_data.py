"""Load feast_driver_hourly_stats into Postgres (run once before materialize)."""
from datetime import datetime, timedelta
import os
import psycopg
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.infra.utils.postgres.connection_utils import df_to_postgres_table
from feast.infra.utils.postgres.postgres_config import PostgreSQLConfig

host = os.environ.get("PG_HOST", "postgres.feast.svc.cluster.local")
port = int(os.environ.get("PG_PORT", "5432"))
user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
database = os.environ.get("POSTGRES_DB", "feast")
schema = os.environ.get("POSTGRES_SCHEMA", "public")

end = datetime.now().replace(microsecond=0, second=0, minute=0)
start = end - timedelta(days=15)
df = create_driver_hourly_stats_df([1001, 1002, 1003, 1004, 1005], start, end)
# Drop dict/json columns that psycopg cannot adapt for COPY-style inserts.
df = df[
    [
        "driver_id",
        "event_timestamp",
        "created",
        "conv_rate",
        "acc_rate",
        "avg_daily_trips",
    ]
].copy()

conninfo = (
    f"host={host} port={port} dbname={database} user={user} "
    f"password={password} sslmode=disable"
)
with psycopg.connect(conninfo=conninfo, options=f"-c search_path={schema}") as conn, conn.cursor() as cur:
    cur.execute('DROP TABLE IF EXISTS "feast_driver_hourly_stats"')
    conn.commit()

cfg = PostgreSQLConfig(
    host=host,
    port=port,
    database=database,
    db_schema=schema,
    user=user,
    password=password,
    sslmode="disable",
)
df_to_postgres_table(config=cfg, df=df, table_name="feast_driver_hourly_stats")
print(f"Loaded {len(df)} rows into {database}.feast_driver_hourly_stats")
