from pathlib import Path

import duckdb

from taxi_pipeline.config import Config
from taxi_pipeline.ingest import Batch
from taxi_pipeline.quality import QualityResult


TRUSTED_SCHEMA = {
    "service_type": "VARCHAR",
    "pickup_datetime": "TIMESTAMP",
    "dropoff_datetime": "TIMESTAMP",
    "trip_duration_min": "DOUBLE",
    "pu_location_id": "INTEGER",
    "do_location_id": "INTEGER",
    "pu_borough": "VARCHAR",
    "do_borough": "VARCHAR",
    "dq_long_trip": "BOOLEAN",
    "dq_zero_duration": "BOOLEAN",
    "dq_unknown_location": "BOOLEAN",
    "batch_id": "VARCHAR",
    "source_file": "VARCHAR",
    "run_id": "VARCHAR",
}


def partition_file(config: Config, batch: Batch) -> Path:
    return (
        config.path("trusted") / "trips" / f"service_type={batch.service}"
        / f"pickup_month={batch.month}" / "part-0.parquet"
    )


def transform_batch(
    connection: duckdb.DuckDBPyConnection, config: Config,
    batch: Batch, quality: QualityResult, run_id: str,
) -> Path:
    if quality.status != "VALIDATED" or quality.accepted_view != "validated_batch":
        raise ValueError("Transform requires a validated batch.")
    staged = config.path("trusted") / "_staging" / run_id / batch.batch_id / "part-0.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.unlink(missing_ok=True)
    destination = partition_file(config, batch)
    try:
        connection.sql("""
            SELECT
                t.service_type, t.pickup_datetime, t.dropoff_datetime,
                t.trip_duration_min, t.pu_location_id, t.do_location_id,
                pu.borough AS pu_borough, do_zone.borough AS do_borough,
                t.dq_long_trip, t.dq_zero_duration, t.dq_unknown_location,
                t.batch_id, t.source_file, t.run_id
            FROM validated_batch AS t
            LEFT JOIN zone_lookup AS pu ON t.pu_location_id = pu.location_id
            LEFT JOIN zone_lookup AS do_zone ON t.do_location_id = do_zone.location_id
        """).write_parquet(str(staged), compression="zstd")
        destination.parent.mkdir(parents=True, exist_ok=True)
        staged.replace(destination)
    finally:
        staged.unlink(missing_ok=True)
    return destination
