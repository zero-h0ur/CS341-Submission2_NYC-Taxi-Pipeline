import json
from pathlib import Path

import duckdb

from taxi_pipeline.config import Config
from taxi_pipeline.ingest import Batch
from taxi_pipeline.state import marker_path
from taxi_pipeline.transform import partition_file


def published_files(config: Config, batches: list[Batch]) -> list[Path]:
    files = []
    for batch in batches:
        marker = json.loads(marker_path(config, batch.batch_id).read_text(encoding="utf-8"))
        path = partition_file(config, batch)
        if (
            marker.get("status") != "PUBLISHED"
            or marker.get("batch_id") != batch.batch_id
            or marker.get("trusted_file") != str(path.relative_to(config.root))
            or sorted(path.parent.glob("*.parquet")) != [path]
        ):
            raise ValueError(f"Batch has no complete published partition: {batch.batch_id}")
        files.append(path)
    if not files:
        raise ValueError("Curated output requires at least one published batch.")
    return files


def build_curated(
    connection: duckdb.DuckDBPyConnection, config: Config,
    batches: list[Batch], run_id: str,
) -> dict[str, Path]:
    files = published_files(config, batches)
    connection.read_parquet(
        [str(path) for path in files], hive_partitioning=False,
    ).create_view("selected_trips", replace=True)
    staging = config.path("curated") / "_staging" / run_id
    staging.mkdir(parents=True, exist_ok=True)
    queries = {
        "monthly_trip_counts.parquet": """
            SELECT service_type, strftime(pickup_datetime, '%Y-%m') AS pickup_month,
                   count(*) AS number_of_trips
            FROM selected_trips
            GROUP BY service_type, pickup_month
            ORDER BY pickup_month, service_type
        """,
        "trips_by_borough.parquet": """
            SELECT service_type, strftime(pickup_datetime, '%Y-%m') AS pickup_month,
                   coalesce(pu_borough, 'Unknown') AS pu_borough,
                   count(*) AS number_of_trips
            FROM selected_trips
            GROUP BY service_type, pickup_month, coalesce(pu_borough, 'Unknown')
            ORDER BY pickup_month, service_type, pu_borough
        """,
    }
    paths = {}
    for name, query in queries.items():
        path = staging / name
        path.unlink(missing_ok=True)
        connection.sql(query).write_parquet(str(path), compression="zstd")
        paths[name] = path
    return paths
