from datetime import date
from typing import Any

import duckdb

from taxi_pipeline.config import Config
from taxi_pipeline.ingest import Batch
from taxi_pipeline.transform import TRUSTED_SCHEMA, partition_file


def verify_batch(
    connection: duckdb.DuckDBPyConnection, config: Config,
    batch: Batch, quality_report: dict[str, Any],
) -> dict[str, Any]:
    path = partition_file(config, batch)
    files = sorted(path.parent.glob("*.parquet"))
    if files != [path]:
        raise ValueError(f"Partition must contain exactly part-0.parquet: {batch.batch_id}")
    connection.read_parquet(str(path), hive_partitioning=False).create_view(
        "verify_partition", replace=True,
    )
    schema = [
        (row[0], row[1])
        for row in connection.execute("DESCRIBE verify_partition").fetchall()
    ]
    if schema != list(TRUSTED_SCHEMA.items()):
        raise ValueError(f"Trusted schema differs from contract: {schema}")
    required = [name for name in TRUSTED_SCHEMA if name not in {
        "pu_location_id", "do_location_id", "pu_borough", "do_borough",
    }]
    missing = " OR ".join(f"t.{name} IS NULL" for name in required)
    start = date.fromisoformat(f"{batch.month}-01")
    end = (
        date(start.year + 1, 1, 1) if start.month == 12
        else date(start.year, start.month + 1, 1)
    )
    counts = connection.execute(f"""
        SELECT count(*),
            count(*) FILTER (WHERE {missing}),
            count(*) FILTER (WHERE
                t.pickup_datetime < CAST(? AS TIMESTAMP)
                OR t.pickup_datetime >= CAST(? AS TIMESTAMP)),
            count(*) FILTER (WHERE
                t.dropoff_datetime < t.pickup_datetime
                OR t.trip_duration_min IS DISTINCT FROM
                   epoch(t.dropoff_datetime - t.pickup_datetime) / 60.0),
            count(*) FILTER (WHERE
                t.service_type IS DISTINCT FROM ? OR t.batch_id IS DISTINCT FROM ?
                OR t.source_file IS DISTINCT FROM ? OR t.run_id IS DISTINCT FROM ?),
            count(*) FILTER (WHERE
                t.pu_borough IS DISTINCT FROM pu.borough
                OR t.do_borough IS DISTINCT FROM do_zone.borough),
            count(*) FILTER (WHERE
                t.dq_long_trip IS DISTINCT FROM (t.trip_duration_min > ?)
                OR t.dq_zero_duration IS DISTINCT FROM
                   (t.dropoff_datetime = t.pickup_datetime)
                OR t.dq_unknown_location IS DISTINCT FROM (
                    pu.location_id IS NULL OR do_zone.location_id IS NULL))
        FROM verify_partition AS t
        LEFT JOIN zone_lookup AS pu ON t.pu_location_id = pu.location_id
        LEFT JOIN zone_lookup AS do_zone ON t.do_location_id = do_zone.location_id
    """, [
        start.isoformat(), end.isoformat(), batch.service, batch.batch_id,
        batch.raw_path.name, quality_report["run_id"],
        config.values["quality"]["long_trip_minutes"],
    ]).fetchone()
    checks = {
        "schema_match": True,
        "row_count_match": counts[0] == quality_report["valid_rows"],
        "required_non_null": counts[1] == 0,
        "pickup_month_match": counts[2] == 0,
        "valid_time_and_duration": counts[3] == 0,
        "lineage_match": counts[4] == 0,
        "borough_join_match": counts[5] == 0,
        "warning_flags_match": counts[6] == 0,
        "raw_reconciled": quality_report["raw_rows"] == (
            quality_report["valid_rows"] + quality_report["rejected_rows"]
        ),
    }
    if not all(checks.values()):
        raise ValueError(f"VerifyBatch failed for {batch.batch_id}: {checks}")
    return {"rows": counts[0], **checks}
