from datetime import date
import json
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from taxi_pipeline.config import Config
from taxi_pipeline.ingest import Batch, build_batches
from taxi_pipeline.state import load_manifest, marker_path, sha256_file, utc_now
from taxi_pipeline.transform import TRUSTED_SCHEMA, partition_file


def verify_batch(
    connection: duckdb.DuckDBPyConnection, config: Config,
    batch: Batch, quality_report: dict[str, Any],
) -> dict[str, Any]:
    # Read back what was actually written. A finished write call only proves the
    # code ran, not that the partition holds the rows the gate accepted.
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


def verify_run(
    connection: duckdb.DuckDBPyConnection, config: Config,
    batches: list[Batch], run_id: str, paths: dict[str, Path], records: list[dict],
) -> dict[str, Any]:
    manifest = load_manifest(config.path("state") / "manifest.json")
    reference_sha = sha256_file(
        config.path("raw") / "reference" / "taxi_zone_lookup.csv"
    )
    details = []
    trusted_counts = {}
    raw_by_service = {}
    for batch in batches:
        marker = json.loads(marker_path(config, batch.batch_id).read_text(encoding="utf-8"))
        raw = pq.ParquetFile(batch.raw_path).metadata.num_rows
        trusted = pq.ParquetFile(partition_file(config, batch)).metadata.num_rows
        rejected = 0
        if marker["rejected_rows"]:
            rejected = pq.ParquetFile(
                config.root / marker["quality_report"]["rejected_file"]
            ).metadata.num_rows
        source = manifest["files"].get(batch.batch_id, {})
        details.append({
            "batch_id": batch.batch_id, "service_type": batch.service,
            "pickup_month": batch.month, "raw_rows": raw, "valid_rows": trusted,
            "rejected_rows": rejected, "processed_run_id": marker["run_id"],
            "trusted_file": marker["trusted_file"],
            "source_sha256": marker["source_sha256"],
            "trusted_sha256": marker["trusted_sha256"],
            "raw_reconciled": raw == trusted + rejected,
            "marker_counts_match": (
                raw == marker["raw_rows"] and trusted == marker["valid_rows"]
                and rejected == marker["rejected_rows"]
            ),
            "source_fingerprints_match": (
                marker["source_sha256"] == source.get("sha256")
                and marker["reference_sha256"] == reference_sha
                and reference_sha == manifest["files"].get("taxi_zone_lookup", {}).get("sha256")
            ),
        })
        trusted_counts[(batch.service, batch.month)] = trusted
        raw_by_service[batch.service] = raw_by_service.get(batch.service, 0) + raw

    connection.read_parquet(
        str(paths["monthly_trip_counts.parquet"]), hive_partitioning=False,
    ).create_view("verify_monthly", replace=True)
    connection.read_parquet(
        str(paths["trips_by_borough.parquet"]), hive_partitioning=False,
    ).create_view("verify_borough", replace=True)
    monthly_rows = connection.execute("SELECT * FROM verify_monthly").fetchall()
    monthly = {(service, month): count for service, month, count in monthly_rows}
    borough_rows = connection.execute("SELECT * FROM verify_borough").fetchall()
    borough = {}
    for service, month, name, count in borough_rows:
        borough[(service, month)] = borough.get((service, month), 0) + count
    actual_by_service = {}
    for (service, month), count in monthly.items():
        actual_by_service[service] = actual_by_service.get(service, 0) + count

    expected = {(batch.service, batch.month) for batch in batches}
    monthly_schema = [
        (row[0], row[1])
        for row in connection.execute("DESCRIBE verify_monthly").fetchall()
    ]
    borough_schema = [
        (row[0], row[1])
        for row in connection.execute("DESCRIBE verify_borough").fetchall()
    ]
    checks = {
        "curated_schema_match": (
            monthly_schema == [
                ("service_type", "VARCHAR"), ("pickup_month", "VARCHAR"),
                ("number_of_trips", "BIGINT"),
            ]
            and borough_schema == [
                ("service_type", "VARCHAR"), ("pickup_month", "VARCHAR"),
                ("pu_borough", "VARCHAR"), ("number_of_trips", "BIGINT"),
            ]
        ),
        "all_batches_accepted": (
            len(records) == len(batches)
            and {item["batch_id"] for item in records} == {batch.batch_id for batch in batches}
            and all(item["status"] in {"PUBLISHED", "ALREADY_PROCESSED"} for item in records)
        ),
        "raw_reconciled_every_batch": all(item["raw_reconciled"] for item in details),
        "marker_counts_match": all(item["marker_counts_match"] for item in details),
        "source_fingerprints_current": all(item["source_fingerprints_match"] for item in details),
        "monthly_keys_complete_and_unique": (
            set(monthly) == expected and len(monthly_rows) == len(expected)
        ),
        "curated_matches_trusted_per_batch": monthly == trusted_counts,
        "borough_totals_match_monthly": borough == monthly,
        "borough_keys_unique": len({row[:3] for row in borough_rows}) == len(borough_rows),
        "counts_nonnegative": (
            all(row[2] >= 0 for row in monthly_rows)
            and all(row[2] is not None and row[3] >= 0 for row in borough_rows)
        ),
    }
    baseline_config = config.values["baseline"]
    baseline_keys = {
        (batch.service, batch.month)
        for batch in build_batches(config, baseline_config["start"], baseline_config["end"])
    }
    applicable = expected == baseline_keys
    baseline = {"status": "NOT_APPLICABLE", "reason": "Requested scope differs from S1 baseline."}
    if applicable:
        valid_matches = actual_by_service == baseline_config["valid_trips"]
        raw_matches = raw_by_service == baseline_config["raw_rows"]
        checks["baseline_valid_matches"] = valid_matches
        checks["baseline_raw_matches"] = raw_matches
        baseline = {
            "status": "MATCH" if valid_matches and raw_matches else "MISMATCH",
            "expected_valid": baseline_config["valid_trips"],
            "actual_valid": actual_by_service,
            "expected_raw": baseline_config["raw_rows"], "actual_raw": raw_by_service,
            "valid_deltas": {
                service: actual_by_service.get(service, 0) - expected_count
                for service, expected_count in baseline_config["valid_trips"].items()
            },
        }
    passed = all(checks.values())
    return {
        "run_id": run_id, "checked_at": utc_now(),
        "status": "PASSED" if passed else "FAILED", "passed": passed,
        "window": {
            "start": batches[0].month, "end": batches[-1].month,
            "services": list(dict.fromkeys(batch.service for batch in batches)),
        },
        "checks": checks, "baseline": baseline, "batches": details,
        "totals": {
            "raw_rows": sum(item["raw_rows"] for item in details),
            "valid_rows": sum(trusted_counts.values()),
            "rejected_rows": sum(item["rejected_rows"] for item in details),
            "curated_rows": sum(monthly.values()),
            "borough_rows": sum(borough.values()),
        },
        "valid_trips_by_service": actual_by_service,
    }
