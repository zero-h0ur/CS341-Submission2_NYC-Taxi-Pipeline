import csv
import logging
import math
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from taxi_pipeline.config import Config, open_connection
from taxi_pipeline.ingest import Batch
from taxi_pipeline.state import sha256_file, utc_now, write_json_atomic


logger = logging.getLogger(__name__)

REPORT_FIELDS = [
    "run_id", "batch_id", "service_type", "pickup_month", "source_file",
    "source_sha256", "checked_at", "status", "block_reasons",
    "raw_rows", "valid_rows", "rejected_rows", "valid_ratio",
    "outside_month_rows", "outside_month_ratio",
    "reject_missing_pickup", "reject_missing_dropoff",
    "reject_dropoff_before_pickup", "reject_pickup_outside_batch_month",
    "warn_rows", "warn_long_trip", "warn_zero_duration",
    "warn_unknown_location", "rejected_file",
]


@dataclass(frozen=True)
class QualityResult:
    status: str
    report: dict[str, Any]
    accepted_view: str | None = None


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _location_cast(column: str) -> str:
    column = _identifier(column)
    return (
        f"CASE WHEN TRY_CAST({column} AS DOUBLE) = "
        f"TRY_CAST({column} AS INTEGER) "
        f"THEN TRY_CAST({column} AS INTEGER) ELSE NULL END"
    )


def load_zone_lookup(connection: duckdb.DuckDBPyConnection, config: Config) -> None:
    path = config.path("raw") / "reference" / "taxi_zone_lookup.csv"
    connection.read_csv(str(path), header=True, all_varchar=True).create_view(
        "raw_zone_lookup", replace=True,
    )
    columns = {row[0] for row in connection.execute("DESCRIBE raw_zone_lookup").fetchall()}
    if not {"LocationID", "Borough"}.issubset(columns):
        raise ValueError("Zone lookup requires LocationID and Borough columns.")
    connection.execute("""
        CREATE OR REPLACE TEMP TABLE zone_lookup AS
        SELECT
            CASE WHEN regexp_full_match(trim(LocationID), '[0-9]+')
                 THEN TRY_CAST(LocationID AS INTEGER) END AS location_id,
            nullif(trim(Borough), '') AS borough
        FROM raw_zone_lookup
    """)
    rows, unique_ids, invalid = connection.execute("""
        SELECT count(*), count(DISTINCT location_id),
               count(*) FILTER (
                   WHERE location_id IS NULL OR location_id <= 0 OR borough IS NULL
               )
        FROM zone_lookup
    """).fetchone()
    if rows == 0 or rows != unique_ids or invalid:
        raise ValueError("Zone lookup has empty data, duplicate IDs, or invalid values.")


def _rejected_path(config: Config, batch: Batch) -> Path:
    return config.path("quarantine") / "rows" / f"batch_id={batch.batch_id}" / "rejected.parquet"


def _block(
    config: Config, batch: Batch, report: dict[str, Any],
    reasons: list[str], clear_rows: bool = False,
) -> QualityResult:
    if clear_rows:
        rejected = _rejected_path(config, batch)
        rejected.unlink(missing_ok=True)
        rejected.with_name(rejected.name + ".part").unlink(missing_ok=True)
    report.update(
        status="QUARANTINED", block_reasons=";".join(reasons), checked_at=utc_now(),
    )
    path = config.path("quarantine") / "batches" / f"{batch.batch_id}.json"
    write_json_atomic(path, report)
    return QualityResult("QUARANTINED", report)


def _classify(
    connection: duckdb.DuckDBPyConnection, batch: Batch,
    mapping: dict[str, str], run_id: str, long_minutes: float,
) -> None:
    start = date.fromisoformat(f"{batch.month}-01")
    end = (
        date(start.year + 1, 1, 1) if start.month == 12
        else date(start.year, start.month + 1, 1)
    )
    pickup = _identifier(mapping["pickup"])
    dropoff = _identifier(mapping["dropoff"])
    start_sql = f"TIMESTAMP {_literal(start.isoformat())}"
    end_sql = f"TIMESTAMP {_literal(end.isoformat())}"
    connection.execute(f"""
        CREATE OR REPLACE TEMP VIEW standardized_batch AS
        SELECT
            {_literal(batch.service)} AS service_type,
            CAST({pickup} AS TIMESTAMP) AS pickup_datetime,
            CAST({dropoff} AS TIMESTAMP) AS dropoff_datetime,
            {_location_cast(mapping['pu_location'])} AS pu_location_id,
            {_location_cast(mapping['do_location'])} AS do_location_id,
            {_literal(batch.batch_id)} AS batch_id,
            {_literal(batch.raw_path.name)} AS source_file,
            {_literal(run_id)} AS run_id,
            -- first matching rule wins, so a row is never counted under two reasons
            CASE
                WHEN {pickup} IS NULL THEN 'missing_pickup'
                WHEN {dropoff} IS NULL THEN 'missing_dropoff'
                WHEN {dropoff} < {pickup} THEN 'dropoff_before_pickup'
                WHEN {pickup} < {start_sql} OR {pickup} >= {end_sql}
                    THEN 'pickup_outside_batch_month'
                ELSE NULL
            END AS reject_reason,
            coalesce({pickup} < {start_sql} OR {pickup} >= {end_sql}, false)
                AS outside_batch_month
        FROM raw_batch
    """)
    connection.execute(f"""
        CREATE OR REPLACE TEMP VIEW classified_batch AS
        SELECT *,
            epoch(dropoff_datetime - pickup_datetime) / 60.0 AS trip_duration_min,
            coalesce(
                epoch(dropoff_datetime - pickup_datetime) / 60.0 > {long_minutes},
                false
            ) AS dq_long_trip,
            coalesce(dropoff_datetime = pickup_datetime, false) AS dq_zero_duration,
            (
                pu_location_id IS NULL OR do_location_id IS NULL
                OR pu_location_id NOT IN (SELECT location_id FROM zone_lookup)
                OR do_location_id NOT IN (SELECT location_id FROM zone_lookup)
            ) AS dq_unknown_location
        FROM standardized_batch
    """)


def _counts(connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    cursor = connection.execute("""
        SELECT count(*) AS raw_rows,
            count(*) FILTER (WHERE reject_reason IS NULL) AS valid_rows,
            count(*) FILTER (WHERE reject_reason IS NOT NULL) AS rejected_rows,
            count(*) FILTER (WHERE outside_batch_month) AS outside_month_rows,
            count(*) FILTER (WHERE reject_reason = 'missing_pickup')
                AS reject_missing_pickup,
            count(*) FILTER (WHERE reject_reason = 'missing_dropoff')
                AS reject_missing_dropoff,
            count(*) FILTER (WHERE reject_reason = 'dropoff_before_pickup')
                AS reject_dropoff_before_pickup,
            count(*) FILTER (WHERE reject_reason = 'pickup_outside_batch_month')
                AS reject_pickup_outside_batch_month,
            count(*) FILTER (WHERE reject_reason IS NULL AND (
                dq_long_trip OR dq_zero_duration OR dq_unknown_location
            )) AS warn_rows,
            count(*) FILTER (WHERE reject_reason IS NULL AND dq_long_trip)
                AS warn_long_trip,
            count(*) FILTER (WHERE reject_reason IS NULL AND dq_zero_duration)
                AS warn_zero_duration,
            count(*) FILTER (WHERE reject_reason IS NULL AND dq_unknown_location)
                AS warn_unknown_location
        FROM classified_batch
    """)
    names = [item[0] for item in cursor.description]
    return dict(zip(names, cursor.fetchone()))


def validate_batch(
    connection: duckdb.DuckDBPyConnection, config: Config,
    batch: Batch, run_id: str, source_sha256: str | None = None,
) -> QualityResult:
    long_minutes = config.values["quality"]["long_trip_minutes"]
    if (
        type(long_minutes) not in (int, float)
        or not math.isfinite(long_minutes) or long_minutes <= 0
    ):
        raise ValueError("long_trip_minutes must be a finite positive number.")

    report = dict.fromkeys(REPORT_FIELDS)
    report.update(
        run_id=run_id, batch_id=batch.batch_id, service_type=batch.service,
        pickup_month=batch.month, source_file=batch.raw_path.name,
        block_reasons="",
    )
    for view in ("validated_batch", "classified_batch", "standardized_batch", "raw_batch"):
        connection.execute(f"DROP VIEW IF EXISTS {view}")

    try:
        report["source_sha256"] = source_sha256 or sha256_file(batch.raw_path)
        connection.read_parquet(
            str(batch.raw_path), hive_partitioning=False,
        ).create_view("raw_batch", replace=True)
        schema = dict(
            (row[0], row[1])
            for row in connection.execute("DESCRIBE raw_batch").fetchall()
        )
        report["raw_rows"] = connection.execute("SELECT count(*) FROM raw_batch").fetchone()[0]
        if report["raw_rows"] == 0:
            report.update(valid_rows=0, rejected_rows=0)
            return _block(config, batch, report, ["empty_batch"], clear_rows=True)

        mapping = config.values["sources"]["services"][batch.service]
        missing = [name for name in mapping.values() if name not in schema]
        if missing:
            return _block(
                config, batch, report, ["missing_columns:" + ",".join(missing)],
                clear_rows=True,
            )
        timestamp_types = {"TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS"}
        wrong_types = [
            mapping[name] for name in ("pickup", "dropoff")
            if schema[mapping[name]] not in timestamp_types
        ]
        if wrong_types:
            return _block(
                config, batch, report,
                ["timestamp_type_mismatch:" + ",".join(wrong_types)], clear_rows=True,
            )
        _classify(connection, batch, mapping, run_id, float(long_minutes))
        report.update(_counts(connection))
    except (duckdb.OutOfMemoryException, duckdb.InterruptException):
        raise
    except (OSError, duckdb.Error) as exc:
        return _block(
            config, batch, report, [f"unreadable_batch:{exc}"], clear_rows=True,
        )

    if report["raw_rows"] != report["valid_rows"] + report["rejected_rows"]:
        raise RuntimeError(f"Row reconciliation failed: {batch.batch_id}")
    report["valid_ratio"] = report["valid_rows"] / report["raw_rows"]
    report["outside_month_ratio"] = report["outside_month_rows"] / report["raw_rows"]

    rejected = _rejected_path(config, batch)
    part = rejected.with_name(rejected.name + ".part")
    part.unlink(missing_ok=True)
    if report["rejected_rows"]:
        rejected.parent.mkdir(parents=True, exist_ok=True)
        try:
            connection.sql("""
                SELECT * EXCLUDE (outside_batch_month)
                FROM classified_batch WHERE reject_reason IS NOT NULL
            """).write_parquet(str(part), compression="zstd")
            part.replace(rejected)
        finally:
            part.unlink(missing_ok=True)
        report["rejected_file"] = rejected.relative_to(config.root).as_posix()
    else:
        rejected.unlink(missing_ok=True)

    # A few bad rows are normal and go to quarantine. Too many means the file
    # itself is wrong (wrong month, broken export), so the whole batch stops.
    reasons = []
    if report["outside_month_ratio"] > config.values["quality"]["max_outside_month_ratio"]:
        reasons.append("outside_month_ratio_exceeded")
    if report["valid_ratio"] < config.values["quality"]["min_valid_ratio"]:
        reasons.append("valid_ratio_below_minimum")
    if reasons:
        return _block(config, batch, report, reasons)

    connection.execute("""
        CREATE OR REPLACE TEMP VIEW validated_batch AS
        SELECT * EXCLUDE (reject_reason, outside_batch_month)
        FROM classified_batch WHERE reject_reason IS NULL
    """)
    report.update(status="VALIDATED", checked_at=utc_now())
    (config.path("quarantine") / "batches" / f"{batch.batch_id}.json").unlink(missing_ok=True)
    return QualityResult("VALIDATED", report, "validated_batch")


def write_quality_report(config: Config, reports: list[dict[str, Any]]) -> None:
    path = config.path("outputs") / "quality_report.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    try:
        with part.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDS)
            writer.writeheader()
            writer.writerows(reports)
            handle.flush()
            os.fsync(handle.fileno())
        part.replace(path)
    finally:
        part.unlink(missing_ok=True)


def validate_all(config: Config, batches: list[Batch], run_id: str) -> list[QualityResult]:
    results = []
    write_quality_report(config, [])
    with open_connection(config) as connection:
        load_zone_lookup(connection, config)
        for batch in batches:
            result = validate_batch(connection, config, batch, run_id)
            results.append(result)
            write_quality_report(config, [item.report for item in results])
            logger.info(
                "ValidateBatch batch_id=%s status=%s raw=%s valid=%s rejected=%s warn=%s",
                batch.batch_id, result.status, result.report["raw_rows"],
                result.report["valid_rows"], result.report["rejected_rows"],
                result.report["warn_rows"],
            )
    return results
