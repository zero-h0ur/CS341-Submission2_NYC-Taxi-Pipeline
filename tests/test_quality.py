from dataclasses import replace
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from taxi_pipeline.config import open_connection
from taxi_pipeline.quality import load_zone_lookup, validate_batch


def check(config, batch):
    with open_connection(config) as connection:
        load_zone_lookup(connection, config)
        return validate_batch(connection, config, batch, "fixture")


def test_reject_counts_and_warnings_preserve_valid_trips(config, trip_table, write_batch):
    table = trip_table(service="fhv", rows=1000)
    data = table.to_pydict()
    pickup, dropoff = data["pickup_datetime"], data["dropOff_datetime"]
    pickup[0] = None
    dropoff[1] = None
    dropoff[2] = datetime(2024, 1, 15, 11, 59)
    pickup[3] = datetime(2024, 2, 1, 12)
    dropoff[3] = datetime(2024, 2, 1, 12, 10)
    dropoff[4] = pickup[4] + timedelta(days=2)
    dropoff[5] = pickup[5]
    data["PUlocationID"][6] = 999999
    data["DOlocationID"][7] = None
    batch = write_batch(pa.Table.from_pydict(data, schema=table.schema), service="fhv")
    result = check(config, batch)
    assert result.status == "VALIDATED"
    expected = {
        "raw_rows": 1000, "valid_rows": 996, "rejected_rows": 4,
        "warn_rows": 4, "warn_long_trip": 1,
        "warn_zero_duration": 1, "warn_unknown_location": 2,
    }
    assert {key: result.report[key] for key in expected} == expected
    rejected = pq.ParquetFile(config.root / result.report["rejected_file"]).read()
    assert set(rejected["reject_reason"].to_pylist()) == {
        "missing_pickup", "missing_dropoff", "dropoff_before_pickup",
        "pickup_outside_batch_month",
    }
    assert rejected.num_rows == 4


@pytest.mark.parametrize("service", ["yellow", "green", "fhv", "fhvhv"])
def test_service_mapping_and_integer_locations(config, write_batch, service):
    batch = write_batch(service=service)
    with open_connection(config) as connection:
        load_zone_lookup(connection, config)
        result = validate_batch(connection, config, batch, "fixture")
        assert result.status == "VALIDATED"
        row = connection.execute("""
            SELECT pickup_datetime, dropoff_datetime, pu_location_id, do_location_id
            FROM validated_batch LIMIT 1
        """).fetchone()
        assert row == (datetime(2024, 1, 15, 12), datetime(2024, 1, 15, 12, 10), 1, 1)
        schema = {row[0]: row[1] for row in connection.execute("DESCRIBE validated_batch").fetchall()}
        assert schema["pu_location_id"] == schema["do_location_id"] == "INTEGER"


@pytest.mark.parametrize("case,reason", [
    ("unreadable", "unreadable_batch"),
    ("empty", "empty_batch"),
    ("missing_column", "missing_columns"),
    ("pickup_string", "timestamp_type_mismatch"),
    ("outside_month", "outside_month_ratio_exceeded"),
    ("low_valid_ratio", "valid_ratio_below_minimum"),
])
def test_batch_blocking(config, trip_table, write_batch, case, reason):
    table = trip_table(rows=1000)
    if case == "empty":
        table = table.slice(0, 0)
    elif case == "missing_column":
        table = table.drop_columns(["lpep_dropoff_datetime"])
    elif case == "pickup_string":
        table = table.set_column(0, "lpep_pickup_datetime", pa.array(["2024-01-15"] * 1000))
    elif case in {"outside_month", "low_valid_ratio"}:
        data = table.to_pydict()
        for index in range(20):
            data["lpep_pickup_datetime"][index] = (
                datetime(2024, 2, 1) if case == "outside_month" else None
            )
            if case == "outside_month":
                data["lpep_dropoff_datetime"][index] = datetime(2024, 2, 1, 0, 10)
        table = pa.Table.from_pydict(data, schema=table.schema)
    batch = write_batch(table)
    if case == "unreadable":
        batch.raw_path.write_bytes(b"broken parquet")
    result = check(config, batch)
    assert result.status == "QUARANTINED"
    assert result.accepted_view is None
    assert reason in result.report["block_reasons"]
    assert (config.path("quarantine") / "batches" / f"{batch.batch_id}.json").is_file()


def test_exact_gate_boundaries_are_allowed(config, trip_table, write_batch):
    table = trip_table(rows=100)
    data = table.to_pydict()
    data["lpep_pickup_datetime"][0] = datetime(2024, 2, 1)
    data["lpep_dropoff_datetime"][0] = datetime(2024, 2, 1, 0, 10)
    result = check(config, write_batch(pa.Table.from_pydict(data, schema=table.schema)))
    assert result.status == "VALIDATED"
    assert result.report["valid_ratio"] == 0.99
    assert result.report["outside_month_ratio"] == 0.01


def test_primary_reject_reason_does_not_double_count(config, trip_table, write_batch):
    table = trip_table(rows=1000)
    data = table.to_pydict()
    data["lpep_pickup_datetime"][0] = None
    data["lpep_dropoff_datetime"][0] = None
    data["lpep_pickup_datetime"][1] = datetime(2024, 2, 1)
    data["lpep_dropoff_datetime"][1] = datetime(2024, 1, 31)
    result = check(config, write_batch(pa.Table.from_pydict(data, schema=table.schema)))
    assert result.status == "VALIDATED"
    assert result.report["rejected_rows"] == 2
    assert result.report["reject_missing_pickup"] == 1
    assert result.report["reject_missing_dropoff"] == 0
    assert result.report["reject_dropoff_before_pickup"] == 1
    assert result.report["reject_pickup_outside_batch_month"] == 0
    assert result.report["outside_month_rows"] == 1


def test_duplicate_zone_ids_block_lookup(config):
    path = config.path("raw") / "reference" / "taxi_zone_lookup.csv"
    path.write_text("LocationID,Borough\n1,Manhattan\n1,Queens\n", encoding="utf-8")
    with open_connection(config) as connection:
        with pytest.raises(ValueError, match="duplicate"):
            load_zone_lookup(connection, config)


def test_quarantine_with_symlinked_project_root(config, tmp_path, trip_table, write_batch):
    alias = tmp_path / "alias"
    try:
        alias.symlink_to(config.root, target_is_directory=True)
    except OSError:
        pytest.skip("Creating symlinks needs extra privileges on this machine.")
    linked = replace(config, root=alias)
    table = trip_table(rows=100)
    data = table.to_pydict()
    data["lpep_pickup_datetime"][0] = None
    result = check(linked, write_batch(pa.Table.from_pydict(data, schema=table.schema)))
    assert result.status == "VALIDATED"
    path = alias / result.report["rejected_file"]
    assert pq.ParquetFile(path).metadata.num_rows == 1
    assert path.samefile(config.root / result.report["rejected_file"])
