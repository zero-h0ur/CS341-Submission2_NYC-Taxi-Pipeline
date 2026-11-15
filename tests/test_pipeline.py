import csv
import json
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from taxi_pipeline import pipeline
from taxi_pipeline.__main__ import main
from taxi_pipeline.ingest import build_batches
from taxi_pipeline.report import capture_evidence
from taxi_pipeline.state import marker_path
from taxi_pipeline.transform import partition_file


def batches(config):
    return build_batches(config, "2024-01", "2024-01", ["green"])


def read_proof(config):
    return json.loads((config.path("outputs") / "verify_run.json").read_text(encoding="utf-8"))


def read_marker(config):
    return json.loads(marker_path(config, "green_2024-01").read_text(encoding="utf-8"))


def test_publish_rerun_and_marker_loss_keep_trip_count(config, server):
    selected = batches(config)
    first = pipeline.execute_run(config, selected, "full")
    assert first["status"] == "SUCCESS", first["error"]
    assert first["published"] == 1
    original_marker = read_marker(config)
    original_csv = (config.path("outputs") / "monthly_trip_counts.csv").read_bytes()
    trusted = partition_file(config, selected[0])
    original_bytes = trusted.read_bytes()

    second = pipeline.execute_run(config, selected, "full")
    assert second["already_processed"] == 1
    assert second["status"] == "SUCCESS", second["error"]
    assert trusted.read_bytes() == original_bytes
    assert read_marker(config) == original_marker

    marker_path(config, "green_2024-01").unlink()
    third = pipeline.execute_run(config, selected, "full")
    assert third["status"] == "SUCCESS", third["error"]
    assert third["published"] == 1
    assert pq.ParquetFile(trusted).metadata.num_rows == 3
    assert sorted(trusted.parent.glob("*.parquet")) == [trusted]
    assert read_marker(config)["run_id"] != original_marker["run_id"]
    assert (config.path("outputs") / "monthly_trip_counts.csv").read_bytes() == original_csv
    history = [json.loads(line) for line in (
        config.path("state") / "batch_history.jsonl"
    ).read_text(encoding="utf-8").splitlines()]
    assert [row["status"] for row in history] == ["PUBLISHED", "ALREADY_PROCESSED", "PUBLISHED"]


def test_retry_after_verify_failure_replaces_existing_partition(config, server, monkeypatch):
    def fail(*args, **kwargs):
        raise ValueError("Fixture verification failure after writing trusted.")
    with monkeypatch.context() as patch:
        patch.setattr(pipeline, "verify_batch", fail)
        failed = pipeline.execute_run(config, batches(config), "full")
    assert failed["status"] == "FAILED"
    assert failed["failed"] == 1
    assert not marker_path(config, "green_2024-01").exists()
    path = partition_file(config, batches(config)[0])
    assert pq.ParquetFile(path).metadata.num_rows == 3
    recovered = pipeline.execute_run(config, batches(config), "full")
    assert recovered["status"] == "SUCCESS", recovered["error"]
    assert pq.ParquetFile(path).metadata.num_rows == 3
    assert read_proof(config)["passed"] is True


def test_source_change_reprocesses_published_batch(config, server, trip_table):
    selected = batches(config)
    assert pipeline.execute_run(config, selected, "full")["status"] == "SUCCESS"
    previous = read_marker(config)
    server.set_trip(selected[0], trip_table(rows=4))
    result = pipeline.execute_run(config, selected, "full")
    assert result["status"] == "SUCCESS", result["error"]
    assert result["published"] == 1 and result["already_processed"] == 0
    assert read_marker(config)["source_sha256"] != previous["source_sha256"]
    assert read_proof(config)["totals"]["valid_rows"] == 4


def test_reference_change_refreshes_boroughs(config, server):
    assert pipeline.execute_run(config, batches(config), "full")["status"] == "SUCCESS"
    previous = read_marker(config)
    url = config.values["sources"]["zone_lookup_url"]
    server.payloads[url] = (
        b"LocationID,Borough,Zone,service_zone\n"
        b"1,Queens,Fixture One,Boro Zone\n2,Queens,Fixture Two,Boro Zone\n"
    )
    result = pipeline.execute_run(config, batches(config), "full")
    assert result["status"] == "SUCCESS", result["error"]
    assert result["published"] == 1
    current = read_marker(config)
    assert current["reference_sha256"] != previous["reference_sha256"]
    assert current["source_sha256"] == previous["source_sha256"]
    table = pq.ParquetFile(config.root / current["trusted_file"]).read()
    assert set(table["pu_borough"].to_pylist()) == {"Queens"}
    assert table.num_rows == 3


def test_policy_change_recomputes_warnings(config, server):
    assert pipeline.execute_run(config, batches(config), "full")["status"] == "SUCCESS"
    config.values["quality"]["long_trip_minutes"] = 5
    result = pipeline.execute_run(config, batches(config), "full")
    assert result["status"] == "SUCCESS", result["error"]
    assert result["published"] == 1
    marker = read_marker(config)
    assert marker["valid_rows"] == 3
    assert marker["quality_report"]["warn_long_trip"] == 3


def test_blocked_batch_has_no_marker_or_analysis(config, server, trip_table):
    table = trip_table(rows=1000)
    data = table.to_pydict()
    data["lpep_pickup_datetime"][:20] = [None] * 20
    server.set_trip(batches(config)[0], pa.Table.from_pydict(data, schema=table.schema))
    result = pipeline.execute_run(config, batches(config), "full")
    assert result["status"] == "FAILED"
    assert result["quarantined"] == 1
    assert not marker_path(config, "green_2024-01").exists()
    assert not partition_file(config, batches(config)[0]).exists()
    assert not (config.path("outputs") / "answer.md").exists()
    assert read_proof(config)["passed"] is False


def seed_all_services(config, server, trip_table):
    selected = build_batches(config, "2024-01", "2024-06")
    for batch in selected:
        table = trip_table(service=batch.service, month=batch.month)
        if batch.service == "green" and batch.month == "2024-01":
            data = table.to_pydict()
            data["PULocationID"][2] = 999999
            table = pa.Table.from_pydict(data, schema=table.schema)
        server.set_trip(batch, table)
    config.values["baseline"]["valid_trips"] = {batch.service: 18 for batch in selected}
    config.values["baseline"]["raw_rows"] = {batch.service: 18 for batch in selected}
    return selected


def test_curated_keeps_unknown_borough_and_matches_fixture_baseline(config, server, trip_table):
    selected = seed_all_services(config, server, trip_table)
    result = pipeline.execute_run(config, selected, "full")
    assert result["status"] == "SUCCESS", result["error"]
    proof = read_proof(config)
    assert proof["baseline"]["status"] == "MATCH"
    assert proof["totals"]["valid_rows"] == proof["totals"]["borough_rows"] == 72
    with (config.path("outputs") / "trips_by_borough.csv").open(encoding="utf-8", newline="") as handle:
        boroughs = list(csv.DictReader(handle))
    assert sum(int(row["number_of_trips"]) for row in boroughs if row["pu_borough"] == "Unknown") == 1
    with (config.path("outputs") / "sample_trusted_trips.csv").open(encoding="utf-8", newline="") as handle:
        sample = list(csv.DictReader(handle))
    assert len(sample) == 72
    assert {row["service_type"] for row in sample} == {batch.service for batch in selected}


def test_cli_fails_when_baseline_counts_differ(config, server, trip_table, monkeypatch):
    seed_all_services(config, server, trip_table)
    config.values["baseline"]["valid_trips"]["yellow"] = 19
    path = config.root / "config" / "pipeline.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(yaml.safe_dump(config.values), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [
        "taxi_pipeline", "run", "--config", str(path),
        "--start", "2024-01", "--end", "2024-06",
    ])
    assert main() == 1
    proof = read_proof(config)
    assert proof["passed"] is False
    assert proof["baseline"]["status"] == "MISMATCH"
    assert proof["baseline"]["valid_deltas"]["yellow"] == -1
    assert not (config.path("outputs") / "answer.md").exists()


def test_evidence_capture_rejects_changed_artifact(config, server):
    assert pipeline.execute_run(config, batches(config), "full")["status"] == "SUCCESS"
    directory = capture_evidence(config, "A")
    assert (directory / "run.log").is_file()
    assert (directory / "verify_run.json").is_file()
    path = config.path("outputs") / "monthly_trip_counts.csv"
    path.write_text(path.read_text() + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Artifact changed"):
        capture_evidence(config, "B")
