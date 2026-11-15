"""Run E: show the validation gate stopping bad files built from real TLC data.

Two broken batches are made from the real green_2024-03 raw file:
- green_2024-04 is the March file uploaded under the April name (wrong month)
- green_2024-05 is the same file with the dropoff column missing

Everything runs in data/_demo/run_E with its own state, logs and outputs, in
validate-only mode, so nothing is downloaded and the real pipeline state is not touched.
"""
import csv
import json
from pathlib import Path
import shutil

import pyarrow.parquet as pq

from taxi_pipeline.config import load_config
from taxi_pipeline.ingest import build_batches
from taxi_pipeline.pipeline import execute_run
from taxi_pipeline.state import sha256_file, write_json_atomic


SOURCE = Path("data/raw/green/green_tripdata_2024-03.parquet")
CASES = {
    "green_2024-04": "March file uploaded as April",
    "green_2024-05": "lpep_dropoff_datetime column missing",
}


def line_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def prepare_workspace(project, workspace: Path) -> None:
    if workspace.resolve() == project.root:
        raise ValueError("The demo workspace must not be the project root.")
    shutil.rmtree(workspace, ignore_errors=True)
    (workspace / "config").mkdir(parents=True)
    shutil.copyfile(project.root / "config" / "pipeline.yaml", workspace / "config" / "pipeline.yaml")
    demo = load_config(workspace / "config" / "pipeline.yaml")
    raw = demo.path("raw")
    (raw / "reference").mkdir(parents=True)
    shutil.copyfile(
        project.path("raw") / "reference" / "taxi_zone_lookup.csv",
        raw / "reference" / "taxi_zone_lookup.csv",
    )
    (raw / "green").mkdir(parents=True)
    source = project.root / SOURCE
    shutil.copyfile(source, raw / "green" / "green_tripdata_2024-04.parquet")
    table = pq.read_table(source)
    pq.write_table(
        table.drop_columns(["lpep_dropoff_datetime"]),
        raw / "green" / "green_tripdata_2024-05.parquet",
    )


def main():
    project = load_config()
    source = project.root / SOURCE
    if not source.exists():
        raise SystemExit(f"Run the main pipeline first; {SOURCE} is missing.")
    history = project.path("state") / "batch_history.jsonl"
    history_before = line_count(history)

    workspace = project.path("raw").parent / "_demo" / "run_E"
    prepare_workspace(project, workspace)
    demo = load_config(workspace / "config" / "pipeline.yaml")
    batches = build_batches(demo, "2024-04", "2024-05", ["green"])
    result = execute_run(demo, batches, "validate")

    rows = {item["batch_id"]: item for item in result["records"]}
    assert result["quarantined"] == len(CASES), result
    assert all(rows[batch_id]["status"] == "QUARANTINED" for batch_id in CASES)
    assert line_count(history) == history_before

    evidence = project.path("evidence") / "E"
    shutil.rmtree(evidence, ignore_errors=True)
    evidence.mkdir(parents=True)
    shutil.copyfile(demo.path("outputs") / "quality_report.csv", evidence / "quality_report.csv")
    shutil.copyfile(demo.root / result["log_file"], evidence / "run.log")
    for batch_id in CASES:
        shutil.copyfile(
            demo.path("quarantine") / "batches" / f"{batch_id}.json",
            evidence / f"quarantine_{batch_id}.json",
        )
    with (demo.path("outputs") / "quality_report.csv").open(encoding="utf-8", newline="") as handle:
        reports = {row["batch_id"]: row for row in csv.DictReader(handle)}

    summary = {
        "run_id": result["run_id"], "mode": "validate", "run_status": result["status"],
        "source_file": SOURCE.as_posix(), "source_sha256": sha256_file(source),
        "workspace": workspace.relative_to(project.root).as_posix(),
        "project_batch_history_lines": {"before": history_before, "after": line_count(history)},
        "batches": [
            {
                "batch_id": batch_id, "case": case, "status": rows[batch_id]["status"],
                "block_reasons": reports[batch_id]["block_reasons"],
                "raw_rows": int(reports[batch_id]["raw_rows"]),
                "outside_month_ratio": (
                    round(float(reports[batch_id]["outside_month_ratio"]), 6)
                    if reports[batch_id]["outside_month_ratio"] else None
                ),
            }
            for batch_id, case in CASES.items()
        ],
    }
    write_json_atomic(evidence / "run_summary.json", summary)
    for item in summary["batches"]:
        print(f"{item['batch_id']}: {item['status']} ({item['block_reasons']})")
    print(f"Real batch history unchanged: {history_before} lines")
    print(f"Evidence: {evidence}")
    print(json.dumps({"run_id": result["run_id"]}))


if __name__ == "__main__":
    main()
