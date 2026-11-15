import csv
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pyarrow.parquet as pq

from taxi_pipeline.config import load_config
from taxi_pipeline.report import capture_evidence
from taxi_pipeline.state import marker_path, write_json_atomic


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def run_case(config, label, published, skipped):
    subprocess.run([
        sys.executable, "-m", "taxi_pipeline", "run",
        "--start", "2024-01", "--end", "2024-06",
    ], cwd=config.root, check=True)
    proof = read_json(config.path("outputs") / "verify_run.json")
    assert proof["passed"] is True and proof["baseline"]["status"] == "MATCH"
    with (config.path("outputs") / "run_history.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        run = next(row for row in csv.DictReader(handle) if row["run_id"] == proof["run_id"])
    assert int(run["published"]) == published, run
    assert int(run["already_processed"]) == skipped, run
    capture_evidence(config, label)
    return read_json(config.path("evidence") / label / "run_summary.json")


def main():
    config = load_config()
    evidence = config.path("evidence")
    baseline = read_json(evidence / "B" / "run_summary.json")
    requested = int(baseline["requested_batches"])
    summaries = {}
    path = evidence / "C" / "run_summary.json"
    summaries["C"] = read_json(path) if path.exists() else run_case(config, "C", 0, requested)
    assert int(summaries["C"]["published"]) == 0
    assert int(summaries["C"]["already_processed"]) == requested
    assert summaries["C"]["analysis_hashes"] == baseline["analysis_hashes"]

    counts_path = evidence / "D_marker_loss.json"
    path = evidence / "D" / "run_summary.json"
    if path.exists():
        summaries["D"] = read_json(path)
        counts = read_json(counts_path)
        assert counts["passed"] is True
    else:
        marker_file = marker_path(config, "green_2024-03")
        if counts_path.exists():
            counts = read_json(counts_path)
        else:
            marker = read_json(marker_file)
            before_rows = pq.ParquetFile(config.root / marker["trusted_file"]).metadata.num_rows
            assert before_rows == marker["valid_rows"]
            counts = {
                "batch_id": "green_2024-03", "before_rows": before_rows,
                "before_processed_run_id": marker["run_id"],
            }
            write_json_atomic(counts_path, counts)
        marker_file.unlink(missing_ok=True)
        summaries["D"] = run_case(config, "D", 1, requested - 1)
        marker = read_json(marker_file)
        trusted = config.root / marker["trusted_file"]
        after_rows = pq.ParquetFile(trusted).metadata.num_rows
        assert after_rows == counts["before_rows"] == marker["valid_rows"]
        assert sorted(trusted.parent.glob("*.parquet")) == [trusted]
        assert marker["run_id"] != counts["before_processed_run_id"]
        counts.update(
            after_rows=after_rows, after_processed_run_id=marker["run_id"], passed=True,
        )
        write_json_atomic(counts_path, counts)
    assert int(summaries["D"]["published"]) == 1
    assert int(summaries["D"]["already_processed"]) == requested - 1
    assert summaries["D"]["analysis_hashes"] == baseline["analysis_hashes"]

    lines = [
        "# หลักฐานการรัน A–D", "",
        "ผมเริ่ม A ด้วย --force เพื่อสร้าง trusted ของมกราคม–มีนาคมใหม่ รวม batch ที่ทดสอบไว้ก่อนหน้าแล้ว",
        "B ขยายถึงมิถุนายน ส่วน C รันช่วงเดิมซ้ำ และ D ลบเฉพาะ marker ของ green_2024-03 ก่อนรันใหม่", "",
        "| รอบ | ช่วงเดือน | PUBLISHED | ALREADY_PROCESSED | วินาที | สถานะ | หลักฐาน |",
        "|---|---|---:|---:|---:|---|---|",
    ]
    for label in "ABCD":
        row = read_json(evidence / label / "run_summary.json")
        lines.append(
            f"| {label} | {row['start']} ถึง {row['end']} | {row['published']} | "
            f"{row['already_processed']} | {row['duration_seconds']} | {row['status']} | "
            f"[summary](../evidence/{label}/run_summary.json), [log](../evidence/{label}/run.log), "
            f"[verification](../evidence/{label}/verify_run.json) |"
        )
    lines.extend([
        "", f"D มีจำนวนแถวของ partition ก่อนลบ marker {counts['before_rows']:,} แถว "
        f"และหลังประมวลผลใหม่ {counts['after_rows']:,} แถว",
        "[จำนวนแถวก่อนและหลัง](../evidence/D_marker_loss.json)", "",
        "SHA-256 ของ monthly_trip_counts.csv, trips_by_borough.csv และ service_summary.csv ตรงกันใน B, C และ D",
        "การเปรียบเทียบนี้ใช้ตารางวิเคราะห์ ส่วน run ID และ metadata การประมวลผลเปลี่ยนได้", "",
        "เวลาที่รายงานเป็นผลวัดบนเครื่องผม โดย raw ถูกดาวน์โหลดไว้แล้ว จึงไม่ได้ใช้สรุปว่าเร็วกว่า S1 บน Colab",
    ])
    report = config.root / "docs" / "rerun_evidence.md"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    shutil.copyfile(config.path("state") / "manifest.json", evidence / "source_manifest.json")
    shutil.copyfile(config.path("state") / "batch_history.jsonl", evidence / "batch_history.jsonl")
    for label, row in summaries.items():
        print(f"{label}: published={row['published']} already_processed={row['already_processed']}")
    print(f"Marker recovery rows: {counts['before_rows']} -> {counts['after_rows']}")
    print("Analysis CSV hashes B=C=D: PASS")
    print(f"Evidence report: {report}")


if __name__ == "__main__":
    main()
