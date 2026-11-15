import csv
import json
import shutil
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
import pandas as pd

from taxi_pipeline.config import Config
from taxi_pipeline.state import run_lock, sha256_file, write_json_atomic


SERVICE_NAMES = {
    "yellow": "Yellow Taxi", "green": "Green Taxi",
    "fhv": "For-Hire Vehicle (FHV)", "fhvhv": "High Volume FHV (FHVHV)",
}
COLORS = {"yellow": "#FBC02D", "green": "#43A047", "fhv": "#4682B4", "fhvhv": "#7E57C2"}
ANALYSIS_FILES = ("monthly_trip_counts.csv", "trips_by_borough.csv", "service_summary.csv")


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def generate_reports(connection, config, paths, verification) -> dict[Path, Path]:
    staging = paths["monthly_trip_counts.parquet"].parent
    monthly = pd.read_parquet(paths["monthly_trip_counts.parquet"])
    borough = pd.read_parquet(paths["trips_by_borough.parquet"])
    summary = monthly.groupby("service_type", as_index=False).agg(
        total_trips=("number_of_trips", "sum"), months=("pickup_month", "nunique"),
    )
    summary["service_name"] = summary["service_type"].map(SERVICE_NAMES)
    summary["share_percent"] = summary["total_trips"] / summary["total_trips"].sum() * 100
    summary = summary.sort_values(
        ["total_trips", "service_type"], ascending=[False, True],
    ).reset_index(drop=True)
    frames = {
        "monthly_trip_counts.csv": monthly, "trips_by_borough.csv": borough,
        "service_summary.csv": summary,
    }

    details = verification["batches"]
    target = min(1000, sum(item["valid_rows"] for item in details))
    quotas = [min(target // len(details), item["valid_rows"]) for item in details]
    remaining = target - sum(quotas)
    for index, item in enumerate(details):
        extra = min(remaining, item["valid_rows"] - quotas[index])
        quotas[index] += extra
        remaining -= extra
    selections = [
        "SELECT * FROM (SELECT * FROM read_parquet("
        + _literal(str(config.root / item["trusted_file"]))
        + f", hive_partitioning=false) LIMIT {quota})"
        for item, quota in zip(details, quotas) if quota
    ]
    sample_query = " UNION ALL ".join(selections)
    actual_sample = connection.execute(f"SELECT count(*) FROM ({sample_query})").fetchone()[0]
    if actual_sample != target:
        raise ValueError("Trusted sample has an unexpected number of rows.")
    destinations = {
        config.path("curated") / name: path for name, path in paths.items()
    }
    for name, frame in frames.items():
        path = staging / name
        frame.to_csv(path, index=False, float_format="%.6f", lineterminator="\n")
        destinations[config.path("outputs") / name] = path
    sample_path = staging / "sample_trusted_trips.csv"
    connection.execute(
        f"COPY ({sample_query}) TO {_literal(str(sample_path))} (FORMAT CSV, HEADER TRUE)"
    )
    destinations[config.path("outputs") / sample_path.name] = sample_path

    window = verification["window"]
    figure, axis = plt.subplots(figsize=(10, 5.5))
    values = [int(value) for value in summary["total_trips"]]
    axis.barh(
        summary["service_name"], values,
        color=[COLORS[value] for value in summary["service_type"]],
    )
    axis.invert_yaxis()
    axis.set_title(f"NYC TLC valid trip records: {window['start']} to {window['end']}")
    axis.set_xlabel("Number of valid trip records")
    axis.xaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
    maximum = max(values)
    axis.set_xlim(0, maximum * 1.22)
    for index, value in enumerate(values):
        axis.text(value + maximum * 0.015, index, f"{value:,}", va="center", fontsize=9)
    figure.tight_layout()
    chart = staging / "trips_by_service.png"
    figure.savefig(chart, dpi=160, bbox_inches="tight")
    plt.close(figure)
    destinations[config.path("outputs") / chart.name] = chart

    winners = summary[summary["total_trips"] == summary["total_trips"].max()]
    names = ", ".join(winners["service_name"])
    full_scope = verification["baseline"]["status"] != "NOT_APPLICABLE"
    title = (
        "Which taxi type was the most used in the first half of 2024?"
        if full_scope else f"ผลวิเคราะห์ช่วง {window['start']} ถึง {window['end']}"
    )
    lines = [
        f"# {title}", "",
        "ผมวัด most used ด้วยจำนวน valid trip records โดยหนึ่งแถวแทนหนึ่ง trip record",
        "เกณฑ์ valid คือมี pickup และ dropoff, dropoff ไม่ก่อน pickup และ pickup อยู่ในเดือนของไฟล์",
        "", f"ช่วงข้อมูล: {window['start']} ถึง {window['end']}",
        f"ประเภทบริการ: {', '.join(window['services'])}", "",
        f"บริการที่มีจำนวนเที่ยวสูงสุดคือ **{names}** จำนวน **{int(winners.iloc[0]['total_trips']):,} เที่ยว** ต่อประเภท",
        "", "| ประเภทบริการ | Valid trips | สัดส่วน |", "|---|---:|---:|",
    ]
    for row in summary.itertuples(index=False):
        lines.append(f"| {row.service_name} | {int(row.total_trips):,} | {row.share_percent:.2f}% |")
    totals = verification["totals"]
    lines.extend([
        "", f"รวม valid trips: {totals['valid_rows']:,} เที่ยว",
        f"Raw records: {totals['raw_rows']:,} แถว; rejected: {totals['rejected_rows']:,} แถว",
        f"ผลเทียบ baseline S1: {verification['baseline']['status']}", "",
        "ยอดตาม borough ใช้ pickup borough และรวมค่าที่หา borough ไม่ได้ไว้ใน Unknown",
        "ผมใช้ valid trips ทั้งหมดในการคำนวณ ส่วน sample เป็นตัวอย่างจากแต่ละ batch สำหรับตรวจ schema",
        "ที่มาและผลตรวจอยู่ใน monthly_trip_counts.csv, quality_report.csv และ verify_run.json",
    ])
    answer = staging / "answer.md"
    answer.write_text("\n".join(lines) + "\n", encoding="utf-8")
    destinations[config.path("outputs") / answer.name] = answer
    return destinations


def publish_artifacts(config: Config, destinations: dict[Path, Path]) -> dict:
    receipts = {}
    for destination, staged in destinations.items():
        digest = sha256_file(staged)
        size = staged.stat().st_size
        destination.parent.mkdir(parents=True, exist_ok=True)
        staged.replace(destination)
        if sha256_file(destination) != digest:
            raise ValueError(f"Published artifact checksum differs: {destination}")
        receipts[str(destination.relative_to(config.root))] = {"sha256": digest, "size": size}
    return receipts


def _capture_evidence(config: Config, label: str) -> Path:
    if label not in {"A", "B", "C", "D"}:
        raise ValueError("Evidence label must be A, B, C, or D.")
    proof_path = config.path("outputs") / "verify_run.json"
    proof = json.loads(proof_path.read_text(encoding="utf-8"))
    if proof.get("passed") is not True or proof.get("status") != "PASSED":
        raise ValueError("Evidence requires a fully verified run.")
    with (config.path("outputs") / "run_history.csv").open(encoding="utf-8", newline="") as handle:
        matches = [row for row in csv.DictReader(handle) if row["run_id"] == proof["run_id"]]
    if len(matches) != 1 or matches[0]["status"] != "SUCCESS" or matches[0]["mode"] != "full":
        raise ValueError("Verification has no matching successful full run.")
    run = matches[0]
    for name, receipt in proof["artifacts"].items():
        if sha256_file(config.root / name) != receipt["sha256"]:
            raise ValueError(f"Artifact changed after verification: {name}")
    directory = config.path("evidence") / label
    summary_path = directory / "run_summary.json"
    if summary_path.exists():
        previous = json.loads(summary_path.read_text(encoding="utf-8"))
        if previous["run_id"] != run["run_id"]:
            raise ValueError(f"Evidence label already belongs to another run: {label}")
    directory.mkdir(parents=True, exist_ok=True)
    names = ["verify_run.json", "status.txt", "answer.md", *ANALYSIS_FILES]
    for name in names:
        shutil.copyfile(config.path("outputs") / name, directory / name)
    shutil.copyfile(config.root / run["log_file"], directory / "run.log")
    summary = {
        **run,
        "analysis_hashes": {
            name: proof["artifacts"][str((config.path("outputs") / name).relative_to(config.root))]["sha256"]
            for name in ANALYSIS_FILES
        },
    }
    write_json_atomic(summary_path, summary)
    return directory


def capture_evidence(config: Config, label: str) -> Path:
    with run_lock(config.path("state")):
        return _capture_evidence(config, label)
