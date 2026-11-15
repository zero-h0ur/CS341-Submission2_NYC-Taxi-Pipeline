import csv
import json
import shutil
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.ticker import FuncFormatter
import pandas as pd

from taxi_pipeline.config import Config
from taxi_pipeline.state import run_lock, sha256_file, write_json_atomic


SERVICE_NAMES = {
    "yellow": "Yellow Taxi", "green": "Green Taxi",
    "fhv": "For-Hire Vehicle (FHV)", "fhvhv": "High Volume FHV (FHVHV)",
}
ANALYSIS_FILES = ("monthly_trip_counts.csv", "trips_by_borough.csv", "service_summary.csv")

# The leader is drawn in the FHVHV purple used since S1; everything else stays gray
# so the chart reads as one answer instead of four competing colors.
ACCENT = "#7E57C2"
MUTED = "#C9C9C4"
INK = "#1F2328"
INK_SOFT = "#59636E"
INK_FAINT = "#8C959F"
GRID = "#E6E6E3"
VALID_NOTE = (
    "Source: NYC TLC trip records. Valid = pickup and dropoff present, "
    "dropoff not before pickup, pickup inside the file's month."
)


def _font() -> str:
    installed = {item.name for item in font_manager.fontManager.ttflist}
    for name in ("Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"):
        if name in installed:
            return name
    return "DejaVu Sans"


def _compact(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.0f}K"
    return f"{value:,.0f}"


def _chart_style() -> dict:
    return {
        "font.family": _font(), "font.size": 10, "text.color": INK,
        "axes.edgecolor": "#BDBDB8", "axes.labelcolor": INK_SOFT,
        "xtick.color": INK_SOFT, "ytick.color": INK, "figure.facecolor": "white",
    }


def _axis_label(value: float, _position=None) -> str:
    if value == 0:
        return "0"
    if value >= 1_000_000 and value % 1_000_000 == 0:
        return f"{value / 1_000_000:.0f}M"
    return _compact(value)


def _layout(figure, plot_top: float, plot_bottom: float) -> None:
    # Margins are set in inches so both charts line up when shown one above the other.
    height = figure.get_figheight()
    figure.subplots_adjust(
        left=0.25, right=0.975, top=1 - plot_top / height, bottom=plot_bottom / height,
    )


def _header(figure, title: str, subtitle: str) -> None:
    height = figure.get_figheight()
    figure.text(0.012, 1 - 0.14 / height, title, fontsize=13.5, fontweight="bold",
                color=INK, va="top")
    figure.text(0.012, 1 - 0.46 / height, subtitle, fontsize=9.5, color=INK_SOFT, va="top")
    figure.text(0.012, 0.1 / height, VALID_NOTE, fontsize=8, color=INK_FAINT, va="bottom")


def _service_chart(summary: pd.DataFrame, window: dict, path: Path) -> None:
    names = list(summary["service_name"])
    values = [int(value) for value in summary["total_trips"]]
    shares = list(summary["share_percent"])
    maximum = max(values)
    leaders = [name for name, value in zip(names, values) if value == maximum]
    share = sum(s for s, value in zip(shares, values) if value == maximum)
    with plt.rc_context(_chart_style()):
        figure, axis = plt.subplots(figsize=(9, 3.9))
        positions = list(range(len(values)))
        axis.barh(
            positions, values, height=0.44, zorder=2,
            color=[ACCENT if value == maximum else MUTED for value in values],
        )
        axis.set_yticks(positions, names)
        axis.invert_yaxis()
        axis.tick_params(axis="y", length=0, pad=8, labelsize=10.5)
        axis.tick_params(axis="x", length=0, labelsize=9)
        axis.xaxis.set_major_formatter(FuncFormatter(_axis_label))
        axis.grid(axis="x", color=GRID, linewidth=0.8, zorder=0)
        for side in ("top", "right", "bottom"):
            axis.spines[side].set_visible(False)
        axis.set_xlim(0, maximum * 1.24)
        for index, (value, part) in enumerate(zip(values, shares)):
            axis.text(
                value + maximum * 0.012, index, f"{_compact(value)}  ({part:.1f}%)",
                va="center", fontsize=9.5, color=INK,
                fontweight="bold" if value == maximum else "normal",
            )
        _header(
            figure,
            f"{' and '.join(leaders)} had the most valid trips ({share:.1f}% of the total)",
            f"Valid trip records by service, {window['start']} to {window['end']}. "
            "One record counts as one trip.",
        )
        _layout(figure, plot_top=0.92, plot_bottom=0.72)
        figure.savefig(path, dpi=200)
        plt.close(figure)


def _monthly_share_chart(
    monthly: pd.DataFrame, leader: str, window: dict, path: Path,
) -> dict:
    table = monthly.pivot_table(
        index="pickup_month", columns="service_type", values="number_of_trips",
        aggfunc="sum", fill_value=0,
    ).sort_index()
    share = table[leader] / table.sum(axis=1) * 100
    leads_every_month = bool((table.idxmax(axis=1) == leader).all())
    name = SERVICE_NAMES[leader]
    others = table.shape[1] - 1
    labels = [pd.Timestamp(f"{month}-01").strftime("%b %Y") for month in table.index]
    with plt.rc_context(_chart_style()):
        figure, axis = plt.subplots(figsize=(9, 0.4 * len(labels) + 1.45))
        positions = list(range(len(labels)))
        # A white edge is the gap between the two parts of each bar, not a border.
        axis.barh(positions, share, height=0.6, color=ACCENT, edgecolor="white", linewidth=1.5)
        axis.barh(
            positions, 100 - share, left=share, height=0.6, color=MUTED,
            edgecolor="white", linewidth=1.5,
        )
        for index, value in enumerate(share):
            axis.text(1.2, index, f"{value:.1f}%", va="center", ha="left",
                      fontsize=9.5, color="white", fontweight="bold")
            axis.text(98.8, index, f"{100 - value:.1f}%", va="center", ha="right",
                      fontsize=9, color=INK)
        axis.set_yticks(positions, labels)
        axis.invert_yaxis()
        axis.set_xlim(0, 100)
        axis.tick_params(axis="y", length=0, pad=8, labelsize=10)
        axis.set_xticks([])
        for side in ("top", "right", "bottom", "left"):
            axis.spines[side].set_visible(False)
        title = (
            f"{name} is the largest service in every month"
            if leads_every_month else f"{name} share of valid trips by month"
        )
        rest = (
            "Only one service is in this run." if others == 0
            else "Gray is the other service." if others == 1
            else f"Gray is the other {others} services combined."
        )
        _header(
            figure, title,
            f"Share of each month's valid trips, {window['start']} to {window['end']}. {rest}",
        )
        _layout(figure, plot_top=0.86, plot_bottom=0.5)
        figure.savefig(path, dpi=200)
        plt.close(figure)
    return {
        "leads_every_month": leads_every_month,
        "min_share": float(share.min()), "max_share": float(share.max()),
    }


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
    chart = staging / "trips_by_service.png"
    _service_chart(summary, window, chart)
    destinations[config.path("outputs") / chart.name] = chart
    month_chart = staging / "share_by_month.png"
    monthly_view = _monthly_share_chart(monthly, summary.iloc[0]["service_type"], window, month_chart)
    destinations[config.path("outputs") / month_chart.name] = month_chart

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
    if monthly_view["leads_every_month"] and summary["months"].max() > 1:
        lines.extend([
            "", f"เมื่อแยกดูรายเดือน {names} ยังมากที่สุดทุกเดือน โดยมีสัดส่วน "
            f"{monthly_view['min_share']:.1f}% ถึง {monthly_view['max_share']:.1f}% ของ valid trips ในเดือนนั้น",
        ])
    lines.extend([
        "", f"รวม valid trips: {totals['valid_rows']:,} เที่ยว",
        f"Raw records: {totals['raw_rows']:,} แถว; rejected: {totals['rejected_rows']:,} แถว",
        f"ผลเทียบ baseline S1: {verification['baseline']['status']}", "",
        "ยอดตาม borough ใช้ pickup borough และรวมค่าที่หา borough ไม่ได้ไว้ใน Unknown",
        "ผมใช้ valid trips ทั้งหมดในการคำนวณ ส่วน sample เป็นตัวอย่างจากแต่ละ batch สำหรับตรวจ schema",
        "ที่มาและผลตรวจอยู่ใน monthly_trip_counts.csv, quality_report.csv และ verify_run.json",
    ])
    answer = staging / "answer.md"
    answer.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
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
        receipts[destination.relative_to(config.root).as_posix()] = {"sha256": digest, "size": size}
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
            name: proof["artifacts"][(config.path("outputs") / name).relative_to(config.root).as_posix()]["sha256"]
            for name in ANALYSIS_FILES
        },
    }
    write_json_atomic(summary_path, summary)
    return directory


def capture_evidence(config: Config, label: str) -> Path:
    with run_lock(config.path("state")):
        return _capture_evidence(config, label)
