import csv
import fcntl
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from taxi_pipeline.config import Config


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent,
            prefix=f".{path.name}.", suffix=".tmp", delete=False,
        ) as handle:
            temporary = Path(handle.name)
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "files": {}}
    with path.open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    if (
        not isinstance(manifest, dict)
        or manifest.get("version") != 1
        or not isinstance(manifest.get("files"), dict)
        or any(not isinstance(v, dict) for v in manifest["files"].values())
    ):
        raise ValueError(f"Invalid manifest: {path}")
    return manifest


@contextmanager
def run_lock(state_directory: Path) -> Iterator[None]:
    state_directory.mkdir(parents=True, exist_ok=True)
    with (state_directory / "pipeline.lock").open("a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another pipeline process holds the run lock.") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{stamp}_{uuid4().hex[:8]}"


def marker_path(config: Config, batch_id: str) -> Path:
    return config.path("processed") / f"batch_id={batch_id}" / "_SUCCESS.json"


def append_batch_history(config: Config, record: dict[str, Any]) -> None:
    path = config.path("state") / "batch_history.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    separator = ""
    if path.exists() and path.stat().st_size:
        with path.open("rb") as previous:
            previous.seek(-1, os.SEEK_END)
            if previous.read(1) != b"\n":
                separator = "\n"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(separator + json.dumps(record, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


RUN_FIELDS = [
    "run_id", "mode", "start", "end", "services", "started_at", "finished_at",
    "duration_seconds", "status", "requested_batches", "published",
    "already_processed", "quarantined", "failed", "downloaded", "unchanged",
    "validated", "error", "log_file",
]


def append_run_history(config: Config, record: dict[str, Any]) -> None:
    path = config.path("outputs") / "run_history.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    empty = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RUN_FIELDS)
        if empty:
            writer.writeheader()
        writer.writerow({name: record.get(name, "") for name in RUN_FIELDS})
        handle.flush()
        os.fsync(handle.fileno())


@contextmanager
def run_log(config: Config, run_id: str) -> Iterator[Path]:
    path = config.path("logs") / f"run_{run_id}.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, encoding="utf-8")
    formatter = logging.Formatter(
        "%(asctime)sZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S",
    )
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    root = logging.getLogger()
    previous_level = root.level
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    try:
        yield path
    finally:
        root.removeHandler(handler)
        handler.close()
        root.setLevel(previous_level)


def status_text(config: Config) -> str:
    manifest = load_manifest(config.path("state") / "manifest.json")
    latest = {}
    history = config.path("state") / "batch_history.jsonl"
    if history.exists():
        with history.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    record = json.loads(line)
                    latest[record["batch_id"]] = record
                except (ValueError, KeyError, TypeError):
                    logging.warning("Skipping an incomplete batch history line.")
    markers = {
        path.parent.name.removeprefix("batch_id="): path
        for path in config.path("processed").glob("batch_id=*/_SUCCESS.json")
    }
    batch_ids = sorted(
        key for key in set(manifest["files"]) | set(latest) | set(markers)
        if re.fullmatch(r"(yellow|green|fhv|fhvhv)_\d{4}-(0[1-9]|1[0-2])", key)
    )
    rows = []
    for batch_id in batch_ids:
        event = latest.get(batch_id, {})
        marker = None
        invalid_marker = False
        if batch_id in markers:
            try:
                marker = json.loads(markers[batch_id].read_text(encoding="utf-8"))
                if (
                    not isinstance(marker, dict) or marker.get("status") != "PUBLISHED"
                    or marker.get("batch_id") != batch_id
                ):
                    marker = None
                    invalid_marker = True
            except (OSError, ValueError):
                invalid_marker = True
        count = event.get("valid_rows")
        processed_at = str(event.get("processed_at") or "-")
        status = event.get("status", "RAW_READY")
        if status in {"FAILED", "QUARANTINED"}:
            pass
        elif invalid_marker:
            status = "MARKER_INVALID"
        elif marker:
            source = manifest["files"].get(batch_id, {})
            reference = manifest["files"].get("taxi_zone_lookup", {})
            service, month = batch_id.rsplit("_", 1)
            trusted = (
                config.path("trusted") / "trips" / f"service_type={service}"
                / f"pickup_month={month}" / "part-0.parquet"
            )
            stale = (
                marker.get("source_sha256") != source.get("sha256")
                or marker.get("reference_sha256") != reference.get("sha256")
                or marker.get("trusted_file") != str(trusted.relative_to(config.root))
                or not trusted.is_file()
                or not (config.path("raw") / service / f"{service}_tripdata_{month}.parquet").is_file()
                or not (config.path("raw") / "reference" / "taxi_zone_lookup.csv").is_file()
                or (
                    event.get("source_sha256") is not None
                    and event["source_sha256"] != marker.get("source_sha256")
                )
            )
            status = "STALE" if stale else (
                "ALREADY_PROCESSED" if status == "ALREADY_PROCESSED" else "PUBLISHED"
            )
            count = marker.get("valid_rows")
            processed_at = str(marker.get("finished_at") or "-")
        elif status in {"PUBLISHED", "ALREADY_PROCESSED"}:
            status = "MARKER_MISSING"
        elif status in {"DOWNLOADED", "UNCHANGED"}:
            status = "RAW_READY"
        rows.append([batch_id, status, "-" if count is None else str(count), processed_at])
    headers = ["batch_id", "status", "rows", "processed_at"]
    widths = [max(len(row[i]) for row in [headers, *rows]) for i in range(4)]
    text = "\n".join(
        "  ".join(value.ljust(widths[i]) for i, value in enumerate(row)).rstrip()
        for row in [headers, *rows]
    ) + "\n"
    path = config.path("outputs") / "status.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return text
