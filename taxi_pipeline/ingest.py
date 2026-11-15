import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import requests

from taxi_pipeline.config import Config
from taxi_pipeline.state import (
    load_manifest, sha256_file, utc_now, write_json_atomic,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Batch:
    service: str
    month: str
    url: str
    raw_path: Path

    @property
    def batch_id(self) -> str:
        return f"{self.service}_{self.month}"


@dataclass(frozen=True)
class IngestResult:
    source_id: str
    status: str
    metadata: dict[str, Any]


def build_batches(
    config: Config, start: str, end: str,
    services: list[str] | None = None,
) -> list[Batch]:
    for value in (start, end):
        if re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value) is None:
            raise ValueError("Months must use YYYY-MM format.")
    current = date.fromisoformat(f"{start}-01")
    final = date.fromisoformat(f"{end}-01")
    if current > final:
        raise ValueError("Start month must not be after end month.")

    available = config.values["sources"]["services"]
    selected = list(available) if services is None else services
    if not selected or len(selected) != len(set(selected)):
        raise ValueError("Services must be nonempty and must not repeat.")
    if any(service not in available for service in selected):
        raise ValueError("Unknown service type.")

    batches = []
    template = config.values["sources"]["trip_url_template"]
    while True:
        month = f"{current.year:04d}-{current.month:02d}"
        for service in selected:
            filename = f"{service}_tripdata_{month}.parquet"
            batches.append(Batch(
                service, month, template.format(service=service, month=month),
                config.path("raw") / service / filename,
            ))
        if current == final:
            break
        current = (
            date(current.year + 1, 1, 1) if current.month == 12
            else date(current.year, current.month + 1, 1)
        )
    return batches


def _metadata(response: requests.Response) -> dict[str, Any]:
    length = response.headers.get("Content-Length")
    size = int(length) if length is not None else None
    if size is not None and size < 0:
        raise ValueError("Negative Content-Length.")
    return {
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
        "size": size,
    }


def _head(
    session: requests.Session, url: str, timeout: tuple[int, int],
) -> dict[str, Any]:
    try:
        with session.head(
            url, timeout=timeout, allow_redirects=True,
            headers={"Accept-Encoding": "identity"},
        ) as response:
            response.raise_for_status()
            return _metadata(response)
    except (requests.RequestException, ValueError) as exc:
        logger.info("Source metadata unavailable; using GET: %s", exc)
        return {}


def _archive_raw(
    config: Config, path: Path, previous: dict[str, Any],
    digest: str, reason: str,
) -> None:
    relative = path.relative_to(config.path("raw"))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    archive = config.path("raw") / "_archive" / relative.parent / (
        f"{path.stem}__{stamp}__{digest[:12]}__{uuid4().hex[:8]}{path.suffix}"
    )
    archive.parent.mkdir(parents=True, exist_ok=True)
    size = path.stat().st_size
    path.replace(archive)
    write_json_atomic(archive.with_suffix(archive.suffix + ".json"), {
        "archived_at": utc_now(), "reason": reason,
        "original_path": str(path.relative_to(config.root)),
        "sha256": digest, "size": size, "previous_manifest": previous,
    })
    logger.info("Archived raw file: %s reason=%s", archive, reason)


def _download(
    session: requests.Session, source_id: str, url: str,
    part: Path, options: dict[str, Any],
) -> dict[str, Any]:
    timeout = (
        options["connect_timeout_seconds"], options["read_timeout_seconds"],
    )
    attempts = options["max_attempts"]
    part.unlink(missing_ok=True)
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            logger.info("Download source_id=%s attempt=%d/%d", source_id, attempt, attempts)
            with session.get(
                url, stream=True, timeout=timeout,
                headers={"Accept-Encoding": "identity"},
            ) as response:
                response.raise_for_status()
                encoding = response.headers.get("Content-Encoding", "identity")
                if encoding.lower() != "identity":
                    raise ValueError(f"Unexpected Content-Encoding: {encoding}")
                metadata = _metadata(response)
                digest = hashlib.sha256()
                size = 0
                next_progress = 64 * 1024 * 1024
                with part.open("wb") as handle:
                    for chunk in response.iter_content(options["chunk_bytes"]):
                        if not chunk:
                            continue
                        handle.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
                        if size >= next_progress:
                            logger.info("Download source_id=%s bytes=%d", source_id, size)
                            next_progress = size + 64 * 1024 * 1024
                    handle.flush()
                    os.fsync(handle.fileno())
                if metadata["size"] is not None and size != metadata["size"]:
                    raise ValueError("Downloaded size differs from Content-Length.")
                return {**metadata, "size": size, "sha256": digest.hexdigest()}
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            part.unlink(missing_ok=True)
            logger.warning("Download failed source_id=%s: %s", source_id, exc)
            if attempt < attempts:
                time.sleep(min(2 ** (attempt - 1), 8))
    raise RuntimeError(f"Download exhausted retries: {source_id}: {last_error}") from last_error


def ingest_source(
    config: Config, session: requests.Session, manifest: dict[str, Any],
    source_id: str, url: str, path: Path,
) -> IngestResult:
    logger.info("DetectBatch source_id=%s", source_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    part.unlink(missing_ok=True)
    previous = manifest["files"].get(source_id, {})
    local_sha = sha256_file(path) if path.exists() else None
    local_size = path.stat().st_size if path.exists() else None
    options = config.values["ingest"]
    remote = _head(session, url, (
        options["connect_timeout_seconds"], options["read_timeout_seconds"],
    ))
    local_matches = (
        local_sha is not None and previous.get("url") == url
        and previous.get("sha256") == local_sha
        and previous.get("size") == local_size
    )
    remote_matches = (
        remote.get("etag") is not None
        and remote["etag"] == previous.get("etag")
        and (remote.get("size") is None or remote["size"] == local_size)
    )

    if local_matches and remote_matches:
        entry = {**previous, "checked_at": utc_now()}
        status = "UNCHANGED"
    else:
        reasons = []
        if local_sha is not None:
            if previous and not local_matches:
                reasons.append("local_or_url_changed")
            if remote.get("size") is not None and remote["size"] != local_size:
                reasons.append("source_size_changed")
            if (
                remote.get("etag") is not None and previous.get("etag") is not None
                and remote["etag"] != previous["etag"]
            ):
                reasons.append("source_etag_changed")
            if reasons:
                _archive_raw(config, path, previous, local_sha, ",".join(reasons))

        metadata = _download(session, source_id, url, part, options)
        if path.exists() and metadata["sha256"] == local_sha:
            part.unlink()
            status = "UNCHANGED"
        else:
            if path.exists():
                _archive_raw(config, path, previous, local_sha, "source_content_changed")
            part.replace(path)
            status = "DOWNLOADED"
        entry = {
            "url": url, "path": str(path.relative_to(config.root)),
            **metadata, "downloaded_at": utc_now(), "checked_at": utc_now(),
        }

    manifest["files"][source_id] = entry
    write_json_atomic(config.path("state") / "manifest.json", manifest)
    logger.info("DetectBatch source_id=%s status=%s size=%d", source_id, status, entry["size"])
    return IngestResult(source_id, status, entry)


def ingest_all(config: Config, batches: list[Batch]) -> list[IngestResult]:
    options = config.values["ingest"]
    for key in (
        "max_attempts", "connect_timeout_seconds",
        "read_timeout_seconds", "chunk_bytes",
    ):
        if type(options.get(key)) is not int or options[key] < 1:
            raise ValueError(f"Ingest setting must be a positive integer: {key}")
    manifest = load_manifest(config.path("state") / "manifest.json")
    with requests.Session() as session:
        results = [ingest_source(
            config, session, manifest, "taxi_zone_lookup",
            config.values["sources"]["zone_lookup_url"],
            config.path("raw") / "reference" / "taxi_zone_lookup.csv",
        )]
        for batch in batches:
            results.append(ingest_source(
                config, session, manifest, batch.batch_id, batch.url, batch.raw_path,
            ))
    return results
