import hashlib
import json
import logging
import time
from typing import Any

import requests
import pyarrow.parquet as pq

from taxi_pipeline.config import Config, open_connection
from taxi_pipeline.curate import build_curated
from taxi_pipeline.ingest import Batch, ingest_source
from taxi_pipeline.quality import (
    REPORT_FIELDS, load_zone_lookup, validate_batch, write_quality_report,
)
from taxi_pipeline.report import generate_reports, publish_artifacts
from taxi_pipeline.state import (
    append_batch_history, append_run_history, load_manifest, marker_path,
    new_run_id, run_lock, run_log, sha256_file, status_text, utc_now,
    write_json_atomic,
)
from taxi_pipeline.transform import TRUSTED_SCHEMA, partition_file, transform_batch
from taxi_pipeline.verify import verify_batch, verify_run


logger = logging.getLogger(__name__)


def processing_fingerprint(config: Config, batch: Batch) -> str:
    payload = {
        "contract_version": 1, "schema": list(TRUSTED_SCHEMA.items()),
        "mapping": config.values["sources"]["services"][batch.service],
        "quality": config.values["quality"],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _reusable_marker(
    config: Config, batch: Batch, source_sha: str, reference_sha: str,
    fingerprint: str,
) -> dict[str, Any] | None:
    path = marker_path(config, batch.batch_id)
    if not path.exists():
        return None
    try:
        marker = json.loads(path.read_text(encoding="utf-8"))
        trusted = partition_file(config, batch)
        # Skip only if the marker still matches the files on disk. Any doubt
        # sends the batch back through the gate instead of trusting old state.
        if (
            marker.get("status") != "PUBLISHED"
            or marker.get("batch_id") != batch.batch_id
            or marker.get("source_sha256") != source_sha
            or marker.get("reference_sha256") != reference_sha
            or marker.get("processing_fingerprint") != fingerprint
            or marker.get("trusted_file") != trusted.relative_to(config.root).as_posix()
            or sorted(trusted.parent.glob("*.parquet")) != [trusted]
            or not isinstance(marker.get("quality_report"), dict)
        ):
            return None
        report = marker["quality_report"]
        proof = marker.get("verification", {})
        if not isinstance(proof, dict) or not all(proof.get(key) is True for key in (
            "schema_match", "row_count_match", "required_non_null", "pickup_month_match",
            "valid_time_and_duration", "lineage_match", "borough_join_match",
            "warning_flags_match", "raw_reconciled",
        )):
            return None
        if (
            report.get("status") != "VALIDATED"
            or report.get("source_sha256") != source_sha
            or report.get("batch_id") != batch.batch_id
            or any(marker.get(key) != report.get(key) for key in (
                "raw_rows", "valid_rows", "rejected_rows", "warn_rows",
            ))
        ):
            return None
        if sha256_file(trusted) != marker.get("trusted_sha256"):
            return None
        if pq.ParquetFile(trusted).metadata.num_rows != marker.get("valid_rows"):
            return None
        if report["raw_rows"] != report["valid_rows"] + report["rejected_rows"]:
            return None
        if report["rejected_rows"]:
            rejected = config.root / report["rejected_file"]
            if pq.ParquetFile(rejected).metadata.num_rows != report["rejected_rows"]:
                return None
        return marker
    except (OSError, ValueError, AttributeError, TypeError, KeyError):
        logger.warning("CheckBatch found an invalid marker or artifact: %s", batch.batch_id)
        return None


def _process_batch(
    connection, config: Config, session: requests.Session,
    manifest: dict[str, Any], batch: Batch, run_id: str,
    reference_sha: str, force: bool,
) -> dict[str, Any]:
    clock = time.perf_counter()
    record = {
        "run_id": run_id, "batch_id": batch.batch_id,
        "started_at": utc_now(), "source_sha256": None,
        "reference_sha256": reference_sha, "quality_report": None,
    }
    state = "DetectBatch"
    try:
        logger.info("%s batch_id=%s", state, batch.batch_id)
        arrival = ingest_source(
            config, session, manifest, batch.batch_id, batch.url, batch.raw_path,
        )
        source_sha = arrival.metadata["sha256"]
        record["source_sha256"] = source_sha
        fingerprint = processing_fingerprint(config, batch)
        state = "CheckBatch"
        logger.info("%s batch_id=%s force=%s", state, batch.batch_id, force)
        marker = None if force else _reusable_marker(
            config, batch, source_sha, reference_sha, fingerprint,
        )
        if marker is not None:
            record.update(
                status="ALREADY_PROCESSED", quality_report=marker["quality_report"],
                processed_at=marker["finished_at"], trusted_file=marker["trusted_file"],
                verification={"trusted_artifact_unchanged": True},
            )
        else:
            # Remove the old marker before touching data. If this run dies halfway,
            # the next run must not see a "done" flag for a half-written batch.
            marker_path(config, batch.batch_id).unlink(missing_ok=True)
            state = "ValidateBatch"
            logger.info("%s batch_id=%s", state, batch.batch_id)
            quality = validate_batch(connection, config, batch, run_id, source_sha)
            record["quality_report"] = quality.report
            if quality.status == "QUARANTINED":
                record.update(status="QUARANTINED", error=quality.report["block_reasons"])
            else:
                state = "TransformBatch"
                logger.info("%s batch_id=%s", state, batch.batch_id)
                trusted = transform_batch(connection, config, batch, quality, run_id)
                state = "VerifyBatch"
                logger.info("%s batch_id=%s", state, batch.batch_id)
                verification = verify_batch(connection, config, batch, quality.report)
                marker = {
                    "batch_id": batch.batch_id, "status": "PUBLISHED", "run_id": run_id,
                    "source_file": batch.raw_path.name, "source_sha256": source_sha,
                    "reference_sha256": reference_sha,
                    "processing_fingerprint": fingerprint,
                    "trusted_file": trusted.relative_to(config.root).as_posix(),
                    "trusted_sha256": sha256_file(trusted),
                    "started_at": record["started_at"], "finished_at": utc_now(),
                    "quality_report": quality.report, "verification": verification,
                    **{key: quality.report[key] for key in (
                        "raw_rows", "valid_rows", "rejected_rows", "warn_rows",
                    )},
                }
                state = "PublishBatch"
                logger.info("%s batch_id=%s", state, batch.batch_id)
                write_json_atomic(marker_path(config, batch.batch_id), marker)
                record.update(
                    status="PUBLISHED", processed_at=marker["finished_at"],
                    trusted_file=marker["trusted_file"], verification=verification,
                )
    except Exception as exc:
        record.update(status="FAILED", failed_state=state, error=str(exc))
        try:
            marker_path(config, batch.batch_id).unlink(missing_ok=True)
        except OSError as cleanup_error:
            record["marker_cleanup_error"] = str(cleanup_error)
        logger.exception("BatchFailed batch_id=%s state=%s", batch.batch_id, state)
    report = record.get("quality_report") or {}
    record.update({key: report.get(key) for key in (
        "raw_rows", "valid_rows", "rejected_rows", "warn_rows",
    )})
    record.update(finished_at=utc_now(), duration_seconds=round(time.perf_counter() - clock, 3))
    append_batch_history(config, record)
    logger.info(
        "BatchFinished batch_id=%s status=%s duration_seconds=%s",
        batch.batch_id, record["status"], record["duration_seconds"],
    )
    return record


def _write_records_quality(config: Config, records: list[dict[str, Any]]) -> None:
    reports = []
    for item in records:
        report = item.get("quality_report")
        if report is None:
            report = dict.fromkeys(REPORT_FIELDS)
            service, month = item["batch_id"].rsplit("_", 1)
            report.update(
                run_id=item["run_id"], batch_id=item["batch_id"], status="FAILED",
                service_type=service, pickup_month=month,
                source_file=f"{service}_tripdata_{month}.parquet",
                source_sha256=item["source_sha256"],
                block_reasons=item.get("error", ""), checked_at=item["finished_at"],
            )
        reports.append(report)
    write_quality_report(config, reports)


def _publish_all(
    config: Config, batches: list[Batch], run_id: str,
    force: bool, records: list[dict[str, Any]],
):
    write_quality_report(config, [])
    manifest = load_manifest(config.path("state") / "manifest.json")
    with requests.Session() as session, open_connection(config) as connection:
        reference = ingest_source(
            config, session, manifest, "taxi_zone_lookup",
            config.values["sources"]["zone_lookup_url"],
            config.path("raw") / "reference" / "taxi_zone_lookup.csv",
        )
        load_zone_lookup(connection, config)
        for batch in batches:
            record = _process_batch(
                connection, config, session, manifest, batch, run_id,
                reference.metadata["sha256"], force,
            )
            records.append(record)
            _write_records_quality(config, records)
    return records


def _diagnostics(config, batches, run_id, mode, records):
    reference_status = ""
    with requests.Session() as session, open_connection(config) as connection:
        if mode == "ingest":
            manifest = load_manifest(config.path("state") / "manifest.json")
            reference = ingest_source(
                config, session, manifest, "taxi_zone_lookup",
                config.values["sources"]["zone_lookup_url"],
                config.path("raw") / "reference" / "taxi_zone_lookup.csv",
            )
            reference_status = reference.status
        else:
            write_quality_report(config, [])
            load_zone_lookup(connection, config)
        for batch in batches:
            clock = time.perf_counter()
            record = {
                "run_id": run_id, "batch_id": batch.batch_id,
                "source_sha256": None, "started_at": utc_now(),
            }
            try:
                if mode == "ingest":
                    arrival = ingest_source(
                        config, session, manifest, batch.batch_id, batch.url, batch.raw_path,
                    )
                    record.update(status=arrival.status, source_sha256=arrival.metadata["sha256"])
                else:
                    quality = validate_batch(connection, config, batch, run_id)
                    record.update(
                        status=quality.status, quality_report=quality.report,
                        source_sha256=quality.report["source_sha256"],
                    )
                    if quality.status == "QUARANTINED":
                        marker_path(config, batch.batch_id).unlink(missing_ok=True)
            except Exception as exc:
                record.update(status="FAILED", error=str(exc))
                try:
                    marker_path(config, batch.batch_id).unlink(missing_ok=True)
                except OSError as cleanup_error:
                    record["marker_cleanup_error"] = str(cleanup_error)
                logger.exception("DiagnosticBatchFailed batch_id=%s", batch.batch_id)
            report = record.get("quality_report") or {}
            record.update({key: report.get(key) for key in (
                "raw_rows", "valid_rows", "rejected_rows", "warn_rows",
            )})
            record.update(
                finished_at=utc_now(), duration_seconds=round(time.perf_counter() - clock, 3),
            )
            records.append(record)
            append_batch_history(config, record)
            if mode == "validate":
                _write_records_quality(config, records)
            logger.info(
                "DiagnosticBatch batch_id=%s mode=%s status=%s duration_seconds=%s",
                batch.batch_id, mode, record["status"], record["duration_seconds"],
            )
    return reference_status


def _build_outputs(config, batches, run_id, records):
    proof_path = config.path("outputs") / "verify_run.json"
    logger.info("BuildCurated run_id=%s batches=%s", run_id, len(batches))
    with open_connection(config) as connection:
        paths = build_curated(connection, config, batches, run_id)
        logger.info("VerifyRun run_id=%s", run_id)
        verification = verify_run(connection, config, batches, run_id, paths, records)
        if not verification["passed"]:
            write_json_atomic(proof_path, verification)
            failed = [name for name, passed in verification["checks"].items() if not passed]
            raise ValueError(f"VerifyRun failed: {', '.join(failed)}")
        progress = {**verification, "passed": False, "status": "RUNNING", "stage": "Report"}
        write_json_atomic(proof_path, progress)
        logger.info("Report run_id=%s baseline=%s", run_id, verification["baseline"]["status"])
        destinations = generate_reports(connection, config, paths, verification)
        receipts = publish_artifacts(config, destinations)
        verification["artifacts"] = receipts
        verification["checks"]["reports_published"] = True
        write_json_atomic(proof_path, verification)
    logger.info("VerifyRunPassed run_id=%s valid_rows=%s", run_id, verification["totals"]["valid_rows"])


def _fail_run_verification(config: Config, run_id: str, error: str) -> None:
    path = config.path("outputs") / "verify_run.json"
    proof = {}
    if path.exists():
        try:
            candidate = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(candidate, dict) and candidate.get("run_id") == run_id:
                proof = candidate
        except (OSError, ValueError):
            pass
    proof.update(run_id=run_id, status="FAILED", passed=False, error=error, checked_at=utc_now())
    write_json_atomic(path, proof)


def execute_run(config: Config, batches: list[Batch], mode: str, force: bool = False):
    if not batches or mode not in {"full", "ingest", "validate"}:
        raise ValueError("A supported mode and at least one batch are required.")
    for key in ("max_attempts", "connect_timeout_seconds", "read_timeout_seconds", "chunk_bytes"):
        value = config.values["ingest"].get(key)
        if type(value) is not int or value < 1:
            raise ValueError(f"Ingest setting must be a positive integer: {key}")
    run_id = new_run_id()
    clock = time.perf_counter()
    summary = {
        "run_id": run_id, "mode": mode, "start": batches[0].month,
        "end": batches[-1].month,
        "services": "|".join(dict.fromkeys(batch.service for batch in batches)),
        "started_at": utc_now(), "requested_batches": len(batches),
        "error": "", "reference": "", "records": [],
    }
    with run_lock(config.path("state")), run_log(config, run_id) as log_file:
        logger.info("RunStarted run_id=%s mode=%s force=%s", run_id, mode, force)
        try:
            if mode == "full":
                write_json_atomic(config.path("outputs") / "verify_run.json", {
                    "run_id": run_id, "status": "RUNNING", "passed": False,
                    "window": {"start": batches[0].month, "end": batches[-1].month},
                })
                _publish_all(config, batches, run_id, force, summary["records"])
                accepted = (
                    len(summary["records"]) == len(batches)
                    and all(item["status"] in {"PUBLISHED", "ALREADY_PROCESSED"}
                            for item in summary["records"])
                )
                if accepted:
                    _build_outputs(config, batches, run_id, summary["records"])
                else:
                    _fail_run_verification(config, run_id, "Some requested batches were not accepted.")
            else:
                summary["reference"] = _diagnostics(
                    config, batches, run_id, mode, summary["records"],
                )
            status_text(config)
        except Exception as exc:
            summary["error"] = str(exc)
            logger.exception("RunFailed run_id=%s", run_id)
            if mode == "full":
                _fail_run_verification(config, run_id, str(exc))
        for field, status in {
            "published": "PUBLISHED", "already_processed": "ALREADY_PROCESSED",
            "quarantined": "QUARANTINED", "failed": "FAILED",
            "downloaded": "DOWNLOADED", "unchanged": "UNCHANGED", "validated": "VALIDATED",
        }.items():
            summary[field] = sum(item["status"] == status for item in summary["records"])
        complete = len(summary["records"]) == len(batches)
        summary["status"] = "SUCCESS" if (
            complete and not summary["error"] and not summary["failed"] and not summary["quarantined"]
        ) else "FAILED"
        summary.update(
            finished_at=utc_now(), duration_seconds=round(time.perf_counter() - clock, 3),
            log_file=log_file.relative_to(config.root).as_posix(),
        )
        logger.info(
            "RunFinished run_id=%s status=%s duration_seconds=%s",
            run_id, summary["status"], summary["duration_seconds"],
        )
        append_run_history(config, summary)
    return summary
