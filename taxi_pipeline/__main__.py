import argparse
import logging
from pathlib import Path

import duckdb
import yaml

from taxi_pipeline.config import DEFAULT_CONFIG, load_config
from taxi_pipeline.ingest import build_batches
from taxi_pipeline.pipeline import execute_run
from taxi_pipeline.state import status_text


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        prog="taxi_pipeline",
        description="NYC TLC local batch pipeline.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    config_parser = commands.add_parser(
        "config",
        help="Load and display pipeline configuration.",
    )
    config_parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
    )

    status_parser = commands.add_parser("status", help="Display batch processing status.")
    status_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)

    run_parser = commands.add_parser("run", help="Publish monthly trusted batches.")
    run_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    run_parser.add_argument("--start", required=True)
    run_parser.add_argument("--end", required=True)
    run_parser.add_argument(
        "--services", nargs="+", choices=["yellow", "green", "fhv", "fhvhv"],
    )
    mode = run_parser.add_mutually_exclusive_group()
    mode.add_argument("--ingest-only", action="store_true")
    mode.add_argument("--validate-only", action="store_true")
    run_parser.add_argument("--force", action="store_true")

    args = parser.parse_args()

    try:
        config = load_config(args.config)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        parser.error(str(exc))

    if args.command == "config":
        print("Configuration loaded")
        print(f"project_root={config.root}")
        print(f"services={','.join(config.values['sources']['services'])}")
        print(f"memory_limit={config.values['duckdb']['memory_limit']}")
        print(f"threads={config.values['duckdb']['threads']}")
        return 0

    if args.command == "status":
        try:
            print(status_text(config), end="")
        except (OSError, ValueError) as exc:
            logging.error("Status failed: %s", exc)
            return 1
        return 0
    if args.force and (args.ingest_only or args.validate_only):
        parser.error("--force applies to full pipeline runs.")
    try:
        batches = build_batches(config, args.start, args.end, args.services)
    except ValueError as exc:
        parser.error(str(exc))

    run_mode = "ingest" if args.ingest_only else "validate" if args.validate_only else "full"
    try:
        result = execute_run(config, batches, run_mode, args.force)
    except (OSError, ValueError, RuntimeError, duckdb.Error) as exc:
        logging.error("Run failed: %s", exc)
        return 1

    if run_mode == "validate":
        print(
            f"Validation complete: trip_batches={len(result['records'])} "
            f"validated={result['validated']} quarantined={result['quarantined']}"
        )
        print(f"Quality report: {config.path('outputs') / 'quality_report.csv'}")
    elif run_mode == "ingest":
        print(
            f"Ingest complete: trip_batches={len(result['records'])} "
            f"downloaded={result['downloaded']} unchanged={result['unchanged']} "
            f"reference={result['reference']}"
        )
    else:
        print(
            f"Run complete: status={result['status']} published={result['published']} "
            f"already_processed={result['already_processed']} "
            f"quarantined={result['quarantined']} failed={result['failed']}"
        )
        if result["status"] == "SUCCESS":
            print(f"verification={config.path('outputs') / 'verify_run.json'}")
            print(f"answer={config.path('outputs') / 'answer.md'}")
    print(f"run_id={result['run_id']}")
    print(f"run_status={result['status']}")
    print(f"log_file={config.root / result['log_file']}")
    if result["error"]:
        print(f"error={result['error']}")
    return 0 if result["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
