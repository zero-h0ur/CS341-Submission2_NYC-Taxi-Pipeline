import argparse
import logging
from pathlib import Path

import yaml

from taxi_pipeline.config import DEFAULT_CONFIG, load_config
from taxi_pipeline.ingest import build_batches, ingest_all
from taxi_pipeline.state import run_lock


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

    run_parser = commands.add_parser("run", help="Run monthly raw ingestion.")
    run_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    run_parser.add_argument("--start", required=True)
    run_parser.add_argument("--end", required=True)
    run_parser.add_argument(
        "--services", nargs="+", choices=["yellow", "green", "fhv", "fhvhv"],
    )
    run_parser.add_argument("--ingest-only", action="store_true")

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

    if not args.ingest_only:
        parser.error("Raw ingestion currently requires --ingest-only.")
    try:
        batches = build_batches(config, args.start, args.end, args.services)
    except ValueError as exc:
        parser.error(str(exc))

    try:
        with run_lock(config.path("state")):
            results = ingest_all(config, batches)
    except (OSError, ValueError, RuntimeError) as exc:
        logging.error("Ingest failed: %s", exc)
        return 1

    trip_results = results[1:]
    downloaded = sum(item.status == "DOWNLOADED" for item in trip_results)
    unchanged = sum(item.status == "UNCHANGED" for item in trip_results)
    print(
        f"Ingest complete: trip_batches={len(trip_results)} "
        f"downloaded={downloaded} unchanged={unchanged} "
        f"reference={results[0].status}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
