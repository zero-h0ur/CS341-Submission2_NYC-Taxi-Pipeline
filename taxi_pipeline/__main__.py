import argparse
from pathlib import Path

import yaml

from taxi_pipeline.config import DEFAULT_CONFIG, load_config


def main() -> int:
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

    args = parser.parse_args()

    try:
        config = load_config(args.config)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        parser.error(str(exc))

    print("Configuration loaded")
    print(f"project_root={config.root}")
    print(f"services={','.join(config.values['sources']['services'])}")
    print(f"memory_limit={config.values['duckdb']['memory_limit']}")
    print(f"threads={config.values['duckdb']['threads']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
