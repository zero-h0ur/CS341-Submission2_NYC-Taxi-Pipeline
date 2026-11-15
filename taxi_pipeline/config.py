from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import yaml


DEFAULT_CONFIG = (
    Path(__file__).resolve().parents[1] / "config" / "pipeline.yaml"
)


@dataclass(frozen=True)
class Config:
    root: Path
    values: dict[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root).expanduser().resolve())

    def path(self, name: str) -> Path:
        return (self.root / self.values["paths"][name]).resolve()


def load_config(path: str | Path = DEFAULT_CONFIG) -> Config:
    config_path = Path(path).expanduser().resolve()

    with config_path.open(encoding="utf-8") as handle:
        values = yaml.safe_load(handle)

    if not isinstance(values, dict):
        raise ValueError("Configuration must be a YAML mapping.")

    for section in (
        "paths", "sources", "duckdb", "ingest", "quality", "baseline"
    ):
        if not isinstance(values.get(section), dict):
            raise ValueError(f"Missing or invalid config section: {section}")

    for name in (
        "raw", "trusted", "curated", "quarantine", "processed",
        "state", "logs", "outputs", "evidence",
    ):
        value = values["paths"].get(name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Invalid storage path: {name}")

    services = values["sources"].get("services")
    expected = {"yellow", "green", "fhv", "fhvhv"}
    if not isinstance(services, dict) or set(services) != expected:
        raise ValueError("Source mappings must contain all four services.")

    for service, mapping in services.items():
        if not isinstance(mapping, dict):
            raise ValueError(f"Invalid source mapping: {service}")
        for column in ("pickup", "dropoff", "pu_location", "do_location"):
            if not isinstance(mapping.get(column), str):
                raise ValueError(f"Missing source column: {service}.{column}")

    settings = values["duckdb"]
    if type(settings.get("threads")) is not int or settings["threads"] < 1:
        raise ValueError("DuckDB threads must be a positive integer.")

    for key in ("memory_limit", "temp_directory"):
        if not isinstance(settings.get(key), str) or not settings[key].strip():
            raise ValueError(f"Invalid DuckDB setting: {key}")

    for key in ("max_outside_month_ratio", "min_valid_ratio"):
        value = values["quality"].get(key)
        if type(value) not in (int, float) or not 0 <= value <= 1:
            raise ValueError(f"Quality ratio must be between 0 and 1: {key}")

    return Config(root=config_path.parent.parent, values=values)


def open_connection(config: Config) -> duckdb.DuckDBPyConnection:
    settings = config.values["duckdb"]
    temp_directory = (
        config.root / settings["temp_directory"]
    ).resolve()
    temp_directory.mkdir(parents=True, exist_ok=True)

    return duckdb.connect(
        database=":memory:",
        config={
            "memory_limit": settings["memory_limit"],
            "threads": settings["threads"],
            "temp_directory": str(temp_directory),
        },
    )
