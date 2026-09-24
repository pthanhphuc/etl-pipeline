"""Load and validate the metadata used by the ETL pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .errors import ConfigurationError


SUPPORTED_TYPES = {"string", "integer", "decimal", "boolean", "date", "timestamp"}
SUPPORTED_LOAD_MODES = {"insert_only", "differential_update", "replace_all"}


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    source: dict[str, Any]
    load: dict[str, Any]
    columns: dict[str, dict[str, Any]]
    standardization: list[dict[str, Any]]
    produced_columns: list[str]
    constraints: dict[str, dict[str, Any]]
    target_table: str
    file: Path


@dataclass(frozen=True)
class AppConfig:
    root: Path
    settings: dict[str, Any]
    functions: dict[str, dict[str, Any]]
    reference_data: dict[str, Any]
    datasets: dict[str, DatasetConfig]

    @property
    def paths(self) -> dict[str, str]:
        return self.settings["paths"]

    @property
    def readers(self) -> dict[str, Any]:
        return self.settings["readers"]


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigurationError(f"{path}: cannot read YAML: {exc}") from exc
    if not isinstance(value, dict):
        raise ConfigurationError(f"{path}: top level must be a mapping")
    return value


def _require(mapping: dict[str, Any], key: str, path: Path) -> Any:
    if key not in mapping:
        raise ConfigurationError(f"{path}: missing key '{key}'")
    return mapping[key]


def _validate_dataset(raw: dict[str, Any], path: Path, shared: dict[str, Any], functions: dict[str, Any]) -> DatasetConfig:
    name = _require(raw, "dataset", path)
    if not isinstance(name, str) or not name:
        raise ConfigurationError(f"{path}: key 'dataset' must be a non-empty string")
    target = _require(raw, "target_table", path)
    source = {**shared["defaults"]["source"], **_require(raw, "source", path)}
    load = {**shared["defaults"]["load"], **_require(raw, "load", path)}
    columns = _require(raw, "columns", path)
    steps = _require(raw, "standardization", path)
    produced = _require(raw, "produced_columns", path)
    constraints = _require(raw, "constraints", path)
    if not isinstance(columns, dict) or not columns:
        raise ConfigurationError(f"{path}: key 'columns' must be a non-empty mapping")
    if not isinstance(produced, list) or not produced:
        raise ConfigurationError(f"{path}: key 'produced_columns' must be a non-empty list")
    if not isinstance(steps, list) or not isinstance(constraints, dict):
        raise ConfigurationError(f"{path}: standardization and constraints have invalid types")
    for column, declaration in columns.items():
        if not isinstance(declaration, dict):
            raise ConfigurationError(f"{path}: columns.{column} must be a mapping")
        logical_type = declaration.get("type")
        if logical_type not in SUPPORTED_TYPES:
            raise ConfigurationError(f"{path}: columns.{column}.type: unsupported type '{logical_type}'")
        if logical_type == "string" and not isinstance(declaration.get("width"), int):
            raise ConfigurationError(f"{path}: columns.{column}.width: string columns require an integer width")
    unknown = set(produced) - set(columns)
    if unknown:
        raise ConfigurationError(f"{path}: produced_columns contains undeclared columns: {sorted(unknown)}")
    keys = load.get("key_columns")
    if not isinstance(keys, list) or not keys:
        raise ConfigurationError(f"{path}: load.key_columns must be a non-empty list")
    missing_keys = set(keys) - set(columns)
    if missing_keys:
        raise ConfigurationError(f"{path}: load.key_columns references missing columns: {sorted(missing_keys)}")
    declared_pk = {column for column, declaration in columns.items() if declaration.get("primary_key")}
    if set(keys) != declared_pk:
        raise ConfigurationError(f"{path}: load.key_columns must match columns marked primary_key")
    if load.get("mode") not in SUPPORTED_LOAD_MODES:
        raise ConfigurationError(f"{path}: load.mode: unsupported mode '{load.get('mode')}'")
    threshold = load.get("flag_threshold")
    if not isinstance(threshold, (int, float)) or not 0 <= threshold <= 1:
        raise ConfigurationError(f"{path}: load.flag_threshold must be between 0 and 1")
    for step_number, step in enumerate(steps):
        if not isinstance(step, dict) or step.get("function") not in functions:
            function = step.get("function") if isinstance(step, dict) else None
            raise ConfigurationError(f"{path}: standardization[{step_number}].function: unknown function '{function}'")
        if "column" not in step or step["column"] not in columns:
            raise ConfigurationError(f"{path}: standardization[{step_number}].column: must name a declared column")
    for column in constraints:
        if column not in columns:
            raise ConfigurationError(f"{path}: constraints.{column}: column is not declared")
    return DatasetConfig(name, source, load, columns, steps, produced, constraints, target, path)


def load_config(config_dir: str | Path = "config") -> AppConfig:
    """Discover, merge, and validate all configuration under *config_dir*."""
    directory = Path(config_dir).resolve()
    settings_file = directory / "settings.yaml"
    functions_file = directory / "functions.yaml"
    settings = _read_yaml(settings_file)
    functions_raw = _read_yaml(functions_file)
    for key in ("paths", "readers", "defaults", "null_tokens", "warehouse"):
        _require(settings, key, settings_file)
    functions = _require(functions_raw, "functions", functions_file)
    if not isinstance(functions, dict):
        raise ConfigurationError(f"{functions_file}: functions must be a mapping")
    datasets_dir = directory / "datasets"
    files = sorted(datasets_dir.glob("*.yaml"))
    if not files:
        raise ConfigurationError(f"{datasets_dir}: no dataset YAML files found")
    datasets: dict[str, DatasetConfig] = {}
    for file in files:
        dataset = _validate_dataset(_read_yaml(file), file, settings, functions)
        if dataset.name in datasets:
            raise ConfigurationError(f"{file}: duplicate dataset '{dataset.name}'")
        datasets[dataset.name] = dataset
    return AppConfig(directory, settings, functions, functions_raw.get("reference_data", {}), datasets)


load_configuration = load_config
