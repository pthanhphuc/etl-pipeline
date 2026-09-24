"""Build and parse configured dataset/date paths."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import re
from typing import Any

from .config import AppConfig, load_config
from .errors import ConfigurationError


class PathResolutionError(ConfigurationError):
    """Raised when a source prefix cannot be resolved from configuration."""


@dataclass(frozen=True)
class PathContext:
    dataset: str
    source_date: date
    source_prefix: Path
    raw_files: Path
    transform_partition: Path
    warning_partition: Path


def _render(template: str, values: dict[str, Any]) -> Path:
    try:
        return Path(template.format(**values))
    except (KeyError, ValueError) as exc:
        raise PathResolutionError(f"path template '{template}' cannot be rendered: {exc}") from exc


def _values(config: AppConfig, dataset: str, source_date: date) -> dict[str, Any]:
    if dataset not in config.datasets:
        raise PathResolutionError(f"unknown dataset '{dataset}'")
    return {"raw_root": config.paths["raw_root"], "transform_root": config.paths["transform_root"], "warning_root": config.paths["warning_root"], "dataset": dataset, "yyyy": f"{source_date.year:04d}", "mm": f"{source_date.month:02d}", "dd": f"{source_date.day:02d}"}


def build_paths(dataset: str, source_date: date, config: AppConfig | None = None) -> PathContext:
    config = config or load_config()
    values = _values(config, dataset, source_date)
    paths = config.paths
    return PathContext(dataset, source_date,
                       _render(paths["source_prefix"], values),
                       _render(paths["source_files"], values),
                       _render(paths["transform_partition"], values),
                       _render(paths["warning_partition"], values))


def build_source_prefix(dataset: str, source_date: date, config: AppConfig | None = None) -> str:
    return build_paths(dataset, source_date, config).source_prefix.as_posix()


def parse_source_prefix(source_prefix: str | Path, config: AppConfig | None = None) -> PathContext:
    config = config or load_config()
    text = str(source_prefix).replace("\\", "/").rstrip("/")
    template = config.paths["source_prefix"].replace("\\", "/")
    raw_root = re.escape(str(config.paths["raw_root"]).replace("\\", "/").strip("/"))
    pattern = re.escape(template).replace("\\{raw_root\\}", raw_root)
    pattern = pattern.replace(r"\{dataset\}", r"(?P<dataset>[^/]+)")
    pattern = pattern.replace(r"\{yyyy\}", r"(?P<yyyy>\d{4})")
    pattern = pattern.replace(r"\{mm\}", r"(?P<mm>\d{2})")
    pattern = pattern.replace(r"\{dd\}", r"(?P<dd>\d{2})")
    match = re.fullmatch(pattern, text)
    if not match:
        raise PathResolutionError(f"malformed source prefix '{source_prefix}'")
    values = match.groupdict()
    dataset = values["dataset"]
    try:
        source_date = date(int(values["yyyy"]), int(values["mm"]), int(values["dd"]))
    except ValueError as exc:
        raise PathResolutionError(f"impossible date in source prefix '{source_prefix}'") from exc
    return build_paths(dataset, source_date, config)


parse_path = parse_source_prefix
