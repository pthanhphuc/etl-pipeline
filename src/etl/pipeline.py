"""Application orchestration for the metadata-driven ETL stages."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

from .config import AppConfig, DatasetConfig, load_config
from .ddl import generate_create_table
from .loader import LoadDecision, load_parquet
from .outputs import OutputResult, promote_warning_rows, write_outputs
from .paths import PathContext, build_paths, parse_source_prefix
from .readers import read_dataset
from .standardize import standardize
from .validation import validate_raw


@dataclass(frozen=True)
class TransformResult:
    context: PathContext
    run_id: str
    output: OutputResult


def _config(config: AppConfig | None, config_dir: str | Path) -> AppConfig:
    return config or load_config(config_dir)


def _run_context(context: PathContext, run_id: str) -> PathContext:
    """Place each run in an isolated partition below the configured roots."""
    return replace(
        context,
        transform_partition=context.transform_partition / "_runs" / run_id,
        warning_partition=context.warning_partition / "_runs" / run_id,
    )


def resolve_context(source_prefix: str | Path, *, config: AppConfig | None = None,
                    config_dir: str | Path = "config") -> PathContext:
    return parse_source_prefix(source_prefix, _config(config, config_dir))


def transform_source(source_prefix: str | Path, *, config: AppConfig | None = None,
                     config_dir: str | Path = "config", run_id: str | None = None) -> TransformResult:
    """Run extract, raw validation, standardization, and durable output."""
    config = _config(config, config_dir)
    base_context = resolve_context(source_prefix, config=config)
    run_id = run_id or uuid4().hex
    context = _run_context(base_context, run_id)
    dataset = config.datasets[context.dataset]
    raw = read_dataset(dataset, config=config, context=base_context, run_id=run_id)
    validated = validate_raw(raw.frame, dataset, null_tokens=config.settings.get("null_tokens", ()))
    standardized = standardize(validated.frame, dataset, config=config)
    output = write_outputs(standardized, dataset, config=config, context=context)
    return TransformResult(context, run_id, output)


def trusted_path(source_prefix: str | Path, *, config: AppConfig | None = None,
                 config_dir: str | Path = "config", run_id: str | None = None) -> Path:
    context = resolve_context(source_prefix, config=config, config_dir=config_dir)
    return _run_context(context, run_id).transform_partition / "trusted.parquet" if run_id else context.transform_partition / "trusted.parquet"


def load_source(source_prefix: str | Path, *, config: AppConfig | None = None,
                config_dir: str | Path = "config", run_id: str | None = None,
                connection: Any | None = None, schema: str | None = None) -> LoadDecision:
    """Load only the trusted Parquet handoff; raw files are never reread."""
    config = _config(config, config_dir)
    context = resolve_context(source_prefix, config=config)
    dataset = config.datasets[context.dataset]
    path = trusted_path(source_prefix, config=config, run_id=run_id)
    if not path.exists():
        raise FileNotFoundError(f"trusted output does not exist: {path}")
    owns_connection = connection is None
    if owns_connection:
        import psycopg
        connection = psycopg.connect(_connection_info())
    try:
        return load_parquet(connection, dataset, path, schema=schema or _schema(config), batch_id=run_id,
                            audit_columns=config.settings.get("audit_columns"))
    finally:
        if owns_connection:
            connection.close()


def init_database(*, config: AppConfig | None = None, config_dir: str | Path = "config",
                  connection: Any | None = None, schema: str | None = None) -> None:
    config = _config(config, config_dir)
    owns_connection = connection is None
    if owns_connection:
        import psycopg
        connection = psycopg.connect(_connection_info())
    try:
        with connection.transaction():
            cursor = connection.cursor()
            for dataset in config.datasets.values():
                cursor.execute(generate_create_table(dataset, schema=schema or _schema(config),
                                                     audit_columns=config.settings.get("audit_columns")))
            cursor.close()
    finally:
        if owns_connection:
            connection.close()


def read_report(source_prefix: str | Path, *, config: AppConfig | None = None,
                config_dir: str | Path = "config", run_id: str | None = None) -> dict[str, Any]:
    config = _config(config, config_dir)
    context = resolve_context(source_prefix, config=config)
    summary = (_run_context(context, run_id).warning_partition if run_id else context.warning_partition) / "summary.json"
    if not summary.exists():
        raise FileNotFoundError(f"summary does not exist: {summary}")
    return json.loads(summary.read_text(encoding="utf-8"))


def promote_warning(source_prefix: str | Path, corrected: str | Path, *, config: AppConfig | None = None,
                    config_dir: str | Path = "config", run_id: str | None = None) -> int:
    config = _config(config, config_dir)
    context = resolve_context(source_prefix, config=config)
    isolated = _run_context(context, run_id) if run_id else context
    return promote_warning_rows(corrected, trusted_path=isolated.transform_partition / "trusted.parquet",
                                warning_path=isolated.warning_partition / "warning.parquet",
                                dataset=config.datasets[context.dataset], config=config)


def _schema(config: AppConfig) -> str:
    return str(config.settings.get("warehouse", {}).get("schema", os.getenv("ETL_PG_SCHEMA", "public")))


def _connection_info() -> str | None:
    return os.getenv("ETL_DATABASE_URL") or os.getenv("DATABASE_URL")
