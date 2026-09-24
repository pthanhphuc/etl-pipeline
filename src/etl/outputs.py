"""Durable Parquet outputs for the transformation stage.

This module deliberately has no database or orchestration dependencies.  It
accepts the result of :func:`etl.standardize.standardize`, writes the two
handoff files, and returns enough information for a caller to decide whether
the load task may proceed.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import AppConfig, DatasetConfig, load_config
from .paths import PathContext, build_paths
from .validation import REASONS_COLUMN


class OutputError(ValueError):
    """Raised when a transformation output cannot be written or promoted."""


class FlagThresholdExceeded(OutputError):
    """Raised after diagnostic outputs are written when too many rows are flagged."""

    def __init__(self, message: str, result: "OutputResult") -> None:
        super().__init__(message)
        self.result = result


@dataclass(frozen=True)
class OutputResult:
    trusted_path: Path
    warning_path: Path
    summary_path: Path
    extracted: int
    trusted: int
    flagged: int
    flagged_ratio: float
    threshold: float
    can_load: bool


def _is_flagged(value: Any) -> bool:
    return isinstance(value, (list, tuple)) and bool(value)


def split_rows(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(trusted, flagged)`` without changing row order.

    A missing reasons column is equivalent to an empty reasons list.  This is
    useful for an already-corrected frame, while a malformed non-list value is
    rejected instead of silently allowing a row into the trusted output.
    """
    if REASONS_COLUMN not in frame.columns:
        reasons = pd.Series([[] for _ in range(len(frame))], index=frame.index)
    else:
        reasons = frame[REASONS_COLUMN]
        invalid = [value for value in reasons if value is not None and not isinstance(value, (list, tuple))]
        if invalid:
            raise OutputError(f"{REASONS_COLUMN} must contain lists of structured reasons")
    flagged_mask = reasons.map(_is_flagged)
    return frame.loc[~flagged_mask].copy(), frame.loc[flagged_mask].copy()


def _output_columns(frame: pd.DataFrame, *, warning: bool) -> list[str]:
    """Select stable business, hash, audit, and (for warnings) reason fields."""
    columns = [column for column in frame.columns if column != REASONS_COLUMN]
    if warning and REASONS_COLUMN in frame:
        columns.append(REASONS_COLUMN)
    return list(dict.fromkeys(columns))


def _arrow_type(declaration: dict[str, Any]) -> pa.DataType:
    logical_type = declaration["type"]
    if logical_type == "string":
        return pa.string()
    if logical_type == "integer":
        return pa.int64()
    if logical_type == "decimal":
        precision = int(declaration.get("precision", 18))
        scale = int(declaration.get("scale", 2))
        return pa.decimal128(precision, scale)
    return {
        "boolean": pa.bool_(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us", tz="UTC"),
    }[logical_type]


def _schema(frame: pd.DataFrame, dataset: DatasetConfig) -> pa.Schema:
    fields: list[pa.Field] = []
    for column in frame.columns:
        if column in dataset.columns:
            arrow_type = _arrow_type(dataset.columns[column])
        elif column == "_source_line":
            arrow_type = pa.int64()
        else:
            # Hashes, file names, run IDs, and structured reason JSON are
            # represented as strings for a stable cross-file handoff.
            arrow_type = pa.string()
        fields.append(pa.field(column, arrow_type, nullable=True))
    return pa.schema(fields)


def _prepare_for_arrow(frame: pd.DataFrame, dataset: DatasetConfig, columns: list[str]) -> pd.DataFrame:
    result = frame.reindex(columns=columns).copy()
    for column in columns:
        if column not in dataset.columns:
            continue
        logical_type = dataset.columns[column]["type"]
        if logical_type == "string":
            result[column] = result[column].map(lambda value: None if pd.isna(value) else str(value))
        elif logical_type == "integer":
            result[column] = pd.to_numeric(result[column], errors="coerce").astype("Int64")
        elif logical_type == "boolean":
            result[column] = result[column].astype("boolean")
        elif logical_type == "date":
            result[column] = pd.to_datetime(result[column], errors="coerce").dt.date
        elif logical_type == "timestamp":
            values = pd.to_datetime(result[column], errors="coerce", utc=True)
            result[column] = values
        elif logical_type == "decimal":
            # Arrow can convert Decimal objects directly and preserves the
            # declared precision/scale in the schema.
            result[column] = result[column].map(lambda value: None if pd.isna(value) else value)
    if REASONS_COLUMN in result:
        result[REASONS_COLUMN] = result[REASONS_COLUMN].map(
            lambda value: json.dumps(value or [], sort_keys=True, ensure_ascii=False, default=str)
        )
    for column in result.columns:
        if column not in dataset.columns and column != "_source_line":
            result[column] = result[column].map(lambda value: None if pd.isna(value) else str(value))
    return result


def _write_parquet(frame: pd.DataFrame, path: Path, dataset: DatasetConfig, *, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    prepared = _prepare_for_arrow(frame, dataset, columns)
    table = pa.Table.from_pandas(prepared, schema=_schema(prepared, dataset), preserve_index=False, safe=False)
    pq.write_table(table, path, compression="snappy")


def _reason_summary(flagged: pd.DataFrame) -> dict[str, int]:
    checks: Counter[str] = Counter()
    functions: Counter[str] = Counter()
    for reasons in flagged.get(REASONS_COLUMN, pd.Series(dtype=object)):
        for reason in reasons or []:
            if not isinstance(reason, dict):
                continue
            if reason.get("check"):
                checks[str(reason["check"])] += 1
            if reason.get("function"):
                functions[str(reason["function"])] += 1
    return {"checks": dict(sorted(checks.items())), "functions": dict(sorted(functions.items()))}


def _write_summary(path: Path, *, result: OutputResult, flagged: pd.DataFrame) -> None:
    payload = {
        "extracted": result.extracted,
        "trusted": result.trusted,
        "flagged": result.flagged,
        "flagged_ratio": result.flagged_ratio,
        "flag_threshold": result.threshold,
        "can_load": result.can_load,
        **_reason_summary(flagged),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_outputs(
    frame: pd.DataFrame,
    dataset: DatasetConfig | str,
    *,
    context: PathContext | None = None,
    config: AppConfig | None = None,
    output_root: str | Path | None = None,
) -> OutputResult:
    """Split and write trusted/warning Parquet files and warning summary.

    ``FlagThresholdExceeded`` is raised only after all three diagnostics are
    persisted.  Its ``result`` attribute lets a CLI/DAG report paths while
    preventing the subsequent load task from running.
    """
    config = config or load_config()
    if isinstance(dataset, str):
        if dataset not in config.datasets:
            raise OutputError(f"unknown dataset '{dataset}'")
        dataset = config.datasets[dataset]
    if context is None:
        raise OutputError("a resolved PathContext is required")
    if output_root is not None:
        root = Path(output_root)
        transform_partition = root / "transform" / dataset.name / context.source_date.isoformat()
        warning_partition = root / "warning" / dataset.name / context.source_date.isoformat()
    else:
        transform_partition = context.transform_partition
        warning_partition = context.warning_partition
    trusted, flagged = split_rows(frame)
    extracted = len(frame)
    if extracted != len(trusted) + len(flagged):
        raise OutputError("extracted row invariant failed: trusted + flagged != extracted")
    threshold = float(dataset.load["flag_threshold"])
    ratio = (len(flagged) / extracted) if extracted else 0.0
    can_load = ratio <= threshold
    trusted_path = transform_partition / "trusted.parquet"
    warning_path = warning_partition / "warning.parquet"
    summary_path = warning_partition / "summary.json"
    trusted_columns = _output_columns(trusted, warning=False)
    warning_columns = _output_columns(flagged, warning=True)
    # Use the full input schema for empty partitions so an empty side remains
    # readable and compatible with a later append/promotion.
    if not trusted_columns:
        trusted_columns = _output_columns(frame, warning=False)
    if not warning_columns:
        warning_columns = _output_columns(frame, warning=True)
    _write_parquet(trusted, trusted_path, dataset, columns=trusted_columns)
    _write_parquet(flagged, warning_path, dataset, columns=warning_columns)
    result = OutputResult(trusted_path, warning_path, summary_path, extracted, len(trusted), len(flagged), ratio, threshold, can_load)
    _write_summary(summary_path, result=result, flagged=flagged)
    if not can_load:
        raise FlagThresholdExceeded(
            f"flagged ratio {ratio:.4f} exceeds threshold {threshold:.4f}; load is blocked", result
        )
    return result


def promote_warning_rows(
    corrected: pd.DataFrame | str | Path,
    *,
    trusted_path: str | Path,
    warning_path: str | Path | None = None,
    dataset: DatasetConfig | str,
    config: AppConfig | None = None,
) -> int:
    """Promote corrected warning rows into trusted Parquet without raw rereads.

    The corrected input must contain no reasons.  If ``warning_path`` is
    supplied, promoted rows are removed by ``surrogate_key`` (or by their
    complete row identity when no key is available) and the warning file is
    rewritten.
    """
    config = config or load_config()
    if isinstance(dataset, str):
        dataset = config.datasets[dataset]
    trusted_file = Path(trusted_path)
    if isinstance(corrected, (str, Path)):
        corrected_frame = pd.read_parquet(corrected)
    else:
        corrected_frame = corrected.copy()
    if REASONS_COLUMN in corrected_frame:
        reasons = corrected_frame[REASONS_COLUMN].map(lambda value: value if isinstance(value, list) else [])
        if reasons.map(bool).any():
            raise OutputError("corrected warning rows still contain validation reasons")
        corrected_frame = corrected_frame.drop(columns=[REASONS_COLUMN])
    if corrected_frame.empty:
        return 0
    existing = pd.read_parquet(trusted_file) if trusted_file.exists() else pd.DataFrame(columns=corrected_frame.columns)
    columns = list(dict.fromkeys([*existing.columns, *corrected_frame.columns]))
    merged = pd.concat([existing.reindex(columns=columns), corrected_frame.reindex(columns=columns)], ignore_index=True)
    if "surrogate_key" in merged:
        merged = merged.drop_duplicates(subset=["surrogate_key"], keep="last")
    _write_parquet(merged, trusted_file, dataset, columns=columns)
    promoted = len(corrected_frame)
    if warning_path is not None and Path(warning_path).exists():
        warnings = pd.read_parquet(warning_path)
        if "surrogate_key" in warnings and "surrogate_key" in corrected_frame:
            keys = set(corrected_frame["surrogate_key"].dropna().tolist())
            warnings = warnings.loc[~warnings["surrogate_key"].isin(keys)].copy()
        _write_parquet(warnings, Path(warning_path), dataset, columns=_output_columns(warnings, warning=True))
    return promoted


# Names that make the stage easy to discover from a pipeline implementation.
persist_outputs = write_outputs
split_and_write = write_outputs

