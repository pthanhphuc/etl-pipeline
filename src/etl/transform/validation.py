"""Raw, metadata-derived validation checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd

from etl.core.config import DatasetConfig
from etl.core.types import coerce_value, is_null


REASONS_COLUMN = "_validation_reasons"


@dataclass(frozen=True)
class ValidationResult:
    frame: pd.DataFrame
    reasons_column: str = REASONS_COLUMN

    @property
    def flagged(self) -> pd.Series:
        return self.frame[self.reasons_column].map(bool)

    @property
    def valid(self) -> pd.DataFrame:
        return self.frame.loc[~self.flagged].copy()

    @property
    def invalid(self) -> pd.DataFrame:
        return self.frame.loc[self.flagged].copy()


def _reason(check: str, columns: Iterable[str], message: str) -> dict[str, Any]:
    return {"stage": "raw_validation", "check": check, "columns": list(columns), "reason": message}


def _append(reasons: list[dict[str, Any]], check: str, column: str | Iterable[str], message: str) -> None:
    columns = [column] if isinstance(column, str) else list(column)
    reasons.append(_reason(check, columns, message))


def validate_raw(
    frame: pd.DataFrame,
    dataset: DatasetConfig,
    *,
    null_tokens: Iterable[str] = (),
) -> ValidationResult:
    """Apply type, width, required and primary-key checks, accumulating all reasons."""
    result = frame.copy()
    reasons: list[list[dict[str, Any]]] = [[] for _ in range(len(result))]
    columns = dataset.columns
    for column, declaration in columns.items():
        if column not in result:
            continue
        values = result[column]
        logical_type = declaration["type"]
        width = declaration.get("width")
        for position, value in enumerate(values.tolist()):
            if is_null(value, null_tokens):
                if declaration.get("required"):
                    _append(reasons[position], "not_null", column, "required column is empty")
                if declaration.get("primary_key"):
                    _append(reasons[position], "primary_key", column, "primary key is empty")
                continue
            try:
                coerce_value(value, logical_type, null_tokens=null_tokens)
            except ValueError as exc:
                _append(reasons[position], "type", column, str(exc))
            if width is not None and len(str(value)) > width:
                _append(reasons[position], "max_length", column,
                        f"value length {len(str(value))} exceeds maximum {width}")

    key_columns = list(dataset.load.get("key_columns", ()))
    seen: set[tuple[Any, ...]] = set()
    for position, (_, row) in enumerate(result.iterrows()):
        key_values: list[Any] = []
        missing = False
        for column in key_columns:
            value = row.get(column, pd.NA)
            if is_null(value, null_tokens):
                missing = True
                key_values.append(None)
                continue
            try:
                key_values.append(coerce_value(value, columns[column]["type"], null_tokens=null_tokens))
            except ValueError:
                missing = True
                key_values.append(None)
        key = tuple(key_values)
        if missing:
            # Missing-key detail is already present for declared primary-key fields;
            # this also covers malformed composite keys and keeps the check explicit.
            if not any(item["check"] == "primary_key" for item in reasons[position]):
                _append(reasons[position], "primary_key", key_columns, "primary key is missing or invalid")
        elif key in seen:
            _append(reasons[position], "primary_key", key_columns, "duplicate primary key; first row is retained")
        else:
            seen.add(key)

    result[REASONS_COLUMN] = reasons
    return ValidationResult(result)


validate = validate_raw
