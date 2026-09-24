"""Ordered, metadata-driven standardization and final row derivation."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import re
from typing import Any, Iterable

import pandas as pd

from .config import AppConfig, DatasetConfig, load_config
from .conversions import ConversionError, apply_conversion
from .types import coerce_value, is_null
from .validation import REASONS_COLUMN


class StandardizationError(ValueError):
    """Raised when metadata cannot produce the requested output projection."""


def _reason(function: str, column: str, message: str, check: str = "conversion") -> dict[str, Any]:
    return {"stage": "standardization", "check": check, "columns": [column], "function": function, "reason": message}


def _canonical(value: Any) -> str:
    if is_null(value):
        return "<NULL>"
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _append(reasons: list[dict[str, Any]], item: dict[str, Any]) -> None:
    reasons.append(item)


def _check_constraints(value: Any, column: str, constraint: dict[str, Any]) -> str | None:
    if is_null(value):
        return None
    if "allowed" in constraint and value not in constraint["allowed"]:
        return f"value {value!r} is not allowed"
    if "pattern" in constraint and not re.fullmatch(constraint["pattern"], str(value)):
        return f"value does not match pattern {constraint['pattern']!r}"
    for key, op, label in (("min", lambda a, b: a < b, "minimum"), ("max", lambda a, b: a > b, "maximum"),
                           ("min_exclusive", lambda a, b: a <= b, "exclusive minimum"),
                           ("max_exclusive", lambda a, b: a >= b, "exclusive maximum")):
        if key in constraint:
            try:
                if op(value, constraint[key]):
                    return f"value violates {label} {constraint[key]}"
            except TypeError:
                return f"value cannot be compared with {key}"
    if constraint.get("not_future") and value > date.today():
        return "date may not be in the future"
    return None


def _derived_hashes(row: pd.Series, dataset: DatasetConfig) -> tuple[str, str]:
    keys = dataset.load.get("key_columns", [])
    key_payload = "|".join(_canonical(row.get(column)) for column in keys)
    business_columns = list(dataset.produced_columns)
    payload = "|".join(f"{column}={_canonical(row.get(column))}" for column in business_columns)
    return hashlib.sha256(key_payload.encode()).hexdigest(), hashlib.sha256(payload.encode()).hexdigest()


def standardize(
    frame: pd.DataFrame,
    dataset: DatasetConfig,
    *,
    config: AppConfig | None = None,
    null_tokens: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Run configured steps, enforce final declarations, and project outputs.

    Existing raw-validation reasons are carried forward.  Rows with a failed
    conversion continue through the stage, but all outputs of that conversion
    are nulled and the row remains flagged.
    """
    config = config or load_config()
    tokens = tuple(null_tokens if null_tokens is not None else config.settings.get("null_tokens", ()))
    result = frame.copy()
    reasons = [list(items) if isinstance(items, list) else [] for items in result.get(REASONS_COLUMN, [[] for _ in range(len(result))])]

    for step in dataset.standardization:
        function = step["function"]
        column = step["column"]
        if column not in result:
            result[column] = pd.NA
        spec = config.functions.get(function, {})
        declared_outputs = list(spec.get("outputs", ()))
        output_names = declared_outputs or [step.get("output", column)]
        for position, value in enumerate(result[column].tolist()):
            try:
                values = apply_conversion(value, function, options={**step, "column": column},
                                          registry=config.functions, reference_data=config.reference_data,
                                          null_tokens=tokens)
            except (ConversionError, ValueError, TypeError) as exc:
                values = {name: None for name in output_names}
                if not is_null(value, tokens) and str(value).strip():
                    _append(reasons[position], _reason(function, column, str(exc)))
            for name, converted in values.items():
                if name not in result:
                    result[name] = pd.NA
                result.at[result.index[position], name] = converted

    # Coerce every declared output after all steps. This also catches values
    # produced by a conversion that no longer fit its target declaration.
    for column in dataset.produced_columns:
        if column not in result:
            raise StandardizationError(f"declared output column '{column}' was never produced")
        declaration = dataset.columns[column]
        constraint = {**declaration, **dataset.constraints.get(column, {})}
        for position, value in enumerate(result[column].tolist()):
            if is_null(value, tokens):
                result.at[result.index[position], column] = pd.NA
                continue
            try:
                converted = coerce_value(value, declaration["type"], null_tokens=tokens)
                if declaration.get("width") is not None and len(str(converted)) > declaration["width"]:
                    raise ValueError(f"value length exceeds maximum {declaration['width']}")
                violation = _check_constraints(converted, column, constraint)
                if violation:
                    raise ValueError(violation)
                result.at[result.index[position], column] = converted
            except (ValueError, TypeError) as exc:
                result.at[result.index[position], column] = pd.NA
                _append(reasons[position], _reason("coerce", column, str(exc), "constraint"))

    result[REASONS_COLUMN] = reasons
    hashes = result.apply(lambda row: _derived_hashes(row, dataset), axis=1)
    result["surrogate_key"] = hashes.map(lambda item: item[0])
    result["row_hash"] = hashes.map(lambda item: item[1])
    metadata = [column for column in result.columns if column.startswith("_") and column != REASONS_COLUMN]
    projection = list(dataset.produced_columns) + ["surrogate_key", "row_hash"] + metadata + [REASONS_COLUMN]
    projection = list(dict.fromkeys(column for column in projection if column in result))
    return result.loc[:, projection].copy()


standardize_data = standardize
