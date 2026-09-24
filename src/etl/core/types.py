"""Shared, strict coercion helpers used by raw checks and later stages."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

import pandas as pd


DEFAULT_BOOLEAN_VALUES = {
    True: {"true", "t", "yes", "y", "1"},
    False: {"false", "f", "no", "n", "0"},
}


def is_null(value: Any, null_tokens: Iterable[str] = ()) -> bool:
    if value is None or value is pd.NA:
        return True
    try:
        if bool(pd.isna(value)):
            return True
    except (TypeError, ValueError):
        pass
    return isinstance(value, str) and value.strip() in set(null_tokens)


def normalize_null(value: Any, null_tokens: Iterable[str] = ()) -> Any:
    return pd.NA if is_null(value, null_tokens) else value


def normalize_nulls(frame: pd.DataFrame, null_tokens: Iterable[str] = ()) -> pd.DataFrame:
    """Return a copy with configured empty/null tokens represented by ``pd.NA``."""
    tokens = tuple(null_tokens)
    result = frame.copy()
    for column in result.columns:
        result[column] = result[column].map(lambda value: normalize_null(value, tokens))
    return result


def _parse_datetime(value: Any, logical_type: str) -> date | datetime:
    if isinstance(value, (date, datetime)) and not isinstance(value, str):
        return value
    text = str(value).strip()
    parsed = pd.to_datetime(text, errors="raise", dayfirst=False)
    if logical_type == "date":
        return parsed.date()
    return parsed.to_pydatetime()


def coerce_value(value: Any, logical_type: str, *, null_tokens: Iterable[str] = ()) -> Any:
    """Strictly coerce one value, raising ``ValueError`` on invalid input."""
    value = normalize_null(value, null_tokens)
    if is_null(value):
        return None
    if logical_type == "string":
        return str(value)
    if logical_type == "integer":
        text = str(value).strip()
        if not text or (text[0] in "+-" and not text[1:].isdigit()) or not text.lstrip("+-").isdigit():
            raise ValueError(f"{value!r} is not a valid integer")
        return int(text)
    if logical_type == "decimal":
        try:
            return Decimal(str(value).strip())
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"{value!r} is not a valid decimal") from exc
    if logical_type == "boolean":
        lowered = str(value).strip().lower()
        for result, values in DEFAULT_BOOLEAN_VALUES.items():
            if lowered in values:
                return result
        raise ValueError(f"{value!r} is not a valid boolean")
    if logical_type in {"date", "timestamp"}:
        return _parse_datetime(value, logical_type)
    raise ValueError(f"unsupported logical type '{logical_type}'")


def coerce_series(series: pd.Series, logical_type: str, *, null_tokens: Iterable[str] = ()) -> pd.Series:
    """Coerce a series while preserving nulls; invalid values raise ``ValueError``."""
    return series.map(lambda value: coerce_value(value, logical_type, null_tokens=null_tokens))


def postgres_type(declaration: dict[str, Any]) -> str:
    """Map a column declaration to its PostgreSQL type for later DDL generation."""
    logical_type = declaration["type"]
    if logical_type == "string":
        return f"VARCHAR({declaration['width']})"
    if logical_type == "integer":
        return "INTEGER"
    if logical_type == "decimal":
        return f"NUMERIC({declaration.get('precision', 18)},{declaration.get('scale', 2)})"
    return {"boolean": "BOOLEAN", "date": "DATE", "timestamp": "TIMESTAMPTZ"}[logical_type]

