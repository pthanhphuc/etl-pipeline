"""Pure load decisions and transactional PostgreSQL loading."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import csv
import io
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from uuid import uuid4

import pandas as pd

from .config import DatasetConfig
from .ddl import generate_create_table, quote_identifier, quote_qualified, table_columns


@dataclass(frozen=True)
class LoadDecision:
    inserts: tuple[dict[str, Any], ...]
    updates: tuple[dict[str, Any], ...]
    unchanged: tuple[dict[str, Any], ...]
    duplicates: tuple[dict[str, Any], ...]

    @property
    def skipped(self) -> tuple[dict[str, Any], ...]:
        return self.unchanged


def _records(rows: Iterable[Mapping[str, Any]] | pd.DataFrame) -> list[dict[str, Any]]:
    if isinstance(rows, pd.DataFrame):
        return rows.to_dict(orient="records")
    return [dict(row) for row in rows]


def _key(row: Mapping[str, Any], key_columns: Sequence[str]) -> tuple[Any, ...]:
    values = []
    for column in key_columns:
        value = row.get(column)
        try:
            if pd.isna(value):
                value = None
        except (TypeError, ValueError):
            pass
        values.append(value)
    return tuple(values)


def decide_load(
    incoming: Iterable[Mapping[str, Any]] | pd.DataFrame,
    target: Iterable[Mapping[str, Any]] | pd.DataFrame,
    *,
    mode: str,
    key_columns: Sequence[str],
    compare_columns: Sequence[str] | None = None,
) -> LoadDecision:
    """Classify rows without database access.

    Duplicate incoming keys are retained in ``duplicates`` and never loaded.
    For differential updates, ``row_hash`` is preferred when present; otherwise
    the supplied comparison columns (or all incoming columns) are compared.
    """
    if mode not in {"insert_only", "differential_update", "replace_all"}:
        raise ValueError(f"unsupported load mode '{mode}'")
    if not key_columns:
        raise ValueError("at least one key column is required")
    incoming_rows, target_rows = _records(incoming), _records(target)
    target_by_key = {_key(row, key_columns): row for row in target_rows}
    seen: set[tuple[Any, ...]] = set()
    inserts: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    unchanged: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    comparison = list(compare_columns or ())
    for row in incoming_rows:
        key = _key(row, key_columns)
        if key in seen:
            duplicates.append(row)
            continue
        seen.add(key)
        if mode == "replace_all":
            inserts.append(row)
            continue
        existing = target_by_key.get(key)
        if existing is None:
            inserts.append(row)
            continue
        if mode == "insert_only":
            unchanged.append(row)
            continue
        fields = comparison or [name for name in row if not name.startswith("_")]
        if "row_hash" in row and "row_hash" in existing and row.get("row_hash") != existing.get("row_hash"):
            updates.append(row)
        elif any(row.get(field) != existing.get(field) for field in fields):
            updates.append(row)
        else:
            unchanged.append(row)
    return LoadDecision(tuple(inserts), tuple(updates), tuple(unchanged), tuple(duplicates))


def _value(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (date, datetime, Decimal)):
        return value.isoformat() if isinstance(value, (date, datetime)) else str(value)
    return value


def _copy_rows(cursor: Any, table_name: str, rows: list[dict[str, Any]], columns: list[str]) -> None:
    if not rows:
        return
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for row in rows:
        writer.writerow(["\\N" if (value := _value(row.get(column))) is None else value for column in columns])
    buffer.seek(0)
    statement = f"COPY {table_name} ({', '.join(quote_identifier(column) for column in columns)}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    with cursor.copy(statement) as copy:
        copy.write(buffer.getvalue())


def _now() -> datetime:
    return datetime.now().astimezone()


def load_rows(
    connection: Any,
    dataset: DatasetConfig,
    rows: Iterable[Mapping[str, Any]] | pd.DataFrame,
    *,
    schema: str = "public",
    batch_id: str | None = None,
    audit_columns: Iterable[dict[str, Any]] | None = None,
) -> LoadDecision:
    """Stage rows with COPY and apply one transactional, set-based load."""
    batch = batch_id or uuid4().hex
    incoming = [dict(row) for row in _records(rows)]
    target = quote_qualified(schema, dataset.target_table)
    physical_columns = [name for name, _ in table_columns(dataset, audit_columns=audit_columns)]
    business_columns = [name for name in physical_columns if name not in {"created_at", "updated_at", "batch_id"}]
    staged_columns = [name for name in business_columns if any(name in row for row in incoming)]
    if incoming:
        for row in incoming:
            row.setdefault("batch_id", batch)
    staged_columns = list(dict.fromkeys(staged_columns + (["batch_id"] if incoming else [])))
    decision = decide_load(incoming, [], mode="replace_all" if dataset.load["mode"] == "replace_all" else dataset.load["mode"],
                          key_columns=dataset.load["key_columns"])
    with connection.transaction():
        cursor = connection.cursor()
        cursor.execute(generate_create_table(dataset, schema=schema, audit_columns=audit_columns))
        stage = "_etl_stage"
        stage_columns = [(name, type_name) for name, type_name in table_columns(dataset, audit_columns=audit_columns) if name in staged_columns]
        cursor.execute(f"CREATE TEMP TABLE {quote_identifier(stage)} (" + ", ".join(f"{quote_identifier(n)} {t}" for n, t in stage_columns) + ") ON COMMIT DROP")
        _copy_rows(cursor, quote_identifier(stage), incoming, staged_columns)
        select_columns = [quote_identifier(name) for name in physical_columns if name in staged_columns]
        insert_columns = [name for name in physical_columns if name in staged_columns and name not in {"created_at", "updated_at"}]
        insert_sql_columns = ", ".join(quote_identifier(name) for name in insert_columns)
        select_sql = ", ".join(quote_identifier(name) for name in insert_columns)
        now_sql = "CURRENT_TIMESTAMP"
        if "created_at" in physical_columns:
            insert_sql_columns += ', "created_at", "updated_at"'
            select_sql += f", {now_sql}, {now_sql}"
        mode = dataset.load["mode"]
        if mode == "replace_all":
            cursor.execute(f"DELETE FROM {target}")
            cursor.execute(f"INSERT INTO {target} ({insert_sql_columns}) SELECT {select_sql} FROM {quote_identifier(stage)}")
        else:
            key_predicate = " AND ".join(f"t.{quote_identifier(k)} = s.{quote_identifier(k)}" for k in dataset.load["key_columns"])
            cursor.execute(f"INSERT INTO {target} ({insert_sql_columns}) SELECT {select_sql} FROM {quote_identifier(stage)} s WHERE NOT EXISTS (SELECT 1 FROM {target} t WHERE {key_predicate})")
            if mode == "differential_update":
                updates = [name for name in insert_columns if name not in dataset.load["key_columns"] and name not in {"created_at", "updated_at"}]
                changed = " OR ".join(f"t.{quote_identifier(name)} IS DISTINCT FROM s.{quote_identifier(name)}" for name in updates) or "FALSE"
                assignments = ", ".join(f"{quote_identifier(name)} = s.{quote_identifier(name)}" for name in updates)
                if assignments:
                    cursor.execute(f"UPDATE {target} t SET {assignments}, \"updated_at\" = {now_sql} FROM {quote_identifier(stage)} s WHERE {key_predicate} AND ({changed})")
        cursor.close()
    return decision


def load_parquet(connection: Any, dataset: DatasetConfig, path: str | Path, **kwargs: Any) -> LoadDecision:
    return load_rows(connection, dataset, pd.read_parquet(path), **kwargs)

