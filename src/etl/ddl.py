"""PostgreSQL DDL generated from dataset metadata."""

from __future__ import annotations

from typing import Any, Iterable

from .config import DatasetConfig
from .types import postgres_type


def quote_identifier(identifier: str) -> str:
    """Quote one PostgreSQL identifier, including reserved words such as order."""
    if not isinstance(identifier, str) or not identifier:
        raise ValueError("PostgreSQL identifiers must be non-empty strings")
    return '"' + identifier.replace('"', '""') + '"'


def quote_qualified(schema: str, name: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(name)}"


def _audit_declarations(audit_columns: Iterable[dict[str, Any]] | None = None) -> list[tuple[str, str]]:
    declarations = list(audit_columns or ())
    if not declarations:
        declarations = [
            {"name": "_source_file", "type": "string", "width": 1024},
            {"name": "_source_line", "type": "integer"},
            {"name": "_run_id", "type": "string", "width": 64},
        ]
    result = [(item["name"], postgres_type(item)) for item in declarations]
    result.extend([
        ("batch_id", "VARCHAR(64)"),
        ("created_at", "TIMESTAMPTZ"),
        ("updated_at", "TIMESTAMPTZ"),
    ])
    return result


def table_columns(
    dataset: DatasetConfig,
    *,
    audit_columns: Iterable[dict[str, Any]] | None = None,
) -> list[tuple[str, str]]:
    """Return the physical target columns in stable metadata order."""
    columns = [(name, postgres_type(declaration)) for name, declaration in dataset.columns.items()]
    # These are produced by standardization and are present in trusted Parquet.
    for name, type_name in (("surrogate_key", "VARCHAR(64)"), ("row_hash", "VARCHAR(64)")):
        if name not in {column for column, _ in columns}:
            columns.append((name, type_name))
    existing = {name for name, _ in columns}
    columns.extend((name, type_name) for name, type_name in _audit_declarations(audit_columns) if name not in existing)
    return columns


def generate_create_table(
    dataset: DatasetConfig,
    *,
    schema: str = "public",
    audit_columns: Iterable[dict[str, Any]] | None = None,
    if_not_exists: bool = True,
) -> str:
    """Generate one quoted, executable CREATE TABLE statement."""
    columns = table_columns(dataset, audit_columns=audit_columns)
    lines = [f"{quote_identifier(name)} {type_name}" for name, type_name in columns]
    keys = list(dataset.load.get("key_columns", ()))
    if keys:
        lines.append("PRIMARY KEY (" + ", ".join(quote_identifier(key) for key in keys) + ")")
    unique_columns = [name for name, declaration in dataset.columns.items() if declaration.get("unique")]
    for column in unique_columns:
        lines.append(f"UNIQUE ({quote_identifier(column)})")
    existence = "IF NOT EXISTS " if if_not_exists else ""
    return f"CREATE TABLE {existence}{quote_qualified(schema, dataset.target_table)} (\n    " + ",\n    ".join(lines) + "\n);"


generate_ddl = generate_create_table

