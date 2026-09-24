"""Database connection helpers."""

from __future__ import annotations

import os


def connection_info() -> str | None:
    """Return the configured PostgreSQL connection string."""
    return os.getenv("ETL_DATABASE_URL") or os.getenv("DATABASE_URL")
