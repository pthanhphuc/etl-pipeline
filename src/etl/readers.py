"""Format readers and raw-file extraction."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import pandas as pd

from .config import AppConfig, DatasetConfig, load_config
from .errors import EtlError
from .paths import PathContext
from .types import normalize_nulls


class ReaderError(EtlError):
    """Raised when source files cannot be selected or read."""


@dataclass(frozen=True)
class SourceMetadata:
    source_file: str
    source_line: int
    run_id: str


@dataclass(frozen=True)
class RawReadResult:
    frame: pd.DataFrame
    files: tuple[Path, ...]
    run_id: str

    @property
    def dataframe(self) -> pd.DataFrame:
        return self.frame


Reader = Callable[[Path, dict[str, Any]], pd.DataFrame]


def _csv_reader(path: Path, options: dict[str, Any]) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False, **options)


def _json_reader(path: Path, options: dict[str, Any]) -> pd.DataFrame:
    return pd.read_json(path, dtype=False, **options).astype("string")


def _parquet_reader(path: Path, options: dict[str, Any]) -> pd.DataFrame:
    return pd.read_parquet(path, **options).astype("string")


READERS: dict[str, Reader] = {"csv": _csv_reader, "json": _json_reader, "parquet": _parquet_reader}


def _files_for(dataset: DatasetConfig, context: PathContext) -> list[Path]:
    pattern = dataset.source.get("file_glob", "**/*.csv")
    files = sorted((context.source_prefix).glob(pattern), key=lambda path: path.as_posix().casefold())
    return [path for path in files if path.is_file()]


def read_files(
    files: list[Path] | tuple[Path, ...],
    *,
    format_name: str = "csv",
    options: dict[str, Any] | None = None,
    run_id: str | None = None,
    null_tokens: tuple[str, ...] = (),
    expected_columns: tuple[str, ...] | None = None,
) -> RawReadResult:
    """Read files in the supplied order, adding stable source metadata."""
    if not files:
        raise ReaderError("no matching source files")
    reader = READERS.get(format_name)
    if reader is None:
        raise ReaderError(f"unsupported source format '{format_name}'")
    batch_id = run_id or uuid4().hex
    parts: list[pd.DataFrame] = []
    for path in files:
        try:
            frame = reader(path, options or {})
        except Exception as exc:
            raise ReaderError(f"cannot read source file '{path}': {exc}") from exc
        frame = frame.copy()
        frame.columns = [str(column) for column in frame.columns]
        if expected_columns:
            missing = sorted(set(expected_columns) - set(frame.columns))
            if missing:
                raise ReaderError(f"source file '{path}' is missing declared columns: {missing}")
        frame["_source_file"] = path.name
        # Header is line 1, therefore the first data row is line 2.
        frame["_source_line"] = range(2, len(frame) + 2)
        frame["_run_id"] = batch_id
        parts.append(frame)
    result = pd.concat(parts, ignore_index=True, sort=False)
    result = normalize_nulls(result, null_tokens)
    return RawReadResult(result, tuple(files), batch_id)


def read_dataset(
    dataset: DatasetConfig | str,
    *,
    config: AppConfig | None = None,
    context: PathContext | None = None,
    run_id: str | None = None,
) -> RawReadResult:
    config = config or load_config()
    if isinstance(dataset, str):
        if dataset not in config.datasets:
            raise ReaderError(f"unknown dataset '{dataset}'")
        dataset = config.datasets[dataset]
    if context is None:
        raise ReaderError("a resolved PathContext is required to read a dataset")
    files = _files_for(dataset, context)
    if not files:
        raise ReaderError(f"no matching source files for dataset '{dataset.name}' under '{context.source_prefix}'")
    reader_settings = config.readers.get(dataset.source.get("format", "csv"), {})
    options = dict(reader_settings.get("options", {}))
    return read_files(files, format_name=dataset.source.get("format", "csv"), options=options,
                      run_id=run_id, null_tokens=tuple(config.settings.get("null_tokens", ())),
                      expected_columns=tuple(dataset.columns))


extract = read_dataset
