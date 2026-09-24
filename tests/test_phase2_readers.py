from pathlib import Path

import pandas as pd
import pytest

from etl.readers import ReaderError, read_files


def test_csv_is_all_text_and_has_source_metadata(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "input.csv"
    path.parent.mkdir()
    path.write_text("id,amount\n001,12\n002,\n", encoding="utf-8")

    result = read_files([path], run_id="run-1", null_tokens=("",))

    assert result.frame["id"].tolist() == ["001", "002"]
    assert result.frame["amount"].iloc[0] == "12"
    assert pd.isna(result.frame["amount"].iloc[1])
    assert result.frame[["_source_file", "_source_line", "_run_id"]].to_dict("records") == [
        {"_source_file": "input.csv", "_source_line": 2, "_run_id": "run-1"},
        {"_source_file": "input.csv", "_source_line": 3, "_run_id": "run-1"},
    ]


def test_no_files_and_unreadable_file_are_clear(tmp_path: Path) -> None:
    with pytest.raises(ReaderError, match="no matching"):
        read_files([])
    missing = tmp_path / "missing.csv"
    with pytest.raises(ReaderError, match="cannot read source file"):
        read_files([missing])


def test_missing_declared_column_is_reported(tmp_path: Path) -> None:
    path = tmp_path / "input.csv"
    path.write_text("id\n1\n", encoding="utf-8")

    with pytest.raises(ReaderError, match="missing declared columns"):
        read_files([path], expected_columns=("id", "name"))
