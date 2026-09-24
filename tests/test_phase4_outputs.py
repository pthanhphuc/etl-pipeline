import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from etl.config import load_config
from etl.outputs import FlagThresholdExceeded, promote_warning_rows, write_outputs
from etl.paths import build_paths
from etl.standardize import standardize
from etl.validation import REASONS_COLUMN


def _context():
    config = load_config("config")
    return config, config.datasets["customer"], build_paths("customer", date(2026, 9, 15), config)


def test_writes_typed_split_summary_and_preserves_metadata(tmp_path: Path) -> None:
    config, dataset, context = _context()
    frame = pd.DataFrame(
        [
            {"customer_id": 1, "full_name": "A", "email": "a@example.com", "registered_at": "2024-01-01", "is_active": True,
             "_source_file": "a.csv", "_source_line": 2, "_run_id": "run-1", REASONS_COLUMN: []},
            {"customer_id": 2, "full_name": "B", "email": "bad", "registered_at": "2024-01-01", "is_active": True,
             "_source_file": "b.csv", "_source_line": 2, "_run_id": "run-1",
             REASONS_COLUMN: [{"stage": "standardization", "check": "conversion", "function": "conv_email", "columns": ["email"], "reason": "invalid"}]},
        ]
    )
    result = write_outputs(frame, dataset, context=context, config=config, output_root=tmp_path)
    assert (result.extracted, result.trusted, result.flagged) == (2, 1, 1)
    trusted = pd.read_parquet(result.trusted_path)
    warning = pd.read_parquet(result.warning_path)
    assert len(trusted) + len(warning) == 2
    assert str(trusted["customer_id"].dtype) in {"int64", "Int64"}
    assert warning.loc[0, "_run_id"] == "run-1"
    assert json.loads(warning.loc[0, REASONS_COLUMN])[0]["function"] == "conv_email"
    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert summary["checks"] == {"conversion": 1}
    assert summary["functions"] == {"conv_email": 1}


def test_threshold_writes_diagnostics_but_blocks_load(tmp_path: Path) -> None:
    config, dataset, context = _context()
    frame = pd.DataFrame({"x": [1], REASONS_COLUMN: [[{"check": "type"}]]})
    with pytest.raises(FlagThresholdExceeded) as error:
        write_outputs(frame, dataset, context=context, config=config, output_root=tmp_path)
    assert error.value.result.can_load is False
    assert error.value.result.warning_path.exists()
    assert error.value.result.summary_path.exists()


def test_warning_row_can_be_promoted_without_raw_read(tmp_path: Path) -> None:
    config, dataset, context = _context()
    trusted_path = tmp_path / "trusted.parquet"
    warning_path = tmp_path / "warning.parquet"
    base = {"customer_id": 7, "full_name": "A", "email": "a@example.com", "registered_at": "2024-01-01", "is_active": True,
            "surrogate_key": "key-7", "_source_file": "a.csv", "_source_line": 2, "_run_id": "run-1"}
    warning = pd.DataFrame([{**base, REASONS_COLUMN: json.dumps([{"check": "type"}])}])
    warning.to_parquet(warning_path)
    corrected = pd.DataFrame([{**base, REASONS_COLUMN: []}])
    assert promote_warning_rows(corrected, trusted_path=trusted_path, warning_path=warning_path, dataset=dataset, config=config) == 1
    assert pd.read_parquet(trusted_path).iloc[0]["customer_id"] == 7
    assert pd.read_parquet(warning_path).empty

