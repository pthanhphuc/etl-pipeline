import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from etl.core.config import load_config
from etl.transform.outputs import FlagThresholdExceeded, WARNING_REASON_COLUMN, promote_warning_rows, write_outputs
from etl.core.paths import build_paths
from etl.transform.standardize import standardize
from etl.transform.validation import REASONS_COLUMN


def _context():
    config = load_config("config")
    dataset = config.datasets["customer"]
    dataset = replace(dataset, load={**dataset.load, "flag_threshold": 0.5})
    return config, dataset, build_paths("customer", date(2026, 9, 15), config)


def test_writes_typed_output_summary_and_preserves_warning_reason(tmp_path: Path) -> None:
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
    output = pd.read_parquet(result.trusted_path)
    assert len(output) == 2
    assert str(output["customer_id"].dtype) in {"int64", "Int64"}
    assert output.loc[1, "_run_id"] == "run-1"
    assert json.loads(output.loc[1, WARNING_REASON_COLUMN])[0]["function"] == "conv_email"
    assert pd.isna(output.loc[0, WARNING_REASON_COLUMN])
    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert summary["checks"] == {"conversion": 1}
    assert summary["functions"] == {"conv_email": 1}


def test_threshold_writes_diagnostics_but_blocks_load(tmp_path: Path) -> None:
    config, dataset, context = _context()
    frame = pd.DataFrame({"x": [1], REASONS_COLUMN: [[{"check": "type"}]]})
    with pytest.raises(FlagThresholdExceeded) as error:
        write_outputs(frame, dataset, context=context, config=config, output_root=tmp_path)
    assert error.value.result.can_load is False
    assert error.value.result.trusted_path.exists()
    assert error.value.result.summary_path.exists()


def test_warning_row_can_be_promoted_without_raw_read(tmp_path: Path) -> None:
    config, dataset, context = _context()
    trusted_path = tmp_path / "trusted.parquet"
    base = {"customer_id": 7, "full_name": "A", "email": "a@example.com", "registered_at": "2024-01-01", "is_active": True,
            "surrogate_key": "key-7", "_source_file": "a.csv", "_source_line": 2, "_run_id": "run-1"}
    warning = pd.DataFrame([{**base, WARNING_REASON_COLUMN: json.dumps([{"check": "type"}])}])
    warning.to_parquet(trusted_path)
    corrected = pd.DataFrame([{**base, REASONS_COLUMN: []}])
    assert promote_warning_rows(corrected, trusted_path=trusted_path, dataset=dataset, config=config) == 1
    promoted = pd.read_parquet(trusted_path)
    assert promoted.iloc[-1]["customer_id"] == 7
    assert pd.isna(promoted.iloc[-1][WARNING_REASON_COLUMN])
