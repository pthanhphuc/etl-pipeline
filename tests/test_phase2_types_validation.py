from pathlib import Path

import pandas as pd

from etl.config import load_config
from etl.types import coerce_value, normalize_nulls
from etl.validation import validate_raw


def test_shared_coercion_and_null_normalization() -> None:
    assert coerce_value("12", "integer") == 12
    assert coerce_value("12.50", "decimal") == 12.50
    assert coerce_value("Y", "boolean") is True
    assert str(coerce_value("15/03/2024", "date")) == "2024-03-15"
    assert pd.isna(normalize_nulls(pd.DataFrame({"x": ["", "NULL", "ok"]}), ("", "NULL"))["x"].iloc[0])


def test_raw_validation_accumulates_type_width_required_and_duplicate_reasons() -> None:
    config = load_config("config")
    dataset = config.datasets["customer"]
    frame = pd.DataFrame([
        {"customer_id": "abc", "full_name": "", "email": "x" * 201, "registered_at": "bad", "is_active": "maybe"},
        {"customer_id": "1", "full_name": "Valid", "email": "a@b.com", "registered_at": "2024-01-01", "is_active": "Y"},
        {"customer_id": "1", "full_name": "Valid 2", "email": "c@d.com", "registered_at": "2024-01-01", "is_active": "N"},
    ])
    checked = validate_raw(frame, dataset, null_tokens=("", "NULL"))
    reasons = checked.frame["_validation_reasons"].tolist()
    assert {reason["check"] for reason in reasons[0]} >= {"type", "max_length", "not_null"}
    assert any(reason["check"] == "primary_key" for reason in reasons[2])
    assert checked.flagged.tolist() == [True, False, True]

