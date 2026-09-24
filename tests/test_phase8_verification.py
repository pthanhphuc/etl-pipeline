"""Phase 8 verification tests.

Database and Airflow tests are deliberately opt-in so the unit suite remains
useful on a workstation that only has the core ETL dependencies installed.
"""

from __future__ import annotations

from datetime import date
import importlib
import json
from pathlib import Path

import pandas as pd
import pytest

from etl.core.config import ConfigurationError, load_config
from etl.transform.conversions import ConversionError, apply_conversion
from etl.load.loader import decide_load
from etl.core.paths import PathResolutionError, build_paths, parse_source_prefix
from etl.core.pipeline import TransformResult, resolve_context
from etl.extract.readers import ReaderError, read_dataset, read_files
from etl.transform.standardize import standardize
from etl.core.types import coerce_value
from etl.transform.validation import REASONS_COLUMN, validate_raw


CONFIG_DIR = Path(__file__).parents[1] / "config"


def test_config_loads_all_datasets_and_rejects_malformed_config(tmp_path: Path) -> None:
    config = load_config(CONFIG_DIR)
    assert set(config.datasets) == {"car", "car_brand", "customer", "order"}
    assert config.datasets["order"].target_table == "order"

    bad = tmp_path / "config"
    (bad / "datasets").mkdir(parents=True)
    (bad / "settings.yaml").write_text("paths: {}\n", encoding="utf-8")
    (bad / "functions.yaml").write_text("functions: {}\n", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="missing key"):
        load_config(bad)


def test_paths_round_trip_and_reject_unknown_or_impossible_dates() -> None:
    config = load_config(CONFIG_DIR)
    context = build_paths("customer", date(2026, 9, 15), config)
    assert parse_source_prefix(context.source_prefix, config) == context
    assert context.raw_files.as_posix().endswith("customer/2026/09/15/**/*.csv")
    with pytest.raises(PathResolutionError):
        parse_source_prefix("raw_data/customer/2026/02/31", config)
    with pytest.raises(PathResolutionError):
        parse_source_prefix("raw_data/missing/2026/09/15", config)


def test_reader_selects_nested_files_deterministically_and_keeps_line_metadata(tmp_path: Path) -> None:
    first = tmp_path / "b" / "b.csv"
    second = tmp_path / "a" / "a.csv"
    first.parent.mkdir()
    second.parent.mkdir()
    first.write_text("id\n2\n", encoding="utf-8")
    second.write_text("id\n1\n", encoding="utf-8")
    result = read_files([first, second], run_id="r1", expected_columns=("id",))
    assert result.frame["id"].tolist() == ["2", "1"]
    assert result.frame["_source_line"].tolist() == [2, 2]
    assert result.files == (first, second)

    with pytest.raises(ReaderError, match="unsupported source format"):
        read_files([first], format_name="xml")


@pytest.mark.parametrize(
    ("value", "logical_type", "expected"),
    [("12", "integer", 12), ("12.50", "decimal", "12.50"), ("Y", "boolean", True),
     ("2024-03-15", "date", date(2024, 3, 15))],
)
def test_coercion_and_null_behavior(value: str, logical_type: str, expected: object) -> None:
    actual = coerce_value(value, logical_type)
    assert str(actual) == str(expected)
    with pytest.raises(ValueError):
        coerce_value("not-a-value", logical_type)


def test_validation_accumulates_reasons_and_preserves_metadata() -> None:
    dataset = load_config(CONFIG_DIR).datasets["customer"]
    frame = pd.DataFrame([{
        "customer_id": "bad", "full_name": "", "email": "x" * 201,
        "registered_at": "bad", "is_active": "maybe", "_source_file": "x.csv",
        "_source_line": 2, "_run_id": "r1",
    }])
    result = validate_raw(frame, dataset, null_tokens=("", "NULL"))
    reasons = result.frame.iloc[0][REASONS_COLUMN]
    assert {item["check"] for item in reasons} >= {"type", "max_length", "not_null"}
    assert result.frame.loc[0, "_source_file"] == "x.csv"


def test_conversion_registry_outputs_and_errors() -> None:
    config = load_config(CONFIG_DIR)
    email = apply_conversion(" Alice+tag@Example.COM ", "conv_email", options={"strip_plus_tag": True},
                             registry=config.functions, reference_data=config.reference_data)
    assert email == {"email": "alice@example.com", "email_domain": "example.com"}
    phone = apply_conversion("0120 123 4567", "conv_tel", options={"dial_plan": "vietnam"},
                             registry=config.functions, reference_data=config.reference_data)
    assert phone["phone"] == "+84701234567"
    with pytest.raises(ConversionError):
        apply_conversion("not-an-email", "conv_email", registry=config.functions)


def test_standardization_projects_outputs_and_flags_bad_conversion() -> None:
    config = load_config(CONFIG_DIR)
    dataset = config.datasets["customer"]
    raw = pd.DataFrame([{
        "customer_id": "1", "full_name": "Mr John Doe", "email": "bad",
        "phone": "0120 123 4567", "loyalty_tier": "gold", "registered_at": "2024-01-01 10:00:00",
        "is_active": "Y", "_source_file": "x.csv", "_source_line": 2, "_run_id": "r1",
        REASONS_COLUMN: [],
    }])
    result = standardize(raw, dataset, config=config)
    assert {"full_name_last", "full_name_first", "email_domain", "surrogate_key", "row_hash"} <= set(result)
    assert result.loc[0, REASONS_COLUMN]
    assert result.loc[0, "email"] is pd.NA or pd.isna(result.loc[0, "email"])


def test_pure_load_decisions_cover_modes_and_duplicates() -> None:
    incoming = [{"id": 1, "value": "new"}, {"id": 1, "value": "duplicate"}, {"id": 2, "value": "x"}]
    target = [{"id": 1, "value": "old"}]
    result = decide_load(incoming, target, mode="differential_update", key_columns=["id"], compare_columns=["value"])
    assert [row["id"] for row in result.updates] == [1]
    assert [row["id"] for row in result.inserts] == [2]
    assert [row["id"] for row in result.duplicates] == [1]
    assert len(decide_load(incoming, target, mode="replace_all", key_columns=["id"]).inserts) == 2


def test_pipeline_context_and_transform_orchestration(monkeypatch: pytest.MonkeyPatch) -> None:
    import etl.core.pipeline as pipeline

    config = load_config(CONFIG_DIR)
    context = resolve_context("raw_data/customer/2026/09/15", config=config)
    assert context.dataset == "customer"
    sentinel = object()
    monkeypatch.setattr(pipeline, "read_dataset", lambda *args, **kwargs: sentinel)
    monkeypatch.setattr(pipeline, "validate_raw", lambda frame, dataset, **kwargs: type("V", (), {"frame": pd.DataFrame()})())
    monkeypatch.setattr(pipeline, "standardize", lambda frame, dataset, **kwargs: pd.DataFrame())
    monkeypatch.setattr(pipeline, "write_outputs", lambda *args, **kwargs: "output")
    result = pipeline.transform_source("raw_data/customer/2026/09/15", config=config, run_id="run-1")
    assert isinstance(result, TransformResult)
    assert result.run_id == "run-1"
    assert result.output == "output"


def test_cli_commands_are_parseable() -> None:
    from etl.cli import build_parser
    parser = build_parser()
    for command in ("init-db", "transform", "load", "run", "report"):
        args = [command] if command == "init-db" else [command, "--source-prefix", "raw_data/customer/2026/09/15"]
        assert build_parser().parse_args(args).command == command


def test_airflow_dag_contract_when_airflow_is_installed() -> None:
    pytest.importorskip("airflow")
    dag_module = importlib.import_module("dags.etl_dag")
    dag = dag_module.dag
    assert {task.task_id for task in dag.tasks} == {"transform_data", "load_data_into_db"}
    assert dag.max_active_runs == 16


@pytest.mark.integration
def test_postgres_integration_is_opt_in() -> None:
    """Run the live checks with: ETL_DATABASE_URL=... pytest -m integration."""
    url = pytest.importorskip("os").environ.get("ETL_DATABASE_URL")
    if not url:
        pytest.skip("set ETL_DATABASE_URL to run PostgreSQL integration tests")
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(url) as connection:
        assert connection.execute("SELECT 1").fetchone() == (1,)
