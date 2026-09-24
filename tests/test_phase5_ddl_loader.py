from etl.config import load_config
from etl.ddl import generate_create_table
from etl.loader import decide_load


def test_generated_ddl_quotes_reserved_order_and_metadata() -> None:
    dataset = load_config("config").datasets["order"]
    ddl = generate_create_table(dataset, schema="etl")
    assert 'CREATE TABLE IF NOT EXISTS "etl"."order"' in ddl
    assert '"order_id" VARCHAR(20)' in ddl
    assert 'PRIMARY KEY ("order_id")' in ddl
    assert '"created_at" TIMESTAMPTZ' in ddl


def test_decision_modes_and_duplicate_composite_keys() -> None:
    incoming = [{"a": 1, "b": "x", "row_hash": "same"}, {"a": 1, "b": "x", "row_hash": "dup"}, {"a": 2, "b": "new", "row_hash": "n"}]
    target = [{"a": 1, "b": "old", "row_hash": "same"}]
    result = decide_load(incoming, target, mode="differential_update", key_columns=["a"], compare_columns=["b"])
    assert [row["a"] for row in result.updates] == [1]
    assert [row["a"] for row in result.inserts] == [2]
    assert [row["a"] for row in result.duplicates] == [1]

    composite = decide_load([{"a": 1, "b": 2}, {"a": 1, "b": 3}], [], mode="insert_only", key_columns=["a", "b"])
    assert len(composite.inserts) == 2


def test_decision_empty_and_replace_all() -> None:
    assert decide_load([], [{"id": 1}], mode="insert_only", key_columns=["id"]).inserts == ()
    result = decide_load([{"id": 2}], [{"id": 1}], mode="replace_all", key_columns=["id"])
    assert len(result.inserts) == 1
    assert result.updates == ()
    assert result.unchanged == ()

