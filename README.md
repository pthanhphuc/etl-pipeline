# Metadata-driven ETL

This project reads configured CSV datasets, validates and standardizes rows,
writes trusted and warning Parquet handoffs, and loads trusted rows into
PostgreSQL. Dataset names, columns, conversions, paths, thresholds, and load
modes come from `config/`.

## Prerequisites

- Python 3.12
- Docker Desktop with Docker Compose
- PostgreSQL 14+ (the included Compose service is sufficient locally)
- Airflow only when running the DAG, installed with the official constraints

## Install and start PostgreSQL

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
docker compose up -d postgres
$env:ETL_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/metadata_etl"
```

The Compose service exposes database `metadata_etl`, user `postgres`, and password `postgres` on
`localhost:5432`. `DATABASE_URL` is accepted as a fallback.

## Generate sample data

The checked-in files are ready for the assignment. To generate a fresh,
deterministic sample set:

```powershell
python scripts\generate_sample_data.py
```

The generator writes under `raw_data/`.

## CLI

Initialize all configured PostgreSQL tables:

```powershell
python -m etl init-db
```

Run one partition, load its trusted handoff, or do both:

```powershell
python -m etl transform --source-prefix raw_data/customer/2026/09/15
python -m etl load --source-prefix raw_data/customer/2026/09/15 --run-id <run-id>
python -m etl run --source-prefix raw_data/customer/2026/09/15
python -m etl report --source-prefix raw_data/customer/2026/09/15
```

Use `--run-id` to isolate a rerun. A load reads only trusted Parquet and does
not reread raw files. Corrected warning rows can be promoted with:

```powershell
python -m etl promote-warning `
  --source-prefix raw_data/customer/2026/09/15 `
  --corrected path\to\corrected.parquet
```

## Airflow

`dags/metadata_etl.py` defines one parameterized DAG with separate transform
and load tasks. It expects exactly one run configuration field:

```json
{"source_prefix":"raw_data/customer/2026/09/15"}
```

After making this repository importable in Airflow and configuring
`ETL_DATABASE_URL`, trigger it with:

```powershell
airflow dags trigger metadata_etl `
  --conf '{"source_prefix":"raw_data/customer/2026/09/15"}'
```

Concurrent runs are isolated by run ID and each load is transactional.

## Tests

```powershell
python -m compileall -q src dags scripts tests
pytest -q
```

Airflow contract tests skip when Airflow is not installed. PostgreSQL tests
are opt-in and skip with a clear setup message otherwise:

```powershell
$env:ETL_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/metadata_etl"
pytest -q -m integration
```

The environment used for this handoff did not have pytest, pandas, or pyarrow
installed, so compilation was run but the test suite could not be executed.
Install `requirements.txt` first.
