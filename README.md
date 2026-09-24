# Metadata-driven ETL

This project reads configured CSV datasets, validates and standardizes rows,
writes one typed Parquet handoff per dataset/date, and loads only rows without
warnings into PostgreSQL. Dataset names, columns, conversions, paths,
thresholds, and load modes come from `config/`.

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
$env:ETL_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/etl_dealership"
```

The Compose service exposes database `etl_dealership`, user `postgres`, and password `postgres` on
`localhost:5432`. `DATABASE_URL` is accepted as a fallback.

## Run with Docker

Build the ETL image and start PostgreSQL:

```powershell
docker compose build etl
docker compose up -d postgres
```

Run ETL commands inside the container:

```powershell
docker compose run --rm etl python -m etl init-db
docker compose run --rm etl python scripts/generate_sample_data.py
docker compose run --rm etl python -m etl transform --source-prefix raw_data/customer/2026/09/15
docker compose run --rm etl python -m etl load --source-prefix raw_data/customer/2026/09/15
```

The `etl` service mounts `config/`, `raw_data/`, and `transform_data/`, and
connects to PostgreSQL through the internal Compose hostname `postgres`.

## Check records in PostgreSQL

Open `psql` inside the PostgreSQL container:

```powershell
docker compose exec postgres psql -U postgres -d etl_dealership
```

Useful queries:

```sql
SELECT count(*) FROM car_brand;
SELECT count(*) FROM customer;
SELECT count(*) FROM car;
SELECT count(*) FROM "order";

SELECT * FROM customer LIMIT 10;
SELECT * FROM "order" LIMIT 10;
```

`order` is a SQL keyword, so quote it as `"order"`. Exit `psql` with `\q`.

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

Run one partition:

```powershell
python -m etl transform --source-prefix raw_data/customer/2026/09/15
python -m etl load --source-prefix raw_data/customer/2026/09/15
python -m etl run --source-prefix raw_data/customer/2026/09/15
python -m etl report --source-prefix raw_data/customer/2026/09/15
```

Meaning of each command:

- `transform`: reads raw input from `raw_data/...`, validates and standardizes
  it, then writes the transformed Parquet output under `transform_data/...`.
- `load`: reads the transformed Parquet output from `transform_data/...` and
  inserts only clean rows into PostgreSQL.
- `run`: shortcut for `transform` followed by `load` for the same
  `--source-prefix`. Use this when you want to run the full ETL flow in one
  command.
- `report`: prints the latest transform summary for that partition.

Each transform writes the assignment handoff files directly under
`transform_data/{dataset}/{yyyy}/{mm}/{dd}`. Rows with validation or conversion
issues stay in the Parquet file with a populated `warning_reason` column. A load
reads the transform Parquet, skips rows with `warning_reason`, and does not
reread raw files. Corrected warning rows can be promoted with:

```powershell
python -m etl promote-warning `
  --source-prefix raw_data/customer/2026/09/15 `
  --corrected path\to\corrected.parquet
```

## Airflow

`dags/etl_dag.py` defines one parameterized DAG with separate transform
and load tasks. It expects exactly one run configuration field:

```json
{"source_prefix":"raw_data/customer/2026/09/15"}
```

After making this repository importable in Airflow and configuring
`ETL_DATABASE_URL`, trigger it with:

```powershell
airflow dags trigger etl_dag `
  --conf '{"source_prefix":"raw_data/customer/2026/09/15"}'
```

Runs for different datasets/dates use independent partitions and tables. Each
load is transactional.

## Tests

```powershell
python -m compileall -q src dags scripts tests
pytest -q
```

Airflow contract tests skip when Airflow is not installed. PostgreSQL tests
are opt-in and skip with a clear setup message otherwise:

```powershell
$env:ETL_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/etl_dealership"
pytest -q -m integration
```
