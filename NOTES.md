# Implementation notes

## Phase 8 status

- Added unit and contract coverage for configuration, paths, readers, scalar
  coercion, validation, conversions, standardization, outputs, pure load
  decisions, CLI parsing, pipeline orchestration, and the Airflow DAG.
- PostgreSQL checks are marked `integration` and require
  `ETL_DATABASE_URL`; they skip with a clear setup message otherwise.
- No PostgreSQL server or Airflow installation was available during this
  handoff.

## Source inventory

These are physical CSV data rows, excluding headers, from the checked-in
`raw_data` files. They are source counts, not trusted/flagged/loaded counts.

| Dataset | 2026-09-15 | 2026-09-16 |
| --- | ---: | ---: |
| car | 20 | — |
| car_brand | 13 | 11 |
| customer | 30 | 25 |
| order | 36 | 30 |
| **Total** | **99** | **66** |

## Decision placeholders

- [ ] Record extracted, trusted, flagged, and loaded counts after PostgreSQL runs.
- [ ] Record warning counts by check/function from each `summary.json`.
- [ ] Confirm day-2 differential-update, insert-only, and replace-all outcomes.
- [ ] Record PostgreSQL and Airflow versions and exact commands used.

## Current decisions

- Transform runs use isolated `_runs/<run_id>` output partitions.
- Trusted and warning Parquet are the durable transform/load handoff; warning
  rows never enter PostgreSQL.
- `decide_load` is pure and testable without PostgreSQL; database writes are
  transactional in the loader.
- Airflow accepts only `source_prefix` in `dag_run.conf`; table and dataset
  names remain metadata-driven.
