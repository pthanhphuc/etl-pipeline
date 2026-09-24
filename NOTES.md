# Implementation notes

## Decisions

- Output files are written directly to the configured assignment partitions:
  `transform_data/{dataset}/{yyyy}/{mm}/{dd}/trusted.parquet` and
  `transform_data/{dataset}/{yyyy}/{mm}/{dd}/summary.json`. Reruns overwrite the
  same partition for that dataset/date; different datasets and dates do not
  touch each other's files.
- There is no separate `warning_data` output. Flagged rows stay in
  `transform_data` with a populated `warning_reason` JSON column. The load task
  filters those rows out before writing PostgreSQL, so a database outage can be
  handled by rerunning the load command without touching `raw_data`.
- `full_name_last` is not required. Single-token names are allowed, and the
  table keeps `full_name_last` and `full_name_first` while deliberately
  discarding `full_name_middle`.
- `car_brand` uses `replace_all`. If a reference row is flagged, it remains in
  transform output with `warning_reason`; the replace step loads only rows where
  `warning_reason` is empty.
- Conversion output names are derived from function suffixes, for example
  `phone` plus `country_code` becomes `phone_country_code`.

## Actual run counts

These counts are from the deterministic sample generated with seed `20260915`
and loaded into PostgreSQL database `etl_dealership`.

| Dataset | Date | Extracted | Trusted | Flagged | Flag ratio | Load result |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| car_brand | 2026-09-15 | 13 | 12 | 1 | 0.077 | 12 inserts |
| car_brand | 2026-09-16 | 11 | 11 | 0 | 0.000 | replace_all, 11 inserts |
| customer | 2026-09-15 | 41 | 36 | 5 | 0.122 | 36 inserts |
| customer | 2026-09-16 | 25 | 25 | 0 | 0.000 | 10 inserts, 15 updates |
| car | 2026-09-15 | 35 | 30 | 5 | 0.143 | 30 inserts |
| order | 2026-09-15 | 41 | 35 | 6 | 0.146 | 35 inserts |
| order | 2026-09-16 | 30 | 30 | 0 | 0.000 | 20 inserts, 10 skipped |

Rerunning `order` for 2026-09-16 after it was already loaded produced
`0 inserts, 0 updates, 30 unchanged`.

## Warning breakdown

| Dataset | Date | Checks |
| --- | --- | --- |
| car_brand | 2026-09-15 | not_null: 1, primary_key: 1 |
| customer | 2026-09-15 | constraint: 6, conversion: 2, max_length: 1, not_null: 2 |
| car | 2026-09-15 | constraint: 6, max_length: 1, type: 1 |
| order | 2026-09-15 | constraint: 5, conversion: 1, type: 2 |

The check count can be larger than the flagged row count because one row may
carry several reasons. All flagged rows retain `_source_file`, `_source_line`,
`_run_id`, and a structured JSON reason in `warning_reason`.

## Concurrency

Runs are independent by dataset/date partition and target table. A customer run
does not read car output, and an order run does not validate foreign keys
against customer or car because the assignment declares cross-dataset checks out
of scope. Concurrent reruns of the exact same dataset/date would overwrite the
same Parquet handoff, so in production I would add an external run lock or a
versioned publication step.

## What I would improve with another week

- Add an Airflow integration test in an environment with Airflow installed.
- Add a small review UI or notebook for editing warning rows before promotion.
- Add large-file chunking. The current design intentionally keeps one source
  partition in one pandas DataFrame, which is simple but will break down when a
  day's data no longer fits comfortably in memory.
- Add richer locale-specific name handling. The current name parser covers the
  required examples but is still rule-based.
