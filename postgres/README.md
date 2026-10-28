# BELS on PostgreSQL

The web app and API can run with the gazetteer in PostgreSQL instead of BigQuery.
BigQuery stays the engine that builds the gazetteer; a snapshot of its runtime tables
is exported and loaded here. The match strings are built exactly as BigQuery built the
stored strings, so the API answers as the BigQuery version does. The App Engine
deployment is unchanged: `BELS_BACKEND` defaults to `bigquery`.

## Requirements

- PostgreSQL 14 or later with PostGIS (the DDL types `center` as geography).
- Python 3.11 with the pinned packages:
  `pip install --prefer-binary -r requirements-vt.txt`
- `bels/` on `PYTHONPATH`, and the app started from the repository root (the
  vocabulary files are read by relative path).

## Load a snapshot

A snapshot is a directory with the BigQuery Parquet exports of the three
`matchme_*_best_georef` tables and the `countrycode_lookup` CSV, either as single files
(as in `bels_dev_snapshot/`) or as the numbered files of a BigQuery export.

    PSQL=psql python postgres/load_snapshot.py <snapshot dir> \
        --ddl postgres/bels_runtime_ddl_postgresql.sql --jobs 8 --swap

The loader creates the tables in a staging schema (`gazetteer_next`), copies each file
in its own transaction, builds the indexes, then compares every loaded value with the
Parquet files. With `--swap`, and only if verification found no problems, the staging
schema becomes `gazetteer` and the previous one is kept as `gazetteer_prev`. If a load
is interrupted, run the same command with `--resume` to skip the files already done.
Connection settings come from `--dsn` or the usual `PG*` environment variables.

A `--swap` reload holds two copies of the data. The full snapshot of 2026-10 (243
million rows) takes about 130 GB in the database.

## Run the app

    BELS_BACKEND=postgres PYTHONPATH=bels gunicorn main:app

| Setting | Meaning |
| --- | --- |
| `BELS_BACKEND` | `postgres` to use PostgreSQL (default `bigquery`) |
| `BELS_PG_DSN` | connection string (default: the `PG*` environment variables) |
| `BELS_PG_POOL_SIZE` | connections per app process (default 4) |
| `BELS_BULK_DIR` | directory for bulk uploads, jobs and results; without it `/api/bels_csv` answers 503 |
| `BELS_BULK_URL` | base URL of `bels_output/` for the download link in the email (default: the file path) |
| `BELS_EMAIL` | `log` (default) writes messages to `BELS_BULK_DIR/mail/`; `sendgrid` sends them |
| `BELS_EMAIL_ALLOWLIST` | with `sendgrid`, comma-separated addresses to send to, or `*` for anyone |
| `SENDGRID_API_KEY` | SendGrid key, as for App Engine |

## Run the bulk worker

Uploads to `/api/bels_csv` are queued in `BELS_BULK_DIR`. A worker process, started
from the repository root with the same settings, runs them one at a time:

    BELS_BACKEND=postgres BELS_BULK_DIR=... PYTHONPATH=bels python bels/pg_job.py

`--once` runs the queued jobs and exits. The output has the same columns and format as
the BigQuery version, with the rows in input order.

## Tests

From `test/`, with `PYTHONPATH=../bels`:

- `parity_tests.py`: the match strings built in Python equal the stored ones in the
  snapshot. `parity_full.py` runs the same check over a full export, in parallel.
- `pg_query_tests.py`: lookups and API responses from the database (skipped without a
  database).
- `bulk_match_tests.py` and `pg_job_tests.py`: bulk match strings and the bulk path.
- `load_snapshot_tests.py`: the loader, in a test schema (needs `PSQL`).
