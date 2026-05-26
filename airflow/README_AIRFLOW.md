# Airflow Orchestration Layer — pipeline-sentinel

This directory is a complete Astronomer Airflow project that *consumes* the `pipeline-sentinel` library. It serves two purposes simultaneously:

1. **Showcase** — `ratings_etl_with_sentinel` is the canonical example of `@observe` integrated into a scheduled pipeline.
2. **Regression suite** — `sentinel_regression_suite` runs every check type against fixture DataFrames each night, failing the DAG on unexpected results. The library tests itself in production.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                  Astronomer Airflow (port 8080)                  │
│                                                                  │
│  ┌────────────────────────┐  ┌────────────────────────────────┐  │
│  │ ratings_etl_with_      │  │ sentinel_regression_suite      │  │
│  │ sentinel  (3am daily)  │  │ (midnight daily)               │  │
│  │                        │  │                                │  │
│  │ validate_files         │  │ generate_fixtures              │  │
│  │   → ingest             │  │   ├── test_row_count           │  │
│  │   → clean              │  │   ├── test_null_rate           │  │
│  │   → aggregate          │  │   ├── test_schema              │  │
│  │   → log_summary        │  │   ├── test_freshness           │  │
│  │   → notify             │  │   ├── test_distribution        │  │
│  │                        │  │   ├── test_uniqueness          │  │
│  │ @observe sinks:        │  │   ├── test_range               │  │
│  │   LogSink              │  │   └── test_anomaly             │  │
│  │   SlackSink (failure)  │  │       → evaluate_results       │  │
│  │   DuckDBMetricsSink    │  │                                │  │
│  └───────────┬────────────┘  └────────────────────────────────┘  │
│              │                                                   │
│              ▼                                                   │
│      ┌───────────────┐         ┌────────────────────────────┐    │
│      │ DuckDB:       │ ◀────── │ sentinel_weekly_digest     │    │
│      │ sentinel_     │         │ (Mon 9am)                  │    │
│      │ reports       │         │                            │    │
│      └───────────────┘         │ load → trends → Slack      │    │
│                                └────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

## Components

| Path | Purpose |
|---|---|
| `Dockerfile` | Astro Runtime 3.1-5 (Airflow 3.1.x) + sentinel library installed editable |
| `requirements.txt` | Airflow providers: slack, duckdb, pandas, pyarrow |
| `docker-compose.yml` | Postgres + scheduler + webserver + triggerer + airflow-init |
| `dags/ratings_etl_pipeline.py` | Showcase ETL DAG (3am) |
| `dags/sentinel_regression_suite.py` | Library self-test DAG (midnight) |
| `dags/sentinel_report_digest.py` | Weekly digest DAG (Mon 9am) |
| `plugins/sentinel_airflow_hook.py` | `SentinelAirflowHook` + `DuckDBMetricsSink` |
| `include/etl_transforms.py` | `@observe`-decorated ETL functions |
| `include/fixture_datasets.py` | Fixture builders for the regression suite |
| `include/connections.md` | Required Airflow connections |
| `tests/test_dags.py` | DagBag integrity + schedule + structure |
| `tests/test_sentinel_airflow_hook.py` | DuckDB sink unit tests |
| `data/` | Local volume mount: place `ratings_sample.csv` here |

## Local quickstart

```bash
# Drop a MovieLens-style ratings.csv at airflow/data/ratings_sample.csv
# Columns expected: userId, movieId, rating, timestamp (epoch seconds)

cd airflow
docker compose up -d
# Airflow UI: http://localhost:8080 (admin/admin)

# Trigger DAGs from the UI, or:
docker compose exec scheduler airflow dags trigger ratings_etl_with_sentinel
docker compose exec scheduler airflow dags trigger sentinel_regression_suite

# Tear down
docker compose down -v
```

## Design notes

- **No code duplication.** DAGs import `sentinel.checks` directly. There is no "copy of the check logic" — the library *is* the implementation.
- **Sinks resolved at task time.** The `@observe` decorator's sinks are baked in at function definition, but DAG tasks call `_attach_sinks(fn)` to swap them for the production set from `SentinelAirflowHook` before invoking.
- **DuckDB as audit log.** Every check result accumulates as a row in `sentinel_reports.duckdb`. The weekly digest queries 7 days of that table; the report-summary task queries 1 day. New DAGs can query the same table.
- **`only_on_failure` Slack noise control.** `SlackSink` is configured to fire only on FAIL/ERROR — the digest DAG handles routine pass-rate reporting.
- **Regression DAG fails the test on unexpected check status.** That's the point — when the library regresses, the DAG goes red overnight before any downstream consumer notices the broken check.
