.PHONY: install test lint format build example clean airflow-up airflow-down trigger-etl trigger-regression trigger-digest airflow-test

PY := python3
PIP := pip3
VENV := .venv

install:
	$(PY) -m venv $(VENV)
	$(VENV)/bin/$(PIP) install -e ".[dev,spark,airflow]"

test:
	$(VENV)/bin/pytest tests/ -v --cov=sentinel --cov-report=term-missing
	PYTHONPATH=airflow/plugins:airflow/include $(VENV)/bin/pytest airflow/tests/test_sentinel_airflow_hook.py -v

lint:
	$(VENV)/bin/black --check sentinel/ tests/
	$(VENV)/bin/mypy sentinel/

format:
	$(VENV)/bin/black sentinel/ tests/

build:
	$(VENV)/bin/$(PY) -m build

example:
	$(VENV)/bin/$(PY) examples/pandas_example.py

clean:
	rm -rf dist/ build/ *.egg-info .coverage .pytest_cache .mypy_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# Airflow targets (docker compose lives in airflow/)
airflow-up:
	cd airflow && docker compose up -d

airflow-down:
	cd airflow && docker compose down -v

trigger-etl:
	cd airflow && docker compose exec scheduler airflow dags trigger ratings_etl_with_sentinel

trigger-regression:
	cd airflow && docker compose exec scheduler airflow dags trigger sentinel_regression_suite

trigger-digest:
	cd airflow && docker compose exec scheduler airflow dags trigger sentinel_weekly_digest

airflow-test:
	cd airflow && docker compose exec scheduler python -m pytest /usr/local/airflow/tests/ -v
