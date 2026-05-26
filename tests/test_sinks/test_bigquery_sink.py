"""Tests for BigQuerySink (mocks out the google client entirely)."""

import sys
import types
from unittest.mock import MagicMock

from sentinel.report import CheckResult, CheckStatus, ObservabilityReport
from sentinel.sinks import BigQuerySink


def _make_report() -> ObservabilityReport:
    report = ObservabilityReport(pipeline_name="p", table_name="t", row_count=100)
    report.check_results.append(
        CheckResult(
            check_name="RowCountCheck",
            check_params={},
            status=CheckStatus.PASS,
            metric_value=100,
            threshold=10,
            message="ok",
        )
    )
    return report


def test_skips_gracefully_when_bigquery_not_installed(monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", None)

    sink = BigQuerySink(project="proj", dataset="ds")
    # Force the import to fail by clearing 'google.cloud' from imports
    real_import = (
        __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__
    )

    def fake_import(name, *args, **kwargs):
        if name.startswith("google"):
            raise ImportError("google not available")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)
    # Should log a warning but not raise
    sink.write(_make_report())


def test_writes_one_row_per_check_result(monkeypatch):
    captured = {}

    fake_bq = types.ModuleType("bigquery")

    class FakeClient:
        def __init__(self, project, credentials=None):
            captured["project"] = project

        def insert_rows_json(self, table_ref, rows):
            captured["table_ref"] = table_ref
            captured["rows"] = rows
            return []

    fake_bq.Client = FakeClient

    fake_google = types.ModuleType("google")
    fake_cloud = types.ModuleType("google.cloud")
    fake_cloud.bigquery = fake_bq
    fake_google.cloud = fake_cloud
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.cloud", fake_cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bq)

    sink = BigQuerySink(project="proj", dataset="ds", table="sentinel")
    sink.write(_make_report())

    assert captured["table_ref"] == "proj.ds.sentinel"
    assert len(captured["rows"]) == 1
    row = captured["rows"][0]
    assert row["pipeline_name"] == "p"
    assert row["check_name"] == "RowCountCheck"
    assert row["metric_value"] == 100.0
