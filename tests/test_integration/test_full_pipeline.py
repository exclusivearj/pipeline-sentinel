"""End-to-end tests for the @observe decorator."""

import logging

import pandas as pd
import pytest

from sentinel import (
    DataQualityError,
    NullRateCheck,
    RowCountCheck,
    SchemaCheck,
    observe,
)
from sentinel.exceptions import CheckConfigurationError
from sentinel.report import CheckStatus, ObservabilityReport
from sentinel.sinks.base import BaseSink


class _CaptureSink(BaseSink):
    def __init__(self):
        self.reports: list[ObservabilityReport] = []

    def write(self, report):
        self.reports.append(report)


def test_observe_preserves_function_metadata():
    @observe(pipeline_name="p", table_name="t")
    def my_transform(df):
        """Do nothing."""
        return df

    assert my_transform.__name__ == "my_transform"
    assert (my_transform.__doc__ or "").strip() == "Do nothing."


def test_observe_warns_by_default_on_failure(sample_df_with_nulls, caplog):
    sink = _CaptureSink()

    @observe(
        pipeline_name="p",
        table_name="t",
        checks=[NullRateCheck("user_id", threshold=0.001)],
        sinks=[sink],
        on_failure="warn",
    )
    def transform(df):
        return df

    with caplog.at_level(logging.INFO, logger="pipeline-sentinel"):
        result = transform(sample_df_with_nulls)
    assert isinstance(result, pd.DataFrame)
    assert sink.reports
    assert sink.reports[-1].overall_status == CheckStatus.FAIL


def test_observe_raises_on_failure(sample_df_with_nulls):
    @observe(
        pipeline_name="p",
        table_name="t",
        checks=[NullRateCheck("user_id", threshold=0.001)],
        on_failure="raise",
    )
    def transform(df):
        return df

    with pytest.raises(DataQualityError) as exc:
        transform(sample_df_with_nulls)
    assert exc.value.report is not None
    assert exc.value.report.overall_status == CheckStatus.FAIL


def test_observe_default_sink_is_log_sink(sample_df_clean, caplog):
    @observe(
        pipeline_name="p",
        table_name="t",
        checks=[RowCountCheck(min=1)],
    )
    def transform(df):
        return df

    with caplog.at_level(logging.INFO, logger="pipeline-sentinel"):
        transform(sample_df_clean)
    assert any("p/t" in rec.message for rec in caplog.records)


def test_observe_evaluate_input(sample_df_with_nulls):
    captured = []

    class Sink(BaseSink):
        def write(self, report):
            captured.append(report)

    @observe(
        pipeline_name="p",
        table_name="t",
        checks=[NullRateCheck("user_id", threshold=0.001)],
        sinks=[Sink()],
        on_failure="raise",
        evaluate="input",
    )
    def transform(df):
        return df

    with pytest.raises(DataQualityError):
        transform(sample_df_with_nulls)


def test_observe_invalid_on_failure():
    with pytest.raises(CheckConfigurationError):
        observe(on_failure="explode")


def test_observe_invalid_evaluate():
    with pytest.raises(CheckConfigurationError):
        observe(evaluate="sometimes")


def test_observe_no_dataframe_returned_logs_warning(caplog):
    @observe(
        pipeline_name="p",
        table_name="t",
        checks=[RowCountCheck(min=1)],
    )
    def transform():
        return 42

    with caplog.at_level(logging.WARNING):
        result = transform()
    assert result == 42
    assert any("no DataFrame" in rec.message for rec in caplog.records)


def test_overall_status_promotes_correctly():
    """ObservabilityReport.overall_status reflects worst result."""
    from sentinel.report import CheckResult

    report = ObservabilityReport()
    report.check_results.append(
        CheckResult(
            check_name="A",
            check_params={},
            status=CheckStatus.PASS,
            metric_value=1,
            threshold=1,
            message="ok",
        )
    )
    assert report.overall_status == CheckStatus.PASS
    report.check_results.append(
        CheckResult(
            check_name="B",
            check_params={},
            status=CheckStatus.WARN,
            metric_value=1,
            threshold=1,
            message="warn",
        )
    )
    assert report.overall_status == CheckStatus.WARN
    report.check_results.append(
        CheckResult(
            check_name="C",
            check_params={},
            status=CheckStatus.ERROR,
            metric_value=1,
            threshold=1,
            message="err",
        )
    )
    assert report.overall_status == CheckStatus.ERROR
    report.check_results.append(
        CheckResult(
            check_name="D",
            check_params={},
            status=CheckStatus.FAIL,
            metric_value=1,
            threshold=1,
            message="fail",
        )
    )
    assert report.overall_status == CheckStatus.FAIL
