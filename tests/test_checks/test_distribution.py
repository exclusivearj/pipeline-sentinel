"""Tests for DistributionCheck."""

import pytest

from sentinel.checks import DistributionCheck
from sentinel.exceptions import CheckConfigurationError
from sentinel.report import CheckStatus


def test_pass_within_z_threshold(sample_df_clean):
    actual_mean = sample_df_clean["rating"].mean()
    result = DistributionCheck(
        "rating",
        baseline_mean=actual_mean,
        baseline_stddev=0.5,
        z_score_threshold=3.0,
    ).evaluate(sample_df_clean)
    assert result.status == CheckStatus.PASS
    assert result.metric_value["z_score"] < 3.0


def test_fail_when_mean_drift(sample_df_clean):
    result = DistributionCheck(
        "rating",
        baseline_mean=0.5,
        baseline_stddev=0.5,
        z_score_threshold=3.0,
    ).evaluate(sample_df_clean)
    assert result.status == CheckStatus.FAIL
    assert result.metric_value["z_score"] > 3.0


def test_metric_value_keys(sample_df_clean):
    result = DistributionCheck("rating", baseline_mean=3.0, z_score_threshold=10.0).evaluate(
        sample_df_clean
    )
    assert "actual_mean" in result.metric_value
    assert "baseline_mean" in result.metric_value
    assert "z_score" in result.metric_value


def test_skip_when_column_missing(sample_df_clean):
    result = DistributionCheck("missing", baseline_mean=0).evaluate(sample_df_clean)
    assert result.status == CheckStatus.SKIP


def test_configuration_invalid_threshold():
    with pytest.raises(CheckConfigurationError):
        DistributionCheck("col", baseline_mean=0, z_score_threshold=0)
    with pytest.raises(CheckConfigurationError):
        DistributionCheck("col", baseline_mean=0, baseline_stddev=-1)
