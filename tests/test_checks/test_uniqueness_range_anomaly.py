"""Tests for UniquenessCheck, RangeCheck, AnomalyCheck."""

import pytest

from sentinel.checks import AnomalyCheck, RangeCheck, UniquenessCheck
from sentinel.exceptions import CheckConfigurationError
from sentinel.report import CheckStatus

# ----- UniquenessCheck -----


def test_uniqueness_pass_unique_column(sample_df_clean):
    df = sample_df_clean.copy()
    df["id"] = range(len(df))
    result = UniquenessCheck("id", threshold=0.0).evaluate(df)
    assert result.status == CheckStatus.PASS


def test_uniqueness_fail_when_duplicated(sample_df_low_variety_user):
    result = UniquenessCheck("user_id", threshold=0.0).evaluate(sample_df_low_variety_user)
    assert result.status == CheckStatus.FAIL


def test_uniqueness_pass_under_loose_threshold(sample_df_low_variety_user):
    # 1 unique / 1000 rows = dup_rate 0.999; threshold 1.0 admits any duplication
    result = UniquenessCheck("user_id", threshold=1.0).evaluate(sample_df_low_variety_user)
    assert result.status == CheckStatus.PASS


def test_uniqueness_skip_when_column_missing(sample_df_clean):
    result = UniquenessCheck("nope").evaluate(sample_df_clean)
    assert result.status == CheckStatus.SKIP


def test_uniqueness_invalid_threshold():
    with pytest.raises(CheckConfigurationError):
        UniquenessCheck("col", threshold=2.0)


# ----- RangeCheck -----


def test_range_pass_when_all_in_range(sample_df_clean):
    result = RangeCheck("rating", min_val=0.5, max_val=5.0).evaluate(sample_df_clean)
    assert result.status == CheckStatus.PASS
    assert result.metric_value["out_of_range_count"] == 0


def test_range_fail_with_out_of_range_values(sample_df_out_of_range):
    result = RangeCheck("rating", min_val=0.5, max_val=5.0).evaluate(sample_df_out_of_range)
    assert result.status == CheckStatus.FAIL
    assert result.metric_value["out_of_range_count"] >= 1


def test_range_skip_when_column_missing(sample_df_clean):
    result = RangeCheck("nope", min_val=0, max_val=1).evaluate(sample_df_clean)
    assert result.status == CheckStatus.SKIP


def test_range_invalid_bounds():
    with pytest.raises(CheckConfigurationError):
        RangeCheck("col", min_val=5, max_val=1)


# ----- AnomalyCheck -----


def test_anomaly_skip_with_short_baseline(sample_df_clean):
    result = AnomalyCheck(baseline=[100, 110]).evaluate(sample_df_clean)
    assert result.status == CheckStatus.SKIP


def test_anomaly_pass_when_within_threshold(sample_df_clean):
    # baseline centered around 1000 with low variance
    baseline = [990, 1000, 1010, 995, 1005, 1002, 998]
    result = AnomalyCheck(baseline=baseline, stddev_threshold=3.0).evaluate(sample_df_clean)
    assert result.status == CheckStatus.PASS


def test_anomaly_fail_on_drop(sample_df_small):
    baseline = [990, 1000, 1010, 995, 1005, 1002, 998]
    result = AnomalyCheck(baseline=baseline, stddev_threshold=3.0).evaluate(sample_df_small)
    assert result.status == CheckStatus.FAIL
    assert result.metric_value["z_score"] > 3.0


def test_anomaly_mean_metric_requires_column():
    with pytest.raises(CheckConfigurationError):
        AnomalyCheck(metric="mean")


def test_anomaly_invalid_metric():
    with pytest.raises(CheckConfigurationError):
        AnomalyCheck(metric="not_a_metric", baseline=[1, 2, 3])


def test_anomaly_invalid_threshold():
    with pytest.raises(CheckConfigurationError):
        AnomalyCheck(baseline=[1, 2, 3], stddev_threshold=0)


def test_anomaly_mean_metric(sample_df_clean):
    baseline = [3.0, 3.05, 2.95, 3.02, 3.01]
    result = AnomalyCheck(column="rating", metric="mean", baseline=baseline).evaluate(
        sample_df_clean
    )
    assert result.status in {CheckStatus.PASS, CheckStatus.FAIL}
