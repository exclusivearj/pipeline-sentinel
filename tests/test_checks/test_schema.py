"""Tests for SchemaCheck."""

import pytest

from sentinel.checks import SchemaCheck
from sentinel.exceptions import CheckConfigurationError
from sentinel.report import CheckStatus

EXPECTED = {
    "user_id": "object",
    "rating": "float64",
    "event_type": "object",
    "event_ts": "datetime64[ns]",
}


def test_pass_when_schema_matches(sample_df_clean):
    result = SchemaCheck(expected=EXPECTED).evaluate(sample_df_clean)
    assert result.status == CheckStatus.PASS


def test_fail_when_column_missing(sample_df_clean):
    expected = dict(EXPECTED, missing_col="int64")
    result = SchemaCheck(expected=expected).evaluate(sample_df_clean)
    assert result.status == CheckStatus.FAIL
    assert "missing_col" in result.message


def test_fail_on_dtype_mismatch(sample_df_wrong_schema):
    result = SchemaCheck(expected=EXPECTED).evaluate(sample_df_wrong_schema)
    assert result.status == CheckStatus.FAIL
    assert "rating" in result.message


def test_fail_lists_multiple_mismatches(sample_df_clean):
    expected = {"user_id": "int64", "rating": "object", "event_type": "object"}
    result = SchemaCheck(expected=expected).evaluate(sample_df_clean)
    assert result.status == CheckStatus.FAIL
    assert "user_id" in result.message
    assert "rating" in result.message


def test_configuration_empty_expected():
    with pytest.raises(CheckConfigurationError):
        SchemaCheck(expected={})
