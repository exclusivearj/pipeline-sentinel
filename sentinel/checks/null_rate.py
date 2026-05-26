"""NullRateCheck: null fraction per column <= threshold."""

from __future__ import annotations

from typing import Any

import pandas as pd

from sentinel.checks.base import BaseCheck
from sentinel.exceptions import CheckConfigurationError
from sentinel.report import CheckResult, CheckStatus


class NullRateCheck(BaseCheck):
    def __init__(self, column: str, threshold: float = 0.01, name: str | None = None) -> None:
        super().__init__(name=name)
        if not 0.0 <= threshold <= 1.0:
            raise CheckConfigurationError(
                f"NullRateCheck: threshold must be in [0, 1], got {threshold}"
            )
        self.column = column
        self.threshold = threshold

    def _params_dict(self) -> dict:
        return {"column": self.column, "threshold": self.threshold}

    def evaluate(self, df: Any) -> CheckResult:
        if self.column not in self._columns(df):
            return self._skip(
                message=f"column '{self.column}' not in DataFrame",
                column=self.column,
            )
        total = self._get_row_count(df)
        if total == 0:
            return CheckResult(
                check_name=self.name,
                check_params=self._params_dict(),
                status=CheckStatus.SKIP,
                metric_value=None,
                threshold=self.threshold,
                message="empty DataFrame; cannot compute null_rate",
                column=self.column,
            )
        if self._is_spark(df):
            from pyspark.sql import functions as F  # type: ignore

            null_count = int(df.filter(F.col(self.column).isNull()).count())
        else:
            null_count = int(pd.isna(df[self.column]).sum())
        null_rate = round(null_count / total, 4)
        passed = null_rate <= self.threshold
        status = CheckStatus.PASS if passed else CheckStatus.FAIL
        message = (
            f"null_rate={null_rate} <= threshold={self.threshold}"
            if passed
            else f"null_rate={null_rate} > threshold={self.threshold} ({null_count}/{total} null)"
        )
        return CheckResult(
            check_name=self.name,
            check_params=self._params_dict(),
            status=status,
            metric_value=null_rate,
            threshold=self.threshold,
            message=message,
            column=self.column,
        )
