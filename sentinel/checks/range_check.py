"""RangeCheck: all values in [min_val, max_val]."""

from __future__ import annotations

from typing import Any

from sentinel.checks.base import BaseCheck
from sentinel.exceptions import CheckConfigurationError
from sentinel.report import CheckResult, CheckStatus


class RangeCheck(BaseCheck):
    def __init__(
        self,
        column: str,
        min_val: float,
        max_val: float,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name)
        if max_val < min_val:
            raise CheckConfigurationError(
                f"RangeCheck: max_val ({max_val}) must be >= min_val ({min_val})"
            )
        self.column = column
        self.min_val = min_val
        self.max_val = max_val

    def _params_dict(self) -> dict:
        return {"column": self.column, "min_val": self.min_val, "max_val": self.max_val}

    def evaluate(self, df: Any) -> CheckResult:
        if self.column not in self._columns(df):
            return self._skip(
                message=f"column '{self.column}' not in DataFrame",
                column=self.column,
            )
        total = self._get_row_count(df)
        if total == 0:
            return self._skip(
                message="empty DataFrame",
                column=self.column,
            )
        if self._is_spark(df):
            from pyspark.sql import functions as F  # type: ignore

            out_of_range = int(
                df.filter(
                    F.col(self.column).isNotNull()
                    & ((F.col(self.column) < self.min_val) | (F.col(self.column) > self.max_val))
                ).count()
            )
        else:
            series = df[self.column].dropna()
            out_of_range = int(((series < self.min_val) | (series > self.max_val)).sum())

        out_of_range_rate = round(out_of_range / total, 4)
        passed = out_of_range == 0
        status = CheckStatus.PASS if passed else CheckStatus.FAIL
        message = (
            f"all values in [{self.min_val}, {self.max_val}]"
            if passed
            else f"{out_of_range}/{total} values out of [{self.min_val}, {self.max_val}]"
        )
        return CheckResult(
            check_name=self.name,
            check_params=self._params_dict(),
            status=status,
            metric_value={
                "out_of_range_count": out_of_range,
                "out_of_range_rate": out_of_range_rate,
            },
            threshold={"min_val": self.min_val, "max_val": self.max_val},
            message=message,
            column=self.column,
        )
