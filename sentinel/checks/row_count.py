"""RowCountCheck: bounds on DataFrame row count."""

from __future__ import annotations

import math
from typing import Any

from sentinel.checks.base import BaseCheck
from sentinel.exceptions import CheckConfigurationError
from sentinel.report import CheckResult, CheckStatus


class RowCountCheck(BaseCheck):
    def __init__(self, min: int = 0, max: float = math.inf, name: str | None = None) -> None:
        super().__init__(name=name)
        if min < 0:
            raise CheckConfigurationError(f"RowCountCheck: min must be >= 0, got {min}")
        if max < min:
            raise CheckConfigurationError(f"RowCountCheck: max ({max}) must be >= min ({min})")
        self.min = min
        self.max = max

    def _params_dict(self) -> dict:
        return {"min": self.min, "max": self.max if self.max != math.inf else None}

    def evaluate(self, df: Any) -> CheckResult:
        actual = self._get_row_count(df)
        passed = self.min <= actual <= self.max
        status = CheckStatus.PASS if passed else CheckStatus.FAIL
        max_repr = "∞" if self.max == math.inf else str(self.max)
        message = (
            f"row_count={actual} within [{self.min}, {max_repr}]"
            if passed
            else f"row_count={actual} out of bounds [{self.min}, {max_repr}]"
        )
        return CheckResult(
            check_name=self.name,
            check_params=self._params_dict(),
            status=status,
            metric_value=actual,
            threshold={"min": self.min, "max": None if self.max == math.inf else self.max},
            message=message,
        )
