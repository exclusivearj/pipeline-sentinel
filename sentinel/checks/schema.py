"""SchemaCheck: column names and dtypes match expected."""

from __future__ import annotations

from typing import Any

from sentinel.checks.base import BaseCheck
from sentinel.exceptions import CheckConfigurationError
from sentinel.report import CheckResult, CheckStatus


class SchemaCheck(BaseCheck):
    def __init__(self, expected: dict[str, str], name: str | None = None) -> None:
        super().__init__(name=name)
        if not isinstance(expected, dict) or not expected:
            raise CheckConfigurationError(
                "SchemaCheck: expected must be a non-empty dict of {column: dtype}"
            )
        self.expected = expected

    def _params_dict(self) -> dict:
        return {"expected": dict(self.expected)}

    def _actual_schema(self, df: Any) -> dict[str, str]:
        if self._is_spark(df):
            return {f.name: str(f.dataType) for f in df.schema.fields}
        return {col: str(dtype) for col, dtype in df.dtypes.items()}

    def evaluate(self, df: Any) -> CheckResult:
        actual = self._actual_schema(df)
        missing: list[str] = []
        wrong_dtype: list[tuple[str, str, str]] = []

        for col, expected_dtype in self.expected.items():
            if col not in actual:
                missing.append(col)
            elif actual[col] != expected_dtype:
                wrong_dtype.append((col, expected_dtype, actual[col]))

        if not missing and not wrong_dtype:
            return CheckResult(
                check_name=self.name,
                check_params=self._params_dict(),
                status=CheckStatus.PASS,
                metric_value=actual,
                threshold=dict(self.expected),
                message=f"schema matches ({len(self.expected)} columns)",
            )

        parts: list[str] = []
        if missing:
            parts.append(f"missing columns: {missing}")
        if wrong_dtype:
            details = ", ".join(f"{c}: expected {exp}, got {act}" for c, exp, act in wrong_dtype)
            parts.append(f"dtype mismatches: {details}")
        message = "; ".join(parts)

        return CheckResult(
            check_name=self.name,
            check_params=self._params_dict(),
            status=CheckStatus.FAIL,
            metric_value=actual,
            threshold=dict(self.expected),
            message=message,
        )
