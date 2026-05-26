"""@observe decorator and CheckRunner."""

from __future__ import annotations

import functools
import logging
import time
from typing import Any, Callable, Iterable, Optional

import pandas as pd

from sentinel.checks.base import BaseCheck
from sentinel.exceptions import CheckConfigurationError, DataQualityError
from sentinel.report import CheckStatus, ObservabilityReport
from sentinel.sinks.base import BaseSink
from sentinel.sinks.log_sink import LogSink

_VALID_ON_FAILURE = {"warn", "raise"}
_VALID_EVALUATE = {"input", "output", "both"}

_logger = logging.getLogger(__name__)


def _is_dataframe(value: Any) -> bool:
    if isinstance(value, pd.DataFrame):
        return True
    try:
        from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore

        return isinstance(value, SparkDataFrame)
    except Exception:
        return False


def _first_dataframe(args: tuple, kwargs: dict) -> Optional[Any]:
    for v in args:
        if _is_dataframe(v):
            return v
    for v in kwargs.values():
        if _is_dataframe(v):
            return v
    return None


def _row_count(df: Any) -> int:
    if df is None:
        return 0
    try:
        from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore

        if isinstance(df, SparkDataFrame):
            return int(df.count())
    except Exception:
        pass
    return int(len(df))


def _run_checks(
    df: Any,
    checks: Iterable[BaseCheck],
    pipeline_name: str,
    table_name: str,
) -> ObservabilityReport:
    report = ObservabilityReport(
        pipeline_name=pipeline_name,
        table_name=table_name,
        row_count=_row_count(df),
    )
    for check in checks:
        result = check._safe_evaluate(df)
        report.check_results.append(result)
    return report


def observe(
    pipeline_name: str = "",
    table_name: str = "",
    checks: Optional[list[BaseCheck]] = None,
    sinks: Optional[list[BaseSink]] = None,
    on_failure: str = "warn",
    evaluate: str = "output",
) -> Callable:
    """Decorator that runs data quality checks around a transform function.

    Parameters
    ----------
    pipeline_name, table_name: identifiers used in reports/sinks.
    checks: list of BaseCheck instances to evaluate.
    sinks: list of BaseSink instances; defaults to [LogSink()].
    on_failure: "warn" (default, log + continue) or "raise" (raise DataQualityError).
    evaluate: "input", "output" (default), or "both" — which DataFrame(s) to check.
    """
    if on_failure not in _VALID_ON_FAILURE:
        raise CheckConfigurationError(
            f"observe: on_failure must be one of {sorted(_VALID_ON_FAILURE)}, got '{on_failure}'"
        )
    if evaluate not in _VALID_EVALUATE:
        raise CheckConfigurationError(
            f"observe: evaluate must be one of {sorted(_VALID_EVALUATE)}, got '{evaluate}'"
        )

    resolved_checks: list[BaseCheck] = list(checks) if checks else []
    resolved_sinks: list[BaseSink] = list(sinks) if sinks else [LogSink()]

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.monotonic()

            if evaluate in {"input", "both"}:
                input_df = _first_dataframe(args, kwargs)
                if input_df is not None and resolved_checks:
                    input_report = _run_checks(
                        input_df, resolved_checks, pipeline_name, f"{table_name}__input"
                    )
                    input_report.duration_ms = (time.monotonic() - start) * 1000
                    for sink in resolved_sinks:
                        sink._safe_write(input_report)
                    if on_failure == "raise" and input_report.overall_status == CheckStatus.FAIL:
                        raise DataQualityError(
                            f"Input failed {len(input_report.failed_checks)} check(s) "
                            f"in {pipeline_name}/{table_name}",
                            report=input_report,
                        )

            result = fn(*args, **kwargs)

            if evaluate in {"output", "both"}:
                output_df = result if _is_dataframe(result) else _first_dataframe(args, kwargs)
                if output_df is not None and resolved_checks:
                    output_report = _run_checks(
                        output_df, resolved_checks, pipeline_name, table_name
                    )
                    output_report.duration_ms = (time.monotonic() - start) * 1000
                    for sink in resolved_sinks:
                        sink._safe_write(output_report)
                    if on_failure == "raise" and output_report.overall_status == CheckStatus.FAIL:
                        raise DataQualityError(
                            f"Output failed {len(output_report.failed_checks)} check(s) "
                            f"in {pipeline_name}/{table_name}",
                            report=output_report,
                        )
                elif output_df is None and resolved_checks:
                    _logger.warning(
                        "@observe(%s/%s): no DataFrame returned and no DataFrame in args; "
                        "checks skipped.",
                        pipeline_name,
                        table_name,
                    )

            return result

        wrapper.__sentinel_checks__ = resolved_checks  # type: ignore[attr-defined]
        wrapper.__sentinel_sinks__ = resolved_sinks  # type: ignore[attr-defined]
        return wrapper

    return decorator
