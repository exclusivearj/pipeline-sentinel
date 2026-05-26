"""CheckResult, CheckStatus, and ObservabilityReport dataclasses."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class CheckStatus(Enum):
    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"
    SKIP = "skip"
    ERROR = "error"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CheckResult:
    check_name: str
    check_params: dict
    status: CheckStatus
    metric_value: Any
    threshold: Any
    message: str
    evaluated_at: datetime = field(default_factory=_utcnow)
    column: Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value
        d["evaluated_at"] = self.evaluated_at.isoformat()
        return d


@dataclass
class ObservabilityReport:
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name: str = ""
    table_name: str = ""
    check_results: list[CheckResult] = field(default_factory=list)
    row_count: int = 0
    evaluated_at: datetime = field(default_factory=_utcnow)
    duration_ms: float = 0.0

    @property
    def overall_status(self) -> CheckStatus:
        statuses = {r.status for r in self.check_results}
        if CheckStatus.FAIL in statuses:
            return CheckStatus.FAIL
        if CheckStatus.ERROR in statuses:
            return CheckStatus.ERROR
        if CheckStatus.WARN in statuses:
            return CheckStatus.WARN
        return CheckStatus.PASS

    @property
    def failed_checks(self) -> list[CheckResult]:
        return [r for r in self.check_results if r.status == CheckStatus.FAIL]

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "table_name": self.table_name,
            "overall_status": self.overall_status.value,
            "row_count": self.row_count,
            "duration_ms": self.duration_ms,
            "evaluated_at": self.evaluated_at.isoformat(),
            "check_results": [r.to_dict() for r in self.check_results],
        }

    def to_markdown_table(self) -> str:
        if not self.check_results:
            return "_(no checks evaluated)_"
        header = "| Check | Column | Metric | Threshold | Status |\n"
        sep = "|---|---|---|---|---|\n"
        rows = []
        for r in self.check_results:
            metric = _short_repr(r.metric_value)
            threshold = _short_repr(r.threshold)
            col = r.column or "-"
            rows.append(
                f"| {r.check_name} | {col} | {metric} | {threshold} | {r.status.value.upper()} |"
            )
        return header + sep + "\n".join(rows)


def _short_repr(value: Any, max_len: int = 40) -> str:
    if value is None:
        return "-"
    s = repr(value) if not isinstance(value, (str, int, float, bool)) else str(value)
    return s if len(s) <= max_len else s[: max_len - 1] + "…"
