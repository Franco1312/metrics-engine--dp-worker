"""Domain entities."""

from dataclasses import dataclass

from metrics_worker.domain.enums import ExpressionType
from metrics_worker.domain.types import ExpressionJson, JsonValue, Timestamp


@dataclass(frozen=True)
class SeriesRef:
    """Reference to a series in a dataset."""

    dataset_id: str
    series_code: str


@dataclass(frozen=True)
class MetricExpression:
    """Metric expression structure."""

    expression_type: ExpressionType
    expression_json: ExpressionJson


@dataclass(frozen=True)
class MetricOutputManifest:
    """Output manifest for a metric run."""

    run_id: str
    metric_code: str
    version_ts: str
    created_at: Timestamp
    row_count: int
    outputs: dict[str, JsonValue]


@dataclass(frozen=True)
class MetricRunState:
    """State of a metric run."""

    run_id: str
    metric_code: str
    status: str
    version_ts: str | None = None
    output_manifest: str | None = None
    row_count: int | None = None
    error: dict[str, str] | None = None

