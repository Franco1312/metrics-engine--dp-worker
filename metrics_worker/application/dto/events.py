"""Event DTOs."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from metrics_worker.domain.enums import ExpressionType
from metrics_worker.domain.types import CatalogDict, ExpressionJson


class MetricRunRequestedEvent(BaseModel):
    """Metric run requested event from Control Plane."""

    model_config = ConfigDict(populate_by_name=True)

    type: str
    run_id: str = Field(alias="runId")
    metric_code: str = Field(alias="metricCode")
    expression_type: ExpressionType = Field(alias="expressionType")
    expression_json: ExpressionJson = Field(alias="expressionJson")
    inputs: list[dict[str, str]]
    catalog: CatalogDict
    output: dict[str, str]


class MetricRunStartedEvent(BaseModel):
    """Metric run started event to Control Plane."""

    model_config = ConfigDict(populate_by_name=True)

    type: str
    run_id: str = Field(alias="runId")
    started_at: str = Field(alias="startedAt")  # ISO 8601 string, required


class MetricRunHeartbeatEvent(BaseModel):
    """Metric run heartbeat event to Control Plane."""

    model_config = ConfigDict(populate_by_name=True)

    type: str
    run_id: str = Field(alias="runId")
    progress: float = Field(ge=0.0, le=1.0)  # Required, 0.0-1.0
    ts: str  # ISO 8601 string, required


class MetricRunCompletedEvent(BaseModel):
    """Metric run completed event to Control Plane."""

    model_config = ConfigDict(populate_by_name=True)

    type: str
    run_id: str = Field(alias="runId")
    metric_code: str = Field(alias="metricCode")
    status: str  # "SUCCESS" or "FAILURE"
    # Fields for SUCCESS (required when status == "SUCCESS")
    version_ts: str | None = Field(None, alias="versionTs")  # Format: YYYY-MM-DDTHH-mm-ss
    output_manifest: str | None = Field(None, alias="outputManifest")  # Relative path to S3
    row_count: int | None = Field(None, alias="rowCount")
    # Fields for FAILURE (required when status == "FAILURE")
    error: str | None = None  # Error message as string

