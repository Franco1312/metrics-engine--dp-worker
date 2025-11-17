"""Ports (interfaces) for infrastructure adapters."""

from abc import ABC, abstractmethod

from metrics_worker.domain.entities import MetricOutputManifest
from metrics_worker.domain.types import DatasetManifestDict, SeriesFrame, Timestamp


class CatalogPort(ABC):
    """Port for reading dataset manifests from catalog."""

    @abstractmethod
    async def get_dataset_manifest(self, manifest_path: str) -> DatasetManifestDict:
        """Get dataset manifest from catalog."""


class DataReaderPort(ABC):
    """Port for reading series data."""

    @abstractmethod
    async def read_series_from_paths(
        self,
        parquet_paths: list[str],
        series_code: str,
    ) -> SeriesFrame:
        """Read series data from specific parquet file paths."""


class OutputWriterPort(ABC):
    """Port for writing metric outputs."""

    @abstractmethod
    async def write_jsonl(
        self,
        data: SeriesFrame,
        output_path: str,
    ) -> list[str]:
        """Write JSONL file(s) and return list of file paths."""

    @abstractmethod
    async def write_manifest(
        self,
        manifest: MetricOutputManifest,
        manifest_path: str,
    ) -> None:
        """Write output manifest."""

    @abstractmethod
    async def check_run_marker(self, marker_path: str) -> bool:
        """Check if run marker exists (idempotency)."""

    @abstractmethod
    async def create_run_marker(self, marker_path: str) -> None:
        """Create run marker for idempotency."""


class EventBusPort(ABC):
    """Port for publishing events."""

    @abstractmethod
    async def publish_started(
        self,
        run_id: str,
        metric_code: str,
        started_at: Timestamp,
    ) -> None:
        """Publish metric_run_started event."""

    @abstractmethod
    async def publish_heartbeat(
        self,
        run_id: str,
        metric_code: str,
        progress: float,
        ts: Timestamp,
    ) -> None:
        """Publish metric_run_heartbeat event."""

    @abstractmethod
    async def publish_completed(
        self,
        run_id: str,
        metric_code: str,
        status: str,
        version_ts: str | None = None,
        output_manifest: str | None = None,
        row_count: int | None = None,
        error: str | None = None,
    ) -> None:
        """Publish metric_run_completed event."""


class ClockPort(ABC):
    """Port for time operations."""

    @abstractmethod
    def now(self) -> Timestamp:
        """Get current timestamp."""

    @abstractmethod
    def format_version_ts(self, ts: Timestamp) -> str:
        """Format timestamp as version_ts string."""

