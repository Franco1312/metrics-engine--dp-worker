"""Build output manifest."""

from metrics_worker.domain.entities import MetricOutputManifest
from metrics_worker.domain.ports import ClockPort
from metrics_worker.domain.types import Timestamp


async def run(
    run_id: str,
    metric_code: str,
    version_ts: str,
    row_count: int,
    output_files: list[str],
    data_prefix: str,
    clock: ClockPort,
) -> MetricOutputManifest:
    """Build output manifest."""
    created_at: Timestamp = clock.now()

    manifest = MetricOutputManifest(
        run_id=run_id,
        metric_code=metric_code,
        version_ts=version_ts,
        created_at=created_at,
        row_count=row_count,
        outputs={
            "data_prefix": data_prefix,
            "files": output_files,
        },
    )

    return manifest

