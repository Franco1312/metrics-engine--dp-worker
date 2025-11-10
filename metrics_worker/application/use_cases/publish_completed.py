"""Publish metric_run_completed event."""

from metrics_worker.domain.ports import EventBusPort


async def run_success(
    run_id: str,
    metric_code: str,
    version_ts: str,
    output_manifest: str,
    row_count: int,
    event_bus: EventBusPort,
) -> None:
    """Publish SUCCESS metric_run_completed event."""
    await event_bus.publish_completed(
        run_id=run_id,
        metric_code=metric_code,
        status="SUCCESS",
        version_ts=version_ts,
        output_manifest=output_manifest,
        row_count=row_count,
    )


async def run_failure(
    run_id: str,
    metric_code: str,
    error_code: str,
    error_message: str,
    event_bus: EventBusPort,
) -> None:
    """Publish FAILURE metric_run_completed event."""
    error_str = f"{error_code}: {error_message}" if error_code else error_message
    await event_bus.publish_completed(
        run_id=run_id,
        metric_code=metric_code,
        status="FAILURE",
        error=error_str,
    )

