"""Publish metric_run_started event."""

from metrics_worker.domain.ports import ClockPort, EventBusPort


async def run(
    run_id: str,
    metric_code: str,
    event_bus: EventBusPort,
    clock: ClockPort,
) -> None:
    """Publish metric_run_started event."""
    started_at = clock.now()
    await event_bus.publish_started(run_id, metric_code, started_at)

