"""Publish metric_run_heartbeat event."""

from metrics_worker.domain.ports import ClockPort, EventBusPort


async def run(
    run_id: str,
    metric_code: str,
    progress: float,
    event_bus: EventBusPort,
    clock: ClockPort,
) -> None:
    """Publish metric_run_heartbeat event."""
    ts = clock.now()
    await event_bus.publish_heartbeat(run_id, metric_code, progress, ts)

