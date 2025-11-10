"""Health check and metrics server."""

from prometheus_client import start_http_server

from metrics_worker.infrastructure.config.settings import Settings


def start_metrics_server(settings: Settings) -> None:
    """Start Prometheus metrics HTTP server."""
    start_http_server(settings.prometheus_port)

