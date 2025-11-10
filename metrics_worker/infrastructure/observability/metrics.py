"""Prometheus metrics."""

from prometheus_client import Counter, Histogram

runs_started = Counter(
    "metric_runs_started_total",
    "Total number of metric runs started",
)

runs_succeeded = Counter(
    "metric_runs_succeeded_total",
    "Total number of metric runs succeeded",
)

runs_failed = Counter(
    "metric_runs_failed_total",
    "Total number of metric runs failed",
    ["error_code"],
)

run_duration_seconds = Histogram(
    "metric_run_duration_seconds",
    "Duration of metric runs in seconds",
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800],
)

s3_read_mb = Histogram(
    "s3_read_mb",
    "Amount of data read from S3 in MB",
    buckets=[0.1, 1, 10, 100, 1000],
)

s3_write_mb = Histogram(
    "s3_write_mb",
    "Amount of data written to S3 in MB",
    buckets=[0.1, 1, 10, 100, 1000],
)

