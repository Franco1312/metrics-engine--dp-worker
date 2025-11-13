# metrics-engine-dp-worker

Data-plane worker for metrics engine - processes metric runs from control-plane.

## Overview

This service is a batch/event-driven worker that:

- Consumes `metric_run_requested` events from SNS/SQS FIFO
- Resolves input dataset manifests
- Reads Parquet data from S3 in parallel with column pruning and predicate pushdown
- Evaluates metric expressions (series_math, window_op, composite)
- Writes output Parquet files and manifests
- Publishes status events (started, heartbeat, completed)

## Architecture

The project follows **Clean Architecture** strictly:

```
metrics_worker/
  domain/               # Pure business logic (entities, ports, errors)
  application/          # Use cases and services
  infrastructure/       # External adapters (AWS, I/O, observability)
  interfaces/           # Interface adapters
```

### Key Components

- **Domain Layer**: Pure business entities, ports (interfaces), and domain errors
- **Application Layer**: Use cases orchestration, expression evaluation, window operations
- **Infrastructure Layer**: AWS adapters (S3, SNS, SQS), Parquet I/O, logging, metrics
- **Interfaces Layer**: SQS runner adapter

## Features

- ✅ Expression evaluation: `series_math`, `window_op`, `composite`
- ✅ Window operations: SMA, EMA, sum, max, min, lag
- ✅ Efficient Parquet reading with PyArrow (column pruning, predicate pushdown)
- ✅ Parallel series reading for improved performance
- ✅ Idempotency by runId
- ✅ Structured JSON logging (structlog)
- ✅ Prometheus metrics
- ✅ Retry logic with tenacity
- ✅ Type safety with mypy strict mode

## Requirements

- Python 3.11+
- Poetry for dependency management
- AWS credentials configured (IAM roles or environment variables)

## Installation

```bash
# Install dependencies
make install
# or
poetry install
```

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and fill in your actual values:

```bash
cp .env.example .env
# Edit .env with your AWS credentials and configuration
```

**Required variables:**
- `AWS_S3_BUCKET`: S3 bucket for datasets and metrics output
- `AWS_SQS_RUN_REQUEST_QUEUE_URL`: SQS FIFO queue URL for metric run requests
- `AWS_SNS_EVENTS_TOPIC_ARN`: SNS FIFO topic ARN for publishing events

**Optional variables (with defaults):**
- `AWS_REGION` (default: `us-east-1`)
- `AWS_SQS_RUN_REQUEST_QUEUE_ENABLED` (default: `true`)
- `WORKER_HEARTBEAT_ENABLED` (default: `true`)
- `WORKER_HEARTBEAT_INTERVAL_SECONDS` (default: `30`)
- `OUTPUT_COMPRESSION` (default: `snappy`)
- `PROMETHEUS_PORT` (default: `9300`)

The `.env` file is automatically loaded by `pydantic-settings`. You can also set these as environment variables directly.

## Usage

### Run locally

```bash
make run-local
# or
poetry run python -m metrics_worker.infrastructure.runtime.main
```

### Development

```bash
# Format code
make fmt

# Lint
make lint

# Type check
make typecheck

# Run tests
make test

# Run tests with coverage
make test-cov
```

## Expression Grammar

### series_math

```json
{
  "op": "add" | "subtract" | "multiply" | "ratio",
  "left": { "series_code": "X" } | <nested expression>,
  "right": { "series_code": "Y" } | <nested expression>,
  "scale": <number> (optional)
}
```

### window_op

```json
{
  "op": "sma" | "ema" | "sum" | "max" | "min" | "lag",
  "series": { "series_code": "X" } | <nested expression>,
  "window": <int> (>= 1)
}
```

### composite

```json
{
  "op": "sum" | "avg" | "max" | "min",
  "operands": [
    { "series_code": "X" } | <nested expression>,
    ...
  ] (>= 2 operands)
}
```

## Event Contracts

Para detalles completos de los contratos de eventos:
- **Eventos recibidos del Control Plane**: [docs/EVENT_CONTRACTS.md](docs/EVENT_CONTRACTS.md)
- **Eventos publicados al Control Plane**: Ver sección "Eventos Publicados" abajo

### Eventos Recibidos: metric_run_requested

```json
{
  "schema": "metrics.run.v1",
  "type": "metric_run_requested",
  "runId": "uuid",
  "metricCode": "ratio.reserves_to_base",
  "expressionType": "series_math",
  "expressionJson": { ... },
  "inputs": [
    { "datasetId": "dataset_id", "seriesCode": "SERIES_CODE" }
  ],
  "catalog": {
    "datasets": {
      "dataset_id": {
        "manifestPath": "path/to/manifest.json",
        "projectionsPath": "path/to/projections/"
      }
    }
  },
  "output": {
    "basePath": "s3://bucket/metrics/metric_code/"
  }
}
```

### metric_run_completed (SUCCESS)

```json
{
  "schema": "metrics.run.v1",
  "type": "metric_run_completed",
  "runId": "uuid",
  "metricCode": "ratio.reserves_to_base",
  "status": "SUCCESS",
  "versionTs": "2025-01-15T10-30-00",
  "outputManifest": "s3://bucket/metrics/metric_code/version/manifest.json",
  "rowCount": 1000
}
```

### metric_run_completed (FAILURE)

```json
{
  "schema": "metrics.run.v1",
  "type": "metric_run_completed",
  "runId": "uuid",
  "metricCode": "ratio.reserves_to_base",
  "status": "FAILURE",
  "error": {
    "code": "INPUT_READ_ERROR",
    "message": "Failed to read input series: SERIES_CODE"
  }
}
```

## Eventos Publicados al Control Plane

El worker publica eventos a **SNS Topics** para notificar el estado de los runs. Todos los eventos son **requeridos**:

### metric_run_started (Requerido)

```json
{
  "type": "metric_run_started",
  "runId": "550e8400-e29b-41d4-a716-446655440000",
  "startedAt": "2025-01-15T10:30:10Z"
}
```

### metric_run_heartbeat (Requerido)

```json
{
  "type": "metric_run_heartbeat",
  "runId": "550e8400-e29b-41d4-a716-446655440000",
  "progress": 0.5,
  "ts": "2025-01-15T10:32:00Z"
}
```

### metric_run_completed (Requerido)

**SUCCESS:**
```json
{
  "type": "metric_run_completed",
  "runId": "550e8400-e29b-41d4-a716-446655440000",
  "metricCode": "ratio.reserves_to_base",
  "status": "SUCCESS",
  "versionTs": "2025-01-15T10-30-00",
  "outputManifest": "metrics/ratio.reserves_to_base/2025-01-15T10-30-00/manifest.json",
  "rowCount": 1000
}
```

**FAILURE:**
```json
{
  "type": "metric_run_completed",
  "runId": "550e8400-e29b-41d4-a716-446655440000",
  "metricCode": "ratio.reserves_to_base",
  "status": "FAILURE",
  "error": "Failed to read input series: BCRA_RESERVAS_USD_M_D"
}
```

### Configuración de Topics SNS

El worker publica eventos a **SNS Topics** separados. El Control Plane consume de colas SQS que están suscritas a estos topics. **Todos los topics son requeridos**:

- `AWS_SNS_METRIC_RUN_STARTED_TOPIC_ARN` (requerido)
- `AWS_SNS_METRIC_RUN_HEARTBEAT_TOPIC_ARN` (requerido)
- `AWS_SNS_METRIC_RUN_COMPLETED_TOPIC_ARN` (requerido)

**Nota**: Los eventos se publican a SNS, y el Control Plane consume de las colas SQS suscritas a estos topics.

## Error Codes

- `INPUT_READ_ERROR`: Failed to read input series
- `EXPRESSION_EVAL_ERROR`: Error evaluating expression
- `OUTPUT_WRITE_ERROR`: Failed to write output
- `MANIFEST_VALIDATION_ERROR`: Output manifest validation failed
- `CONFIG_ERROR`: Configuration error
- `INTERNAL_ERROR`: Internal processing error

## Data Reading Flow

The worker reads series data using the following process:

1. **Extract manifest information**: From the event's `catalog`, get `manifestPath` and `projectionsPath` for each dataset
2. **Read dataset manifest**: Fetch the manifest JSON from S3 to get the list of `parquet_files`
3. **Filter relevant files**: Identify which parquet files contain the needed series (based on `series_codes` in the manifest)
4. **Construct full paths**: Combine `projectionsPath` + `parquet_file_path` to get the complete S3 paths
5. **Read in parallel**: All series are read concurrently using `asyncio.gather` for optimal performance
6. **Apply filters**: PyArrow applies column pruning and predicate pushdown for efficient data reading

For more details, see [docs/DATA_FLOW.md](docs/DATA_FLOW.md).

## S3 Structure

### Output

```
metrics/{metricCode}/{versionTs}/
  data/metrics.parquet
  manifest.json
metrics/{metricCode}/current/manifest.json  # alias
metrics/{metricCode}/runs/{runId}.ok        # idempotency marker
```

## Observability

### Logging

Structured JSON logs with `structlog`:
- `run_id`, `metric_code`, `event_type`
- Timing information
- Error details

### Metrics (Prometheus)

- `metric_runs_started_total`: Counter
- `metric_runs_succeeded_total`: Counter
- `metric_runs_failed_total`: Counter (with `error_code` label)
- `metric_run_duration_seconds`: Histogram
- `s3_read_mb`: Histogram
- `s3_write_mb`: Histogram

Metrics endpoint: `http://localhost:9300/metrics`

## Idempotency

The worker ensures exactly-once processing:

1. Checks for run marker: `metrics/{metricCode}/runs/{runId}.ok`
2. If marker exists, skips processing and republishes SUCCESS
3. Creates marker after successful completion
4. Validates output manifest before reporting SUCCESS

## Testing

```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# All tests
pytest
```

## Docker

```bash
# Build image
make docker-build
# or
docker build -t metrics-engine-dp-worker:latest .

# Run container
docker run -e AWS_REGION=us-east-1 -e AWS_S3_BUCKET=... metrics-engine-dp-worker:latest
```

## CI/CD

GitHub Actions workflow runs:
- Code formatting check
- Linting
- Type checking
- Tests
- Docker build

## IAM Permissions

Minimum required permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::bucket/datasets/*",
        "arn:aws:s3:::bucket/metrics/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "sns:Publish"
      ],
      "Resource": "arn:aws:sns:region:account:topic.fifo"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility"
      ],
      "Resource": "arn:aws:sqs:region:account:queue.fifo"
    }
  ]
}
```

## License

[Your License Here]
