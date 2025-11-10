#!/bin/bash
# Script para ejecutar el worker localmente con variables de entorno de ejemplo

set -e

# Activar entorno virtual
source venv/bin/activate

# Variables de entorno de ejemplo (ajustar según necesidad)
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_S3_BUCKET="${AWS_S3_BUCKET:-test-bucket}"
export AWS_SQS_RUN_REQUEST_QUEUE_URL="${AWS_SQS_RUN_REQUEST_QUEUE_URL:-https://sqs.us-east-1.amazonaws.com/123456789012/metric-run-request.fifo}"
export AWS_SQS_RUN_REQUEST_QUEUE_ENABLED="${AWS_SQS_RUN_REQUEST_QUEUE_ENABLED:-true}"
export AWS_SNS_EVENTS_TOPIC_ARN="${AWS_SNS_EVENTS_TOPIC_ARN:-arn:aws:sns:us-east-1:123456789012:metric-run-events.fifo}"
export WORKER_HEARTBEAT_ENABLED="${WORKER_HEARTBEAT_ENABLED:-true}"
export WORKER_HEARTBEAT_INTERVAL_SECONDS="${WORKER_HEARTBEAT_INTERVAL_SECONDS:-30}"
export OUTPUT_COMPRESSION="${OUTPUT_COMPRESSION:-snappy}"
export PROMETHEUS_PORT="${PROMETHEUS_PORT:-9300}"

echo "🚀 Iniciando metrics-engine-dp-worker..."
echo "   AWS_REGION: $AWS_REGION"
echo "   AWS_S3_BUCKET: $AWS_S3_BUCKET"
echo "   PROMETHEUS_PORT: $PROMETHEUS_PORT"
echo ""

# Ejecutar el worker
python -m metrics_worker.infrastructure.runtime.main

