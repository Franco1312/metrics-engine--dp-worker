#!/bin/bash
# Script para ejecutar el worker localmente
# Carga variables del .env si existe, y usa valores por defecto solo si no están definidas

set -e

# Activar entorno virtual
source venv/bin/activate

# Cargar variables del .env si existe (sin sobrescribir variables ya definidas)
if [ -f .env ]; then
    # Exportar variables del .env que no estén ya definidas
    set -a
    source .env
    set +a
    echo "✓ Variables cargadas desde .env"
fi

# Solo exportar variables con valores por defecto si no están ya definidas
# AWS Configuration
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_S3_BUCKET="${AWS_S3_BUCKET:-test-bucket}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"

# SQS Configuration
export AWS_SQS_RUN_REQUEST_QUEUE_URL="${AWS_SQS_RUN_REQUEST_QUEUE_URL:-https://sqs.us-east-1.amazonaws.com/123456789012/metric-run-request.fifo}"
export AWS_SQS_RUN_REQUEST_QUEUE_ENABLED="${AWS_SQS_RUN_REQUEST_QUEUE_ENABLED:-true}"
export AWS_SQS_VISIBILITY_TIMEOUT_SECONDS="${AWS_SQS_VISIBILITY_TIMEOUT_SECONDS:-300}"  # 5 minutes default
export AWS_SQS_VISIBILITY_TIMEOUT_EXTENSION_SECONDS="${AWS_SQS_VISIBILITY_TIMEOUT_EXTENSION_SECONDS:-60}"  # Extend by 1 minute

# SNS Topics (all required)
export AWS_SNS_METRIC_RUN_STARTED_TOPIC_ARN="${AWS_SNS_METRIC_RUN_STARTED_TOPIC_ARN:-arn:aws:sns:us-east-1:123456789012:metric-run-started.fifo}"
export AWS_SNS_METRIC_RUN_HEARTBEAT_TOPIC_ARN="${AWS_SNS_METRIC_RUN_HEARTBEAT_TOPIC_ARN:-arn:aws:sns:us-east-1:123456789012:metric-run-heartbeat.fifo}"
export AWS_SNS_METRIC_RUN_COMPLETED_TOPIC_ARN="${AWS_SNS_METRIC_RUN_COMPLETED_TOPIC_ARN:-arn:aws:sns:us-east-1:123456789012:metric-run-completed.fifo}"

# Worker Configuration
export WORKER_HEARTBEAT_INTERVAL_SECONDS="${WORKER_HEARTBEAT_INTERVAL_SECONDS:-30}"
export OUTPUT_COMPRESSION="${OUTPUT_COMPRESSION:-snappy}"
export PROMETHEUS_PORT="${PROMETHEUS_PORT:-9300}"

echo "🚀 Iniciando metrics-engine-dp-worker..."
echo "   AWS_REGION: $AWS_REGION"
echo "   AWS_S3_BUCKET: $AWS_S3_BUCKET"
echo "   AWS_SQS_QUEUE: ${AWS_SQS_RUN_REQUEST_QUEUE_URL##*/}"  # Solo muestra el nombre de la cola
echo "   PROMETHEUS_PORT: $PROMETHEUS_PORT"
if [ -n "$AWS_ACCESS_KEY_ID" ]; then
    echo "   AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:0:8}..."  # Solo muestra primeros 8 caracteres
fi
echo ""

# Ejecutar el worker
python -m metrics_worker.infrastructure.runtime.main

