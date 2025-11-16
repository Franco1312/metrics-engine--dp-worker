FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./

# Install only main dependencies directly with pip
RUN pip install --no-cache-dir \
    pydantic>=2.5.0 \
    pydantic-settings>=2.1.0 \
    boto3>=1.34.0 \
    botocore>=1.34.0 \
    pyarrow>=15.0.0 \
    pandas>=2.1.0 \
    numpy>=1.26.0 \
    tenacity>=8.2.3 \
    structlog>=24.1.0 \
    prometheus-client>=0.19.0

RUN apt-get purge -y gcc g++ && apt-get autoremove -y

COPY metrics_worker ./metrics_worker

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 9300

CMD ["python", "-m", "metrics_worker.infrastructure.runtime.main"]

