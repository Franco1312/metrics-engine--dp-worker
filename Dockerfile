FROM python:3.11-slim as builder

WORKDIR /build

RUN pip install --no-cache-dir poetry==1.7.1 poetry-plugin-export

COPY pyproject.toml poetry.lock* ./

# Generate requirements.txt - export only main dependencies
# If lock file exists, use it. Otherwise, we'll need to create it but skip dev deps
RUN if [ -f poetry.lock ]; then \
    poetry export -f requirements.txt --output requirements.txt --without-hashes --no-interaction --only main; \
    else \
    echo "ERROR: poetry.lock file is required. Please run 'poetry lock' locally and commit the file." && exit 1; \
    fi

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get purge -y gcc g++ && apt-get autoremove -y

COPY metrics_worker ./metrics_worker

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 9300

CMD ["python", "-m", "metrics_worker.infrastructure.runtime.main"]

