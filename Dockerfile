FROM python:3.11-slim as builder

WORKDIR /build

RUN pip install --no-cache-dir poetry==1.7.1

COPY pyproject.toml poetry.lock* ./

RUN poetry export -f requirements.txt --output requirements.txt --without-hashes || \
    poetry export -f requirements.txt --output requirements.txt --without-hashes --no-interaction || \
    echo "No poetry.lock found, will install from pyproject.toml"

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/requirements.txt* ./
COPY pyproject.toml ./
RUN if [ -f requirements.txt ]; then \
    pip install --no-cache-dir -r requirements.txt; \
    else \
    pip install --no-cache-dir poetry==1.7.1 && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --no-dev; \
    fi

RUN apt-get purge -y gcc g++ && apt-get autoremove -y

COPY metrics_worker ./metrics_worker

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 9300

CMD ["python", "-m", "metrics_worker.infrastructure.runtime.main"]

