FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir poetry==1.7.1

COPY pyproject.toml ./

# Install only main dependencies (no dev dependencies)
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-root --only main

RUN apt-get purge -y gcc g++ && apt-get autoremove -y

COPY metrics_worker ./metrics_worker

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 9300

CMD ["python", "-m", "metrics_worker.infrastructure.runtime.main"]

