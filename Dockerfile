FROM python:3.12-slim

WORKDIR /app

# Install system dependencies needed for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Clean up build dependencies
RUN apt-get purge -y gcc g++ && apt-get autoremove -y

# Copy application code
COPY metrics_worker ./metrics_worker

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 9300

CMD ["python", "-m", "metrics_worker.infrastructure.runtime.main"]

