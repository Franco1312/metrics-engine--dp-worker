.PHONY: help install fmt lint typecheck test test-cov run-local clean docker-build

help:
	@echo "Available targets:"
	@echo "  install      - Install dependencies"
	@echo "  fmt          - Format code with ruff"
	@echo "  lint         - Lint code with ruff"
	@echo "  typecheck    - Type check with mypy"
	@echo "  test         - Run tests"
	@echo "  test-cov     - Run tests with coverage"
	@echo "  run-local    - Run worker locally (requires env vars)"
	@echo "  clean        - Clean build artifacts"
	@echo "  docker-build - Build Docker image"

install:
	poetry install

fmt:
	poetry run ruff format metrics_worker tests
	poetry run ruff check --fix metrics_worker tests

lint:
	poetry run ruff check metrics_worker tests

typecheck:
	poetry run mypy metrics_worker

test:
	poetry run pytest -q --maxfail=1 -x

test-cov:
	poetry run pytest --cov=metrics_worker --cov-report=term-missing

run-local:
	poetry run python -m metrics_worker.infrastructure.runtime.main

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov dist *.egg-info
	find . -type d -name __pycache__ -exec rm -r {} +
	find . -type f -name "*.pyc" -delete

docker-build:
	docker build -t metrics-engine-dp-worker:latest .

