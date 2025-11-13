"""Main entrypoint."""

import asyncio
import signal

import structlog

from metrics_worker.application.use_cases.handle_run_request import run as handle_run
from metrics_worker.infrastructure.aws.sns_publisher import SNSPublisher
from metrics_worker.infrastructure.aws.sqs_consumer import SQSConsumer
from metrics_worker.infrastructure.config.settings import Settings
from metrics_worker.infrastructure.io.parquet_reader import ParquetReader
from metrics_worker.infrastructure.io.parquet_writer import ParquetWriter
from metrics_worker.infrastructure.observability.logging import configure_logging
from metrics_worker.infrastructure.observability.metrics import (
    runs_failed,
    runs_started,
    runs_succeeded,
)
from metrics_worker.infrastructure.runtime.catalog_adapter import S3CatalogAdapter
from metrics_worker.infrastructure.runtime.clock import SystemClock
from metrics_worker.infrastructure.aws.s3_io import S3IO
from metrics_worker.infrastructure.aws.s3_path import S3Path
from metrics_worker.infrastructure.runtime.health import start_metrics_server

logger = structlog.get_logger()

shutdown_event = asyncio.Event()


def signal_handler() -> None:
    """Handle shutdown signal."""
    logger.info("shutdown_signal_received")
    shutdown_event.set()


async def main_loop() -> None:
    """Main event loop."""
    configure_logging()
    logger.info("worker_starting")

    settings = Settings()
    
    # Load AWS credentials from Settings to environment for boto3
    import os
    has_credentials = False
    if settings.aws_access_key_id:
        os.environ["AWS_ACCESS_KEY_ID"] = settings.aws_access_key_id
        has_credentials = True
    if settings.aws_secret_access_key:
        os.environ["AWS_SECRET_ACCESS_KEY"] = settings.aws_secret_access_key
        has_credentials = True
    if settings.aws_session_token:
        os.environ["AWS_SESSION_TOKEN"] = settings.aws_session_token
    
    # Check if credentials are available from environment or .env
    env_has_access_key = bool(os.environ.get("AWS_ACCESS_KEY_ID"))
    env_has_secret_key = bool(os.environ.get("AWS_SECRET_ACCESS_KEY"))
    
    logger.info(
        "settings_loaded",
        region=settings.aws_region,
        bucket=settings.aws_s3_bucket,
        sqs_queue_url=settings.aws_sqs_run_request_queue_url,
        sqs_queue_enabled=settings.aws_sqs_run_request_queue_enabled,
        credentials_from_settings=has_credentials,
        credentials_from_env=env_has_access_key and env_has_secret_key,
        has_access_key=env_has_access_key,
        has_secret_key=env_has_secret_key,
    )
    
    if not env_has_access_key or not env_has_secret_key:
        logger.warning(
            "aws_credentials_missing",
            message="AWS credentials not found. Make sure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set in .env or environment",
        )

    start_metrics_server(settings)

    s3_io = S3IO(settings)
    catalog = S3CatalogAdapter(s3_io)
    data_reader = ParquetReader(s3_io)
    output_writer = ParquetWriter(s3_io)
    event_bus = SNSPublisher(settings)
    clock = SystemClock()

    if not settings.aws_sqs_run_request_queue_enabled:
        logger.warning("sqs_queue_disabled")
        return

    sqs_consumer = SQSConsumer(settings)

    logger.info("worker_ready")

    while not shutdown_event.is_set():
        try:
            event, receipt_handle = await sqs_consumer.receive_message()

            if event is None:
                continue

            runs_started.inc()

            try:
                marker_path = S3Path.join("metrics", event.metric_code, "runs", f"{event.run_id}.ok")
                marker_exists = await output_writer.check_run_marker(marker_path)

                if marker_exists:
                    logger.info("run_already_completed", run_id=event.run_id, metric_code=event.metric_code)
                    await sqs_consumer.delete_message(receipt_handle)
                    continue

                await handle_run(
                    event,
                    catalog,
                    data_reader,
                    output_writer,
                    event_bus,
                    clock,
                )

                runs_succeeded.inc()
                await sqs_consumer.delete_message(receipt_handle)

            except Exception as e:
                runs_failed.labels(error_code="INTERNAL_ERROR").inc()
                logger.error("run_processing_error", exc_info=True, error=str(e))
                await sqs_consumer.delete_message(receipt_handle)

        except KeyboardInterrupt:
            logger.info("keyboard_interrupt")
            break
        except Exception as e:
            logger.error("main_loop_error", exc_info=True, error=str(e))
            await asyncio.sleep(5)

    logger.info("worker_shutting_down")


def main() -> None:
    """Entrypoint."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    try:
        loop.run_until_complete(main_loop())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()

