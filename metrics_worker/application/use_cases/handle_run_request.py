"""Handle metric run request - main orchestration."""

import pandas as pd
import structlog

from metrics_worker.application.dto.catalog import DatasetManifest
from metrics_worker.application.dto.events import MetricRunRequestedEvent
from metrics_worker.application.services.expression_eval import evaluate_expression
from metrics_worker.application.services.planner import ReadPlan, plan_reads
from metrics_worker.application.use_cases.build_output_manifest import run as build_manifest
from metrics_worker.application.use_cases.publish_completed import (
    run_failure,
    run_success,
)
from metrics_worker.application.use_cases.publish_started import run as publish_started
from metrics_worker.application.use_cases.validate_output_manifest import (
    run as validate_manifest,
)
from metrics_worker.domain.entities import MetricOutputManifest
from metrics_worker.domain.ports import (
    CatalogPort,
    ClockPort,
    DataReaderPort,
    EventBusPort,
    OutputWriterPort,
)
from metrics_worker.domain.types import SeriesFrame
from metrics_worker.infrastructure.aws.s3_path import S3Path

logger = structlog.get_logger()


async def run(
    event: MetricRunRequestedEvent,
    catalog: CatalogPort,
    data_reader: DataReaderPort,
    output_writer: OutputWriterPort,
    event_bus: EventBusPort,
    clock: ClockPort,
) -> None:
    """Handle metric run request."""
    run_id = event.run_id
    metric_code = event.metric_code

    try:
        logger.info("processing_run", run_id=run_id, metric_code=metric_code)

        await publish_started(run_id, metric_code, event_bus, clock)

        read_plan = plan_reads(
            event.expression_json,
            event.expression_type,
            event.inputs,
        )

        series_data = await _read_series_data(
            read_plan,
            event.catalog,
            catalog,
            data_reader,
        )

        result_df = evaluate_expression(
            event.expression_json,
            event.expression_type,
            series_data,
        )

        row_count = _get_row_count(result_df)
        version_ts = clock.format_version_ts(clock.now())

        output_paths = _calculate_output_paths(event.output["basePath"], version_ts, run_id)

        manifest = await _write_results(
            result_df,
            run_id,
            metric_code,
            version_ts,
            row_count,
            output_paths,
            output_writer,
            clock,
        )

        await validate_manifest(manifest, run_id, metric_code)

        await output_writer.create_run_marker(output_paths.marker_path)

        await run_success(
            run_id,
            metric_code,
            version_ts,
            output_paths.manifest_relative_path,
            row_count,
            event_bus,
        )

        logger.info("run_completed", run_id=run_id, status="SUCCESS", row_count=row_count)

    except Exception as e:
        error_code, error_message = _classify_error(e)
        logger.error(
            "run_failed",
            run_id=run_id,
            metric_code=metric_code,
            error_code=error_code,
            error_message=error_message,
            exc_info=True,
        )
        await run_failure(run_id, metric_code, error_code, error_message, event_bus)


async def _read_series_data(
    read_plan: ReadPlan,
    catalog_info: dict,
    catalog: CatalogPort,
    data_reader: DataReaderPort,
) -> dict[str, SeriesFrame]:
    """Read all series data according to the read plan."""
    series_data: dict[str, SeriesFrame] = {}
    dataset_manifests: dict[str, DatasetManifest] = {}

    for dataset_id, series_codes in read_plan.series_by_dataset.items():
        dataset_info = catalog_info["datasets"][dataset_id]
        manifest_path = dataset_info["manifestPath"]

        logger.info(
            "reading_dataset_series",
            dataset_id=dataset_id,
            manifest_path=manifest_path,
            series_codes=series_codes,
        )

        if dataset_id not in dataset_manifests:
            manifest_dict = await catalog.get_dataset_manifest(manifest_path)
            dataset_manifests[dataset_id] = DatasetManifest(**manifest_dict)

        dataset_manifest = dataset_manifests[dataset_id]
        data_prefix = dataset_manifest.outputs["data_prefix"]

        logger.info(
            "reading_series_from_dataset",
            dataset_id=dataset_id,
            data_prefix=data_prefix,
            series_count=len(series_codes),
        )

        for series_code in series_codes:
            try:
                series_df = await data_reader.read_series(data_prefix, series_code)
                series_data[series_code] = series_df
                logger.info(
                    "series_read_success",
                    series_code=series_code,
                    dataset_id=dataset_id,
                    row_count=len(series_df),
                )
            except (ValueError, OSError) as e:
                logger.error(
                    "failed_to_read_series",
                    series_code=series_code,
                    dataset_id=dataset_id,
                    data_prefix=data_prefix,
                    error=str(e),
                )
                raise

    return series_data


def _get_row_count(result_df: pd.DataFrame) -> int:
    """Get row count from result dataframe."""
    return len(result_df)


class _OutputPaths:
    """Output paths for metric run results."""

    def __init__(
        self,
        parquet_path: str,
        manifest_path: str,
        current_manifest_path: str,
        marker_path: str,
        manifest_relative_path: str,
    ) -> None:
        """Initialize output paths."""
        self.parquet_path = parquet_path
        self.manifest_path = manifest_path
        self.current_manifest_path = current_manifest_path
        self.marker_path = marker_path
        self.manifest_relative_path = manifest_relative_path


def _calculate_output_paths(base_path: str, version_ts: str, run_id: str) -> _OutputPaths:
    """Calculate all output paths for metric run."""
    normalized_path = S3Path.normalize(base_path)
    prefix = S3Path.rstrip_separator(normalized_path)

    parquet_path = S3Path.join(prefix, version_ts, "data", "metrics.parquet")
    manifest_path = S3Path.join(prefix, version_ts, "manifest.json")
    current_manifest_path = S3Path.join(prefix, "current", "manifest.json")
    marker_path = S3Path.join(prefix, "runs", f"{run_id}.ok")
    manifest_relative_path = manifest_path

    return _OutputPaths(
        parquet_path=parquet_path,
        manifest_path=manifest_path,
        current_manifest_path=current_manifest_path,
        marker_path=marker_path,
        manifest_relative_path=manifest_relative_path,
    )


async def _write_results(
    result_df: pd.DataFrame,
    run_id: str,
    metric_code: str,
    version_ts: str,
    row_count: int,
    output_paths: _OutputPaths,
    output_writer: OutputWriterPort,
    clock: ClockPort,
) -> MetricOutputManifest:
    """Write results to S3 (Parquet and manifest)."""
    output_files = await output_writer.write_parquet(
        result_df,
        output_paths.parquet_path,
        compression="snappy",
    )

    manifest_data_prefix = S3Path.parent(output_paths.manifest_path)
    manifest = await build_manifest(
        run_id,
        metric_code,
        version_ts,
        row_count,
        output_files,
        manifest_data_prefix,
        clock,
    )

    await output_writer.write_manifest(manifest, output_paths.manifest_path)
    await output_writer.write_manifest(manifest, output_paths.current_manifest_path)

    return manifest


def _classify_error(error: Exception) -> tuple[str, str]:
    """Classify error and return error code and message."""
    error_message = str(error)

    if "Series not found" in error_message:
        return "INPUT_READ_ERROR", error_message
    if "Expression" in error_message or "Invalid" in error_message:
        return "EXPRESSION_EVAL_ERROR", error_message
    if "Manifest" in error_message or "validation" in error_message.lower():
        return "MANIFEST_VALIDATION_ERROR", error_message
    if "S3" in error_message or "write" in error_message.lower():
        return "OUTPUT_WRITE_ERROR", error_message

    return "INTERNAL_ERROR", error_message
