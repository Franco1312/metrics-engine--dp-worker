"""Parquet reader with PyArrow."""

import structlog

import pyarrow.dataset as ds
from pyarrow import Table
from pyarrow.fs import S3FileSystem

from metrics_worker.domain.ports import DataReaderPort
from metrics_worker.domain.types import SeriesFrame
from metrics_worker.infrastructure.aws.s3_io import S3IO
from metrics_worker.infrastructure.aws.s3_path import S3Path
from metrics_worker.infrastructure.observability.metrics import s3_read_mb

logger = structlog.get_logger()


class ParquetReader(DataReaderPort):
    """Parquet reader with column pruning and predicate pushdown."""

    def __init__(self, s3_io: S3IO) -> None:
        """Initialize parquet reader."""
        self.s3_io = s3_io
        self.bucket = s3_io.bucket

    async def read_series_from_paths(
        self,
        parquet_paths: list[str],
        series_code: str,
    ) -> SeriesFrame:
        """Read series data from specific parquet file paths."""
        if not parquet_paths:
            raise ValueError(f"No parquet paths provided for series {series_code}")

        # PyArrow S3FileSystem expects paths in format: bucket/key (not s3://bucket/key)
        # So we need to construct paths as bucket/path
        pyarrow_paths = [
            f"{self.bucket}/{path.lstrip('/')}" for path in parquet_paths
        ]

        logger.info(
            "reading_series_from_paths",
            series_code=series_code,
            parquet_paths=parquet_paths,
            pyarrow_paths=pyarrow_paths,
            bucket=self.bucket,
        )

        filesystem = S3FileSystem()
        try:
            # Create dataset from multiple parquet files
            # PyArrow expects paths in format: bucket/key
            dataset = ds.dataset(
                pyarrow_paths,
                format="parquet",
                filesystem=filesystem,
            )
        except (OSError, FileNotFoundError, ValueError) as e:
            logger.error(
                "failed_to_open_dataset_from_paths",
                series_code=series_code,
                parquet_paths=parquet_paths,
                pyarrow_paths=pyarrow_paths,
                error=str(e),
            )
            raise ValueError(
                f"Series not found: {series_code} (failed to open dataset from paths): {e}"
            ) from e

        columns = ["obs_time", "value", "internal_series_code"]

        scanner = dataset.scanner(
            columns=columns,
            filter=ds.field("internal_series_code") == series_code,
        )

        table: Table = scanner.to_table()

        if len(table) == 0:
            available_series = self._list_available_series(dataset)
            error_msg = (
                f"Series not found: {series_code} "
                f"(searched in {len(parquet_paths)} parquet files). "
                f"Available series: {available_series[:20] if available_series else 'none found'}"
            )
            logger.error(
                "series_not_found_in_paths",
                series_code=series_code,
                parquet_paths=parquet_paths,
                pyarrow_paths=pyarrow_paths,
                available_count=len(available_series) if available_series else 0,
                available_series=available_series[:10] if available_series else [],
            )
            raise ValueError(error_msg)

        df = table.to_pandas()

        # Filter already applied in scanner, but double-check and select columns
        df = df[df["internal_series_code"] == series_code][["obs_time", "value"]].copy()
        df["obs_time"] = df["obs_time"].astype("datetime64[ns]")
        df["value"] = df["value"].astype("float64")
        df = df.sort_values("obs_time").reset_index(drop=True)

        size_mb = table.nbytes / (1024 * 1024)
        s3_read_mb.observe(size_mb)

        logger.info(
            "series_read_success_from_paths",
            series_code=series_code,
            row_count=len(df),
            size_mb=size_mb,
            parquet_files_count=len(parquet_paths),
        )

        return df

    def _list_available_series(self, dataset: ds.Dataset) -> list[str]:
        """List available series codes in the dataset."""
        try:
            # Read all internal_series_code values to see what's available
            scanner = dataset.scanner(columns=["internal_series_code"])
            table = scanner.to_table()
            if len(table) == 0:
                return []
            df = table.to_pandas()
            available = df["internal_series_code"].unique().tolist()
            return sorted(available)
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("failed_to_list_available_series", error=str(e))
            return []

