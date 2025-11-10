"""Parquet writer."""

import io

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from metrics_worker.domain.entities import MetricOutputManifest
from metrics_worker.domain.ports import OutputWriterPort
from metrics_worker.domain.types import ManifestSerializationDict, RunMarkerDict, SeriesFrame
from metrics_worker.infrastructure.aws.s3_io import S3IO
from metrics_worker.infrastructure.aws.s3_path import S3Path
from metrics_worker.infrastructure.observability.metrics import s3_write_mb


class ParquetWriter(OutputWriterPort):
    """Parquet writer."""

    def __init__(self, s3_io: S3IO) -> None:
        """Initialize parquet writer."""
        self.s3_io = s3_io

    async def write_parquet(
        self,
        data: SeriesFrame,
        output_path: str,
        compression: str = "snappy",
    ) -> list[str]:
        """Write parquet file(s) to S3."""
        if isinstance(data, pd.DataFrame):
            table = pa.Table.from_pandas(data)
        else:
            table = data

        buffer = io.BytesIO()
        pq.write_table(
            table,
            buffer,
            compression=compression,
            use_dictionary=True,
        )

        buffer.seek(0)
        content = buffer.read()

        await self.s3_io.put_object(output_path, content, "application/x-parquet")

        size_mb = len(content) / (1024 * 1024)
        s3_write_mb.observe(size_mb)

        return [S3Path.basename(output_path)]

    async def write_manifest(
        self,
        manifest: MetricOutputManifest,
        manifest_path: str,
    ) -> None:
        """Write output manifest to S3."""
        manifest_dict: ManifestSerializationDict = {
            "run_id": manifest.run_id,
            "metric_code": manifest.metric_code,
            "version_ts": manifest.version_ts,
            "created_at": manifest.created_at.isoformat() + "Z",
            "row_count": manifest.row_count,
            "outputs": manifest.outputs,
        }

        await self.s3_io.put_json(manifest_path, manifest_dict)

    async def check_run_marker(self, marker_path: str) -> bool:
        """Check if run marker exists."""
        return await self.s3_io.object_exists(marker_path)

    async def create_run_marker(self, marker_path: str) -> None:
        """Create run marker."""
        run_id = S3Path.stem(marker_path)
        marker_dict: RunMarkerDict = {"run_id": run_id}
        await self.s3_io.put_json(marker_path, marker_dict)

