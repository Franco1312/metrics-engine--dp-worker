"""JSONL writer."""

import io
import json

import pandas as pd

from metrics_worker.domain.entities import MetricOutputManifest
from metrics_worker.domain.ports import OutputWriterPort
from metrics_worker.domain.types import ManifestSerializationDict, RunMarkerDict, SeriesFrame
from metrics_worker.infrastructure.aws.s3_io import S3IO
from metrics_worker.infrastructure.aws.s3_path import S3Path
from metrics_worker.infrastructure.observability.metrics import s3_write_mb


class JsonlWriter(OutputWriterPort):
    """JSONL writer for metric outputs."""

    def __init__(self, s3_io: S3IO) -> None:
        """Initialize JSONL writer."""
        self.s3_io = s3_io

    async def write_jsonl(
        self,
        data: SeriesFrame,
        output_path: str,
    ) -> list[str]:
        """Write JSONL file(s) to S3."""
        if isinstance(data, pd.DataFrame):
            df = data
        else:
            # Convert PyArrow Table to DataFrame
            df = data.to_pandas()

        # Convert DataFrame to JSONL format
        # Each row becomes a JSON object on a single line
        buffer = io.StringIO()
        
        # Identify datetime columns for proper serialization
        datetime_columns = {
            col for col in df.columns 
            if pd.api.types.is_datetime64_any_dtype(df[col])
        }
        
        # Use orient='records' to get list of dicts, then write each as a line
        records = df.to_dict(orient='records')
        for record in records:
            # Convert any NaN/NaT values to None (null in JSON)
            cleaned_record = {
                k: (None if pd.isna(v) else v) 
                for k, v in record.items()
            }
            # Serialize datetime objects to ISO format strings
            for key in datetime_columns:
                if cleaned_record[key] is not None:
                    cleaned_record[key] = pd.Timestamp(cleaned_record[key]).isoformat()
            
            json_line = json.dumps(cleaned_record, ensure_ascii=False, default=str)
            buffer.write(json_line)
            buffer.write('\n')

        content = buffer.getvalue().encode('utf-8')

        await self.s3_io.put_object(output_path, content, "application/x-ndjson")

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

