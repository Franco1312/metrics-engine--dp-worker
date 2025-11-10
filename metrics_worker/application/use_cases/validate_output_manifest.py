"""Validate output manifest."""

from metrics_worker.domain.entities import MetricOutputManifest
from metrics_worker.domain.errors import ManifestValidationError


async def run(
    manifest: MetricOutputManifest,
    expected_run_id: str,
    expected_metric_code: str,
) -> None:
    """Validate output manifest."""
    if manifest.run_id != expected_run_id:
        raise ManifestValidationError(
            f"Manifest run_id mismatch: {manifest.run_id} != {expected_run_id}",
        )

    if manifest.metric_code != expected_metric_code:
        raise ManifestValidationError(
            f"Manifest metric_code mismatch: {manifest.metric_code} != {expected_metric_code}",
        )

    if not manifest.version_ts:
        raise ManifestValidationError("Manifest missing version_ts")

    if manifest.row_count < 0:
        raise ManifestValidationError(f"Invalid row_count: {manifest.row_count}")

    if not manifest.outputs.get("data_prefix"):
        raise ManifestValidationError("Manifest missing outputs.data_prefix")

    files = manifest.outputs.get("files", [])
    if not files:
        raise ManifestValidationError("Manifest missing outputs.files")

