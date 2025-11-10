"""Catalog DTOs."""

from pydantic import BaseModel

from metrics_worker.domain.types import JsonValue


class DatasetManifest(BaseModel):
    """Dataset manifest structure."""

    outputs: dict[str, JsonValue]
    files: list[str] | None = None

