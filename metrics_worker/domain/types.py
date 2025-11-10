"""Domain types and aliases."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, TypedDict

import pandas as pd
import pyarrow as pa

Timestamp = datetime
SeriesFrame = pd.DataFrame | pa.Table
ExpressionResult = pd.DataFrame | pa.Table

# JSON-serializable types (recursive)
# Using TYPE_CHECKING to avoid circular reference issues
if TYPE_CHECKING:
    JsonValue = str | int | float | bool | None | dict[str, "JsonValue"] | list["JsonValue"]
else:
    # Runtime fallback - JSON values can be any JSON-serializable type
    JsonValue = str | int | float | bool | None | dict | list

# Expression JSON structure (recursive)
ExpressionJson = dict[str, JsonValue]

# Output manifest structure
class OutputManifestDict(TypedDict):
    """Output manifest dictionary structure."""
    data_prefix: str
    files: list[str]

# Catalog structure
class DatasetInfo(TypedDict):
    """Dataset information in catalog."""
    manifestPath: str

class CatalogDict(TypedDict):
    """Catalog dictionary structure."""
    datasets: dict[str, DatasetInfo]

# Dataset manifest structure (from S3)
class DatasetManifestDict(TypedDict, total=False):
    """Dataset manifest dictionary structure."""
    outputs: dict[str, JsonValue]
    files: list[str] | None

# Manifest serialization structure
class ManifestSerializationDict(TypedDict):
    """Manifest serialization dictionary structure."""
    run_id: str
    metric_code: str
    version_ts: str
    created_at: str
    row_count: int
    outputs: dict[str, JsonValue]

# Run marker structure
class RunMarkerDict(TypedDict):
    """Run marker dictionary structure."""
    run_id: str

