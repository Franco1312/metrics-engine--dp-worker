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
    projectionsPath: str

class CatalogDict(TypedDict):
    """Catalog dictionary structure."""
    datasets: dict[str, DatasetInfo]

# Dataset manifest structure (from S3)
class DateRangeDict(TypedDict):
    """Date range structure in dataset manifest."""
    min_obs_time: str
    max_obs_time: str


class DatasetManifestDict(TypedDict, total=False):
    """Dataset manifest dictionary structure."""
    version_id: str
    dataset_id: str
    created_at: str
    collection_date: str
    data_points_count: int
    series_count: int
    series_codes: list[str]
    date_range: DateRangeDict
    parquet_files: list[str]
    partitions: list[str]
    partition_strategy: str

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

