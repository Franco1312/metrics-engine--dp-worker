"""Catalog DTOs."""

from pydantic import BaseModel


class DateRange(BaseModel):
    """Date range structure in dataset manifest."""

    min_obs_time: str
    max_obs_time: str


class DatasetManifest(BaseModel):
    """Dataset manifest structure."""

    version_id: str
    dataset_id: str
    created_at: str
    collection_date: str
    data_points_count: int
    series_count: int
    series_codes: list[str]
    date_range: DateRange
    parquet_files: list[str]
    partitions: list[str]
    partition_strategy: str

