"""Unit tests for handle_run_request use case."""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from metrics_worker.application.dto.catalog import DatasetManifest, DateRange
from metrics_worker.application.services.planner import ReadPlan
from metrics_worker.application.use_cases.handle_run_request import (
    _calculate_output_paths,
    _read_all_series,
    _read_single_series,
)
from metrics_worker.domain.ports import CatalogPort, DataReaderPort


@pytest.fixture
def sample_series_frame():
    """Create sample series frame."""
    return pd.DataFrame(
        {
            "obs_time": pd.date_range("2024-01-01", periods=5, freq="D"),
            "value": [1.0, 2.0, 3.0, 4.0, 5.0],
        }
    )


@pytest.fixture
def sample_dataset_manifest():
    """Create sample dataset manifest."""
    return DatasetManifest(
        version_id="v20240101_120000",
        dataset_id="test-dataset",
        created_at="2024-01-01T12:00:00Z",
        collection_date="2024-01-01T11:00:00Z",
        data_points_count=100,
        series_count=2,
        series_codes=["SERIES_A", "SERIES_B"],
        date_range=DateRange(
            min_obs_time="2024-01-01T00:00:00Z",
            max_obs_time="2024-01-05T00:00:00Z",
        ),
        parquet_files=[
            "SERIES_A/year=2024/month=01/data.parquet",
            "SERIES_B/year=2024/month=01/data.parquet",
        ],
        partitions=[],
        partition_strategy="series_year_month",
    )


@pytest.fixture
def mock_catalog():
    """Create mock catalog."""
    catalog = MagicMock(spec=CatalogPort)
    catalog.get_dataset_manifest = AsyncMock()
    return catalog


@pytest.fixture
def mock_data_reader():
    """Create mock data reader."""
    reader = MagicMock(spec=DataReaderPort)
    reader.read_series_from_paths = AsyncMock()
    return reader


@pytest.mark.asyncio
async def test_read_single_series_success(
    mock_data_reader, sample_series_frame  # noqa: F811
):
    """Test reading a single series successfully."""
    mock_data_reader.read_series_from_paths.return_value = sample_series_frame

    result = await _read_single_series(
        series_code="SERIES_A",
        dataset_id="test-dataset",
        projections_path="datasets/test-dataset/projections",
        all_parquet_files=[
            "SERIES_A/year=2024/month=01/data.parquet",
            "SERIES_B/year=2024/month=01/data.parquet",
        ],
        data_reader=mock_data_reader,
    )

    assert len(result) == 5
    assert "obs_time" in result.columns
    assert "value" in result.columns
    mock_data_reader.read_series_from_paths.assert_called_once()
    call_args = mock_data_reader.read_series_from_paths.call_args
    assert "SERIES_A" in call_args[0][0][0]  # Check path contains series code
    assert call_args[0][1] == "SERIES_A"  # Check series_code parameter


@pytest.mark.asyncio
async def test_read_single_series_no_files_found(mock_data_reader):  # noqa: F811
    """Test reading a series when no parquet files are found."""
    with pytest.raises(ValueError, match="No parquet files found"):
        await _read_single_series(
            series_code="SERIES_C",
            dataset_id="test-dataset",
            projections_path="datasets/test-dataset/projections",
            all_parquet_files=[
                "SERIES_A/year=2024/month=01/data.parquet",
                "SERIES_B/year=2024/month=01/data.parquet",
            ],
            data_reader=mock_data_reader,
        )


@pytest.mark.asyncio
async def test_read_single_series_read_error(mock_data_reader):  # noqa: F811
    """Test reading a series when data reader raises an error."""
    mock_data_reader.read_series_from_paths.side_effect = ValueError("File not found")

    with pytest.raises(ValueError, match="File not found"):
        await _read_single_series(
            series_code="SERIES_A",
            dataset_id="test-dataset",
            projections_path="datasets/test-dataset/projections",
            all_parquet_files=["SERIES_A/year=2024/month=01/data.parquet"],
            data_reader=mock_data_reader,
        )


@pytest.mark.asyncio
async def test_read_all_series_single_dataset(
    mock_catalog, mock_data_reader, sample_series_frame  # noqa: F811
):
    """Test reading all series from a single dataset."""
    read_plan = ReadPlan()
    read_plan.add_series("test-dataset", "SERIES_A")
    read_plan.add_series("test-dataset", "SERIES_B")

    catalog_info = {
        "datasets": {
            "test-dataset": {
                "manifestPath": "datasets/test-dataset/manifest.json",
                "projectionsPath": "datasets/test-dataset/projections",
            }
        }
    }

    mock_catalog.get_dataset_manifest.return_value = {
        "version_id": "v20240101_120000",
        "dataset_id": "test-dataset",
        "created_at": "2024-01-01T12:00:00Z",
        "collection_date": "2024-01-01T11:00:00Z",
        "data_points_count": 100,
        "series_count": 2,
        "series_codes": ["SERIES_A", "SERIES_B"],
        "date_range": {
            "min_obs_time": "2024-01-01T00:00:00Z",
            "max_obs_time": "2024-01-05T00:00:00Z",
        },
        "parquet_files": [
            "SERIES_A/year=2024/month=01/data.parquet",
            "SERIES_B/year=2024/month=01/data.parquet",
        ],
        "partitions": [],
        "partition_strategy": "series_year_month",
    }

    mock_data_reader.read_series_from_paths.return_value = sample_series_frame

    result = await _read_all_series(
        read_plan=read_plan,
        catalog_info=catalog_info,
        catalog=mock_catalog,
        data_reader=mock_data_reader,
    )

    assert len(result) == 2
    assert "SERIES_A" in result
    assert "SERIES_B" in result
    assert len(result["SERIES_A"]) == 5
    assert len(result["SERIES_B"]) == 5
    # Should read manifest once
    assert mock_catalog.get_dataset_manifest.call_count == 1
    # Should read both series (in parallel)
    assert mock_data_reader.read_series_from_paths.call_count == 2


@pytest.mark.asyncio
async def test_read_all_series_multiple_datasets(
    mock_catalog, mock_data_reader, sample_series_frame  # noqa: F811
):
    """Test reading series from multiple datasets."""
    read_plan = ReadPlan()
    read_plan.add_series("dataset1", "SERIES_A")
    read_plan.add_series("dataset2", "SERIES_B")

    catalog_info = {
        "datasets": {
            "dataset1": {
                "manifestPath": "datasets/dataset1/manifest.json",
                "projectionsPath": "datasets/dataset1/projections",
            },
            "dataset2": {
                "manifestPath": "datasets/dataset2/manifest.json",
                "projectionsPath": "datasets/dataset2/projections",
            },
        }
    }

    def get_manifest_side_effect(path: str):
        if "dataset1" in path:
            return {
                "version_id": "v1",
                "dataset_id": "dataset1",
                "created_at": "2024-01-01T12:00:00Z",
                "collection_date": "2024-01-01T11:00:00Z",
                "data_points_count": 50,
                "series_count": 1,
                "series_codes": ["SERIES_A"],
                "date_range": {
                    "min_obs_time": "2024-01-01T00:00:00Z",
                    "max_obs_time": "2024-01-05T00:00:00Z",
                },
                "parquet_files": ["SERIES_A/year=2024/month=01/data.parquet"],
                "partitions": [],
                "partition_strategy": "series_year_month",
            }
        else:
            return {
                "version_id": "v2",
                "dataset_id": "dataset2",
                "created_at": "2024-01-01T12:00:00Z",
                "collection_date": "2024-01-01T11:00:00Z",
                "data_points_count": 50,
                "series_count": 1,
                "series_codes": ["SERIES_B"],
                "date_range": {
                    "min_obs_time": "2024-01-01T00:00:00Z",
                    "max_obs_time": "2024-01-05T00:00:00Z",
                },
                "parquet_files": ["SERIES_B/year=2024/month=01/data.parquet"],
                "partitions": [],
                "partition_strategy": "series_year_month",
            }

    mock_catalog.get_dataset_manifest.side_effect = get_manifest_side_effect
    mock_data_reader.read_series_from_paths.return_value = sample_series_frame

    result = await _read_all_series(
        read_plan=read_plan,
        catalog_info=catalog_info,
        catalog=mock_catalog,
        data_reader=mock_data_reader,
    )

    assert len(result) == 2
    assert "SERIES_A" in result
    assert "SERIES_B" in result
    # Should read both manifests (in parallel)
    assert mock_catalog.get_dataset_manifest.call_count == 2
    # Should read both series (in parallel)
    assert mock_data_reader.read_series_from_paths.call_count == 2


@pytest.mark.asyncio
async def test_read_all_series_error_propagation(
    mock_catalog, mock_data_reader  # noqa: F811
):
    """Test that errors in reading series are properly propagated."""
    read_plan = ReadPlan()
    read_plan.add_series("test-dataset", "SERIES_A")

    catalog_info = {
        "datasets": {
            "test-dataset": {
                "manifestPath": "datasets/test-dataset/manifest.json",
                "projectionsPath": "datasets/test-dataset/projections",
            }
        }
    }

    mock_catalog.get_dataset_manifest.return_value = {
        "version_id": "v1",
        "dataset_id": "test-dataset",
        "created_at": "2024-01-01T12:00:00Z",
        "collection_date": "2024-01-01T11:00:00Z",
        "data_points_count": 50,
        "series_count": 1,
        "series_codes": ["SERIES_A"],
        "date_range": {
            "min_obs_time": "2024-01-01T00:00:00Z",
            "max_obs_time": "2024-01-05T00:00:00Z",
        },
        "parquet_files": ["SERIES_A/year=2024/month=01/data.parquet"],
        "partitions": [],
        "partition_strategy": "series_year_month",
    }

    mock_data_reader.read_series_from_paths.side_effect = ValueError("Read error")

    with pytest.raises(ValueError, match="Read error"):
        await _read_all_series(
            read_plan=read_plan,
            catalog_info=catalog_info,
            catalog=mock_catalog,
            data_reader=mock_data_reader,
        )


def test_calculate_output_paths():
    """Test calculating output paths."""
    base_path = "s3://bucket/metrics/test-metric"
    version_ts = "v20240101_120000"
    run_id = "test-run-id"

    paths = _calculate_output_paths(base_path, version_ts, run_id)

    assert "v20240101_120000" in paths.parquet_path
    assert "metrics.parquet" in paths.parquet_path
    assert "v20240101_120000" in paths.manifest_path
    assert "manifest.json" in paths.manifest_path
    assert "current" in paths.current_manifest_path
    assert "runs" in paths.marker_path
    assert run_id in paths.marker_path
    assert paths.manifest_relative_path == paths.manifest_path

