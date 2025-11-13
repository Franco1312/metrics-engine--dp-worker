"""Unit tests for ParquetReader."""

from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from pyarrow.fs import S3FileSystem

from metrics_worker.infrastructure.aws.s3_io import S3IO
from metrics_worker.infrastructure.io.parquet_reader import ParquetReader


@pytest.fixture
def mock_s3_io():
    """Create mock S3IO."""
    s3_io = MagicMock(spec=S3IO)
    s3_io.bucket = "test-bucket"
    return s3_io


@pytest.fixture
def sample_series_data():
    """Create sample series data."""
    return pd.DataFrame(
        {
            "obs_time": pd.date_range("2024-01-01", periods=5, freq="D"),
            "value": [1.0, 2.0, 3.0, 4.0, 5.0],
            "internal_series_code": ["TEST_SERIES"] * 5,
        }
    )


@pytest.fixture
def parquet_reader(mock_s3_io):
    """Create ParquetReader instance."""
    return ParquetReader(mock_s3_io)


@pytest.mark.asyncio
async def test_read_series_from_paths_success(
    parquet_reader, sample_series_data
):
    """Test reading series from specific paths successfully."""
    parquet_paths = [
        "datasets/test-dataset/projections/TEST_SERIES/year=2024/month=01/data.parquet"
    ]

    # Create a mock dataset and scanner
    mock_table = pa.Table.from_pandas(sample_series_data)
    mock_scanner = MagicMock()
    mock_scanner.to_table.return_value = mock_table

    mock_dataset = MagicMock()
    mock_dataset.scanner.return_value = mock_scanner

    with patch("pyarrow.fs.S3FileSystem") as mock_fs_class, patch(
        "pyarrow.dataset.dataset"
    ) as mock_dataset_func:
        mock_fs_instance = MagicMock()
        mock_fs_class.return_value = mock_fs_instance
        mock_dataset_func.return_value = mock_dataset

        result = await parquet_reader.read_series_from_paths(
            parquet_paths, "TEST_SERIES"
        )

        assert len(result) == 5
        assert "obs_time" in result.columns
        assert "value" in result.columns
        assert "internal_series_code" not in result.columns
        assert result["obs_time"].dtype == "datetime64[ns]"
        assert result["value"].dtype == "float64"

        # Verify dataset was created with correct paths
        call_args = mock_dataset_func.call_args
        assert call_args[0][0] == [
            "test-bucket/datasets/test-dataset/projections/TEST_SERIES/year=2024/month=01/data.parquet"
        ]

        # Verify scanner was called with correct filter
        scanner_call = mock_dataset.scanner.call_args
        assert scanner_call[1]["columns"] == ["obs_time", "value", "internal_series_code"]


@pytest.mark.asyncio
async def test_read_series_from_paths_multiple_files(
    parquet_reader, sample_series_data
):
    """Test reading series from multiple parquet files."""
    parquet_paths = [
        "datasets/test-dataset/projections/TEST_SERIES/year=2024/month=01/data.parquet",
        "datasets/test-dataset/projections/TEST_SERIES/year=2024/month=02/data.parquet",
    ]

    # Create combined data
    combined_data = pd.concat([sample_series_data, sample_series_data]).reset_index(
        drop=True
    )
    mock_table = pa.Table.from_pandas(combined_data)
    mock_scanner = MagicMock()
    mock_scanner.to_table.return_value = mock_table

    mock_dataset = MagicMock()
    mock_dataset.scanner.return_value = mock_scanner

    with patch("pyarrow.fs.S3FileSystem") as mock_fs_class, patch(
        "pyarrow.dataset.dataset"
    ) as mock_dataset_func:
        mock_fs_class.return_value = MagicMock()
        mock_dataset_func.return_value = mock_dataset

        result = await parquet_reader.read_series_from_paths(
            parquet_paths, "TEST_SERIES"
        )

        assert len(result) == 10
        # Verify both paths were passed
        call_args = mock_dataset_func.call_args
        assert len(call_args[0][0]) == 2


@pytest.mark.asyncio
async def test_read_series_from_paths_empty_paths(parquet_reader):
    """Test reading series with empty paths list."""
    with pytest.raises(ValueError, match="No parquet paths provided"):
        await parquet_reader.read_series_from_paths([], "TEST_SERIES")


@pytest.mark.asyncio
async def test_read_series_from_paths_series_not_found(parquet_reader):
    """Test reading series when series is not found in files."""
    parquet_paths = [
        "datasets/test-dataset/projections/OTHER_SERIES/year=2024/month=01/data.parquet"
    ]

    # Create empty table
    mock_table = pa.Table.from_pandas(
        pd.DataFrame({"internal_series_code": [], "obs_time": [], "value": []})
    )
    mock_scanner = MagicMock()
    mock_scanner.to_table.return_value = mock_table

    mock_dataset = MagicMock()
    mock_dataset.scanner.return_value = mock_scanner

    # Mock _list_available_series to return empty list
    with patch("pyarrow.fs.S3FileSystem") as mock_fs_class, patch(
        "pyarrow.dataset.dataset"
    ) as mock_dataset_func:
        mock_fs_class.return_value = MagicMock()
        mock_dataset_func.return_value = mock_dataset

        with pytest.raises(ValueError, match="Series not found: TEST_SERIES"):
            await parquet_reader.read_series_from_paths(parquet_paths, "TEST_SERIES")


@pytest.mark.asyncio
async def test_read_series_from_paths_dataset_error(parquet_reader):
    """Test reading series when dataset creation fails."""
    parquet_paths = [
        "datasets/test-dataset/projections/TEST_SERIES/year=2024/month=01/data.parquet"
    ]

    with patch("pyarrow.fs.S3FileSystem") as mock_fs_class, patch(
        "pyarrow.dataset.dataset"
    ) as mock_dataset_func:
        mock_fs_class.return_value = MagicMock()
        mock_dataset_func.side_effect = OSError("File not found")

        with pytest.raises(ValueError, match="failed to open dataset from paths"):
            await parquet_reader.read_series_from_paths(parquet_paths, "TEST_SERIES")


@pytest.mark.asyncio
async def test_read_series_from_paths_path_normalization(parquet_reader, sample_series_data):
    """Test that paths are normalized correctly (removing leading slashes)."""
    parquet_paths = [
        "/datasets/test-dataset/projections/TEST_SERIES/year=2024/month=01/data.parquet"
    ]

    mock_table = pa.Table.from_pandas(sample_series_data)
    mock_scanner = MagicMock()
    mock_scanner.to_table.return_value = mock_table

    mock_dataset = MagicMock()
    mock_dataset.scanner.return_value = mock_scanner

    with patch("pyarrow.fs.S3FileSystem") as mock_fs_class, patch(
        "pyarrow.dataset.dataset"
    ) as mock_dataset_func:
        mock_fs_class.return_value = MagicMock()
        mock_dataset_func.return_value = mock_dataset

        await parquet_reader.read_series_from_paths(parquet_paths, "TEST_SERIES")

        # Verify path doesn't have leading slash
        call_args = mock_dataset_func.call_args
        assert not call_args[0][0][0].startswith("/")
        assert call_args[0][0][0].startswith("test-bucket/")


@pytest.mark.asyncio
async def test_read_series_from_paths_sorted_output(parquet_reader):
    """Test that output is sorted by obs_time."""
    # Create unsorted data
    unsorted_data = pd.DataFrame(
        {
            "obs_time": pd.to_datetime(
                ["2024-01-05", "2024-01-01", "2024-01-03", "2024-01-02", "2024-01-04"]
            ),
            "value": [5.0, 1.0, 3.0, 2.0, 4.0],
            "internal_series_code": ["TEST_SERIES"] * 5,
        }
    )

    parquet_paths = [
        "datasets/test-dataset/projections/TEST_SERIES/year=2024/month=01/data.parquet"
    ]

    mock_table = pa.Table.from_pandas(unsorted_data)
    mock_scanner = MagicMock()
    mock_scanner.to_table.return_value = mock_table

    mock_dataset = MagicMock()
    mock_dataset.scanner.return_value = mock_scanner

    with patch("pyarrow.fs.S3FileSystem") as mock_fs_class, patch(
        "pyarrow.dataset.dataset"
    ) as mock_dataset_func:
        mock_fs_class.return_value = MagicMock()
        mock_dataset_func.return_value = mock_dataset

        result = await parquet_reader.read_series_from_paths(
            parquet_paths, "TEST_SERIES"
        )

        # Verify data is sorted
        assert result["obs_time"].is_monotonic_increasing
        assert result["obs_time"].iloc[0] == pd.Timestamp("2024-01-01")
        assert result["obs_time"].iloc[-1] == pd.Timestamp("2024-01-05")

