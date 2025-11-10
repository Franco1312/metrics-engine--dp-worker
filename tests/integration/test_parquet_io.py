"""Integration tests for Parquet I/O."""

import io
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from metrics_worker.infrastructure.io.parquet_reader import ParquetReader
from metrics_worker.infrastructure.io.parquet_writer import ParquetWriter


@pytest.fixture
def sample_data():
    """Create sample series data."""
    return pd.DataFrame(
        {
            "obs_time": pd.date_range("2024-01-01", periods=10, freq="D"),
            "value": [1.0 * i for i in range(10)],
            "internal_series_code": ["TEST_SERIES"] * 10,
        },
    )


def test_parquet_write_read_roundtrip(sample_data):
    """Test writing and reading Parquet."""
    import pyarrow as pa

    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = Path(tmpdir) / "test.parquet"
        df = pd.DataFrame(sample_data)
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table,
            str(parquet_path),
            compression="snappy",
        )

        table = pq.read_table(str(parquet_path))
        df = table.to_pandas()

        assert len(df) == 10
        assert "obs_time" in df.columns
        assert "value" in df.columns
        assert "internal_series_code" in df.columns

