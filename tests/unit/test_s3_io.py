"""Unit tests for S3 I/O operations."""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from metrics_worker.infrastructure.aws.s3_io import S3IO


@pytest.fixture
def mock_settings():
    """Create mock settings."""
    settings = MagicMock()
    settings.aws_region = "us-east-1"
    settings.aws_s3_bucket = "test-bucket"
    return settings


@pytest.fixture
def s3_io(mock_settings):
    """Create S3IO instance."""
    with patch("metrics_worker.infrastructure.aws.s3_io.boto3") as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        s3 = S3IO(mock_settings)
        s3.s3_client = mock_client
        return s3


@pytest.mark.asyncio
async def test_get_json_success(s3_io):
    """Test getting JSON from S3 successfully."""
    expected_data = {"key": "value"}
    mock_response = {"Body": MagicMock()}
    mock_response["Body"].read.return_value = b'{"key": "value"}'

    s3_io.s3_client.get_object = MagicMock(return_value=mock_response)

    result = await s3_io.get_json("test-key.json")

    assert result == expected_data
    s3_io.s3_client.get_object.assert_called_once_with(Bucket="test-bucket", Key="test-key.json")


@pytest.mark.asyncio
async def test_get_json_client_error(s3_io):
    """Test getting JSON from S3 with ClientError."""
    from tenacity import RetryError

    s3_io.s3_client.get_object = MagicMock(side_effect=ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject"))

    with pytest.raises((RuntimeError, RetryError), match="Failed to read S3 object|RetryError"):
        await s3_io.get_json("test-key.json")


@pytest.mark.asyncio
async def test_put_json_success(s3_io):
    """Test putting JSON to S3 successfully."""
    data = {"key": "value"}

    s3_io.s3_client.put_object = MagicMock()

    await s3_io.put_json("test-key.json", data)

    s3_io.s3_client.put_object.assert_called_once()
    call_kwargs = s3_io.s3_client.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "test-bucket"
    assert call_kwargs["Key"] == "test-key.json"
    assert call_kwargs["ContentType"] == "application/json"


@pytest.mark.asyncio
async def test_put_json_client_error(s3_io):
    """Test putting JSON to S3 with ClientError."""
    from tenacity import RetryError

    data = {"key": "value"}
    s3_io.s3_client.put_object = MagicMock(side_effect=ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject"))

    with pytest.raises((RuntimeError, RetryError), match="Failed to write S3 object|RetryError"):
        await s3_io.put_json("test-key.json", data)


@pytest.mark.asyncio
async def test_put_object_success(s3_io):
    """Test putting object to S3 successfully."""
    content = b"test content"
    content_type = "text/plain"

    s3_io.s3_client.put_object = MagicMock()

    await s3_io.put_object("test-key.txt", content, content_type)

    s3_io.s3_client.put_object.assert_called_once()
    call_kwargs = s3_io.s3_client.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "test-bucket"
    assert call_kwargs["Key"] == "test-key.txt"
    assert call_kwargs["Body"] == content
    assert call_kwargs["ContentType"] == content_type


@pytest.mark.asyncio
async def test_put_object_client_error(s3_io):
    """Test putting object to S3 with ClientError."""
    from tenacity import RetryError

    content = b"test content"
    s3_io.s3_client.put_object = MagicMock(side_effect=ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject"))

    with pytest.raises((RuntimeError, RetryError), match="Failed to write S3 object|RetryError"):
        await s3_io.put_object("test-key.txt", content, "text/plain")


@pytest.mark.asyncio
async def test_object_exists_true(s3_io):
    """Test checking if object exists (True)."""
    s3_io.s3_client.head_object = MagicMock()

    result = await s3_io.object_exists("test-key.txt")

    assert result is True
    s3_io.s3_client.head_object.assert_called_once_with(Bucket="test-bucket", Key="test-key.txt")


@pytest.mark.asyncio
async def test_object_exists_false(s3_io):
    """Test checking if object exists (False)."""
    s3_io.s3_client.head_object = MagicMock(side_effect=ClientError({"Error": {"Code": "404"}}, "HeadObject"))

    result = await s3_io.object_exists("test-key.txt")

    assert result is False

