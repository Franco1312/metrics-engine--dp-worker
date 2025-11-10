"""Unit tests for S3 path utilities."""

import pytest

from metrics_worker.infrastructure.aws.s3_path import S3Path


def test_normalize_with_s3_prefix():
    """Test normalize with s3:// prefix."""
    path = "s3://bucket/path/to/file"
    result = S3Path.normalize(path)
    assert result == "bucket/path/to/file"


def test_normalize_without_s3_prefix():
    """Test normalize without s3:// prefix."""
    path = "bucket/path/to/file"
    result = S3Path.normalize(path)
    assert result == "bucket/path/to/file"


def test_to_full_path():
    """Test building full S3 path."""
    bucket = "my-bucket"
    key = "path/to/file"
    result = S3Path.to_full_path(bucket, key)
    assert result == "s3://my-bucket/path/to/file"


def test_to_full_path_with_leading_slash():
    """Test building full S3 path with leading slash in key."""
    bucket = "my-bucket"
    key = "/path/to/file"
    result = S3Path.to_full_path(bucket, key)
    assert result == "s3://my-bucket/path/to/file"


def test_join():
    """Test joining path parts."""
    result = S3Path.join("bucket", "path", "to", "file")
    assert result == "bucket/path/to/file"


def test_join_with_slashes():
    """Test joining path parts with slashes."""
    result = S3Path.join("bucket/", "/path/", "/to/", "/file")
    assert result == "bucket/path/to/file"


def test_join_empty_parts():
    """Test joining with empty parts."""
    result = S3Path.join("bucket", "", "path", "file")
    assert result == "bucket/path/file"


def test_parent():
    """Test getting parent directory."""
    assert S3Path.parent("bucket/path/to/file") == "bucket/path/to"
    assert S3Path.parent("bucket/path/to/") == "bucket/path"
    assert S3Path.parent("file") == ""
    assert S3Path.parent("bucket/file") == "bucket"


def test_basename():
    """Test getting basename."""
    assert S3Path.basename("bucket/path/to/file.txt") == "file.txt"
    assert S3Path.basename("bucket/path/to/") == "to"
    assert S3Path.basename("file.txt") == "file.txt"
    assert S3Path.basename("bucket") == "bucket"


def test_stem():
    """Test getting stem (filename without extension)."""
    assert S3Path.stem("bucket/path/to/file.txt") == "file"
    assert S3Path.stem("file.txt") == "file"
    assert S3Path.stem("file") == "file"
    assert S3Path.stem("bucket/path/to/file.tar.gz") == "file.tar"


def test_suffix():
    """Test getting suffix (extension)."""
    assert S3Path.suffix("bucket/path/to/file.txt") == ".txt"
    assert S3Path.suffix("file.txt") == ".txt"
    assert S3Path.suffix("file") == ""
    assert S3Path.suffix("bucket/path/to/file.tar.gz") == ".gz"


def test_with_name():
    """Test replacing filename."""
    assert S3Path.with_name("bucket/path/to/file.txt", "newfile.txt") == "bucket/path/to/newfile.txt"
    assert S3Path.with_name("file.txt", "newfile.txt") == "newfile.txt"


def test_with_suffix():
    """Test replacing suffix."""
    assert S3Path.with_suffix("bucket/path/to/file.txt", ".json") == "bucket/path/to/file.json"
    assert S3Path.with_suffix("file.txt", "json") == "file.json"
    assert S3Path.with_suffix("file", ".json") == "file.json"


def test_rstrip_separator():
    """Test removing trailing separator."""
    assert S3Path.rstrip_separator("bucket/path/to/") == "bucket/path/to"
    assert S3Path.rstrip_separator("bucket/path/to") == "bucket/path/to"
    assert S3Path.rstrip_separator("bucket/") == "bucket"

