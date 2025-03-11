import pytest
import time
from unittest.mock import patch, MagicMock
from moto import mock_s3
import boto3

from my_module import RedshiftS3Loader  # Replace with actual import

# Mock Redshift credentials and S3 settings
IAM_ROLE = "arn:aws:iam::123456789012:role/RedshiftS3Role"
S3_BUCKET = "test-bucket"
S3_PREFIX = "redshift_exports/"
SCHEMA = "public"
TABLE = "test_table"
S3_STAGING_LOCATION = f"s3://{S3_BUCKET}/{S3_PREFIX}"

@pytest.fixture
def redshift_loader():
    """Fixture for RedshiftS3Loader instance."""
    return RedshiftS3Loader("fake-host", "testdb", "testuser", "testpassword", IAM_ROLE)

@mock_s3
def test_successful_unload(redshift_loader):
    """Test successful UNLOAD operation."""
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=S3_BUCKET)

    with patch("psycopg.connect") as mock_connect:
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value = mock_cursor
        
        redshift_loader.initiate_unload_to_s3(SCHEMA, TABLE, S3_STAGING_LOCATION)

        mock_cursor.fetchone.return_value = {"state": "completed"}
        assert redshift_loader.check_unload_status(S3_STAGING_LOCATION) is True

@mock_s3
def test_unload_failure(redshift_loader):
    """Test UNLOAD failure case."""
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=S3_BUCKET)

    with patch("psycopg.connect") as mock_connect:
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value = mock_cursor

        redshift_loader.initiate_unload_to_s3(SCHEMA, TABLE, S3_STAGING_LOCATION)

        mock_cursor.fetchone.return_value = {"state": "failed"}
        assert redshift_loader.check_unload_status(S3_STAGING_LOCATION) is False

@mock_s3
def test_unload_timeout(redshift_loader):
    """Test UNLOAD operation timeout."""
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=S3_BUCKET)

    with patch("psycopg.connect") as mock_connect:
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value = mock_cursor

        redshift_loader.initiate_unload_to_s3(SCHEMA, TABLE, S3_STAGING_LOCATION)

        mock_cursor.fetchone.side_effect = [{"state": "running"}] * 50
        assert redshift_loader.check_unload_status(S3_STAGING_LOCATION) is False

@mock_s3
def test_successful_copy(redshift_loader):
    """Test successful COPY operation."""
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=S3_BUCKET)

    with patch("psycopg.connect") as mock_connect:
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value = mock_cursor

        redshift_loader.initiate_load_from_s3(SCHEMA, TABLE, S3_STAGING_LOCATION)

        mock_cursor.fetchone.return_value = {"state": "completed"}
        assert redshift_loader.check_load_status(S3_STAGING_LOCATION) is True

@mock_s3
def test_copy_failure(redshift_loader):
    """Test COPY failure case."""
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=S3_BUCKET)

    with patch("psycopg.connect") as mock_connect:
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value = mock_cursor

        redshift_loader.initiate_load_from_s3(SCHEMA, TABLE, S3_STAGING_LOCATION)

        mock_cursor.fetchone.return_value = {"state": "failed"}
        assert redshift_loader.check_load_status(S3_STAGING_LOCATION) is False

@mock_s3
def test_copy_timeout(redshift_loader):
    """Test COPY operation timeout."""
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=S3_BUCKET)

    with patch("psycopg.connect") as mock_connect:
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value = mock_cursor

        redshift_loader.initiate_load_from_s3(SCHEMA, TABLE, S3_STAGING_LOCATION)

        mock_cursor.fetchone.side_effect = [{"state": "running"}] * 50
        assert redshift_loader.check_load_status(S3_STAGING_LOCATION) is False
