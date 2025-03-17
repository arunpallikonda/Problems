import pytest
import boto3
import time
from unittest.mock import patch, MagicMock
from moto import mock_s3
from redshift_s3 import (  # Import functions to be tested
    delete_all_s3_objects, unload_to_s3, check_unload_status,
    load_from_s3, check_load_status, get_connection
)

# Constants for testing
BUCKET_NAME = "source-s3copy"
S3_PREFIX = "export/"


@pytest.fixture
def mock_s3_bucket():
    """Creates a mock S3 bucket with Moto."""
    with mock_s3():
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        yield s3_client


# -------------------------
# 🚀 TEST DELETE S3 OBJECTS
# -------------------------
def test_delete_all_s3_objects(mock_s3_bucket):
    """Tests if all objects are deleted from S3."""
    # Upload mock files
    mock_s3_bucket.put_object(Bucket=BUCKET_NAME, Key="export/file1.parquet", Body=b"data")
    mock_s3_bucket.put_object(Bucket=BUCKET_NAME, Key="export/file2.parquet", Body=b"data")

    delete_all_s3_objects()

    # Verify objects are deleted
    response = mock_s3_bucket.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_PREFIX)
    assert "Contents" not in response  # No files should be left


# -------------------------
# 🚀 TEST UNLOAD FUNCTION
# -------------------------
@patch("redshift_s3.get_connection")
def test_unload_to_s3_success(mock_get_connection):
    """Tests successful UNLOAD command execution."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor  # Mocking cursor context manager

    unload_to_s3()

    mock_cursor.execute.assert_called_once()  # Ensure UNLOAD was executed


@patch("redshift_s3.get_connection")
def test_unload_to_s3_failure(mock_get_connection):
    """Tests UNLOAD failure due to Redshift connection error."""
    mock_get_connection.side_effect = Exception("Redshift connection failed")

    with pytest.raises(Exception, match="Redshift connection failed"):
        unload_to_s3()


# -------------------------
# 🚀 TEST UNLOAD STATUS POLLING
# -------------------------
@patch("redshift_s3.get_connection")
def test_check_unload_status_completed(mock_get_connection):
    """Tests polling until UNLOAD is completed."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Mocking different polling states
    poll_results = [
        {"query": "12345", "status": "Running", "execution_time_sec": 2.5},
        {"query": "12345", "status": "Completed", "execution_time_sec": 6.2}
    ]

    mock_cursor.fetchone.side_effect = poll_results  # Each call returns next status

    check_unload_status()

    assert mock_cursor.execute.call_count == 2  # Ensure it polled twice


@patch("redshift_s3.get_connection")
def test_check_unload_status_failed(mock_get_connection):
    """Tests when UNLOAD fails after polling."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.side_effect = [{"query": "12345", "status": "Failed", "execution_time_sec": 3.1}]

    with pytest.raises(Exception, match="UNLOAD operation failed!"):
        check_unload_status()


# -------------------------
# 🚀 TEST LOAD FUNCTION
# -------------------------
@patch("redshift_s3.get_connection")
def test_load_from_s3_success(mock_get_connection):
    """Tests successful LOAD execution."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    load_from_s3()

    mock_cursor.execute.assert_called_once()  # Ensure COPY was executed


@patch("redshift_s3.get_connection")
def test_load_from_s3_failure(mock_get_connection):
    """Tests LOAD failure due to Redshift error."""
    mock_get_connection.side_effect = Exception("Redshift load failed")

    with pytest.raises(Exception, match="Redshift load failed"):
        load_from_s3()


# -------------------------
# 🚀 TEST LOAD STATUS POLLING
# -------------------------
@patch("redshift_s3.get_connection")
def test_check_load_status_completed(mock_get_connection):
    """Tests polling until LOAD is completed."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    poll_results = [
        {"query": "67890", "status": "Running", "execution_time_sec": 1.8},
        {"query": "67890", "status": "Completed", "execution_time_sec": 4.5}
    ]

    mock_cursor.fetchone.side_effect = poll_results

    check_load_status()

    assert mock_cursor.execute.call_count == 2  # Ensure it polled twice


@patch("redshift_s3.get_connection")
def test_check_load_status_failed(mock_get_connection):
    """Tests when LOAD fails after polling."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.side_effect = [{"query": "67890", "status": "Failed", "execution_time_sec": 2.9}]

    with pytest.raises(Exception, match="LOAD operation failed!"):
        check_load_status()


# -------------------------
# 🚀 TEST EDGE CASES
# -------------------------
@patch("redshift_s3.get_connection")
def test_check_unload_status_no_results(mock_get_connection):
    """Tests polling when no UNLOAD queries are found."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.side_effect = [None, None,
                                        {"query": "12345", "status": "Completed", "execution_time_sec": 5.4}]

    check_unload_status()

    assert mock_cursor.execute.call_count == 3  # It should retry twice before finding a completed UNLOAD


@patch("redshift_s3.get_connection")
def test_check_load_status_no_results(mock_get_connection):
    """Tests polling when no LOAD queries are found."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.side_effect = [None, None,
                                        {"query": "67890", "status": "Completed", "execution_time_sec": 6.1}]

    check_load_status()

    assert mock_cursor.execute.call_count == 3  # It should retry twice before finding a completed LOAD
