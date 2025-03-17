import pytest
import boto3
import time
import threading
from unittest.mock import patch, MagicMock
from moto import mock_s3
from redshift_s3 import (  # Import functions to be tested
    delete_all_s3_objects, unload_to_s3, check_unload_status,
    load_from_s3, check_load_status, get_connection, unload_done, load_done
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
    mock_s3_bucket.put_object(Bucket=BUCKET_NAME, Key="export/file1.parquet", Body=b"data")
    mock_s3_bucket.put_object(Bucket=BUCKET_NAME, Key="export/file2.parquet", Body=b"data")

    delete_all_s3_objects()

    response = mock_s3_bucket.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_PREFIX)
    assert "Contents" not in response  # No files should be left

# -------------------------
# 🚀 TEST UNLOAD FUNCTION (THREAD)
# -------------------------
@patch("redshift_s3.get_connection")
def test_unload_to_s3_success(mock_get_connection):
    """Tests successful UNLOAD execution in a separate thread."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    unload_done.clear()
    unload_to_s3()

    # Wait for the thread to finish
    unload_done.wait(timeout=5)
    assert unload_done.is_set()  # Ensure the event was set

    mock_cursor.execute.assert_called_once()  # Ensure UNLOAD was executed

@patch("redshift_s3.get_connection")
def test_unload_to_s3_failure(mock_get_connection):
    """Tests UNLOAD failure due to Redshift connection error."""
    mock_get_connection.side_effect = Exception("Redshift connection failed")

    unload_done.clear()
    unload_to_s3()

    unload_done.wait(timeout=5)
    assert unload_done.is_set()  # The event should be set even on failure

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

    poll_results = [
        {"query": "12345", "status": "Running", "execution_time_sec": 2.5},
        {"query": "12345", "status": "Completed", "execution_time_sec": 6.2}
    ]
    
    mock_cursor.fetchone.side_effect = poll_results

    unload_done.set()  # Simulate UNLOAD thread completion
    check_unload_status()

    assert mock_cursor.execute.call_count == 2  # Ensure it polled twice

# -------------------------
# 🚀 TEST LOAD FUNCTION (THREAD)
# -------------------------
@patch("redshift_s3.get_connection")
def test_load_from_s3_success(mock_get_connection):
    """Tests successful LOAD execution in a separate thread."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    load_done.clear()
    load_from_s3()

    # Wait for the thread to finish
    load_done.wait(timeout=5)
    assert load_done.is_set()  # Ensure the event was set

    mock_cursor.execute.assert_called_once()  # Ensure COPY was executed

@patch("redshift_s3.get_connection")
def test_load_from_s3_failure(mock_get_connection):
    """Tests LOAD failure due to Redshift connection error."""
    mock_get_connection.side_effect = Exception("Redshift load failed")

    load_done.clear()
    load_from_s3()

    load_done.wait(timeout=5)
    assert load_done.is_set()  # The event should be set even on failure

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

    load_done.set()  # Simulate LOAD thread completion
    check_load_status()

    assert mock_cursor.execute.call_count == 2  # Ensure it polled twice

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

    mock_cursor.fetchone.side_effect = [None, None, {"query": "12345", "status": "Completed", "execution_time_sec": 5.4}]

    unload_done.set()
    check_unload_status()

    assert mock_cursor.execute.call_count == 3  # It should retry twice before finding a completed UNLOAD

@patch("redshift_s3.get_connection")
def test_check_load_status_no_results(mock_get_connection):
    """Tests polling when no LOAD queries are found."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.side_effect = [None, None, {"query": "67890", "status": "Completed", "execution_time_sec": 6.1}]

    load_done.set()
    check_load_status()

    assert mock_cursor.execute.call_count == 3  # It should retry twice before finding a completed LOAD
