
from unittest.mock import AsyncMock, Mock
import httpx

def create_mock_response(status_code=200, json_data=None):
    """Creates a mock HTTP response with specified status code and JSON data."""
    mock_response = AsyncMock()
    mock_response.status_code = status_code
    mock_response.json = Mock(return_value=json_data if json_data is not None else {})
    return mock_response

def create_mock_http_error(error_message="Network error"):
    """Creates a mock HTTP error for testing error handling."""
    return httpx.RequestError(error_message)

def verify_batch_metadata(batch_metadata, expected_status, expected_retained=True):
    """Verifies batch metadata properties."""
    assert batch_metadata.status == expected_status
    assert batch_metadata.retained == expected_retained
    assert batch_metadata.start_ingest_time is not None
    if expected_status in ["ACTIVE", "INACTIVE"]:
        assert batch_metadata.end_ingest_time is not None