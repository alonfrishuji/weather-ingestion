import pytest
from unittest.mock import patch
import redis
from server.ingestion_service import fetch_batches, fetch_batch_data
from config import REDIS_URL

# Setup Redis client for testing
redis_client = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)

@pytest.mark.skip(reason="Skipping fetch_batches_with_cache test")
@pytest.fixture(scope="function", autouse=True)
def clear_redis_cache():
    """
    Fixture to clear Redis cache before each test.
    """
    redis_client.flushdb()

@pytest.mark.skip(reason="Skipping fetch_batches_with_cache test")
@pytest.mark.asyncio
async def test_fetch_batches_with_cache():
    """
    Test fetch_batches to ensure it uses Redis caching.
    """
    mock_batches = [{"batch_id": "batch1"}, {"batch_id": "batch2"}]

    with patch("server.ingestion_service.httpx.AsyncClient.get") as mock_get:
        # Mock API response
        mock_response = mock_get.return_value
        mock_response.status_code = 200
        mock_response.json = lambda: mock_batches

        # First call should fetch data from the API
        batches = await fetch_batches()
        assert batches == mock_batches, "Failed to fetch batches correctly from the API"
        mock_get.assert_called_once()

        # Verify data is cached
        cached_batches = redis_client.get("batches")
        assert cached_batches is not None, "Batches not cached"
        assert eval(cached_batches) == mock_batches, "Cached data does not match fetched data"

        # Clear the mock call history
        mock_get.reset_mock()

        # Second call should use the cache
        batches = await fetch_batches()
        assert batches == mock_batches, "Failed to fetch batches correctly from the cache"
        mock_get.assert_not_called()

@pytest.mark.skip(reason="Skipping fetch_batches_with_cache test for now")
@pytest.mark.asyncio
async def test_fetch_batch_data_with_cache():
    """
    Test fetch_batch_data to ensure it uses Redis caching.
    """
    batch_id = "test_batch"
    mock_batch_data = [
        {"latitude": 40.7128, "longitude": -74.0060, "temperature": 15.0, "humidity": 60.0, "precipitation_rate": 0.1},
        {"latitude": 34.0522, "longitude": -118.2437, "temperature": 20.0, "humidity": 55.0, "precipitation_rate": 0.2},
    ]

    with patch("server.ingestion_service.httpx.AsyncClient.get") as mock_get:
        # Mock API response
        mock_response = mock_get.return_value
        mock_response.status_code = 200
        mock_response.json = lambda: {"data": mock_batch_data}

        # First call should fetch data from the API
        batch_data = await fetch_batch_data(batch_id, total_pages=1)
        assert batch_data == mock_batch_data, "Failed to fetch batch data correctly from the API"
        mock_get.assert_called_once()

        # Verify data is cached
        cached_data = redis_client.get(f"batch_data:{batch_id}")
        assert cached_data is not None, "Batch data not cached"
        assert eval(cached_data) == mock_batch_data, "Cached data does not match fetched data"

        # Clear the mock call history
        mock_get.reset_mock()

        # Second call should use the cache
        batch_data = await fetch_batch_data(batch_id, total_pages=1)
        assert batch_data == mock_batch_data, "Failed to fetch batch data correctly from the cache"
        mock_get.assert_not_called()
