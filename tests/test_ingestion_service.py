import pytest
from unittest.mock import AsyncMock, patch
from server.ingestion_service import fetch_batches, delete_weather_data_for_non_retained_batches, ingest_batch, fetch_batch_data
from server.models import BatchMetadata, WeatherData
from server.database import SessionLocal
from config import BATCH_DATA_ENDPOINT
from datetime import datetime

@pytest.fixture
def setup_test_data():
    """
    Fixture to set up initial test data in the database.
    """
    session = SessionLocal()

    # Add mock data for retained and non-retained batches
    retained_batches = [
        BatchMetadata(
            batch_id="retained1",
            forecast_time="2024-01-01T12:00:00",
            number_of_rows=100,
            start_ingest_time="2024-01-01T12:00:00",
            end_ingest_time="2024-01-01T12:10:00",
            status="INACTIVE",
            retained=True,
        ),
        BatchMetadata(
            batch_id="retained2",
            forecast_time="2024-01-01T12:00:00",
            number_of_rows=100,
            start_ingest_time="2024-01-01T12:00:00",
            end_ingest_time="2024-01-01T12:10:00",
            status="INACTIVE",
            retained=True,
        ),
    ]

    non_retained_batches = [
        BatchMetadata(
            batch_id="non_retained1",
            forecast_time="2024-01-01T12:00:00",
            number_of_rows=100,
            start_ingest_time="2024-01-01T12:00:00",
            end_ingest_time="2024-01-01T12:10:00",
            status="INACTIVE",
            retained=False,
        ),
        BatchMetadata(
            batch_id="non_retained2",
            forecast_time="2024-01-01T12:00:00",
            number_of_rows=100,
            start_ingest_time="2024-01-01T12:00:00",
            end_ingest_time="2024-01-01T12:10:00",
            status="INACTIVE",
            retained=False,
        ),
    ]

    # Add mock weather data
    weather_data = [
        WeatherData(
            batch_id="retained1",
            latitude=40.7128,
            longitude=-74.0060,
            forecast_time="2024-01-01T12:00:00",
            temperature=15.0,
            precipitation_rate=0.1,
            humidity=60.0,
        ),
        WeatherData(
            batch_id="non_retained1",
            latitude=34.0522,
            longitude=-118.2437,
            forecast_time="2024-01-01T12:00:00",
            temperature=20.0,
            precipitation_rate=0.2,
            humidity=55.0,
        ),
    ]

    session.add_all(retained_batches + non_retained_batches + weather_data)
    session.commit()
    session.close()


@pytest.fixture
def cleanup_test_data():
    """
    Fixture to clean up test data after each test.
    """
    yield
    session = SessionLocal()
    session.query(WeatherData).delete()
    session.query(BatchMetadata).delete()
    session.commit()
    session.close()


@pytest.mark.asyncio
async def test_fetch_batches():
    """
    Test fetching batch IDs from the external API.
    """
    mock_batches = [{"batch_id": "batch1"}, {"batch_id": "batch2"}, {"batch_id": "batch3"}]

    # Patch the httpx.AsyncClient class
    with patch("server.ingestion_service.httpx.AsyncClient") as mock_client:
        # Create a mock response
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json = AsyncMock(return_value=mock_batches)

        # Configure the mocked client to return the mock response
        mock_client.return_value.__aenter__.return_value.get.return_value = mock_response

        # Call the function
        batches = await fetch_batches()
        # Assertions
        assert batches == mock_batches, "Failed to fetch batches correctly"
        

@pytest.mark.asyncio
async def test_fetch_batch_data():
    """
    Test fetching paginated batch data from the external API.
    """
    batch_id = "test_batch"
    total_pages = 1  # Only one page of data
    mock_data = {
        "data": [
            {"latitude": 40.7128, "longitude": -74.0060, "temperature": 15.0, "humidity": 60.0, "precipitation_rate": 0.1},
        ]
    }

    with patch("server.ingestion_service.httpx.AsyncClient") as mock_client:
        # Create a mock response
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json = AsyncMock(return_value=mock_data)

        # Configure the mock client to return the mock response
        mock_client.return_value.__aenter__.return_value.get.return_value = mock_response

        # Call the function
        data = await fetch_batch_data(batch_id, total_pages)
        
        # Assertions
        assert len(data) == len(mock_data["data"]), "Failed to fetch the correct number of records."
        assert data[0]["latitude"] == mock_data["data"][0]["latitude"], "Latitude mismatch in the fetched data."
        assert data[0]["longitude"] == mock_data["data"][0]["longitude"], "Longitude mismatch in the fetched data."
        assert data[0]["temperature"] == mock_data["data"][0]["temperature"], "Temperature mismatch in the fetched data."
        assert data[0]["humidity"] == mock_data["data"][0]["humidity"], "Humidity mismatch in the fetched data."
        assert data[0]["precipitation_rate"] == mock_data["data"][0]["precipitation_rate"], "Precipitation rate mismatch in the fetched data."

        # Ensure the GET method was called with the correct URL and parameters
        mock_client.return_value.__aenter__.return_value.get.assert_called_once_with(
            BATCH_DATA_ENDPOINT.format(batch_id=batch_id),
            params={"page": 0},
        )    

@pytest.mark.asyncio
async def test_ingest_batch(setup_test_data, cleanup_test_data):
    """
    Test the ingestion of a batch into the database.
    """
    # Mock external API responses
    mock_batch = {
    "batch_id": f"test_batch_{datetime.now().timestamp()}",
    "forecast_time": "2025-01-01T00:00:00Z",
}
    mock_batch_data = [
        {"latitude": 40.7128, "longitude": -74.0060, "temperature": 15.0, "humidity": 60.0, "precipitation_rate": 0.1},
        {"latitude": 34.0522, "longitude": -118.2437, "temperature": 20.0, "humidity": 55.0, "precipitation_rate": 0.2},
    ]
    with patch("server.ingestion_service.fetch_batch_data", return_value=mock_batch_data):
        with patch("server.ingestion_service.fetch_total_pages", return_value=1):
            await ingest_batch(mock_batch)

            # Validate data in database
            session = SessionLocal()
            batch_metadata = session.query(BatchMetadata).filter_by(batch_id="test_batch").one_or_none()
            assert batch_metadata is not None, "Batch metadata not stored"

            weather_data = session.query(WeatherData).filter_by(batch_id="test_batch").all()
            assert len(weather_data) == len(mock_batch_data), "Weather data not stored correctly"
            session.close()


def test_delete_weather_data_for_non_retained_batches(setup_test_data, cleanup_test_data):
    """
    Test the deletion of weather data for non-retained batches.
    """
    session = SessionLocal()

    # Ensure non-retained batches exist
    non_retained_count = session.query(BatchMetadata).filter_by(retained=False).count()
    assert non_retained_count > 0, "No non-retained batches found"

    # Call the function
    delete_weather_data_for_non_retained_batches()

    # Assertions
    retained_weather_data = session.query(WeatherData).filter_by(batch_id="retained1").count()
    non_retained_weather_data = session.query(WeatherData).filter_by(batch_id="non_retained1").count()

    assert retained_weather_data > 0, "Retained weather data should not be deleted"
    assert non_retained_weather_data == 0, "Non-retained weather data should be deleted"
    session.close()
