import pytest
from unittest.mock import AsyncMock, patch
from server.ingestion_service import fetch_batches, delete_weather_data_for_non_retained_batches, ingest_batch, fetch_batch_data
from server.models import BatchMetadata, WeatherData
from server.database import SessionLocal
import asyncio 

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
    mock_batches = ["batch1", "batch2", "batch3"]
    with patch("server.ingestion_service.httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        # Mock the API response
        mock_get.return_value.status_code = 200
        mock_get.return_value.json = AsyncMock(return_value=mock_batches)
        # Call the function
        batches = await fetch_batches()

        # Assertions
        assert batches == mock_batches, "Failed to fetch batches correctly"
        mock_get.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_batch_data():
    """
    Test fetching paginated batch data from the external API.
    """
    batch_id = "test_batch"
    page = 1
    mock_data = {
        "data": [
            {"latitude": 40.7128, "longitude": -74.0060, "temperature": 15.0, "humidity": 60.0, "precipitation_rate": 0.1},
        ]
    }
    with patch("server.ingestion_service.httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        # Mock the API response
        mock_get.return_value.json.return_value = mock_data
        mock_get.return_value.status_code = 200

        # Call the function
        data = await fetch_batch_data(batch_id, page)

        # Assertions
        assert "data" in data, "Weather data missing in response"
        mock_get.assert_called_once_with(
            f"https://us-east1-climacell-platform-production.cloudfunctions.net/weather-data/batches/{batch_id}",
            params={"page": page},
        )

@pytest.mark.asyncio
async def test_ingest_batch(setup_test_data, cleanup_test_data):
    """
    Test the ingestion of a batch into the database.
    """
    # Mock external API responses
    mock_batch_data = [
        {"latitude": 40.7128, "longitude": -74.0060, "temperature": 15.0, "humidity": 60.0, "precipitation_rate": 0.1},
        {"latitude": 34.0522, "longitude": -118.2437, "temperature": 20.0, "humidity": 55.0, "precipitation_rate": 0.2},
    ]
    with patch("server.ingestion_service.fetch_batch_data", return_value=mock_batch_data):
        with patch("server.ingestion_service.fetch_total_pages", return_value=1):
            await ingest_batch("test_batch")

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
