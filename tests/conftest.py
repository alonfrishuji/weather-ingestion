
import pytest
from datetime import datetime
from unittest.mock import MagicMock
from server.models import BatchMetadata, WeatherData

@pytest.fixture
def mock_batch_data():
    """Provides sample weather data records for testing."""
    return [
        {
            "latitude": 40.7128,
            "longitude": -74.0060,
            "temperature": 72.5,
            "precipitation_rate": 0.0,
            "humidity": 65
        },
        {
            "latitude": 34.0522,
            "longitude": -118.2437,
            "temperature": 85.0,
            "precipitation_rate": 0.2,
            "humidity": 45
        }
    ]

@pytest.fixture
def mock_batches():
    """Provides sample batch metadata for testing."""
    return [
        {"batch_id": "batch1", "forecast_time": "2024-01-01T00:00:00Z"},
        {"batch_id": "batch2", "forecast_time": "2024-01-01T01:00:00Z"},
        {"batch_id": "batch3", "forecast_time": "2024-01-01T02:00:00Z"}
    ]

@pytest.fixture
def mock_db_session():
    """Creates a mock database session for testing."""
    session = MagicMock()
    session.query = MagicMock(return_value=session)
    session.filter = MagicMock(return_value=session)
    session.filter_by = MagicMock(return_value=session)
    session.order_by = MagicMock(return_value=session)
    return session

@pytest.fixture
def sample_weather_records():
    """Creates sample WeatherData records for testing batch operations."""
    return [
        WeatherData(
            batch_id="test_batch",
            latitude=40.7128,
            longitude=-74.0060,
            forecast_time=datetime(2024, 1, 1),
            temperature=72.5,
            precipitation_rate=0.0,
            humidity=65
        ),
        WeatherData(
            batch_id="test_batch",
            latitude=34.0522,
            longitude=-118.2437,
            forecast_time=datetime(2024, 1, 1),
            temperature=85.0,
            precipitation_rate=0.2,
            humidity=45
        )
    ]

@pytest.fixture
def sample_batch_metadata():
    """Creates sample BatchMetadata records for testing."""
    return [
        BatchMetadata(
            batch_id=f"batch{i}",
            forecast_time=datetime(2024, 1, i+1),
            status="ACTIVE" if i < 3 else "INACTIVE",
            retained=True,
            number_of_rows=100,
            start_ingest_time=datetime(2024, 1, 1),
            end_ingest_time=datetime(2024, 1, 1)
        )
        for i in range(5)
    ]