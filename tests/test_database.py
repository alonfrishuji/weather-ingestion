from server.database import SessionLocal
from server.models import WeatherData, BatchMetadata

def test_weather_data():
    session = SessionLocal()
    try:
        # Fetch data from the database
        results = session.query(WeatherData).all()
        assert len(results) > 0, "Weather data table is empty."
        print("Weather data:", results)
    finally:
        session.close()

def test_batch_metadata():
    session = SessionLocal()
    try:
        # Fetch batch metadata from the database
        results = session.query(BatchMetadata).all()
        assert len(results) > 0, "Batch metadata table is empty."
        print("Batch metadata:", results)
    finally:
        session.close()
        
        
def test_weather_data_persistence():
    """Test saving and retrieving weather data from the real database."""
    session = SessionLocal()
    try:
        weather = WeatherData(
            batch_id="test_batch",
            latitude=12.34,
            longitude=56.78,
            forecast_time="2025-01-01T00:00:00Z",
            temperature=25.5,
            precipitation_rate=0.2,
            humidity=80.0,
        )
        session.add(weather)
        session.commit()
        result = session.query(WeatherData).filter_by(batch_id="test_batch").first()
        assert result is not None, "WeatherData record not found."
        assert result.temperature == 25.5, "Temperature value mismatch."
    finally:
        session.close()

def test_batch_metadata_persistence():
    """Test saving and retrieving batch metadata from the real database."""
    session = SessionLocal()
    try:
        batch = BatchMetadata(
            batch_id="test_batch",
            forecast_time="2025-01-01T00:00:00Z",
            status="RUNNING",
            number_of_rows=100,
        )
        session.add(batch)
        session.commit()
        result = session.query(BatchMetadata).filter_by(batch_id="test_batch").first()
        assert result is not None, "BatchMetadata record not found."
        assert result.status == "RUNNING", "Status value mismatch."
    finally:
        session.close()
        
        
if __name__ == "__main__":
    test_weather_data()
    test_batch_metadata()
    test_weather_data_persistence()
    test_batch_metadata_persistence()