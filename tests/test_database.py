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

if __name__ == "__main__":
    test_weather_data()
    test_batch_metadata()
