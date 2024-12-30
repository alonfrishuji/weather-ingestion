from server.database import SessionLocal
from server.models import WeatherData, BatchMetadata

# Create a new database session
session = SessionLocal()

try:
    # Query all weather data
    weather_data = session.query(WeatherData).all()
    print("Weather Data:")
    for data in weather_data:
        print(f"ID: {data.id}, Lat: {data.latitude}, Lon: {data.longitude}, Temp: {data.temperature}")

    # Query all batch metadata
    batch_metadata = session.query(BatchMetadata).all()
    print("\nBatch Metadata:")
    for batch in batch_metadata:
        print(f"Batch ID: {batch.batch_id}, Status: {batch.status}, Rows: {batch.number_of_rows}")

except Exception as e:
    print(f"Error retrieving data: {e}")
finally:
    session.close()
