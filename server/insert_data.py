from server.database import SessionLocal
from server.models import WeatherData, BatchMetadata

# Create a new database session
session = SessionLocal()

try:
    # Insert a new weather data record
    new_weather = WeatherData(
        latitude=40.7128,
        longitude=-74.0060,
        forecast_time="2024-12-30 10:00:00",
        temperature=15.0,
        precipitation_rate=0.0,
        humidity=60.0
    )
    session.add(new_weather)

    # Insert a new batch metadata record
    new_batch = BatchMetadata(
        batch_id="batch_001",
        forecast_time="2024-12-30 10:00:00",
        number_of_rows=1,
        start_ingest_time="2024-12-30 09:00:00",
        end_ingest_time="2024-12-30 09:15:00",
        status="ACTIVE"
    )
    session.add(new_batch)

    # Commit the changes
    session.commit()
    print("Data inserted successfully!")
except Exception as e:
    session.rollback()
    print(f"Error inserting data: {e}")
finally:
    session.close()
