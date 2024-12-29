from server.database import SessionLocal
from server.models import WeatherData

# Test the database by adding a single weather record
def test_database_insert():
    session = SessionLocal()

    new_weather = WeatherData(
        latitude=40.7128,
        longitude=-74.0060,
        forecast_time="2024-12-01T10:00:00",
        temperature=15.0,
        precipitation_rate=0.0,
        humidity=60.0
    )

    session.add(new_weather)
    session.commit()
    session.close()
    print("Test weather data inserted successfully!")

if __name__ == "__main__":
    test_database_insert()
