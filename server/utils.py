import logging
from sqlalchemy.sql import func
from server.database import SessionLocal
from server.models import WeatherData, BatchMetadata

logger = logging.getLogger(__name__)

def fetch_weather_data(latitude: float, longitude: float):
    """
    Fetch weather data based on latitude and longitude.
    """
    session = SessionLocal()
    try:
        return session.query(WeatherData).filter_by(latitude=latitude, longitude=longitude).all()
    except Exception as e:
        logger.error(f"Error fetching weather data: {e}")
        raise
    finally:
        session.close()

def summarize_weather_data(latitude: float, longitude: float):
    """
    Summarize weather data statistics.
    """
    session = SessionLocal()
    try:
        return session.query(
            func.max(WeatherData.temperature).label("max_temperature"),
            func.min(WeatherData.temperature).label("min_temperature"),
            func.avg(WeatherData.temperature).label("avg_temperature"),
            func.max(WeatherData.precipitation_rate).label("max_precipitation_rate"),
            func.min(WeatherData.precipitation_rate).label("min_precipitation_rate"),
            func.avg(WeatherData.precipitation_rate).label("avg_precipitation_rate"),
            func.max(WeatherData.humidity).label("max_humidity"),
            func.min(WeatherData.humidity).label("min_humidity"),
            func.avg(WeatherData.humidity).label("avg_humidity"),
        ).filter(
            WeatherData.latitude == latitude,
            WeatherData.longitude == longitude
        ).one()
    except Exception as e:
        logger.error(f"Error summarizing weather data: {e}")
        raise
    finally:
        session.close()

def fetch_batches():
    """
    Fetch all batches from the database.
    """
    session = SessionLocal()
    try:
        return session.query(BatchMetadata).all()
    except Exception as e:
        logger.error(f"Error fetching batches: {e}")
        raise
    finally:
        session.close()
def format_weather_data(data):
    """
    Format weather data into a JSON-serializable list of dictionaries.
    """
    return [{
        "latitude": d.latitude,
        "longitude": d.longitude,
        "forecast_time": d.forecast_time,
        "temperature": d.temperature,
        "precipitation_rate": d.precipitation_rate,
        "humidity": d.humidity,
    } for d in data]

def format_weather_summary(summary):
    """
    Format weather summary statistics into a dictionary.
    """
    return {
        "temperature": {
            "max": summary.max_temperature,
            "min": summary.min_temperature,
            "avg": summary.avg_temperature,
        },
        "precipitation_rate": {
            "max": summary.max_precipitation_rate,
            "min": summary.min_precipitation_rate,
            "avg": summary.avg_precipitation_rate,
        },
        "humidity": {
            "max": summary.max_humidity,
            "min": summary.min_humidity,
            "avg": summary.avg_humidity,
        },
    }
