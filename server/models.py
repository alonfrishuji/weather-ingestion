from sqlalchemy import Column, Integer, Float, String, TIMESTAMP, Boolean, Index
from server.database import Base

class WeatherData(Base):
    __tablename__ = "weather_data"
    id = Column(Integer, primary_key=True, index=True)
    batch_id = Column(String, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    forecast_time = Column(TIMESTAMP, nullable=False)
    temperature = Column(Float)
    precipitation_rate = Column(Float)
    humidity = Column(Float)
    
    __table_args__ = (
        Index("ix_weather_lat_lon_time", "latitude", "longitude", "forecast_time"),
        Index("ix_weather_id", "batch_id"),
    )


class BatchMetadata(Base):
    __tablename__ = "batch_metadata"

    batch_id = Column(String, primary_key=True)
    forecast_time = Column(TIMESTAMP, nullable=False)
    number_of_rows = Column(Integer, nullable=False)
    start_ingest_time = Column(TIMESTAMP, nullable=False)
    end_ingest_time = Column(TIMESTAMP, nullable=True)
    status = Column(String, nullable=False)  # ACTIVE, INACTIVE
    retained = Column(Boolean, default=True)  # Indicates if metadata is retained

    __table_args__ = (
        Index("ix_batch_active", "status", postgresql_where=(status == "ACTIVE")),
    )