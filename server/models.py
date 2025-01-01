from sqlalchemy import (TIMESTAMP, Boolean, Column, Float, Index, Integer,
                        String)
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP
from sqlalchemy.sql import func

from server.database import Base


class WeatherData(Base):
    __tablename__ = "weather_data"
    id = Column(Integer, primary_key=True, index=True)
    batch_id = Column(String, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    forecast_time = Column(PG_TIMESTAMP(timezone=True), nullable=False)
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
    start_ingest_time =  Column(PG_TIMESTAMP(timezone=True), nullable=False, default=func.now())
    end_ingest_time = Column(PG_TIMESTAMP(timezone=True), nullable=True)
    status = Column(String, nullable=False)  # ACTIVE, INACTIVE 
    retained = Column(Boolean, default=True)  

    __table_args__ = (
        Index("ix_batch_active", "status", postgresql_where=(status == "ACTIVE")),
    )