import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, List, Union
from server.cache_utils import cache_get, cache_set
from config import CACHE_EXPIRATION
import httpx
from dateutil.parser import isoparse
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from sqlalchemy.exc import OperationalError

from server.database import SessionLocal
from server.models import BatchMetadata, WeatherData
from config import BATCHES_ENDPOINT, BATCH_DATA_ENDPOINT, BATCH_SIZE

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=2, max=10), retry=retry_if_exception_type(httpx.RequestError))
async def fetch_batches() -> List[Dict[str, str]]:
    """Fetch available batches from the external API."""
    cache_key = "batches"
    cached_batches = cache_get(cache_key)
    if cached_batches:
        logger.info("Returning cached batches.")
        return cached_batches
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(BATCHES_ENDPOINT)
            response.raise_for_status()
            logger.info("Fetched batches successfully.")
            batches = response.json()
            cache_set(cache_key, batches, CACHE_EXPIRATION)
            return batches
        except httpx.RequestError as e:
            logger.error(f"Error fetching batches: {e}")
            return []

@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=2, max=10), retry=retry_if_exception_type(httpx.RequestError))
async def fetch_batch_data(batch_id: str, total_pages: int = 5) -> List[Dict[str, Union[str, float]]]:
    """Fetch paginated batch data from the external API."""
    cache_key = f"batch_data:{batch_id}"
    cached_data = cache_get(cache_key)
    if cached_data:
        logger.info(f"Returning cached data for batch_id {batch_id}.")
        return cached_data
    async with httpx.AsyncClient() as client:
        try:
            tasks = [client.get(BATCH_DATA_ENDPOINT.format(batch_id=batch_id), params={"page": page}) for page in range(total_pages)]
            responses = await asyncio.gather(*tasks)
            batch_data = []
            for response in responses:
                if response.status_code == 200:
                    json_data = response.json()
                    batch_data.extend(json_data.get("data", []))
            logger.info(f"Fetched {len(batch_data)} records for batch {batch_id}.")
            cache_set(cache_key, batch_data)
            return batch_data
        except httpx.RequestError as e:
            logger.error(f"Error fetching batch data for {batch_id}: {e}")
            return []

def delete_old_active_batches() -> None:

    session = SessionLocal()
    try:
        active_batches = session.query(BatchMetadata).filter(
            BatchMetadata.status == "ACTIVE"
        ).order_by(BatchMetadata.forecast_time).all()

        if len(active_batches) > 3:
            excess_batches = len(active_batches) - 3
            for batch in active_batches[:excess_batches]:
                session.query(WeatherData).filter(WeatherData.batch_id == batch.batch_id).delete()
                batch.status = "INACTIVE"
            session.commit()
            logger.info(f"Deleted {excess_batches} old active batches.")
            
    except Exception as e:
        session.rollback()
        print(f"Error deleting old active batches: {e}")
    finally:
        session.close()

def delete_weather_data_for_non_retained_batches() -> None:

    session = SessionLocal()
    try:
        # Find non-retained batches
        non_retained_batches = session.query(BatchMetadata).filter(
            BatchMetadata.retained == False
        ).all()

        for batch in non_retained_batches:
            # Delete associated weather data
            session.query(WeatherData).filter(
                WeatherData.batch_id == batch.batch_id
            ).delete()
            
        logger.info("Deleting weather data for non-retained batches.")
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error deleting weather data for non-retained batches: {e}")
    finally:
        session.close()

# Retain metadata for deleted batches
def retain_metadata_for_deleted_batches() -> None:

    session = SessionLocal()
    try:
        inactive_batches = session.query(BatchMetadata).filter(
            BatchMetadata.status == "INACTIVE"
        ).order_by(BatchMetadata.forecast_time).all()

        if len(inactive_batches) > 3:
            excess_batches = len(inactive_batches) - 3

            for batch in inactive_batches[:excess_batches]:
                batch.retained = False
                session.commit()

            logger.info(f"Updated metadata retention for {excess_batches} old inactive batches.")
    except Exception as e:
        session.rollback()
        print(f"Error during metadata retention: {e}")
    finally:
        session.close()


@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=2, max=10), retry=retry_if_exception_type(OperationalError))
def batch_insert_weather_data(weather_records: List[WeatherData]) -> None:
    """Insert weather data into the database in batches."""
    session = SessionLocal()
    try:
        total_records = len(weather_records)
        total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE
        for i in range(0, total_records, BATCH_SIZE):
            batch = weather_records[i:i + BATCH_SIZE]
            start_time = time.time()
            session.bulk_save_objects(batch)
            session.commit()
            end_time = time.time()
            logger.info(f"Inserted batch {i // BATCH_SIZE + 1}/{total_batches} for batch_id {batch[0].batch_id}: "
            f"{len(batch)} records in {end_time - start_time:.2f} seconds.")

    except Exception as e:
        session.rollback()
        logger.error(f"Error during batch insert: {e}")
    finally:
        session.close()

@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=2, max=10), retry=retry_if_exception_type(httpx.RequestError))
async def fetch_total_pages(batch_id: str) -> int:
    """Fetch the total number of pages for a batch ID."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(BATCH_DATA_ENDPOINT.format(batch_id=batch_id), params={"page": 0})
            response.raise_for_status()
            data = response.json()
            return data.get("metadata", {}).get("total_pages", 1)
        except httpx.RequestError as e:
            logger.error(f"Error fetching total pages for batch {batch_id}: {e}")
            return 1

async def ingest_batch(batch: Dict[str, Union[str, int]]) -> None:
    """Ingest batch data and update database metadata."""
    session = SessionLocal()
    try:
        batch_id = batch["batch_id"]
        batch_forecast_time = isoparse(batch["forecast_time"])
        existing_metadata = session.query(BatchMetadata).filter_by(batch_id=batch_id).first()

        if existing_metadata:
            logger.warning(f"Duplicate batch detected: {batch['batch_id']}. Skipping insertion.")
            return

        logger.info(f"Starting ingestion for batch {batch_id}.")
        batch_data, metadata = await initialize_metadata(session, batch_id, batch_forecast_time)

        process_batch_weather_data(batch_id, batch_forecast_time, batch_data)
        update_metadata_status(session, metadata)
        logger.info(f"Batch {batch_id} ingested successfully.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error ingesting batch {batch_id}: {e}")
        metadata = session.query(BatchMetadata).filter_by(batch_id=batch_id).first()
        if metadata:
            metadata.status = "FAILED"
            session.commit()
    finally:
        session.close()

def update_metadata_status(session, metadata):
    metadata.status = "ACTIVE"
    metadata.end_ingest_time = datetime.now()
    session.commit()

def process_batch_weather_data(batch_id, batch_forecast_time, batch_data):
    weather_records = [
            WeatherData(
                batch_id=batch_id,
                latitude=record["latitude"],
                longitude=record["longitude"],
                forecast_time=batch_forecast_time,
                temperature=record.get("temperature"),
                precipitation_rate=record.get("precipitation_rate"),
                humidity=record.get("humidity"),
            )
            for record in batch_data
        ]
    batch_insert_weather_data(weather_records)

async def initialize_metadata(session, batch_id, batch_forecast_time):
    total_pages = await fetch_total_pages(batch_id)
    batch_data = await fetch_batch_data(batch_id, total_pages)
    metadata = BatchMetadata(
            batch_id=batch_id,
            forecast_time=batch_forecast_time,
            status="RUNNING",
            number_of_rows=len(batch_data),
            start_ingest_time=datetime.now(),
        )
    session.add(metadata)
    session.commit()
    return batch_data,metadata

async def process_batches() -> None:
    """Fetch and process all batches."""
    batches = await fetch_batches()
    sorted_batches = sorted(batches, key=lambda x: isoparse(x["forecast_time"]))
    for batch in sorted_batches:
        await ingest_batch(batch)  
        
    # Cleanup old active batches
    delete_old_active_batches()
    # Cleanup non-retained batches
    delete_weather_data_for_non_retained_batches()
    # Retain metadata for deleted batches
    retain_metadata_for_deleted_batches()
    
    logger.info("Batch processing completed successfully.")

if __name__ == "__main__":
    asyncio.run(process_batches())
