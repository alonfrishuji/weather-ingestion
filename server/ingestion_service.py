import asyncio
from datetime import datetime
from typing import Dict, List, Union
from sqlalchemy.exc import OperationalError
import httpx
import redis
from dateutil.parser import isoparse
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

from server.database import SessionLocal
from server.models import BatchMetadata, WeatherData

# External API configuration
API_BASE_URL = "https://us-east1-climacell-platform-production.cloudfunctions.net/weather-data"

BATCHES_ENDPOINT = f"{API_BASE_URL}/batches"
BATCH_DATA_ENDPOINT = f"{API_BASE_URL}/batches/{{batch_id}}"

# Redis configuration
REDIS_URL = "redis://localhost:6379/0"  # Update this for production (e.g., Heroku Redis URL)
redis_client = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)

# Redis TTL (cache expiration in seconds)
CACHE_TTL = 300  # 5 minutes

@retry(
    stop=stop_after_attempt(5),  # Retry up to 5 times
    wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff
    retry=retry_if_exception_type(httpx.RequestError),  # Retry on specific exceptions
)
# Function to fetch available batches
async def fetch_batches() ->  List[Dict[str, str]]:
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(BATCHES_ENDPOINT)
            response.raise_for_status()
            print("Fetched batches successfully.")
            return response.json()  # Assuming the response is a list of batch IDs
        except httpx.RequestError as e:
            print(f"Error fetching batches: {e}")
            return []
        
@retry(
    stop=stop_after_attempt(5),  # Retry up to 5 times
    wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff
    retry=retry_if_exception_type(httpx.RequestError),  # Retry on specific exceptions
)        
# Function to fetch data for a specific batch with concurrent requests
async def fetch_batch_data(batch_id: str, total_pages: int = 5) -> List[Dict[str, Union[str, float]]]:
    async with httpx.AsyncClient() as client:
        try:
            # Fetch all pages concurrently
            tasks = [
                client.get(
                    BATCH_DATA_ENDPOINT.format(batch_id=batch_id),
                    params={"page": page},
                )
                for page in range(total_pages)
            ]
            responses = await asyncio.gather(*tasks)
            print(f"Fetched batch data for {batch_id} successfully.")
            
            batch_data = []
            for response in responses:
                if response.status_code == 200:
                    batch_data.extend(response.json().get("data", []))
            return batch_data
        except httpx.RequestError as e:
            print(f"Error fetching batch data for {batch_id}: {e}")
            return []

# Function to delete old active batches
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
            print(f"Deleted {excess_batches} old active batches.")
            
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
            
        print("Deleting weather data for non-retained batches")
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

            print(f"Updated metadata retention for {excess_batches} old inactive batches.")
    except Exception as e:
        session.rollback()
        print(f"Error during metadata retention: {e}")
    finally:
        session.close()

# Function to ingest a new batch
# Batch insert WeatherData objects into the database
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(OperationalError)
)
def batch_insert_weather_data(weather_data_list: List[WeatherData],batch_size = 2000) -> None:
    session = SessionLocal()
    try:
        for i in range(0, len(weather_data_list), batch_size):
            batch = weather_data_list[i:i + batch_size]
            session.bulk_save_objects(batch)
            session.commit()
            print(f"Inserted batch {i // batch_size + 1}")
    except Exception as e:
        session.rollback()
        print(f"Error during batch insert: {e}")
    finally:
        session.close()


@retry(
    stop=stop_after_attempt(5),  # Retry up to 5 times
    wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff
    retry=retry_if_exception_type(httpx.RequestError),  # Retry on specific exceptions
)
async def fetch_total_pages(batch_id: str) -> int:
    """
    Fetch the total number of pages for a given batch ID.
    """
    async with httpx.AsyncClient() as client:
        try:
            print(f"Fetching total pages for batch {batch_id}")
            response = await client.get(
                BATCH_DATA_ENDPOINT.format(batch_id=batch_id), params={"page": 0}
            )
            print(f"Response status: {response.status_code}")
            response.raise_for_status()
            data = response.json()
            return data.get("metadata", {}).get("total_pages", 0)  # Default to 0 if not provided
        except httpx.RequestError as e:
            print(f"Error fetching total pages for batch {batch_id}: {e}")
            return 1


async def ingest_batch(batch: Dict[str, Union[str, int]]):
    """
    Ingest all data for a batch by dynamically determining total pages.
    """
    session = SessionLocal()
    try:
        batch_id = batch["batch_id"] if isinstance(batch, dict) else batch
        batch_forecast_time = isoparse(batch["forecast_time"]) if isinstance(batch, dict) else datetime.now()
        existing_metadata = session.query(BatchMetadata).filter_by(batch_id=batch_id).first()        
        
        if existing_metadata:
            print(f"Batch {batch_id} already exists. Skipping metadata insertion.")
            return
            
        start_time = datetime.now()
        print(f"Starting ingestion for batch {batch_id}")

        # Fetch the total number of pages for the batch
        total_pages = await fetch_total_pages(batch_id)
        print(f"Total pages for batch {batch_id}: {total_pages}")

        # Fetch all pages for the batch concurrently
        batch_data = await fetch_batch_data(batch_id, total_pages)
        print(f"Total records fetched for batch {batch_id}: {len(batch_data)}")

        # Store batch metadata
        metadata = BatchMetadata(
            batch_id=batch_id,
            forecast_time=batch_forecast_time,
            status="ACTIVE",
            number_of_rows=len(batch_data),
            start_ingest_time=start_time,
            end_ingest_time=datetime.now(),
        )
        session.add(metadata)
        session.commit()
        

        weather_data_list = [
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
        # Batch insert weather data
        batch_insert_weather_data(weather_data_list)
        print(f"Batch {batch_id} ingested successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error ingesting batch {batch_id}: {e}")
    finally:
        session.close()


# Main function to fetch and process all batches
async def process_batches() -> None:
    batches = await fetch_batches()
    for batch in batches:
        await ingest_batch(batch)

    # Step 2: Cleanup old active batches
    print("Deleting old active batches")
    delete_old_active_batches()

    # Step 3: Cleanup non-retained batches
    delete_weather_data_for_non_retained_batches()

    # Step 4: Retain metadata for deleted batches
    retain_metadata_for_deleted_batches()

    print("Batch processing and cleanup completed successfully")
    
    
if __name__ == "__main__":
    asyncio.run(process_batches())
