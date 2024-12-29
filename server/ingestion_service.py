import httpx
import redis
import json
from datetime import datetime
from server.database import SessionLocal
from server.models import WeatherData, BatchMetadata

# External API configuration
API_BASE_URL = "https://example-weather-data-provider.com"
BATCHES_ENDPOINT = f"{API_BASE_URL}/batches"
BATCH_DATA_ENDPOINT = f"{API_BASE_URL}/batches/{{batch_id}}"

# Redis configuration
REDIS_URL = "redis://localhost:6379/0"  # Update this for production (e.g., Heroku Redis URL)
redis_client = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)

# Redis TTL (cache expiration in seconds)
CACHE_TTL = 300  # 5 minutes

# Function to fetch available batches
async def fetch_batches():
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(BATCHES_ENDPOINT)
            response.raise_for_status()
            return response.json()  # Assuming the response is a list of batch IDs
        except httpx.RequestError as e:
            print(f"Error fetching batches: {e}")
            return []

# Function to fetch data for a specific batch with caching
async def fetch_batch_data(batch_id, page=1):
    cache_key = f"batch_data:{batch_id}:page:{page}"

    # Check if data is already cached
    cached_data = redis_client.get(cache_key)
    if cached_data:
        print(f"Cache hit for batch {batch_id}, page {page}")
        return json.loads(cached_data)

    # Fetch data from API if not cached
    print(f"Cache miss for batch {batch_id}, page {page}. Fetching from API...")
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                BATCH_DATA_ENDPOINT.format(batch_id=batch_id), params={"page": page}
            )
            response.raise_for_status()
            data = response.json()  # Assuming paginated response

            # Cache the fetched data
            redis_client.set(cache_key, json.dumps(data), ex=CACHE_TTL)
            return data
        except httpx.RequestError as e:
            print(f"Error fetching batch data for {batch_id}, page {page}: {e}")
            return None

# Function to delete old active batches
def delete_old_active_batches():
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

# Retain metadata for deleted batches
def retain_metadata_for_deleted_batches():
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
async def ingest_batch(batch_id):
    session = SessionLocal()
    try:
        print(f"Starting ingestion for batch {batch_id}")
        delete_old_active_batches()
        retain_metadata_for_deleted_batches()

        start_time = datetime.now()

        # Fetch all pages for the batch
        page = 1
        batch_data = []
        while True:
            data = await fetch_batch_data(batch_id, page=page)
            if not data or "weather_data" not in data:
                break
            batch_data.extend(data["weather_data"])
            page += 1

        # Store batch metadata
        metadata = BatchMetadata(
            batch_id=batch_id,
            forecast_time=start_time,
            status="ACTIVE",
            number_of_rows=len(batch_data),
            start_ingest_time=start_time,
            end_ingest_time=datetime.utcnow(),
        )
        session.add(metadata)

        # Store weather data
        for record in batch_data:
            weather_entry = WeatherData(
                batch_id=batch_id,
                latitude=record["latitude"],
                longitude=record["longitude"],
                forecast_time=record["forecast_time"],
                temperature=record.get("temperature"),
                precipitation_rate=record.get("precipitation_rate"),
                humidity=record.get("humidity"),
            )
            session.add(weather_entry)

        session.commit()
        print(f"Batch {batch_id} ingested successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error ingesting batch {batch_id}: {e}")
    finally:
        session.close()

# Main function to fetch and process all batches
async def process_batches():
    batches = await fetch_batches()
    for batch_id in batches:
        await ingest_batch(batch_id)

if __name__ == "__main__":
    import asyncio
    asyncio.run(process_batches())
