# Files relevant with Redis implementation


## main.py

```python
from flask import Flask, request, jsonify
import logging
from server.cache_utils import initialize_redis
from config import REDIS_URL
from server.database import init_db
from server.ingestion_service import process_batches
from server.utils import fetch_weather_data, summarize_weather_data, fetch_batches, format_weather_data, format_weather_summary
import asyncio
import threading
import datetime

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def start_ingestion_service():
    """
    Start the ingestion service in a background thread.
    """
    logger.info("Starting the ingestion services")
    threading.Thread(target=lambda: asyncio.run(keep_running_ingestion()), daemon=True).start()
    logger.info("Ingestion service started.")

async def keep_running_ingestion():
    """
    Continuously run the ingestion service in a loop.
    """
    while True:
        start_time = datetime.datetime.now()
        logger.info(f"[{start_time}] Starting ingestion service")
        
        try:
            await process_batches()
        except Exception as e:
            logger.error(f"[{datetime.datetime.now()}] Error in ingestion service: {e}")
        
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"[{end_time}] Ingestion service completed. Duration: {duration:.2f} seconds.")
        
        logger.info(f"[{datetime.datetime.now()}] Waiting before the next ingestion cycle")
        await asyncio.sleep(300)  # Wait 10 minutes before the next cycle

# Initialize the system
def initialize_system():
    """
    Initialize the database at application startup.
    """
    init_db()
    logger.info("Database initialized successfully.")
    initialize_redis(REDIS_URL)
    logger.info(" Redis initialized successfully.")
    
    
@app.route("/weather/data", methods=["GET"])
def get_weather_data():
    latitude = request.args.get("latitude", type=float)
    longitude = request.args.get("longitude", type=float)

    if latitude is None or longitude is None:
        return jsonify({"error": "Missing latitude or longitude"}), 400
    
    try:
        data = fetch_weather_data(latitude, longitude)
        formatted_data = format_weather_data(data)
        return jsonify(formatted_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/weather/summarize", methods=["GET"])
def summarize_weather():
    latitude = request.args.get("latitude", type=float)
    longitude = request.args.get("longitude", type=float)

    if latitude is None or longitude is None:
        return jsonify({"error": "Missing latitude or longitude"}), 400
    
    try:
        summary = summarize_weather_data(latitude, longitude)
        formatted_summary = format_weather_summary(summary)
        
        # Store the result in the cache
        
        return jsonify(formatted_summary)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/batches", methods=["GET"])
def get_batches():
    try:
        # Fetch batch data from the database
        batches = fetch_batches()
        formatted_batches = [{
            "batch_id": b.batch_id,
            "forecast_time": b.forecast_time,
            "number_of_rows": b.number_of_rows,
            "start_ingest_time": b.start_ingest_time,
            "end_ingest_time": b.end_ingest_time,
            "status": b.status,
        } for b in batches]
                
        return jsonify(formatted_batches)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# start app
initialize_system()
start_ingestion_service()
```

## cache_utils.py
```python
import json
from redis import Redis

redis_client = None  

def initialize_redis(redis_url: str):
    global redis_client
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    return redis_client

def cache_get(key: str):
    """
    Retrieve cached data from Redis.
    """
    cached_data = redis_client.get(key)
    if cached_data:
        return json.loads(cached_data)
    return None

def cache_set(key: str, data, expiration: int):
    """
    Store data in Redis with an expiration time.
    """
    redis_client.setex(key, expiration, json.dumps(data, default=str))
```

## config.py
```python
import os
# External API configuration
API_BASE_URL = os.getenv("API_BASE_URL", "https://us-east1-climacell-platform-production.cloudfunctions.net/weather-data")
BATCHES_ENDPOINT = f"{API_BASE_URL}/batches"
BATCH_DATA_ENDPOINT = f"{API_BASE_URL}/batches/{{batch_id}}"
WEB_SERVICE_BASE_URL = "https://weather-ingestion.onrender.com"

# Redis configurations 
CACHE_EXPIRATION = 6000
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Database settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 4000))
```

## requirement.txt
```python
flask
python-dateutil
httpx
gunicorn
sqlalchemy
psycopg2-binary
tenacity
pytest
pytest-asyncio
python-dotenv
```