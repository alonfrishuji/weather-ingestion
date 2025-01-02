from flask import Flask, request, jsonify
import logging
import json
from config import REDIS_URL, CACHE_EXPIRATION
from server.database import init_db
from server.ingestion_service import process_batches
from redis import Redis
from server.utils import fetch_weather_data, summarize_weather_data, fetch_batches, format_weather_data, format_weather_summary
import asyncio
import threading
import datetime

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
redis_client = None

def initialize_redis():
    """
    Initialize the Redis client and test the connection.
    """
    global redis_client
    try:
        # Replace 'your-redis-url' with your Redis connection URL
        redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
        # Test connection
        redis_client.ping()
        logger.info("Connected to Redis successfully.")
    except ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

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
        await asyncio.sleep(6000)  # Wait 100 minutes before the next cycle


# Initialize the system
def initialize_system():
    """
    Initialize the database at application startup.
    """
    init_db()
    logger.info("Database initialized successfully.")
    initialize_redis()
    logger.info("Redis initialized successfully.")

@app.route("/weather/data", methods=["GET"])
def get_weather_data():
    latitude = request.args.get("latitude", type=float)
    longitude = request.args.get("longitude", type=float)

    if latitude is None or longitude is None:
        return jsonify({"error": "Missing latitude or longitude"}), 400
    
    cache_key = f"weather:data:{latitude}:{longitude}"
    
    # Check if data is in the cache
    cached_data = redis_client.get(cache_key)
    if cached_data:
        return jsonify({"cache": True, "cached_data": json.loads(cached_data)})
    try:
        data = fetch_weather_data(latitude, longitude)
        formatted_data = format_weather_data(data)
        formatted_data = {
            key: (value.isoformat() if isinstance(value, datetime.datetime) else value)
            for key, value in formatted_data.items()
        }
        
        redis_client.setex(cache_key, CACHE_EXPIRATION, json.dumps(formatted_data))
        return jsonify(formatted_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/weather/summarize", methods=["GET"])
def summarize_weather():
    latitude = request.args.get("latitude", type=float)
    longitude = request.args.get("longitude", type=float)

    if latitude is None or longitude is None:
        return jsonify({"error": "Missing latitude or longitude"}), 400
    
    cache_key = f"weather:summary:{latitude}:{longitude}"
    
    cached_summary = redis_client.get(cache_key)
    if cached_summary:
       return jsonify({"cache": True, "cached_summary": json.loads(cached_summary)})
    
    try:
        summary = summarize_weather_data(latitude, longitude)
        formatted_summary = format_weather_summary(summary)
        
        # Store the result in the cache
        redis_client.setex(cache_key, CACHE_EXPIRATION, json.dumps(formatted_summary))
        return jsonify(formatted_summary)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/batches", methods=["GET"])
def get_batches():
    cache_key = "batches"
    
    cached_batches = redis_client.get(cache_key)
    if cached_batches:
        return jsonify({"cache": True, "cached_batches": json.loads(cached_batches)})
    try:
        # Fetch batch data from the database
        batches = fetch_batches()
        formatted_batches = [{
            "batch_id": b.batch_id,
            "forecast_time": b.forecast_time.isoformat() if b.forecast_time else None,
            "number_of_rows": b.number_of_rows,
            "start_ingest_time": b.start_ingest_time.isoformat() if b.start_ingest_time else None,
            "end_ingest_time": b.end_ingest_time.isoformat() if b.end_ingest_time else None,
            "status": b.status,
        } for b in batches]
        
        # Store the result in the cache
        redis_client.setex(cache_key, CACHE_EXPIRATION, json.dumps(formatted_batches))
        return jsonify(formatted_batches)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# start app
initialize_system()
start_ingestion_service()