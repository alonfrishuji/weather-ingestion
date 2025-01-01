import os

# External API configuration
API_BASE_URL = os.getenv("API_BASE_URL", "https://us-east1-climacell-platform-production.cloudfunctions.net/weather-data")
BATCHES_ENDPOINT = f"{API_BASE_URL}/batches"
BATCH_DATA_ENDPOINT = f"{API_BASE_URL}/batches/{{batch_id}}"
WEB_SERVICE_BASE_URL = "https://weather-ingestion.onrender.com"

# Redis configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CACHE_TTL = int(os.getenv("CACHE_TTL", 300))  # 5 minutes

# Database settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 4000))