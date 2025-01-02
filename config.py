import os

# External API configuration
API_BASE_URL = os.getenv("API_BASE_URL", "https://us-east1-climacell-platform-production.cloudfunctions.net/weather-data")
BATCHES_ENDPOINT = f"{API_BASE_URL}/batches"
BATCH_DATA_ENDPOINT = f"{API_BASE_URL}/batches/{{batch_id}}"
WEB_SERVICE_BASE_URL = "https://weather-ingestion.onrender.com"

# Database settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 4000))
