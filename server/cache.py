import redis
import os

# Redis connection URL
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Create a Redis client
redis_client = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)
