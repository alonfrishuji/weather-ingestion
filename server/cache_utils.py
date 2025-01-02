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
