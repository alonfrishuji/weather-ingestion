# ⚡ Weather Ingestion Project 
author: Alon Frishberg  

## Overview
This project ingests weather data from an external provider, processes it, and provides APIs to:
- Retrieve the weather forecast for a specific location.
- Summarize weather data (max, min, average) for a location.
- View metadata about ingested weather data batches.

The backend is built using **Flask** and uses a **PostgreSQL** database for data storage.

Below are some useful links:

- [Usage Guide](usage.md)
- [Local Testing Guide](local_testing.md)






##  Ideas to improve Performance and Pitfalls:
**Clustered Index**

Cluster the data physically based on the order of an index to speed up range queries.

```sql
CLUSTER weather_data USING ix_weather_lat_lon_time;
```
Use Case: Optimize range queries, such as:
```sql
SELECT * FROM weather_data
WHERE forecast_time BETWEEN '2024-01-01T12:00:00' AND '2024-01-01T18:00:00';
```
 **Using Batched Concurrency for efficient insertion handling** 

```python
from asyncio import Semaphore

async def ingest_batch_limited(batch, semaphore: Semaphore):
    async with semaphore:
        await ingest_batch(batch)

async def process_batches() -> None:
    """Fetch and process all batches."""
    batches = await fetch_batches()
    sorted_batches = sorted(batches, key=lambda x: isoparse(x["forecast_time"]))

    semaphore = Semaphore(5)  # Allow up to 5 concurrent tasks
    tasks = [ingest_batch_limited(batch, semaphore) for batch in sorted_batches]
    await asyncio.gather(*tasks)
```
**Redis implemenation caching API Responses**

**Why**:

 To reduce redundant calls to external APIs or the database, ensuring faster response times for frequently requested data.
### Where:
**fetch_batches**: Cache the results of this function to avoid fetching the same batch data repeatedly from the database or external API.

**fetch_weather_data**: Cache weather data based on latitude and longitude to minimize database queries for repeated locations.

- [Redis implementation](/redis_impl.md) 

## Preventive Measures for database performance
### Monitor Disk Usage Regularly

Use database monitoring tools provided by Render or integrate third-party monitoring (e.g., Datadog, New Relic).

###  Set Data Retention Policies
For time-series data (like weather), remove older data periodically:
```sql
DELETE FROM weather_data WHERE forecast_time < NOW() - INTERVAL '30 days';
```
### Schedule Maintenance

Scheduling regular VACUUM and REINDEX operations to clean up dead tuples and optimize indexes.

### Efficient Indexing

Ensuring queries are optimized and using the right indexes to avoid unnecessary writes.


## Clear Unnecessary Data
Once you identify large tables or unused indexes -> clear or delete data.

```sql
DELETE FROM weather_data WHERE forecast_time < NOW() - INTERVAL '30 days';
```

## Scalability and Performance Considerations

In this project, I focused on designing a system that is scalable and efficient for handling large datasets.

## Infrastructure Optimization:
 I would fine-tune the database settings to handle high-throughput operations by implementing efficient connection pooling, optimizing queries, and using bulk inserts to reduce transaction overhead.
## Backend Framework: 
I chose FastAPI as the backend framework because of its asynchronous support, high performance, and rich ecosystem. FastAPI allows me to handle concurrent requests effectively while integrating seamlessly with tools like Pydantic and OpenAPI for better validation and documentation.
## Database Performance:
 To optimize database performance, I would go beyond just using indexes. Strategies like query caching, denormalization for read-heavy operations, table partitioning for large datasets, and leveraging database-specific features such as materialized views or JSONB for semi-structured data would be implemented.

## Conclusion
These decisions reflect my focus on building a robust, efficient, and scalable system that can adapt to growing data and user demands.