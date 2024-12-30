import asyncio
from server.ingestion_service import fetch_batches, fetch_batch_data

async def test_fetch_batches():
    batches = await fetch_batches()
    assert isinstance(batches, list), "Batches should be a list."
    print("Batches fetched:", batches)

async def test_fetch_batch_data():
    batch_id = "test_batch_id"
    page = 1
    data = await fetch_batch_data(batch_id, page)
    assert data is not None, "Batch data should not be None."
    assert "weather_data" in data, "'weather_data' key missing in response."
    print("Batch data fetched:", data)

if __name__ == "__main__":
    asyncio.run(test_fetch_batches())
    asyncio.run(test_fetch_batch_data())
