# Weather Ingestion Project

## Overview
This project ingests weather data from an external provider, processes it, and provides APIs to:
- Retrieve the weather forecast for a specific location.
- Summarize weather data (max, min, average) for a location.
- View metadata about ingested weather data batches.

The backend is built using **Flask** and uses a **PostgreSQL** database for data storage.

---

## Features
1. **Weather Data Ingestion**: Ingest weather data batches from an external API.
2. **API Endpoints**:
   - `/weather/data`: Retrieve weather forecast for a specific location.
   - `/weather/summarize`: Get summarized weather data (max, min, avg).
   - `/batches/`: View metadata about ingested weather data batches.
3. **Database Management**:
   - Retains only the latest three active data batches.
   - Retains metadata for all ingested batches, including deleted ones.

---

## Prerequisites
1. **Python 3.9+**
2. **PostgreSQL Database**
3. **Pipenv** or **pip** for package management (recommended).

## how to use it ? 
 `pip install -r requirements.txt`
2. 
3. Create a database:
    `createdb weather_db`

4. Update the connection string in server/database.py:
    DATABASE_URL = "postgresql://user:password@localhost/weather_db"
5. run the initialization script to create tables:
    `python server/database.py`

6.  Run the Ingestion Script
    `python server/ingestion_service.py`
7. start the server : 
    `gunicorn server.main:app --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000`


## Usage: 
API Endpoints
*Get Weather Data for a Location*
GET /weather/data?latitude=40.7128&longitude=-74.0060

*Summarize Weather Data*
GET /weather/summarize?latitude=40.7128&longitude=-74.0060

*View Batch Metadata*
GET /batches/


Running Tests
- pytests /tests

## Project Structure
