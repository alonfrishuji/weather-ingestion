from fastapi import FastAPI
from server.database import init_db
from server.models import WeatherData

app = FastAPI()

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    await init_db()

@app.get("/weather/data")
async def get_weather_data(latitude: float, longitude: float):
    # Dummy response for now
    return {"latitude": latitude, "longitude": longitude, "forecast": "Sunny"}
