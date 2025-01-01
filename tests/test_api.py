import pytest
import requests
from config import WEB_SERVICE_BASE_URL


@pytest.fixture
def test_server():
    """Fixture to ensure the server is running before tests."""   
    response = requests.get(f"{WEB_SERVICE_BASE_URL}/")
    print("Server Status Code:", response.status_code)
    assert response.status_code == 404 or response.status_code == 200, "Server is not running."

def test_weather_data_endpoint(test_server):
    """Test the /weather/data endpoint."""
    params = {
        "latitude": 90,
        "longitude": -178.5
    }
    response = requests.get(f"{WEB_SERVICE_BASE_URL}/weather/data", params=params)
    assert response.status_code == 200, "Failed to fetch weather data."
    data = response.json()
    assert isinstance(data, list), "Response should be a list of weather data."
    for item in data:
        assert "latitude" in item, "Missing 'latitude' in response item."
        assert "longitude" in item, "Missing 'longitude' in response item."
        assert "temperature" in item, "Missing 'temperature' in response item."
    print("Weather Data Response:", data)

def test_weather_summarize_endpoint(test_server):
    """Test the /weather/summarize endpoint."""
    params = {
        "latitude": 90,
        "longitude": -178.5
    }
    response = requests.get(f"{WEB_SERVICE_BASE_URL}/weather/summarize", params=params)
    assert response.status_code == 200, "Failed to fetch weather summary."
    summary = response.json()
    assert "temperature" in summary, "Temperature summary missing."
    assert "precipitation_rate" in summary, "Precipitation rate summary missing."
    assert "humidity" in summary, "Humidity summary missing."
    for metric in ["temperature", "precipitation_rate", "humidity"]:
        assert "max" in summary[metric], f"Missing 'max' in {metric} summary."
        assert "min" in summary[metric], f"Missing 'min' in {metric} summary."
        assert "avg" in summary[metric], f"Missing 'avg' in {metric} summary."
    print("Weather Summarize Response:", summary)

def test_batches_endpoint(test_server):
    """Test the /batches endpoint."""
    response = requests.get(f"{WEB_SERVICE_BASE_URL}/batches")
    assert response.status_code == 200, "Failed to fetch batches metadata."
    batches = response.json()
    assert isinstance(batches, list), "Response should be a list of batches."
    for batch in batches:
        assert "batch_id" in batch, "Missing 'batch_id' in batch metadata."
        assert "forecast_time" in batch, "Missing 'forecast_time' in batch metadata."
    print("Batches Metadata Response:", batches)
