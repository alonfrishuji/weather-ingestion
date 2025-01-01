# **Guide to Testing the Weather Ingestion Service**

This guide will walk you through the steps to test the Weather Ingestion Service deployed on **Render**. You’ll learn how to interact with the available APIs and verify their functionality.

---

## **Base URL**

```arduino
https://weather-ingestion.onrender.com
```

---

## **Testing the APIs**

### **1. Get Weather Data**

Fetch weather data for a specific latitude and longitude.

- **Endpoint**: `/weather/data`
- **Method**: `GET`
- **Parameters**:
    - `latitude`: Latitude of the location (e.g., `40.7128` for New York City).
    - `longitude`: Longitude of the location (e.g., `74.0060` for New York City).
- **Example**:
    
    ```arduino
    GET https://weather-ingestion.onrender.com/batches/weather/data?latitude=40.7128&longitude=-74.0060
    
    ```
    
- **Expected Response**:
A JSON array of weather data records for the specified location:
    
    ```json
    
    [
        {
            "latitude": 40.7128,
            "longitude": -74.006,
            "forecast_time": "2025-01-01T12:00:00Z",
            "temperature": 15.0,
            "precipitation_rate": 0.1,
            "humidity": 60.0
        },
        {
            "latitude": 40.7128,
            "longitude": -74.006,
            "forecast_time": "2025-01-02T12:00:00Z",
            "temperature": 18.0,
            "precipitation_rate": 0.0,
            "humidity": 55.0
        }
    ]
    ```
    

---

### **2. Summarize Weather Data**

Fetch summary statistics (max, min, avg) for weather data at a specific location.

- **Endpoint**: `/weather/summarize`
- **Method**: `GET`
- **Parameters**:
    - `latitude`: Latitude of the location.
    - `longitude`: Longitude of the location.
- **Example**:
    
    ```arduino
    GET https://weather-ingestion.onrender.com/weather/summarize?latitude=40.7128&longitude=-74.0060
    
    ```
    
- **Expected Response**:
A JSON object summarizing temperature, precipitation rate, and humidity:
    
    ```json
    {
        "temperature": {
            "max": 18.0,
            "min": 15.0,
            "avg": 16.5
        },
        "precipitation_rate": {
            "max": 0.1,
            "min": 0.0,
            "avg": 0.05
        },
        "humidity": {
            "max": 60.0,
            "min": 55.0,
            "avg": 57.5
        }
    }
    ```
    

---

### **3. Get Batch Metadata**

Retrieve metadata for all ingested data batches.

- **Endpoint**: `/batches`
- **Method**: `GET`
- **Example**:
    
    ```arduino
    GET https://weather-ingestion.onrender.com/batches
    
    ```
    
- **Expected Response**:
A JSON array of batch metadata: