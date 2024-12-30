from flask import Flask, request, jsonify
from server.database import SessionLocal
from server.models import WeatherData, BatchMetadata
from sqlalchemy.sql import func

app = Flask(__name__)

@app.route("/weather/data", methods=["GET"])
def get_weather_data():
    latitude = request.args.get("latitude", type=float)
    longitude = request.args.get("longitude", type=float)

    session = SessionLocal()
    try:
        data = session.query(WeatherData).filter_by(latitude=latitude, longitude=longitude).all()
        return jsonify([{
            "latitude": d.latitude,
            "longitude": d.longitude,
            "forecast_time": d.forecast_time,
            "temperature": d.temperature,
            "precipitation_rate": d.precipitation_rate,
            "humidity": d.humidity
        } for d in data])
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

@app.route("/weather/summarize", methods=["GET"])
def summarize_weather_data():
    latitude = request.args.get("latitude", type=float)
    longitude = request.args.get("longitude", type=float)

    if latitude is None or longitude is None:
        return jsonify({"error": "Missing latitude or longitude"}), 400

    session = SessionLocal()
    try:
        # Query for summary statistics
        summary = session.query(
            func.max(WeatherData.temperature).label("max_temperature"),
            func.min(WeatherData.temperature).label("min_temperature"),
            func.avg(WeatherData.temperature).label("avg_temperature"),
            func.max(WeatherData.precipitation_rate).label("max_precipitation_rate"),
            func.min(WeatherData.precipitation_rate).label("min_precipitation_rate"),
            func.avg(WeatherData.precipitation_rate).label("avg_precipitation_rate"),
            func.max(WeatherData.humidity).label("max_humidity"),
            func.min(WeatherData.humidity).label("min_humidity"),
            func.avg(WeatherData.humidity).label("avg_humidity"),
        ).filter(
            WeatherData.latitude == latitude,
            WeatherData.longitude == longitude
        ).one()

        # Format the response
        result = {
            "temperature": {
                "max": summary.max_temperature,
                "min": summary.min_temperature,
                "avg": summary.avg_temperature,
            },
            "precipitation_rate": {
                "max": summary.max_precipitation_rate,
                "min": summary.min_precipitation_rate,
                "avg": summary.avg_precipitation_rate,
            },
            "humidity": {
                "max": summary.max_humidity,
                "min": summary.min_humidity,
                "avg": summary.avg_humidity,
            },
        }
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()
  
@app.route("/batches", methods=["GET"])
def get_batches():
    session = SessionLocal()
    try:
        batches = session.query(BatchMetadata).all()
        return jsonify([{
            "batch_id": b.batch_id,
            "forecast_time": b.forecast_time,
            "number_of_rows": b.number_of_rows,
            "start_ingest_time": b.start_ingest_time,
            "end_ingest_time": b.end_ingest_time,
            "status": b.status,
        } for b in batches])
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()