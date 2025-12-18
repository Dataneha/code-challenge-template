from flask import Flask, request, jsonify
from flasgger import Swagger
import sqlite3

DB_PATH = "weather.db"

app = Flask(__name__)
Swagger(app)

def get_db():
    return sqlite3.connect(DB_PATH)

def paginate(query, params, page, limit):
    offset = (page - 1) * limit
    return f"{query} LIMIT {limit} OFFSET {offset}", params

@app.route("/api/weather", methods=["GET"])
def get_weather():
    """
    Get ingested weather data
    ---
    parameters:
      - name: station_id
        in: query
        type: string
      - name: start_date
        in: query
        type: string
      - name: end_date
        in: query
        type: string
      - name: page
        in: query
        type: integer
        default: 1
      - name: limit
        in: query
        type: integer
        default: 50
    responses:
      200:
        description: Weather records
    """
    station_id = request.args.get("station_id")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 50))

    query = """
        SELECT station_id, obs_date, max_temp_tenth_c, min_temp_tenth_c, precip_tenth_mm
        FROM weather WHERE 1=1
    """
    params = []

    if station_id:
        query += " AND station_id = ?"
        params.append(station_id)
    if start_date:
        query += " AND obs_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND obs_date <= ?"
        params.append(end_date)

    query, params = paginate(query, params, page, limit)

    conn = get_db()
    rows = conn.execute(query, params).fetchall()
    conn.close()

    return jsonify([
        {
            "station_id": r[0],
            "date": r[1],
            "max_temp_tenth_c": r[2],
            "min_temp_tenth_c": r[3],
            "precip_tenth_mm": r[4]
        } for r in rows
    ])

@app.route("/api/weather/stats", methods=["GET"])
def get_weather_stats():
    """
    Get yearly weather statistics
    ---
    parameters:
      - name: station_id
        in: query
        type: string
      - name: year
        in: query
        type: integer
      - name: page
        in: query
        type: integer
        default: 1
      - name: limit
        in: query
        type: integer
        default: 50
    responses:
      200:
        description: Weather statistics
    """
    station_id = request.args.get("station_id")
    year = request.args.get("year")
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 50))

    query = """
        SELECT station_id, year, avg_max_temp_c, avg_min_temp_c, total_precip_cm
        FROM weather_stats WHERE 1=1
    """
    params = []

    if station_id:
        query += " AND station_id = ?"
        params.append(station_id)
    if year:
        query += " AND year = ?"
        params.append(year)

    query, params = paginate(query, params, page, limit)

    conn = get_db()
    rows = conn.execute(query, params).fetchall()
    conn.close()

    return jsonify([
        {
            "station_id": r[0],
            "year": r[1],
            "avg_max_temp_c": r[2],
            "avg_min_temp_c": r[3],
            "total_precip_cm": r[4]
        } for r in rows
    ])

if __name__ == "__main__":
    app.run(debug=True)

