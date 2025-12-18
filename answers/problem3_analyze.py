import sqlite3
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
LOG = logging.getLogger("analyze")

DB_PATH = "weather.db"

def analyze():
    start = datetime.utcnow()
    LOG.info("Analysis started at %s UTC", start.isoformat())

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO weather_stats
        (station_id, year, avg_max_temp_c, avg_min_temp_c, total_precip_cm)
        SELECT
            station_id,
            CAST(strftime('%Y', obs_date) AS INTEGER) AS year,
            AVG(max_temp_tenth_c) / 10.0 AS avg_max_temp_c,
            AVG(min_temp_tenth_c) / 10.0 AS avg_min_temp_c,
            SUM(precip_tenth_mm) / 100.0 AS total_precip_cm
        FROM weather
        WHERE
            max_temp_tenth_c IS NOT NULL
            OR min_temp_tenth_c IS NOT NULL
            OR precip_tenth_mm IS NOT NULL
        GROUP BY station_id, year;
    """)

    conn.commit()
    conn.close()

    LOG.info("Analysis finished")

if __name__ == "__main__":
    analyze()

