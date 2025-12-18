import os
import sqlite3
import logging
from datetime import datetime

MISSING = -9999

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
LOG = logging.getLogger("ingest")

DB_PATH = os.getenv("DB_PATH", "weather.db")
WX_DIR = os.getenv("WX_DIR", "../wx_data")

DDL_PATH = os.path.join(os.path.dirname(__file__), "problem1.sql")

def to_nullable_int(v):
    n = int(v)
    return None if n == MISSING else n

def yyyymmdd_to_iso(d):
    return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"

def init_db(conn):
    with open(DDL_PATH, "r") as f:
        conn.executescript(f.read())
    conn.commit()

def ingest():
    start = datetime.utcnow()
    LOG.info("Ingestion started at %s UTC", start.isoformat())

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    inserted = 0
    try:
        for fname in sorted(os.listdir(WX_DIR)):
            fpath = os.path.join(WX_DIR, fname)
            if not os.path.isfile(fpath):
                continue

            station_id = os.path.splitext(fname)[0]
            rows = []

            with open(fpath) as f:
                for line in f:
                    parts = line.strip().split("\t")
                    if len(parts) != 4:
                        continue
                    rows.append((
                        station_id,
                        yyyymmdd_to_iso(parts[0]),
                        to_nullable_int(parts[1]),
                        to_nullable_int(parts[2]),
                        to_nullable_int(parts[3]),
                    ))

            cur = conn.cursor()
            cur.executemany(
                """
                INSERT OR IGNORE INTO weather
                (station_id, obs_date, max_temp_tenth_c, min_temp_tenth_c, precip_tenth_mm)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows
            )
            inserted += cur.rowcount if cur.rowcount != -1 else 0
            conn.commit()

        LOG.info("Ingestion finished. Records inserted: %d", inserted)
    finally:
        conn.close()

if __name__ == "__main__":
    ingest()

