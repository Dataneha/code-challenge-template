-- Problem 1: Data Modeling (SQLite)

CREATE TABLE IF NOT EXISTS weather (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  station_id TEXT NOT NULL,
  obs_date TEXT NOT NULL,
  max_temp_tenth_c INTEGER,
  min_temp_tenth_c INTEGER,
  precip_tenth_mm INTEGER,
  CONSTRAINT uq_station_date UNIQUE (station_id, obs_date)
);

CREATE INDEX IF NOT EXISTS idx_weather_station_date
  ON weather(station_id, obs_date);

CREATE TABLE IF NOT EXISTS weather_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  station_id TEXT NOT NULL,
  year INTEGER NOT NULL,
  avg_max_temp_c REAL,
  avg_min_temp_c REAL,
  total_precip_cm REAL,
  CONSTRAINT uq_station_year UNIQUE (station_id, year)
);
 
