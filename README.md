## Solutions Overview

All challenge solutions are located in the `answers/` directory.

### Problem 1 – SQL Analysis
- File: `answers/problem1.sql`
- Contains SQL queries to analyze historical weather data.

### Problem 2 – Data Ingestion
- File: `answers/problem2_ingest.py`
- Ingests raw weather data into a SQLite database.
- Designed to be idempotent and safe to re-run.

### Problem 3 – Data Analysis
- File: `answers/problem3_analyze.py`
- Computes yearly weather statistics per station.

### Problem 4 – REST API
- File: `answers/problem4_api.py`
- Flask-based REST API to expose weather statistics.
- Run locally with:
  ```bash
  python3 answers/problem4_api.py
## Notes
- The generated SQLite database (`weather.db`) is intentionally excluded from version control.

