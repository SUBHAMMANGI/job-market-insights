import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load env
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH)

DB_URL = os.getenv("DB_URL")
APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")

if not DB_URL:
    raise Exception("DB_URL missing in .env")
if not APP_ID or not APP_KEY:
    raise Exception("Adzuna API keys missing in .env")

engine = create_engine(DB_URL)

RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

SOURCE = "adzuna"
COUNTRY = "us"

# Keep your deep-scan list here (you can make config-driven later)
KEYWORDS = ["Data Analyst", "Business Intelligence", "Analytics"]
STATES = ["Texas", "California", "New York"]

RESULTS_PER_PAGE = 50
SLEEP_SECONDS = 2
TIMEOUT = 30
SORT_BY = "date"


def fetch_jobs(keyword, location):
    url = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search/1"
    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "what": keyword,
        "where": location,
        "results_per_page": RESULTS_PER_PAGE,
        "sort_by": SORT_BY
    }
    r = requests.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def insert_rows(rows):
    sql = text("""
        INSERT INTO raw_job_postings (
            source, fetched_at, job_id, title, company, location,
            description, posted_at, url, salary_min, salary_max, raw_json, query_state
        )
        VALUES (
            :source, :fetched_at, :job_id, :title, :company, :location,
            :description, :posted_at, :url, :salary_min, :salary_max,
            CAST(:raw_json AS jsonb), :query_state
        )
        ON CONFLICT (source, job_id) DO UPDATE SET
          fetched_at = EXCLUDED.fetched_at,
          title = EXCLUDED.title,
          company = EXCLUDED.company,
          location = EXCLUDED.location,
          description = EXCLUDED.description,
          posted_at = EXCLUDED.posted_at,
          url = EXCLUDED.url,
          salary_min = EXCLUDED.salary_min,
          salary_max = EXCLUDED.salary_max,
          raw_json = EXCLUDED.raw_json,
          query_state = EXCLUDED.query_state;
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)


def save_raw_overwrite(payload, state, keyword):
    safe_state = state.replace(" ", "_")
    safe_keyword = keyword.replace(" ", "_")
    fname = f"{SOURCE}_{safe_state}_{safe_keyword}.json"   # ✅ overwrite-per-key
    raw_path = RAW_DIR / fname
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def main():
    fetched_at = datetime.now(timezone.utc)
    total_rows = 0

    # Minimal logs
    print("[START] Deep scan ingestion")

    for state in STATES:
        for keyword in KEYWORDS:
            payload = fetch_jobs(keyword, state)

            # ✅ Overwrite raw snapshot for this (state,keyword)
            save_raw_overwrite(payload, state, keyword)

            rows = []
            for job in payload.get("results", []):
                rows.append({
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "job_id": str(job.get("id")),
                    "title": job.get("title"),
                    "company": (job.get("company") or {}).get("display_name"),
                    "location": (job.get("location") or {}).get("display_name"),
                    "description": job.get("description"),
                    "posted_at": job.get("created"),
                    "url": job.get("redirect_url"),
                    "salary_min": job.get("salary_min"),
                    "salary_max": job.get("salary_max"),
                    "raw_json": json.dumps(job),
                    "query_state": state
                })

            if rows:
                insert_rows(rows)
                total_rows += len(rows)

            time.sleep(SLEEP_SECONDS)

    print(f"[DONE] Deep scan complete | rows_processed={total_rows}")


if __name__ == "__main__":
    main()
