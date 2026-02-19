import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
CONFIG_PATH = PROJECT_ROOT / "config" / "ingestion.yml"

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

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

SOURCE = cfg.get("source", "adzuna")
COUNTRY = cfg.get("country", "us")

KEYWORDS = cfg["queries"]["keywords"]
LOCATIONS_FILE = cfg["queries"]["locations_file"]
LOCATIONS_PATH = PROJECT_ROOT / LOCATIONS_FILE

FRESH = cfg.get("fresh_sweep", {})
FRESH_RESULTS = int(FRESH.get("results_per_page", 10))
FRESH_SLEEP = float(FRESH.get("sleep_seconds_between_calls", 1))
KEYWORD_STATE_FILE = FRESH.get("keyword_state_file", "config/keyword_state.json")
KEYWORD_STATE_PATH = PROJECT_ROOT / KEYWORD_STATE_FILE

TIMEOUT = int(cfg["api"].get("timeout_seconds", 30))
SORT_BY = cfg["api"].get("sort_by", "date")


def read_locations():
    out = []
    with open(LOCATIONS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("#"):
                out.append(s)
    return out


def load_keyword_state():
    if KEYWORD_STATE_PATH.exists():
        try:
            return json.loads(KEYWORD_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {"next_keyword_index": 0}
    return {"next_keyword_index": 0}


def save_keyword_state(state):
    KEYWORD_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    KEYWORD_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def pick_keyword_for_run():
    st = load_keyword_state()
    idx = int(st.get("next_keyword_index", 0)) % len(KEYWORDS)
    kw = KEYWORDS[idx]
    st["next_keyword_index"] = (idx + 1) % len(KEYWORDS)
    save_keyword_state(st)
    return kw


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


def fetch_jobs(keyword, location):
    url = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search/1"
    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "what": keyword,
        "where": location,
        "results_per_page": FRESH_RESULTS,
        "sort_by": SORT_BY
    }
    r = requests.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def save_raw_overwrite(payload, location, keyword):
    safe_loc = location.replace(" ", "_")
    safe_keyword = keyword.replace(" ", "_")
    fname = f"{SOURCE}_FRESH_{safe_loc}_{safe_keyword}.json"  # ✅ overwrite-per-key
    with open(RAW_DIR / fname, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def main():
    keyword = pick_keyword_for_run()
    locations = read_locations()
    fetched_at = datetime.now(timezone.utc)

    print(f"[START] Fresh sweep | keyword='{keyword}' | locations={len(locations)}")

    total_rows = 0

    for loc in locations:
        payload = fetch_jobs(keyword, loc)

        # ✅ Overwrite snapshot for this (loc,keyword)
        save_raw_overwrite(payload, loc, keyword)

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
                "query_state": loc
            })

        if rows:
            insert_rows(rows)
            total_rows += len(rows)

        time.sleep(FRESH_SLEEP)

    print(f"[DONE] Fresh sweep complete | rows_processed={total_rows}")


if __name__ == "__main__":
    main()
