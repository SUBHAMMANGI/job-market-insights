import os
import re
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# -----------------------
# Load .env (Windows-safe)
# -----------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH)

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise Exception("DB_URL missing in .env")

engine = create_engine(DB_URL)

# -----------------------
# State mapping
# -----------------------
US_STATES = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut",
    "DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
    "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan",
    "MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire",
    "NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
    "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee","TX":"Texas",
    "UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming"
}
STATE_NAMES = set(US_STATES.values())
STATE_ABBRS = set(US_STATES.keys())

# -----------------------
# Helpers
# -----------------------
def clean_text(html_text: str) -> str:
    """Remove HTML tags and normalize whitespace."""
    if not html_text:
        return None
    text_only = re.sub(r"<[^>]+>", " ", html_text)
    text_only = re.sub(r"\s+", " ", text_only).strip()
    return text_only

def parse_city_state(location_raw: str):
    """
    Extracts city/state from:
    - 'Dallas, Texas'
    - 'New York, NY'
    - 'Austin TX'
    If not found, returns (city, None)
    """
    if not location_raw:
        return (None, None)

    loc = location_raw.strip()
    parts = [p.strip() for p in loc.split(",") if p.strip()]

    city = None
    state = None

    if len(parts) >= 2:
        city = parts[0]
        maybe_state = parts[1]

        if maybe_state in STATE_ABBRS:
            state = US_STATES[maybe_state]
        elif maybe_state in STATE_NAMES:
            state = maybe_state
        else:
            token = maybe_state.split()[0].strip()
            if token in STATE_ABBRS:
                state = US_STATES[token]
            elif token in STATE_NAMES:
                state = token
    else:
        # Single part: try to detect trailing 'TX' etc.
        tokens = loc.split()
        if tokens:
            last = tokens[-1].strip()
            if last in STATE_ABBRS:
                state = US_STATES[last]
                city = " ".join(tokens[:-1]).strip() or None
            else:
                # If it's only city like "Austin", keep it as city; state will be filled by fallback
                city = loc

    return (city, state)

def compute_salary_mid(smin, smax):
    try:
        if smin is not None and smax is not None:
            return (float(smin) + float(smax)) / 2.0
    except:
        return None
    return None

# -----------------------
# Main
# -----------------------
def main():
    print("[INFO] Starting Phase 2 clean transform...")

    # Pull raw rows INCLUDING query_state
    raw_sql = text("""
        SELECT
          source, fetched_at, job_id, title, company, location, description,
          posted_at, url, salary_min, salary_max, query_state
        FROM raw_job_postings
    """)

    with engine.begin() as conn:
        raw_rows = conn.execute(raw_sql).mappings().all()

    print(f"[INFO] Raw rows fetched: {len(raw_rows)}")

    cleaned = []
    for r in raw_rows:
        city, state = parse_city_state(r["location"])

        # ✅ Fallback: if state cannot be parsed, use query_state (most reliable)
        if not state and r.get("query_state"):
            state = r["query_state"]

        desc_clean = clean_text(r["description"])
        salary_mid = compute_salary_mid(r["salary_min"], r["salary_max"])

        cleaned.append({
            "job_id": r["job_id"],
            "source": r["source"],
            "fetched_at": r["fetched_at"],
            "posted_at": r["posted_at"],
            "title": r["title"],
            "company": r["company"],
            "location_raw": r["location"],
            "city": city,
            "state": state,
            "url": r["url"],
            "salary_min": r["salary_min"],
            "salary_max": r["salary_max"],
            "salary_mid": salary_mid,
            "description_clean": desc_clean
        })

    upsert_sql = text("""
        INSERT INTO job_postings_clean (
          job_id, source, fetched_at, posted_at, title, company, location_raw,
          city, state, url, salary_min, salary_max, salary_mid, description_clean
        )
        VALUES (
          :job_id, :source, :fetched_at, :posted_at, :title, :company, :location_raw,
          :city, :state, :url, :salary_min, :salary_max, :salary_mid, :description_clean
        )
        ON CONFLICT (job_id) DO UPDATE SET
          source = EXCLUDED.source,
          fetched_at = EXCLUDED.fetched_at,
          posted_at = EXCLUDED.posted_at,
          title = EXCLUDED.title,
          company = EXCLUDED.company,
          location_raw = EXCLUDED.location_raw,
          city = EXCLUDED.city,
          state = EXCLUDED.state,
          url = EXCLUDED.url,
          salary_min = EXCLUDED.salary_min,
          salary_max = EXCLUDED.salary_max,
          salary_mid = EXCLUDED.salary_mid,
          description_clean = EXCLUDED.description_clean;
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, cleaned)

    print(f"[DONE] Clean rows upserted: {len(cleaned)}")
    print("[NEXT] Verify state fill: SELECT state, COUNT(*) FROM job_postings_clean GROUP BY state;")

if __name__ == "__main__":
    main()
