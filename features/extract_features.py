import os
import re
import json
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal

import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
SKILLS_PATH = PROJECT_ROOT / "config" / "skills.yml"

load_dotenv(dotenv_path=ENV_PATH)

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise Exception("DB_URL missing in .env")

engine = create_engine(DB_URL)

if not SKILLS_PATH.exists():
    raise Exception(f"Missing skills config: {SKILLS_PATH}")

with open(SKILLS_PATH, "r", encoding="utf-8") as f:
    skills_cfg = yaml.safe_load(f) or {}

skills_map = skills_cfg.get("skills", {})
if not isinstance(skills_map, dict) or not skills_map:
    raise Exception("config/skills.yml must be a dict of canonical_skill -> [aliases]")

# Compile alias regex per canonical skill
compiled = []
for canonical, aliases in skills_map.items():
    if not aliases:
        continue
    canonical_norm = str(canonical).strip().lower()
    alias_list = [str(a).strip().lower() for a in aliases if str(a).strip()]
    alias_patterns = []
    for a in alias_list:
        pat = re.escape(a).replace(r"\ ", r"\s+")
        alias_patterns.append(pat)
    rx = re.compile(r"(?i)(?<!\w)(" + "|".join(alias_patterns) + r")(?!\w)")
    compiled.append((canonical_norm, rx))


def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[/,_\-|]", " ", s)
    s = re.sub(r"[^\w\s\+]", " ", s)  # keep + for years patterns
    s = re.sub(r"\s+", " ", s).strip()
    return s


def classify_role_family(title: str) -> str:
    t = (title or "").lower()
    if "data engineer" in t or "etl" in t or "pipeline" in t:
        return "Data Engineering"
    if "data scientist" in t or "machine learning" in t or re.search(r"\bml\b", t):
        return "Data Science"
    if "business intelligence" in t or "power bi" in t or "tableau" in t or re.search(r"\bbi\b", t):
        return "Business Intelligence"
    if "analyst" in t or "analytics" in t:
        return "Analytics"
    return "Other"


def infer_seniority(title: str) -> str:
    t = (title or "").lower()
    if any(x in t for x in ["intern", "co-op", "student"]):
        return "Intern"
    if any(x in t for x in ["director", "head", "vp", "vice president", "manager"]):
        return "Management"
    if any(x in t for x in ["principal", "staff", "lead", "senior", "sr.", "sr "]):
        return "Senior"
    if any(x in t for x in ["junior", "jr.", "entry", "associate", "new grad"]):
        return "Entry"
    return "Mid"


def infer_remote(text_blob: str) -> bool:
    t = (text_blob or "").lower()
    remote_signals = ["remote", "work from home", "wfh", "telecommute", "fully remote", "100% remote", "anywhere"]
    return any(s in t for s in remote_signals)


def extract_years_experience(text_blob: str):
    t = normalize_text(text_blob or "")
    patterns = [
        r"(\d{1,2})\s*\+\s*(?:years|yrs)\b",
        r"minimum\s+(\d{1,2})\s*(?:years|yrs)\b",
        r"at\s+least\s+(\d{1,2})\s*(?:years|yrs)\b",
        r"(\d{1,2})\s*(?:years|yrs)\s+of\s+experience",
    ]
    hits = []
    for p in patterns:
        for m in re.finditer(p, t):
            try:
                hits.append(int(m.group(1)))
            except:
                pass
    return Decimal(min(hits)) if hits else None


def extract_skills(text_blob: str):
    t = normalize_text(text_blob or "")
    found = []
    for canonical, rx in compiled:
        if rx.search(t):
            found.append(canonical)
    return found, found[:10]


def baseline_skills_for_role(role_family: str):
    """
    These are baseline expectations, not claims from the posting.
    Keeps the model honest and still useful for dashboards.
    """
    role = (role_family or "Other").strip()

    mapping = {
        "Analytics": ["sql", "excel"],
        "Business Intelligence": ["sql", "power bi", "tableau"],
        "Data Engineering": ["sql", "python", "etl", "cloud"],
        "Data Science": ["python", "statistics", "machine learning"],
        "Other": []
    }
    return mapping.get(role, [])


def main():
    print("[START] Feature extraction started")

    extracted_at = datetime.now(timezone.utc)

    # Recompute for ALL jobs (keeps it consistent after logic changes)
    select_sql = text("""
        SELECT job_id, title, city, state, location_raw, description_clean
        FROM job_postings_clean
        LIMIT 50000;
    """)

    with engine.begin() as conn:
        rows = conn.execute(select_sql).mappings().all()

    print(f"[INFO] Jobs fetched for feature extraction: {len(rows)}")

    out = []
    for r in rows:
        title = r["title"] or ""
        desc = r["description_clean"] or ""
        loc_raw = r["location_raw"] or ""
        blob = f"{title}\n{loc_raw}\n{desc}"

        role_family = classify_role_family(title)
        seniority = infer_seniority(title)
        is_remote = infer_remote(blob)
        yoe_min = extract_years_experience(blob)

        skills_found, top_skills = extract_skills(blob)
        skills_count = int(len(skills_found))
        has_explicit = skills_count > 0
        baseline = baseline_skills_for_role(role_family)

        out.append({
            "job_id": r["job_id"],
            "extracted_at": extracted_at,
            "state": r["state"],
            "city": r["city"],
            "role_family": role_family,
            "seniority": seniority,
            "is_remote": is_remote,
            "years_experience_min": yoe_min,
            "skills": json.dumps(skills_found),
            "skills_count": skills_count,
            "top_skills": json.dumps(top_skills),
            "has_explicit_skills": has_explicit,
            "role_baseline_skills": json.dumps(baseline),
        })

    upsert_sql = text("""
        INSERT INTO job_postings_features (
          job_id, extracted_at, state, city, role_family, seniority,
          is_remote, years_experience_min, skills, skills_count, top_skills,
          has_explicit_skills, role_baseline_skills
        )
        VALUES (
          :job_id, :extracted_at, :state, :city, :role_family, :seniority,
          :is_remote, :years_experience_min, CAST(:skills AS jsonb), :skills_count,
          CAST(:top_skills AS jsonb), :has_explicit_skills, CAST(:role_baseline_skills AS jsonb)
        )
        ON CONFLICT (job_id) DO UPDATE SET
          extracted_at = EXCLUDED.extracted_at,
          state = EXCLUDED.state,
          city = EXCLUDED.city,
          role_family = EXCLUDED.role_family,
          seniority = EXCLUDED.seniority,
          is_remote = EXCLUDED.is_remote,
          years_experience_min = EXCLUDED.years_experience_min,
          skills = EXCLUDED.skills,
          skills_count = EXCLUDED.skills_count,
          top_skills = EXCLUDED.top_skills,
          has_explicit_skills = EXCLUDED.has_explicit_skills,
          role_baseline_skills = EXCLUDED.role_baseline_skills;
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, out)

    print(f"[DONE] Features upserted: {len(out)}")


if __name__ == "__main__":
    main()
