import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH)

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise Exception("DB_URL missing")

engine = create_engine(DB_URL)

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"monitoring_{RUN_TS}.log"


def log_line(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")


def log_alert(alert_type, severity, details):
    sql = text("""
        INSERT INTO job_monitoring_alerts (
          detected_at, alert_type, severity, details
        )
        VALUES (
          :detected_at, :alert_type, :severity, :details
        );
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "detected_at": datetime.now(timezone.utc),
            "alert_type": alert_type,
            "severity": severity,
            "details": details
        })

    log_line(f"ALERT | type={alert_type} severity={severity} details={details}")


def to_float(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def main():
    log_line("========== MONITORING RUN START ==========")

    now = datetime.now(timezone.utc)

    # ---- Freshness & pipeline status ----
    q_last = text("""
        SELECT ended_at, status
        FROM pipeline_runs
        WHERE pipeline_name='daily_metrics'
        ORDER BY run_id DESC
        LIMIT 1;
    """)

    with engine.begin() as conn:
        row = conn.execute(q_last).mappings().first()

    if not row:
        log_alert("freshness", "HIGH", "No pipeline run found.")
        log_line("END (no pipeline run)")
        return

    ended_at = row["ended_at"]
    status = row["status"]

    if ended_at and ended_at.tzinfo is None:
        ended_at = ended_at.replace(tzinfo=timezone.utc)

    log_line(f"Last pipeline | status={status} ended_at={ended_at}")

    if status != "SUCCESS":
        log_alert("pipeline_status", "HIGH", f"Status={status}")

    if ended_at and (now - ended_at) > timedelta(minutes=20):
        mins = int((now - ended_at).total_seconds() / 60)
        log_alert("freshness", "MEDIUM", f"Last update {mins} minutes ago")

    # ---- Volume anomaly ----
    q_vol = text("""
        WITH daily AS (
          SELECT dt, SUM(jobs_posted) AS total_jobs
          FROM job_daily_metrics
          GROUP BY dt
        ),
        last7 AS (
          SELECT AVG(total_jobs) AS avg_jobs
          FROM daily
          WHERE dt >= CURRENT_DATE - INTERVAL '7 days'
        ),
        today AS (
          SELECT total_jobs AS today_jobs
          FROM daily
          WHERE dt = CURRENT_DATE
        )
        SELECT
          (SELECT avg_jobs FROM last7) AS avg_jobs,
          (SELECT today_jobs FROM today) AS today_jobs;
    """)

    with engine.begin() as conn:
        v = conn.execute(q_vol).mappings().first()

    avg_jobs = to_float(v["avg_jobs"]) if v else None
    today_jobs = to_float(v["today_jobs"]) if v else None

    log_line(f"Volume | avg_7d={avg_jobs} today={today_jobs}")

    if avg_jobs and today_jobs:
        if today_jobs < 0.5 * avg_jobs:
            log_alert("volume_drop", "MEDIUM", f"{today_jobs} < 50% of avg {avg_jobs:.1f}")
        elif today_jobs > 2.0 * avg_jobs:
            log_alert("volume_spike", "MEDIUM", f"{today_jobs} > 200% of avg {avg_jobs:.1f}")

    log_line("MONITORING RUN SUCCESS ✅")
    log_line("========== MONITORING RUN END ==========")


if __name__ == "__main__":
    main()
