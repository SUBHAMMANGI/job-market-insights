import subprocess
import traceback
import re
from pathlib import Path
from datetime import datetime

# -----------------------
# Paths
# -----------------------
PROJECT_ROOT = Path(__file__).resolve().parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"pipeline_{RUN_TS}.log"

# Use venv python ALWAYS (Windows)
VENV_PY = r"venv\Scripts\python.exe"


# -----------------------
# Logging helper
# -----------------------
def log_line(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")


# -----------------------
# Run step helper
# -----------------------
def run_step(cmd: str, label: str):
    start = datetime.now()

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        shell=True,
        capture_output=True,
        text=True
    )

    end = datetime.now()
    secs = int((end - start).total_seconds())

    stdout = result.stdout or ""
    stderr = result.stderr or ""

    if result.returncode != 0:
        log_line(f"FAIL {label} | secs={secs} | exit_code={result.returncode}")
        log_line("---- STDOUT ----")
        for line in stdout.splitlines():
            log_line(line)
        log_line("---- STDERR ----")
        for line in stderr.splitlines():
            log_line(line)
        raise RuntimeError(f"{label} failed")

    summary = parse_summary(label, stdout)
    log_line(f"OK  {label:<12} | secs={secs} | {summary}")


# -----------------------
# Compact summaries
# -----------------------
def parse_summary(label: str, stdout: str) -> str:
    if label == "RawCleanup":
        m1 = re.search(r"deleted=(\d+)", stdout)
        m2 = re.search(r"kept=(\d+)", stdout)
        d = m1.group(1) if m1 else "?"
        k = m2.group(1) if m2 else "?"
        return f"deleted={d} | kept={k}"

    if label == "FreshSweep":
        m1 = re.search(r"locations=(\d+)", stdout)
        m2 = re.search(r"rows_processed=(\d+)", stdout)
        locs = m1.group(1) if m1 else "?"
        rows = m2.group(1) if m2 else "?"
        return f"locations={locs} | rows={rows}"

    if label == "Ingestion":
        m = re.search(r"rows_processed=(\d+)", stdout)
        return f"rows={m.group(1)}" if m else "rows=?"

    if label == "Cleaning":
        m1 = re.search(r"Raw rows fetched:\s*(\d+)", stdout)
        m2 = re.search(r"Clean rows upserted:\s*(\d+)", stdout)
        raw = m1.group(1) if m1 else "?"
        up = m2.group(1) if m2 else "?"
        return f"raw={raw} | clean={up}"

    if label == "Metrics":
        m1 = re.search(r"Rows fetched:\s*(\d+)", stdout)
        m2 = re.search(r"Aggregated rows:\s*(\d+)", stdout)
        clean = m1.group(1) if m1 else "?"
        agg = m2.group(1) if m2 else "?"
        return f"clean={clean} | days={agg}"

    return "ok"


# -----------------------
# Main pipeline
# -----------------------
def main():
    log_line(f"RUN START | log={LOG_FILE.name}")

    try:
        # 0️⃣ Raw retention (delete files ≥ 10 days old)
        run_step(fr"{VENV_PY} monitoring\cleanup_raw_files.py", "RawCleanup")

        # 1️⃣ Fresh sweep (all states × 1 rotating keyword)
        run_step(fr"{VENV_PY} ingestion\fresh_sweep.py", "FreshSweep")

        # 2️⃣ Deep scan (few states × all keywords)
        run_step(fr"{VENV_PY} ingestion\fetch_jobs.py", "Ingestion")

        # 3️⃣ Clean + enrich
        run_step(fr"{VENV_PY} processing\clean_transform.py", "Cleaning")

        # 4️⃣ Metrics
        run_step(fr"{VENV_PY} warehouse\build_daily_metrics.py", "Metrics")

        log_line("RUN SUCCESS ✅")

    except Exception as e:
        log_line(f"RUN FAILED ❌ | reason={str(e)}")
        log_line("TRACEBACK:")
        for line in traceback.format_exc().splitlines():
            log_line(line)
        raise

    finally:
        log_line("RUN END")


if __name__ == "__main__":
    main()
