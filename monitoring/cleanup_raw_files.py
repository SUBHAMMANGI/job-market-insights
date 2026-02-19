from pathlib import Path
from datetime import datetime, timedelta
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

# 🔧 Retention period
RETENTION_DAYS = 10

def main():
    if not RAW_DIR.exists():
        print("[SKIP] Raw directory does not exist")
        return

    cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)

    deleted = 0
    kept = 0

    for f in RAW_DIR.glob("*.json"):
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime)
            if mtime < cutoff:
                os.remove(f)
                deleted += 1
            else:
                kept += 1
        except Exception as e:
            print(f"[WARN] Failed to process {f.name}: {e}")

    print(f"[DONE] Raw cleanup complete | deleted={deleted} | kept={kept}")

if __name__ == "__main__":
    main()
