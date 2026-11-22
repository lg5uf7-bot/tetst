"""
Step 2: Apply the 1,960 verified ATS configurations from data/final_verified_api_boards.json
onto the `companies` table in Supabase (run after `sync_companies.py`).
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from supabase_client import SupabaseREST, utc_now_iso

DEFAULT_VERIFIED_JSON = Path(__file__).resolve().parent.parent / "data" / "final_verified_api_boards.json"


def main() -> None:
    started_ts = time.time()
    json_path = Path(os.environ.get("VERIFIED_BOARDS_PATH", str(DEFAULT_VERIFIED_JSON)))
    if not json_path.exists():
        raise FileNotFoundError(f"Missing verified ATS file: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    sb = SupabaseREST()
    db_run_id = sb.start_run("apply-verified-ats")
    now = utc_now_iso()

    try:
        rows = []
        for rec in records:
            if not rec.get("id") or not rec.get("ats_provider") or not rec.get("ats_slug"):
                continue
            rows.append(
                {
                    "id": str(rec["id"]),
                    "source": str(rec.get("source") or "unknown"),
                    "name": str(rec.get("name") or rec["id"]),
                    "ats_provider": rec["ats_provider"],
                    "ats_slug": rec["ats_slug"],
                    "api_url": rec.get("api_url"),
                    "board_url": rec.get("board_url"),
                    "ats_active": True,
                    "open_jobs_count": int(rec.get("live_job_count") or rec.get("job_count") or 0),
                    "updated_at": now,
                }
            )

        upserted = sb.upsert_batch("companies", rows, on_conflict="id", batch_size=500)
        print(f"Applied verified ATS config to {upserted} companies.")

        by_provider: dict[str, int] = {}
        for r in rows:
            p = r["ats_provider"]
            by_provider[p] = by_provider.get(p, 0) + 1

        sb.finish_run(
            db_run_id,
            started_ts,
            status="success",
            companies_synced=upserted,
            stats_by_provider=by_provider,
        )
    except Exception as exc:
        sb.finish_run(
            db_run_id,
            started_ts,
            status="failed",
            error_log=[{"stage": "apply_verified_ats", "error": str(exc)}],
        )
        raise
    finally:
        sb.close()


if __name__ == "__main__":
    main()
