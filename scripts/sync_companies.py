"""
Step 1: Sync all ~24,077 companies directly from yigitmeteozcan/startups/data/all.json
into Supabase `companies`.

Important: On conflict (`id`), PostgREST only updates the fields present in this
payload — so existing `ats_provider`, `ats_slug`, `api_url`, `board_url`, and
`ats_active` values are preserved safely across daily runs.
"""

from __future__ import annotations

import os
import time
from urllib.parse import urlparse
import httpx

from supabase_client import SupabaseREST, utc_now_iso

ALL_JSON_URL = os.environ.get(
    "ALL_JSON_URL",
    "https://raw.githubusercontent.com/yigitmeteozcan/startups/main/data/all.json",
)


def extract_domain(website: str | None) -> str | None:
    if not website or not isinstance(website, str):
        return None
    w = website.strip()
    if not w:
        return None
    if not w.startswith(("http://", "https://")):
        w = "https://" + w
    try:
        host = urlparse(w).netloc.lower().split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        return host or None
    except Exception:
        return None


def map_company(raw: dict, synced_at: str) -> dict:
    website = raw.get("website") or None
    return {
        "id": str(raw["id"]),
        "source": str(raw.get("source") or "unknown"),
        "name": str(raw.get("name") or raw["id"]),
        "description": raw.get("description") or None,
        "logo": raw.get("logo") or None,
        "website": website,
        "domain": extract_domain(website),
        "source_url": raw.get("sourceUrl") or None,
        "location": raw.get("location") or None,
        "country": raw.get("country") or None,
        "region": raw.get("region") or None,
        "tags": raw.get("tags") if isinstance(raw.get("tags"), list) else [],
        "year": raw.get("year") if isinstance(raw.get("year"), int) else None,
        "program": raw.get("program") or None,
        "status": raw.get("status") or "unknown",
        "is_exit": bool(raw.get("isExit", False)),
        "is_unicorn": bool(raw.get("isUnicorn", False)),
        "is_hiring": bool(raw.get("isHiring", False)),
        "founders": raw.get("founders") if isinstance(raw.get("founders"), list) else [],
        "social": raw.get("social") if isinstance(raw.get("social"), dict) else {},
        "last_all_json_sync_at": synced_at,
        "updated_at": synced_at,
    }


def main() -> None:
    started_ts = time.time()
    sb = SupabaseREST()
    db_run_id = sb.start_run("sync-companies")

    try:
        print(f"Downloading {ALL_JSON_URL} ...")
        with httpx.Client(timeout=60.0, follow_redirects=True) as http:
            resp = http.get(ALL_JSON_URL)
            resp.raise_for_status()
            raw_companies = resp.json()

        print(f"Fetched {len(raw_companies)} companies from all.json")

        # Count how many IDs already exist so we can report new_companies_discovered
        existing_rows = sb.select_all("companies", select="id")
        existing_ids = {r["id"] for r in existing_rows}

        synced_at = utc_now_iso()
        mapped = [map_company(r, synced_at) for r in raw_companies if r.get("id")]
        new_count = sum(1 for r in mapped if r["id"] not in existing_ids)

        upserted = sb.upsert_batch("companies", mapped, on_conflict="id", batch_size=500)
        print(f"Upserted {upserted} companies ({new_count} newly added).")

        sb.finish_run(
            db_run_id,
            started_ts,
            status="success",
            companies_synced=upserted,
            new_companies_discovered=new_count,
        )
    except Exception as exc:
        sb.finish_run(
            db_run_id,
            started_ts,
            status="failed",
            error_log=[{"stage": "sync_companies", "error": str(exc)}],
        )
        raise
    finally:
        sb.close()


if __name__ == "__main__":
    main()
