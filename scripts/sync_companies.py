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
import ftfy
import httpx
import pycountry

from supabase_client import SupabaseREST, utc_now_iso

_COMPANY_COUNTRY_OVERRIDES = {
    "turkey": "Turkey",
    "türkiye": "Turkey",
    "russia": "Russia",
    "ivory coast": "Côte d'Ivoire",
    "cote d'ivoire": "Côte d'Ivoire",
    "kosovo": "Kosovo",
    "palestine": "Palestine",
    "british virgin islands": "British Virgin Islands",
    "democratic republic of the congo": "Democratic Republic of the Congo",
    "usa": "United States",
    "us": "United States",
    "uk": "United Kingdom",
}


def normalize_company_country(val: str | None) -> str | None:
    if not val or not isinstance(val, str):
        return None
    s = ftfy.fix_text(val.strip())
    if not s:
        return None
    low = s.lower()
    if low in _COMPANY_COUNTRY_OVERRIDES:
        return _COMPANY_COUNTRY_OVERRIDES[low]
    c = pycountry.countries.get(name=s) or pycountry.countries.get(common_name=s) or pycountry.countries.get(alpha_2=s.upper()) or pycountry.countries.get(alpha_3=s.upper())
    if c:
        return getattr(c, "common_name", None) or c.name
    return s

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
    extra = raw.get("extra") if isinstance(raw.get("extra"), dict) else {}
    team_size = raw.get("teamSize")
    total_raised = raw.get("totalRaised")
    return {
        "id": str(raw["id"]),
        "source": str(raw.get("source") or "unknown"),
        "name": ftfy.fix_text(str(raw.get("name") or raw["id"])),
        "description": ftfy.fix_text(raw["description"]) if raw.get("description") else None,
        "logo": raw.get("logo") or None,
        "website": website,
        "domain": extract_domain(website),
        "source_url": raw.get("sourceUrl") or None,
        "location": ftfy.fix_text(raw["location"]) if raw.get("location") else None,
        "country": normalize_company_country(raw.get("country") or extra.get("country")),
        "city": ftfy.fix_text(extra["city"]) if extra.get("city") else None,
        "state_province": ftfy.fix_text(extra["state_province"]) if extra.get("state_province") else None,
        "region": raw.get("region") or None,
        "subregion": raw.get("subregion") or None,
        "tags": raw.get("tags") if isinstance(raw.get("tags"), list) else [],
        "year": raw.get("year") if isinstance(raw.get("year"), int) else None,
        "program": raw.get("program") or None,
        "stage": raw.get("stage") or None,
        "team_size": int(team_size) if isinstance(team_size, (int, float)) and not isinstance(team_size, bool) else None,
        "team_size_range": raw.get("teamSizeRange") or None,
        "total_raised": float(total_raised) if isinstance(total_raised, (int, float)) and not isinstance(total_raised, bool) else None,
        "status": raw.get("status") or "unknown",
        "is_exit": bool(raw.get("isExit", False)),
        "is_unicorn": bool(raw.get("isUnicorn", False)),
        "is_hiring": bool(raw.get("isHiring", False)),
        "is_bcorp": bool(raw.get("isBCorp", False)),
        "is_current_session": bool(raw.get("isCurrentSession", False)),
        "is_top_company": bool(raw.get("topCompany", False)),
        "is_nonprofit": bool(raw.get("nonprofit", False)),
        "former_names": raw.get("formerNames") if isinstance(raw.get("formerNames"), list) else [],
        "also_in": raw.get("alsoIn") if isinstance(raw.get("alsoIn"), list) else [],
        "founders": raw.get("founders") if isinstance(raw.get("founders"), list) else [],
        "social": {k: v for k, v in raw["social"].items() if v} if isinstance(raw.get("social"), dict) else {},
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
