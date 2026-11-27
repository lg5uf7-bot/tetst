"""Weekly Discovery & Enrichment: Ingest new startups, discover new ATS boards, backfill founders/socials."""

from __future__ import annotations

import asyncio
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import discover_and_verify_ats, seed_companies_and_founders
from src.enrich_founders_socials import (
    clean_socials,
    dedupe_founders,
    enrich_websites_founders_and_socials,
    fetch_wikidata_batch,
)
from src.supabase_client import SupabaseREST, utc_now_iso


async def backfill_missing_richness() -> None:
    """Backfill founders + socials on active hiring companies still missing them."""
    sb = SupabaseREST()
    try:
        comps = sb.select_all("companies", select="domain,name,website,socials,open_jobs_count", filters={"open_jobs_count": "gt.0"})
        existing_f = sb.select_all("founders", select="company_domain")
        has_founders = {r["company_domain"] for r in existing_f}

        missing = [c for c in comps if c["domain"] not in has_founders or not c.get("socials")]
        missing.sort(key=lambda c: c.get("open_jobs_count", 0), reverse=True)
        batch = missing[:250]
        if not batch:
            print("[backfill] All active hiring companies already have founders + socials!")
            return

        doms = [c["domain"] for c in batch]
        name_map = {c["domain"]: c["name"] for c in batch}
        wd_socs, wd_fnds = await fetch_wikidata_batch(doms)
        site_socs, site_fnds = await enrich_websites_founders_and_socials(doms, name_map, use_jina_fallback=True, jina_limit=40)

        now = utc_now_iso()
        comp_updates = []
        merged_f: dict[str, list[dict]] = defaultdict(list)
        for c in batch:
            d = c["domain"]
            socs = clean_socials(list(c.get("socials") or []) + wd_socs.get(d, []) + site_socs.get(d, []))
            if socs != list(c.get("socials") or []):
                comp_updates.append({"domain": d, "name": c["name"], "website": c.get("website") or f"https://{d}", "socials": socs, "updated_at": now})
            merged_f[d].extend(wd_fnds.get(d, []))
            merged_f[d].extend(site_fnds.get(d, []))

        f_rows = dedupe_founders(merged_f)
        sb.upsert_batch("companies", comp_updates, on_conflict="domain")
        sb.upsert_batch("founders", f_rows, on_conflict="company_domain,name_key")
        print(f"[backfill] Updated socials on {len(comp_updates)} companies, upserted {len(f_rows)} founders.")
    finally:
        sb.close()


async def main_async() -> None:
    t0 = time.time()
    print("=== WEEKLY DISCOVERY & ENRICHMENT ===")
    await seed_companies_and_founders.main_async()
    await discover_and_verify_ats.main_async()
    await backfill_missing_richness()
    print(f"Weekly pipeline complete in {time.time() - t0:.1f}s.")


if __name__ == "__main__":
    asyncio.run(main_async())
