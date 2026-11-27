"""Step 1: Clean-slate Seed Companies, Provenance, Founders & Socials from Pure Direct Sources."""

from __future__ import annotations

import asyncio
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.classifiers import HybridClassifier
from src.enrich_founders_socials import clean_socials, dedupe_founders, enrich_yc_pages, fetch_wikidata_batch
from src.sources import (
    fetch_500_direct,
    fetch_alchemist_all,
    fetch_sec_form_d,
    fetch_speedrun_direct,
    fetch_yc_oss,
    merge_canonical_companies,
)
from src.supabase_client import SupabaseREST, utc_now_iso


async def main_async() -> None:
    started = time.time()
    sb = SupabaseREST()
    run_id = sb.start_run("01_seed_companies_and_founders")
    try:
        with httpx.Client(headers={"User-Agent": "StartupIntelBot/2.0"}, follow_redirects=True) as sync_c:
            print("[1/5] Fetching pure direct startup sources (yc-oss, 500.co, Speedrun, SEC Form D)...")
            yc = fetch_yc_oss(sync_c)
            g500 = fetch_500_direct(sync_c)
            speedrun, sr_faces = fetch_speedrun_direct(sync_c)
            sec = fetch_sec_form_d(sync_c, limit=60)

        async with httpx.AsyncClient(headers={"User-Agent": "StartupIntelBot/2.0"}, follow_redirects=True) as ac:
            alchemist = await fetch_alchemist_all(ac)

        print(f"  Raw counts -> YC={len(yc)}, 500={len(g500)}, Speedrun={len(speedrun)}, Alchemist={len(alchemist)}, SEC={len(sec)}")
        canon, source_links = merge_canonical_companies(yc, g500, speedrun)
        print(f"[2/5] Merged into {len(canon)} canonical root domains ({len(source_links)} provenance rows)")

        # Match Alchemist & SEC Form D records onto canonical domains by normalized company name
        norm_to_dom = {re.sub(r"[^a-z0-9]", "", v["name"].lower()): d for d, v in canon.items() if v.get("name")}
        sec_founders: dict[str, list[dict]] = defaultdict(list)
        for a in alchemist:
            nk = re.sub(r"[^a-z0-9]", "", (a.get("name") or "").lower())
            dom = norm_to_dom.get(nk)
            if dom:
                c = canon[dom]
                if "alchemist_direct" not in c["sources"]:
                    c["sources"].append("alchemist_direct")
                if not c["total_raised"] and a.get("total_raised"):
                    c["total_raised"] = a["total_raised"]
                source_links.append({"company_domain": dom, "source": "alchemist_direct", "source_id": a["source_id"], "source_url": ""})
        for s in sec:
            nk = re.sub(r"[^a-z0-9]", "", (s.get("name") or "").lower())
            dom = norm_to_dom.get(nk)
            if dom:
                c = canon[dom]
                if "sec_form_d" not in c["sources"]:
                    c["sources"].append("sec_form_d")
                if s.get("total_raised") and (not c["total_raised"] or s["total_raised"] > c["total_raised"]):
                    c["total_raised"] = s["total_raised"]
                source_links.append({"company_domain": dom, "source": "sec_form_d", "source_id": s["source_id"], "source_url": s.get("filing_url", "")})
                for ex in s.get("executives") or []:
                    sec_founders[dom].append(ex)

        print("[3/5] Training leak-free HybridClassifier & enriching industries/customer_type...")
        clf = HybridClassifier()
        clf.fit_companies(list(canon.values()))

        print("[4/5] Enriching founders & socials (YC pages + Speedrun faces + Wikidata SPARQL)...")
        all_yc_flag = os.environ.get("ALL_YC_PAGES", "0") == "1"
        yc_socs, yc_fnds = await enrich_yc_pages(yc, only_hiring=(not all_yc_flag))

        hiring_or_top = [d for d, v in canon.items() if v.get("is_hiring")][:1200]
        wd_socs, wd_fnds = await fetch_wikidata_batch(hiring_or_top)

        founders_by_dom: dict[str, list[dict]] = defaultdict(list)
        for dom, items in yc_fnds.items():
            founders_by_dom[dom].extend(items)
        for nk, items in sr_faces.items():
            dom = norm_to_dom.get(nk)
            if dom:
                founders_by_dom[dom].extend(items)
        for dom, items in sec_founders.items():
            founders_by_dom[dom].extend(items)
        for dom, items in wd_fnds.items():
            founders_by_dom[dom].extend(items)

        now = utc_now_iso()
        comp_rows = []
        for dom, c in canon.items():
            ml = clf.classify_company(c)
            socs = clean_socials(list(c.get("socials") or []) + yc_socs.get(dom, []) + wd_socs.get(dom, []))
            tr = c.get("total_raised")
            try:
                tr_int = int(float(tr)) if tr else None
            except Exception:
                tr_int = None
            ts = c.get("team_size")
            try:
                ts_int = int(float(ts)) if ts else None
            except Exception:
                ts_int = None
            comp_rows.append({
                "domain": dom,
                "name": c.get("name") or dom,
                "website": c.get("website") or f"https://{dom}",
                "one_liner": (c.get("one_liner") or "")[:500],
                "description": (c.get("description") or "")[:4000],
                **ml,
                "stage": (c.get("stage") or "")[:80],
                "team_size": ts_int,
                "total_raised_usd": tr_int,
                "country": (c.get("country") or "")[:80],
                "city": (c.get("city") or "")[:80],
                "is_hiring": c.get("is_hiring"),
                "sources": c.get("sources") or [],
                "socials": socs,
                "updated_at": now,
            })

        founder_rows = [r for r in dedupe_founders(founders_by_dom) if r["company_domain"] in canon]

        # Dedupe company_sources PK (company_domain, source, source_id) & enforce FK safety
        src_map = {
            (r["company_domain"], r["source"], r["source_id"]): r
            for r in source_links
            if r["company_domain"] in canon
        }
        deduped_sources = list(src_map.values())

        print(f"[5/5] Upserting {len(comp_rows)} companies, {len(deduped_sources)} sources, {len(founder_rows)} founders to Supabase...")
        sb.upsert_batch("companies", comp_rows, on_conflict="domain")
        sb.upsert_batch("company_sources", deduped_sources, on_conflict="company_domain,source,source_id")
        sb.upsert_batch("founders", founder_rows, on_conflict="company_domain,name_key")

        sb.finish_run(
            run_id, started, "succeeded",
            companies_synced=len(comp_rows),
            founders_synced=len(founder_rows),
            notes=f"yc_socs={len(yc_socs)}, wd_socs={len(wd_socs)}",
        )
        print(f"Done in {time.time() - started:.1f}s -> companies={len(comp_rows)}, founders={len(founder_rows)}")
    except Exception as exc:
        sb.finish_run(run_id, started, "failed", notes=str(exc)[:500])
        raise
    finally:
        sb.close()


if __name__ == "__main__":
    asyncio.run(main_async())
