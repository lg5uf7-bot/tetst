"""Step 2: Discover & Strictly Verify 1-to-1 ATS Boards (Homepage/Careers + HN + Common Crawl + Slug Probes)."""

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

from src.enrich_founders_socials import (
    SOC_RE,
    clean_socials,
    dedupe_founders,
    enrich_websites_founders_and_socials,
    fetch_wikidata_batch,
)
from src.sources import canonical_root_domain, fetch_common_crawl_boards, fetch_hn_hiring
from src.supabase_client import SupabaseREST, utc_now_iso
from src.validators import (
    PARKED_HINTS,
    _norm_brand,
    extract_ats_candidates,
    probe_ats_ownership,
    score_ats_ownership,
)


async def crawl_company_sites(companies: list[dict], max_sites: int = 2500) -> dict[str, dict]:
    """Crawl /, /careers, /jobs on prioritized unboarded startups for ATS candidates + liveness + socials."""
    sem = asyncio.Semaphore(60)
    results: dict[str, dict] = {}
    targets = companies[:max_sites]

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; StartupIntelBot/2.0)"},
        follow_redirects=True,
        max_redirects=3,
        timeout=httpx.Timeout(8.0, connect=4.0),
    ) as ac:
        async def one(c: dict):
            dom = c["domain"]
            async with sem:
                status = "dead_http"
                cands: list[tuple[str, str]] = []
                socs: list[str] = []
                meta_desc = ""
                for path in ["", "/careers", "/jobs"]:
                    try:
                        r = await ac.get(f"https://{dom}{path}")
                        if r.status_code in (403, 429):
                            if path == "":
                                status = "alive_waf"
                            continue
                        if r.status_code >= 400:
                            continue
                        txt = r.text[:250000]
                        low = txt.lower()
                        if path == "" and any(p in low for p in PARKED_HINTS):
                            status = "parked"
                            break
                        status = "alive"
                        cands.extend(extract_ats_candidates(str(r.url), txt))
                        socs.extend(m.group(1) for m in SOC_RE.finditer(txt))
                        if path == "" and not meta_desc:
                            mm = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', txt, re.I)
                            if mm:
                                meta_desc = mm.group(1).strip()[:500]
                        if cands and path in ("/careers", "/jobs"):
                            break
                    except httpx.TimeoutException:
                        if path == "":
                            status = "timeout"
                            break
                    except Exception:
                        if path == "":
                            status = "dead_dns_or_ssl"
                            break
                seen, ucands = set(), []
                for p, s in cands:
                    k = (p, s.lower())
                    if k not in seen:
                        seen.add(k)
                        ucands.append((p, s))
                results[dom] = {
                    "liveness_status": status,
                    "cands": ucands,
                    "socials": clean_socials(socs),
                    "meta_desc": meta_desc,
                }

        await asyncio.gather(*(one(c) for c in targets))
    return results


async def main_async() -> None:
    started = time.time()
    sb = SupabaseREST()
    run_id = sb.start_run("02_discover_and_verify_ats")
    try:
        print("[1/5] Loading existing companies & verified boards from Supabase...")
        comps = sb.select_all("companies", select="domain,name,website,description,is_hiring,liveness_status,team_size,socials,sources")
        existing_boards = sb.select_all("ats_boards", select="domain,ats_provider,ats_slug")
        boarded_doms = {b["domain"] for b in existing_boards}
        taken_boards = {(b["ats_provider"], b["ats_slug"].lower()) for b in existing_boards}
        comp_by_dom = {c["domain"]: c for c in comps}

        # Prioritize hiring + unknown-liveness unboarded startups
        unboarded = [c for c in comps if c["domain"] not in boarded_doms and c.get("liveness_status") not in ("dead_dns_or_ssl", "parked")]
        unboarded.sort(key=lambda c: (0 if c.get("is_hiring") else 1, 0 if c.get("liveness_status") == "unknown" else 1))

        max_sites = int(os.environ.get("MAX_CRAWL_SITES", "2500"))
        print(f"[2/5] Crawling up to {min(max_sites, len(unboarded))} unboarded sites + HN + Common Crawl...")
        crawl_res = await crawl_company_sites(unboarded, max_sites=max_sites)

        with httpx.Client(headers={"User-Agent": "StartupIntelBot/2.0"}, follow_redirects=True) as sync_c:
            hn_rows = fetch_hn_hiring(sync_c, months=6)
        async with httpx.AsyncClient(headers={"User-Agent": "StartupIntelBot/2.0"}, follow_redirects=True) as ac:
            cc_boards = await fetch_common_crawl_boards(ac, limit_per_ats=int(os.environ.get("CC_LIMIT", "300")))

        # Build candidate pairs: (dom, prov, slug, channel, site_linked)
        cand_map: dict[tuple[str, str], dict] = {}
        for dom, res in crawl_res.items():
            for prov, slug in res["cands"]:
                k = (prov, slug.lower())
                if k not in taken_boards and k not in cand_map:
                    cand_map[k] = {"dom": dom, "prov": prov, "slug": slug, "channel": "site_crawl", "site_linked": True}
            # Smart slug_guess for alive hiring startups with JS-rendered /careers pages
            if not res["cands"] and res["liveness_status"] in ("alive", "alive_waf") and comp_by_dom.get(dom, {}).get("is_hiring"):
                dom_base = dom.split(".")[0].lower()
                if len(dom_base) >= 3:
                    for prov in ("ashby", "greenhouse", "lever"):
                        k = (prov, dom_base)
                        if k not in taken_boards and k not in cand_map:
                            cand_map[k] = {"dom": dom, "prov": prov, "slug": dom_base, "channel": "slug_guess", "site_linked": False}

        for h in hn_rows:
            k = (h["ats_provider"], h["ats_slug"].lower())
            if k not in taken_boards and k not in cand_map:
                cand_map[k] = {"dom": h.get("domain") or "", "prov": h["ats_provider"], "slug": h["ats_slug"], "channel": "hn_hiring", "site_linked": False, "hn_name": h.get("name", "")}

        for prov, slug in cc_boards:
            k = (prov, slug.lower())
            if k not in taken_boards and k not in cand_map:
                cand_map[k] = {"dom": "", "prov": prov, "slug": slug, "channel": "common_crawl", "site_linked": False}

        print(f"[3/5] Probing {len(cand_map)} candidate ATS boards for strict 1-to-1 ownership...")
        probe_sem = asyncio.Semaphore(50)
        verified_boards: list[dict] = []
        new_companies: dict[str, dict] = {}
        new_sources: list[dict] = []
        now = utc_now_iso()

        async with httpx.AsyncClient(headers={"User-Agent": "StartupIntelBot/2.0"}, follow_redirects=True) as ac:
            async def verify_one(item: dict):
                async with probe_sem:
                    meta = await probe_ats_ownership(ac, item["prov"], item["slug"])
                    if meta["http_status"] != 200:
                        return
                    dom = item["dom"]
                    # External resolution for HN/CC boards without initial domain
                    if not dom:
                        b_slug = item["slug"].lower()
                        b_norm = _norm_brand(meta.get("board_name") or item.get("hn_name") or b_slug)
                        for ed in meta.get("ext_domains") or []:
                            ed_canon = canonical_root_domain(ed)
                            if not ed_canon:
                                continue
                            ed_base = ed_canon.split(".")[0].lower()
                            if ed_base in b_slug or b_slug in ed_base or (b_norm and ed_base in b_norm.replace(" ", "")):
                                dom = ed_canon
                                break
                    if not dom or dom in boarded_doms:
                        return
                    comp_meta = comp_by_dom.get(dom) or {
                        "domain": dom,
                        "name": meta.get("board_name") or item.get("hn_name") or dom,
                        "team_size": None,
                        "open_jobs_count": 0,
                    }
                    ok, score, reason = score_ats_ownership(
                        {**comp_meta, "ats_slug": item["slug"]},
                        meta,
                    )
                    if item["site_linked"]:
                        score = min(100, score + 85)
                        ok = True
                        reason = f"site_links_to_ats,{reason}"
                    if not ok:
                        return
                    boarded_doms.add(dom)
                    verified_boards.append({
                        "domain": dom,
                        "ats_provider": item["prov"],
                        "ats_slug": item["slug"],
                        "board_name": (meta.get("board_name") or comp_meta.get("name") or "")[:120],
                        "verification_score": score,
                        "verification_reason": reason[:240],
                        "discovery_channel": item["channel"],
                        "poll_tier": "daily",
                        "next_poll_at": now,
                        "updated_at": now,
                    })
                    if dom not in comp_by_dom:
                        cname = (meta.get("board_name") or item.get("hn_name") or dom)[:120]
                        new_companies[dom] = {
                            "domain": dom,
                            "name": cname,
                            "website": f"https://{dom}",
                            "sources": [item["channel"]],
                            "is_hiring": True,
                            "updated_at": now,
                        }
                        new_sources.append({
                            "company_domain": dom,
                            "source": item["channel"],
                            "source_id": f"{item['prov']}:{item['slug']}",
                            "source_url": f"https://{dom}",
                        })

            await asyncio.gather(*(verify_one(it) for it in cand_map.values()))

        # Backfill liveness, socials, descriptions, and founders on crawled + newly discovered companies
        print(f"[4/5] Enriching liveness/socials/founders on {len(new_companies)} new + crawled domains...")
        company_patches = []
        for dom, res in crawl_res.items():
            c = comp_by_dom.get(dom)
            if not c:
                continue
            merged_socs = clean_socials(list(c.get("socials") or []) + res["socials"])
            company_patches.append({
                "domain": dom,
                "name": c["name"],
                "website": c.get("website") or f"https://{dom}",
                "description": c.get("description") or res["meta_desc"] or "",
                "liveness_status": res["liveness_status"],
                "liveness_checked_at": now,
                "socials": merged_socs,
                "updated_at": now,
            })

        founder_rows = []
        if new_companies:
            new_doms = list(new_companies.keys())
            wd_socs, wd_fnds = await fetch_wikidata_batch(new_doms)
            site_socs, site_fnds = await enrich_websites_founders_and_socials(
                new_doms,
                {d: v["name"] for d, v in new_companies.items()},
                use_jina_fallback=True,
                jina_limit=15,
            )
            merged_f: dict[str, list[dict]] = defaultdict(list)
            for d, v in new_companies.items():
                v["socials"] = clean_socials(wd_socs.get(d, []) + site_socs.get(d, []))
                merged_f[d].extend(wd_fnds.get(d, []))
                merged_f[d].extend(site_fnds.get(d, []))
            valid_doms = set(comp_by_dom) | set(new_companies)
            founder_rows = [r for r in dedupe_founders(merged_f) if r["company_domain"] in valid_doms]

        print(f"[5/5] Upserting {len(new_companies)} new companies, {len(company_patches)} liveness updates, {len(verified_boards)} verified boards...")
        sb.upsert_batch("companies", list(new_companies.values()), on_conflict="domain")
        sb.upsert_batch("companies", company_patches, on_conflict="domain")
        sb.upsert_batch("company_sources", new_sources, on_conflict="company_domain,source,source_id")
        sb.upsert_batch("ats_boards", verified_boards, on_conflict="domain")
        sb.upsert_batch("founders", founder_rows, on_conflict="company_domain,name_key")

        sb.finish_run(
            run_id, started, "succeeded",
            companies_synced=len(new_companies) + len(company_patches),
            boards_polled=len(verified_boards),
            founders_synced=len(founder_rows),
            notes=f"new_verified_boards={len(verified_boards)}, hn={len(hn_rows)}, cc={len(cc_boards)}",
        )
        print(f"Done in {time.time() - started:.1f}s -> +{len(verified_boards)} verified 1-to-1 ATS boards!")
    except Exception as exc:
        sb.finish_run(run_id, started, "failed", notes=str(exc)[:500])
        raise
    finally:
        sb.close()


if __name__ == "__main__":
    asyncio.run(main_async())
