"""Pure direct startup & board sources (zero all.json dependency)."""

from __future__ import annotations

import asyncio
import html
import json
import re
import urllib.parse
import xml.etree.ElementTree as ET
from collections import defaultdict
import httpx

GENERIC_SUBDOMAINS = {
    "www", "www2", "corp", "us", "uk", "eu", "jp", "japan", "ww", "solutions",
    "app", "web", "jobs", "careers", "get", "try", "join", "use", "my", "hq",
    "blog", "docs", "portal", "hello", "info", "home", "en", "de", "fr",
}

MULTI_PART_TLDS = {
    "co.uk", "org.uk", "ac.uk", "co.jp", "ne.jp", "or.jp", "com.br", "com.au",
    "com.sg", "co.kr", "co.in", "co.il", "com.mx", "com.tr", "co.za", "co.nz",
    "com.cn", "com.tw", "com.hk", "co.id",
}

BAD_COMPANY_HOSTS = {
    "linkedin.com", "twitter.com", "x.com", "facebook.com", "instagram.com",
    "github.com", "youtube.com", "medium.com", "substack.com", "crunchbase.com",
    "angel.co", "wellfound.com", "ycombinator.com", "techstars.com", "linktr.ee",
    "greenhouse.io", "lever.co", "ashbyhq.com", "workable.com", "rippling.com",
}


def extract_host(url: str | None) -> str:
    if not url:
        return ""
    u = url.strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    try:
        return urllib.parse.urlparse(u).netloc.lower().split(":")[0].strip(".")
    except Exception:
        return ""


def canonical_root_domain(url_or_domain: str | None) -> str:
    host = extract_host(url_or_domain)
    if not host or "." not in host:
        return ""
    parts = host.split(".")
    tail2 = ".".join(parts[-2:])
    min_keep = 3 if tail2 in MULTI_PART_TLDS and len(parts) >= 3 else 2
    while len(parts) > min_keep and parts[0] in GENERIC_SUBDOMAINS:
        parts = parts[1:]
    if len(parts) > min_keep:
        parts = parts[-min_keep:]
    root = ".".join(parts)
    if root in BAD_COMPANY_HOSTS or root.endswith((".edu", ".gov", ".mil")):
        return ""
    return root


def fetch_yc_oss(client: httpx.Client) -> list[dict]:
    data = client.get("https://yc-oss.github.io/api/companies/all.json", timeout=30).json()
    out = []
    for c in data:
        dom = canonical_root_domain(c.get("website"))
        out.append({
            "source": "yc_oss",
            "source_id": str(c.get("slug") or c.get("id")),
            "source_url": c.get("url") or f"https://www.ycombinator.com/companies/{c.get('slug')}",
            "name": (c.get("name") or "").strip(),
            "website": c.get("website") or "",
            "domain": dom,
            "one_liner": (c.get("one_liner") or "").strip(),
            "description": (c.get("long_description") or c.get("one_liner") or "").strip(),
            "industry_raw": c.get("industry") or "",
            "subindustry_raw": c.get("subindustry") or "",
            "tags": list(c.get("tags") or []),
            "stage": c.get("stage") or "",
            "team_size": c.get("team_size"),
            "is_hiring": c.get("isHiring"),
            "nonprofit": bool(c.get("nonprofit")),
            "status": c.get("status") or "Active",
            "location": c.get("all_locations") or "",
            "batch": c.get("batch") or "",
        })
    return out


def fetch_500_direct(client: httpx.Client) -> list[dict]:
    raw = client.get(
        "https://500.co/api/startups",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    ).json()
    items = raw.get("res", []) if isinstance(raw, dict) else raw
    out = []
    for c in items:
        org = c.get("organization") or {}
        web = org.get("companyUrl") or org.get("website") or ""
        dom = canonical_root_domain(web)
        bm = c.get("businessModel") or []
        if isinstance(bm, list):
            ct = "B2B2C" if ("B2B" in bm and "B2C" in bm) else ("B2B" if "B2B" in bm else ("B2C" if "B2C" in bm else ""))
        else:
            ct = ""
        inds = [i.get("name") for i in (c.get("industries") or []) if isinstance(i, dict) and i.get("name")]
        stage_obj = c.get("stage")
        stage = stage_obj.get("name") if isinstance(stage_obj, dict) else (stage_obj or "")
        li = (org.get("companyLinkedIn") or "").strip()
        out.append({
            "source": "500_direct",
            "source_id": str(c.get("id") or org.get("id") or ""),
            "source_url": "https://500.co/companies",
            "name": (org.get("name") or "").strip(),
            "website": web,
            "domain": dom,
            "one_liner": (c.get("oneLiner") or "").strip(),
            "description": (org.get("description") or c.get("oneLiner") or "").strip(),
            "industry_raw": inds[0] if inds else "",
            "customer_type_raw": ct,
            "tags": inds,
            "stage": stage,
            "country": (org.get("country") or {}).get("name") if isinstance(org.get("country"), dict) else "",
            "socials": [li] if li else [],
        })
    return out


def fetch_speedrun_direct(client: httpx.Client) -> tuple[list[dict], dict[str, list[dict]]]:
    """Fetch all SR001-SR007 cohorts plus embedded founder faces[] by normalized company name."""
    out: list[dict] = []
    faces_by_name: dict[str, list[dict]] = defaultdict(list)
    seen = set()
    html_txt = client.get("https://speedrun.a16z.com/companies", headers={"User-Agent": "Mozilla/5.0"}, timeout=25).text
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)</script>', html_txt)
    if not m:
        return [], {}
    nd = json.loads(m.group(1))
    pp = nd.get("props", {}).get("pageProps", {})
    cohorts = pp.get("options", {}).get("cohorts") or []
    pages = [pp]
    for coh in cohorts:
        if coh == "SR007":
            continue
        try:
            ht = client.get(f"https://speedrun.a16z.com/companies?cohort={coh}", headers={"User-Agent": "Mozilla/5.0"}, timeout=15).text
            mm = re.search(r'<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)</script>', ht)
            if mm:
                pages.append(json.loads(mm.group(1)).get("props", {}).get("pageProps", {}))
        except Exception:
            pass
    for p in pages:
        list_obj = p.get("list")
        items = list_obj.get("companies", []) if isinstance(list_obj, dict) else (list_obj if isinstance(list_obj, list) else [])
        for c in items:
            if not isinstance(c, dict):
                continue
            sid = str(c.get("slug") or c.get("id") or "")
            if sid in seen:
                continue
            seen.add(sid)
            name = (c.get("name") or "").strip()
            web = c.get("website") or c.get("url") or ""
            dom = canonical_root_domain(web)
            nkey = re.sub(r"[^a-z0-9]", "", name.lower())
            for f in c.get("faces") or []:
                fn = (f.get("name") or "").strip()
                if fn and len(fn.split()) >= 2 and nkey:
                    faces_by_name[nkey].append({
                        "full_name": fn,
                        "title": "Founder",
                        "linkedin_url": "",
                        "twitter_url": "",
                        "source": "speedrun_faces",
                        "confidence": 0.98,
                    })
            out.append({
                "source": "speedrun_direct",
                "source_id": sid,
                "source_url": f"https://speedrun.a16z.com/companies/{sid}",
                "name": name,
                "website": web,
                "domain": dom,
                "one_liner": (c.get("oneLiner") or c.get("shortDescription") or "").strip(),
                "description": (c.get("description") or c.get("oneLiner") or "").strip(),
                "tags": [t.get("name") if isinstance(t, dict) else str(t) for t in (c.get("industries") or c.get("categories") or c.get("tags") or [])],
                "team_size": c.get("teamSize"),
                "stage": c.get("cohort") or "",
            })
    return out, dict(faces_by_name)


async def fetch_alchemist_all(client: httpx.AsyncClient) -> list[dict]:
    out = []
    hdrs = {"Accept": "application/vnd.api+json, application/json", "Origin": "https://www.alchemistaccelerator.com"}
    for page in range(1, 15):
        try:
            r = await client.get(
                f"https://vault.alchemistaccelerator.com/api/v1/alchemist_companies?page[size]=50&page[number]={page}",
                headers=hdrs,
                timeout=15,
            )
            if r.status_code != 200:
                break
            rows = r.json().get("data", [])
            if not rows:
                break
            for x in rows:
                attr = x.get("attributes") or {}
                meta = x.get("meta") or {}
                out.append({
                    "source": "alchemist_direct",
                    "source_id": str(x.get("id") or ""),
                    "name": (attr.get("name") or "").strip(),
                    "one_liner": (meta.get("oneliner") or "").strip(),
                    "description": (meta.get("description") or meta.get("startup_solution") or meta.get("oneliner") or "").strip(),
                    "total_raised": meta.get("startup_totalraise"),
                    "team_size": meta.get("nrofemployees"),
                    "stage": meta.get("last_round_stage") or "",
                    "location": meta.get("location_formatted_address") or "",
                })
            if len(rows) < 50:
                break
        except Exception:
            break
    return out


def fetch_hn_hiring(client: httpx.Client, months: int = 6) -> list[dict]:
    search = client.get(
        "https://hn.algolia.com/api/v1/search_by_date?tags=story,author_whoishiring&hitsPerPage=10",
        timeout=15,
    ).json()
    stories = [h for h in search.get("hits", []) if "who is hiring" in (h.get("title") or "").lower()][:months]
    out = []
    seen_boards = set()
    for st in stories:
        item = client.get(f"https://hn.algolia.com/api/v1/items/{st['objectID']}", timeout=25).json()
        for ch in item.get("children", []):
            raw_html = html.unescape(ch.get("text") or "")
            plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", raw_html)).strip()
            if not plain:
                continue
            first_seg = plain.split("|")[0].strip()[:80]
            urls = re.findall(r"https?://[^\s\"'<>]+", raw_html)
            comp_dom = ""
            ats_pair = None
            for u in urls:
                low = u.lower()
                pu = urllib.parse.urlparse(u)
                slug = pu.path.strip("/").split("/")[0]
                if not slug:
                    continue
                if "greenhouse.io/" in low and slug.lower() not in ("embed", "v1"):
                    ats_pair = ("greenhouse", slug)
                elif "jobs.lever.co/" in low:
                    ats_pair = ("lever", slug)
                elif "jobs.ashbyhq.com/" in low:
                    ats_pair = ("ashby", slug)
                elif "apply.workable.com/" in low:
                    ats_pair = ("workable", slug)
                elif "ats.rippling.com/" in low:
                    ats_pair = ("rippling", slug)
                elif not comp_dom:
                    d = canonical_root_domain(u)
                    if d and "news.ycombinator" not in d:
                        comp_dom = d
            if ats_pair and ats_pair not in seen_boards:
                seen_boards.add(ats_pair)
                out.append({
                    "source": "hn_hiring",
                    "source_id": f"{ats_pair[0]}:{ats_pair[1]}",
                    "name": re.sub(r"https?://\S+", "", first_seg).strip(" -–—:(") or ats_pair[1],
                    "domain": comp_dom,
                    "ats_provider": ats_pair[0],
                    "ats_slug": ats_pair[1],
                    "description": plain[:400],
                    "is_hiring": True,
                })
    return out


def fetch_sec_form_d(client: httpx.Client, limit: int = 60) -> list[dict]:
    sec_hdr = {"User-Agent": "StartupIntelBot/2.0 research@startupintel.dev"}
    idx = client.get(
        'https://efts.sec.gov/LATEST/search-index?forms=D&q="Technology"+OR+"Biotechnology"+OR+"Software"',
        headers=sec_hdr,
        timeout=20,
    ).json()
    hits = idx.get("hits", {}).get("hits", [])
    tech_groups = {
        "Other Technology", "Biotechnology", "Pharmaceuticals", "Computers",
        "Telecommunications", "Other Health Care", "Health Insurance", "Energy",
    }
    out = []
    for h in hits[:limit]:
        src = h.get("_source", {})
        adsh, ciks = src.get("adsh", ""), src.get("ciks", [])
        if not adsh or not ciks:
            continue
        cik = str(int(ciks[0]))
        xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh.replace('-', '')}/primary_doc.xml"
        try:
            xr = client.get(xml_url, headers=sec_hdr, timeout=10)
            if xr.status_code != 200:
                continue
            root = ET.fromstring(xr.text)
            igrp = root.findtext(".//industryGroup/industryGroupType") or ""
            sold = float(root.findtext(".//offeringSalesAmounts/totalAmountSold") or 0)
            ename = (root.findtext(".//primaryIssuer/entityName") or "").strip()
            yr = root.findtext(".//yearOfIncorporation/value") or ""
            st = root.findtext(".//primaryIssuer/issuerAddress/stateOrCountryDescription") or ""
            city = root.findtext(".//primaryIssuer/issuerAddress/city") or ""
            if igrp in tech_groups and sold >= 100000 and ename:
                persons = []
                for rp in root.findall(".//relatedPersonInfo"):
                    fn = (rp.findtext(".//firstName") or "").strip().title()
                    ln = (rp.findtext(".//lastName") or "").strip().title()
                    rels = [r.text.strip() for r in rp.findall(".//relationship") if r.text]
                    if fn and ln and "Executive Officer" in rels:
                        persons.append({
                            "full_name": f"{fn} {ln}",
                            "title": ", ".join(rels),
                            "linkedin_url": "",
                            "twitter_url": "",
                            "source": "sec_form_d",
                            "confidence": 0.96,
                        })
                out.append({
                    "source": "sec_form_d",
                    "source_id": adsh,
                    "name": ename,
                    "industry_raw": igrp,
                    "total_raised": int(sold),
                    "year": int(yr) if yr.isdigit() else None,
                    "state": st,
                    "city": city,
                    "filing_url": xml_url,
                    "executives": persons,
                })
        except Exception:
            continue
    return out


async def fetch_common_crawl_boards(client: httpx.AsyncClient, limit_per_ats: int = 300) -> list[tuple[str, str]]:
    """Auto-discover the latest Common Crawl CDX index and harvest active ATS slugs."""
    cdx_base = "https://index.commoncrawl.org/CC-MAIN-2026-47-index"
    try:
        rc = await client.get("https://index.commoncrawl.org/collinfo.json", timeout=15)
        if rc.status_code == 200 and rc.json():
            cdx_base = rc.json()[0].get("cdx-api") or cdx_base
    except Exception:
        pass

    boards = []
    seen = set()
    targets = [
        ("jobs.ashbyhq.com/*", "ashby", limit_per_ats),
        ("ats.rippling.com/*", "rippling", limit_per_ats),
        ("apply.workable.com/*", "workable", max(150, limit_per_ats // 2)),
        ("jobs.lever.co/*", "lever", limit_per_ats),
        ("job-boards.greenhouse.io/*", "greenhouse", limit_per_ats),
        ("*.teamtailor.com/*", "teamtailor", max(150, limit_per_ats // 2)),
    ]
    for pat, prov, lim in targets:
        for attempt in range(3):
            try:
                r = await client.get(cdx_base, params={"url": pat, "output": "json", "fl": "url", "limit": lim}, timeout=25)
                if r.status_code == 200 and r.text.strip():
                    for line in r.text.splitlines():
                        if not line.strip():
                            continue
                        try:
                            u = json.loads(line).get("url", "")
                            pu = urllib.parse.urlparse(u)
                            slug = pu.netloc.split(".")[0].lower() if prov == "teamtailor" else pu.path.strip("/").split("/")[0].lower()
                            if slug and slug not in ("www", "embed", "api", "assets", "jobs", "v1", "v0", "careers"):
                                k = (prov, slug)
                                if k not in seen:
                                    seen.add(k)
                                    boards.append(k)
                        except Exception:
                            pass
                    break
                await asyncio.sleep(1.5 * (attempt + 1))
            except Exception:
                await asyncio.sleep(1.5 * (attempt + 1))
        await asyncio.sleep(1.0)
    return boards


def merge_canonical_companies(yc: list[dict], g500: list[dict], speedrun: list[dict]) -> tuple[dict[str, dict], list[dict]]:
    """Merge pure direct startup sources into 1 canonical entity per normalized root domain."""
    by_domain: dict[str, dict] = {}
    source_links: list[dict] = []

    def _clean_name_pick(cur_name: str, new_name: str, dom: str) -> str:
        if not new_name:
            return cur_name
        if cur_name.lower() == dom.lower() and new_name.lower() != dom.lower():
            return new_name
        return cur_name

    def ingest(rec: dict):
        if rec.get("nonprofit"):
            return
        dom = rec.get("domain") or canonical_root_domain(rec.get("website"))
        if not dom:
            return
        source_links.append({
            "company_domain": dom,
            "source": rec.get("source"),
            "source_id": str(rec.get("source_id") or rec.get("name") or dom),
            "source_url": rec.get("source_url") or rec.get("website") or "",
        })
        cur = by_domain.get(dom)
        if cur is None:
            by_domain[dom] = {
                "domain": dom,
                "name": rec.get("name") or dom,
                "website": rec.get("website") or f"https://{dom}",
                "one_liner": rec.get("one_liner") or "",
                "description": rec.get("description") or "",
                "industry_raw": rec.get("industry_raw") or "",
                "subindustry_raw": rec.get("subindustry_raw") or "",
                "customer_type_raw": rec.get("customer_type_raw") or "",
                "tags": list(rec.get("tags") or []),
                "stage": rec.get("stage") or "",
                "team_size": rec.get("team_size"),
                "total_raised": rec.get("total_raised"),
                "country": rec.get("country") or "",
                "city": rec.get("city") or "",
                "is_hiring": rec.get("is_hiring"),
                "sources": [rec.get("source")],
                "socials": list(rec.get("socials") or []),
            }
        else:
            if rec.get("source") not in cur["sources"]:
                cur["sources"].append(rec.get("source"))
            cur["name"] = _clean_name_pick(cur["name"], rec.get("name") or "", dom)
            if not cur["one_liner"] and rec.get("one_liner"):
                cur["one_liner"] = rec["one_liner"]
            if len(rec.get("description") or "") > len(cur.get("description") or ""):
                cur["description"] = rec["description"]
            if not cur["industry_raw"] and rec.get("industry_raw"):
                cur["industry_raw"] = rec["industry_raw"]
            if not cur["subindustry_raw"] and rec.get("subindustry_raw"):
                cur["subindustry_raw"] = rec["subindustry_raw"]
            if not cur["customer_type_raw"] and rec.get("customer_type_raw"):
                cur["customer_type_raw"] = rec["customer_type_raw"]
            if not cur["stage"] and rec.get("stage"):
                cur["stage"] = rec["stage"]
            if not cur["team_size"] and rec.get("team_size"):
                cur["team_size"] = rec["team_size"]
            if not cur["total_raised"] and rec.get("total_raised"):
                cur["total_raised"] = rec.get("total_raised")
            if cur["is_hiring"] is None and rec.get("is_hiring") is not None:
                cur["is_hiring"] = rec["is_hiring"]
            for s in (rec.get("socials") or []):
                if s and s not in cur["socials"]:
                    cur["socials"].append(s)
            for t in (rec.get("tags") or []):
                if t and t not in cur["tags"]:
                    cur["tags"].append(t)

    for r in yc:
        ingest(r)
    for r in g500:
        ingest(r)
    for r in speedrun:
        ingest(r)

    return by_domain, source_links
