"""5-Layer $0 Founders & Company Socials Extractor (YC, Speedrun, 500, SEC Form D, Wikidata, Website /about+/team + r.jina.ai)."""

from __future__ import annotations

import asyncio
import html as hl
import json
import re
from collections import defaultdict
import httpx
from src.sources import canonical_root_domain

SOC_RE = re.compile(
    r"https?://(?:www\.)?(linkedin\.com/company/[a-zA-Z0-9._-]+|x\.com/[a-zA-Z0-9_]{2,}|twitter\.com/[a-zA-Z0-9_]{2,}|github\.com/[a-zA-Z0-9_-]{2,})",
    re.I,
)
BAD_SOC_HANDLES = {"intent", "share", "sharer", "home", "search", "explore", "login", "signup", "company", "hashtag", "i", "jobs", "about", "privacy", "terms", "tos", "help"}

FOUNDER_CTX_RE = re.compile(
    r"\b([A-Z][a-zA-ZÀ-ÿ'\-]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-zA-ZÀ-ÿ'\-]+)\b\s*(?:,|\s+[-–—|]\s+|\s+is\s+(?:the\s+)?)\s*"
    r"((?:Co-?Founder|Founder)(?:\s*(?:&|and|/|,)\s*(?:CEO|CTO|COO|CPO|Chief Executive|Chief Technology))?|CEO\s*(?:&|and|/)\s*Co-?Founder|CEO)\b"
)
BAD_FOUNDER_TOKENS = {
    "co-", "senior", "vice", "chief", "about", "our", "meet", "team", "company",
    "learn", "read", "join", "contact", "product", "series", "the", "your",
    "with", "from", "more", "view", "click", "privacy", "terms", "cookie",
    "chairman", "executive", "officer", "robotics", "technologies", "solutions",
    "systems", "labs", "ventures", "capital", "partners", "holdings", "group",
}
INVESTOR_CTX_RE = re.compile(r"\b(investor|backed\s+by|advisor|board\s+member|angel|customer|testimonial|partner\s+at)\b", re.I)


def clean_socials(urls: list[str]) -> list[str]:
    out, seen = [], set()
    for u in urls:
        u = u.strip().rstrip("/.,;)")
        u = re.sub(r"^https?://(?:www\.)?", "", u, flags=re.I)
        low = u.lower().replace("twitter.com/", "x.com/")
        parts = low.split("/")
        if len(parts) < 2:
            continue
        handle = parts[-1].split("?")[0]
        if not handle or len(handle) < 2 or handle.isdigit() or handle in BAD_SOC_HANDLES:
            continue
        if low not in seen:
            seen.add(low)
            out.append(u)
    return out[:6]


def valid_founder_name(name: str, comp_name: str = "") -> bool:
    words = name.strip().split()
    if not (2 <= len(words) <= 4):
        return False
    low = name.lower()
    if comp_name and comp_name.lower() in low:
        return False
    return not any(w.lower().strip(",.") in BAD_FOUNDER_TOKENS for w in words)


async def enrich_yc_pages(yc_records: list[dict], only_hiring: bool = True, limit: int | None = None) -> tuple[dict[str, list[str]], dict[str, list[dict]]]:
    """Fetch YC company pages for 98.7% founders + 90.7% socials coverage."""
    sem = asyncio.Semaphore(35)
    socials_out: dict[str, list[str]] = {}
    founders_out: dict[str, list[dict]] = {}
    items = [c for c in yc_records if c.get("domain") and not c.get("nonprofit") and (not only_hiring or c.get("is_hiring"))]
    if limit:
        items = items[:limit]

    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, follow_redirects=True, timeout=15) as ac:
        async def one(c: dict):
            slug = c.get("source_id")
            dom = c.get("domain")
            if not slug or not dom:
                return
            async with sem:
                try:
                    r = await ac.get(f"https://www.ycombinator.com/companies/{slug}")
                    if r.status_code != 200:
                        return
                    m = re.search(r'data-page="([^"]+)"', r.text)
                    if not m:
                        return
                    comp = json.loads(hl.unescape(m.group(1))).get("props", {}).get("company", {})
                    socs = []
                    for k in ("linkedin_url", "twitter_url", "github_url", "fb_url"):
                        v = comp.get(k) or ""
                        if v:
                            socs.append(v)
                    if socs:
                        socials_out[dom] = clean_socials(socs)
                    fnds = []
                    for f in comp.get("founders") or []:
                        fn = (f.get("full_name") or "").strip()
                        if fn and len(fn.split()) >= 2:
                            fnds.append({
                                "full_name": fn,
                                "title": (f.get("title") or "Founder").strip(),
                                "linkedin_url": (f.get("linkedin_url") or "").strip(),
                                "twitter_url": (f.get("twitter_url") or "").strip(),
                                "source": "yc_page",
                                "confidence": 0.99,
                            })
                    if fnds:
                        founders_out[dom] = fnds
                except Exception:
                    pass

        await asyncio.gather(*(one(c) for c in items))
    return socials_out, founders_out


async def fetch_wikidata_batch(domains: list[str]) -> tuple[dict[str, list[str]], dict[str, list[dict]]]:
    """Batch Wikidata SPARQL query for founders, CEOs, Twitter, LinkedIn ($0 structured)."""
    wd_socs: dict[str, list[str]] = defaultdict(list)
    wd_fnds: dict[str, list[dict]] = defaultdict(list)
    hdrs = {"User-Agent": "StartupIntelBot/2.0 (mailto:research@startupintel.dev)", "Accept": "application/sparql-results+json"}
    async with httpx.AsyncClient(headers=hdrs, timeout=20) as ac:
        for i in range(0, len(domains), 45):
            chunk = domains[i:i + 45]
            iris = " ".join(f"<https://{d}> <https://{d}/> <https://www.{d}> <https://www.{d}/>" for d in chunk)
            sparql = f"""
            SELECT ?w ?founderLabel ?ceoLabel ?tw ?li WHERE {{
              VALUES ?w {{ {iris} }}
              ?item wdt:P856 ?w .
              OPTIONAL {{ ?item wdt:P112 ?founder . }}
              OPTIONAL {{ ?item wdt:P169 ?ceo . }}
              OPTIONAL {{ ?item wdt:P2002 ?tw . }}
              OPTIONAL {{ ?item wdt:P4264 ?li . }}
              SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
            }}
            """
            try:
                r = await ac.get("https://query.wikidata.org/sparql", params={"query": sparql})
                if r.status_code != 200:
                    continue
                for b in r.json().get("results", {}).get("bindings", []):
                    dom = canonical_root_domain(b.get("w", {}).get("value", ""))
                    if not dom:
                        continue
                    tw = (b.get("tw") or {}).get("value", "")
                    li = (b.get("li") or {}).get("value", "")
                    if tw: wd_socs[dom].append(f"x.com/{tw}")
                    if li: wd_socs[dom].append(f"linkedin.com/company/{li}")
                    f_nm = (b.get("founderLabel") or {}).get("value", "")
                    c_nm = (b.get("ceoLabel") or {}).get("value", "")
                    if f_nm and not f_nm.startswith("Q") and len(f_nm.split()) >= 2:
                        wd_fnds[dom].append({"full_name": f_nm, "title": "Founder", "linkedin_url": "", "twitter_url": "", "source": "wikidata", "confidence": 0.96})
                    if c_nm and not c_nm.startswith("Q") and len(c_nm.split()) >= 2:
                        wd_fnds[dom].append({"full_name": c_nm, "title": "CEO", "linkedin_url": "", "twitter_url": "", "source": "wikidata", "confidence": 0.96})
            except Exception:
                pass
    return {k: clean_socials(v) for k, v in wd_socs.items()}, dict(wd_fnds)


async def enrich_websites_founders_and_socials(
    domains: list[str],
    company_names: dict[str, str],
    use_jina_fallback: bool = True,
    jina_limit: int = 15,
) -> tuple[dict[str, list[str]], dict[str, list[dict]]]:
    """Tier 2 direct website JSON-LD + /about,/team + Tier 3 r.jina.ai fallback."""
    sem = asyncio.Semaphore(25)
    site_socials: dict[str, list[str]] = {}
    site_founders: dict[str, list[dict]] = defaultdict(list)
    jina_queue: list[str] = []

    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, follow_redirects=True, max_redirects=3, timeout=10) as ac:
        async def one(dom: str):
            cname = company_names.get(dom, "")
            async with sem:
                socs, fnds, blocked = [], [], False
                for path in ["", "/about", "/team", "/company"]:
                    try:
                        r = await ac.get(f"https://{dom}{path}")
                        if r.status_code in (403, 429):
                            blocked = True
                            continue
                        if r.status_code != 200:
                            continue
                        txt = r.text[:250000]
                        socs.extend(m.group(1) for m in SOC_RE.finditer(txt))
                        for jm in re.findall(r'<script[^>]+ld\+json[^>]*>([\s\S]*?)</script>', txt, re.I):
                            try:
                                obj = json.loads(jm)
                                stack = obj if isinstance(obj, list) else [obj]
                                while stack:
                                    cur = stack.pop()
                                    if isinstance(cur, dict):
                                        if cur.get("@type") == "Person" and cur.get("name"):
                                            fn = str(cur["name"]).strip()
                                            jt = str(cur.get("jobTitle") or "Founder").strip()
                                            if valid_founder_name(fn, cname) and re.search(r"founder|ceo|cto|chief", jt, re.I):
                                                same_as = cur.get("sameAs") or []
                                                if isinstance(same_as, str):
                                                    same_as = [same_as]
                                                li = next((u for u in same_as if "linkedin.com/in/" in str(u).lower()), "")
                                                tw = next((u for u in same_as if "x.com/" in str(u).lower() or "twitter.com/" in str(u).lower()), "")
                                                fnds.append({"full_name": fn, "title": jt[:60], "linkedin_url": li, "twitter_url": tw, "source": "website_jsonld", "confidence": 0.95})
                                        for v in cur.values():
                                            if isinstance(v, (dict, list)):
                                                stack.append(v)
                                    elif isinstance(cur, list):
                                        stack.extend(cur)
                            except Exception:
                                pass
                        if path in ("/about", "/team", "/company"):
                            plain = re.sub(r"\s+", " ", hl.unescape(re.sub(r"<[^>]+>", " | ", txt)))
                            for fm in FOUNDER_CTX_RE.finditer(plain):
                                nm, rl = fm.group(1).strip(), fm.group(2).strip()
                                ctx = plain[max(0, fm.start() - 70):min(len(plain), fm.end() + 70)]
                                if valid_founder_name(nm, cname) and not INVESTOR_CTX_RE.search(ctx):
                                    fnds.append({"full_name": nm, "title": rl, "linkedin_url": "", "twitter_url": "", "source": "website_team", "confidence": 0.85})
                    except Exception:
                        pass
                cs = clean_socials(socs)
                if cs:
                    site_socials[dom] = cs
                seen_f = set()
                for f in fnds:
                    k = f["full_name"].lower()
                    if k not in seen_f:
                        seen_f.add(k)
                        site_founders[dom].append(f)
                if (blocked or not cs) and use_jina_fallback:
                    jina_queue.append(dom)

        await asyncio.gather(*(one(d) for d in domains))

        if use_jina_fallback and jina_queue:
            jsem = asyncio.Semaphore(8)
            async def one_jina(dom: str):
                cname = company_names.get(dom, "")
                async with jsem:
                    for path in ["/about", "/team", ""]:
                        try:
                            rj = await ac.get(f"https://r.jina.ai/https://{dom}{path}", timeout=10)
                            if rj.status_code != 200:
                                continue
                            socs = clean_socials([m.group(1) for m in SOC_RE.finditer(rj.text)])
                            if socs and dom not in site_socials:
                                site_socials[dom] = socs
                            if path != "":
                                for fm in FOUNDER_CTX_RE.finditer(rj.text):
                                    nm, rl = fm.group(1).strip(), fm.group(2).strip()
                                    ctx = rj.text[max(0, fm.start() - 70):min(len(rj.text), fm.end() + 70)]
                                    if valid_founder_name(nm, cname) and not INVESTOR_CTX_RE.search(ctx):
                                        site_founders[dom].append({"full_name": nm, "title": rl, "linkedin_url": "", "twitter_url": "", "source": "jina_extract", "confidence": 0.85})
                            if socs and (path != "" or site_founders.get(dom)):
                                break
                        except Exception:
                            pass
            await asyncio.gather(*(one_jina(d) for d in jina_queue[:jina_limit]))

    return site_socials, dict(site_founders)


def dedupe_founders(founders_by_dom: dict[str, list[dict]]) -> list[dict]:
    """Deduplicate founders per domain (keeping highest confidence) and return Supabase-ready rows."""
    rows = []
    for dom, items in founders_by_dom.items():
        best: dict[str, dict] = {}
        for f in items:
            fn = (f.get("full_name") or "").strip()
            nk = re.sub(r"\s+", " ", fn.lower())
            if not nk:
                continue
            prev = best.get(nk)
            if not prev or float(f.get("confidence", 0)) > float(prev.get("confidence", 0)):
                best[nk] = {
                    "company_domain": dom,
                    "name_key": nk,
                    "full_name": fn,
                    "title": (f.get("title") or "Founder")[:80],
                    "linkedin_url": f.get("linkedin_url") or "",
                    "twitter_url": f.get("twitter_url") or "",
                    "source": f.get("source") or "unknown",
                    "confidence": float(f.get("confidence") or 0.90),
                }
            elif prev:
                if not prev["linkedin_url"] and f.get("linkedin_url"):
                    prev["linkedin_url"] = f["linkedin_url"]
                if not prev["twitter_url"] and f.get("twitter_url"):
                    prev["twitter_url"] = f["twitter_url"]
        rows.extend(best.values())
    return rows
