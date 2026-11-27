"""Strict 1-to-1 ATS ownership verification, website ATS extraction, and liveness checks."""

from __future__ import annotations

import asyncio
import re
import httpx
from rapidfuzz import fuzz

GENERIC_BAD_SLUGS = {
    "corp", "us", "uk", "eu", "jp", "japan", "ww", "solutions", "app", "web",
    "tech", "team", "jobs", "careers", "hello", "get", "try", "join", "use",
    "my", "hq", "home", "info", "portal", "www", "embed", "api", "assets",
}

PARKED_HINTS = (
    "domain is for sale", "buy this domain", "sedoparking", "godaddy",
    "hugedomains", "dan.com", "afternic", "parked free", "this domain has expired",
    "domain parking", "is available for purchase",
)

IGNORE_EXT_DOMAINS = {
    "greenhouse.io", "ashbyhq.com", "lever.co", "workable.com", "rippling.com",
    "bamboohr.com", "smartrecruiters.com", "breezy.hr", "recruitee.com",
    "pinpointhq.com", "personio.de", "teamtailor.com", "ashbyprd.com",
    "cloudfront.net", "amazonaws.com", "w3.org", "schema.org", "ogp.me",
    "google.com", "googletagmanager.com", "gstatic.com", "typekit.net",
    "cloudflare.com", "sentry.io", "datadoghq.com", "recaptcha.net",
    "linkedin.com", "twitter.com", "x.com", "facebook.com", "instagram.com",
    "youtube.com", "apple.com", "apps.apple.com", "play.google.com",
    "wikipedia.org", "crunchbase.com", "g2.com", "bloomberg.com", "example.com",
    "optimizely.com", "unpkg.com", "jsdelivr.net", "fonts.googleapis.com",
    "bfldr.com", "fontawesome.com", "notion.site", "glassdoor.com", "indeed.com",
    "googleapis.com", "cookielaw.org", "cdn-cookieyes.com", "cookieyes.com",
    "cloudinary.com", "onetrust.com", "usercentrics.eu", "zendesk.com",
    "intercom.io", "hubspot.com", "segment.com", "mixpanel.com",
}

_LEGAL_SUFFIX_RE = re.compile(
    r"(?:,?\s+(?:inc\.?|incorporated|llc|ltd\.?|limited|corp\.?|corporation|co\.?|plc|gmbh|ag|sa|s\.a\.|bv|pty|pte))+$",
    re.I,
)

ATS_ANY_RE = re.compile(
    r"https?://(?:(boards|job-boards)\.(greenhouse)\.io|"
    r"jobs\.(lever)\.co|"
    r"jobs\.(ashbyhq)\.com|"
    r"apply\.(workable)\.com|"
    r"jobs\.(smartrecruiters)\.com|"
    r"ats\.(rippling)\.com|"
    r"([a-z0-9-]+)\.jobs\.(personio)\.de|"
    r"([a-z0-9-]+)\.(teamtailor)\.com)/([A-Za-z0-9_.-]+)",
    re.I,
)
GH_EMBED_RE = re.compile(r"greenhouse\.io/embed/job_board/(?:js\?for=)?([A-Za-z0-9_.-]+)", re.I)
ASHBY_EMBED_RE = re.compile(r"ashbyhq\.com/posting-api/job-board/([A-Za-z0-9_.-]+)|ashby_jid=([A-Za-z0-9_.-]+)", re.I)


def extract_ats_candidates(url_str: str, html_txt: str) -> list[tuple[str, str]]:
    found = []
    for txt in (url_str, html_txt):
        for m in ATS_ANY_RE.finditer(txt):
            g = m.groups()
            seg = g[10]
            if seg and seg.lower() in ("embed", "api", "assets", "jobs", "v1", "v0"):
                continue
            if g[1]: found.append(("greenhouse", seg))
            elif g[2]: found.append(("lever", seg))
            elif g[3]: found.append(("ashby", seg))
            elif g[4]: found.append(("workable", seg))
            elif g[5]: found.append(("smartrecruiters", seg))
            elif g[6]: found.append(("rippling", seg))
            elif g[8]: found.append(("personio", g[7]))
            elif g[9]: found.append(("teamtailor", g[9]))
        for m in GH_EMBED_RE.finditer(txt):
            if m.group(1) and m.group(1).lower() not in ("js", "for"):
                found.append(("greenhouse", m.group(1)))
        for m in ASHBY_EMBED_RE.finditer(txt):
            slug = m.group(1) or m.group(2)
            if slug:
                found.append(("ashby", slug))
    seen, out = set(), []
    for p, s in found:
        k = (p, s.lower())
        if k not in seen:
            seen.add(k)
            out.append((p, s))
    return out


def _root_dom(host: str) -> str:
    parts = host.lower().strip(".").split(".")
    if len(parts) >= 3 and ".".join(parts[-2:]) in {"co.uk", "co.jp", "com.br", "com.au", "co.il", "co.kr"}:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:]) if len(parts) >= 2 else host.lower()


def _norm_brand(name: str) -> str:
    s = (name or "").strip().lower()
    s = _LEGAL_SUFFIX_RE.sub("", s).strip(" ,.-")
    return re.sub(r"\s+", " ", s)


async def probe_ats_ownership(client: httpx.AsyncClient, provider: str, slug: str) -> dict:
    url_map = {
        "greenhouse": f"https://boards-api.greenhouse.io/v1/boards/{slug}",
        "greenhouse_html": f"https://boards.greenhouse.io/{slug}",
        "ashby": f"https://jobs.ashbyhq.com/{slug}",
        "lever": f"https://jobs.lever.co/{slug}",
        "workable": f"https://apply.workable.com/{slug}/",
        "rippling": f"https://ats.rippling.com/{slug}/jobs",
        "teamtailor": f"https://{slug}.teamtailor.com/jobs",
    }
    board_name = ""
    ext_domains: set[str] = set()
    http_status = 0
    try:
        if provider == "greenhouse":
            r_api, r_html = await asyncio.gather(
                client.get(url_map["greenhouse"], timeout=10),
                client.get(url_map["greenhouse_html"], timeout=10, follow_redirects=True),
                return_exceptions=True,
            )
            if isinstance(r_api, httpx.Response) and r_api.status_code == 200:
                http_status = 200
                board_name = (r_api.json().get("name") or "").strip()
            if isinstance(r_html, httpx.Response):
                http_status = http_status or r_html.status_code
                if not board_name:
                    m = re.search(r"<title>(.*?)</title>", r_html.text, re.I | re.S)
                    if m:
                        board_name = re.sub(r"^(?:jobs at|careers at)\s+", "", m.group(1).strip(), flags=re.I)
                for h in re.findall(r'https?://([a-z0-9.-]+\.[a-z]{2,10})', r_html.text, re.I):
                    rd = _root_dom(h)
                    if rd and rd not in IGNORE_EXT_DOMAINS and not rd.endswith((".edu", ".gov", ".mil")):
                        ext_domains.add(rd)
            if http_status == 200 and not ext_domains:
                try:
                    rj = await client.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true", timeout=10)
                    if rj.status_code == 200:
                        for j in (rj.json().get("jobs") or [])[:5]:
                            for h in re.findall(r'https?://([a-z0-9.-]+\.[a-z]{2,10})', j.get("content") or "", re.I):
                                rd = _root_dom(h)
                                if rd and rd not in IGNORE_EXT_DOMAINS and not rd.endswith((".edu", ".gov", ".mil")):
                                    ext_domains.add(rd)
                except Exception:
                    pass
        elif provider in url_map:
            r = await client.get(url_map[provider], timeout=10, follow_redirects=True)
            http_status = r.status_code
            if r.status_code == 200:
                m = re.search(r"<title>(.*?)</title>", r.text, re.I | re.S)
                if m:
                    t = m.group(1).strip()
                    board_name = re.sub(r"\s*(?:Jobs|Careers|Open Roles|\|.*|- Jobs).*$", "", t, flags=re.I).strip()
                for h in re.findall(r'https?://([a-z0-9.-]+\.[a-z]{2,10})', r.text, re.I):
                    rd = _root_dom(h)
                    if rd and rd not in IGNORE_EXT_DOMAINS and not rd.endswith((".edu", ".gov", ".mil")):
                        ext_domains.add(rd)
    except Exception:
        pass
    return {
        "provider": provider,
        "slug": slug,
        "http_status": http_status,
        "board_name": board_name,
        "ext_domains": sorted(ext_domains),
    }


def score_ats_ownership(company: dict, board_meta: dict) -> tuple[bool, int, str]:
    slug = (company.get("ats_slug") or "").lower()
    dom = _root_dom(company.get("domain") or "")
    dom_base = dom.split(".")[0] if dom else ""
    cname = _norm_brand(company.get("name") or "")
    team = company.get("team_size")
    jobs_cnt = company.get("open_jobs_count") or 0
    bname = _norm_brand(board_meta.get("board_name") or "")
    ext_doms = set(board_meta.get("ext_domains") or [])

    if slug in GENERIC_BAD_SLUGS and dom not in ext_doms:
        return False, 5, f"generic_slug:{slug}"
    if team and team > 0 and jobs_cnt > max(50, team * 5):
        return False, 10, f"jobs_vs_team_mismatch:{jobs_cnt}>{team}*5"

    score = 0
    reasons = []
    dom_linked = bool(dom and dom in ext_doms)
    if dom_linked:
        score += 75
        reasons.append("board_links_to_domain")
    elif ext_doms and dom:
        if any(dom_base and fuzz.ratio(dom_base, ed.split(".")[0]) >= 80 for ed in ext_doms):
            score -= 50
            reasons.append(f"board_links_other_domain:{sorted(ext_doms)[:2]}")

    if bname:
        ns = fuzz.token_set_ratio(cname, bname)
        nr = fuzz.ratio(cname, bname)
        best_n = max(ns, nr)
        if nr >= 88:
            score += 45
            reasons.append(f"exact_board_name({nr:.0f})")
        elif best_n >= 75:
            score += 25
            reasons.append(f"close_board_name({best_n:.0f})")
        elif not dom_linked:
            score -= 50
            reasons.append(f"board_name_mismatch:'{bname}'({best_n:.0f})")
    else:
        if dom_base and slug == dom_base:
            score += 40
            reasons.append("slug==domain_base")
        elif max(fuzz.ratio(cname.replace(" ", ""), slug), fuzz.ratio(dom_base, slug)) >= 80:
            score += 25
            reasons.append("slug_similar")

    if dom_base and slug == dom_base:
        score += 20
        reasons.append("slug==dom")

    score = max(0, min(100, score))
    return score >= 55, score, ",".join(reasons) or "weak_signals"
