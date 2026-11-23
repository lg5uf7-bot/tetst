"""
Step 3: Poll all `ats_active = true` boards across 10 ATS providers using
verified per-provider adapters + detail endpoints, normalize all fields
(including country, experience_level, structured salary, full description),
deduplicate per (company_id, ats_provider, external_id), diff via
`content_hash`, and log telemetry to `github_action_runs`.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import httpx
from bs4 import BeautifulSoup

from supabase_client import SupabaseREST, utc_now_iso

MAX_CONCURRENT_BY_PROVIDER = {
    "ashby": 5,           # strict rate limits; paired with 0.18s spacing
    "greenhouse": 25,
    "lever": 25,
    "rippling": 20,
    "workable": 15,
    "breezy": 15,
    "bamboohr": 15,
    "pinpoint": 15,
    "recruitee": 15,
    "smartrecruiters": 15,
}
ASHBY_SPACING_SECONDS = 0.18
DETAIL_CONCURRENCY = 25
MISSING_STREAK_TO_CLOSE = int(os.environ.get("MISSING_STREAK_TO_CLOSE", "2"))

US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
    "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV",
    "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
}

COUNTRY_ALIASES = {
    "us": "United States",
    "usa": "United States",
    "u.s.": "United States",
    "u.s.a.": "United States",
    "united states": "United States",
    "united states of america": "United States",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "gb": "United Kingdom",
    "great britain": "United Kingdom",
    "england": "United Kingdom",
    "united kingdom": "United Kingdom",
    "ca": "Canada",
    "canada": "Canada",
    "de": "Germany",
    "germany": "Germany",
    "deutschland": "Germany",
    "fr": "France",
    "france": "France",
    "es": "Spain",
    "spain": "Spain",
    "in": "India",
    "india": "India",
    "au": "Australia",
    "australia": "Australia",
    "nl": "Netherlands",
    "netherlands": "Netherlands",
    "sg": "Singapore",
    "singapore": "Singapore",
    "ie": "Ireland",
    "ireland": "Ireland",
    "il": "Israel",
    "israel": "Israel",
    "br": "Brazil",
    "brazil": "Brazil",
    "mx": "Mexico",
    "mexico": "Mexico",
    "pl": "Poland",
    "poland": "Poland",
    "se": "Sweden",
    "sweden": "Sweden",
    "ch": "Switzerland",
    "switzerland": "Switzerland",
    "jp": "Japan",
    "japan": "Japan",
    "kr": "South Korea",
    "south korea": "South Korea",
    "pt": "Portugal",
    "portugal": "Portugal",
    "it": "Italy",
    "italy": "Italy",
    "ua": "Ukraine",
    "ukraine": "Ukraine",
    "co": "Colombia",
    "colombia": "Colombia",
    "ar": "Argentina",
    "argentina": "Argentina",
}

US_CITIES = {
    "san francisco", "new york", "nyc", "brooklyn", "manhattan", "seattle", "austin",
    "boston", "los angeles", "chicago", "palo alto", "mountain view", "menlo park",
    "sunnyvale", "santa clara", "san jose", "san diego", "denver", "boulder", "miami",
    "atlanta", "washington", "redwood city", "san mateo", "oakland", "berkeley",
}


def clean_str(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, str):
        return val.strip()
    return str(val).strip()


def strip_html(raw: str | None) -> str:
    if not raw:
        return ""
    text = html.unescape(str(raw))
    if "<" in text and ">" in text:
        soup = BeautifulSoup(text, "lxml")
        text = soup.get_text(separator="\n")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
    return "\n".join(ln for ln in lines if ln)[:20000]


def make_content_hash(title: str, location: str, department: str, description_text: str) -> str:
    payload = f"v2|{title.strip()}|{location.strip()}|{department.strip()}|{description_text.strip()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_iso_ts(val: Any) -> str | None:
    if not val:
        return None
    if isinstance(val, (int, float)):
        sec = val / 1000.0 if val > 1e12 else float(val)
        return datetime.fromtimestamp(sec, tz=timezone.utc).isoformat()
    if isinstance(val, str):
        s = val.strip()
        return s or None
    return None


def infer_workplace(raw_workplace: str | None, location: str, title: str) -> tuple[str, bool]:
    w = clean_str(raw_workplace).lower()
    loc = clean_str(location).lower()
    t = clean_str(title).lower()
    if "remote" in w or "remote" in loc or "(remote)" in t or re.search(r"\bremote\b", t):
        return ("remote", True)
    if "hybrid" in w or "hybrid" in loc or "hybrid" in t:
        return ("hybrid", False)
    if "onsite" in w or "on-site" in w or "on_site" in w or "in-office" in w or "in person" in loc:
        return ("onsite", False)
    return ("unknown", False)


def normalize_employment_type(val: Any) -> str | None:
    s = clean_str(val).lower().replace("-", "_").replace(" ", "_")
    if not s:
        return None
    if any(k in s for k in ("full_time", "fulltime", "salaried_ft", "permanent", "full")):
        return "full_time"
    if any(k in s for k in ("part_time", "parttime", "part")):
        return "part_time"
    if any(k in s for k in ("intern", "internship", "co_op", "coop")):
        return "intern"
    if any(k in s for k in ("contract", "contractor", "freelance", "temporary", "temp")):
        return "contract"
    return s[:40]


def extract_country(location: str, raw_country: Any = None) -> str | None:
    rc = clean_str(raw_country)
    if rc:
        key = rc.lower()
        return COUNTRY_ALIASES.get(key, rc)

    loc = clean_str(location)
    if not loc:
        return None

    low = loc.lower()
    # Direct mention inside parentheses or tokens, e.g. "Remote (United States)"
    for token in re.split(r"[,;/()|]+", low):
        tok = token.strip()
        if tok in COUNTRY_ALIASES:
            return COUNTRY_ALIASES[tok]

    # Trailing 2-letter US state code like "San Francisco, CA"
    parts = [p.strip() for p in loc.split(",") if p.strip()]
    if parts:
        last = parts[-1].split()[0] if parts[-1].split() else ""
        if last.upper() in US_STATES:
            return "United States"

    if any(city in low for city in US_CITIES):
        return "United States"

    return None


def extract_experience_level(title: str, raw_exp: Any = None) -> str | None:
    rexp = clean_str(raw_exp).lower()
    if rexp and rexp not in ("not applicable", "not_applicable", "none"):
        if "intern" in rexp:
            return "intern"
        if any(k in rexp for k in ("entry", "junior", "associate", "new grad")):
            return "junior"
        if "senior" in rexp or "sr" in rexp:
            return "senior"
        if "mid" in rexp:
            return "mid"
        if "director" in rexp or "executive" in rexp or "vp" in rexp:
            return "executive"
        return rexp[:40]

    t = f" {clean_str(title).lower()} "
    if any(k in t for k in (" intern ", " internship ", " co-op ", " coop ")):
        return "intern"
    if any(k in t for k in (" chief ", " ceo ", " cto ", " cfo ", " coo ", " cpo ", " vp ", " vice president ", " head of ")):
        return "executive"
    if " director " in t:
        return "director"
    if " principal " in t:
        return "principal"
    if " staff " in t:
        return "staff"
    if " lead " in t or " team lead" in t or " tech lead" in t:
        return "lead"
    if " manager " in t or " engineering manager" in t:
        return "manager"
    if any(k in t for k in (" senior ", " sr. ", " sr ")):
        return "senior"
    if any(k in t for k in (" junior ", " jr. ", " jr ", " entry level ", " entry-level ", " new grad ", " graduate ")):
        return "junior"
    return None


def parse_salary_from_text(text: str) -> tuple[float | None, float | None, str | None]:
    if not text:
        return (None, None, None)
    # Match ranges like "$130,000 - $180,000", "$150K - $220K", "€100k - €140k"
    m = re.search(
        r"([\$€£])\s*(\d{2,3}(?:,\d{3})+|\d{2,3}\s*[kK])\s*[-–—to]+\s*[\$€£]?\s*(\d{2,3}(?:,\d{3})+|\d{2,3}\s*[kK])",
        text,
    )
    if not m:
        return (None, None, None)

    sym, raw_min, raw_max = m.groups()
    cur = {"$": "USD", "€": "EUR", "£": "GBP"}.get(sym)

    def _to_num(s: str) -> float:
        s_clean = s.replace(",", "").strip().lower()
        if s_clean.endswith("k"):
            return float(s_clean[:-1]) * 1000.0
        return float(s_clean)

    smin, smax = _to_num(raw_min), _to_num(raw_max)
    if smin < 15000 or smax > 2000000 or smin > smax:
        return (None, None, None)
    return (smin, smax, cur)


def build_job_record(
    *,
    external_id: str,
    title: str,
    department: str = "",
    team: str = "",
    location: str = "",
    raw_country: Any = None,
    raw_workplace: str | None = None,
    is_remote_override: bool | None = None,
    raw_employment_type: Any = None,
    raw_experience: Any = None,
    salary_min: float | None = None,
    salary_max: float | None = None,
    salary_currency: str | None = None,
    salary_text: str | None = None,
    apply_url: str | None = None,
    job_url: str | None = None,
    description_text: str = "",
    published_at: str | None = None,
) -> dict[str, Any]:
    wp, is_rem = infer_workplace(raw_workplace, location, title)
    if is_remote_override is True:
        wp, is_rem = "remote", True

    sal_txt = clean_str(salary_text) or None
    smin, smax, scur = salary_min, salary_max, clean_str(salary_currency).upper() or None
    if smin is None and (sal_txt or description_text):
        p_min, p_max, p_cur = parse_salary_from_text(sal_txt or description_text[:6000])
        if p_min is not None:
            smin, smax = p_min, p_max
            scur = scur or p_cur
            if not sal_txt and scur:
                sal_txt = f"{scur} {int(smin):,} - {int(smax):,}"

    return {
        "external_id": external_id,
        "title": title,
        "department": clean_str(department) or None,
        "team": clean_str(team) or None,
        "location": clean_str(location) or None,
        "country": extract_country(location, raw_country),
        "workplace_type": wp,
        "is_remote": is_rem,
        "employment_type": normalize_employment_type(raw_employment_type),
        "experience_level": extract_experience_level(title, raw_experience),
        "salary_min": smin,
        "salary_max": smax,
        "salary_currency": scur,
        "salary_text": sal_txt,
        "apply_url": clean_str(apply_url) or clean_str(job_url) or None,
        "job_url": clean_str(job_url) or clean_str(apply_url) or None,
        "description_text": clean_str(description_text) or None,
        "published_at": published_at,
        "content_hash": make_content_hash(title, location, department, description_text),
    }


# ---------------------------------------------------------------------------
# Per-ATS Adapters
# ---------------------------------------------------------------------------

def parse_ashby(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    out = []
    for j in payload.get("jobs", []):
        ext_id = clean_str(j.get("id"))
        if not ext_id:
            continue
        title = clean_str(j.get("title"))
        dept = clean_str(j.get("department"))
        team = clean_str(j.get("team"))
        loc = clean_str(j.get("location"))
        addr = (j.get("address") or {}).get("postalAddress") if isinstance(j.get("address"), dict) else {}
        raw_country = addr.get("addressCountry") if isinstance(addr, dict) else None
        desc = strip_html(j.get("descriptionPlain") or j.get("descriptionHtml"))

        comp = j.get("compensation") if isinstance(j.get("compensation"), dict) else {}
        sal_text = clean_str(comp.get("compensationTierSummary") or comp.get("scrapeableCompensationSalarySummary"))
        smin = smax = None
        scur = None
        for sc in comp.get("summaryComponents") or []:
            if isinstance(sc, dict) and (sc.get("minValue") is not None or sc.get("maxValue") is not None):
                if sc.get("compensationType") == "Salary" or smin is None:
                    smin = sc.get("minValue")
                    smax = sc.get("maxValue")
                    scur = sc.get("currencyCode")
                    if sc.get("compensationType") == "Salary":
                        break

        out.append(build_job_record(
            external_id=ext_id,
            title=title,
            department=dept,
            team=team,
            location=loc,
            raw_country=raw_country,
            raw_workplace=j.get("workplaceType"),
            is_remote_override=bool(j.get("isRemote")) if j.get("isRemote") is not None else None,
            raw_employment_type=j.get("employmentType"),
            salary_min=float(smin) if smin is not None else None,
            salary_max=float(smax) if smax is not None else None,
            salary_currency=scur,
            salary_text=sal_text,
            apply_url=j.get("applyUrl"),
            job_url=j.get("jobUrl"),
            description_text=desc,
            published_at=parse_iso_ts(j.get("publishedAt")),
        ))
    return out


def parse_greenhouse(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    out = []
    for j in payload.get("jobs", []):
        ext_id = clean_str(j.get("id"))
        if not ext_id:
            continue
        title = clean_str(j.get("title"))
        depts = [d.get("name") for d in (j.get("departments") or []) if isinstance(d, dict) and d.get("name")]
        dept = ", ".join(depts)
        loc_obj = j.get("location") if isinstance(j.get("location"), dict) else {}
        loc = clean_str(loc_obj.get("name"))
        offices = j.get("offices") or []
        office_loc = clean_str(offices[0].get("location")) if offices and isinstance(offices[0], dict) else ""
        desc = strip_html(j.get("content"))
        url = clean_str(j.get("absolute_url"))
        out.append(build_job_record(
            external_id=ext_id,
            title=title,
            department=dept,
            location=loc or office_loc,
            raw_country=extract_country(office_loc),
            apply_url=url,
            job_url=url,
            description_text=desc,
            published_at=parse_iso_ts(j.get("first_published") or j.get("updated_at")),
        ))
    return out


def parse_lever(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    out = []
    for j in payload:
        ext_id = clean_str(j.get("id"))
        if not ext_id:
            continue
        title = clean_str(j.get("text"))
        cats = j.get("categories") if isinstance(j.get("categories"), dict) else {}
        dept = clean_str(cats.get("department"))
        team = clean_str(cats.get("team"))
        loc = clean_str(cats.get("location"))
        if not loc and isinstance(cats.get("allLocations"), list) and cats["allLocations"]:
            loc = clean_str(cats["allLocations"][0])

        desc_parts = [strip_html(j.get("openingPlain") or j.get("opening") or j.get("descriptionPlain") or j.get("description"))]
        for lst in j.get("lists") or []:
            if isinstance(lst, dict):
                hdr = clean_str(lst.get("text"))
                body = strip_html(lst.get("content"))
                if hdr or body:
                    desc_parts.append(f"{hdr}\n{body}".strip())
        add_plain = strip_html(j.get("additionalPlain") or j.get("additional"))
        if add_plain:
            desc_parts.append(add_plain)
        desc = "\n\n".join(p for p in desc_parts if p)[:20000]

        sr = j.get("salaryRange") if isinstance(j.get("salaryRange"), dict) else {}
        smin = float(sr["min"]) if sr.get("min") is not None else None
        smax = float(sr["max"]) if sr.get("max") is not None else None
        scur = clean_str(sr.get("currency")) or None
        sal_txt = clean_str(j.get("salaryDescriptionPlain"))
        if not sal_txt and smin is not None and smax is not None:
            sal_txt = f"{scur or ''} {int(smin):,} - {int(smax):,}".strip()

        out.append(build_job_record(
            external_id=ext_id,
            title=title,
            department=dept,
            team=team,
            location=loc,
            raw_country=j.get("country"),
            raw_workplace=j.get("workplaceType"),
            raw_employment_type=cats.get("commitment"),
            salary_min=smin,
            salary_max=smax,
            salary_currency=scur,
            salary_text=sal_txt,
            apply_url=j.get("applyUrl"),
            job_url=j.get("hostedUrl"),
            description_text=desc,
            published_at=parse_iso_ts(j.get("createdAt")),
        ))
    return out


def parse_rippling_item(j: dict[str, Any], detail: dict[str, Any] | None = None) -> dict[str, Any] | None:
    ext_id = clean_str(j.get("uuid") or j.get("id"))
    if not ext_id:
        return None
    d = detail or {}
    title = clean_str(d.get("name") or j.get("name") or j.get("title"))

    dept_obj = d.get("department") or j.get("department")
    dept = ""
    if isinstance(dept_obj, dict):
        dept = clean_str(dept_obj.get("label") or dept_obj.get("name") or dept_obj.get("base_department"))
    elif dept_obj:
        dept = clean_str(dept_obj)

    loc = ""
    wls = d.get("workLocations")
    if isinstance(wls, list) and wls:
        first = wls[0]
        loc = clean_str(first.get("label") if isinstance(first, dict) else first)
    if not loc:
        wl = j.get("workLocation")
        if isinstance(wl, dict):
            loc = clean_str(wl.get("label") or wl.get("name"))
        elif wl:
            loc = clean_str(wl)

    emp_obj = d.get("employmentType")
    emp = clean_str(emp_obj.get("label") or emp_obj.get("id")) if isinstance(emp_obj, dict) else clean_str(emp_obj)

    desc_obj = d.get("description")
    if isinstance(desc_obj, dict):
        desc = "\n\n".join(
            p for p in (strip_html(desc_obj.get("role")), strip_html(desc_obj.get("company")), strip_html(desc_obj.get("requirements"))) if p
        )[:20000]
    else:
        desc = strip_html(desc_obj)

    smin = smax = None
    scur = None
    sal_txt = ""
    prd = d.get("payRangeDetails")
    if isinstance(prd, list) and prd:
        p0 = prd[0] if isinstance(prd[0], dict) else {}
        smin = float(p0["rangeStart"]) if p0.get("rangeStart") is not None else None
        smax = float(p0["rangeEnd"]) if p0.get("rangeEnd") is not None else None
        scur = clean_str(p0.get("currency")) or None
        if smin is not None and smax is not None:
            sal_txt = f"{scur or ''} {int(smin):,} - {int(smax):,}".strip()

    url = clean_str(d.get("url") or j.get("url"))
    return build_job_record(
        external_id=ext_id,
        title=title,
        department=dept,
        location=loc,
        raw_employment_type=emp,
        salary_min=smin,
        salary_max=smax,
        salary_currency=scur,
        salary_text=sal_txt,
        apply_url=url,
        job_url=url,
        description_text=desc,
        published_at=parse_iso_ts(d.get("createdOn")),
    )


def parse_workable_item(slug: str, j: dict[str, Any], detail: dict[str, Any] | None = None) -> dict[str, Any] | None:
    ext_id = clean_str(j.get("shortcode") or j.get("id"))
    if not ext_id:
        return None
    d = detail or {}
    title = clean_str(d.get("title") or j.get("title"))

    dept_val = d.get("department") or j.get("department")
    dept = ", ".join(clean_str(x) for x in dept_val if x) if isinstance(dept_val, list) else clean_str(dept_val)

    loc_obj = d.get("location") or j.get("location") or {}
    if isinstance(loc_obj, dict):
        loc = ", ".join(filter(None, [clean_str(loc_obj.get("city")), clean_str(loc_obj.get("region")), clean_str(loc_obj.get("country"))]))
        raw_country = loc_obj.get("country") or loc_obj.get("countryCode")
    else:
        loc, raw_country = "", None

    desc_parts = [strip_html(d.get("description")), strip_html(d.get("requirements")), strip_html(d.get("benefits"))]
    desc = "\n\n".join(p for p in desc_parts if p)[:20000]

    smin = float(d["salary_from"]) if d.get("salary_from") is not None else None
    smax = float(d["salary_to"]) if d.get("salary_to") is not None else None
    scur = clean_str(d.get("salary_currency_iso_code")) or None
    sal_txt = f"{scur or ''} {int(smin):,} - {int(smax):,}".strip() if (smin is not None and smax is not None) else None

    url = clean_str(j.get("url")) or f"https://apply.workable.com/{slug}/j/{ext_id}/"
    is_rem = d.get("remote") if d.get("remote") is not None else j.get("remote")
    return build_job_record(
        external_id=ext_id,
        title=title,
        department=dept,
        location=loc,
        raw_country=raw_country,
        raw_workplace=d.get("workplace") or j.get("workplace"),
        is_remote_override=bool(is_rem) if is_rem is not None else None,
        raw_employment_type=d.get("type") or j.get("type"),
        salary_min=smin,
        salary_max=smax,
        salary_currency=scur,
        salary_text=sal_txt,
        apply_url=url,
        job_url=url,
        description_text=desc,
        published_at=parse_iso_ts(d.get("published") or j.get("published")),
    )


def parse_breezy(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    out = []
    for j in payload:
        ext_id = clean_str(j.get("id"))
        if not ext_id:
            continue
        loc_obj = j.get("location") if isinstance(j.get("location"), dict) else {}
        loc = clean_str(loc_obj.get("name"))
        ctry_obj = loc_obj.get("country") if isinstance(loc_obj.get("country"), dict) else {}
        raw_country = ctry_obj.get("name") or ctry_obj.get("id")
        is_rem = bool(loc_obj.get("is_remote")) if loc_obj.get("is_remote") is not None else None
        t_obj = j.get("type") if isinstance(j.get("type"), dict) else {}
        url = clean_str(j.get("url"))
        out.append(build_job_record(
            external_id=ext_id,
            title=clean_str(j.get("name")),
            department=clean_str(j.get("department")),
            location=loc,
            raw_country=raw_country,
            is_remote_override=is_rem,
            raw_employment_type=t_obj.get("name") or t_obj.get("id"),
            salary_text=clean_str(j.get("salary")),
            apply_url=url,
            job_url=url,
            published_at=parse_iso_ts(j.get("published_date")),
        ))
    return out


def parse_bamboohr_item(slug: str, j: dict[str, Any], detail: dict[str, Any] | None = None) -> dict[str, Any] | None:
    ext_id = clean_str(j.get("id"))
    if not ext_id:
        continue_flag = True
        if continue_flag:
            return None
    jo = (detail or {}).get("result", {}).get("jobOpening", {}) if isinstance(detail, dict) else {}
    if not isinstance(jo, dict):
        jo = {}

    title = clean_str(jo.get("jobOpeningName") or j.get("jobOpeningName"))
    dept = clean_str(jo.get("departmentLabel") or j.get("departmentLabel"))
    ats_loc = jo.get("atsLocation") or j.get("atsLocation") or {}
    loc_obj = jo.get("location") or j.get("location") or {}
    city = clean_str(ats_loc.get("city") or loc_obj.get("city"))
    prov = clean_str(ats_loc.get("province") or ats_loc.get("state") or loc_obj.get("state"))
    ctry = clean_str(ats_loc.get("country"))
    loc = ", ".join(filter(None, [city, prov, ctry]))

    is_rem = jo.get("isRemote") if jo.get("isRemote") is not None else j.get("isRemote")
    if str(jo.get("locationType") or j.get("locationType") or "") == "1" and is_rem is None:
        is_rem = True

    url = clean_str(jo.get("jobOpeningShareUrl")) or f"https://{slug}.bamboohr.com/careers/{ext_id}"
    return build_job_record(
        external_id=ext_id,
        title=title,
        department=dept,
        location=loc,
        raw_country=ctry,
        is_remote_override=bool(is_rem) if is_rem is not None else None,
        raw_employment_type=jo.get("employmentStatusLabel") or j.get("employmentStatusLabel"),
        raw_experience=jo.get("minimumExperience"),
        salary_text=clean_str(jo.get("compensation")),
        apply_url=url,
        job_url=url,
        description_text=strip_html(jo.get("description")),
        published_at=parse_iso_ts(jo.get("datePosted")),
    )


def parse_pinpoint(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    out = []
    for j in payload.get("data", []):
        ext_id = clean_str(j.get("id"))
        if not ext_id:
            continue
        job_obj = j.get("job") if isinstance(j.get("job"), dict) else {}
        dept_obj = job_obj.get("department") or j.get("department") or {}
        dept = clean_str(dept_obj.get("name")) if isinstance(dept_obj, dict) else ""
        loc_obj = j.get("location") if isinstance(j.get("location"), dict) else {}
        loc = clean_str(loc_obj.get("name"))
        desc = "\n\n".join(
            p for p in (
                strip_html(j.get("description")),
                strip_html(j.get("key_responsibilities")),
                strip_html(j.get("skills_knowledge_expertise")),
                strip_html(j.get("benefits")),
            ) if p
        )[:20000]
        url = clean_str(j.get("url"))
        smin = float(j["compensation_minimum"]) if j.get("compensation_minimum") is not None else None
        smax = float(j["compensation_maximum"]) if j.get("compensation_maximum") is not None else None
        out.append(build_job_record(
            external_id=ext_id,
            title=clean_str(j.get("title")),
            department=dept,
            location=loc,
            raw_workplace=j.get("workplace_type") or j.get("workplace_type_text"),
            raw_employment_type=j.get("employment_type") or j.get("employment_type_text"),
            salary_min=smin,
            salary_max=smax,
            salary_currency=clean_str(j.get("compensation_currency")) or None,
            salary_text=clean_str(j.get("compensation")),
            apply_url=url,
            job_url=url,
            description_text=desc,
            published_at=None,
        ))
    return out


def parse_recruitee(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    out = []
    for j in payload.get("offers", []):
        ext_id = clean_str(j.get("id"))
        if not ext_id:
            continue
        desc = "\n\n".join(
            p for p in (strip_html(j.get("description")), strip_html(j.get("requirements"))) if p
        )[:20000]
        wp = None
        if j.get("remote"):
            wp = "remote"
        elif j.get("hybrid"):
            wp = "hybrid"
        elif j.get("on_site"):
            wp = "onsite"
        sal = j.get("salary")
        sal_txt = clean_str(sal) if isinstance(sal, str) else ""
        out.append(build_job_record(
            external_id=ext_id,
            title=clean_str(j.get("title")),
            department=clean_str(j.get("department")),
            location=clean_str(j.get("location")),
            raw_country=j.get("country") or j.get("country_code"),
            raw_workplace=wp,
            is_remote_override=bool(j.get("remote")) if j.get("remote") is not None else None,
            raw_employment_type=j.get("employment_type_code"),
            raw_experience=j.get("experience_code"),
            salary_text=sal_txt,
            apply_url=j.get("careers_apply_url") or j.get("careers_url"),
            job_url=j.get("careers_url"),
            description_text=desc,
            published_at=parse_iso_ts(j.get("published_at")),
        ))
    return out


def parse_smartrecruiters(slug: str, payload: Any, details: list[Any] | None = None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    out = []
    items = payload.get("content", [])
    det_list = details if details and len(details) == len(items) else [None] * len(items)
    for j, det in zip(items, det_list):
        ext_id = clean_str(j.get("id"))
        if not ext_id:
            continue
        d = det if isinstance(det, dict) else {}
        dept_obj = (d.get("department") or j.get("department")) if isinstance(d.get("department") or j.get("department"), dict) else {}
        fn_obj = (d.get("function") or j.get("function")) if isinstance(d.get("function") or j.get("function"), dict) else {}
        dept = clean_str(dept_obj.get("label") or fn_obj.get("label"))

        loc_obj = (d.get("location") or j.get("location")) if isinstance(d.get("location") or j.get("location"), dict) else {}
        loc = clean_str(loc_obj.get("fullLocation")) or ", ".join(filter(None, [clean_str(loc_obj.get("city")), clean_str(loc_obj.get("country"))]))
        cf_country = None
        for cf in (d.get("customField") or j.get("customField") or []):
            if isinstance(cf, dict) and cf.get("fieldId") == "COUNTRY":
                cf_country = cf.get("valueLabel") or cf.get("valueId")
                break
        wp = "remote" if loc_obj.get("remote") else ("hybrid" if loc_obj.get("hybrid") else None)
        emp = (d.get("typeOfEmployment") or j.get("typeOfEmployment") or {}).get("label") if isinstance(d.get("typeOfEmployment") or j.get("typeOfEmployment"), dict) else None
        exp = (d.get("experienceLevel") or j.get("experienceLevel") or {}).get("label") if isinstance(d.get("experienceLevel") or j.get("experienceLevel"), dict) else None

        sections = ((d.get("jobAd") or {}).get("sections") or {}) if isinstance(d.get("jobAd"), dict) else {}
        desc_parts = []
        for sec_key in ("companyDescription", "jobDescription", "qualifications", "additionalInformation"):
            sec = sections.get(sec_key)
            if isinstance(sec, dict) and sec.get("text"):
                title_sec = clean_str(sec.get("title"))
                body_sec = strip_html(sec.get("text"))
                if body_sec:
                    desc_parts.append(f"{title_sec}\n{body_sec}".strip() if title_sec else body_sec)
        desc = "\n\n".join(desc_parts)[:20000]

        job_url = clean_str(d.get("postingUrl")) or f"https://jobs.smartrecruiters.com/{slug}/{ext_id}"
        apply_url = clean_str(d.get("applyUrl")) or job_url
        out.append(build_job_record(
            external_id=ext_id,
            title=clean_str(d.get("name") or j.get("name")),
            department=dept,
            location=loc,
            raw_country=cf_country or loc_obj.get("country"),
            raw_workplace=wp,
            is_remote_override=bool(loc_obj.get("remote")) if loc_obj.get("remote") is not None else None,
            raw_employment_type=emp,
            raw_experience=exp,
            apply_url=apply_url,
            job_url=job_url,
            description_text=desc,
            published_at=parse_iso_ts(d.get("releasedDate") or j.get("releasedDate")),
        ))
    return out


# ---------------------------------------------------------------------------
# Async Fetcher (Pagination + Detail Endpoints + Safe Rate Limits)
# ---------------------------------------------------------------------------

async def fetch_json(client: httpx.AsyncClient, sem: asyncio.Semaphore, url: str) -> dict[str, Any] | list[Any] | None:
    async with sem:
        try:
            r = await client.get(url)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
    return None


async def fetch_board(
    client: httpx.AsyncClient,
    sems: dict[str, asyncio.Semaphore],
    detail_sem: asyncio.Semaphore,
    ashby_lock: asyncio.Lock,
    board_key: tuple[str, str, str],
) -> tuple[tuple[str, str, str], list[dict[str, Any]] | None, str | None]:
    provider, slug, api_url = board_key
    sem = sems.get(provider, asyncio.Semaphore(10))

    async with sem:
        for attempt in range(2):
            try:
                if provider == "ashby":
                    async with ashby_lock:
                        await asyncio.sleep(ASHBY_SPACING_SECONDS)
                    sep = "&" if "?" in api_url else "?"
                    r = await client.get(f"{api_url}{sep}includeCompensation=true")

                elif provider == "greenhouse":
                    sep = "&" if "?" in api_url else "?"
                    r = await client.get(f"{api_url}{sep}content=true")

                elif provider == "workable":
                    all_items: list[dict[str, Any]] = []
                    token: str | None = None
                    for _page in range(25):
                        body: dict[str, Any] = {"query": "", "location": [], "department": [], "worktype": [], "remote": []}
                        if token:
                            body["token"] = token
                        r = await client.post(api_url, json=body)
                        if r.status_code != 200:
                            break
                        data = r.json()
                        all_items.extend(data.get("results", []))
                        token = data.get("nextPage")
                        if not token:
                            break
                    if r.status_code == 429:
                        await asyncio.sleep(5)
                        continue
                    if r.status_code != 200 and not all_items:
                        return (board_key, None, f"HTTP {r.status_code}")

                    # Fetch v2 detail JSON for description & salary
                    detail_urls = [
                        f"https://apply.workable.com/api/v2/accounts/{slug}/jobs/{clean_str(j.get('shortcode') or j.get('id'))}"
                        for j in all_items
                    ]
                    details = await asyncio.gather(*[fetch_json(client, detail_sem, u) for u in detail_urls])
                    parsed = [
                        item for j, det in zip(all_items, details)
                        if (item := parse_workable_item(slug, j, det if isinstance(det, dict) else None)) is not None
                    ]
                    dedup = {x["external_id"]: x for x in parsed}
                    return (board_key, list(dedup.values()), None)

                else:
                    r = await client.get(api_url)

                if r.status_code == 429:
                    wait = int(r.headers.get("Retry-After", "5"))
                    await asyncio.sleep(min(wait, 15))
                    continue
                if r.status_code != 200:
                    return (board_key, None, f"HTTP {r.status_code}")

                payload = r.json()

                if provider == "ashby":
                    parsed = parse_ashby(payload)
                elif provider == "greenhouse":
                    parsed = parse_greenhouse(payload)
                elif provider == "lever":
                    parsed = parse_lever(payload)
                elif provider == "rippling":
                    items = payload if isinstance(payload, list) else []
                    det_urls = [
                        f"https://api.rippling.com/platform/api/ats/v1/board/{slug}/jobs/{clean_str(j.get('uuid') or j.get('id'))}"
                        for j in items
                    ]
                    details = await asyncio.gather(*[fetch_json(client, detail_sem, u) for u in det_urls])
                    parsed = [
                        item for j, det in zip(items, details)
                        if (item := parse_rippling_item(j, det if isinstance(det, dict) else None)) is not None
                    ]
                elif provider == "breezy":
                    parsed = parse_breezy(payload)
                elif provider == "bamboohr":
                    items = payload.get("result", []) if isinstance(payload, dict) else []
                    det_urls = [f"https://{slug}.bamboohr.com/careers/{clean_str(j.get('id'))}/detail" for j in items]
                    details = await asyncio.gather(*[fetch_json(client, detail_sem, u) for u in det_urls])
                    parsed = [
                        item for j, det in zip(items, details)
                        if (item := parse_bamboohr_item(slug, j, det if isinstance(det, dict) else None)) is not None
                    ]
                elif provider == "pinpoint":
                    parsed = parse_pinpoint(payload)
                elif provider == "recruitee":
                    parsed = parse_recruitee(payload)
                elif provider == "smartrecruiters":
                    items = payload.get("content", []) if isinstance(payload, dict) else []
                    det_urls = [f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/{clean_str(j.get('id'))}" for j in items]
                    details = await asyncio.gather(*[fetch_json(client, detail_sem, u) for u in det_urls])
                    parsed = parse_smartrecruiters(slug, payload, details)
                else:
                    parsed = []

                dedup = {x["external_id"]: x for x in parsed}
                return (board_key, list(dedup.values()), None)

            except Exception as exc:
                if attempt == 1:
                    return (board_key, None, str(exc)[:200])
                await asyncio.sleep(0.5)
    return (board_key, None, "rate_limited")


async def poll_all_boards(unique_boards: list[tuple[str, str, str]]) -> dict[tuple[str, str, str], tuple[list[dict[str, Any]] | None, str | None]]:
    sems = {p: asyncio.Semaphore(c) for p, c in MAX_CONCURRENT_BY_PROVIDER.items()}
    detail_sem = asyncio.Semaphore(DETAIL_CONCURRENCY)
    ashby_lock = asyncio.Lock()
    limits = httpx.Limits(max_connections=140, max_keepalive_connections=70)
    async with httpx.AsyncClient(timeout=18.0, follow_redirects=True, limits=limits) as client:
        tasks = [fetch_board(client, sems, detail_sem, ashby_lock, bk) for bk in unique_boards]
        results = await asyncio.gather(*tasks)
    return {bk: (jobs, err) for bk, jobs, err in results}


async def patch_many_async(sb: SupabaseREST, patches: list[tuple[str, dict[str, str], dict[str, Any]]]) -> None:
    if not patches:
        return
    sem = asyncio.Semaphore(20)
    headers = {**sb.headers, "Prefer": "return=minimal"}
    async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
        async def _one(table: str, flt: dict[str, str], body: dict[str, Any]) -> None:
            async with sem:
                r = await client.patch(f"{sb.rest_url}/{table}", params=flt, json=body)
                if r.status_code >= 400:
                    raise RuntimeError(f"Patch {table} failed ({r.status_code}): {r.text[:300]}")
        await asyncio.gather(*[_one(t, f, b) for t, f, b in patches])


def main() -> None:
    started_ts = time.time()
    sb = SupabaseREST(timeout=60.0)
    db_run_id = sb.start_run("sync-jobs")

    try:
        companies = sb.select_all(
            "companies",
            select="id,ats_provider,ats_slug,api_url,consecutive_failures",
            filters={"ats_active": "eq.true"},
        )
        print(f"Loaded {len(companies)} ats_active companies from Supabase.")

        boards_to_companies: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for c in companies:
            p, s, u = c.get("ats_provider"), c.get("ats_slug"), c.get("api_url")
            if p and s and u:
                boards_to_companies[(p, s, u)].append(c)

        unique_boards = list(boards_to_companies.keys())
        print(f"Polling {len(unique_boards)} unique ATS JSON boards...")
        board_results = asyncio.run(poll_all_boards(unique_boards))

        existing_jobs = sb.select_all(
            "jobs",
            select="id,company_id,ats_provider,external_id,content_hash,status,missing_streak,first_seen_at",
        )
        existing_map: dict[tuple[str, str, str], dict[str, Any]] = {}
        open_by_company: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for j in existing_jobs:
            key = (j["company_id"], j["ats_provider"], j["external_id"])
            existing_map[key] = j
            if j["status"] == "open":
                open_by_company[j["company_id"]].append(j)

        now = utc_now_iso()
        upsert_rows: list[dict[str, Any]] = []
        patches: list[tuple[str, dict[str, str], dict[str, Any]]] = []
        errors: list[dict[str, Any]] = []
        stats_by_provider: dict[str, int] = defaultdict(int)

        jobs_added = 0
        jobs_updated = 0
        jobs_closed = 0
        boards_checked = len(unique_boards)
        boards_failed = 0

        for bk, comp_list in boards_to_companies.items():
            provider, slug, _url = bk
            fetched_jobs, err = board_results[bk]

            if err is not None or fetched_jobs is None:
                boards_failed += 1
                errors.append({"provider": provider, "slug": slug, "error": err})
                for c in comp_list:
                    patches.append((
                        "companies",
                        {"id": f"eq.{c['id']}"},
                        {
                            "last_jobs_checked_at": now,
                            "consecutive_failures": int(c.get("consecutive_failures") or 0) + 1,
                            "last_error": err,
                        },
                    ))
                continue

            stats_by_provider[provider] += len(fetched_jobs)
            live_ext_ids = {j["external_id"] for j in fetched_jobs}

            for c in comp_list:
                cid = c["id"]
                for j in fetched_jobs:
                    key = (cid, provider, j["external_id"])
                    prev = existing_map.get(key)

                    if prev is None:
                        jobs_added += 1
                        upsert_rows.append({
                            "company_id": cid,
                            "ats_provider": provider,
                            "status": "open",
                            "missing_streak": 0,
                            "first_seen_at": now,
                            "last_seen_at": now,
                            "closed_at": None,
                            "updated_at": now,
                            **j,
                        })
                    elif prev["content_hash"] != j["content_hash"] or prev["status"] != "open":
                        jobs_updated += 1
                        upsert_rows.append({
                            "company_id": cid,
                            "ats_provider": provider,
                            "status": "open",
                            "missing_streak": 0,
                            "first_seen_at": prev.get("first_seen_at") or now,
                            "last_seen_at": now,
                            "closed_at": None,
                            "updated_at": now,
                            **j,
                        })

                for prev in open_by_company.get(cid, []):
                    if prev["external_id"] not in live_ext_ids:
                        new_streak = int(prev.get("missing_streak") or 0) + 1
                        if new_streak >= MISSING_STREAK_TO_CLOSE:
                            jobs_closed += 1
                            patches.append(("jobs", {"id": f"eq.{prev['id']}"}, {"status": "closed", "closed_at": now, "updated_at": now}))
                        else:
                            patches.append(("jobs", {"id": f"eq.{prev['id']}"}, {"missing_streak": new_streak, "updated_at": now}))

                patches.append((
                    "companies",
                    {"id": f"eq.{cid}"},
                    {
                        "open_jobs_count": len(fetched_jobs),
                        "last_jobs_checked_at": now,
                        "last_jobs_success_at": now,
                        "consecutive_failures": 0,
                        "last_error": None,
                    },
                ))

        if upsert_rows:
            print(f"Upserting {len(upsert_rows)} job rows to Supabase...")
            sb.upsert_batch("jobs", upsert_rows, on_conflict="company_id,ats_provider,external_id", batch_size=150)

        if patches:
            print(f"Applying {len(patches)} company/job status updates...")
            asyncio.run(patch_many_async(sb, patches))

        run_status = "success" if boards_failed == 0 else "partial"
        print(
            f"Done: boards={boards_checked} failed={boards_failed} | "
            f"+{jobs_added} added, ~{jobs_updated} updated, -{jobs_closed} closed"
        )
        sb.finish_run(
            db_run_id,
            started_ts,
            status=run_status,
            boards_checked=boards_checked,
            boards_failed=boards_failed,
            jobs_added=jobs_added,
            jobs_updated=jobs_updated,
            jobs_closed=jobs_closed,
            stats_by_provider=dict(stats_by_provider),
            error_log=errors[:100],
        )
    except Exception as exc:
        sb.finish_run(
            db_run_id,
            started_ts,
            status="failed",
            error_log=[{"stage": "sync_jobs", "error": str(exc)}],
        )
        raise
    finally:
        sb.close()


if __name__ == "__main__":
    main()
