"""
Step 3: Poll all `ats_active = true` boards across 10 ATS providers using
verified per-provider adapters + detail endpoints, normalize all fields
(country, workplace_type, employment_type, experience_level, structured salary,
full description), deduplicate per (company_id, ats_provider, external_id),
diff via `content_hash`, and log telemetry to `github_action_runs`.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import os
import re
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import ftfy
import geonamescache
import httpx
import pycountry
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser

from supabase_client import SupabaseREST, utc_now_iso

MAX_CONCURRENT_BY_PROVIDER = {
    "ashby": 5,           # strict rate limits; paired with 0.18s spacing
    "greenhouse": 25,
    "lever": 25,
    "rippling": 20,
    "workable": 10,
    "breezy": 15,
    "bamboohr": 15,
    "pinpoint": 15,
    "recruitee": 15,
    "smartrecruiters": 15,
}
ASHBY_SPACING_SECONDS = 0.18
DETAIL_CONCURRENCY = 20
MISSING_STREAK_TO_CLOSE = int(os.environ.get("MISSING_STREAK_TO_CLOSE", "2"))

US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
    "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV",
    "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
}

COUNTRY_DISPLAY_OVERRIDES = {
    "US": "United States",
    "GB": "United Kingdom",
    "UK": "United Kingdom",
    "RU": "Russia",
    "TR": "Turkey",
    "TW": "Taiwan",
    "KR": "South Korea",
    "VN": "Vietnam",
    "BO": "Bolivia",
    "VE": "Venezuela",
    "TZ": "Tanzania",
    "BN": "Brunei",
    "LA": "Laos",
    "CI": "Côte d'Ivoire",
    "XK": "Kosovo",
    "PS": "Palestine",
    "CZ": "Czech Republic",
    "CD": "Democratic Republic of the Congo",
    "CG": "Congo",
    "IR": "Iran",
    "SY": "Syria",
    "MD": "Moldova",
    "MK": "North Macedonia",
}

def _ascii_fold(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _build_geo_indexes() -> tuple[dict[str, str], dict[str, str], dict[str, str], dict[str, str], dict[str, str]]:
    """Build O(1) normalized lookup tables from pycountry + geonamescache."""
    iso2_to_name: dict[str, str] = {}
    name_to_canonical: dict[str, str] = {}

    for c in pycountry.countries:
        canonical = COUNTRY_DISPLAY_OVERRIDES.get(c.alpha_2) or getattr(c, "common_name", None) or c.name
        iso2_to_name[c.alpha_2.upper()] = canonical
        iso2_to_name[c.alpha_3.upper()] = canonical
        for cand in filter(None, [c.name, getattr(c, "common_name", None), getattr(c, "official_name", None)]):
            name_to_canonical[cand.lower()] = canonical
            name_to_canonical[_ascii_fold(cand).lower()] = canonical

    # Explicit extra country aliases / typos seen in ATS payloads
    extra_country_aliases = {
        "us": "United States", "usa": "United States", "u.s": "United States", "u.s.": "United States",
        "u.s.a": "United States", "u.s.a.": "United States", "united states of america": "United States",
        "uk": "United Kingdom", "u.k.": "United Kingdom", "great britain": "United Kingdom",
        "england": "United Kingdom", "scotland": "United Kingdom", "wales": "United Kingdom",
        "northern ireland": "United Kingdom", "united kingdon": "United Kingdom",
        "deutschland": "Germany", "brasil": "Brazil", "méxico": "Mexico", "mexico": "Mexico",
        "türkiye": "Turkey", "turkey": "Turkey", "czechia": "Czech Republic", "czech republic": "Czech Republic",
        "south korea": "South Korea", "republic of korea": "South Korea", "north korea": "North Korea",
        "people's republic of china": "China", "france, metropolitan": "France",
        "cote d'ivoire (ivory coast)": "Côte d'Ivoire", "ivory coast": "Côte d'Ivoire",
        "côte d'ivoire": "Côte d'Ivoire", "côte d’ivoire": "Côte d'Ivoire",
        "russia": "Russia", "vietnam": "Vietnam", "taiwan": "Taiwan", "hong kong": "Hong Kong",
        "macao": "Macao", "kosovo": "Kosovo", "palestine": "Palestine", "uae": "United Arab Emirates",
        "north america": "United States", "the usa": "United States", "the us": "United States",
        "eu": "European Union", "european union": "European Union",
        "latam": "Latin America", "latin america": "Latin America",
        "worldwide": "Global", "world wide": "Global", "global": "Global",
        "anywhere in the world": "Global",
    }
    name_to_canonical.update(extra_country_aliases)

    # ISO-3166-2 subdivisions (full names + US/CA/AU/BR state names)
    subdiv_to_country: dict[str, str] = {}
    for sub in pycountry.subdivisions:
        cname = iso2_to_name.get(sub.country_code.upper())
        if not cname:
            continue
        sname = sub.name.lower()
        # Avoid ambiguous single words like "georgia" (US state vs country) or short noise
        if len(sname) >= 4 and sname not in name_to_canonical:
            subdiv_to_country.setdefault(sname, cname)
            subdiv_to_country.setdefault(_ascii_fold(sname), cname)

    # Major world cities from geonamescache (pop >= 50k, highest population wins)
    city_to_country: dict[str, tuple[str, int]] = {}
    big_city_to_country: dict[str, tuple[str, int]] = {}
    skip_city_words = {
        "remote", "hybrid", "onsite", "office", "anywhere", "global", "europe", "americas",
        "latam", "apac", "emea", "mobile", "union", "orange", "normal", "general", "central",
        "mission", "nice", "bay area", "north", "south", "east", "west",
    }
    gc = geonamescache.GeonamesCache()
    for _cid, cinfo in gc.get_cities().items():
        pop = int(cinfo.get("population") or 0)
        if pop < 50000:
            continue
        cname = iso2_to_name.get((cinfo.get("countrycode") or "").upper())
        if not cname:
            continue
        for variant in {cinfo["name"].lower(), _ascii_fold(cinfo["name"]).lower()}:
            if len(variant) < 4 or variant in skip_city_words or variant in name_to_canonical:
                continue
            prev = city_to_country.get(variant)
            if prev is None or pop > prev[1]:
                city_to_country[variant] = (cname, pop)
            if pop >= 250000:
                prev_big = big_city_to_country.get(variant)
                if prev_big is None or pop > prev_big[1]:
                    big_city_to_country[variant] = (cname, pop)

    city_flat = {k: v[0] for k, v in city_to_country.items()}
    big_city_flat = {k: v[0] for k, v in big_city_to_country.items()}
    return iso2_to_name, name_to_canonical, subdiv_to_country, city_flat, big_city_flat


ISO2_TO_COUNTRY, COUNTRY_NAME_MAP, SUBDIV_NAME_MAP, CITY_NAME_MAP, BIG_CITY_NAME_MAP = _build_geo_indexes()

COUNTRY_ALIASES = {
    **COUNTRY_NAME_MAP,
    # Extra US/UK/global metro & county aliases not in geonamescache cities
    "nyc": "United States", "bay area": "United States", "silicon valley": "United States",
    "washington dc": "United States", "washington, d.c.": "United States", "us remote": "United States",
    "remote us": "United States", "remote usa": "United States", "remote in us": "United States",
    "us east coast": "United States", "us west coast": "United States",
    "greater london": "United Kingdom", "greater manchester": "United Kingdom",
    "west midlands": "United Kingdom", "south yorkshire": "United Kingdom", "west yorkshire": "United Kingdom",
    "hampshire": "United Kingdom", "hertfordshire": "United Kingdom", "nottinghamshire": "United Kingdom",
    "oxfordshire": "United Kingdom", "lincolnshire": "United Kingdom", "staffordshire": "United Kingdom",
    "lancashire": "United Kingdom", "derbyshire": "United Kingdom", "south wales": "United Kingdom",
    "north west england": "United Kingdom", "sussex": "United Kingdom", "berkshire": "United Kingdom",
    "gloucestershire": "United Kingdom", "warwickshire": "United Kingdom", "leicestershire": "United Kingdom",
    "surrey": "United Kingdom", "kent": "United Kingdom", "essex": "United Kingdom",
    "cheshire": "United Kingdom", "devon": "United Kingdom", "cornwall": "United Kingdom",
    "norfolk": "United Kingdom", "suffolk": "United Kingdom", "somerset": "United Kingdom",
    "dorset": "United Kingdom", "cdmx": "Mexico", "tel-aviv": "Israel", "bengaluru": "India",
    "bangalore": "India", "gurugram": "India", "gurgaon": "India", "münchen": "Germany",
    "zürich": "Switzerland", "kraków": "Poland", "montréal": "Canada", "bogotá": "Colombia",
    "são paulo": "Brazil", "sao paulo": "Brazil", "pristina": "Kosovo", "prishtina": "Kosovo",
    "san_francisconew_york": "United States",
}

MULTI_WORD_COUNTRIES = sorted(
    [(k, v) for k, v in COUNTRY_ALIASES.items() if " " in k or "." in k],
    key=lambda kv: len(kv[0]),
    reverse=True,
)


_MOJIBAKE_RE = re.compile(r"(?:Ã.|â€|Â|\ufffd|ðŸ)")


def clean_str(val: Any) -> str:
    if val is None:
        return ""
    s = val if isinstance(val, str) else str(val)
    s = s.strip()
    if s and not s.isascii() and _MOJIBAKE_RE.search(s):
        s = ftfy.fix_text(s).strip()
    return s


def strip_html(raw: str | None) -> str:
    if not raw:
        return ""
    text = html.unescape(str(raw))
    if "<" in text and ">" in text:
        soup = BeautifulSoup(text, "lxml")
        text = soup.get_text(separator="\n")
    if not text.isascii() and _MOJIBAKE_RE.search(text):
        text = ftfy.fix_text(text)
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
    return "\n".join(ln for ln in lines if ln)[:20000]


def make_content_hash(title: str, location: str, department: str, description_text: str) -> str:
    payload = f"v3|{title.strip()}|{location.strip()}|{department.strip()}|{description_text.strip()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_iso_ts(val: Any) -> str | None:
    if not val:
        return None
    if isinstance(val, (int, float)):
        sec = val / 1000.0 if val > 1e12 else float(val)
        return datetime.fromtimestamp(sec, tz=timezone.utc).isoformat()
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = dateutil_parser.parse(s)
            except Exception:
                return s
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.isoformat()
    return None


def infer_workplace(
    raw_workplace: str | None,
    location: str,
    title: str,
    description_text: str = "",
) -> tuple[str, bool]:
    w = clean_str(raw_workplace).lower()
    loc = clean_str(location).lower()
    t = clean_str(title).lower()

    if "remote" in w or "remote" in loc or re.search(r"\bremote\b", t):
        return ("remote", True)
    if "hybrid" in w or "hybrid" in loc or re.search(r"\bhybrid\b", t):
        return ("hybrid", False)
    if any(k in w for k in ("onsite", "on-site", "on_site", "in-office")) or any(k in loc for k in ("on-site", "onsite", "in person", "in-office")):
        return ("onsite", False)

    if description_text:
        head = description_text[:6000].lower()
        if re.search(r"\b(hybrid[- ]remote|hybrid work|hybrid model|hybrid schedule|hybrid role|hybrid team|hybrid environment|hybrid workplace|hybrid basis|\d\s*[-–to]*\s*\d?\s*days?\s*(?:a|per)\s*week\s*in[- ]office|this (?:role|position) is hybrid)\b", head):
            return ("hybrid", False)
        if re.search(r"\b(fully remote|100% remote|remote[- ]first|work from anywhere|this (?:role|position) is remote|remote position)\b", head):
            return ("remote", True)
        if re.search(r"\b(100% on[- ]?site|in[- ]person role|in[- ]office role|on[- ]site in|onsite in|this is an? on[- ]?site|based in our .{1,30} office)\b", head):
            return ("onsite", False)

    return ("unknown", False)


def normalize_employment_type(val: Any, title: str = "", description_text: str = "") -> str | None:
    s = clean_str(val).lower().replace("-", "_").replace(" ", "_")
    if s:
        if any(k in s for k in ("intern", "internship", "co_op", "coop", "extern")):
            return "intern"
        if any(k in s for k in ("part_time", "parttime")) or s == "part":
            return "part_time"
        if any(k in s for k in ("contract", "contractor", "freelance", "temporary", "temp", "locum", "fixed_term", "per_diem")):
            return "contract"
        if any(k in s for k in ("full_time", "fulltime", "salaried_ft", "permanent", "regular", "salaried")) or s == "full":
            return "full_time"
        return s[:40]

    # Fallback from title + description head (crucial for Greenhouse!)
    t_low = clean_str(title).lower()
    if re.search(r"\b(intern|internship|co-op|coop|externship)\b", t_low):
        return "intern"
    if re.search(r"\b(part[- ]?time)\b", t_low):
        return "part_time"
    if re.search(r"\b(contract|contractor|1099|freelance|locum)\b", t_low):
        return "contract"

    if description_text:
        head = description_text[:1500].lower()
        if re.search(r"\b(full[- ]?time)\b", head):
            return "full_time"
        if re.search(r"\b(part[- ]?time)\b", head):
            return "part_time"
        if re.search(r"\b(internship|summer intern)\b", head):
            return "intern"
        if re.search(r"\b(independent contractor|contract position|contract role)\b", head):
            return "contract"

    return None


def _match_country_in_str(text: str, allow_city_lookup: bool = True) -> str | None:
    if not text:
        return None
    low = text.lower()
    low_ascii = _ascii_fold(low)

    # 1. Multi-word country/metro phrases first (longest first)
    for phrase, canonical in MULTI_WORD_COUNTRIES:
        if re.search(rf"(?<![a-z]){re.escape(phrase)}(?![a-z])", low):
            return canonical

    # 2. Split right-to-left across common ATS delimiters (plus " or " / " in the " / " in ")
    tokens = [t.strip() for t in re.split(r"[,;:/()|>\-]+|\s+(?:or|in the|in)\s+", low) if t.strip()]
    for tok in reversed(tokens):
        tok_a = _ascii_fold(tok)
        # Disambiguate Georgia (US state vs country): if US city/prefix appears, prefer United States
        if tok == "georgia" and any(w in low for w in ("us-", "usa", "united states", "atlanta", "dallas", "chicago", "nyc", "seattle", "austin", "boston", "san francisco")):
            return "United States"
        if tok in COUNTRY_ALIASES:
            return COUNTRY_ALIASES[tok]
        if tok_a in COUNTRY_ALIASES:
            return COUNTRY_ALIASES[tok_a]
        # 2-letter ISO code in a structured multi-part string like "Toronto, ON, CA"
        up = tok.upper()
        if len(up) == 2 and up in ISO2_TO_COUNTRY and up not in US_STATES and up not in ("IN", "AT", "BY", "TO", "OR", "ON"):
            return ISO2_TO_COUNTRY[up]
        # Trailing US 2-letter state code inside segment: "Lisle, IL" or "Houston TX"
        words = tok.split()
        if words and words[-1].upper() in US_STATES and len(words[-1]) == 2:
            return "United States"
        if words and words[0] in COUNTRY_ALIASES and words[0] not in ("in", "at", "us", "ca", "co", "ma", "id", "me", "or", "no"):
            return COUNTRY_ALIASES[words[0]]

    # 3. ISO-3166-2 subdivisions (e.g. "California", "Minas Gerais", "Ontario")
    for tok in reversed(tokens):
        tok_a = _ascii_fold(tok)
        if tok in SUBDIV_NAME_MAP:
            return SUBDIV_NAME_MAP[tok]
        if tok_a in SUBDIV_NAME_MAP:
            return SUBDIV_NAME_MAP[tok_a]

    # 4. World city lookup from geonamescache (exact token, first/last word for "Denver Headquarters" or "Western Sydney")
    if allow_city_lookup:
        for tok in reversed(tokens):
            tok_a = _ascii_fold(tok)
            if tok in CITY_NAME_MAP:
                return CITY_NAME_MAP[tok]
            if tok_a in CITY_NAME_MAP:
                return CITY_NAME_MAP[tok_a]
            words = tok_a.split()
            if len(words) >= 2:
                if words[-1] in BIG_CITY_NAME_MAP:
                    return BIG_CITY_NAME_MAP[words[-1]]
                if words[0] in BIG_CITY_NAME_MAP:
                    return BIG_CITY_NAME_MAP[words[0]]

    # 5. Exact trailing 2-letter US state code like "San Francisco, CA"
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if parts:
        last_w = parts[-1].split()[0] if parts[-1].split() else ""
        if last_w.upper() in US_STATES and len(last_w) == 2:
            return "United States"

    return None


def extract_country(location: str, raw_country: Any = None, title: str = "") -> str | None:
    rc = clean_str(raw_country)
    if rc and rc.lower() not in ("remote", "n/a", "na", "none", "anywhere"):
        up = rc.upper()
        if up in ISO2_TO_COUNTRY:
            return ISO2_TO_COUNTRY[up]
        low = rc.lower()
        if low in COUNTRY_ALIASES:
            return COUNTRY_ALIASES[low]
        low_a = _ascii_fold(low)
        if low_a in COUNTRY_ALIASES:
            return COUNTRY_ALIASES[low_a]
        # Try compound raw_country like "United States & Canada"
        hit = _match_country_in_str(rc, allow_city_lookup=False)
        return hit or rc

    loc = clean_str(location)
    if loc and loc.lower() not in ("n/a", "na", "none", "remote", "anywhere"):
        hit = _match_country_in_str(loc, allow_city_lookup=True)
        if hit:
            return hit

    if title:
        # Check explicit location markers inside title like "(US Remote)" or "- London"
        for seg in re.findall(r"\(([^)]+)\)|\s[-–—]\s([^-–—]+)$", title):
            cand = seg[0] or seg[1]
            hit = _match_country_in_str(cand, allow_city_lookup=True)
            if hit:
                return hit

    return None


def extract_experience_level(title: str, raw_exp: Any = None) -> str | None:
    rexp = clean_str(raw_exp).lower()
    if rexp and rexp not in ("not applicable", "not_applicable", "none", "n/a"):
        if "intern" in rexp:
            return "intern"
        if any(k in rexp for k in ("entry", "junior", "associate", "new grad")):
            return "junior"
        if "senior" in rexp or re.search(r"\bsr\b", rexp):
            return "senior"
        if "mid" in rexp:
            return "mid"
        if any(k in rexp for k in ("director", "executive", "vp")):
            return "executive"
        return rexp[:40]

    t = clean_str(title).lower()
    if not t:
        return None
    if re.search(r"\b(intern|internship|co-op|coop|apprentice)\b", t):
        return "intern"
    if re.search(r"\b(ceo|cto|cfo|coo|cpo|cmo|cro|ciso|chief\s+\w+\s+officer|vp|svp|evp|vice\s+president|head\s+of)\b", t):
        return "executive"
    if re.search(r"\bdirector\b", t):
        return "director"
    if re.search(r"\b(principal|distinguished|fellow)\b", t):
        return "principal"
    if re.search(r"\bstaff\b", t):
        return "staff"
    if re.search(r"\b(lead|tech\s+lead|team\s+lead)\b", t):
        return "lead"
    if re.search(r"\b(manager|mgr|supervisor)\b", t):
        return "manager"
    if re.search(r"\b(senior|sr\.?|level\s*(?:iii|iv|3|4))\b", t):
        return "senior"
    if re.search(r"\b(junior|jr\.?|entry[- ]?level|new\s+grad|graduate|associate)\b", t):
        return "junior"
    return None


def clean_salary_text(raw: Any) -> str | None:
    s = clean_str(raw)
    if not s or s in ("0", "0.0", "N/A", "n/a", "None"):
        return None
    # Reject generic non-salary paragraphs (like BambooHR culture blurbs)
    if len(s) > 120 and not re.search(r"[\$€£]|\b(?:USD|EUR|GBP|CAD|AUD)\b", s, re.I):
        return None
    if not re.search(r"\d", s):
        return None
    return s[:200]


def normalize_salary_period(raw_period: Any) -> str | None:
    s = clean_str(raw_period).lower()
    if not s:
        return None
    if any(k in s for k in ("year", "annual", "yr", "annum")):
        return "year"
    if "month" in s or s == "mo":
        return "month"
    if "week" in s or s == "wk":
        return "week"
    if "day" in s or "daily" in s or "diem" in s:
        return "day"
    if "hour" in s or s == "hr":
        return "hour"
    return None


def parse_salary_from_text(text: str) -> tuple[float | None, float | None, str | None, str | None]:
    """Return (min, max, currency, period) for both annual AND hourly/monthly/daily ranges."""
    if not text:
        return (None, None, None, None)

    def _to_num(s: str, default_k: bool = False) -> float:
        sc = s.replace(",", "").strip().lower()
        if sc.endswith("k"):
            return float(sc[:-1]) * 1000.0
        val = float(sc)
        return val * 1000.0 if (default_k and val < 1000) else val

    # Detect explicit period from text
    period: str | None = None
    pm = re.search(r"(?:/|\bper\b|\ba\b)\s*(hour|hr|hourly|day|daily|week|weekly|month|monthly|year|yr|annual|annum)\b", text, re.I)
    if pm:
        period = normalize_salary_period(pm.group(1))
    elif re.search(r"\b(hourly|per hour)\b", text, re.I):
        period = "hour"
    elif re.search(r"\b(annual|yearly|per year)\b", text, re.I):
        period = "year"

    # 1. Currency symbol range: "$130,000 - $180,000", "$150K - $220K", "$24 - $27 / hour"
    m = re.search(
        r"([\$€£])\s*(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?\s*[kK]?)\s*[-–—to]+\s*[\$€£]?\s*(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?\s*[kK]?)",
        text,
    )
    if not m:
        # 2. ISO currency code range: "GBP 24 - 27 / hourly", "GBP 150-200K", "USD 120,000 - 160,000"
        m2 = re.search(
            r"\b(USD|EUR|GBP|CAD|AUD|CHF|SGD)\b\s*[:\-]?\s*(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?\s*[kK]?)\s*[-–—to]+\s*(?:USD|EUR|GBP|CAD|AUD|CHF|SGD)?\s*(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?\s*[kK]?)",
            text,
            re.I,
        )
        if not m2:
            return (None, None, None, None)
        cur_raw, raw_min, raw_max = m2.groups()
        cur = cur_raw.upper()
        has_k = "k" in raw_max.lower()
        smin, smax = _to_num(raw_min, default_k=has_k), _to_num(raw_max, default_k=has_k)
    else:
        sym, raw_min, raw_max = m.groups()
        cur = {"$": "USD", "€": "EUR", "£": "GBP"}.get(sym)
        has_k = "k" in raw_max.lower()
        smin, smax = _to_num(raw_min, default_k=has_k), _to_num(raw_max, default_k=has_k)

    if smin <= 0 or smax <= 0 or smin > smax:
        return (None, None, None, None)

    if period is None:
        if smin >= 15000 and smax <= 2000000:
            period = "year"
        else:
            # Ambiguous small numbers without explicit period/hourly marker
            return (None, None, None, None)

    if period == "year" and (smin < 15000 or smax > 2000000):
        return (None, None, None, None)
    if period == "hour" and (smin < 5 or smax > 1000):
        return (None, None, None, None)

    return (smin, smax, cur, period)


def _dedupe_list(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        s = clean_str(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def build_job_record(
    *,
    external_id: str,
    title: str,
    department: str = "",
    team: str = "",
    location: str = "",
    all_locations: list[str] | None = None,
    raw_country: Any = None,
    raw_countries: list[str] | None = None,
    raw_workplace: str | None = None,
    is_remote_override: bool | None = None,
    raw_employment_type: Any = None,
    raw_experience: Any = None,
    education: Any = None,
    language: Any = None,
    requisition_id: Any = None,
    salary_min: float | None = None,
    salary_max: float | None = None,
    salary_currency: str | None = None,
    salary_period: Any = None,
    salary_text: str | None = None,
    salary_is_annual: bool | None = None,
    apply_url: str | None = None,
    job_url: str | None = None,
    description_text: str = "",
    published_at: str | None = None,
) -> dict[str, Any]:
    wp, is_rem = infer_workplace(raw_workplace, location, title, description_text)
    if is_remote_override is True:
        wp, is_rem = "remote", True

    locs_clean = _dedupe_list(([location] if location else []) + (all_locations or []))
    primary_loc = locs_clean[0] if locs_clean else clean_str(location)
    primary_country = extract_country(primary_loc, raw_country, title)
    countries_clean = _dedupe_list(
        ([primary_country] if primary_country else [])
        + [c for rc in (raw_countries or []) if (c := extract_country("", rc)) is not None]
        + [c for l_item in locs_clean if (c := extract_country(l_item)) is not None]
    )

    sal_txt = clean_salary_text(salary_text)
    smin, smax, scur = salary_min, salary_max, clean_str(salary_currency).upper() or None
    speriod = normalize_salary_period(salary_period)
    if smin is not None and smin <= 0 and (smax is None or smax <= 0):
        smin = smax = None
    if smin is None and not sal_txt:
        speriod = None
    elif smin is not None and not speriod:
        if salary_is_annual is True or smin >= 15000:
            speriod = "year"
        elif smin < 500:
            speriod = "hour"

    if smin is None and (sal_txt or description_text):
        p_min, p_max, p_cur, p_per = parse_salary_from_text(sal_txt or description_text[:6000])
        if p_min is not None:
            smin, smax = p_min, p_max
            scur = scur or p_cur
            speriod = speriod or p_per
            if not sal_txt and scur:
                suf = f" / {speriod}" if speriod and speriod != "year" else ""
                sal_txt = f"{scur} {int(smin):,} - {int(smax):,}{suf}"

    lang_clean = clean_str(language).lower()[:16] or None
    edu_clean = clean_str(education)[:80] or None
    req_clean = clean_str(requisition_id)[:80] or None

    return {
        "external_id": external_id,
        "title": title,
        "department": clean_str(department) or None,
        "team": clean_str(team) or None,
        "location": primary_loc or None,
        "locations": locs_clean,
        "country": primary_country,
        "countries": countries_clean,
        "workplace_type": wp,
        "is_remote": is_rem,
        "employment_type": normalize_employment_type(raw_employment_type, title, description_text),
        "experience_level": extract_experience_level(title, raw_experience),
        "education": edu_clean,
        "language": lang_clean,
        "requisition_id": req_clean,
        "salary_min": smin,
        "salary_max": smax,
        "salary_currency": scur,
        "salary_period": speriod,
        "salary_text": sal_txt,
        "apply_url": clean_str(apply_url) or clean_str(job_url) or None,
        "job_url": clean_str(job_url) or clean_str(apply_url) or None,
        "description_text": clean_str(description_text) or None,
        "published_at": published_at,
        "content_hash": make_content_hash(title, primary_loc, department, description_text),
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
        if not isinstance(addr, dict):
            addr = {}
        if not loc:
            loc = ", ".join(filter(None, [clean_str(addr.get("addressLocality")), clean_str(addr.get("addressRegion")), clean_str(addr.get("addressCountry"))]))
        raw_country = addr.get("addressCountry") or extract_country(", ".join(filter(None, [clean_str(addr.get("addressLocality")), clean_str(addr.get("addressRegion"))])))
        all_locs = [loc] if loc else []
        raw_countries: list[str] = [raw_country] if raw_country else []
        if isinstance(j.get("secondaryLocations"), list):
            for sl in j["secondaryLocations"]:
                if isinstance(sl, dict):
                    sl_name = clean_str(sl.get("location"))
                    if sl_name:
                        all_locs.append(sl_name)
                    s_addr = (sl.get("address") or {}).get("postalAddress") if isinstance(sl.get("address"), dict) else {}
                    if isinstance(s_addr, dict) and s_addr.get("addressCountry"):
                        raw_countries.append(clean_str(s_addr["addressCountry"]))
                        if not raw_country:
                            raw_country = s_addr["addressCountry"]
                    elif sl_name:
                        hit = extract_country(sl_name)
                        if hit:
                            raw_countries.append(hit)
                            if not raw_country:
                                raw_country = hit
        desc = strip_html(j.get("descriptionPlain") or j.get("descriptionHtml"))

        comp = j.get("compensation") if isinstance(j.get("compensation"), dict) else {}
        sal_text = clean_str(comp.get("compensationTierSummary") or comp.get("scrapeableCompensationSalarySummary"))
        smin = smax = None
        scur = None
        speriod = None
        for sc in comp.get("summaryComponents") or []:
            if isinstance(sc, dict) and (sc.get("minValue") is not None or sc.get("maxValue") is not None):
                ctype = clean_str(sc.get("compensationType")).lower()
                if ctype == "salary" or smin is None:
                    smin = sc.get("minValue")
                    smax = sc.get("maxValue")
                    scur = sc.get("currencyCode")
                    speriod = sc.get("interval")
                    if ctype == "salary":
                        break

        out.append(build_job_record(
            external_id=ext_id,
            title=title,
            department=dept,
            team=team,
            location=loc,
            all_locations=all_locs,
            raw_country=raw_country,
            raw_countries=raw_countries,
            raw_workplace=j.get("workplaceType"),
            is_remote_override=bool(j.get("isRemote")) if j.get("isRemote") is not None else None,
            raw_employment_type=j.get("employmentType"),
            salary_min=float(smin) if smin is not None else None,
            salary_max=float(smax) if smax is not None else None,
            salary_currency=scur,
            salary_period=speriod or "year",
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
        office_loc = ""
        office_country = None
        office_locs: list[str] = []
        for off in offices:
            if not isinstance(off, dict):
                continue
            cand_loc = clean_str(off.get("location")) or clean_str(off.get("name"))
            if cand_loc:
                office_locs.append(cand_loc)
            if cand_loc and not office_loc:
                office_loc = cand_loc
            if cand_loc and not office_country:
                office_country = extract_country(cand_loc)
        desc = strip_html(j.get("content"))
        url = clean_str(j.get("absolute_url"))

        # Read structured fields from Greenhouse `metadata` array when present
        meta_team = ""
        meta_emp = None
        meta_wp = None
        meta_exp = None
        meta_country = None
        meta_city = None
        meta_state = None
        meta_sal_band = None
        meta_sal_type = None
        for m in j.get("metadata") or []:
            if not isinstance(m, dict):
                continue
            mname = clean_str(m.get("name")).lower()
            mval = m.get("value")
            if mval is None or mval == "" or mval == []:
                continue
            if isinstance(mval, dict) and ("min_value" in mval or "max_value" in mval):
                if any(k in mname for k in ("salary band", "salary range", "pay range")) and not meta_sal_band:
                    meta_sal_band = mval
                continue
            mstr = ", ".join(clean_str(x) for x in mval if x) if isinstance(mval, list) else clean_str(mval)
            if not mstr or mstr.lower() == "none":
                continue
            if mname in ("employment type", "time type", "full-time/ part-time", "type of contract", "job type", "work pattern", "employee type", "worker sub-type") and not meta_emp:
                meta_emp = mstr
            elif mname in ("workplace type", "work style", "remote worker") and not meta_wp:
                meta_wp = "onsite" if mstr.lower() in ("in office", "in market", "frontline") else mstr
            elif mname == "position open to remote" and mstr.lower() == "true" and not meta_wp:
                meta_wp = "remote"
            elif mname in ("experience level", "grade") and not meta_exp:
                meta_exp = mstr
            elif mname in ("team", "job family", "job family group", "career site function", "business unit", "job post department", "careersite department (for job postings)", "external department") and not meta_team:
                meta_team = mstr
            elif mname in ("unique role location", "country", "espirita country") and not meta_country:
                meta_country = mstr
            elif mname in ("town/city",) and not meta_city:
                meta_city = mstr
            elif mname in ("us state", "county") and not meta_state:
                meta_state = mstr
            elif mname == "salary type" and not meta_sal_type:
                meta_sal_type = mstr

        if not dept and meta_team:
            dept = meta_team

        smin = smax = None
        scur = None
        speriod = None
        sal_txt = None
        if isinstance(meta_sal_band, dict):
            try:
                bmin = float(meta_sal_band.get("min_value") or 0)
                bmax = float(meta_sal_band.get("max_value") or 0)
            except (TypeError, ValueError):
                bmin = bmax = 0.0
            if bmin > 0 or bmax > 0:
                scur = clean_str(meta_sal_band.get("unit")).upper() or None
                stype = clean_str(meta_sal_type).lower()
                speriod = normalize_salary_period(stype) or ("year" if bmin >= 15000 else None)
                smin, smax = bmin, (bmax or bmin)
                suffix = f" / {speriod}" if speriod and speriod != "year" else ""
                lo_s = f"{bmin:,.2f}".rstrip("0").rstrip(".")
                hi_s = f"{bmax:,.2f}".rstrip("0").rstrip(".") if bmax else lo_s
                sal_txt = f"{scur or ''} {lo_s} - {hi_s}{suffix}".strip()

        meta_loc_str = ", ".join(filter(None, [meta_city, meta_state, meta_country]))
        final_loc = loc or office_loc or meta_loc_str

        out.append(build_job_record(
            external_id=ext_id,
            title=title,
            department=dept,
            team=meta_team,
            location=final_loc,
            all_locations=([final_loc] if final_loc else []) + office_locs,
            raw_country=office_country or extract_country(meta_country or "") or extract_country(meta_loc_str),
            raw_workplace=meta_wp,
            raw_employment_type=meta_emp,
            raw_experience=meta_exp,
            education=j.get("education"),
            language=j.get("language"),
            requisition_id=j.get("requisition_id"),
            salary_min=smin,
            salary_max=smax,
            salary_currency=scur,
            salary_period=speriod,
            salary_text=sal_txt,
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
        all_locs = [clean_str(x) for x in (cats.get("allLocations") or []) if clean_str(x)]
        if not loc and all_locs:
            loc = all_locs[0]
        raw_country = j.get("country")
        if not raw_country:
            for cand_loc in ([loc] + all_locs):
                hit = extract_country(cand_loc)
                if hit:
                    raw_country = hit
                    break

        opening = strip_html(j.get("openingPlain") or j.get("opening"))
        body_main = strip_html(j.get("descriptionBodyPlain") or j.get("descriptionBody") or j.get("descriptionPlain") or j.get("description"))
        desc_parts = []
        if opening:
            desc_parts.append(opening)
        if body_main and body_main != opening:
            desc_parts.append(body_main)
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
        if smin is not None and smax is not None and smin <= 0 and smax <= 0:
            smin = smax = None
        scur = clean_str(sr.get("currency")) or None
        interval = clean_str(sr.get("interval")).lower()
        sal_annual = None
        if interval:
            sal_annual = ("year" in interval or "annual" in interval)
        sal_txt = clean_str(j.get("salaryDescriptionPlain"))
        if not sal_txt and smin is not None and smax is not None:
            suffix = ""
            if "hour" in interval:
                suffix = " / hour"
            elif "month" in interval:
                suffix = " / month"
            elif "week" in interval:
                suffix = " / week"
            sal_txt = f"{scur or ''} {int(smin):,} - {int(smax):,}{suffix}".strip()

        out.append(build_job_record(
            external_id=ext_id,
            title=title,
            department=dept,
            team=team,
            location=loc,
            all_locations=([loc] if loc else []) + all_locs,
            raw_country=raw_country,
            raw_workplace=j.get("workplaceType"),
            raw_employment_type=cats.get("commitment"),
            salary_min=smin,
            salary_max=smax,
            salary_currency=scur,
            salary_period=interval or None,
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
    team = ""
    if isinstance(dept_obj, dict):
        tree = [clean_str(x) for x in (dept_obj.get("department_tree") or []) if clean_str(x)]
        dept = clean_str(dept_obj.get("label") or dept_obj.get("name") or dept_obj.get("base_department")) or (tree[0] if tree else "")
        if len(tree) > 1:
            team = tree[-1]
    elif dept_obj:
        dept = clean_str(dept_obj)

    loc = ""
    wl_strs: list[str] = []
    raw_country = None
    raw_wp = None
    wls = d.get("workLocations")
    if isinstance(wls, list) and wls:
        wl_strs = [clean_str(x.get("label") if isinstance(x, dict) else x) for x in wls]
        wl_strs = [x for x in wl_strs if x]
        if wl_strs:
            loc = wl_strs[0]
            for ws in wl_strs:
                hit = extract_country(ws)
                if hit:
                    raw_country = hit
                    break
            has_rem = any("remote" in x.lower() for x in wl_strs)
            has_non_rem = any("remote" not in x.lower() for x in wl_strs)
            if has_rem and has_non_rem:
                raw_wp = "hybrid"
            elif has_rem:
                raw_wp = "remote"
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
    freq = ""
    prd = d.get("payRangeDetails")
    if isinstance(prd, list) and prd:
        p0 = next((x for x in prd if isinstance(x, dict) and (x.get("rangeStart") or x.get("rangeEnd"))), prd[0] if isinstance(prd[0], dict) else {})
        smin = float(p0["rangeStart"]) if p0.get("rangeStart") is not None else None
        smax = float(p0["rangeEnd"]) if p0.get("rangeEnd") is not None else None
        scur = clean_str(p0.get("currency")) or None
        freq = clean_str(p0.get("frequency")).lower()
        if smin is not None and smax is not None and (smin > 0 or smax > 0):
            suf = " / hour" if "hour" in freq else (" / month" if "month" in freq else "")
            sal_txt = f"{scur or ''} {int(smin):,} - {int(smax):,}{suf}".strip()
        if p0.get("isRemote") is True and not raw_wp:
            raw_wp = "remote"

    url = clean_str(d.get("url") or j.get("url"))
    return build_job_record(
        external_id=ext_id,
        title=title,
        department=dept,
        team=team,
        location=loc,
        all_locations=wl_strs or ([loc] if loc else []),
        raw_country=raw_country,
        raw_workplace=raw_wp,
        raw_employment_type=emp,
        salary_min=smin,
        salary_max=smax,
        salary_currency=scur,
        salary_period=freq or None,
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
    all_locs: list[str] = [loc] if loc else []
    raw_countries: list[str] = [clean_str(raw_country)] if raw_country else []
    for l_item in (d.get("locations") or j.get("locations") or []):
        if isinstance(l_item, dict):
            l_str = ", ".join(filter(None, [clean_str(l_item.get("city")), clean_str(l_item.get("region")), clean_str(l_item.get("country"))]))
            if l_str:
                all_locs.append(l_str)
            rc = l_item.get("country") or l_item.get("countryCode")
            if rc:
                raw_countries.append(clean_str(rc))
                if not raw_country:
                    raw_country = rc

    desc_parts = [strip_html(d.get("description")), strip_html(d.get("requirements")), strip_html(d.get("benefits"))]
    desc = "\n\n".join(p for p in desc_parts if p)[:20000]

    smin = float(d["salary_from"]) if d.get("salary_from") is not None else None
    smax = float(d["salary_to"]) if d.get("salary_to") is not None else None
    scur = clean_str(d.get("salary_currency_iso_code")) or None
    sfreq = clean_str(d.get("salary_frequency")).lower()
    sal_txt = f"{scur or ''} {int(smin):,} - {int(smax):,}".strip() if (smin is not None and smax is not None) else None

    url = clean_str(j.get("url")) or f"https://apply.workable.com/{slug}/j/{ext_id}/"
    is_rem = d.get("remote") if d.get("remote") is not None else j.get("remote")
    return build_job_record(
        external_id=ext_id,
        title=title,
        department=dept,
        location=loc,
        all_locations=all_locs,
        raw_country=raw_country,
        raw_countries=raw_countries,
        raw_workplace=d.get("workplace") or j.get("workplace"),
        is_remote_override=bool(is_rem) if is_rem is not None else None,
        raw_employment_type=d.get("type") or j.get("type"),
        language=d.get("language") or j.get("language"),
        requisition_id=d.get("code") or j.get("code"),
        salary_min=smin,
        salary_max=smax,
        salary_currency=scur,
        salary_period=sfreq or None,
        salary_text=sal_txt,
        apply_url=url,
        job_url=url,
        description_text=desc,
        published_at=parse_iso_ts(d.get("published") or j.get("published")),
    )


def parse_breezy(payload: Any, html_pages: list[str | None] | None = None) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    out = []
    pages = html_pages if html_pages and len(html_pages) == len(payload) else [None] * len(payload)
    for j, page_html in zip(payload, pages):
        ext_id = clean_str(j.get("id"))
        if not ext_id:
            continue
        loc_obj = j.get("location") if isinstance(j.get("location"), dict) else {}
        loc = clean_str(loc_obj.get("name"))
        ctry_obj = loc_obj.get("country") if isinstance(loc_obj.get("country"), dict) else {}
        raw_country = ctry_obj.get("name") or ctry_obj.get("id")
        all_locs = [loc] if loc else []
        raw_countries = [clean_str(raw_country)] if raw_country else []
        for l_item in j.get("locations") or []:
            if isinstance(l_item, dict):
                lname = clean_str(l_item.get("name")) or ", ".join(filter(None, [clean_str(l_item.get("city")), clean_str((l_item.get("state") or {}).get("name") if isinstance(l_item.get("state"), dict) else ""), clean_str((l_item.get("country") or {}).get("name") if isinstance(l_item.get("country"), dict) else "")]))
                if lname:
                    all_locs.append(lname)
                co = l_item.get("country") if isinstance(l_item.get("country"), dict) else {}
                if co.get("name") or co.get("id"):
                    raw_countries.append(clean_str(co.get("name") or co.get("id")))
                    if not raw_country:
                        raw_country = co.get("name") or co.get("id")
        is_rem = bool(loc_obj.get("is_remote")) if loc_obj.get("is_remote") is not None else None
        t_obj = j.get("type") if isinstance(j.get("type"), dict) else {}
        url = clean_str(j.get("url"))

        desc = ""
        if page_html:
            for m in re.finditer(r'<script type="application/ld\+json">(.*?)</script>', page_html, re.S):
                try:
                    jd = json.loads(m.group(1))
                    if jd.get("@type") == "JobPosting" and jd.get("description"):
                        desc = strip_html(jd["description"])
                        break
                except Exception:
                    pass

        out.append(build_job_record(
            external_id=ext_id,
            title=clean_str(j.get("name")),
            department=clean_str(j.get("department")),
            location=loc,
            all_locations=all_locs,
            raw_country=raw_country,
            raw_countries=raw_countries,
            is_remote_override=is_rem,
            raw_employment_type=t_obj.get("name") or t_obj.get("id"),
            salary_text=clean_str(j.get("salary")),
            apply_url=url,
            job_url=url,
            description_text=desc,
            published_at=parse_iso_ts(j.get("published_date")),
        ))
    return out


def parse_bamboohr_item(slug: str, j: dict[str, Any], detail: dict[str, Any] | None = None) -> dict[str, Any] | None:
    ext_id = clean_str(j.get("id"))
    if not ext_id:
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
    ctry = clean_str(ats_loc.get("country") or loc_obj.get("addressCountry"))
    loc = ", ".join(filter(None, [city, prov, ctry]))

    loc_type = str(jo.get("locationType") or j.get("locationType") or "")
    is_rem = jo.get("isRemote") if jo.get("isRemote") is not None else j.get("isRemote")
    raw_wp = None
    if loc_type == "1" and is_rem is None:
        is_rem = True
        raw_wp = "remote"
    elif loc_type in ("0", "2") and not is_rem:
        raw_wp = "onsite"

    url = clean_str(jo.get("jobOpeningShareUrl")) or f"https://{slug}.bamboohr.com/careers/{ext_id}"
    return build_job_record(
        external_id=ext_id,
        title=title,
        department=dept,
        location=loc,
        raw_country=ctry,
        raw_workplace=raw_wp,
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
        geo_str = ", ".join(filter(None, [clean_str(loc_obj.get("city")), clean_str(loc_obj.get("province"))]))
        raw_country = extract_country(loc) or extract_country(geo_str)
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
        cfreq = clean_str(j.get("compensation_frequency")).lower()
        out.append(build_job_record(
            external_id=ext_id,
            title=clean_str(j.get("title")),
            department=dept,
            location=loc or geo_str,
            raw_country=raw_country,
            raw_workplace=j.get("workplace_type") or j.get("workplace_type_text"),
            raw_employment_type=j.get("employment_type") or j.get("employment_type_text"),
            requisition_id=job_obj.get("requisition_id"),
            salary_min=smin,
            salary_max=smax,
            salary_currency=clean_str(j.get("compensation_currency")) or None,
            salary_period=cfreq or None,
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
        sal_txt = ""
        smin = smax = None
        scur = None
        per = ""
        if isinstance(sal, dict):
            smin = float(sal["min"]) if sal.get("min") is not None else None
            smax = float(sal["max"]) if sal.get("max") is not None else None
            scur = clean_str(sal.get("currency")) or None
            per = clean_str(sal.get("period")).lower()
            if smin is not None and smax is not None and smin > 0:
                sal_txt = f"{scur or ''} {int(smin):,} - {int(smax):,}" + (f" / {per}" if per else "")
        elif isinstance(sal, str):
            sal_txt = clean_str(sal)
        loc = clean_str(j.get("location")) or ", ".join(filter(None, [clean_str(j.get("city")), clean_str(j.get("state_name")), clean_str(j.get("country"))]))
        raw_country = j.get("country") or j.get("country_code")
        all_locs = [loc] if loc else []
        raw_countries = [clean_str(raw_country)] if raw_country else []
        for l_item in j.get("locations") or []:
            if isinstance(l_item, dict):
                if l_item.get("name"):
                    all_locs.append(clean_str(l_item["name"]))
                if l_item.get("country"):
                    raw_countries.append(clean_str(l_item["country"]))
                    if not raw_country:
                        raw_country = l_item["country"]
        out.append(build_job_record(
            external_id=ext_id,
            title=clean_str(j.get("title")),
            department=clean_str(j.get("department")),
            team=clean_str(j.get("category_code")),
            location=loc,
            all_locations=all_locs,
            raw_country=raw_country,
            raw_countries=raw_countries,
            raw_workplace=wp,
            is_remote_override=bool(j.get("remote")) if j.get("remote") is not None else None,
            raw_employment_type=j.get("employment_type_code"),
            raw_experience=j.get("experience_code"),
            education=j.get("education_code"),
            salary_min=smin,
            salary_max=smax,
            salary_currency=scur,
            salary_period=per or None,
            salary_text=sal_txt,
            apply_url=j.get("careers_apply_url") or j.get("careers_url"),
            job_url=j.get("careers_url"),
            description_text=desc,
            published_at=parse_iso_ts(j.get("published_at") or j.get("created_at")),
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
        team = clean_str(fn_obj.get("label")) if dept_obj.get("label") and fn_obj.get("label") else ""

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
        lang_obj = (d.get("language") or j.get("language")) if isinstance(d.get("language") or j.get("language"), dict) else {}

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
            team=team,
            location=loc,
            raw_country=cf_country or loc_obj.get("country"),
            raw_workplace=wp,
            is_remote_override=bool(loc_obj.get("remote")) if loc_obj.get("remote") is not None else None,
            raw_employment_type=emp,
            raw_experience=exp,
            language=lang_obj.get("code"),
            requisition_id=d.get("refNumber") or j.get("refNumber"),
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


async def fetch_text(client: httpx.AsyncClient, sem: asyncio.Semaphore, url: str) -> str | None:
    async with sem:
        try:
            r = await client.get(url)
            if r.status_code == 200:
                return r.text
        except Exception:
            pass
    return None


async def fetch_board(
    client: httpx.AsyncClient,
    sems: dict[str, asyncio.Semaphore],
    detail_sem: asyncio.Semaphore,
    workable_detail_sem: asyncio.Semaphore,
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

                    # Gentle concurrency on Workable v2 details to avoid 429 rate_limit
                    detail_urls = [
                        f"https://apply.workable.com/api/v2/accounts/{slug}/jobs/{clean_str(j.get('shortcode') or j.get('id'))}"
                        for j in all_items
                    ]
                    details = await asyncio.gather(*[fetch_json(client, workable_detail_sem, u) for u in detail_urls])
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
                    items = payload if isinstance(payload, list) else []
                    pages = await asyncio.gather(*[fetch_text(client, detail_sem, clean_str(j.get("url"))) for j in items])
                    parsed = parse_breezy(payload, pages)
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
    workable_detail_sem = asyncio.Semaphore(3)
    ashby_lock = asyncio.Lock()
    limits = httpx.Limits(max_connections=140, max_keepalive_connections=70)
    async with httpx.AsyncClient(timeout=18.0, follow_redirects=True, limits=limits) as client:
        tasks = [fetch_board(client, sems, detail_sem, workable_detail_sem, ashby_lock, bk) for bk in unique_boards]
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
