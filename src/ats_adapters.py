"""Full production ATS adapters across all 12 startup ATS providers."""

from __future__ import annotations

import asyncio
import html as hl
import re
import xml.etree.ElementTree as ET
import httpx


async def fetch_jobs_for_board(client: httpx.AsyncClient, prov: str, slug: str, dom: str) -> tuple[list[dict], str | None]:
    """
    Returns (normalized_jobs_list, error_string_or_None).
    Includes Ashby workplaceType+compensationTiers+isListed, Greenhouse html.unescape+pay_input_ranges,
    Lever US + api.eu.lever.co + lists[] + additionalPlain + salaryRange, Personio XML, Teamtailor RSS, etc.
    """
    jobs: list[dict] = []
    last_err: str | None = None
    for attempt in range(2):
        try:
            if prov == "greenhouse":
                r = await client.get(
                    f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
                    timeout=httpx.Timeout(30.0, connect=8.0, pool=30.0),
                )
                if r.status_code in (429, 502, 503):
                    last_err = f"HTTP {r.status_code}"
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                if r.status_code in (404, 410):
                    return [], f"HTTP {r.status_code}"
                r.raise_for_status()
                for j in r.json().get("jobs", []):
                    txt = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", hl.unescape(j.get("content") or ""))).strip()
                    depts = [d.get("name") for d in (j.get("departments") or []) if d.get("name")]
                    jobs.append({
                        "ext_id": str(j.get("id")), "provider": prov, "slug": slug, "domain": dom,
                        "title": j.get("title") or "", "department": depts[0] if depts else "", "team": "",
                        "location": (j.get("location") or {}).get("name") or "",
                        "apply_url": j.get("absolute_url") or f"https://boards.greenhouse.io/{slug}/jobs/{j.get('id')}",
                        "workplace_type": "unknown", "description_text": txt, "raw_comp": j.get("pay_input_ranges"),
                    })
                return jobs, None

            if prov == "ashby":
                r = await client.get(
                    f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true",
                    timeout=httpx.Timeout(25.0, connect=8.0, pool=30.0),
                )
                if r.status_code in (404, 410):
                    return [], f"HTTP {r.status_code}"
                r.raise_for_status()
                for j in r.json().get("jobs", []):
                    if not j.get("isListed", True):
                        continue
                    wp_raw = (j.get("workplaceType") or "").lower()
                    wp = wp_raw if wp_raw in ("remote", "hybrid", "onsite") else ("remote" if j.get("isRemote") else "unknown")
                    jobs.append({
                        "ext_id": str(j.get("id") or j.get("jobUrl")), "provider": prov, "slug": slug, "domain": dom,
                        "title": j.get("title") or "", "department": j.get("department") or "", "team": j.get("team") or "",
                        "location": j.get("location") or "",
                        "apply_url": j.get("applyUrl") or j.get("jobUrl") or f"https://jobs.ashbyhq.com/{slug}",
                        "workplace_type": wp, "description_text": (j.get("descriptionPlain") or "").strip(), "raw_comp": j.get("compensation"),
                    })
                return jobs, None

            if prov == "lever":
                r = await client.get(f"https://api.lever.co/v0/postings/{slug}?mode=json", timeout=25)
                if r.status_code == 404:
                    r = await client.get(f"https://api.eu.lever.co/v0/postings/{slug}?mode=json", timeout=25)
                if r.status_code in (404, 410):
                    return [], f"HTTP {r.status_code}"
                r.raise_for_status()
                if isinstance(r.json(), list):
                    for j in r.json():
                        cats = j.get("categories") or {}
                        wp = (j.get("workplaceType") or "unknown").lower()
                        wp = "onsite" if wp in ("on-site", "onsite") else (wp if wp in ("remote", "hybrid") else "unknown")
                        list_txt = " ".join(
                            f"{li.get('text', '')} {re.sub(r'<[^>]+>', ' ', li.get('content', ''))}"
                            for li in (j.get("lists") or [])
                        )
                        full_txt = re.sub(r"\s+", " ", f"{j.get('descriptionPlain') or ''} {list_txt} {j.get('additionalPlain') or ''}").strip()
                        jobs.append({
                            "ext_id": str(j.get("id")), "provider": prov, "slug": slug, "domain": dom,
                            "title": j.get("text") or "", "department": cats.get("department") or "", "team": cats.get("team") or "",
                            "location": cats.get("location") or "",
                            "apply_url": j.get("applyUrl") or j.get("hostedUrl") or f"https://jobs.lever.co/{slug}",
                            "workplace_type": wp, "description_text": full_txt, "raw_comp": j.get("salaryRange"),
                        })
                return jobs, None

            if prov == "workable":
                r = await client.get(f"https://apply.workable.com/api/v3/accounts/{slug}/jobs", timeout=15)
                if r.status_code in (404, 410):
                    return [], f"HTTP {r.status_code}"
                r.raise_for_status()
                for j in r.json().get("results", []):
                    wp = "remote" if j.get("remote") else ((j.get("workplace") or "unknown").lower())
                    if wp not in ("remote", "hybrid", "onsite"):
                        wp = "unknown"
                    loc = ", ".join(filter(None, [j.get("city"), j.get("state"), j.get("country")]))
                    dept = (j.get("department") or [""])[0] if isinstance(j.get("department"), list) else (j.get("department") or "")
                    jobs.append({
                        "ext_id": str(j.get("shortcode") or j.get("id")), "provider": prov, "slug": slug, "domain": dom,
                        "title": j.get("title") or "", "department": dept, "team": "",
                        "location": loc, "apply_url": j.get("url") or f"https://apply.workable.com/{slug}/j/{j.get('shortcode')}/",
                        "workplace_type": wp, "description_text": "", "raw_comp": None,
                    })
                return jobs, None

            if prov == "rippling":
                r = await client.get(f"https://api.rippling.com/platform/api/ats/v1/board/{slug}/jobs", timeout=15)
                if r.status_code in (404, 410):
                    return [], f"HTTP {r.status_code}"
                r.raise_for_status()
                if isinstance(r.json(), list):
                    for j in r.json():
                        loc = (j.get("workLocation") or {}).get("label") or ""
                        wp = "remote" if "remote" in loc.lower() else "unknown"
                        jobs.append({
                            "ext_id": str(j.get("uuid") or j.get("id")), "provider": prov, "slug": slug, "domain": dom,
                            "title": j.get("name") or "", "department": (j.get("department") or {}).get("label") or "", "team": "",
                            "location": loc, "apply_url": j.get("url") or f"https://ats.rippling.com/{slug}/jobs/{j.get('uuid')}",
                            "workplace_type": wp, "description_text": "", "raw_comp": None,
                        })
                return jobs, None

            if prov == "bamboohr":
                r = await client.get(f"https://{slug}.bamboohr.com/careers/list", headers={"Accept": "application/json"}, timeout=15)
                if r.status_code in (404, 410):
                    return [], f"HTTP {r.status_code}"
                r.raise_for_status()
                for j in r.json().get("result", []):
                    lo = j.get("location") or {}
                    loc = ", ".join(filter(None, [lo.get("city"), lo.get("state"), lo.get("country")]))
                    wp = "remote" if str(j.get("isRemote")).lower() == "true" else "unknown"
                    jobs.append({
                        "ext_id": str(j.get("id")), "provider": prov, "slug": slug, "domain": dom,
                        "title": j.get("jobOpeningName") or "", "department": j.get("departmentLabel") or "", "team": "",
                        "location": loc, "apply_url": f"https://{slug}.bamboohr.com/careers/{j.get('id')}",
                        "workplace_type": wp, "description_text": "", "raw_comp": None,
                    })
                return jobs, None

            if prov == "smartrecruiters":
                r = await client.get(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings", timeout=15)
                if r.status_code in (404, 410):
                    return [], f"HTTP {r.status_code}"
                r.raise_for_status()
                for j in r.json().get("content", []):
                    lo = j.get("location") or {}
                    loc = ", ".join(filter(None, [lo.get("city"), lo.get("country")]))
                    wp = "remote" if lo.get("remote") else "unknown"
                    jobs.append({
                        "ext_id": str(j.get("id")), "provider": prov, "slug": slug, "domain": dom,
                        "title": j.get("name") or "", "department": (j.get("department") or {}).get("label") or "", "team": "",
                        "location": loc, "apply_url": f"https://jobs.smartrecruiters.com/{slug}/{j.get('id')}",
                        "workplace_type": wp, "description_text": "", "raw_comp": None,
                    })
                return jobs, None

            if prov == "teamtailor":
                r = await client.get(f"https://{slug}.teamtailor.com/jobs.rss", timeout=15)
                if r.status_code in (404, 410):
                    return [], f"HTTP {r.status_code}"
                r.raise_for_status()
                root = ET.fromstring(r.text)
                for it in root.findall(".//item"):
                    txt = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", hl.unescape(it.findtext("description") or ""))).strip()
                    jobs.append({
                        "ext_id": it.findtext("guid") or it.findtext("link") or "", "provider": prov, "slug": slug, "domain": dom,
                        "title": it.findtext("title") or "", "department": "", "team": "", "location": "",
                        "apply_url": it.findtext("link") or "", "workplace_type": "unknown", "description_text": txt, "raw_comp": None,
                    })
                return jobs, None

            if prov == "personio":
                r = await client.get(f"https://{slug}.jobs.personio.de/xml", timeout=15)
                if r.status_code in (404, 410):
                    return [], f"HTTP {r.status_code}"
                r.raise_for_status()
                root = ET.fromstring(r.text)
                for pos in root.findall(".//position"):
                    txt = " ".join(re.sub(r"<[^>]+>", " ", hl.unescape(d.findtext("value") or "")) for d in pos.findall(".//jobDescription"))
                    jobs.append({
                        "ext_id": pos.findtext("id") or "", "provider": prov, "slug": slug, "domain": dom,
                        "title": pos.findtext("name") or "", "department": pos.findtext("department") or "", "team": "",
                        "location": pos.findtext("office") or "", "apply_url": f"https://{slug}.jobs.personio.de/job/{pos.findtext('id')}",
                        "workplace_type": "unknown", "description_text": re.sub(r"\s+", " ", txt).strip(), "raw_comp": None,
                    })
                return jobs, None

            if prov == "breezy":
                r = await client.get(f"https://{slug}.breezy.hr/json", timeout=15)
                if r.status_code in (404, 410):
                    return [], f"HTTP {r.status_code}"
                r.raise_for_status()
                if isinstance(r.json(), list):
                    for j in r.json():
                        lo = j.get("location") or {}
                        wp = "remote" if lo.get("is_remote") else "unknown"
                        jobs.append({
                            "ext_id": str(j.get("id")), "provider": prov, "slug": slug, "domain": dom,
                            "title": j.get("name") or "", "department": j.get("department") or "", "team": "",
                            "location": lo.get("name") or "", "apply_url": j.get("url") or "",
                            "workplace_type": wp, "description_text": "", "raw_comp": None,
                        })
                return jobs, None

            if prov == "pinpoint":
                r = await client.get(f"https://{slug}.pinpointhq.com/postings.json", timeout=15)
                if r.status_code in (404, 410):
                    return [], f"HTTP {r.status_code}"
                r.raise_for_status()
                for j in r.json().get("data", []):
                    wp = (j.get("workplace_type") or "unknown").lower()
                    if wp not in ("remote", "hybrid", "onsite"):
                        wp = "unknown"
                    txt = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", hl.unescape(j.get("description") or ""))).strip()
                    jobs.append({
                        "ext_id": str(j.get("id")), "provider": prov, "slug": slug, "domain": dom,
                        "title": j.get("title") or "", "department": (j.get("department") or {}).get("name") or "", "team": "",
                        "location": (j.get("location") or {}).get("name") or "", "apply_url": j.get("url") or "",
                        "workplace_type": wp, "description_text": txt, "raw_comp": None,
                    })
                return jobs, None

            if prov == "recruitee":
                r = await client.get(f"https://{slug}.recruitee.com/api/offers/", timeout=15)
                if r.status_code in (404, 410):
                    return [], f"HTTP {r.status_code}"
                r.raise_for_status()
                for j in r.json().get("offers", []):
                    wp = "remote" if j.get("remote") else ("hybrid" if j.get("hybrid") else ("onsite" if j.get("on_site") else "unknown"))
                    txt = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", hl.unescape(j.get("description") or ""))).strip()
                    jobs.append({
                        "ext_id": str(j.get("id")), "provider": prov, "slug": slug, "domain": dom,
                        "title": j.get("title") or "", "department": j.get("department") or "", "team": "",
                        "location": j.get("location") or "", "apply_url": j.get("careers_url") or "",
                        "workplace_type": wp, "description_text": txt, "raw_comp": None,
                    })
                return jobs, None

            return [], f"unsupported_provider:{prov}"
        except Exception as exc:
            last_err = f"{type(exc).__name__}: {exc}"[:200]
            await asyncio.sleep(1.0 * (attempt + 1))
    return jobs, last_err
