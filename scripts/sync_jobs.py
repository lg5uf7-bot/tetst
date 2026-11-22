"""
Step 3: Poll all `ats_active = true` boards across 10 ATS providers,
normalize jobs, deduplicate per (company_id, ats_provider, external_id),
diff via `content_hash`, close roles missing after `missing_streak >= 2`,
and log summary telemetry to `github_action_runs`.
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
MISSING_STREAK_TO_CLOSE = int(os.environ.get("MISSING_STREAK_TO_CLOSE", "2"))


def strip_html(raw: str | None) -> str:
    if not raw:
        return ""
    text = html.unescape(raw)
    if "<" in text and ">" in text:
        soup = BeautifulSoup(text, "lxml")
        text = soup.get_text(separator="\n")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
    return "\n".join(ln for ln in lines if ln)[:20000]


def make_content_hash(title: str, location: str, department: str, description_text: str) -> str:
    payload = f"{title.strip()}|{location.strip()}|{department.strip()}|{description_text.strip()}"
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
    w = (raw_workplace or "").lower()
    loc = (location or "").lower()
    t = (title or "").lower()
    if "remote" in w or "remote" in loc or "(remote)" in t:
        return ("remote", True)
    if "hybrid" in w or "hybrid" in loc:
        return ("hybrid", False)
    if "onsite" in w or "on-site" in w or "on_site" in w:
        return ("onsite", False)
    return ("unknown", False)


def normalize_jobs(provider: str, slug: str, payload: Any) -> list[dict[str, Any]]:
    raw_jobs: list[dict[str, Any]] = []

    if provider == "ashby" and isinstance(payload, dict):
        for j in payload.get("jobs", []):
            ext_id = str(j.get("id") or "")
            if not ext_id:
                continue
            title = (j.get("title") or "").strip()
            dept = (j.get("department") or "").strip()
            team = (j.get("team") or "").strip()
            loc = (j.get("location") or "").strip()
            desc = strip_html(j.get("descriptionPlain") or j.get("descriptionHtml"))
            wp, is_rem = infer_workplace(j.get("workplaceType"), loc, title)
            if j.get("isRemote"):
                wp, is_rem = "remote", True
            comp = j.get("compensation") or {}
            sal_text = comp.get("compensationTierSummary") if isinstance(comp, dict) else None
            raw_jobs.append({
                "external_id": ext_id,
                "title": title,
                "department": dept or None,
                "team": team or None,
                "location": loc or None,
                "workplace_type": wp,
                "is_remote": is_rem,
                "employment_type": (j.get("employmentType") or "").lower() or None,
                "salary_text": sal_text,
                "apply_url": j.get("applyUrl") or j.get("jobUrl"),
                "job_url": j.get("jobUrl"),
                "description_text": desc or None,
                "published_at": parse_iso_ts(j.get("publishedAt")),
                "content_hash": make_content_hash(title, loc, dept, desc),
            })

    elif provider == "greenhouse" and isinstance(payload, dict):
        for j in payload.get("jobs", []):
            ext_id = str(j.get("id") or "")
            if not ext_id:
                continue
            title = (j.get("title") or "").strip()
            depts = j.get("departments") or []
            dept = (depts[0].get("name") if depts and isinstance(depts[0], dict) else "") or ""
            loc_obj = j.get("location") or {}
            loc = (loc_obj.get("name") if isinstance(loc_obj, dict) else "") or ""
            desc = strip_html(j.get("content"))
            wp, is_rem = infer_workplace(None, loc, title)
            url = j.get("absolute_url")
            raw_jobs.append({
                "external_id": ext_id,
                "title": title,
                "department": dept or None,
                "team": None,
                "location": loc or None,
                "workplace_type": wp,
                "is_remote": is_rem,
                "employment_type": None,
                "salary_text": None,
                "apply_url": url,
                "job_url": url,
                "description_text": desc or None,
                "published_at": parse_iso_ts(j.get("updated_at")),
                "content_hash": make_content_hash(title, loc, dept, desc),
            })

    elif provider == "lever" and isinstance(payload, list):
        for j in payload:
            ext_id = str(j.get("id") or "")
            if not ext_id:
                continue
            title = (j.get("text") or "").strip()
            cats = j.get("categories") or {}
            dept = (cats.get("department") or "").strip()
            team = (cats.get("team") or "").strip()
            loc = (cats.get("location") or "").strip()
            desc = strip_html(j.get("descriptionPlain") or j.get("description"))
            wp, is_rem = infer_workplace(j.get("workplaceType"), loc, title)
            raw_jobs.append({
                "external_id": ext_id,
                "title": title,
                "department": dept or None,
                "team": team or None,
                "location": loc or None,
                "workplace_type": wp,
                "is_remote": is_rem,
                "employment_type": (cats.get("commitment") or "").lower() or None,
                "salary_text": None,
                "apply_url": j.get("applyUrl") or j.get("hostedUrl"),
                "job_url": j.get("hostedUrl"),
                "description_text": desc or None,
                "published_at": parse_iso_ts(j.get("createdAt")),
                "content_hash": make_content_hash(title, loc, dept, desc),
            })

    elif provider == "rippling" and isinstance(payload, list):
        for j in payload:
            ext_id = str(j.get("uuid") or j.get("id") or "")
            if not ext_id:
                continue
            title = (j.get("name") or j.get("title") or "").strip()
            dept_obj = j.get("department") or {}
            dept = (dept_obj.get("name") if isinstance(dept_obj, dict) else str(dept_obj or "")).strip()
            loc_obj = j.get("workLocation") or {}
            loc = (loc_obj.get("label") if isinstance(loc_obj, dict) else str(loc_obj or "")).strip()
            url = j.get("url")
            wp, is_rem = infer_workplace(None, loc, title)
            raw_jobs.append({
                "external_id": ext_id,
                "title": title,
                "department": dept or None,
                "team": None,
                "location": loc or None,
                "workplace_type": wp,
                "is_remote": is_rem,
                "employment_type": None,
                "salary_text": None,
                "apply_url": url,
                "job_url": url,
                "description_text": None,
                "published_at": None,
                "content_hash": make_content_hash(title, loc, dept, ""),
            })

    elif provider == "workable" and isinstance(payload, dict):
        for j in payload.get("results", []):
            ext_id = str(j.get("shortcode") or j.get("id") or "")
            if not ext_id:
                continue
            title = (j.get("title") or "").strip()
            dept = ", ".join(j.get("department") or []) if isinstance(j.get("department"), list) else str(j.get("department") or "")
            loc_obj = j.get("location") or {}
            loc = ", ".join(filter(None, [loc_obj.get("city"), loc_obj.get("region"), loc_obj.get("country")])) if isinstance(loc_obj, dict) else ""
            wp, is_rem = infer_workplace(j.get("workplace"), loc, title)
            if j.get("remote"):
                wp, is_rem = "remote", True
            url = j.get("url") or f"https://apply.workable.com/{slug}/j/{ext_id}/"
            raw_jobs.append({
                "external_id": ext_id,
                "title": title,
                "department": dept or None,
                "team": None,
                "location": loc or None,
                "workplace_type": wp,
                "is_remote": is_rem,
                "employment_type": (j.get("type") or "").lower() or None,
                "salary_text": None,
                "apply_url": url,
                "job_url": url,
                "description_text": None,
                "published_at": parse_iso_ts(j.get("published")),
                "content_hash": make_content_hash(title, loc, dept, ""),
            })

    elif provider == "breezy" and isinstance(payload, list):
        for j in payload:
            ext_id = str(j.get("id") or "")
            if not ext_id:
                continue
            title = (j.get("name") or "").strip()
            dept = (j.get("department") or "").strip()
            loc_obj = j.get("location") or {}
            loc = (loc_obj.get("name") if isinstance(loc_obj, dict) else "") or ""
            is_rem = bool(loc_obj.get("is_remote")) if isinstance(loc_obj, dict) else False
            wp = "remote" if is_rem else infer_workplace(None, loc, title)[0]
            url = j.get("url")
            raw_jobs.append({
                "external_id": ext_id,
                "title": title,
                "department": dept or None,
                "team": None,
                "location": loc or None,
                "workplace_type": wp,
                "is_remote": is_rem,
                "employment_type": (j.get("type", {}) or {}).get("name") if isinstance(j.get("type"), dict) else None,
                "salary_text": None,
                "apply_url": url,
                "job_url": url,
                "description_text": None,
                "published_at": parse_iso_ts(j.get("published_date")),
                "content_hash": make_content_hash(title, loc, dept, ""),
            })

    elif provider == "bamboohr" and isinstance(payload, dict):
        for j in payload.get("result", []):
            ext_id = str(j.get("id") or "")
            if not ext_id:
                continue
            title = (j.get("jobOpeningName") or "").strip()
            dept = (j.get("departmentLabel") or "").strip()
            loc_obj = j.get("location") or {}
            loc = ", ".join(filter(None, [loc_obj.get("city"), loc_obj.get("state"), loc_obj.get("country")])) if isinstance(loc_obj, dict) else ""
            wp, is_rem = infer_workplace(None, loc, title)
            url = f"https://{slug}.bamboohr.com/careers/{ext_id}"
            raw_jobs.append({
                "external_id": ext_id,
                "title": title,
                "department": dept or None,
                "team": None,
                "location": loc or None,
                "workplace_type": wp,
                "is_remote": is_rem,
                "employment_type": (j.get("employmentStatusLabel") or "").lower() or None,
                "salary_text": None,
                "apply_url": url,
                "job_url": url,
                "description_text": None,
                "published_at": None,
                "content_hash": make_content_hash(title, loc, dept, ""),
            })

    elif provider == "pinpoint" and isinstance(payload, dict):
        for j in payload.get("data", []):
            ext_id = str(j.get("id") or "")
            if not ext_id:
                continue
            title = (j.get("title") or "").strip()
            dept = ((j.get("department") or {}).get("name") if isinstance(j.get("department"), dict) else "") or ""
            loc = ((j.get("location") or {}).get("name") if isinstance(j.get("location"), dict) else "") or ""
            desc = strip_html(j.get("description"))
            wp, is_rem = infer_workplace(j.get("workplace_type"), loc, title)
            url = j.get("url")
            raw_jobs.append({
                "external_id": ext_id,
                "title": title,
                "department": dept or None,
                "team": None,
                "location": loc or None,
                "workplace_type": wp,
                "is_remote": is_rem,
                "employment_type": (j.get("employment_type_text") or "").lower() or None,
                "salary_text": None,
                "apply_url": url,
                "job_url": url,
                "description_text": desc or None,
                "published_at": None,
                "content_hash": make_content_hash(title, loc, dept, desc),
            })

    elif provider == "recruitee" and isinstance(payload, dict):
        for j in payload.get("offers", []):
            ext_id = str(j.get("id") or "")
            if not ext_id:
                continue
            title = (j.get("title") or "").strip()
            dept = (j.get("department") or "").strip()
            loc = (j.get("location") or "").strip()
            desc = strip_html(j.get("description"))
            is_rem = bool(j.get("remote"))
            wp = "remote" if is_rem else infer_workplace(None, loc, title)[0]
            url = j.get("careers_url")
            raw_jobs.append({
                "external_id": ext_id,
                "title": title,
                "department": dept or None,
                "team": None,
                "location": loc or None,
                "workplace_type": wp,
                "is_remote": is_rem,
                "employment_type": j.get("employment_type_code") or None,
                "salary_text": None,
                "apply_url": url,
                "job_url": url,
                "description_text": desc or None,
                "published_at": parse_iso_ts(j.get("published_at")),
                "content_hash": make_content_hash(title, loc, dept, desc),
            })

    elif provider == "smartrecruiters" and isinstance(payload, dict):
        for j in payload.get("content", []):
            ext_id = str(j.get("id") or "")
            if not ext_id:
                continue
            title = (j.get("name") or "").strip()
            dept = ((j.get("department") or {}).get("label") if isinstance(j.get("department"), dict) else "") or ""
            loc_obj = j.get("location") or {}
            loc = ", ".join(filter(None, [loc_obj.get("city"), loc_obj.get("country")])) if isinstance(loc_obj, dict) else ""
            wp, is_rem = infer_workplace(None, loc, title)
            url = f"https://jobs.smartrecruiters.com/{slug}/{ext_id}"
            raw_jobs.append({
                "external_id": ext_id,
                "title": title,
                "department": dept or None,
                "team": None,
                "location": loc or None,
                "workplace_type": wp,
                "is_remote": is_rem,
                "employment_type": ((j.get("typeOfEmployment") or {}).get("label") if isinstance(j.get("typeOfEmployment"), dict) else None),
                "salary_text": None,
                "apply_url": url,
                "job_url": url,
                "description_text": None,
                "published_at": parse_iso_ts(j.get("releasedDate")),
                "content_hash": make_content_hash(title, loc, dept, ""),
            })

    # Deduplicate external_id inside the same board response so Postgres ON CONFLICT never hits a row twice
    deduped: dict[str, dict[str, Any]] = {}
    for j in raw_jobs:
        deduped[j["external_id"]] = j
    return list(deduped.values())


async def fetch_board(
    client: httpx.AsyncClient,
    sems: dict[str, asyncio.Semaphore],
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
                if provider == "workable":
                    r = await client.post(api_url, json={"query": "", "location": [], "department": [], "worktype": [], "remote": []})
                elif provider == "greenhouse":
                    sep = "&" if "?" in api_url else "?"
                    r = await client.get(f"{api_url}{sep}content=true")
                else:
                    r = await client.get(api_url)

                if r.status_code == 429:
                    wait = int(r.headers.get("Retry-After", "5"))
                    await asyncio.sleep(min(wait, 15))
                    continue
                if r.status_code != 200:
                    return (board_key, None, f"HTTP {r.status_code}")

                payload = r.json()
                return (board_key, normalize_jobs(provider, slug, payload), None)
            except Exception as exc:
                if attempt == 1:
                    return (board_key, None, str(exc)[:200])
                await asyncio.sleep(0.5)
    return (board_key, None, "rate_limited")


async def poll_all_boards(unique_boards: list[tuple[str, str, str]]) -> dict[tuple[str, str, str], tuple[list[dict[str, Any]] | None, str | None]]:
    sems = {p: asyncio.Semaphore(c) for p, c in MAX_CONCURRENT_BY_PROVIDER.items()}
    ashby_lock = asyncio.Lock()
    limits = httpx.Limits(max_connections=120, max_keepalive_connections=60)
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True, limits=limits) as client:
        tasks = [fetch_board(client, sems, ashby_lock, bk) for bk in unique_boards]
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
            select="id,company_id,ats_provider,external_id,content_hash,status,missing_streak",
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
