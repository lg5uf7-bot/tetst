"""Step 3 / Daily Job Sync: Poll ATS Boards, Compute content_hash Delta, Log job_events & Aggregate Company Data Gold."""

from __future__ import annotations

import asyncio
import os
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ats_adapters import fetch_jobs_for_board
from src.classifiers import HybridClassifier
from src.supabase_client import SupabaseREST, utc_now_iso


def _iso_after(days: int = 0, hours: int = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days, hours=hours)).isoformat()


async def main_async() -> None:
    started = time.time()
    sb = SupabaseREST()
    run_id = sb.start_run("02_daily_sync_jobs")
    try:
        sync_mode = os.environ.get("SYNC_MODE", "due")  # 'due' | 'all'
        now_iso = utc_now_iso()

        print("[1/5] Loading verified ATS boards, companies & existing jobs from Supabase...")
        all_boards = sb.select_all("ats_boards", select="domain,ats_provider,ats_slug,poll_tier,consecutive_errors,next_poll_at")
        if sync_mode == "all":
            due_boards = [b for b in all_boards if b.get("poll_tier") != "dead"]
        else:
            due_boards = [
                b for b in all_boards
                if b.get("poll_tier") != "dead" and (not b.get("next_poll_at") or b["next_poll_at"] <= now_iso)
            ]

        comps = sb.select_all("companies", select="domain,name,website")
        comp_names = {c["domain"]: c["name"] for c in comps}
        comp_websites = {c["domain"]: c.get("website") or f"https://{c['domain']}" for c in comps}

        existing_jobs = sb.select_all("jobs", select="id,company_domain,status,content_hash,salary_min,salary_max")
        jobs_by_id = {j["id"]: j for j in existing_jobs}
        open_jobs_by_dom: dict[str, list[dict]] = defaultdict(list)
        for j in existing_jobs:
            if j.get("status") == "open":
                open_jobs_by_dom[j["company_domain"]].append(j)

        print(f"  Polling {len(due_boards)}/{len(all_boards)} boards (existing jobs={len(existing_jobs)})...")

        sem = asyncio.Semaphore(20)
        limits = httpx.Limits(max_connections=60, max_keepalive_connections=30)
        polled: list[tuple[dict, list[dict], str | None]] = []

        async with httpx.AsyncClient(headers={"User-Agent": "StartupIntelBot/2.0"}, follow_redirects=True, limits=limits) as ac:
            async def poll_one(b: dict):
                async with sem:
                    jobs, err = await fetch_jobs_for_board(ac, b["ats_provider"], b["ats_slug"], b["domain"])
                    polled.append((b, jobs, err))

            await asyncio.gather(*(poll_one(b) for b in due_boards))

        print("[2/5] Fitting HybridClassifier on live jobs & processing content_hash delta...")
        flat_raw = [j for _, jobs, err in polled if not err for j in jobs]
        clf = HybridClassifier()
        if flat_raw:
            clf.fit_jobs(flat_raw)

        upsert_jobs: list[dict] = []
        touch_ids: list[str] = []
        close_ids: list[str] = []
        events: list[dict] = []
        board_updates: list[dict] = []

        opened_cnt = reopened_cnt = updated_cnt = closed_cnt = unchanged_cnt = 0

        for b, jobs, err in polled:
            dom = b["domain"]
            cname = comp_names.get(dom, dom)
            if err:
                errs = int(b.get("consecutive_errors") or 0) + 1
                tier = "dead" if errs >= 5 or "404" in err or "410" in err else b.get("poll_tier", "weekly")
                board_updates.append({
                    "domain": dom, "ats_provider": b["ats_provider"], "ats_slug": b["ats_slug"],
                    "consecutive_errors": errs, "poll_tier": tier, "last_error": err[:240],
                    "last_polled_at": now_iso, "next_poll_at": _iso_after(days=7 if tier != "dead" else 30), "updated_at": now_iso,
                })
                continue

            seen_ids: set[str] = set()
            for raw in jobs:
                jid = f"{raw['provider']}:{raw['slug']}:{raw['ext_id']}"
                if jid in seen_ids:
                    continue
                seen_ids.add(jid)
                enr = clf.enrich_job(raw, company_name=cname)
                prev = jobs_by_id.get(jid)

                if prev is None:
                    opened_cnt += 1
                    events.append({"job_id": jid, "company_domain": dom, "event_type": "opened", "new_value": {"title": raw["title"]}, "occurred_at": now_iso})
                    upsert_jobs.append({
                        "id": jid, "company_domain": dom, "ats_provider": raw["provider"], "ats_slug": raw["slug"],
                        "external_id": raw["ext_id"], "title": raw["title"], "department": raw["department"], "team": raw["team"],
                        "location": raw["location"], "apply_url": raw["apply_url"], "status": "open",
                        "first_seen_at": now_iso, "last_seen_at": now_iso, "closed_at": None, "updated_at": now_iso, **enr,
                    })
                elif prev.get("status") == "closed":
                    reopened_cnt += 1
                    events.append({"job_id": jid, "company_domain": dom, "event_type": "reopened", "occurred_at": now_iso})
                    upsert_jobs.append({
                        "id": jid, "company_domain": dom, "ats_provider": raw["provider"], "ats_slug": raw["slug"],
                        "external_id": raw["ext_id"], "title": raw["title"], "department": raw["department"], "team": raw["team"],
                        "location": raw["location"], "apply_url": raw["apply_url"], "status": "open",
                        "last_seen_at": now_iso, "closed_at": None, "updated_at": now_iso, **enr,
                    })
                elif prev.get("content_hash") == enr["content_hash"]:
                    unchanged_cnt += 1
                    touch_ids.append(jid)
                else:
                    updated_cnt += 1
                    if prev.get("salary_min") != enr["salary_min"] or prev.get("salary_max") != enr["salary_max"]:
                        events.append({
                            "job_id": jid, "company_domain": dom, "event_type": "salary_changed",
                            "old_value": {"min": prev.get("salary_min"), "max": prev.get("salary_max")},
                            "new_value": {"min": enr["salary_min"], "max": enr["salary_max"]},
                            "occurred_at": now_iso,
                        })
                    else:
                        events.append({"job_id": jid, "company_domain": dom, "event_type": "updated", "occurred_at": now_iso})
                    upsert_jobs.append({
                        "id": jid, "company_domain": dom, "ats_provider": raw["provider"], "ats_slug": raw["slug"],
                        "external_id": raw["ext_id"], "title": raw["title"], "department": raw["department"], "team": raw["team"],
                        "location": raw["location"], "apply_url": raw["apply_url"], "status": "open",
                        "last_seen_at": now_iso, "closed_at": None, "updated_at": now_iso, **enr,
                    })

            # Close previously open jobs on this board that disappeared from ATS
            for old_j in open_jobs_by_dom.get(dom, []):
                if old_j["id"] not in seen_ids:
                    closed_cnt += 1
                    close_ids.append(old_j["id"])
                    events.append({"job_id": old_j["id"], "company_domain": dom, "event_type": "closed", "occurred_at": now_iso})

            n_open = len(seen_ids)
            next_tier = "daily" if n_open >= 3 else ("weekly" if n_open >= 1 else "monthly")
            next_at = _iso_after(hours=20) if next_tier == "daily" else (_iso_after(days=7) if next_tier == "weekly" else _iso_after(days=30))
            board_updates.append({
                "domain": dom, "ats_provider": b["ats_provider"], "ats_slug": b["ats_slug"],
                "open_jobs_count": n_open, "consecutive_errors": 0, "poll_tier": next_tier,
                "last_error": None, "last_polled_at": now_iso, "next_poll_at": next_at, "updated_at": now_iso,
            })

        print(f"[3/5] Applying deltas -> opened={opened_cnt}, reopened={reopened_cnt}, updated={updated_cnt}, closed={closed_cnt}, unchanged_skipped={unchanged_cnt}")
        sb.upsert_batch("jobs", upsert_jobs, on_conflict="id", batch_size=100)
        sb.upsert_batch("ats_boards", board_updates, on_conflict="domain")
        sb.insert_batch("job_events", events, batch_size=300)

        for i in range(0, len(touch_ids), 40):
            chunk = ",".join(touch_ids[i:i + 40])
            sb.patch("jobs", {"id": f"in.({chunk})"}, {"last_seen_at": now_iso})
        for i in range(0, len(close_ids), 40):
            chunk = ",".join(close_ids[i:i + 40])
            sb.patch("jobs", {"id": f"in.({chunk})"}, {"status": "closed", "closed_at": now_iso, "updated_at": now_iso})

        print("[4/5] Recomputing company Data Gold aggregates (open_jobs_count, tech_stack, median_salary_usd, velocity_7d)...")
        all_open = sb.select_all("jobs", select="company_domain,role_family,skills,salary_min,salary_max,salary_currency,salary_interval,workplace_type,offers_equity", filters={"status": "eq.open"})
        cutoff_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        recent_opens = sb.select_all("job_events", select="company_domain", filters={"event_type": "in.(opened,reopened)", "occurred_at": f"gte.{cutoff_7d}"})
        vel_7d = Counter(r["company_domain"] for r in recent_opens)

        by_dom_jobs: dict[str, list[dict]] = defaultdict(list)
        for j in all_open:
            by_dom_jobs[j["company_domain"]].append(j)

        comp_aggs = []
        for dom, cname in comp_names.items():
            jlist = by_dom_jobs.get(dom, [])
            if not jlist and dom not in vel_7d:
                continue
            depts = Counter(j["role_family"] for j in jlist)
            skills = Counter(sk for j in jlist for sk in (j.get("skills") or []))
            sals = [
                int(((j["salary_min"] or j["salary_max"]) + (j["salary_max"] or j["salary_min"])) / 2)
                for j in jlist
                if (j.get("salary_min") or j.get("salary_max"))
                and j.get("salary_currency") == "USD"
                and j.get("salary_interval") == "year"
            ]
            rem = sum(1 for j in jlist if j.get("workplace_type") == "remote")
            eq = sum(1 for j in jlist if j.get("offers_equity"))
            n = len(jlist)
            comp_aggs.append({
                "domain": dom,
                "name": cname,
                "website": comp_websites.get(dom, f"https://{dom}"),
                "is_hiring": n > 0,
                "open_jobs_count": n,
                "hiring_velocity_7d": vel_7d.get(dom, 0),
                "hiring_departments": dict(depts),
                "tech_stack_inferred": [k for k, _ in skills.most_common(10)],
                "median_salary_usd": int(statistics.median(sals)) if sals else None,
                "remote_ratio": round(rem / n, 3) if n else None,
                "offers_equity_ratio": round(eq / n, 3) if n else None,
                "updated_at": now_iso,
            })
        sb.upsert_batch("companies", comp_aggs, on_conflict="domain")

        sb.finish_run(
            run_id, started, "succeeded",
            companies_synced=len(comp_aggs),
            boards_polled=len(due_boards),
            jobs_opened=opened_cnt + reopened_cnt,
            jobs_closed=closed_cnt,
            jobs_updated=updated_cnt,
            notes=f"unchanged_skipped={unchanged_cnt}, total_open_jobs={len(all_open)}",
        )
        print(f"[5/5] Done in {time.time() - started:.1f}s -> total_open_jobs={len(all_open)}, boards_polled={len(due_boards)}")
    except Exception as exc:
        sb.finish_run(run_id, started, "failed", notes=str(exc)[:500])
        raise
    finally:
        sb.close()


if __name__ == "__main__":
    asyncio.run(main_async())
