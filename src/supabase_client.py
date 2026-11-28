"""Lightweight Supabase PostgREST + telemetry helper using httpx (IPv4-safe in GitHub Actions)."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any
import httpx


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SupabaseREST:
    def __init__(self, url: str | None = None, service_key: str | None = None, timeout: float = 45.0):
        self.base_url = (url or os.environ["SUPABASE_URL"]).rstrip("/")
        self.key = service_key or os.environ["SUPABASE_SERVICE_ROLE_KEY"]
        self.rest_url = f"{self.base_url}/rest/v1"
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
        }
        self.client = httpx.Client(headers=self.headers, timeout=timeout)

    def close(self) -> None:
        self.client.close()

    def select_all(
        self,
        table: str,
        select: str = "*",
        filters: dict[str, str] | None = None,
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            params: dict[str, str] = {"select": select, "limit": str(page_size), "offset": str(offset)}
            if filters:
                params.update(filters)
            r = self.client.get(f"{self.rest_url}/{table}", params=params)
            r.raise_for_status()
            batch = r.json()
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return rows

    def upsert_batch(
        self,
        table: str,
        rows: list[dict[str, Any]],
        on_conflict: str,
        batch_size: int = 400,
    ) -> int:
        if not rows:
            return 0
        # PostgREST PGRST102 requires identical keys in every object of a JSON batch.
        grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(tuple(sorted(row.keys())), []).append(row)

        total = 0
        headers = {**self.headers, "Prefer": "resolution=merge-duplicates,return=minimal"}
        for group_rows in grouped.values():
            for i in range(0, len(group_rows), batch_size):
                chunk = group_rows[i : i + batch_size]
                for attempt in range(3):
                    r = self.client.post(
                        f"{self.rest_url}/{table}",
                        params={"on_conflict": on_conflict},
                        headers=headers,
                        json=chunk,
                    )
                    if r.status_code < 400:
                        break
                    if r.status_code >= 500 and attempt < 2:
                        time.sleep(1.5 * (attempt + 1))
                        continue
                    raise RuntimeError(f"Upsert {table} failed ({r.status_code}): {r.text[:600]}")
                total += len(chunk)
        return total

    def insert_batch(self, table: str, rows: list[dict[str, Any]], batch_size: int = 400) -> int:
        if not rows:
            return 0
        grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(tuple(sorted(row.keys())), []).append(row)

        total = 0
        headers = {**self.headers, "Prefer": "return=minimal"}
        for group_rows in grouped.values():
            for i in range(0, len(group_rows), batch_size):
                chunk = group_rows[i : i + batch_size]
                r = self.client.post(f"{self.rest_url}/{table}", headers=headers, json=chunk)
                if r.status_code >= 400:
                    raise RuntimeError(f"Insert {table} failed ({r.status_code}): {r.text[:600]}")
                total += len(chunk)
        return total

    def patch(self, table: str, filters: dict[str, str], payload: dict[str, Any]) -> None:
        headers = {**self.headers, "Prefer": "return=minimal"}
        r = self.client.patch(f"{self.rest_url}/{table}", params=filters, headers=headers, json=payload)
        if r.status_code >= 400:
            raise RuntimeError(f"Patch {table} failed ({r.status_code}): {r.text[:600]}")

    def start_run(self, workflow_name: str) -> str | None:
        run_id = os.environ.get("GITHUB_RUN_ID")
        repo = os.environ.get("GITHUB_REPOSITORY")
        server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
        event = os.environ.get("GITHUB_EVENT_NAME", "manual")
        run_url = f"{server}/{repo}/actions/runs/{run_id}" if (repo and run_id) else None
        payload = {
            "workflow_name": workflow_name,
            "github_run_id": run_id,
            "github_run_url": run_url,
            "trigger_type": event,
            "status": "running",
            "started_at": utc_now_iso(),
        }
        headers = {**self.headers, "Prefer": "return=representation"}
        try:
            r = self.client.post(f"{self.rest_url}/github_action_runs", headers=headers, json=payload)
            if r.status_code < 400:
                data = r.json()
                return data[0]["id"] if data else None
        except Exception as exc:
            print(f"[warn] start_run skipped: {exc}")
        return None

    def finish_run(self, db_run_id: str | None, started_ts: float, status: str, **metrics: Any) -> None:
        if not db_run_id:
            return
        payload = {
            "status": status,
            "finished_at": utc_now_iso(),
            "duration_seconds": round(time.time() - started_ts, 2),
            **metrics,
        }
        try:
            self.patch("github_action_runs", {"id": f"eq.{db_run_id}"}, payload)
        except Exception as exc:
            print(f"[warn] finish_run failed: {exc}")
