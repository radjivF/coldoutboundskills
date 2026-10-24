#!/usr/bin/env python3
"""Backfill Instantly AU campaign leads into Supabase au_jobs_dm_leads.

Requires:
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY
  INSTANTLY_API_KEY (to fetch leads)

Optional:
  AU_PIPELINE_BACKFILL_DATE=YYYY-MM-DD  (default: all campaign leads)
  INSTANTLY_CAMPAIGN_ID (default: Linkedin fresh job board AU)

Apply schema first: scripts/sql/au_jobs_dm_leads.sql
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# Reuse pipeline helpers when available
sys.path.insert(0, str(Path(__file__).resolve().parent))
from au_jobs_dm_pipeline import (  # noqa: E402
    INSTANTLY_API,
    INSTANTLY_CAMPAIGN_ID,
    PipelineError,
    _load_dotenv,
    _secret,
    push_supabase,
    stop_on_http_error,
)


def fetch_instantly_leads(api_key: str, campaign_id: str) -> list[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {api_key}"}
    items: list[dict[str, Any]] = []
    starting_after: str | None = None
    for _ in range(50):
        body: dict[str, Any] = {"campaign": campaign_id, "limit": 100}
        if starting_after:
            body["starting_after"] = starting_after
        try:
            resp = requests.post(
                f"{INSTANTLY_API}/api/v2/leads/list",
                headers=headers,
                json=body,
                timeout=60,
            )
        except requests.RequestException as exc:
            raise PipelineError(f"Instantly list network error: {exc}") from exc
        stop_on_http_error(resp, "Instantly")
        batch = (resp.json() or {}).get("items") or []
        if not batch:
            break
        items.extend(batch)
        starting_after = batch[-1].get("id")
        if len(batch) < 100:
            break
    return items


def instantly_to_lead_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    leads: list[dict[str, Any]] = []
    for it in items:
        payload = it.get("payload") if isinstance(it.get("payload"), dict) else {}
        email = (it.get("email") or payload.get("email") or "").strip().lower()
        if not email:
            continue
        created = (it.get("timestamp_created") or "")[:10] or None
        leads.append(
            {
                "email": email,
                "first_name": it.get("first_name") or payload.get("firstName") or "",
                "last_name": it.get("last_name") or payload.get("lastName") or "",
                "company_name": it.get("company_name")
                or payload.get("companyName")
                or "",
                "website": it.get("website")
                or it.get("company_domain")
                or payload.get("website")
                or "",
                "linkedin_url": payload.get("linkedin_url") or "",
                "jobTitle": payload.get("jobTitle") or it.get("job_title") or "",
                "position": payload.get("position") or it.get("job_title") or "",
                "instantly_lead_id": it.get("id") or "",
                "run_date": created,
                "raw": it,
            }
        )
    return leads


def main() -> int:
    _load_dotenv()
    date_filter = (os.environ.get("AU_PIPELINE_BACKFILL_DATE") or "").strip()
    campaign_id = (
        _secret("INSTANTLY_CAMPAIGN_ID") or INSTANTLY_CAMPAIGN_ID
    )
    instantly_key = _secret("INSTANTLY_API_KEY", "INSTANTLY_KEY")
    if not instantly_key:
        print("Missing INSTANTLY_API_KEY", file=sys.stderr)
        return 2

    print(f"[Backfill] Fetching Instantly leads for campaign {campaign_id}...")
    items = fetch_instantly_leads(instantly_key, campaign_id)
    if date_filter:
        items = [
            it
            for it in items
            if (it.get("timestamp_created") or "").startswith(date_filter)
        ]
        print(f"[Backfill] Filtered to date {date_filter}: {len(items)} leads")
    else:
        print(f"[Backfill] All campaign leads: {len(items)}")

    leads = instantly_to_lead_rows(items)
    out = Path("/tmp/au-jobs-dm-pipeline")
    out.mkdir(parents=True, exist_ok=True)
    (out / "backfill_leads.json").write_text(json.dumps(leads, indent=2, default=str))

    try:
        summary = push_supabase(leads)
    except PipelineError as exc:
        print(f"[Backfill] STOPPED: {exc}", file=sys.stderr)
        print(
            "Ensure SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY are set, "
            "and run scripts/sql/au_jobs_dm_leads.sql once.",
            file=sys.stderr,
        )
        return 2

    print(
        json.dumps(
            {
                "finished": datetime.now(timezone.utc).isoformat(),
                "leads": len(leads),
                "supabase": summary,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
