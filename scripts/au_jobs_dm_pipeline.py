#!/usr/bin/env python3
"""
Daily AU LinkedIn jobs → decision-maker → Instantly outreach pipeline.

Steps:
  1. Apify LinkedIn jobs scrape (curious_coder/linkedin-jobs-scraper)
  2. Filter: drop companyEmployeesCount >= 1000, drop recruiters,
     keep resume-fit titles (AI / fullstack React-TS-Node / tech lead / solutions architect)
  3. Prospeo decision-maker search + enrich
  4. MillionVerifier: keep result == ok only
  5. Join jobs with DMs (jobTitle = hiring-for, position = person title)
  6. Push leads to Instantly campaign via POST /api/v2/leads/add
     (does NOT activate the campaign)

On API errors or rate limits: stop and report (no blind retries).
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import requests

try:
    from apify_client import ApifyClient
except ImportError:  # pragma: no cover
    ApifyClient = None  # type: ignore

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ACTOR_ID = "curious_coder/linkedin-jobs-scraper"
INSTANTLY_CAMPAIGN_ID = "5f5f4dc8-e9ba-47c6-be09-faec97ef26d0"
INSTANTLY_API = "https://api.instantly.ai"
PROSPEO_API = "https://api.prospeo.io"
MV_API = "https://api.millionverifier.com/api/v3/"
AU_GEO_ID = "101452733"
MAX_JOBS = 50
MAX_COMPANY_EMPLOYEES = 999
OUT_DIR = Path(os.environ.get("AU_PIPELINE_OUT", "/tmp/au-jobs-dm-pipeline"))

SEARCH_KEYWORDS = [
    "software engineer",
    "fullstack",
    "react",
    "python",
    "AI",
]

# Job titles we want to hire-for (resume-fit)
RESUME_FIT_PATTERNS = [
    r"\bai\b",
    r"\bmachine learning\b",
    r"\bml engineer\b",
    r"\bllm\b",
    r"\bgenai\b",
    r"\bgenerative ai\b",
    r"\bfull[\s\-]?stack\b",
    r"\breact\b",
    r"\btypescript\b",
    r"\bnode\.?js\b",
    r"\btech(?:nical)?\s*lead\b",
    r"\blead (?:software |frontend |backend |full[\s\-]?stack )?engineer\b",
    r"\bsolutions?\s*architect\b",
    r"\bsoftware architect\b",
]

RECRUITER_PATTERNS = [
    r"\brecruiter\b",
    r"\btalent acquisition\b",
    r"\btalent partner\b",
    r"\bstaffing\b",
    r"\bsourcer\b",
    r"\bpeople partner\b",
    r"\bhr business partner\b",
    r"\bhuman resources\b",
]

# People we want to email at those companies
DM_TITLES = [
    "CTO",
    "Chief Technology Officer",
    "VP Engineering",
    "VP of Engineering",
    "Vice President of Engineering",
    "Head of Engineering",
    "Director of Engineering",
    "Engineering Manager",
    "Founder",
    "Co-Founder",
    "CEO",
    "Chief Executive Officer",
    "Head of Product",
    "VP Product",
]

HTTP_TIMEOUT = 60


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class PipelineError(RuntimeError):
    """Fatal pipeline error — stop and report."""


def _load_dotenv(path: Path = Path("/workspace/.env")) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


def _secret(*names: str) -> str | None:
    for name in names:
        val = os.environ.get(name)
        if val and val.strip():
            return val.strip()
    return None


def require_secrets() -> dict[str, str]:
    """Resolve required API keys. Accepts apify as alias for APIFY_KEY."""
    secrets = {
        "APIFY_KEY": _secret("APIFY_KEY", "APIFY_TOKEN", "APIFY_API_KEY", "apify"),
        "PROSPEO_API_KEY": _secret("PROSPEO_API_KEY", "PROSPEO_KEY"),
        "MILLIONVERIFIER_API_KEY": _secret("MILLIONVERIFIER_API_KEY", "MILLIONVERIFIER_KEY"),
        "INSTANTLY_API_KEY": _secret("INSTANTLY_API_KEY", "INSTANTLY_KEY"),
    }
    missing = [k for k, v in secrets.items() if not v]
    if missing:
        raise PipelineError(
            "Missing required env secrets: "
            + ", ".join(missing)
            + ". Inject them into the cloud agent environment (or .env) and re-run."
        )
    return secrets  # type: ignore[return-value]


def _matches_any(text: str, patterns: list[str]) -> bool:
    t = (text or "").lower()
    return any(re.search(p, t, re.I) for p in patterns)


def _company_employees(job: dict[str, Any]) -> int | None:
    for key in (
        "companyEmployeesCount",
        "companyEmployeeCount",
        "employeesCount",
        "companyStaffCount",
        "staffCount",
    ):
        val = job.get(key)
        if val is None and isinstance(job.get("company"), dict):
            val = job["company"].get(key) or job["company"].get("employeeCount")
        if val is None:
            continue
        if isinstance(val, (int, float)):
            return int(val)
        # ranges like "51-200" or "1K-5K"
        s = str(val).strip().replace(",", "")
        m = re.match(r"^(\d+(?:\.\d+)?)\s*[kK]?\s*[-–]\s*(\d+(?:\.\d+)?)\s*[kK]?", s)
        if m:
            hi = float(m.group(2))
            if "k" in s.lower().split("-")[-1] or s.lower().endswith("k"):
                hi *= 1000
            return int(hi)
        m = re.search(r"(\d+(?:\.\d+)?)\s*[kK]\+?", s)
        if m:
            return int(float(m.group(1)) * 1000)
        m = re.search(r"(\d+)", s)
        if m:
            return int(m.group(1))
    return None


def _job_title(job: dict[str, Any]) -> str:
    return (
        job.get("title")
        or job.get("jobTitle")
        or job.get("job_title")
        or ""
    ).strip()


def _company_name(job: dict[str, Any]) -> str:
    if isinstance(job.get("company"), dict):
        return (job["company"].get("name") or job["company"].get("companyName") or "").strip()
    return (
        job.get("companyName")
        or job.get("company_name")
        or job.get("company")
        or ""
    ).strip() if not isinstance(job.get("company"), dict) else ""


def _company_domain(job: dict[str, Any]) -> str:
    candidates = []
    company = job.get("company") if isinstance(job.get("company"), dict) else {}
    for key in ("companyWebsite", "website", "companyUrl", "company_url", "url"):
        candidates.append(job.get(key))
        candidates.append(company.get(key) if company else None)
    for raw in candidates:
        if not raw:
            continue
        s = str(raw).strip()
        if "linkedin.com" in s.lower():
            continue
        if "://" not in s:
            s = "https://" + s
        try:
            host = urlparse(s).hostname or ""
        except Exception:
            continue
        host = host.lower().removeprefix("www.")
        if host and "linkedin.com" not in host:
            return host
    return ""


def _company_linkedin(job: dict[str, Any]) -> str:
    company = job.get("company") if isinstance(job.get("company"), dict) else {}
    for key in ("companyLinkedinUrl", "companyUrl", "linkedinUrl", "url"):
        val = job.get(key) or (company.get(key) if company else None)
        if val and "linkedin.com/company" in str(val).lower():
            return str(val).strip()
    return ""


def stop_on_http_error(resp: requests.Response, service: str) -> None:
    if resp.status_code == 429:
        raise PipelineError(f"{service} rate limit (HTTP 429): {resp.text[:500]}")
    if resp.status_code >= 400:
        raise PipelineError(
            f"{service} API error HTTP {resp.status_code}: {resp.text[:800]}"
        )


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def build_search_urls() -> list[str]:
    urls = []
    for kw in SEARCH_KEYWORDS:
        urls.append(
            "https://www.linkedin.com/jobs/search/?"
            f"keywords={quote(kw)}&location=Australia&geoId={AU_GEO_ID}"
            "&f_TPR=r86400&sortBy=DD"
        )
    return urls


def scrape_jobs(apify_key: str) -> list[dict[str, Any]]:
    if ApifyClient is None:
        raise PipelineError("apify-client not installed. Run: pip install apify-client")

    urls = build_search_urls()
    print(f"[Apify] Starting {ACTOR_ID} with {len(urls)} AU search URLs, count={MAX_JOBS}, scrapeCompany=true")
    client = ApifyClient(apify_key)
    run_input = {
        "urls": urls,
        "scrapeCompany": True,
        "count": MAX_JOBS,
        # alias some callers use
        "maxJobs": MAX_JOBS,
    }
    try:
        run = client.actor(ACTOR_ID).call(run_input=run_input)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if "429" in msg or "rate" in msg.lower():
            raise PipelineError(f"Apify rate limit: {msg}") from exc
        raise PipelineError(f"Apify actor call failed: {msg}") from exc

    if not run:
        raise PipelineError("Apify returned empty run result")
    status = run.get("status")
    if status and status not in ("SUCCEEDED", "SUCCEEDED_WITH_WARNINGS"):
        raise PipelineError(f"Apify run status={status}: {json.dumps(run)[:800]}")

    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        raise PipelineError(f"Apify run missing defaultDatasetId: {json.dumps(run)[:800]}")

    jobs = list(client.dataset(dataset_id).iterate_items())
    print(f"[Apify] Scraped {len(jobs)} raw jobs (runId={run.get('id')})")
    write_json(OUT_DIR / "01_raw_jobs.json", jobs)
    return jobs


def filter_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    stats = {"raw": len(jobs), "drop_size": 0, "drop_recruiter": 0, "drop_title": 0, "kept": 0}
    for job in jobs:
        title = _job_title(job)
        emp = _company_employees(job)
        if emp is not None and emp >= 1000:
            stats["drop_size"] += 1
            continue
        if _matches_any(title, RECRUITER_PATTERNS):
            stats["drop_recruiter"] += 1
            continue
        if not _matches_any(title, RESUME_FIT_PATTERNS):
            stats["drop_title"] += 1
            continue
        # Deduplicate by job URL / id if present
        kept.append(job)

    # de-dupe by link/title+company
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for job in kept:
        key = (
            str(job.get("link") or job.get("jobUrl") or job.get("url") or "")
            or f"{_company_name(job)}::{_job_title(job)}"
        ).lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(job)

    stats["kept"] = len(deduped)
    print(
        f"[Filter] raw={stats['raw']} drop_size>={1000}:{stats['drop_size']} "
        f"drop_recruiter={stats['drop_recruiter']} drop_title={stats['drop_title']} "
        f"kept={stats['kept']}"
    )
    write_json(OUT_DIR / "02_filtered_jobs.json", {"stats": stats, "jobs": deduped})
    return deduped


def prospeo_search_dms(api_key: str, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Search decision-makers at kept companies; enrich emails."""
    dms: list[dict[str, Any]] = []
    # Group jobs by company domain/name
    companies: dict[str, dict[str, Any]] = {}
    for job in jobs:
        domain = _company_domain(job)
        name = _company_name(job)
        key = domain or name.lower()
        if not key:
            continue
        entry = companies.setdefault(
            key,
            {
                "domain": domain,
                "name": name,
                "linkedin": _company_linkedin(job),
                "jobs": [],
            },
        )
        entry["jobs"].append(job)

    print(f"[Prospeo] Searching DMs at {len(companies)} companies...")
    for i, (key, company) in enumerate(companies.items(), 1):
        filters: dict[str, Any] = {
            "person_job_title": {
                "include": DM_TITLES,
                "match_only_exact_job_titles": False,
            },
        }
        if company["domain"]:
            filters["company"] = {"websites": {"include": [company["domain"]]}}
        elif company["name"]:
            filters["company"] = {"names": {"include": [company["name"]]}}
        else:
            continue

        payload = {"page": 1, "filters": filters}
        try:
            resp = requests.post(
                f"{PROSPEO_API}/search-person",
                headers={"X-KEY": api_key, "Content-Type": "application/json"},
                json=payload,
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException as exc:
            raise PipelineError(f"Prospeo search network error: {exc}") from exc

        if resp.status_code == 429:
            raise PipelineError(f"Prospeo rate limit (HTTP 429): {resp.text[:500]}")
        if resp.status_code >= 400:
            # NO_RESULTS is not fatal for a single company
            try:
                body = resp.json()
            except Exception:
                body = {}
            code = body.get("error_code") or ""
            if code in ("NO_RESULTS",):
                print(f"  [{i}/{len(companies)}] {company['name'] or company['domain']}: no results")
                continue
            raise PipelineError(
                f"Prospeo search API error HTTP {resp.status_code}: {resp.text[:800]}"
            )

        body = resp.json()
        results = body.get("results") or []
        print(
            f"  [{i}/{len(companies)}] {company['name'] or company['domain']}: "
            f"{len(results)} people"
        )

        for row in results[:5]:  # cap DMs per company
            person = row.get("person") or {}
            title = person.get("current_job_title") or ""
            if _matches_any(title, RECRUITER_PATTERNS):
                continue
            person_id = person.get("person_id")
            if not person_id:
                continue
            enriched = prospeo_enrich(api_key, person_id)
            email = (
                ((enriched.get("person") or {}).get("email") or {})
            )
            if isinstance(email, dict):
                email_addr = email.get("email") or ""
            else:
                email_addr = email or ""
            if not email_addr:
                # fallback nested shapes
                email_addr = (
                    (enriched.get("person") or {}).get("email")
                    if isinstance((enriched.get("person") or {}).get("email"), str)
                    else ""
                ) or ""

            dm = {
                "person_id": person_id,
                "first_name": person.get("first_name")
                or (enriched.get("person") or {}).get("first_name")
                or "",
                "last_name": person.get("last_name")
                or (enriched.get("person") or {}).get("last_name")
                or "",
                "position": title
                or (enriched.get("person") or {}).get("current_job_title")
                or "",
                "email": email_addr if isinstance(email_addr, str) else "",
                "linkedin_url": person.get("linkedin_url")
                or (enriched.get("person") or {}).get("linkedin_url")
                or "",
                "company_name": company["name"]
                or ((enriched.get("company") or {}).get("name") or ""),
                "company_domain": company["domain"]
                or ((enriched.get("company") or {}).get("domain") or ""),
                "jobs": company["jobs"],
            }
            dms.append(dm)

        # polite pacing (~2 rps max); stop on rate limit rather than retry loops
        time.sleep(0.55)

    print(f"[Prospeo] Found {len(dms)} decision-makers (pre-MV)")
    write_json(OUT_DIR / "03_dms_enriched.json", dms)
    return dms


def prospeo_enrich(api_key: str, person_id: str) -> dict[str, Any]:
    try:
        resp = requests.post(
            f"{PROSPEO_API}/enrich-person",
            headers={"X-KEY": api_key, "Content-Type": "application/json"},
            json={"only_verified_email": True, "data": {"person_id": person_id}},
            timeout=HTTP_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise PipelineError(f"Prospeo enrich network error: {exc}") from exc

    if resp.status_code == 429:
        raise PipelineError(f"Prospeo enrich rate limit (HTTP 429): {resp.text[:500]}")
    if resp.status_code >= 400:
        # Enrich miss is not always fatal — return empty
        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text[:500]}
        code = body.get("error_code") or ""
        if code in ("NO_RESULTS", "INVALID_REQUEST", "INSUFFICIENT_CREDITS"):
            if code == "INSUFFICIENT_CREDITS":
                raise PipelineError(f"Prospeo insufficient credits: {resp.text[:500]}")
            return {}
        raise PipelineError(
            f"Prospeo enrich API error HTTP {resp.status_code}: {resp.text[:800]}"
        )
    return resp.json() if resp.content else {}


def millionverifier_keep_ok(api_key: str, dms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    stats = {"checked": 0, "ok": 0, "catch_all": 0, "invalid": 0, "unknown": 0, "other": 0, "no_email": 0}
    for dm in dms:
        email = (dm.get("email") or "").strip()
        if not email or "@" not in email:
            stats["no_email"] += 1
            continue
        stats["checked"] += 1
        try:
            resp = requests.get(
                MV_API,
                params={"api": api_key, "email": email, "timeout": 10},
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException as exc:
            raise PipelineError(f"MillionVerifier network error: {exc}") from exc

        if resp.status_code == 429:
            raise PipelineError(f"MillionVerifier rate limit (HTTP 429): {resp.text[:500]}")
        if resp.status_code >= 400:
            raise PipelineError(
                f"MillionVerifier API error HTTP {resp.status_code}: {resp.text[:800]}"
            )

        data = resp.json()
        # result is string like ok / catch_all / invalid / unknown
        result = str(data.get("result") or data.get("resultcode") or "").lower()
        dm["mv_result"] = result
        dm["mv_raw"] = data
        if result in ("ok", "1"):
            stats["ok"] += 1
            kept.append(dm)
        elif "catch" in result:
            stats["catch_all"] += 1
        elif result in ("invalid", "2", "5", "disposable"):
            stats["invalid"] += 1
        elif result in ("unknown", "4", "3"):
            stats["unknown"] += 1
        else:
            stats["other"] += 1
        time.sleep(0.2)

    print(
        f"[MillionVerifier] checked={stats['checked']} ok={stats['ok']} "
        f"catch_all={stats['catch_all']} invalid={stats['invalid']} "
        f"unknown={stats['unknown']} no_email={stats['no_email']} other={stats['other']}"
    )
    write_json(OUT_DIR / "04_dms_ok.json", {"stats": stats, "dms": kept})
    return kept


def join_jobs_with_dms(dms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One lead row per DM×primary-job; custom vars jobTitle / position."""
    leads: list[dict[str, Any]] = []
    seen_emails: set[str] = set()
    for dm in dms:
        email = (dm.get("email") or "").strip().lower()
        if not email or email in seen_emails:
            continue
        seen_emails.add(email)
        jobs = dm.get("jobs") or []
        primary = jobs[0] if jobs else {}
        job_title = _job_title(primary)
        lead = {
            "email": email,
            "first_name": dm.get("first_name") or "",
            "last_name": dm.get("last_name") or "",
            "company_name": dm.get("company_name") or _company_name(primary),
            "website": dm.get("company_domain") or "",
            "linkedin_url": dm.get("linkedin_url") or "",
            "jobTitle": job_title,  # hiring-for
            "position": dm.get("position") or "",  # person title
        }
        leads.append(lead)
    print(f"[Join] {len(leads)} unique leads ready for Instantly")
    write_json(OUT_DIR / "05_leads.json", leads)
    return leads


def push_instantly(api_key: str, leads: list[dict[str, Any]]) -> dict[str, Any]:
    if not leads:
        summary = {
            "leads_uploaded": 0,
            "duplicated_leads": 0,
            "skipped_count": 0,
            "total_sent": 0,
            "note": "no leads to upload",
        }
        print("[Instantly] No leads to upload")
        return summary

    payload_leads = []
    for lead in leads:
        payload_leads.append(
            {
                "email": lead["email"],
                "first_name": lead.get("first_name") or "",
                "last_name": lead.get("last_name") or "",
                "company_name": lead.get("company_name") or "",
                "website": lead.get("website") or "",
                "custom_variables": {
                    "jobTitle": lead.get("jobTitle") or "",
                    "position": lead.get("position") or "",
                    "linkedin_url": lead.get("linkedin_url") or "",
                },
            }
        )

    print(
        f"[Instantly] POST /api/v2/leads/add → campaign {INSTANTLY_CAMPAIGN_ID} "
        f"({len(payload_leads)} leads). Campaign will NOT be activated."
    )
    try:
        resp = requests.post(
            f"{INSTANTLY_API}/api/v2/leads/add",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "campaign_id": INSTANTLY_CAMPAIGN_ID,
                "leads": payload_leads,
                "skip_if_in_campaign": True,
                "skip_if_in_workspace": False,
            },
            timeout=HTTP_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise PipelineError(f"Instantly network error: {exc}") from exc

    stop_on_http_error(resp, "Instantly")
    data = resp.json() if resp.content else {}
    summary = {
        "leads_uploaded": data.get("leads_uploaded", 0),
        "duplicated_leads": data.get("duplicated_leads", 0),
        "skipped_count": data.get("skipped_count", 0),
        "invalid_email_count": data.get("invalid_email_count", 0),
        "in_blocklist": data.get("in_blocklist", 0),
        "total_sent": data.get("total_sent", len(payload_leads)),
        "raw": data,
    }
    print(
        f"[Instantly] uploaded={summary['leads_uploaded']} "
        f"duplicates={summary['duplicated_leads']} "
        f"skipped={summary['skipped_count']} "
        f"(invalid={summary['invalid_email_count']}, blocklist={summary['in_blocklist']})"
    )
    write_json(OUT_DIR / "06_instantly.json", summary)
    return summary


def print_summary(
    jobs_kept: int,
    dms: int,
    ok_emails: int,
    instantly: dict[str, Any],
) -> None:
    print("\n========== AU JOBS DM PIPELINE SUMMARY ==========")
    print(f"jobs kept:           {jobs_kept}")
    print(f"DMs (enriched):      {dms}")
    print(f"ok emails (MV):      {ok_emails}")
    print(f"Instantly uploaded:  {instantly.get('leads_uploaded', 0)}")
    print(f"Instantly duplicates:{instantly.get('duplicated_leads', 0)}")
    print(f"Instantly skipped:   {instantly.get('skipped_count', 0)}")
    print("campaign activated:  NO")
    print(f"artifacts:           {OUT_DIR}")
    print("=================================================\n")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    _load_dotenv()
    started = datetime.now(timezone.utc).isoformat()
    print(f"[Pipeline] start {started}")

    try:
        secrets = require_secrets()
        print("[Pipeline] All required secrets present (APIFY_KEY, PROSPEO_API_KEY, MILLIONVERIFIER_API_KEY, INSTANTLY_API_KEY)")

        jobs = scrape_jobs(secrets["APIFY_KEY"])
        kept = filter_jobs(jobs)
        dms = prospeo_search_dms(secrets["PROSPEO_API_KEY"], kept) if kept else []
        ok = millionverifier_keep_ok(secrets["MILLIONVERIFIER_API_KEY"], dms) if dms else []
        leads = join_jobs_with_dms(ok) if ok else []
        instantly = push_instantly(secrets["INSTANTLY_API_KEY"], leads)

        summary = {
            "started": started,
            "finished": datetime.now(timezone.utc).isoformat(),
            "jobs_kept": len(kept),
            "dms": len(dms),
            "ok_emails": len(ok),
            "instantly": {
                "uploaded": instantly.get("leads_uploaded", 0),
                "duplicates": instantly.get("duplicated_leads", 0),
                "skipped": instantly.get("skipped_count", 0),
            },
            "campaign_id": INSTANTLY_CAMPAIGN_ID,
            "campaign_activated": False,
        }
        write_json(OUT_DIR / "summary.json", summary)
        print_summary(len(kept), len(dms), len(ok), instantly)
        return 0

    except PipelineError as exc:
        print(f"\n[Pipeline] STOPPED: {exc}", file=sys.stderr)
        write_json(
            OUT_DIR / "error.json",
            {
                "started": started,
                "finished": datetime.now(timezone.utc).isoformat(),
                "error": str(exc),
            },
        )
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"\n[Pipeline] UNEXPECTED ERROR: {exc}", file=sys.stderr)
        write_json(
            OUT_DIR / "error.json",
            {
                "started": started,
                "finished": datetime.now(timezone.utc).isoformat(),
                "error": f"unexpected: {exc}",
            },
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
