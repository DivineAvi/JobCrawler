from __future__ import annotations

import json
import re
from typing import Any

from bs4 import BeautifulSoup

from scraper.urls import absolutize

_MAX_JSON_NODES = 80_000

# Heuristic: objects that look like Greenhouse / Lever / similar job rows inside big JSON blobs.
_JOB_HINT_KEYS = frozenset(
    {
        "absolute_url",
        "hostedurl",
        "hosted_url",
        "joburl",
        "applyurl",
        "requisition_id",
        "job_id",
        "jobid",
        "gh_office_id",
        "offices",
        "first_published",
        "updated_at",
        "departments",
    }
)


def _normalize_title_blocklist() -> frozenset[str]:
    return frozenset(
        {
            "culture",
            "benefits",
            "privacy policy",
            "open positions",
            "current openings",
            "view all jobs",
        }
    )


_TITLE_BLOCK = _normalize_title_blocklist()


def _job_like_object(d: dict[str, Any]) -> bool:
    keys = {str(k).lower() for k in d.keys()}
    if not keys & _JOB_HINT_KEYS:
        return False
    title = d.get("title") or d.get("text") or d.get("jobTitle") or d.get("name")
    return isinstance(title, str) and len(title.strip()) >= 2


def _extract_url_title_location(d: dict[str, Any], page_url: str) -> dict[str, Any] | None:
    title = d.get("title") or d.get("text") or d.get("jobTitle")
    if not isinstance(title, str):
        return None
    title = title.strip()
    if len(title) < 2 or title.lower() in _TITLE_BLOCK:
        return None

    url: str | None = None
    for key in ("absolute_url", "hostedUrl", "hosted_url", "jobUrl", "public_url", "applyUrl", "apply_url"):
        v = d.get(key)
        if isinstance(v, str) and v.strip().startswith("http"):
            url = v.strip().split("#")[0]
            break
    if not url:
        v = d.get("url")
        if isinstance(v, str) and v.strip().startswith("http"):
            low = v.lower()
            if any(x in low for x in ("greenhouse", "lever", "ashby", "workday", "smartrecruiters", "icims", "/job")):
                url = v.strip().split("#")[0]
        elif isinstance(v, str) and v.strip().startswith("/"):
            url = absolutize(page_url, v.strip()).split("#")[0]

    if not url:
        return None

    location = None
    loc = d.get("location") or d.get("jobLocation")
    if isinstance(loc, dict):
        location = loc.get("name") or loc.get("title")
        addr = loc.get("address")
        if not location and isinstance(addr, dict):
            parts = [addr.get("addressLocality"), addr.get("addressRegion"), addr.get("addressCountry")]
            location = ", ".join(str(p) for p in parts if p)
    elif isinstance(loc, str):
        location = loc

    rec: dict[str, Any] = {"source": "embedded_json", "title": title, "url": url, "location": location}
    au = d.get("applyUrl") or d.get("apply_url")
    if isinstance(au, str) and au.strip().startswith("http"):
        rec["apply_url"] = au.strip().split("#")[0]
    return rec


def _walk_json(obj: Any, page_url: str, out: list[dict[str, Any]], counter: list[int]) -> None:
    if counter[0] >= _MAX_JSON_NODES:
        return
    counter[0] += 1

    if isinstance(obj, dict):
        if _job_like_object(obj):
            rec = _extract_url_title_location(obj, page_url)
            if rec:
                out.append(rec)
        for v in obj.values():
            _walk_json(v, page_url, out, counter)
    elif isinstance(obj, list):
        for item in obj:
            _walk_json(item, page_url, out, counter)


def extract_jobs_from_embedded_json(page_url: str, html_text: str) -> list[dict[str, Any]]:
    """Pull job rows from __NEXT_DATA__, application/json scripts, and other large JSON blobs."""
    soup = BeautifulSoup(html_text, "html.parser")
    seen: set[str] = set()
    out: list[dict[str, Any]] = []

    script_sources: list[str] = []
    for script in soup.find_all("script"):
        sid = (script.get("id") or "").lower()
        typ = (script.get("type") or "").lower()
        if "ld+json" in typ:
            continue
        if sid == "__next_data__" or ("application/json" in typ and "ld+json" not in typ):
            raw = script.string or script.get_text() or ""
            raw = raw.strip()
            if raw and len(raw) > 20:
                script_sources.append(raw)
        # Webflow / bundles: inline JSON with Greenhouse job objects.
        if typ in ("", "text/javascript"):
            raw = script.string or ""
            if len(raw) > 4000 and "absolute_url" in raw and "greenhouse" in raw.lower():
                script_sources.append(raw)

    for raw in script_sources:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        chunk: list[dict[str, Any]] = []
        _walk_json(data, page_url, chunk, [0])
        for job in chunk:
            key = (job.get("url") or "") + "\0" + (job.get("title") or "")
            if key in seen:
                continue
            seen.add(key)
            out.append(job)

    return out
