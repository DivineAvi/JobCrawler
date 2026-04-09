from __future__ import annotations

import re
from typing import Any

from scraper.http import client

# Embedded ATS URLs inside career pages (custom domains that point to Greenhouse/Lever).
_RE_GREENHOUSE_BOARD = re.compile(
    r"https?://boards\.greenhouse\.io/([a-zA-Z0-9_-]+)",
    re.I,
)
_RE_LEVER_COMPANY = re.compile(
    r"https?://jobs\.lever\.co/([a-zA-Z0-9_-]+)",
    re.I,
)
_RE_ASHBY_BOARD = re.compile(
    r"https?://(?:www\.)?jobs\.ashbyhq\.com/([a-zA-Z0-9_-]+)",
    re.I,
)

_ASHBY_RESERVED_SLUGS = frozenset({"api", "www", "static", "embed"})


def parse_greenhouse_board_url(page_url: str) -> str | None:
    # https://boards.greenhouse.io/{token}
    m = re.search(r"boards\.greenhouse\.io/([^/?#]+)", page_url, re.I)
    return m.group(1) if m else None


def parse_lever_company_url(page_url: str) -> str | None:
    # https://jobs.lever.co/{company}
    m = re.search(r"jobs\.lever\.co/([^/?#]+)", page_url, re.I)
    return m.group(1) if m else None


def parse_ashby_board_slug(page_url: str) -> str | None:
    """First path segment on jobs.ashbyhq.com is the job board name (public API slug)."""
    m = _RE_ASHBY_BOARD.search(page_url)
    if not m:
        return None
    slug = m.group(1)
    if slug.lower() in _ASHBY_RESERVED_SLUGS:
        return None
    return slug


def fetch_greenhouse_jobs(board_token: str) -> list[dict[str, Any]]:
    api = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs?content=true"
    with client() as c:
        r = c.get(api)
        if r.status_code != 200:
            return []
        data = r.json()
    out: list[dict[str, Any]] = []
    for job in data.get("jobs") or []:
        loc = job.get("location")
        if isinstance(loc, dict):
            loc_name = loc.get("name")
        else:
            loc_name = loc
        out.append(
            {
                "source": "greenhouse",
                "title": job.get("title"),
                "url": job.get("absolute_url"),
                "location": loc_name,
            }
        )
    return out


def fetch_lever_jobs(company_slug: str) -> list[dict[str, Any]]:
    api = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"
    with client() as c:
        r = c.get(api)
        if r.status_code != 200:
            return []
        data = r.json()
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for job in data:
        locs = job.get("categories", {}).get("location") if isinstance(job.get("categories"), dict) else None
        location = ", ".join(locs) if isinstance(locs, list) else locs
        out.append(
            {
                "source": "lever",
                "title": job.get("text"),
                "url": job.get("hostedUrl") or job.get("applyUrl"),
                "location": location,
            }
        )
    return out


def fetch_ashby_jobs(board_slug: str) -> list[dict[str, Any]]:
    """Public Ashby job board API (no auth)."""
    api = f"https://api.ashbyhq.com/posting-api/job-board/{board_slug}?includeCompensation=false"
    with client() as c:
        r = c.get(api)
        if r.status_code != 200:
            return []
        data = r.json()
    out: list[dict[str, Any]] = []
    for job in data.get("jobs") or []:
        if job.get("isListed") is False:
            continue
        out.append(
            {
                "source": "ashby",
                "title": job.get("title"),
                "url": job.get("jobUrl"),
                "location": job.get("location"),
                "apply_url": job.get("applyUrl"),
            }
        )
    return out


def try_fetch_ats_jobs(careers_url: str) -> list[dict[str, Any]]:
    gh = parse_greenhouse_board_url(careers_url)
    if gh:
        return fetch_greenhouse_jobs(gh)
    lv = parse_lever_company_url(careers_url)
    if lv:
        return fetch_lever_jobs(lv)
    ab = parse_ashby_board_slug(careers_url)
    if ab:
        return fetch_ashby_jobs(ab)
    return []


def _tokens_from_html(html: str) -> tuple[set[str], set[str], set[str]]:
    gh = set(_RE_GREENHOUSE_BOARD.findall(html or ""))
    lv = set(_RE_LEVER_COMPANY.findall(html or ""))
    ab = set(_RE_ASHBY_BOARD.findall(html or ""))
    # Drop junk captures (scripts, extensions).
    gh = {t for t in gh if t and len(t) >= 2 and not t.lower().endswith((".js", ".css", ".png"))}
    lv = {t for t in lv if t and len(t) >= 2 and not t.lower().endswith((".js", ".css"))}
    ab = {t for t in ab if t and len(t) >= 2 and t.lower() not in _ASHBY_RESERVED_SLUGS}
    return gh, lv, ab


def try_fetch_ats_jobs_from_html(html: str) -> list[dict[str, Any]]:
    """When the careers URL is a marketing domain but the page embeds Greenhouse/Lever/Ashby links."""
    boards, levers, ashby_slugs = _tokens_from_html(html)
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for token in sorted(boards):
        for job in fetch_greenhouse_jobs(token):
            u = job.get("url") or ""
            if u and u not in seen:
                seen.add(u)
                out.append(job)
    for slug in sorted(levers):
        for job in fetch_lever_jobs(slug):
            u = job.get("url") or ""
            if u and u not in seen:
                seen.add(u)
                out.append(job)
    for slug in sorted(ashby_slugs):
        for job in fetch_ashby_jobs(slug):
            u = job.get("url") or ""
            if u and u not in seen:
                seen.add(u)
                out.append(job)
    return out
