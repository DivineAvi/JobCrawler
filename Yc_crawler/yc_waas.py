from __future__ import annotations

import html
import json
import re
from typing import Any

from scraper.http import client


def fetch_yc_listed_jobs(slug: str) -> list[dict[str, Any]]:
    """Jobs listed on ycombinator.com/companies/{slug}/jobs (Work at a Startup listings)."""
    url = f"https://www.ycombinator.com/companies/{slug}/jobs"
    with client() as c:
        r = c.get(url)
        r.raise_for_status()
    m = re.search(r'data-page="([^"]+)"', r.text)
    if not m:
        return []
    payload = json.loads(html.unescape(m.group(1)))
    return list(payload.get("props", {}).get("jobPostings") or [])
