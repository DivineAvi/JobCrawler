from __future__ import annotations

import hashlib
import re
from typing import Any, Iterator

from scraper.http import client

YC_COMPANIES_URL = "https://www.ycombinator.com/companies"
ALGOLIA_HOST = "https://45BWZJ1SGC-dsn.algolia.net"
INDEX = "YCCompany_production"


def fetch_algolia_credentials() -> tuple[str, str]:
    with client() as c:
        r = c.get(YC_COMPANIES_URL)
        r.raise_for_status()
    m = re.search(
        r'window\.AlgoliaOpts\s*=\s*\{\s*"app"\s*:\s*"([^"]+)"\s*,\s*"key"\s*:\s*"([^"]+)"',
        r.text,
    )
    if not m:
        raise RuntimeError("Could not parse Algolia app id / key from YC companies page")
    return m.group(1), m.group(2)


def slug_shard_index(slug: str, num_shards: int) -> int:
    """
    Stable shard for a YC company slug (same across processes / machines).
    Uses SHA-256 — not Python's built-in hash() (randomized per process).
    """
    if num_shards <= 1:
        return 0
    h = hashlib.sha256(slug.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big") % num_shards


def iter_companies(
    *,
    only_hiring: bool = True,
    hits_per_page: int = 100,
    shard: int = 0,
    num_shards: int = 1,
) -> Iterator[dict[str, Any]]:
    app_id, api_key = fetch_algolia_credentials()
    headers = {
        "X-Algolia-Application-Id": app_id,
        "X-Algolia-API-Key": api_key,
        "Content-Type": "application/json",
    }
    facet_filters = "isHiring:true" if only_hiring else ""
    page = 0
    with client() as c:
        while True:
            params = f"hitsPerPage={hits_per_page}&page={page}&query="
            if facet_filters:
                params += f"&facetFilters={facet_filters}"
            r = c.post(
                f"{ALGOLIA_HOST}/1/indexes/{INDEX}/query",
                headers=headers,
                json={"params": params},
            )
            r.raise_for_status()
            data = r.json()
            for hit in data.get("hits", []):
                if num_shards > 1:
                    s = (hit.get("slug") or "") or str(hit.get("objectID") or "")
                    if slug_shard_index(s, num_shards) != shard:
                        continue
                yield hit
            page += 1
            if page >= data.get("nbPages", 0):
                break
