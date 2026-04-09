from __future__ import annotations

from urllib.parse import urljoin, urlparse


def normalize_website(url: str | None) -> str | None:
    if not url or not str(url).strip():
        return None
    u = str(url).strip()
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    parsed = urlparse(u)
    if not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def absolutize(base: str, href: str) -> str:
    return urljoin(base if base.endswith("/") else base + "/", href)
