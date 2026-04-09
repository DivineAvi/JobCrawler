from __future__ import annotations

import json
import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from scraper.ats import try_fetch_ats_jobs, try_fetch_ats_jobs_from_html
from scraper.embedded_json import extract_jobs_from_embedded_json
from scraper.http import client
from scraper.urls import absolutize, normalize_website

CAREER_PATHS = (
    "/careers",
    "/jobs",
    "/career",
    "/join",
    "/join-us",
    "/about/careers",
    "/about/jobs",
    "/work-with-us",
)

ATS_HINTS = re.compile(
    r"(greenhouse\.io|lever\.co|ashbyhq\.com|workable\.com|breezy\.hr|bamboohr\.com)",
    re.I,
)

# Path segments that are almost never a single job posting (nav / policy / culture pages).
_NON_JOB_SEGMENTS = frozenset(
    {
        "culture",
        "benefits",
        "benefit",
        "perks",
        "perk",
        "life-at",
        "lifeat",
        "diversity",
        "inclusion",
        "dei",
        "deib",
        "belonging",
        "values",
        "mission",
        "vision",
        "our-team",
        "team",
        "leadership",
        "management",
        "about",
        "who-we-are",
        "faq",
        "faqs",
        "contact",
        "legal",
        "privacy",
        "policy",
        "policies",
        "notice",
        "notices",
        "terms",
        "cookies",
        "cookie",
        "accessibility",
        "a11y",
        "veterans",
        "hiring-veterans",
        "apprentice-program",
        "apprenticeships",
        "internships",
        "students",
        "universities",
        "branches",
        "corporate",
        "job-applicant-privacy-notice",
        "applicant-privacy",
    }
)

# Last URL segment: listing hubs, not a single role (do not use "jobs" globally — breaks /jobs/123).
_HUB_LAST_SEGMENTS = frozenset(
    {
        "jobs",
        "job",
        "careers",
        "career",
        "openings",
        "opening",
        "open-positions",
        "open_positions",
        "current-openings",
        "current_openings",
        "all-jobs",
        "search",
        "listings",
        "listing",
    }
)

# Anchor text that describes sections or hubs, not one role.
_NON_JOB_LINK_TITLES = frozenset(
    {
        "culture",
        "our culture",
        "benefits",
        "perks",
        "diversity",
        "inclusion",
        "privacy policy",
        "privacy notice",
        "terms of use",
        "terms",
        "contact",
        "faq",
        "our team",
        "team",
        "leadership",
        "values",
        "mission",
        "veterans",
        "internships",
        "apprenticeships",
        "current openings",
        "open positions",
        "open roles",
        "all jobs",
        "view all jobs",
        "browse jobs",
        "see openings",
        "job openings",
        "careers home",
        "home",
        "life at",
        "why join us",
        "why join",
    }
)

_PATH_DENY_RE = re.compile(
    r"(privacy|applicant-privacy|job-applicant|cookie-policy|accessibility-statement|/legal/|/terms)",
    re.I,
)

_LISTING_PAGE_RE = re.compile(
    r"(open-?positions|current-?openings|careers/openings|/jobs/?$|all-?openings|job-?openings|"
    r"/positions/?$|open-roles?|/roles/?$|/vacancies?|/opportunities|job-?board|/listings/?$)",
    re.I,
)

_LISTING_TITLE_RE = re.compile(
    r"^(all\s+)?open(ings|\s+positions|ings)?$|"
    r"^current\s+openings$|"
    r"^view(\s+all)?\s+jobs$|"
    r"^browse(\s+all)?\s+jobs$|"
    r"^see\s+openings$|"
    r"^job\s+openings$",
    re.I,
)

# Marketing CTAs: "View openings", "See open roles" — often the only href to the real job list.
_CTA_TEXT_RE = re.compile(
    r"\b(view|see|browse|explore|find|show|check\s+out)\s+.{0,24}\b(openings?|roles?|positions?|jobs?|"
    r"opportunities|vacancies)\b|"
    r"\b(all\s+)?(open|current)\s+(openings?|positions?|roles?)\b|"
    r"\bsearch\s+(our\s+)?(jobs?|roles?|openings?|opportunities)\b|"
    r"\bopen\s+roles?\b|"
    r"\b(job\s+)?openings?\b|"
    r"\bjoin\s+our\s+team\b|"
    r"\bwe['’]re\s+hiring\b|"
    r"\bopen\s+careers\b",
    re.I,
)


def _reg_host(url: str) -> str:
    p = urlparse(url)
    host = (p.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _same_registrable_domain(a: str, b: str) -> bool:
    return _reg_host(a) == _reg_host(b)


def _same_org_or_careers_portal(base: str, url: str) -> bool:
    """Same registrable host, or same first label (e.g. instacart.com ↔ instacart.careers)."""
    if _same_registrable_domain(base, url):
        return True
    bh = _reg_host(base)
    uh = _reg_host(url)
    if not bh or not uh:
        return False
    if bh.split(".")[0] == uh.split(".")[0]:
        return True
    return False


def _is_ats_url(url: str) -> bool:
    return bool(ATS_HINTS.search(url))


def _path_segments(path: str) -> list[str]:
    return [s.lower() for s in path.split("/") if s]


def _path_has_non_job_segment(path: str) -> bool:
    for seg in _path_segments(path):
        base = seg.split("?")[0]
        if base in _NON_JOB_SEGMENTS:
            return True
    return False


def _hub_last_segment(path: str) -> str:
    parts = [p for p in path.rstrip("/").split("/") if p]
    if not parts:
        return ""
    return parts[-1].lower().split("?")[0]


def _normalize_title(title: str) -> str:
    return " ".join(title.lower().split())


def _title_is_non_job_nav(title: str) -> bool:
    t = _normalize_title(title)
    if not t:
        return True
    if t in _NON_JOB_LINK_TITLES:
        return True
    if _LISTING_TITLE_RE.match(t):
        return True
    return False


def _url_looks_like_job_listing(path: str, title: str) -> bool:
    """True only if URL/title plausibly point to one role (not culture/benefits hubs)."""
    path_l = path.lower()
    if _PATH_DENY_RE.search(path_l):
        return False
    if _path_has_non_job_segment(path_l):
        return False

    last_seg = _hub_last_segment(path_l)
    if last_seg in _HUB_LAST_SEGMENTS and not re.search(r"/jobs?/\d+", path_l):
        return False

    # Strong signals: ATS job board hosts (handled elsewhere) or numeric / structured job IDs.
    if re.search(r"/jobs?/\d+", path_l):
        return True
    if re.search(r"/job/\d+", path_l):
        return True
    if re.search(r"/positions?/[^/]+", path_l) and last not in _NON_JOB_SEGMENTS:
        # e.g. /positions/12345-engineer
        if re.search(r"/positions?/(\d+|[\w-]{8,})", path_l):
            return True
    if re.search(r"/role/[^/]+-[^/]+", path_l):
        return True

    # /jobs/ or /careers/ with a multi-token slug (hyphen) — excludes single-word "culture".
    m = re.search(r"/(?:jobs?|careers)/([^/?#]+)/?$", path_l)
    if m:
        slug = m.group(1)
        if slug in _NON_JOB_SEGMENTS:
            return False
        if "-" in slug and len(slug) >= 8:
            return True
        if re.match(r"^\d{5,}-", slug) or re.match(r"^\d{6,}$", slug):
            return True

    if "/apply/" in path_l and len(last) >= 6:
        return True

    # Title must not be a nav label; require role-like title (heuristic).
    if _title_is_non_job_nav(title):
        return False
    if len(_normalize_title(title)) < 4:
        return False
    # Reject very generic single-word titles that slipped through
    if len(title.split()) == 1 and _normalize_title(title) in {"engineering", "product", "sales", "design"}:
        return False

    # Weak fallback: role-like title + career path with a substantive slug (not already rejected).
    if re.search(r"/(careers|jobs)/[^/]{12,}", path_l) and "-" in last_seg:
        return True

    return False


def _walk_ld_json(obj: object):
    if isinstance(obj, dict):
        if "@graph" in obj:
            yield from _walk_ld_json(obj["@graph"])
        yield obj
    elif isinstance(obj, list):
        for x in obj:
            yield from _walk_ld_json(x)


def extract_jsonld_job_postings(page_url: str, html_text: str) -> list[dict]:
    soup = BeautifulSoup(html_text, "html.parser")
    out: list[dict] = []
    seen: set[str] = set()
    for script in soup.find_all("script", attrs={"type": True}):
        t = (script.get("type") or "").lower()
        if "ld+json" not in t:
            continue
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for obj in _walk_ld_json(data):
            if not isinstance(obj, dict):
                continue
            typ = obj.get("@type")
            types = typ if isinstance(typ, list) else [typ] if typ else []
            type_strs = [x for x in types if isinstance(x, str)]
            if "JobPosting" not in type_strs:
                continue
            title = obj.get("title")
            if not title or not str(title).strip():
                continue
            title_s = str(title).strip()
            url_val = obj.get("url") or obj.get("sameAs")
            url: str | None
            if isinstance(url_val, list):
                url = next((x for x in url_val if isinstance(x, str) and x.startswith("http")), None)
            elif isinstance(url_val, str):
                u = url_val.strip()
                url = absolutize(page_url, u) if u.startswith("/") else u.split("#")[0]
            else:
                url = None

            location = None
            loc = obj.get("jobLocation")
            if isinstance(loc, dict):
                addr = loc.get("address")
                if isinstance(addr, dict):
                    parts = [
                        addr.get("addressLocality"),
                        addr.get("addressRegion"),
                        addr.get("addressCountry"),
                    ]
                    location = ", ".join(str(p) for p in parts if p)
                elif isinstance(addr, str):
                    location = addr
            elif isinstance(loc, list) and loc:
                first = loc[0]
                if isinstance(first, dict):
                    addr = first.get("address")
                    if isinstance(addr, dict):
                        parts = [
                            addr.get("addressLocality"),
                            addr.get("addressRegion"),
                            addr.get("addressCountry"),
                        ]
                        location = ", ".join(str(p) for p in parts if p)

            key = (url or "") + "\0" + title_s
            if key in seen:
                continue
            seen.add(key)
            out.append({"source": "json_ld", "title": title_s, "url": url, "location": location})
    return out


def _anchor_accessible_text(a) -> str:
    parts: list[str] = []
    for attr in ("aria-label", "title"):
        v = a.get(attr)
        if v and str(v).strip():
            parts.append(str(v).strip())
    t = a.get_text(separator=" ", strip=True)
    if t:
        parts.append(t)
    return " ".join(parts)


def _hub_follow_urls(page_url: str, html_text: str, *, max_urls: int = 8) -> list[str]:
    """
    URLs that likely lead to the real job list (path hints or CTA copy like 'View openings').
    One level only — we fetch these after the main careers landing HTML.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    best_score: dict[str, int] = {}

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        low = href.lower()
        if low.startswith("javascript:") or low.startswith("mailto:") or low.startswith("tel:"):
            continue
        full = absolutize(page_url, href).split("#")[0]
        if not _same_org_or_careers_portal(page_url, full) and not _is_ats_url(full):
            continue

        path = urlparse(full).path or ""
        label = _anchor_accessible_text(a)
        norm = _normalize_title(label)
        path_l = path.lower()

        path_listing = bool(_LISTING_PAGE_RE.search(path_l))
        cta = bool(label and _CTA_TEXT_RE.search(label))
        title_hub = bool(norm and _LISTING_TITLE_RE.match(norm))

        if _path_has_non_job_segment(path_l) or _PATH_DENY_RE.search(path_l):
            if not cta:
                continue

        score = 0
        if cta and path_listing:
            score = 500
        elif cta and ("/job" in path_l or "/career" in path_l or "/role" in path_l or "/position" in path_l):
            score = 450
        elif cta:
            score = 400
        elif path_listing:
            score = 300
        elif title_hub and ("/career" in path_l or "/job" in path_l or "/role" in path_l):
            score = 250

        if score <= 0:
            continue

        if score > best_score.get(full, 0):
            best_score[full] = score

    ordered = [u for u, _ in sorted(best_score.items(), key=lambda x: -x[1])]
    return ordered[:max_urls]


def extract_job_links_from_html(page_url: str, html_text: str, *, same_site_only: bool = True) -> list[dict]:
    soup = BeautifulSoup(html_text, "html.parser")
    seen: set[str] = set()
    jobs: list[dict] = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        full = absolutize(page_url, href).split("#")[0]
        if full in seen:
            continue
        if same_site_only and not _same_org_or_careers_portal(page_url, full) and not _is_ats_url(full):
            continue
        path = urlparse(full).path
        title = " ".join((a.get_text() or "").split())
        if not title or len(title) > 200:
            continue
        if not _url_looks_like_job_listing(path, title):
            continue
        seen.add(full)
        jobs.append({"source": "html_link", "title": title, "url": full, "location": None})
    return jobs


def _merge_jobs(jobs: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for j in jobs:
        u = (j.get("url") or "").strip()
        key = u if u else f"{j.get('source')}:{j.get('title')}"
        if key in seen:
            continue
        seen.add(key)
        out.append(j)
    return out


def _collect_from_page(careers_url: str, html_text: str) -> list[dict]:
    jobs: list[dict] = []
    jobs.extend(try_fetch_ats_jobs_from_html(html_text))
    jobs.extend(extract_jobs_from_embedded_json(careers_url, html_text))
    jobs.extend(extract_jsonld_job_postings(careers_url, html_text))
    jobs.extend(extract_job_links_from_html(careers_url, html_text))
    return _merge_jobs(jobs)


def _careers_candidate_score(url: str, base_root: str) -> int:
    """Higher = better default careers landing (avoid /careers/culture, etc.)."""
    if not _same_org_or_careers_portal(base_root, url) and not _is_ats_url(url):
        return -1
    if _is_ats_url(url):
        return 1000
    path = urlparse(url).path.lower().rstrip("/")
    if _path_has_non_job_segment(path) or _PATH_DENY_RE.search(path):
        return -1
    # Exact /careers or /jobs on registrable root (best).
    if path in ("/careers", "/jobs", "/job", "/career"):
        return 900
    if path.endswith("/careers") and path.count("/") <= 2:
        return 850
    if path.endswith("/jobs") and path.count("/") <= 2:
        return 850
    if _LISTING_PAGE_RE.search(path):
        return 750
    if "/current-openings" in path or "/open-positions" in path:
        return 700
    if re.search(r"/(jobs|careers)/[^/]+", path):
        return 400
    if any(
        x in url.lower()
        for x in (
            "/join-us",
            "/work-with-us",
            "/about/careers",
            "/about/jobs",
        )
    ):
        return 350
    return -1


def discover_careers_url(website_root: str) -> str | None:
    """Return a careers or ATS URL reachable from the company homepage."""
    root = normalize_website(website_root)
    if not root:
        return None
    with client() as c:
        try:
            home = c.get(root)
        except Exception:
            return None
        if home.status_code != 200:
            return None
        text = home.text or ""
        soup = BeautifulSoup(text, "html.parser")

        # 1) Embedded ATS links (most reliable).
        if ATS_HINTS.search(text):
            best: tuple[int, str] | None = None
            for a in soup.select("a[href]"):
                href = (a.get("href") or "").strip()
                if not href or href.startswith("#"):
                    continue
                full = absolutize(root, href).split("#")[0]
                if ATS_HINTS.search(full):
                    sc = _careers_candidate_score(full, root)
                    if sc < 0:
                        continue
                    if best is None or sc > best[0]:
                        best = (sc, full)
            if best:
                return best[1]

        # 2) Guess common paths on the site root first (before random nav links).
        for path in CAREER_PATHS:
            candidate = root.rstrip("/") + path
            try:
                g = c.get(candidate)
            except Exception:
                continue
            if g.status_code == 200 and "text/html" in (g.headers.get("content-type") or "").lower():
                return str(g.url).split("#")[0]

        # 3) Same-site links from the homepage — pick highest-scoring candidate.
        best: tuple[int, str] | None = None
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href or href.startswith("#"):
                continue
            full = absolutize(root, href).split("#")[0]
            low = full.lower()
            if not (
                "/career" in low
                or "/jobs" in low
                or "/join-us" in low
                or "/work-with-us" in low
                or _is_ats_url(full)
            ):
                continue
            sc = _careers_candidate_score(full, root)
            if sc < 0:
                continue
            if best is None or sc > best[0]:
                best = (sc, full)
        if best:
            return best[1]
    return None


def fetch_external_jobs(
    website: str,
    *,
    use_playwright: bool = False,
    playwright_wait_ms: int = 5000,
) -> tuple[str | None, list[dict]]:
    """
    Resolve a careers URL from the public website, then pull jobs via ATS APIs,
    embedded JSON (__NEXT_DATA__, etc.), JSON-LD, strict links, and optional Playwright.
    """
    careers = discover_careers_url(website)
    if not careers:
        return None, []

    direct = try_fetch_ats_jobs(careers)
    if direct:
        return careers, direct

    def load_html(url: str) -> tuple[str, str]:
        """Returns (resolved_page_url, html). Empty html on failure."""
        if use_playwright:
            from scraper.browser import fetch_html_playwright

            final_u, html = fetch_html_playwright(url, wait_ms=playwright_wait_ms)
            if not html:
                return url, ""
            return (final_u or url).split("#")[0], html
        with client() as c:
            try:
                r = c.get(url)
            except Exception:
                return url, ""
            if r.status_code != 200:
                return url, ""
            return str(r.url).split("#")[0], r.text or ""

    final_url, html_text = load_html(careers)
    if not html_text.strip():
        return careers, []

    jobs = _collect_from_page(final_url, html_text)

    for extra in _hub_follow_urls(final_url, html_text):
        if extra.rstrip("/") == final_url.rstrip("/"):
            continue
        hub_url, hub_html = load_html(extra)
        if hub_html.strip():
            jobs = _merge_jobs(jobs + _collect_from_page(hub_url, hub_html))

    return final_url, jobs
