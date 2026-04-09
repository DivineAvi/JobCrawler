from __future__ import annotations

"""Optional Playwright rendering for JS-heavy career sites."""


def fetch_html_playwright(
    url: str,
    *,
    wait_ms: int = 5000,
    timeout_ms: int = 90_000,
    headless: bool = True,
) -> tuple[str | None, str | None]:
    """
    Load URL in Chromium, wait for JS to populate the DOM.
    Returns (final_url_after_redirects, html) or (None, None) if Playwright is missing or navigation fails.
    Requires: pip install playwright && playwright install chromium
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None, None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(wait_ms)
            return page.url, page.content()
        except Exception:
            return None, None
        finally:
            browser.close()
