#!/usr/bin/env python3
"""
Watch Listing Monitor
Scrapes pre-owned F.P. Journe and De Bethune listings from major watch
marketplaces and sends a daily digest email with photos, titles, prices,
and source links.

Environment variables required:
  RESEND_API_KEY    — Resend.com API key
  RESEND_FROM       — Verified sender, e.g. "Watch Monitor <watch@1916co.com>"
  RECIPIENT_EMAIL   — Destination address (default: hardcoded below)
"""

import argparse
import base64
import json
import logging
import os
import pathlib
import re
import subprocess
import sys
import time
import webbrowser
from dataclasses import dataclass
from datetime import date
from html import escape
from typing import Optional
from urllib.parse import quote_plus

import requests
import resend
from bs4 import BeautifulSoup

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
resend.api_key = os.environ["RESEND_API_KEY"]
RESEND_FROM = os.environ.get("RESEND_FROM", "Watch Monitor <watch@1916co.com>")
RECIPIENT = os.environ.get("RECIPIENT_EMAIL", "you@1916co.com")

# How many result pages to fetch per brand per site (Chrono24 / eBay)
MAX_PAGES = 3

REQUEST_TIMEOUT = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── Data Model ─────────────────────────────────────────────────────────────────
@dataclass
class Listing:
    title: str
    price: str
    image_url: str
    listing_url: str
    source: str
    brand: str  # "FP Journe" | "De Bethune"
    reference_number: str = ""  # e.g. "FPJ-39-RG" — populated where available
    first_seen_at: str = ""     # "Apr 6, 2026" — populated from Supabase after upsert

    def dedup_key(self) -> str:
        """Stable key for deduplication across scrapers."""
        return self.listing_url.split("?")[0].rstrip("/")


@dataclass
class AuctionLot:
    title: str          # "F.P. Journe Centigraphe Souverain"
    estimate: str       # "$80,000 – $160,000"
    sale_date: str      # "April 8, 2026"
    sale_location: str  # "New York" | "Geneva" | "Hong Kong"
    image_url: str
    lot_url: str
    brand: str          # "FP Journe" | "De Bethune"


def detect_brand(text: str) -> Optional[str]:
    """Return 'FP Journe' or 'De Bethune' if the text matches, else None."""
    t = text.lower()
    if "journe" in t:
        return "FP Journe"
    if "bethune" in t or "de bethune" in t:
        return "De Bethune"
    return None


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch(url: str, session: requests.Session, **kwargs) -> Optional[requests.Response]:
    """GET with timeout and graceful failure."""
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
        resp.raise_for_status()
        return resp
    except Exception as exc:
        log.warning("Fetch failed [%s]: %s", url[:80], exc)
        return None


def abs_url(href: str, base: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("//"):
        return "https:" + href
    return base.rstrip("/") + "/" + href.lstrip("/")


def best_img(tag) -> str:
    if tag is None:
        return ""
    for attr in ("data-src", "data-lazy-src", "data-original", "src"):
        val = tag.get(attr, "")
        if val and val.startswith(("http", "//")):
            return val if not val.startswith("//") else "https:" + val
    return ""


# ── Chrono24 ───────────────────────────────────────────────────────────────────
def scrape_chrono24(session: requests.Session) -> list[Listing]:
    """
    Chrono24 uses Cloudflare bot protection — requests-based approaches get
    a 403 before any HTML is served. Playwright + playwright-stealth runs a real
    browser JS engine and patches all webdriver detection signals.

    Search URL: /search/index.htm?dosearch=true&query={brand}&sortorder=5&pageSize=120
    Listing card selector: .js-listing-item-link (120 per page)
    Card container:        .wt-search-result / .listing-item
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
        from playwright_stealth import Stealth
    except ImportError as e:
        log.warning("Chrono24: missing dependency (%s) — skipping", e)
        return []

    listings: list[Listing] = []
    queries = [
        ("FP Journe",  "F.P. Journe"),
        ("De Bethune", "De Bethune"),
    ]
    pw_stealth = Stealth()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        for brand, query in queries:
            ctx = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                viewport={"width": 1440, "height": 900},
                locale="en-US",
            )
            page = ctx.new_page()
            pw_stealth.apply_stealth_sync(page)

            url = (
                "https://www.chrono24.com/search/index.htm"
                f"?dosearch=true&query={quote_plus(query)}&sortorder=5&pageSize=120"
            )
            try:
                # domcontentloaded is reliable in CI — networkidle can hang
                # indefinitely when the page fires continuous background requests
                # (analytics, Algolia keep-alive). We give JS 6 s to render
                # the listing cards, then wait explicitly for the selector.
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                page.wait_for_timeout(6_000)

                try:
                    page.wait_for_selector(".js-listing-item-link", timeout=45_000)
                except PwTimeout:
                    log.warning(
                        "Chrono24 %s: listing selector not found — page title: %r",
                        brand, page.title(),
                    )
                    ctx.close()
                    continue

                items = page.evaluate("""() => {
                    const seen = new Set();
                    const out = [];
                    document.querySelectorAll(".js-listing-item-link").forEach(a => {
                        const href = a.href;
                        if (!href || seen.has(href)) return;
                        seen.add(href);
                        const card = a.closest(
                            ".wt-search-result, .listing-item, .js-listing-item"
                        ) || a;
                        const img = card.querySelector(
                            ".watch-image img, .listing-item-image img, img"
                        );
                        const texts = Array.from(card.querySelectorAll("*"))
                            .filter(el => el.children.length === 0)
                            .map(el => el.textContent.trim())
                            .filter(t => t.length > 1);
                        out.push({
                            url:    href,
                            imgSrc: img ? (img.src || img.dataset.src || "") : "",
                            texts:  texts,
                        });
                    });
                    return out;
                }""")

                log.info("Chrono24 %s: %d listings", brand, len(items))

                for item in items:
                    texts = item.get("texts", [])

                    # Title: find the text that contains the brand name, then
                    # append the next text (model name) if it isn't a price.
                    title = "Unknown"
                    for i, t in enumerate(texts):
                        if detect_brand(t):
                            parts = [t]
                            if i + 1 < len(texts):
                                nxt = texts[i + 1]
                                if "$" not in nxt and "price" not in nxt.lower() and len(nxt) > 3:
                                    parts.append(nxt)
                            title = " ".join(parts)
                            break

                    price = next(
                        (t for t in texts if "$" in t and any(c.isdigit() for c in t)),
                        next((t for t in texts if "price on request" in t.lower()), "—"),
                    )

                    img_url = item.get("imgSrc", "")
                    # Upgrade to a larger thumbnail for the email digest
                    img_url = img_url.replace("-Square28.", "-Square40.") if img_url else ""

                    listings.append(Listing(
                        title=title,
                        price=price,
                        image_url=img_url,
                        listing_url=item.get("url", ""),
                        source="Chrono24",
                        brand=brand,
                    ))

            except PwTimeout:
                log.warning("Chrono24 timed out for %s", brand)
            except Exception as exc:
                log.error("Chrono24 %s failed: %s", brand, exc)
            finally:
                ctx.close()
            time.sleep(2)

        browser.close()

    return deduplicate(listings)


# ── eBay ───────────────────────────────────────────────────────────────────────
def _ebay_app_token() -> Optional[str]:
    """
    Fetches an OAuth 2.0 application-level access token from eBay.
    Requires EBAY_CLIENT_ID and EBAY_CLIENT_SECRET env vars.
    Returns None (and logs a warning) if credentials are absent.
    """
    client_id     = os.environ.get("EBAY_CLIENT_ID", "")
    client_secret = os.environ.get("EBAY_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        log.warning("EBAY_CLIENT_ID / EBAY_CLIENT_SECRET not set — skipping eBay")
        return None
    creds   = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    scope   = "https://api.ebay.com/oauth/api_scope"
    try:
        resp = requests.post(
            "https://api.ebay.com/identity/v1/oauth2/token",
            headers={
                "Content-Type":  "application/x-www-form-urlencoded",
                "Authorization": f"Basic {creds}",
            },
            data=f"grant_type=client_credentials&scope={quote_plus(scope)}",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]
    except Exception as exc:
        log.error("eBay token fetch failed: %s", exc)
        return None


def scrape_ebay(session: requests.Session) -> list[Listing]:
    """
    eBay Browse API — official JSON endpoint, no HTML parsing, no bot blocks.
    Returns up to 200 listings per brand query (eBay's max per request).
    Requires free eBay developer account: https://developer.ebay.com
    """
    token = _ebay_app_token()
    if not token:
        return []

    listings: list[Listing] = []
    api_headers = {
        "Authorization":           f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
        "Content-Type":            "application/json",
    }
    queries = [
        ("FP Journe",  "F.P. Journe watch"),
        ("De Bethune", "De Bethune watch"),
    ]
    for brand, query in queries:
        offset = 0
        while offset < MAX_PAGES * 50:
            try:
                resp = requests.get(
                    "https://api.ebay.com/buy/browse/v1/item_summary/search",
                    headers=api_headers,
                    params={
                        "q":            query,
                        "category_ids": "31387",   # Wristwatches
                        "limit":        "50",
                        "offset":       str(offset),
                    },
                    timeout=REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                log.error("eBay Browse API error (%s offset=%d): %s", brand, offset, exc)
                break

            items = data.get("itemSummaries", [])
            log.info("eBay %s offset=%d: %d items", brand, offset, len(items))
            if not items:
                break

            for item in items:
                price_val = item.get("price", {})
                try:
                    price = f"${float(price_val.get('value', 0)):,.0f}"
                except (ValueError, TypeError):
                    price = price_val.get("value", "—")

                listings.append(Listing(
                    title=item.get("title", "Unknown"),
                    price=price,
                    image_url=item.get("image", {}).get("imageUrl", ""),
                    listing_url=item.get("itemWebUrl", ""),
                    source="eBay",
                    brand=brand,
                ))

            offset += 50
            if offset >= data.get("total", 0):
                break
            time.sleep(0.5)

    return deduplicate(listings)


# ── Shopify generic ────────────────────────────────────────────────────────────
def scrape_shopify_store(
    session: requests.Session,
    base_url: str,
    source_name: str,
) -> list[Listing]:
    """
    Shopify's /products.json endpoint is a reliable, structured alternative
    to scraping the HTML. Returns all products matching each search term.
    Works for A Collected Man, Hodinkee Shop, and any other Shopify store.
    """
    listings: list[Listing] = []
    queries = ["fp journe", "f.p. journe", "de bethune"]

    for query in queries:
        page = 1
        while page <= MAX_PAGES:
            url = (
                f"{base_url}/products.json"
                f"?q={quote_plus(query)}&limit=250&page={page}"
            )
            resp = fetch(url, session, headers={"Accept": "application/json"})
            if not resp:
                break
            try:
                data = resp.json()
            except Exception:
                break

            products = data.get("products", [])
            log.info("%s '%s' page %d: %d products", source_name, query, page, len(products))
            if not products:
                break

            for p in products:
                title = p.get("title", "Unknown")
                brand = detect_brand(title)
                if not brand:
                    # Check tags and product_type too
                    tags = " ".join(p.get("tags", []))
                    brand = detect_brand(tags)
                if not brand:
                    continue

                variants = p.get("variants", [])

                # Skip sold-out listings — Shopify sets available=false on
                # every variant when sold; price "0.00" is a secondary signal.
                available = any(v.get("available", False) for v in variants)
                if not available:
                    continue

                price = "—"
                if variants:
                    raw = variants[0].get("price", "")
                    try:
                        price = f"${float(raw):,.0f}"
                    except (ValueError, TypeError):
                        price = str(raw)

                images = p.get("images", [])
                img_url = images[0]["src"] if images else ""

                handle = p.get("handle", "")
                listing_url = f"{base_url}/products/{handle}"

                # SKU is the closest thing Shopify has to a reference number
                ref_num = variants[0].get("sku", "") if variants else ""

                listings.append(Listing(
                    title=title, price=price, image_url=img_url,
                    listing_url=listing_url, source=source_name, brand=brand,
                    reference_number=ref_num or "",
                ))
            page += 1
            time.sleep(0.5)

    return deduplicate(listings)


# ── WatchFinder ────────────────────────────────────────────────────────────────
def scrape_watchfinder(session: requests.Session) -> list[Listing]:
    """
    WatchFinder is a JS/Algolia-rendered SPA — BeautifulSoup returns an empty
    shell. Instead, we call the Algolia Search API directly using credentials
    captured from the page's network traffic (public search-only key).

    Algolia app:  OKFY50YJB0
    Index:        prod-stock-index-us-published-desc
    URL pattern:  /{Brand}/{SeriesSlug}/{ModelSlug}/{ModelId}/item/{StockId}
    """
    from urllib.parse import quote as url_quote

    APP_ID  = "OKFY50YJB0"
    API_KEY = "764287a20e17e2fd10d8dc8bfb1291eb"
    INDEX   = "prod-stock-index-us-published-desc"
    BASE    = "https://www.watchfinder.com"
    ALGOLIA = f"https://{APP_ID}-dsn.algolia.net/1/indexes/{INDEX}/query"
    HEADERS = {
        "X-Algolia-Application-Id": APP_ID,
        "X-Algolia-API-Key":        API_KEY,
        "Content-Type":             "application/json",
    }

    listings: list[Listing] = []
    queries = [
        ("FP Journe",  "F.P. Journe"),
        ("De Bethune", "De Bethune"),
    ]

    for brand, query in queries:
        page = 0
        while True:
            try:
                resp = session.post(
                    ALGOLIA,
                    headers=HEADERS,
                    json={"query": query, "hitsPerPage": 100, "page": page},
                    timeout=REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                log.warning("WatchFinder Algolia error (%s page %d): %s", brand, page, exc)
                break

            hits     = data.get("hits", [])
            nb_pages = data.get("nbPages", 1)
            log.info("WatchFinder %s page %d: %d hits", brand, page, len(hits))

            for h in hits:
                brand_name  = h.get("Brand", "")
                series      = h.get("Series", "")
                series_slug = h.get("SeriesSlug", series)
                model_slug  = h.get("ModelSlug", "")
                model_id    = h.get("ModelId", "")
                stock_id    = h.get("StockId", "")
                model_num   = h.get("ModelNumber", "")

                title = f"{brand_name} {series} {model_num}".strip()

                price_on_app = h.get("PriceOnApplication", False)
                sales_price  = h.get("SalesPrice", 0)
                price = "POA" if price_on_app else (
                    f"${sales_price:,.0f}" if sales_price else "—"
                )

                # Each path segment URL-encoded individually
                listing_url = (
                    f"{BASE}/{url_quote(brand_name, safe='.')}"
                    f"/{url_quote(series_slug, safe='')}"
                    f"/{url_quote(model_slug, safe='')}"
                    f"/{model_id}/item/{stock_id}"
                )

                listings.append(Listing(
                    title=title,
                    price=price,
                    image_url=h.get("Image", ""),
                    listing_url=listing_url,
                    source="WatchFinder",
                    brand=brand,
                    reference_number=model_num or "",
                ))

            page += 1
            if page >= nb_pages:
                break
            time.sleep(0.5)

    return deduplicate(listings)


# ── European Watch Company ─────────────────────────────────────────────────────
def _ewc_parse_next_json(html: str) -> list[dict]:
    """
    EWC runs on Next.js RSC (React Server Components). Watch data is serialised
    as JSON objects inside self.__next_f.push() script tags, with internal
    double-quotes escaped as \".  We un-escape, locate every object that
    contains a 'stock_number' key, and return the parsed dicts.
    """
    # Un-escape the JS string layer so we can use json.loads reliably
    text = html.replace('\\"', '"').replace("\\'", "'").replace('\\\\', '\\')

    results: list[dict] = []
    seen: set[int] = set()

    for m in re.finditer(r'"stock_number"\s*:\s*(\d+)', text):
        stock_num = int(m.group(1))
        if stock_num in seen:
            continue

        # Walk backwards from the match to find the opening '{'
        start = m.start()
        while start > 0 and text[start] != '{':
            start -= 1
        if text[start] != '{':
            continue

        # Match braces forward (ignoring chars inside strings)
        depth, in_str, esc, end = 0, False, False, start
        for i in range(start, min(start + 20_000, len(text))):
            c = text[i]
            if esc:
                esc = False
                continue
            if c == '\\' and in_str:
                esc = True
                continue
            if c == '"':
                in_str = not in_str
            elif not in_str:
                if c == '{':
                    depth += 1
                elif c == '}':
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break

        try:
            obj = json.loads(text[start:end])
        except json.JSONDecodeError:
            continue

        if isinstance(obj, dict) and "web_price" in obj:
            seen.add(stock_num)
            results.append(obj)

    return results


# Statuses EWC uses for unavailable inventory
_EWC_SKIP_STATUSES = {"SOLD", "ARCHIVED", "DELETED", "HOLD"}


def scrape_european_watch_co(session: requests.Session) -> list[Listing]:
    """
    EWC embeds structured JSON in Next.js RSC script tags — no Playwright needed.
    Uses brand page for FP Journe and the search endpoint for De Bethune.
    Skips sold / archived / on-hold pieces.
    """
    BASE = "https://www.europeanwatch.com"
    urls = [
        ("FP Journe",  f"{BASE}/brand/f-p-journe"),
        ("De Bethune", f"{BASE}/search?search=de%20bethune"),
    ]
    listings: list[Listing] = []

    for brand, url in urls:
        resp = fetch(url, session)
        if not resp:
            continue

        raw = _ewc_parse_next_json(resp.text)
        log.info("European Watch Co %s: %d raw items", brand, len(raw))

        for item in raw:
            status = (item.get("status") or "").upper()
            if status in _EWC_SKIP_STATUSES:
                continue

            title = f"{item.get('brand', '')} {item.get('model', '')}".strip()
            detected = detect_brand(title)
            if not detected:
                continue

            price_num = item.get("web_price") or 0
            price = f"${price_num:,.0f}" if price_num else "—"

            images = item.get("images") or []
            img_url = images[0] if images else ""

            slug = item.get("slug", "")
            listing_url = f"{BASE}/watch/{slug}" if slug else BASE

            # EWC stores the manufacturer reference number under several possible keys
            ref_num = (
                str(item.get("reference_number") or "")
                or str(item.get("reference") or "")
                or str(item.get("model_reference") or "")
            )

            listings.append(Listing(
                title=title, price=price, image_url=img_url,
                listing_url=listing_url, source="European Watch Co.", brand=detected,
                reference_number=ref_num,
            ))

        time.sleep(1)

    return deduplicate(listings)


# ── WristCheck ─────────────────────────────────────────────────────────────────
def scrape_wristcheck(session: requests.Session) -> list[Listing]:
    """
    WristCheck (wristcheck.com) is JS-rendered — Playwright collects listing URLs
    and cover images from the brand page. The accurate watch title is pulled from
    each listing's server-rendered <title> tag via a lightweight requests.get().

    Brand pages:
      https://wristcheck.com/us/buy/f-p-journe
      https://wristcheck.com/us/buy/de-bethune
    Individual listings: /us/buy/{brand}/{model-slug}

    Server-rendered title format:
      "F.P. Journe Octa Automatique ... - Make an offer | Wristcheck"
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
    except ImportError:
        log.warning("Playwright not installed — skipping WristCheck.")
        return []

    listings: list[Listing] = []
    pages_to_visit = [
        ("FP Journe",  "https://wristcheck.com/us/buy/f-p-journe"),
        ("De Bethune", "https://wristcheck.com/us/buy/de-bethune"),
    ]

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=HEADERS["User-Agent"])
        page = ctx.new_page()

        for brand, url in pages_to_visit:
            try:
                page.goto(url, wait_until="networkidle", timeout=45_000)
                # WristCheck listing cards link to /us/buy/{brand}/{model-slug}
                page.wait_for_selector('a[href*="/us/buy/"]', timeout=20_000)

                items = page.evaluate("""() => {
                    const seen = new Set();
                    const results = [];
                    document.querySelectorAll('a[href*="/us/buy/"]').forEach(a => {
                        const href = a.href;
                        // Skip brand root links — only want model-level pages (2+ segments after /us/buy/)
                        const parts = href.split('/us/buy/')[1] || '';
                        if (!parts.includes('/')) return;
                        if (seen.has(href)) return;
                        seen.add(href);
                        const card = a.closest('li, article, [class*="card"], [class*="item"], [class*="listing"]') || a;
                        const img = card.querySelector('img');
                        const texts = [];
                        card.querySelectorAll('*').forEach(el => {
                            if (el.children.length === 0) {
                                const t = el.textContent.trim();
                                if (t) texts.push(t);
                            }
                        });
                        results.push({
                            url:      href,
                            imageUrl: img ? (img.src || img.dataset.src || '') : '',
                            texts:    texts,
                        });
                    });
                    return results;
                }""")

                log.info("WristCheck %s: %d listings found", brand, len(items))

                for item in items:
                    listing_url = item.get("url", "")
                    texts = item.get("texts", [])

                    # Fetch the individual listing page to get the server-rendered title.
                    # WristCheck SSR populates <title> with the full model name.
                    title = "Unknown"
                    if listing_url:
                        detail = fetch(listing_url, session)
                        if detail:
                            m = re.search(
                                r"<title>(.*?)</title>",
                                detail.text,
                                re.DOTALL | re.IGNORECASE,
                            )
                            if m:
                                raw = m.group(1).strip()
                                # Strip " - Make an offer | Wristcheck" and similar suffixes
                                title = re.sub(
                                    r"\s*[-–]\s*(Make an offer[^|]*)\s*\|.*$",
                                    "",
                                    raw,
                                    flags=re.IGNORECASE,
                                ).strip()
                        time.sleep(0.3)

                    # Fall back to card text if title fetch failed
                    if title == "Unknown":
                        title = next((t for t in texts if len(t) > 6), "Unknown")

                    price = next(
                        (t for t in texts if "$" in t and any(c.isdigit() for c in t)),
                        "—",
                    )
                    listings.append(Listing(
                        title=title,
                        price=price,
                        image_url=item.get("imageUrl", ""),
                        listing_url=listing_url,
                        source="WristCheck",
                        brand=brand,
                    ))

            except PwTimeout:
                log.warning("WristCheck timed out for %s", brand)
            except Exception as exc:
                log.warning("WristCheck error for %s: %s", brand, exc)

        browser.close()

    return deduplicate(listings)


# ── Bezel ──────────────────────────────────────────────────────────────────────
def scrape_bezel(session: requests.Session) -> list[Listing]:
    """
    Bezel (shop.getbezel.com) two-phase approach:

    Phase 1 — Playwright loads the explore pages and collects model-page URLs.
      Explore pages: /explore/fp-journe, /explore/de-bethune
      Model pages end with /id-{model_id}  (one page per reference/watch model)

    Phase 2 — Each model page is fetched statically (no Playwright needed).
      Bezel embeds full listing data in a <script id="__NEXT_DATA__"> tag:
        props.pageProps.listings[]
          .id             → individual listing ID
          .model.name     → model name (e.g. "Octa Automatique Reserve Lune")
          .manufactureYear→ year (e.g. 2024)
          .priceCents     → price in cents
          .status         → "PUBLISHED" = active
          .active         → bool
          .images[]       → listing-specific photos (bunnyUrl / cloudinaryUrl)

    Individual listing URL: /watches/{brand}/{model}/ref-{ref}/listing/id-{listing_id}
    Full title:  "{year} {Brand Display} {model.name}"
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
    except ImportError:
        log.warning(
            "Playwright not installed — skipping Bezel. "
            "Run: pip install playwright && playwright install chromium"
        )
        return []

    BASE = "https://shop.getbezel.com"
    explore_pages = [
        ("FP Journe",  f"{BASE}/explore/fp-journe",  "F.P. Journe"),
        ("De Bethune", f"{BASE}/explore/de-bethune", "De Bethune"),
    ]

    # ── Phase 1: collect model-page URLs via Playwright ──────────────────────
    # Each entry: (brand_key, brand_display, model_page_url)
    model_pages: list[tuple[str, str, str]] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=HEADERS["User-Agent"])
        page = ctx.new_page()

        for brand, explore_url, brand_display in explore_pages:
            try:
                page.goto(explore_url, wait_until="networkidle", timeout=45_000)
                page.wait_for_selector('a[href*="/watches/"]', timeout=20_000)

                hrefs = page.evaluate("""() => {
                    const seen = new Set();
                    const results = [];
                    document.querySelectorAll('a[href*="/watches/"]').forEach(a => {
                        const href = a.href;
                        // Model pages end with /id-{number} (not /listing/id-{number})
                        if (!/\\/id-\\d+$/.test(href)) return;
                        if (seen.has(href)) return;
                        seen.add(href);
                        results.push(href);
                    });
                    return results;
                }""")

                log.info("Bezel explore %s: %d model pages", brand, len(hrefs))
                for href in hrefs:
                    model_pages.append((brand, brand_display, href))

            except PwTimeout:
                log.warning("Bezel explore timed out for %s", brand)
            except Exception as exc:
                log.warning("Bezel explore error for %s: %s", brand, exc)

        browser.close()

    # ── Phase 2: parse __NEXT_DATA__ from each model page ────────────────────
    listings: list[Listing] = []

    for brand, brand_display, model_url in model_pages:
        resp = fetch(model_url, session)
        if not resp:
            continue

        try:
            m = re.search(
                r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                resp.text,
                re.DOTALL,
            )
            if not m:
                log.warning("Bezel: no __NEXT_DATA__ at %s", model_url)
                continue
            data = json.loads(m.group(1))
            raw_listings = (
                data.get("props", {}).get("pageProps", {}).get("listings", [])
            )
        except Exception as exc:
            log.warning("Bezel: JSON parse error at %s: %s", model_url, exc)
            continue

        # Base URL for constructing individual listing URLs:
        # strip trailing /id-{model_id}, append /listing/id-{listing_id}
        url_base = re.sub(r"/id-\d+$", "", model_url)

        for lst in raw_listings:
            if not lst.get("active") or lst.get("status") != "PUBLISHED":
                continue

            listing_id = lst.get("id")
            if not listing_id:
                continue

            model_info = lst.get("model", {})
            model_name = model_info.get("name", "")
            year = lst.get("manufactureYear", "")

            # Build full title: "2024 F.P. Journe Octa Automatique Reserve Lune"
            title_parts = [str(year) if year else "", brand_display, model_name]
            title = " ".join(p for p in title_parts if p).strip()

            price_cents = lst.get("priceCents") or 0
            price = f"${price_cents / 100:,.0f}" if price_cents else "—"

            # Prefer listing-specific images; fall back to model-level images
            img_url = ""
            for img_obj in lst.get("images", []):
                img = img_obj.get("image", {})
                img_url = (
                    img.get("bunnyUrl")
                    or img.get("cloudinaryUrl")
                    or img.get("rawUrl", "")
                )
                if img_url:
                    break
            if not img_url:
                for img_obj in model_info.get("images", []):
                    img_url = (
                        img_obj.get("bunnyUrl")
                        or img_obj.get("cloudinaryUrl")
                        or img_obj.get("url", "")
                    )
                    if img_url:
                        break

            listing_url = f"{url_base}/listing/id-{listing_id}"
            ref_num = str(lst.get("referenceNumber") or model_info.get("referenceNumber") or "")

            listings.append(Listing(
                title=title,
                price=price,
                image_url=img_url,
                listing_url=listing_url,
                source="Bezel",
                brand=brand,
                reference_number=ref_num,
            ))

        time.sleep(0.5)

    log.info("Bezel total: %d individual listings", len(listings))
    return deduplicate(listings)


# ── 1stDibs ────────────────────────────────────────────────────────────────────
def scrape_1stdibs(session: requests.Session) -> list[Listing]:
    listings: list[Listing] = []
    BASE = "https://www.1stdibs.com"
    queries = [
        ("FP Journe",  f"{BASE}/jewelry/watches/watches/?q=fp+journe"),
        ("De Bethune", f"{BASE}/jewelry/watches/watches/?q=de+bethune"),
    ]
    for brand, url in queries:
        resp = fetch(url, session)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "lxml")
        cards = soup.select(
            "[data-tn='search-result-item'], "
            "[class*='item-tile'], [class*='product-tile']"
        )
        log.info("1stDibs %s: %d cards", brand, len(cards))
        for card in cards:
            a = card.find("a", href=True)
            if not a:
                continue
            href = abs_url(a["href"], BASE)
            title_el = card.select_one(
                "[data-tn='item-title'], h2, h3, [class*='title']"
            )
            title = title_el.get_text(" ", strip=True) if title_el else "Unknown"
            price_el = card.select_one("[data-tn='item-price'], [class*='price']")
            price = price_el.get_text(" ", strip=True) if price_el else "—"
            img_url = best_img(card.find("img"))
            listings.append(Listing(
                title=title, price=price, image_url=img_url,
                listing_url=href, source="1stDibs", brand=brand,
            ))
        time.sleep(1)
    return deduplicate(listings)


# ── Watches of Switzerland ─────────────────────────────────────────────────────
def scrape_watches_of_switzerland(session: requests.Session) -> list[Listing]:
    """WoS pre-owned section."""
    listings: list[Listing] = []
    BASE = "https://www.watchesofswitzerland.com"
    queries = [
        ("FP Journe",  f"{BASE}/search?q=fp+journe"),
        ("De Bethune", f"{BASE}/search?q=de+bethune"),
    ]
    for brand, url in queries:
        resp = fetch(url, session)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "lxml")
        cards = soup.select("[class*='product'], [class*='item-tile'], article")
        log.info("WoS %s: %d cards", brand, len(cards))
        for card in cards:
            a = card.find("a", href=True)
            if not a:
                continue
            href = abs_url(a["href"], BASE)
            title_el = card.select_one("h2, h3, [class*='title'], [class*='name']")
            title = title_el.get_text(" ", strip=True) if title_el else "Unknown"
            price_el = card.select_one("[class*='price'], .money")
            price = price_el.get_text(" ", strip=True) if price_el else "—"
            img_url = best_img(card.find("img"))
            listings.append(Listing(
                title=title, price=price, image_url=img_url,
                listing_url=href, source="Watches of Switzerland", brand=brand,
            ))
        time.sleep(1)
    return deduplicate(listings)


# ── Phillips Auction ───────────────────────────────────────────────────────────
_PHILLIPS_LOCATIONS = {"NY": "New York", "CH": "Geneva", "HK": "Hong Kong",
                       "LO": "London",  "AU": "Auckland"}


def _phillips_sale_location(sale_number: str) -> str:
    return _PHILLIPS_LOCATIONS.get(sale_number[:2].upper(), "Phillips")


def _phillips_parse_date(iso: str) -> str:
    """'2026-04-08T16:00:00+00:00'  →  'April 8, 2026'"""
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(iso)
        return dt.strftime("%B %-d, %Y")   # Linux/Mac
    except ValueError:
        try:
            return dt.strftime("%B %#d, %Y")  # Windows
        except Exception:
            return iso[:10]
    except Exception:
        return iso[:10]


def _phillips_extract_upcoming(html: str) -> list[dict]:
    """
    Phillips artist pages embed all lot data as a JSON string inside a React
    component prop.  The key sequence is always:
      "upcomingLots" … "data" … [  { lot objects }  ]
    Phillips Unicode-escapes quote characters as \\u0022 rather than \\\"
    so we decode all \\uXXXX sequences before searching for lot data.
    """
    text = re.sub(r'\\u([0-9a-fA-F]{4})', lambda m: chr(int(m.group(1), 16)), html)
    text = text.replace('\\"', '"').replace('\\\\', '\\')

    key = '"upcomingLots"'
    start = text.find(key)
    if start == -1:
        return []

    # Find the "data" key inside the upcomingLots object
    data_key = text.find('"data"', start)
    if data_key == -1:
        return []

    arr_start = text.find('[', data_key)
    if arr_start == -1:
        return []

    # Match brackets
    depth, in_str, esc, arr_end = 0, False, False, arr_start
    for i in range(arr_start, min(arr_start + 200_000, len(text))):
        c = text[i]
        if esc:
            esc = False; continue
        if c == '\\' and in_str:
            esc = True; continue
        if c == '"':
            in_str = not in_str
        elif not in_str:
            if c == '[': depth += 1
            elif c == ']':
                depth -= 1
                if depth == 0:
                    arr_end = i + 1; break

    try:
        return json.loads(text[arr_start:arr_end])
    except json.JSONDecodeError:
        return []


def scrape_phillips(session: requests.Session) -> list[AuctionLot]:
    """
    Phillips artist pages for FP Journe and De Bethune.
    Data is server-embedded JSON — no Playwright needed.
    Only returns lots where isSaleOver is false (upcoming).
    """
    BASE = "https://www.phillips.com"
    pages = [
        ("FP Journe",  f"{BASE}/artist/13096/f-p-journe"),
        ("De Bethune", f"{BASE}/artist/13224/de-bethune"),
    ]
    lots: list[AuctionLot] = []

    for brand, url in pages:
        resp = fetch(url, session)
        if not resp:
            continue

        raw = _phillips_extract_upcoming(resp.text)
        log.info("Phillips %s: %d upcoming lot(s)", brand, len(raw))

        for lot in raw:
            if lot.get("isSaleOver", True):
                continue

            maker   = lot.get("makerName", "")
            model   = lot.get("wModelName", "")
            title   = f"{maker} {model}".strip()

            low     = lot.get("lowEstimate", 0)
            high    = lot.get("highEstimate", 0)
            sign    = lot.get("currencySign", "$")
            estimate = (
                f"{sign}{int(low):,} – {sign}{int(high):,}"
                if low and high else "—"
            )

            sale_num  = lot.get("saleNumber", "")
            location  = _phillips_sale_location(sale_num)
            iso_date  = lot.get("auctionStartDateTimeOffset", "")
            sale_date = _phillips_parse_date(iso_date) if iso_date else "—"

            detail   = lot.get("detailLink", "")
            lot_url  = detail if detail.startswith("http") else BASE + detail

            img_path = lot.get("imagePath", "")
            if img_path.startswith("//"):
                img_url = "https:" + img_path
            elif img_path.startswith("/"):
                img_url = BASE + img_path
            else:
                img_url = img_path

            lots.append(AuctionLot(
                title=title, estimate=estimate,
                sale_date=sale_date, sale_location=location,
                image_url=img_url, lot_url=lot_url, brand=brand,
            ))

        time.sleep(1)

    return lots


# ── Price parsing helper ───────────────────────────────────────────────────────
def _parse_price_amount(price_str: str) -> Optional[float]:
    """
    Convert a human-readable price string to a float for DB storage.
    "$42,500"   → 42500.0
    "$1,200,000"→ 1200000.0
    "POA" / "—" → None
    """
    if not price_str:
        return None
    cleaned = re.sub(r"[^\d.]", "", price_str)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


# ── Supabase persistence ───────────────────────────────────────────────────────
def save_to_supabase(listings: list[Listing]) -> None:
    """
    Upsert all active listings to Supabase, record price history on changes,
    and mark any previously-active listings that were NOT seen this run as inactive.

    Table schema (run once in the Supabase SQL editor):
    ─────────────────────────────────────────────────────────────────────────────
    CREATE TABLE listings (
        url              TEXT PRIMARY KEY,
        source           TEXT NOT NULL,
        brand            TEXT NOT NULL,
        title            TEXT,
        reference_number TEXT,
        image_url        TEXT,
        price            TEXT,
        price_amount     NUMERIC,
        first_seen_at    TIMESTAMPTZ DEFAULT NOW(),
        last_seen_at     TIMESTAMPTZ DEFAULT NOW(),
        is_active        BOOLEAN DEFAULT TRUE
    );

    CREATE TABLE price_history (
        id           BIGSERIAL PRIMARY KEY,
        listing_url  TEXT NOT NULL REFERENCES listings(url),
        price        TEXT NOT NULL,
        price_amount NUMERIC,
        scraped_at   TIMESTAMPTZ DEFAULT NOW()
    );
    ─────────────────────────────────────────────────────────────────────────────
    """
    url_env = os.environ.get("SUPABASE_URL", "")
    key_env = os.environ.get("SUPABASE_KEY", "")
    if not url_env or not key_env:
        log.warning("SUPABASE_URL / SUPABASE_KEY not set — skipping DB save")
        return

    try:
        from supabase import create_client
    except ImportError:
        log.warning("supabase package not installed — skipping DB save")
        return

    try:
        sb = create_client(url_env, key_env)
    except Exception as exc:
        log.error("Supabase client init failed: %s", exc)
        return

    # ── 1. Fetch current state of all listings in DB ───────────────────────────
    try:
        existing_resp = sb.table("listings").select("url, price, is_active, first_seen_at").execute()
        existing: dict[str, dict] = {
            row["url"]: row for row in (existing_resp.data or [])
        }
    except Exception as exc:
        log.error("Supabase fetch existing listings failed: %s", exc)
        existing = {}

    # ── 2. Upsert each listing ──────────────────────────────────────────────────
    seen_urls: set[str] = set()
    price_history_rows: list[dict] = []

    for lst in listings:
        url_key = lst.dedup_key()
        seen_urls.add(url_key)
        price_amount = _parse_price_amount(lst.price)

        row = {
            "url":              url_key,
            "source":           lst.source,
            "brand":            lst.brand,
            "title":            lst.title,
            "reference_number": lst.reference_number or None,
            "image_url":        lst.image_url or None,
            "price":            lst.price,
            "price_amount":     price_amount,
            "last_seen_at":     "now()",
            "is_active":        True,
        }

        try:
            sb.table("listings").upsert(row, on_conflict="url").execute()
        except Exception as exc:
            log.warning("Supabase upsert failed for %s: %s", url_key[:60], exc)
            continue

        # Populate first_seen_at on the listing object for use in the email
        prev = existing.get(url_key)
        if prev and prev.get("first_seen_at"):
            # Parse ISO timestamp from DB → "Apr 6, 2026"
            try:
                from datetime import datetime, timezone
                dt = datetime.fromisoformat(prev["first_seen_at"].replace("Z", "+00:00"))
                lst.first_seen_at = f"{dt.strftime('%b')} {dt.day}, {dt.year}"
            except Exception:
                lst.first_seen_at = prev["first_seen_at"][:10]
        else:
            # Brand-new listing — first seen today
            today = date.today()
            lst.first_seen_at = f"{today.strftime('%b')} {today.day}, {today.year}"

        # Record price history when price changes (or listing is brand-new)
        if prev is None or prev.get("price") != lst.price:
            price_history_rows.append({
                "listing_url":  url_key,
                "price":        lst.price,
                "price_amount": price_amount,
            })

    # ── 3. Bulk-insert price history rows ──────────────────────────────────────
    if price_history_rows:
        try:
            sb.table("price_history").insert(price_history_rows).execute()
            log.info("Supabase: recorded %d price history row(s)", len(price_history_rows))
        except Exception as exc:
            log.warning("Supabase price_history insert failed: %s", exc)

    # ── 4. Mark listings not seen this run as inactive ──────────────────────────
    stale_urls = [
        u for u, r in existing.items()
        if r.get("is_active") and u not in seen_urls
    ]
    if stale_urls:
        try:
            sb.table("listings").update({"is_active": False}).in_("url", stale_urls).execute()
            log.info("Supabase: marked %d listing(s) inactive", len(stale_urls))
        except Exception as exc:
            log.warning("Supabase mark-inactive failed: %s", exc)

    log.info(
        "Supabase: upserted %d listing(s) (%d price change(s), %d marked inactive)",
        len(seen_urls), len(price_history_rows), len(stale_urls),
    )


# ── Deduplication ──────────────────────────────────────────────────────────────
def deduplicate(listings: list[Listing]) -> list[Listing]:
    seen: set[str] = set()
    out: list[Listing] = []
    for l in listings:
        key = l.dedup_key()
        if key not in seen:
            seen.add(key)
            out.append(l)
    return out


# ── Main Gathering ─────────────────────────────────────────────────────────────
SCRAPERS = [
    ("Chrono24",               scrape_chrono24),
    ("eBay",                   scrape_ebay),
    ("A Collected Man",        lambda s: scrape_shopify_store(
        s, "https://www.acollectedman.com", "A Collected Man"
    )),
    ("Wrist Aficionado",       lambda s: scrape_shopify_store(
        s, "https://wristaficionado.com", "Wrist Aficionado"
    )),
    ("G&G Timepieces",         lambda s: scrape_shopify_store(
        s, "https://gandgtimepieces.com", "G&G Timepieces"
    )),
    ("WatchX NYC",             lambda s: scrape_shopify_store(
        s, "https://watchxnyc.com", "WatchX NYC"
    )),
    ("Hodinkee Shop",          lambda s: scrape_shopify_store(
        s, "https://shop.hodinkee.com", "Hodinkee Shop"
    )),
    ("WatchFinder",            scrape_watchfinder),
    ("European Watch Co.",     scrape_european_watch_co),
    ("WristCheck",             scrape_wristcheck),
    ("Bezel",                  scrape_bezel),
    ("1stDibs",                scrape_1stdibs),
    ("Watches of Switzerland", scrape_watches_of_switzerland),
]


def gather_all(
    only_source: Optional[str] = None,
) -> tuple[list[Listing], list[AuctionLot], list[dict]]:
    """
    Run all scrapers (or just one if only_source is given).
    only_source is matched case-insensitively as a substring, e.g. "chrono" matches "Chrono24".
    Returns (listings, auction_lots, stats).
    """
    session = make_session()
    all_listings: list[Listing] = []
    auction_lots: list[AuctionLot] = []
    stats: list[dict] = []

    for name, scraper_fn in SCRAPERS:
        if only_source and only_source.lower() not in name.lower():
            continue
        log.info("── Scraping %s …", name)
        try:
            results = scraper_fn(session)
            new = [r for r in results if r.dedup_key() not in {x.dedup_key() for x in all_listings}]
            all_listings.extend(new)
            stats.append({
                "source": name,
                "count":  len(results),
                "fpj":    sum(1 for r in results if r.brand == "FP Journe"),
                "db":     sum(1 for r in results if r.brand == "De Bethune"),
                "error":  None,
            })
            log.info("   → %d listings (%d after global dedup)", len(results), len(new))
        except Exception as exc:
            log.error("Scraper %s crashed: %s", name, exc)
            stats.append({"source": name, "count": 0, "fpj": 0, "db": 0, "error": str(exc)})

    # Phillips runs separately — returns AuctionLot, not Listing
    if not only_source or "phillips" in (only_source or "").lower():
        log.info("── Scraping Phillips …")
        try:
            lots = scrape_phillips(session)
            auction_lots.extend(lots)
            stats.append({
                "source": "Phillips (upcoming)",
                "count":  len(lots),
                "fpj":    sum(1 for l in lots if l.brand == "FP Journe"),
                "db":     sum(1 for l in lots if l.brand == "De Bethune"),
                "error":  None,
            })
            log.info("   → %d upcoming lot(s)", len(lots))
        except Exception as exc:
            log.error("Phillips scraper crashed: %s", exc)
            stats.append({"source": "Phillips (upcoming)", "count": 0, "fpj": 0, "db": 0, "error": str(exc)})

    return all_listings, auction_lots, stats


def print_console_summary(
    listings: list[Listing], auction_lots: list[AuctionLot], stats: list[dict]
) -> None:
    """Print a readable summary table to stdout — always shown in dev mode."""
    fpj = [l for l in listings if l.brand == "FP Journe"]
    db  = [l for l in listings if l.brand == "De Bethune"]
    w = 60
    print("\n" + "━" * w)
    print(f"  Watch Listings — {date.today():%B %d, %Y}")
    print(f"  {len(listings)} total  ({len(fpj)} FP Journe · {len(db)} De Bethune)")
    print("━" * w)

    for brand_name, brand_listings in [("F.P. Journe", fpj), ("De Bethune", db)]:
        if not brand_listings:
            continue
        print(f"\n  {brand_name} ({len(brand_listings)})")
        print("  " + "─" * (w - 2))
        for l in brand_listings:
            title = l.title if len(l.title) <= 44 else l.title[:41] + "…"
            print(f"  {title:<44}  {l.price.ljust(12)}  {l.source[:18]}")
        print()

    if auction_lots:
        print(f"  Phillips Upcoming ({len(auction_lots)} lot(s))")
        print("  " + "─" * (w - 2))
        for lot in auction_lots:
            title = lot.title if len(lot.title) <= 44 else lot.title[:41] + "…"
            print(f"  {title:<44}  {lot.estimate[:12].ljust(12)}  {lot.sale_date}")
        print()

    print("  Source breakdown:")
    for s in stats:
        status = f"{s['count']} listings" if s["count"] > 0 else "0 listings"
        err    = f"  ⚠ {s['error']}" if s["error"] else ""
        print(f"    {s['source']:<28}  {status}{err}")
    print("━" * w + "\n")


# ── Email ──────────────────────────────────────────────────────────────────────
def listing_table_html(brand_listings: list[Listing]) -> str:
    if not brand_listings:
        return "<p style='color:#999;font-style:italic;margin:8px 0;'>No listings found.</p>"

    rows = []
    for l in brand_listings:
        if l.image_url:
            img_cell = (
                f'<a href="{escape(l.listing_url)}">'
                f'<img src="{escape(l.image_url)}" width="80" height="80" '
                f'style="object-fit:cover;border-radius:4px;display:block;border:0;" '
                f'onerror="this.parentElement.innerHTML=\'&nbsp;\'" /></a>'
            )
        else:
            img_cell = "&nbsp;"

        found_cell = (
            f'<td style="padding:10px 8px;vertical-align:middle;color:#999;font-size:11px;'
            f'white-space:nowrap;">{escape(l.first_seen_at)}</td>'
            if l.first_seen_at else
            f'<td style="padding:10px 8px;vertical-align:middle;color:#ddd;font-size:11px;">—</td>'
        )

        rows.append(
            f'<tr style="border-bottom:1px solid #ede8e0;">'
            f'<td style="padding:10px 8px;width:96px;vertical-align:middle;">{img_cell}</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;">'
            f'  <a href="{escape(l.listing_url)}" '
            f'     style="color:#1a3550;font-weight:600;text-decoration:none;font-size:14px;">'
            f'     {escape(l.title)}</a>'
            f'</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;white-space:nowrap;'
            f'font-weight:700;color:#2a6b2a;font-size:15px;">{escape(l.price)}</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;color:#777;font-size:12px;'
            f'white-space:nowrap;">{escape(l.source)}</td>'
            f'{found_cell}'
            f'</tr>'
        )

    return (
        '<table width="100%" cellpadding="0" cellspacing="0" '
        'style="border-collapse:collapse;font-size:14px;'
        'border:1px solid #e0d8cc;border-radius:6px;overflow:hidden;">'
        '<thead>'
        '<tr style="background:#f8f5f0;">'
        '<th style="padding:9px 8px;text-align:left;font-size:11px;color:#999;'
        'font-weight:600;text-transform:uppercase;letter-spacing:.5px;">Photo</th>'
        '<th style="padding:9px 8px;text-align:left;font-size:11px;color:#999;'
        'font-weight:600;text-transform:uppercase;letter-spacing:.5px;">Model</th>'
        '<th style="padding:9px 8px;text-align:left;font-size:11px;color:#999;'
        'font-weight:600;text-transform:uppercase;letter-spacing:.5px;">Price</th>'
        '<th style="padding:9px 8px;text-align:left;font-size:11px;color:#999;'
        'font-weight:600;text-transform:uppercase;letter-spacing:.5px;">Source</th>'
        '<th style="padding:9px 8px;text-align:left;font-size:11px;color:#999;'
        'font-weight:600;text-transform:uppercase;letter-spacing:.5px;">Date Found</th>'
        '</tr>'
        '</thead>'
        '<tbody>' + "".join(rows) + "</tbody>"
        "</table>"
    )


def auction_table_html(lots: list[AuctionLot]) -> str:
    if not lots:
        return "<p style='color:#999;font-style:italic;margin:8px 0;'>No upcoming lots found.</p>"
    rows = []
    for lot in lots:
        img_cell = (
            f'<a href="{escape(lot.lot_url)}">'
            f'<img src="{escape(lot.image_url)}" width="80" height="80" '
            f'style="object-fit:cover;border-radius:4px;display:block;border:0;" '
            f'onerror="this.parentElement.innerHTML=\'&nbsp;\'" /></a>'
            if lot.image_url else "&nbsp;"
        )
        rows.append(
            f'<tr style="border-bottom:1px solid #e8e4f0;">'
            f'<td style="padding:10px 8px;width:96px;vertical-align:middle;">{img_cell}</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;">'
            f'  <a href="{escape(lot.lot_url)}" '
            f'     style="color:#3a1a55;font-weight:600;text-decoration:none;font-size:14px;">'
            f'     {escape(lot.title)}</a>'
            f'</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;white-space:nowrap;'
            f'font-weight:700;color:#5a3a80;font-size:14px;">{escape(lot.estimate)}</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;font-size:12px;color:#777;">'
            f'  {escape(lot.sale_date)}<br>'
            f'  <span style="color:#aaa;">{escape(lot.sale_location)}</span>'
            f'</td>'
            f'</tr>'
        )
    return (
        '<table width="100%" cellpadding="0" cellspacing="0" '
        'style="border-collapse:collapse;font-size:14px;'
        'border:1px solid #d8d0e8;border-radius:6px;overflow:hidden;">'
        '<thead><tr style="background:#f5f2fa;">'
        '<th style="padding:9px 8px;text-align:left;font-size:11px;color:#999;'
        'font-weight:600;text-transform:uppercase;letter-spacing:.5px;">Photo</th>'
        '<th style="padding:9px 8px;text-align:left;font-size:11px;color:#999;'
        'font-weight:600;text-transform:uppercase;letter-spacing:.5px;">Lot</th>'
        '<th style="padding:9px 8px;text-align:left;font-size:11px;color:#999;'
        'font-weight:600;text-transform:uppercase;letter-spacing:.5px;">Estimate</th>'
        '<th style="padding:9px 8px;text-align:left;font-size:11px;color:#999;'
        'font-weight:600;text-transform:uppercase;letter-spacing:.5px;">Sale Date</th>'
        '</tr></thead>'
        '<tbody>' + "".join(rows) + "</tbody></table>"
    )


def stats_table_html(stats: list[dict], brand: str) -> str:
    """Render the source breakdown table filtered to one brand's counts."""
    brand_key = "fpj" if brand == "FP Journe" else "db"
    rows = []
    for s in stats:
        n = s.get(brand_key, s.get("count", 0))
        color = "#2a6b2a" if n > 0 else "#bbb"
        err = (
            f' <span style="color:#b94040;font-size:11px;">({escape(str(s["error"]))})</span>'
            if s["error"] else ""
        )
        rows.append(
            f'<tr>'
            f'<td style="padding:5px 8px;">{escape(s["source"])}</td>'
            f'<td style="padding:5px 8px;font-weight:600;color:{color};">{n}{err}</td>'
            f'</tr>'
        )
    return (
        '<table cellpadding="0" cellspacing="0" '
        'style="border-collapse:collapse;font-size:12px;color:#555;'
        'border:1px solid #e0e0e0;">'
        '<thead><tr style="background:#f4f4f4;">'
        '<th style="padding:6px 8px;text-align:left;">Source</th>'
        '<th style="padding:6px 8px;text-align:left;">Listings found</th>'
        '</tr></thead>'
        "<tbody>" + "".join(rows) + "</tbody></table>"
    )


# Brand display names used in subjects and headers
BRAND_DISPLAY = {
    "FP Journe":  "F.P. Journe",
    "De Bethune": "De Bethune",
}


def build_email(
    brand: str,                    # "FP Journe" or "De Bethune"
    listings: list[Listing],
    auction_lots: list[AuctionLot],
    stats: list[dict],
) -> tuple[str, str, str]:
    """Build subject + plain + HTML for one brand's email."""
    today        = date.today().strftime("%B %d, %Y")
    display      = BRAND_DISPLAY.get(brand, brand)
    b_listings   = [l for l in listings   if l.brand == brand]
    b_lots       = [l for l in auction_lots if l.brand == brand]
    n_list       = len(b_listings)
    n_lots       = len(b_lots)
    brand_key    = "fpj" if brand == "FP Journe" else "db"
    active_src   = sum(1 for s in stats if s.get(brand_key, s.get("count", 0)) > 0)

    subject = (
        f"{display} Listings — {today} "
        f"({n_list} listing{'s' if n_list != 1 else ''}"
        + (f" · {n_lots} auction lot{'s' if n_lots != 1 else ''}" if n_lots else "")
        + ")"
    )

    # ── Plain text ───────────────────────────────────────────────────────────
    lines = [f"{display} Listings — {today}", f"{n_list} listing(s)\n"]
    for l in b_listings:
        lines.append(f"  {l.title}")
        found = f" · found {l.first_seen_at}" if l.first_seen_at else ""
        lines.append(f"  {l.price} | {l.source}{found}")
        lines.append(f"  {l.listing_url}\n")
    if b_lots:
        lines.append(f"── Phillips Upcoming ({n_lots}) ──")
        for lot in b_lots:
            lines.append(f"  {lot.title}")
            lines.append(f"  Est. {lot.estimate} | {lot.sale_date} · {lot.sale_location}")
            lines.append(f"  {lot.lot_url}\n")
    plain = "\n".join(lines)

    # ── HTML ─────────────────────────────────────────────────────────────────
    count_str  = f"{n_list} listing{'s' if n_list != 1 else ''}"
    lots_str   = (
        f' &nbsp;+&nbsp; <span style="color:#3a1a55;">'
        f'{n_lots} Phillips lot{"s" if n_lots != 1 else ""}</span>'
        if n_lots else ""
    )

    auction_html = ""
    if b_lots:
        auction_html = (
            f'<div style="margin-top:40px;">'
            f'<h3 style="margin:0 0 10px;color:#3a1a55;font-size:18px;'
            f'border-bottom:2px solid #d8d0e8;padding-bottom:6px;">'
            f'Upcoming at Auction — Phillips '
            f'<span style="font-size:13px;font-weight:normal;color:#aaa;">'
            f'{n_lots} lot{"s" if n_lots != 1 else ""}</span></h3>'
            + auction_table_html(b_lots)
            + '</div>'
        )

    html = f"""<!DOCTYPE html>
<html>
<body style="font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;
             max-width:900px;margin:auto;color:#222;padding:24px;background:#fff;">

  <div style="border-bottom:3px solid #1a3550;padding-bottom:14px;margin-bottom:4px;">
    <h2 style="margin:0;font-size:24px;color:#1a3550;letter-spacing:-.3px;">
      {escape(display)} Listings
    </h2>
    <p style="margin:5px 0 0;color:#999;font-size:13px;">
      {escape(today)} &mdash;
      <strong style="color:#444;">{count_str}</strong>{lots_str}
      across <strong style="color:#444;">{active_src}</strong> source{'s' if active_src != 1 else ''}
    </p>
  </div>

  <h3 style="margin:24px 0 10px;color:#1a3550;font-size:18px;
             border-bottom:2px solid #e0d8cc;padding-bottom:6px;">
    {escape(display)}
    <span style="font-size:13px;font-weight:normal;color:#aaa;">{count_str}</span>
  </h3>
  {listing_table_html(b_listings)}
  {auction_html}

  <div style="margin-top:40px;border-top:1px solid #e8e8e8;padding-top:16px;">
    <p style="font-size:12px;color:#bbb;margin:0 0 8px;">Source breakdown</p>
    {stats_table_html(stats, brand)}
  </div>

</body>
</html>"""

    return subject, plain, html


def send_emails(
    listings: list[Listing], auction_lots: list[AuctionLot], stats: list[dict]
) -> None:
    """Send one email per brand — F.P. Journe first, then De Bethune."""
    for brand in ("FP Journe", "De Bethune"):
        subject, plain, html = build_email(brand, listings, auction_lots, stats)
        resend.Emails.send({
            "from": RESEND_FROM,
            "to":   RECIPIENT,
            "subject": subject,
            "html": html,
            "text": plain,
        })
        b_count = sum(1 for l in listings    if l.brand == brand)
        l_count = sum(1 for l in auction_lots if l.brand == brand)
        log.info("Email sent [%s] → %s | %d listing(s), %d lot(s)",
                 brand, RECIPIENT, b_count, l_count)


# ── Entry Point ────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Watch Listing Monitor — FP Journe & De Bethune pre-owned tracker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper.py                        # full run + send email
  python scraper.py --preview              # full run, open browser, no email
  python scraper.py --no-email             # full run, console summary, no email
  python scraper.py --source chrono24      # one source only, console summary
  python scraper.py --source ebay --preview  # one source, open browser
  python scraper.py --list-sources         # print available source names and exit
        """,
    )
    p.add_argument(
        "--preview", "-p",
        action="store_true",
        help="Save output to preview.html and open in browser. Skips email.",
    )
    p.add_argument(
        "--no-email",
        action="store_true",
        help="Run all scrapers and print console summary, but do not send email.",
    )
    p.add_argument(
        "--source", "-s",
        metavar="NAME",
        default=None,
        help=(
            "Run only the scraper whose name contains NAME (case-insensitive). "
            "Implies --no-email unless --preview is also set. "
            "Use --list-sources to see available names."
        ),
    )
    p.add_argument(
        "--list-sources",
        action="store_true",
        help="Print all available source names and exit.",
    )
    p.add_argument(
        "--out",
        metavar="FILE",
        default="preview.html",
        help="Path to write the preview HTML file (default: preview.html).",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.list_sources:
        print("Available sources (use any substring with --source):")
        for name, _ in SCRAPERS:
            print(f"  {name}")
        sys.exit(0)

    # --source alone implies no email (dev / debug mode)
    skip_email = args.no_email or args.preview or (args.source is not None)

    # Resend is not needed if we're not sending — skip the env check
    if skip_email:
        resend.api_key = os.environ.get("RESEND_API_KEY", "preview-mode-no-key-needed")

    log.info("Watch Listing Monitor starting%s…",
             f" [source={args.source}]" if args.source else "")

    listings, auction_lots, stats = gather_all(only_source=args.source)

    fpj_count = sum(1 for l in listings if l.brand == "FP Journe")
    db_count  = sum(1 for l in listings if l.brand == "De Bethune")
    log.info("Total: %d listings (%d FPJ, %d DB) + %d Phillips lot(s)",
             len(listings), fpj_count, db_count, len(auction_lots))

    # Persist to Supabase (only when running a full scrape or in CI)
    if not args.source:
        save_to_supabase(listings)

    if skip_email:
        print_console_summary(listings, auction_lots, stats)

    if args.preview:
        base = pathlib.Path(args.out)
        stem, suffix = base.stem, base.suffix
        for brand, slug in [("FP Journe", "fpj"), ("De Bethune", "db")]:
            _, _, html = build_email(brand, listings, auction_lots, stats)
            out_path = base.with_name(f"{stem}-{slug}{suffix}").resolve()
            out_path.write_text(html, encoding="utf-8")
            log.info("Preview written → %s", out_path)
            webbrowser.open(out_path.as_uri())
    elif not skip_email:
        send_emails(listings, auction_lots, stats)

    log.info("Done.")
