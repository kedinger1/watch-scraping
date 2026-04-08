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
from dataclasses import dataclass, field
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
    reference_number: str = ""        # e.g. "FPJ-39-RG" — populated where available
    first_seen_at: str = ""           # "Apr 6, 2026" — populated from Supabase after upsert
    also_on: list[str] = field(default_factory=list)  # other sources carrying same watch

    def dedup_key(self) -> str:
        """Stable key for deduplication across scrapers."""
        return self.listing_url.split("?")[0].rstrip("/")


@dataclass
class AuctionLot:
    title: str                      # "F.P. Journe Centigraphe Souverain"
    estimate: str                   # "$80,000 – $160,000" (formatted for display)
    sale_date: str                  # "April 8, 2026"
    sale_location: str              # "New York" | "Geneva" | "Online"
    image_url: str
    lot_url: str
    brand: str                      # "FP Journe" | "De Bethune"
    auction_house: str = "Phillips"
    sale_name: str = ""             # "Important Watches, New York"
    lot_number: str = ""
    estimate_low: Optional[float] = None
    estimate_high: Optional[float] = None
    currency: str = "USD"
    is_upcoming: bool = True
    sale_date_end: Optional[str] = None   # ISO date "2026-05-14" — close/hammer date where known


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
    Chrono24 uses Cloudflare bot protection — GitHub Actions datacenter IPs are
    blocked at IP-reputation level regardless of browser stealth.

    Only De Bethune is scraped (~100 listings, mostly real held inventory).
    FP Journe is intentionally excluded — ~900 listings of which ~70% are broker
    placeholders with no held stock (per internal intel).

    Routing through a Bright Data Web Unlocker residential proxy bypasses
    Cloudflare. The static HTML served through the proxy already contains all
    listing cards, so no Playwright is needed — plain requests + BeautifulSoup.
    Credentials read from BRIGHT_DATA_PROXY env var.

    URL:      /debethune/index.htm (brand page, up to 120 listings)
    Selector: .js-listing-item-link
    """
    proxy_url = os.environ.get("BRIGHT_DATA_PROXY", "")
    if not proxy_url:
        raise RuntimeError("BRIGHT_DATA_PROXY not set — skipping Chrono24")

    proxies = {"http": proxy_url, "https": proxy_url}
    BASE = "https://www.chrono24.com"
    url = f"{BASE}/debethune/index.htm"

    try:
        resp = session.get(url, proxies=proxies, verify=False, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        raise RuntimeError(f"Chrono24 request failed: {exc}") from exc

    soup = BeautifulSoup(resp.text, "lxml")
    cards = soup.select(".js-listing-item-link")
    log.info("Chrono24 De Bethune: %d listing cards", len(cards))

    if not cards:
        # Check if we hit a challenge page
        title = soup.title.string if soup.title else ""
        if "moment" in title.lower() or "cloudflare" in title.lower():
            raise RuntimeError(f"Blocked by Cloudflare ({title!r})")
        raise RuntimeError("No listing cards found — page structure may have changed")

    listings: list[Listing] = []
    for a in cards:
        href = a.get("href", "")
        if not href:
            continue
        listing_url = href if href.startswith("http") else BASE + href

        card = a.find_parent(class_=re.compile(r"wt-search-result|listing-item|js-listing-item")) or a
        img_tag = card.find("img")
        img_url = best_img(img_tag)
        img_url = img_url.replace("-Square28.", "-Square40.") if img_url else ""

        texts = [el.get_text(" ", strip=True) for el in card.find_all(string=True, recursive=True)
                 if el.get_text(strip=True)]
        texts = [t for t in texts if len(t) > 1]

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

        listings.append(Listing(
            title=title,
            price=price,
            image_url=img_url,
            listing_url=listing_url,
            source="Chrono24",
            brand="De Bethune",
        ))

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
        raise RuntimeError("eBay token fetch failed — check EBAY_CLIENT_ID / EBAY_CLIENT_SECRET")

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
    Bezel (shop.getbezel.com) — all Playwright, single browser session.

    Phase 1 — paginate through /explore/{brand} pages, reading __NEXT_DATA__
      to collect model IDs and their active listing counts.  Only models with
      count > 0 are queued for Phase 2.

    Phase 2 — navigate to each model page inside the same Playwright context
      (required to bypass Cloudflare/reCAPTCHA that blocks raw requests).
      Listing data is embedded in __NEXT_DATA__ → props.pageProps.listings[].

    Key fields per listing:
      .id              → listing ID  (used for URL)
      .model.name      → model name
      .manufactureYear → production year
      .priceCents      → price in cents
      .status          → "PUBLISHED" = active
      .active          → bool
      .images[]        → listing-specific photos
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
    except ImportError:
        log.warning("Playwright not installed — skipping Bezel.")
        return []

    BASE = "https://shop.getbezel.com"
    explore_configs = [
        ("FP Journe",  "fp-journe",  "F.P. Journe"),
        ("De Bethune", "de-bethune", "De Bethune"),
    ]
    listings: list[Listing] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)

        for brand, brand_key, brand_display in explore_configs:
            # Fresh context per brand — avoids accumulated state from model-page
            # navigations in the previous brand's Phase 2 contaminating Phase 1.
            ctx = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                viewport={"width": 1440, "height": 900},
            )
            page = ctx.new_page()
            # ── Phase 1: collect model IDs with active listings ───────────────
            model_queue: list[tuple[str, str]] = []  # (model_url, model_slug)
            explore_page_num = 1

            while True:
                # Note: ?page=1 strips listing counts from the response — use
                # the base URL for page 1, only append ?page=N for pages 2+.
                explore_url = (
                    f"{BASE}/explore/{brand_key}"
                    if explore_page_num == 1
                    else f"{BASE}/explore/{brand_key}?page={explore_page_num}"
                )
                try:
                    page.goto(explore_url, wait_until="domcontentloaded", timeout=60_000)
                    page.wait_for_timeout(4_000)
                    html = page.content()
                except PwTimeout:
                    log.warning("Bezel explore timed out: %s page %d", brand, explore_page_num)
                    break
                except Exception as exc:
                    log.warning("Bezel explore error %s page %d: %s", brand, explore_page_num, exc)
                    break

                m = re.search(
                    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
                )
                if not m:
                    break
                try:
                    nd = json.loads(m.group(1))
                    models_block = nd.get("props", {}).get("pageProps", {}).get("models", {})
                except Exception:
                    break

                hits = models_block.get("hits", [])
                if not hits:
                    break

                # Build id → URL map from all /id-N links on this explore page
                id_to_url: dict[int, str] = {}
                try:
                    hrefs = page.evaluate("""() => {
                        const out = {};
                        document.querySelectorAll('a[href*="/id-"]').forEach(a => {
                            const m = a.href.match(/\\/id-(\\d+)$/);
                            if (m) out[parseInt(m[1])] = a.href;
                        });
                        return out;
                    }""")
                    id_to_url = {int(k): v for k, v in hrefs.items()}
                except Exception:
                    pass

                for hit in hits:
                    obj = hit.get("object", {})
                    count = obj.get("count", 0) or 0
                    if count <= 0:
                        continue
                    model_id = obj.get("id")
                    if not model_id:
                        continue
                    href = id_to_url.get(model_id)
                    if not href:
                        # Fallback: construct URL directly from brand key + model id
                        href = f"{BASE}/watches/{brand_key}/model/ref-/id-{model_id}"
                    model_queue.append((href, str(model_id)))

                # Check if there are more pages
                total = models_block.get("totalModelCount", 0)
                page_size = len(hits)
                if explore_page_num * page_size >= total:
                    break
                explore_page_num += 1
                time.sleep(0.5)

            log.info("Bezel %s: %d model pages with active listings", brand, len(model_queue))

            # ── Phase 2: navigate to each model page and extract listings ─────
            for model_url, model_id_str in model_queue:
                try:
                    page.goto(model_url, wait_until="domcontentloaded", timeout=60_000)
                    page.wait_for_timeout(3_000)
                    html = page.content()
                except PwTimeout:
                    log.warning("Bezel model page timed out: %s", model_url)
                    continue
                except Exception as exc:
                    log.warning("Bezel model page error %s: %s", model_url, exc)
                    continue

                m = re.search(
                    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
                )
                if not m:
                    log.warning("Bezel: no __NEXT_DATA__ at %s", model_url)
                    continue
                try:
                    nd = json.loads(m.group(1))
                    raw_listings = (
                        nd.get("props", {}).get("pageProps", {}).get("listings", [])
                    )
                except Exception as exc:
                    log.warning("Bezel JSON parse error %s: %s", model_url, exc)
                    continue

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
                    title_parts = [str(year) if year else "", brand_display, model_name]
                    title = " ".join(p for p in title_parts if p).strip()

                    price_cents = lst.get("priceCents") or 0
                    price = f"${price_cents / 100:,.0f}" if price_cents else "—"

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
                    ref_num = str(
                        lst.get("referenceNumber")
                        or model_info.get("referenceNumber")
                        or ""
                    )

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

            ctx.close()  # discard accumulated state before next brand

        browser.close()

    log.info("Bezel total: %d individual listings", len(listings))
    return deduplicate(listings)


# ── 1stDibs ────────────────────────────────────────────────────────────────────
def scrape_1stdibs(session: requests.Session) -> list[Listing]:
    """
    1stDibs is fully JS-rendered — BeautifulSoup returns an empty shell.
    Playwright loads the search results page and extracts listing cards
    directly from the DOM.

    Search URLs:
      /buy/luxury-watches/?q=f.p.+journe
      /buy/luxury-watches/?q=de+bethune
    Card selector: [data-tn="search-result-item"]
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
    except ImportError:
        log.warning("Playwright not installed — skipping 1stDibs")
        return []

    BASE = "https://www.1stdibs.com"
    queries = [
        ("FP Journe",  f"{BASE}/buy/luxury-watches/?q=f.p.+journe"),
        ("De Bethune", f"{BASE}/buy/luxury-watches/?q=de+bethune"),
    ]
    listings: list[Listing] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            viewport={"width": 1440, "height": 900},
            locale="en-US",
        )
        page = ctx.new_page()

        for brand, url in queries:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                page.wait_for_timeout(5_000)
                page.wait_for_selector('[data-tn="search-result-item"], [class*="item-tile"]', timeout=30_000)

                items = page.evaluate("""() => {
                    const results = [];
                    const cards = document.querySelectorAll(
                        '[data-tn="search-result-item"], [class*="item-tile"], [class*="ItemTile"]'
                    );
                    cards.forEach(card => {
                        const a = card.querySelector('a[href]');
                        if (!a) return;
                        const titleEl = card.querySelector(
                            '[data-tn="item-title"], [class*="title"], [class*="Title"], h2, h3'
                        );
                        const priceEl = card.querySelector(
                            '[data-tn="item-price"], [class*="price"], [class*="Price"]'
                        );
                        const img = card.querySelector('img');
                        results.push({
                            url:   a.href,
                            title: titleEl ? titleEl.textContent.trim() : '',
                            price: priceEl ? priceEl.textContent.trim() : '—',
                            img:   img ? (img.src || img.dataset.src || '') : '',
                        });
                    });
                    return results;
                }""")

                log.info("1stDibs %s: %d items", brand, len(items))
                for item in items:
                    title = item.get("title") or "Unknown"
                    if not detect_brand(title):
                        continue
                    listings.append(Listing(
                        title=title,
                        price=item.get("price") or "—",
                        image_url=item.get("img") or "",
                        listing_url=item.get("url") or "",
                        source="1stDibs",
                        brand=brand,
                    ))

            except PwTimeout:
                log.warning("1stDibs timed out for %s", brand)
            except Exception as exc:
                log.warning("1stDibs error for %s: %s", brand, exc)
            time.sleep(2)

        browser.close()

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

            low_f  = float(low)  if low  else None
            high_f = float(high) if high else None
            lots.append(AuctionLot(
                title=title, estimate=estimate,
                sale_date=sale_date, sale_location=location,
                image_url=img_url, lot_url=lot_url, brand=brand,
                auction_house="Phillips",
                sale_name=f"Phillips {location}",
                lot_number=str(lot.get("lotNumber", "")),
                estimate_low=low_f, estimate_high=high_f,
                currency=lot.get("currencySign", "$").replace("$", "USD"),
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
        log.info("Supabase client initialised → %s", url_env)
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
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()

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
            "last_seen_at":     now_iso,
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


def save_auction_lots_to_supabase(lots: list[AuctionLot]) -> None:
    """
    Upsert auction lots to the auction_lots table and mark any
    previously-upcoming lots not seen this run as inactive (is_upcoming=false).
    """
    url_env = os.environ.get("SUPABASE_URL", "")
    key_env = os.environ.get("SUPABASE_KEY", "")
    if not url_env or not key_env:
        log.warning("SUPABASE_URL / SUPABASE_KEY not set — skipping auction DB save")
        return

    try:
        from supabase import create_client
    except ImportError:
        log.warning("supabase package not installed — skipping auction DB save")
        return

    try:
        sb = create_client(url_env, key_env)
    except Exception as exc:
        log.error("Supabase client init failed (auction_lots): %s", exc)
        return

    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()

    # Fetch existing upcoming lots so we can mark stale ones inactive
    try:
        existing_resp = (
            sb.table("auction_lots")
            .select("lot_url, is_upcoming")
            .eq("is_upcoming", True)
            .execute()
        )
        existing_upcoming: set[str] = {
            row["lot_url"] for row in (existing_resp.data or [])
        }
    except Exception as exc:
        log.error("Supabase fetch existing auction_lots failed: %s", exc)
        existing_upcoming = set()

    seen_urls: set[str] = set()

    for lot in lots:
        seen_urls.add(lot.lot_url)

        # Convert human-readable sale_date to ISO date for the DATE column
        sale_date_iso = None
        if lot.sale_date and lot.sale_date not in ("TBA", "—"):
            for fmt in ("%b %d, %Y", "%B %d, %Y"):
                try:
                    from datetime import datetime as _dt
                    sale_date_iso = _dt.strptime(lot.sale_date, fmt).date().isoformat()
                    break
                except ValueError:
                    continue

        row = {
            "lot_url":       lot.lot_url,
            "auction_house": lot.auction_house,
            "brand":         lot.brand,
            "title":         lot.title,
            "lot_number":    lot.lot_number or None,
            "sale_name":     lot.sale_name or None,
            "estimate_low":  lot.estimate_low,
            "estimate_high": lot.estimate_high,
            "currency":      lot.currency,
            "sale_date":     sale_date_iso,
            "sale_date_end": lot.sale_date_end,
            "location":      lot.sale_location,
            "image_url":     lot.image_url or None,
            "is_upcoming":   True,
            "last_seen_at":  now_iso,
        }

        try:
            sb.table("auction_lots").upsert(row, on_conflict="lot_url").execute()
        except Exception as exc:
            log.warning("Supabase upsert auction lot failed for %s: %s",
                        lot.lot_url[:60], exc)

    # Mark lots that disappeared from the live feed as no longer upcoming
    stale = [u for u in existing_upcoming if u not in seen_urls]
    if stale:
        try:
            (
                sb.table("auction_lots")
                .update({"is_upcoming": False})
                .in_("lot_url", stale)
                .execute()
            )
            log.info("Supabase: marked %d auction lot(s) inactive", len(stale))
        except Exception as exc:
            log.warning("Supabase mark-inactive auction_lots failed: %s", exc)

    log.info(
        "Supabase auction_lots: upserted %d lot(s), %d marked inactive",
        len(seen_urls), len(stale),
    )


# ── Sotheby's ──────────────────────────────────────────────────────────────────
def _sothebys_sale_dates(session: requests.Session, sale_slug: str, base: str) -> tuple[str, str | None]:
    """
    Fetch a Sotheby's sale page and return (sale_date_str, sale_date_end_iso).

    Sale pages embed startDate (preview/open) and auctionDate (hammer/close)
    in __NEXT_DATA__. lot-level hits on creator pages don't carry dates at all,
    so we fetch the sale page once per unique sale slug and cache the result.

    Returns ("TBA", None) if the page can't be fetched or parsed.
    """
    from datetime import datetime, timezone
    url = f"{base}/en/buy/auction/{sale_slug.lstrip('/')}"
    resp = fetch(url, session)
    if not resp:
        return "TBA", None
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.DOTALL)
    if not m:
        return "TBA", None
    try:
        nd = json.loads(m.group(1))
        pp = nd.get("props", {}).get("pageProps", {})

        # Walk all keys looking for startDate / auctionDate at any nesting level
        def _find(obj: object, keys: list[str]) -> dict[str, str]:
            found: dict[str, str] = {}
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k in keys and isinstance(v, str) and v:
                        found[k] = v
                    found.update(_find(v, keys))
            elif isinstance(obj, list):
                for item in obj:
                    found.update(_find(item, keys))
            return found

        dates = _find(pp, ["startDate", "auctionDate"])
        start_raw = dates.get("startDate", "")
        close_raw = dates.get("auctionDate", "") or start_raw

        def _fmt(iso: str) -> str:
            try:
                dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
                return f"{dt.strftime('%b')} {dt.day}, {dt.year}"
            except Exception:
                return ""

        def _iso_date(iso: str) -> str | None:
            try:
                return datetime.fromisoformat(iso.replace("Z", "+00:00")).date().isoformat()
            except Exception:
                return None

        sale_date = _fmt(start_raw) or _fmt(close_raw) or "TBA"
        sale_date_end = _iso_date(close_raw) if close_raw != start_raw else None
        return sale_date, sale_date_end
    except Exception as exc:
        log.warning("Sotheby's sale-page date parse error (%s): %s", sale_slug, exc)
        return "TBA", None


def scrape_sothebys(session: requests.Session) -> list[AuctionLot]:
    """
    Sotheby's auction lot scraper.

    Sotheby's mixes auction lots and fixed-price marketplace items in the same
    Algolia index (prod_product_items).  The correct way to isolate auction lots
    is to filter by waysToBuy=bid (as opposed to buyNow / private).

    Creator pages (SSR) expose all of this via __NEXT_DATA__:
      /en/buy/luxury/watches/watch/f-p-journe
      /en/buy/luxury/watches/watch/de-bethune

    Lot-level hits carry no date fields — dates are fetched once per unique sale
    slug from the sale page and cached in sale_date_cache.

    NOTE: Sotheby's typically lists FP Journe and De Bethune auction lots only
    in the lead-up to their dedicated watch sale events (2–3× per year).
    The scraper correctly returns 0 when no lots are in market.
    """
    BASE = "https://www.sothebys.com"
    queries = [
        ("FP Journe",  "f-p-journe",  "f.p. journe"),
        ("De Bethune", None,           "de bethune"),
    ]
    lots: list[AuctionLot] = []
    # slug like "2026/important-watches-4"  →  (sale_date_str, sale_date_end_iso)
    sale_date_cache: dict[str, tuple[str, str | None]] = {}

    for brand, creator_slug, search_q in queries:
        hits: list[dict] = []
        if creator_slug:
            url = f"{BASE}/en/buy/luxury/watches/watch/{creator_slug}?waysToBuy=bid"
            resp = fetch(url, session)
            if resp and resp.status_code == 200:
                m = re.search(
                    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                    resp.text, re.DOTALL,
                )
                if m:
                    try:
                        nd  = json.loads(m.group(1))
                        pp  = nd.get("props", {}).get("pageProps", {})
                        rs  = pp.get("resultsState", {})
                        hits = rs.get("rawResults", [{}])[0].get("hits", [])
                    except Exception as exc:
                        log.warning("Sotheby's %s creator-page JSON error: %s", brand, exc)

        if not hits and search_q:
            url = f"{BASE}/en/search?query={quote_plus(search_q)}"
            resp = fetch(url, session)
            if resp:
                m = re.search(
                    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                    resp.text, re.DOTALL,
                )
                if m:
                    try:
                        nd  = json.loads(m.group(1))
                        pp  = nd.get("props", {}).get("pageProps", {})
                        hits = pp.get("results", {}).get("hits", [])
                    except Exception as exc:
                        log.warning("Sotheby's %s search-page JSON error: %s", brand, exc)

        log.info("Sotheby's %s: %d candidate hit(s)", brand, len(hits))

        for h in hits:
            if (
                h.get("waysToBuy") == "buyNow"
                or h.get("type") == "Buy Now"
                or h.get("salesChannel") == "retail"
            ):
                continue

            low        = h.get("lowEstimate")  or 0
            high       = h.get("highEstimate") or 0
            list_price = h.get("listPrice")    or 0
            if low and high and low == high == list_price:
                continue

            title    = h.get("title") or "Unknown"
            currency = h.get("currency", "USD")
            sign     = "$" if currency == "USD" else currency

            if low and high and low != high:
                estimate = f"{sign}{int(low):,} – {sign}{int(high):,}"
            elif low or high:
                estimate = f"{sign}{int(low or high):,}"
            else:
                estimate = "TBA"

            lot_path = h.get("url", "") or h.get("slug", "")
            if lot_path.startswith("http"):
                lot_url = lot_path
            elif lot_path:
                lot_url = f"{BASE}/en/{lot_path.lstrip('/')}"
            else:
                lot_url = url

            # Derive sale slug from lot path: /en/buy/auction/YEAR/SALE-NAME/lot-slug
            # → "YEAR/SALE-NAME"
            sale_date = "TBA"
            sale_date_end = None
            slug_clean = lot_path.lstrip("/")
            # slug_clean like "en/buy/auction/2026/important-watches-4/elegante-..."
            m_slug = re.match(r"(?:en/buy/auction/)?(\d{4}/[^/]+)/", slug_clean)
            if m_slug:
                sale_slug = m_slug.group(1)
                if sale_slug not in sale_date_cache:
                    sale_date_cache[sale_slug] = _sothebys_sale_dates(session, sale_slug, BASE)
                sale_date, sale_date_end = sale_date_cache[sale_slug]

            location   = h.get("saleLocation") or h.get("location") or h.get("auctionLocation") or "Sotheby's"
            sale_name  = h.get("saleName") or h.get("sale_name") or "Sotheby's Watch Sale"
            lot_number = str(h.get("lotNumber") or h.get("lot_number") or h.get("lotNr") or "")
            image_url  = h.get("imageUrl") or h.get("image_url") or ""

            lots.append(AuctionLot(
                title=title,
                estimate=estimate,
                sale_date=sale_date,
                sale_location=location,
                image_url=image_url,
                lot_url=lot_url,
                brand=brand,
                auction_house="Sotheby's",
                sale_name=sale_name,
                lot_number=lot_number,
                estimate_low=float(low) if low else None,
                estimate_high=float(high) if high else None,
                currency=currency,
                sale_date_end=sale_date_end,
            ))

        time.sleep(1)

    log.info("Sotheby's total: %d lot(s)", len(lots))
    return lots


# ── Christie's ─────────────────────────────────────────────────────────────────
def _christies_parse_lot(h: dict, brand: str) -> Optional[AuctionLot]:
    """Parse a single Christie's lot dict (from XHR JSON) into an AuctionLot."""
    BASE = "https://www.christies.com"

    title_raw = h.get("object_name") or h.get("title") or ""
    maker_raw = h.get("maker") or h.get("creator_name") or h.get("creatorName") or ""
    title = f"{maker_raw} {title_raw}".strip() if maker_raw else title_raw
    if not title:
        return None

    lot_num  = str(h.get("lot_number") or h.get("lotNumber") or h.get("lot_id_txt") or "")
    lot_id   = h.get("lot_id") or h.get("lotId") or ""
    lot_path = h.get("url") or h.get("lot_url") or h.get("lotUrl") or ""
    if lot_path:
        lot_url = lot_path if lot_path.startswith("http") else BASE + lot_path
    elif lot_id:
        lot_url = f"{BASE}/en/lot/{lot_id}"
    else:
        lot_url = f"{BASE}/en/results?filters=department_id%3A46"

    # Image
    img_url = ""
    for img_key in ("primary_image", "image_url", "imageUrl", "thumbnail"):
        img_url = h.get(img_key) or ""
        if img_url:
            break
    if not img_url:
        imgs = h.get("images") or []
        if imgs:
            first = imgs[0] if isinstance(imgs[0], dict) else {}
            img_url = first.get("src") or first.get("url") or first.get("image_url") or ""

    # Estimate
    low_raw  = h.get("estimate_low")  or h.get("estimateLow")  or 0
    high_raw = h.get("estimate_high") or h.get("estimateHigh") or 0
    currency = h.get("currency") or h.get("currency_cd") or "USD"
    sign     = "$" if currency == "USD" else ("£" if currency == "GBP" else "€" if currency == "EUR" else currency)
    try:
        low_f  = float(low_raw)  if low_raw  else None
        high_f = float(high_raw) if high_raw else None
    except (TypeError, ValueError):
        low_f = high_f = None
    if low_f and high_f:
        estimate = f"{sign}{int(low_f):,} – {sign}{int(high_f):,}"
    else:
        estimate = h.get("estimate_text") or h.get("price_realised_txt") or "—"

    # Sale date
    sale_date_raw = (
        h.get("sale_date") or h.get("saleDate") or
        h.get("auction_date") or h.get("auctionDate") or ""
    )
    if sale_date_raw:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(str(sale_date_raw).replace("Z", "+00:00"))
            try:
                sale_date = dt.strftime("%B %-d, %Y")
            except ValueError:
                sale_date = dt.strftime("%B %#d, %Y")
        except Exception:
            sale_date = str(sale_date_raw)[:10]
    else:
        sale_date = "—"

    sale_location = (
        h.get("sale_location") or h.get("saleLocation") or
        h.get("location") or "Christie's"
    )
    sale_name = (
        h.get("sale_name") or h.get("saleName") or
        h.get("event_name") or h.get("eventName") or "Christie's Watch Sale"
    )

    return AuctionLot(
        title=title,
        estimate=estimate,
        sale_date=sale_date,
        sale_location=sale_location,
        image_url=img_url,
        lot_url=lot_url,
        brand=brand,
        auction_house="Christie's",
        sale_name=sale_name,
        lot_number=lot_num,
        estimate_low=low_f,
        estimate_high=high_f,
        currency=currency,
    )


def scrape_christies(session: requests.Session) -> list[AuctionLot]:
    """
    Christie's lot scraper using Playwright + Bright Data residential proxy.

    Christie's resets TCP connections from cloud/datacenter IPs before any
    HTTP exchange — plain requests won't work.  Routing Playwright through the
    Bright Data proxy (same credential used for Chrono24) bypasses this block.

    Strategy:
    1.  Parse BRIGHT_DATA_PROXY into host/username/password for Playwright.
    2.  Launch Chromium with that proxy; search Christie's /en/results for each
        brand in the Watches department (filter=upcoming).
    3.  Intercept XHR / fetch responses whose URL contains "lotfinder" or
        "lot-search" to capture raw JSON lot data.
    4.  Fall back to parsing __NEXT_DATA__ embedded JSON if no XHR is captured.

    If BRIGHT_DATA_PROXY is not set the scraper raises (no point trying without
    it — bare Playwright from a cloud runner is always blocked).
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
    except ImportError:
        raise RuntimeError("Playwright not installed — skipping Christie's")

    proxy_url = os.environ.get("BRIGHT_DATA_PROXY", "")
    if not proxy_url:
        raise RuntimeError("BRIGHT_DATA_PROXY not set — skipping Christie's")

    # Parse proxy URL: http://user:pass@host:port  →  Playwright proxy dict
    from urllib.parse import urlparse as _urlparse
    _p = _urlparse(proxy_url)
    playwright_proxy = {
        "server":   f"{_p.scheme or 'http'}://{_p.hostname}:{_p.port}",
        "username": _p.username or "",
        "password": _p.password or "",
    }

    BASE = "https://www.christies.com"
    # Watches department, upcoming only, sorted by sale date
    SEARCH_TMPL = (
        BASE + "/en/results"
        "?filters=department_id%3A46%7Cauction_status%3Aupcoming"
        "&keyword={kw}"
        "&action=sort_by&sortby=sale_date_asc"
        "&startindex={start}"
    )
    PAGE_SIZE = 30  # Christie's default page size

    searches = [
        ("FP Journe",  "F.P.+Journe"),
        ("De Bethune", "De+Bethune"),
    ]

    lots: list[AuctionLot] = []
    captured_json: list[dict] = []  # filled by XHR interceptor

    def _on_response(response):
        """Intercept XHR responses containing lot JSON."""
        url = response.url
        if not any(k in url for k in ("lotfinder", "lot-search", "lotsearch", "results")):
            return
        if "christies.com" not in url:
            return
        try:
            ct = response.headers.get("content-type", "")
            if "json" not in ct:
                return
            data = response.json()
            if isinstance(data, dict) and (
                "lots" in data or "results" in data or "data" in data
            ):
                captured_json.append(data)
        except Exception:
            pass

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            proxy=playwright_proxy,
        )
        ctx = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )

        for brand, kw in searches:
            for page_idx in range(MAX_PAGES):
                start = page_idx * PAGE_SIZE
                url = SEARCH_TMPL.format(kw=kw, start=start)
                captured_json.clear()

                pg = ctx.new_page()
                pg.on("response", _on_response)
                try:
                    pg.goto(url, wait_until="networkidle", timeout=60_000)
                    pg.wait_for_timeout(3_000)
                    html = pg.content()
                except PwTimeout:
                    log.warning("Christie's %s page %d timed out", brand, page_idx + 1)
                    pg.close()
                    break
                finally:
                    pg.close()

                page_lots: list[AuctionLot] = []

                # ── Path A: XHR JSON captured ─────────────────────────────────
                for data in captured_json:
                    hits = (
                        data.get("lots") or
                        data.get("results") or
                        (data.get("data") or {}).get("lots") or
                        []
                    )
                    if isinstance(hits, dict):
                        hits = hits.get("results") or hits.get("items") or []
                    for h in hits:
                        if not detect_brand(f"{h.get('title','')} {h.get('object_name','')} {h.get('maker','')} {kw}"):
                            continue
                        lot = _christies_parse_lot(h, brand)
                        if lot:
                            page_lots.append(lot)

                # ── Path B: __NEXT_DATA__ embedded JSON ───────────────────────
                if not page_lots:
                    soup = BeautifulSoup(html, "lxml")
                    nd_tag = soup.find("script", id="__NEXT_DATA__")
                    if nd_tag and nd_tag.string:
                        try:
                            nd = json.loads(nd_tag.string)
                            # Walk the props tree looking for lot arrays
                            raw_str = json.dumps(nd)
                            if "lot_number" in raw_str or "lotNumber" in raw_str:
                                # Try common paths in Christie's Next.js page props
                                props = nd.get("props", {}).get("pageProps", {})
                                for key in ("lots", "results", "searchResults", "items"):
                                    hits = props.get(key) or []
                                    if hits:
                                        break
                                for h in hits:
                                    if not detect_brand(f"{h.get('title','')} {h.get('object_name','')} {kw}"):
                                        continue
                                    lot = _christies_parse_lot(h, brand)
                                    if lot:
                                        page_lots.append(lot)
                        except Exception as exc:
                            log.debug("Christie's __NEXT_DATA__ parse error: %s", exc)

                log.info("Christie's %s page %d: %d lot(s)", brand, page_idx + 1, len(page_lots))
                lots.extend(page_lots)

                if len(page_lots) < PAGE_SIZE:
                    break  # Last page
                time.sleep(2)

        browser.close()

    log.info("Christie's total: %d lot(s)", len(lots))
    return lots


# ── Barnebys ───────────────────────────────────────────────────────────────────
def scrape_barnebys(session: requests.Session) -> list[AuctionLot]:
    """
    Barnebys auction aggregator — upcoming lots only.

    Barnebys embeds search results in a Redux state object on their search page:
      window.__redux.search.resultState.rawResults[0].hits[]

    Each hit contains: uid, ah (auction house), url, img, i18n (title),
    ts.ends (Unix timestamp), priceE (estimate range), loc (location).

    We search /auctions/search?q=<brand> which returns only active/upcoming lots
    (as opposed to /realized-prices/search which returns sold lots).
    """
    BASE = "https://www.barnebys.com"
    queries = [
        ("FP Journe",  "fp journe"),
        ("De Bethune", "de bethune"),
    ]
    lots: list[AuctionLot] = []
    seen_urls: set[str] = set()

    for brand, query in queries:
        url = f"{BASE}/auctions/search?q={quote_plus(query)}"
        resp = fetch(url, session)
        if not resp:
            log.warning("Barnebys %s: no response", brand)
            continue

        # Extract __redux JSON from script tag
        m = re.search(r'window\.__redux\s*=\s*(\{.*?\});\s*</script>', resp.text, re.DOTALL)
        if not m:
            log.warning("Barnebys %s: __redux not found", brand)
            continue

        try:
            redux = json.loads(m.group(1))
        except json.JSONDecodeError as exc:
            log.warning("Barnebys %s: JSON parse error: %s", brand, exc)
            continue

        raw_results = (
            redux.get("search", {})
                 .get("resultState", {})
                 .get("rawResults", [{}])
        )
        hits = raw_results[0].get("hits", []) if raw_results else []
        log.info("Barnebys %s: %d hit(s)", brand, len(hits))

        from datetime import datetime, timezone
        for h in hits:
            lot_url = h.get("url", "")
            if not lot_url:
                continue
            if not lot_url.startswith("http"):
                lot_url = BASE + lot_url
            if lot_url in seen_urls:
                continue
            seen_urls.add(lot_url)

            # Title from i18n — prefer English, fall back to first available
            i18n = h.get("i18n") or {}
            title = (
                i18n.get("en", {}).get("title")
                or next((v.get("title") for v in i18n.values() if isinstance(v, dict) and v.get("title")), None)
                or "Unknown"
            )

            # Image
            img = h.get("img") or ""
            if img and not img.startswith("http"):
                img = "https:" + img

            # Estimate — priceE is a dict like {"USD": [low, high], "EUR": [...]}
            price_e = h.get("priceE") or {}
            estimate = "—"
            for currency in ("USD", "EUR", "GBP", "SEK", "CHF"):
                rng = price_e.get(currency)
                if rng and len(rng) >= 2 and rng[0] and rng[1]:
                    sign = {"USD": "$", "EUR": "€", "GBP": "£", "SEK": "SEK ", "CHF": "CHF "}.get(currency, currency + " ")
                    estimate = f"{sign}{int(rng[0]):,} – {sign}{int(rng[1]):,}"
                    break

            # Sale dates: ts.starts = open, ts.ends = close
            ts = h.get("ts") or {}
            starts_ts = ts.get("starts") or 0
            ends_ts   = ts.get("ends")   or 0
            sale_date = "—"
            sale_date_end_iso = None
            if starts_ts:
                try:
                    dt = datetime.fromtimestamp(starts_ts, tz=timezone.utc)
                    sale_date = f"{dt.strftime('%b')} {dt.day}, {dt.year}"
                except Exception:
                    pass
            elif ends_ts:
                # Fall back to close date if no open date
                try:
                    dt = datetime.fromtimestamp(ends_ts, tz=timezone.utc)
                    sale_date = f"{dt.strftime('%b')} {dt.day}, {dt.year}"
                except Exception:
                    pass
            if ends_ts:
                try:
                    dt_end = datetime.fromtimestamp(ends_ts, tz=timezone.utc)
                    sale_date_end_iso = dt_end.date().isoformat()
                except Exception:
                    pass

            # Location
            loc = h.get("loc") or {}
            location = loc.get("city") or loc.get("region") or loc.get("country") or "Barnebys"

            # Auction house name
            auction_house_name = h.get("ah") or "Barnebys"

            lots.append(AuctionLot(
                title=title,
                estimate=estimate,
                sale_date=sale_date,
                sale_location=location,
                image_url=img,
                lot_url=lot_url,
                brand=brand,
                auction_house=auction_house_name,
                sale_name=f"{auction_house_name}",
                lot_number=str(h.get("uid") or ""),
                estimate_low=float(price_e.get("USD", [0])[0]) if price_e.get("USD") else None,
                estimate_high=float(price_e.get("USD", [0, 0])[1]) if price_e.get("USD") and len(price_e.get("USD", [])) >= 2 else None,
                currency="USD",
                sale_date_end=sale_date_end_iso,
            ))

        time.sleep(1)

    log.info("Barnebys total: %d lot(s)", len(lots))
    return lots


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


# ── Cross-platform deduplication ──────────────────────────────────────────────
def cross_platform_dedup(listings: list[Listing]) -> list[Listing]:
    """
    Collapse listings that appear on multiple platforms with identical
    brand + price + normalized title into a single row.

    The first-seen listing is kept; subsequent duplicates are dropped and
    their source names are added to the kept listing's `also_on` list.

    Normalization: lowercase, collapse whitespace, strip punctuation noise
    so minor formatting differences don't prevent matching.
    """
    def norm(s: str) -> str:
        s = s.lower().strip()
        s = re.sub(r"[^\w\s]", " ", s)   # punctuation → space
        s = re.sub(r"\s+", " ", s)        # collapse whitespace
        return s

    # Key: (brand, normalized_title, price)
    seen: dict[tuple, Listing] = {}
    out: list[Listing] = []

    for lst in listings:
        key = (lst.brand, norm(lst.title), lst.price)
        if key in seen:
            # Duplicate — record source on the kept listing
            kept = seen[key]
            if lst.source not in kept.also_on and lst.source != kept.source:
                kept.also_on.append(lst.source)
        else:
            seen[key] = lst
            out.append(lst)

    dupes = len(listings) - len(out)
    if dupes:
        log.info("Cross-platform dedup: removed %d duplicate(s) across sources", dupes)
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

    def _matches(name: str, query: str) -> bool:
        """Case-insensitive substring match, ignoring punctuation."""
        q = re.sub(r"[^\w]", "", query.lower())
        n = re.sub(r"[^\w]", "", name.lower())
        return q in n

    for name, scraper_fn in SCRAPERS:
        if only_source and not _matches(name, only_source):
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

    # Auction house scrapers — return AuctionLot, not Listing
    auction_scrapers = [
        ("Phillips",    scrape_phillips),
        ("Sotheby's",   scrape_sothebys),
        ("Christie's",  scrape_christies),
        ("Barnebys",    scrape_barnebys),
    ]
    for auction_name, auction_fn in auction_scrapers:
        label = f"{auction_name} (upcoming)"
        if only_source and not _matches(auction_name, only_source):
            continue
        log.info("── Scraping %s …", auction_name)
        try:
            lots = auction_fn(session)
            auction_lots.extend(lots)
            stats.append({
                "source": label,
                "count":  len(lots),
                "fpj":    sum(1 for l in lots if l.brand == "FP Journe"),
                "db":     sum(1 for l in lots if l.brand == "De Bethune"),
                "error":  None,
            })
            log.info("   → %d upcoming lot(s)", len(lots))
        except Exception as exc:
            log.error("%s scraper crashed: %s", auction_name, exc)
            stats.append({"source": label, "count": 0, "fpj": 0, "db": 0, "error": str(exc)})

    all_listings = cross_platform_dedup(all_listings)
    return all_listings, auction_lots, stats


def print_console_summary(
    listings: list[Listing], auction_lots: list[AuctionLot], stats: list[dict]
) -> None:
    """Print a readable summary table to stdout — always shown in dev mode."""
    fpj = [l for l in listings if l.brand == "FP Journe"]
    db  = [l for l in listings if l.brand == "De Bethune"]
    w = 60
    bar = "=" * w
    print(f"\n{bar}")
    print(f"  Watch Listings -- {date.today():%B %d, %Y}")
    print(f"  {len(listings)} total  ({len(fpj)} FP Journe / {len(db)} De Bethune)")
    print(bar)

    for brand_name, brand_listings in [("F.P. Journe", fpj), ("De Bethune", db)]:
        if not brand_listings:
            continue
        print(f"\n  {brand_name} ({len(brand_listings)})")
        print("  " + "-" * (w - 2))
        for l in brand_listings:
            title = l.title if len(l.title) <= 44 else l.title[:41] + "…"
            print(f"  {title:<44}  {l.price.ljust(12)}  {l.source[:18]}")
        print()

    if auction_lots:
        print(f"  Upcoming at Auction ({len(auction_lots)} lot(s))")
        print("  " + "-" * (w - 2))
        for lot in auction_lots:
            title = lot.title if len(lot.title) <= 40 else lot.title[:37] + "…"
            house = lot.auction_house[:10]
            print(f"  {title:<40}  {lot.estimate[:12].ljust(12)}  {house:<10}  {lot.sale_date}")
        print()

    print("  Source breakdown:")
    for s in stats:
        status = f"{s['count']} listings" if s["count"] > 0 else "0 listings"
        err    = f"  ! {s['error']}" if s["error"] else ""
        print(f"    {s['source']:<28}  {status}{err}")
    print(bar + "\n")


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
            f'<td style="padding:10px 8px;vertical-align:middle;color:#777;font-size:12px;">'
            f'{escape(l.source)}'
            + (
                f'<br><span style="color:#bbb;font-size:10px;">also: '
                + escape(", ".join(l.also_on))
                + '</span>'
                if l.also_on else ""
            )
            + f'</td>'
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
        lot_num_html = (
            f'<br><span style="color:#bbb;font-size:10px;">Lot {escape(lot.lot_number)}</span>'
            if lot.lot_number else ""
        )
        sale_name_html = (
            f'<span style="color:#888;font-size:11px;">{escape(lot.sale_name)}</span><br>'
            if lot.sale_name else ""
        )
        rows.append(
            f'<tr style="border-bottom:1px solid #e8e4f0;">'
            f'<td style="padding:10px 8px;width:96px;vertical-align:middle;">{img_cell}</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;">'
            f'  <a href="{escape(lot.lot_url)}" '
            f'     style="color:#3a1a55;font-weight:600;text-decoration:none;font-size:14px;">'
            f'     {escape(lot.title)}</a>{lot_num_html}'
            f'</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;white-space:nowrap;'
            f'font-weight:700;color:#5a3a80;font-size:14px;">{escape(lot.estimate)}</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;font-size:12px;color:#777;">'
            f'  {sale_name_html}'
            f'  {escape(lot.sale_date)}<br>'
            f'  <span style="color:#aaa;">{escape(lot.sale_location)}</span>'
            f'</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;font-size:12px;'
            f'color:#777;white-space:nowrap;">{escape(lot.auction_house)}</td>'
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
        'font-weight:600;text-transform:uppercase;letter-spacing:.5px;">Sale</th>'
        '<th style="padding:9px 8px;text-align:left;font-size:11px;color:#999;'
        'font-weight:600;text-transform:uppercase;letter-spacing:.5px;">House</th>'
        '</tr></thead>'
        '<tbody>' + "".join(rows) + "</tbody></table>"
    )


def stats_table_html(stats: list[dict], brand: str) -> str:
    """Render the source breakdown table filtered to one brand's counts."""
    brand_key = "fpj" if brand == "FP Journe" else "db"
    rows = []
    for s in stats:
        n = s.get(brand_key, s.get("count", 0))
        if s["error"]:
            value_cell = '<span style="color:#b94040;font-weight:600;">failed</span>'
        elif n > 0:
            value_cell = f'<span style="color:#2a6b2a;font-weight:600;">{n}</span>'
        else:
            value_cell = '<span style="color:#bbb;">0</span>'
        rows.append(
            f'<tr>'
            f'<td style="padding:5px 8px;">{escape(s["source"])}</td>'
            f'<td style="padding:5px 8px;">{value_cell}</td>'
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


def _is_new_today(l: "Listing") -> bool:
    """Return True if this listing was first seen today (brand-new discovery)."""
    if not l.first_seen_at:
        return False
    today = date.today()
    today_str = f"{today.strftime('%b')} {today.day}, {today.year}"
    return l.first_seen_at == today_str


def new_listings_html(new_listings: list["Listing"]) -> str:
    """
    Compact highlight table for listings seen for the first time today.
    Shows above the main table with a gold/amber accent to catch the eye.
    """
    if not new_listings:
        return ""

    rows = []
    for l in new_listings:
        img_cell = (
            f'<a href="{escape(l.listing_url)}">'
            f'<img src="{escape(l.image_url)}" width="64" height="64" '
            f'style="object-fit:cover;border-radius:4px;display:block;border:0;" '
            f'onerror="this.parentElement.innerHTML=\'&nbsp;\'" /></a>'
            if l.image_url else "&nbsp;"
        )
        also_html = (
            f'<br><span style="color:#bbb;font-size:10px;">also: '
            + escape(", ".join(l.also_on))
            + '</span>'
            if l.also_on else ""
        )
        rows.append(
            f'<tr style="border-bottom:1px solid #f0e8cc;">'
            f'<td style="padding:10px 8px;width:80px;vertical-align:middle;">{img_cell}</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;">'
            f'  <span style="display:inline-block;background:#b8860b;color:#fff;'
            f'  font-size:9px;font-weight:700;letter-spacing:.8px;padding:2px 5px;'
            f'  border-radius:3px;margin-right:6px;vertical-align:middle;">NEW</span>'
            f'  <a href="{escape(l.listing_url)}" '
            f'     style="color:#1a3550;font-weight:600;text-decoration:none;font-size:14px;">'
            f'     {escape(l.title)}</a>'
            f'</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;white-space:nowrap;'
            f'font-weight:700;color:#2a6b2a;font-size:15px;">{escape(l.price)}</td>'
            f'<td style="padding:10px 8px;vertical-align:middle;color:#777;font-size:12px;">'
            f'{escape(l.source)}{also_html}</td>'
            f'</tr>'
        )

    n = len(new_listings)
    return (
        f'<div style="margin:20px 0 0;border:2px solid #d4a017;border-radius:6px;overflow:hidden;">'
        f'<div style="background:#fdf3d0;padding:8px 12px;border-bottom:1px solid #d4a017;">'
        f'  <span style="font-size:13px;font-weight:700;color:#8b6000;letter-spacing:.3px;">'
        f'  ★ Newly Listed Today &nbsp;</span>'
        f'  <span style="font-size:12px;color:#a07820;">'
        f'{n} new listing{"s" if n != 1 else ""} spotted for the first time</span>'
        f'</div>'
        f'<table width="100%" cellpadding="0" cellspacing="0" '
        f'style="border-collapse:collapse;font-size:14px;background:#fffdf5;">'
        f'<tbody>' + "".join(rows) + '</tbody>'
        f'</table></div>'
    )


def build_email(
    brand: str,                    # "FP Journe" or "De Bethune"
    listings: list[Listing],
    auction_lots: list[AuctionLot],
    stats: list[dict],
) -> tuple[str, str, str]:
    """Build subject + plain + HTML for one brand's email."""
    today        = date.today().strftime("%B %d, %Y")
    display      = BRAND_DISPLAY.get(brand, brand)
    b_listings   = [l for l in listings    if l.brand == brand]
    b_lots       = [l for l in auction_lots if l.brand == brand]
    b_new        = [l for l in b_listings  if _is_new_today(l)]
    n_list       = len(b_listings)
    n_lots       = len(b_lots)
    n_new        = len(b_new)
    brand_key    = "fpj" if brand == "FP Journe" else "db"
    active_src   = sum(1 for s in stats if s.get(brand_key, s.get("count", 0)) > 0)

    subject = (
        f"{display} Listings — {today} "
        f"({n_list} listing{'s' if n_list != 1 else ''}"
        + (f" · {n_new} NEW" if n_new else "")
        + (f" · {n_lots} auction lot{'s' if n_lots != 1 else ''}" if n_lots else "")
        + ")"
    )

    # ── Plain text ───────────────────────────────────────────────────────────
    lines = [f"{display} Listings — {today}", f"{n_list} listing(s)\n"]
    if b_new:
        lines.append(f"── NEW TODAY ({n_new}) ──")
        for l in b_new:
            also = f" · also: {', '.join(l.also_on)}" if l.also_on else ""
            lines.append(f"  ★ {l.title}")
            lines.append(f"  {l.price} | {l.source}{also}")
            lines.append(f"  {l.listing_url}\n")
    lines.append(f"── All Listings ({n_list}) ──")
    for l in b_listings:
        found = f" · found {l.first_seen_at}" if l.first_seen_at else ""
        also = f" · also: {', '.join(l.also_on)}" if l.also_on else ""
        lines.append(f"  {l.title}")
        lines.append(f"  {l.price} | {l.source}{also}{found}")
        lines.append(f"  {l.listing_url}\n")
    if b_lots:
        lines.append(f"── Upcoming at Auction ({n_lots}) ──")
        for lot in b_lots:
            lines.append(f"  {lot.title}")
            lines.append(f"  Est. {lot.estimate} | {lot.auction_house} · {lot.sale_date} · {lot.sale_location}")
            lines.append(f"  {lot.lot_url}\n")
    plain = "\n".join(lines)

    # ── HTML ─────────────────────────────────────────────────────────────────
    count_str  = f"{n_list} listing{'s' if n_list != 1 else ''}"
    new_str    = (
        f' &nbsp;<span style="background:#b8860b;color:#fff;font-size:11px;'
        f'font-weight:700;padding:2px 6px;border-radius:3px;vertical-align:middle;">'
        f'{n_new} NEW</span>'
        if n_new else ""
    )
    lots_str   = (
        f' &nbsp;+&nbsp; <span style="color:#3a1a55;">'
        f'{n_lots} auction lot{"s" if n_lots != 1 else ""}</span>'
        if n_lots else ""
    )

    new_section = new_listings_html(b_new)

    auction_html = ""
    if b_lots:
        # Group lots by auction house for the sub-header list
        houses = []
        seen_h: set[str] = set()
        for _l in b_lots:
            if _l.auction_house not in seen_h:
                seen_h.add(_l.auction_house)
                houses.append(_l.auction_house)
        houses_str = " · ".join(houses)
        auction_html = (
            f'<div style="margin-top:40px;">'
            f'<h3 style="margin:0 0 10px;color:#3a1a55;font-size:18px;'
            f'border-bottom:2px solid #d8d0e8;padding-bottom:6px;">'
            f'Upcoming at Auction '
            f'<span style="font-size:13px;font-weight:normal;color:#aaa;">'
            f'{n_lots} lot{"s" if n_lots != 1 else ""} &mdash; {escape(houses_str)}</span></h3>'
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
      <strong style="color:#444;">{count_str}</strong>{new_str}{lots_str}
      across <strong style="color:#444;">{active_src}</strong> source{'s' if active_src != 1 else ''}
    </p>
  </div>
  {new_section}

  <h3 style="margin:24px 0 10px;color:#1a3550;font-size:18px;
             border-bottom:2px solid #e0d8cc;padding-bottom:6px;">
    All {escape(display)} Listings
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
        "--auctions-only",
        action="store_true",
        help=(
            "Run only auction-house scrapers (Phillips, Sotheby's, Christie's), "
            "save results to Supabase, and skip the email. "
            "Designed for the weekly auction refresh GitHub Action."
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
        print("  Phillips    (auction lots)")
        print("  Sotheby's   (auction lots)")
        print("  Christie's  (auction lots)")
        sys.exit(0)

    # --source alone implies no email (dev / debug mode)
    skip_email = args.no_email or args.preview or (args.source is not None) or args.auctions_only

    # Resend is not needed if we're not sending — skip the env check
    if skip_email:
        resend.api_key = os.environ.get("RESEND_API_KEY", "preview-mode-no-key-needed")

    log.info("Watch Listing Monitor starting%s…",
             f" [source={args.source}]" if args.source
             else " [auctions-only]" if args.auctions_only
             else "")

    # --auctions-only: pass a sentinel that matches auction scrapers but not marketplace scrapers.
    # Auction scraper names all contain the house name; marketplace scrapers don't contain
    # "Phillips", "Sotheby", or "Christie", so the existing substring filter works perfectly.
    _only = None
    if args.auctions_only:
        # We need to run all three auction houses — gather_all's only_source only allows one
        # substring match, so we run each house in turn.
        session = requests.Session()
        session.headers.update(HEADERS)
        all_auction_lots: list[AuctionLot] = []
        all_stats: list[dict] = []
        for house, fn in [("Phillips", scrape_phillips), ("Sotheby's", scrape_sothebys), ("Christie's", scrape_christies), ("Barnebys", scrape_barnebys)]:
            log.info("── Scraping %s (auction lots) …", house)
            try:
                lots = fn(session)
                all_auction_lots.extend(lots)
                all_stats.append({"source": house, "count": len(lots), "status": "ok", "error": None})
                log.info("%s: %d lot(s)", house, len(lots))
            except Exception as exc:
                log.error("%s scraper failed: %s", house, exc)
                all_stats.append({"source": house, "count": 0, "status": "failed", "error": str(exc)})
        save_auction_lots_to_supabase(all_auction_lots)
        print_console_summary([], all_auction_lots, all_stats)
        log.info("Done.")
        sys.exit(0)

    listings, auction_lots, stats = gather_all(only_source=args.source)

    fpj_count = sum(1 for l in listings if l.brand == "FP Journe")
    db_count  = sum(1 for l in listings if l.brand == "De Bethune")
    log.info("Total: %d listings (%d FPJ, %d DB) + %d auction lot(s)",
             len(listings), fpj_count, db_count, len(auction_lots))

    # Persist to Supabase (only when running a full scrape or in CI)
    if not args.source:
        save_to_supabase(listings)
        save_auction_lots_to_supabase(auction_lots)

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
