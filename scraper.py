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
import logging
import os
import pathlib
import subprocess
import sys
import time
import webbrowser
from dataclasses import dataclass
from datetime import date
from html import escape
from typing import Optional
from urllib.parse import quote_plus

import chrono24 as c24
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

    def dedup_key(self) -> str:
        """Stable key for deduplication across scrapers."""
        return self.listing_url.split("?")[0].rstrip("/")


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
    Uses the `chrono24` Python library which correctly handles Chrono24's
    session/anti-bot layer and returns structured JSON — no HTML parsing.
    120 listings per page; we fetch up to MAX_PAGES pages per brand.
    """
    listings: list[Listing] = []
    queries = [
        ("FP Journe",  "F.P. Journe"),
        ("De Bethune", "De Bethune"),
    ]
    for brand, query in queries:
        try:
            results = c24.query(query).search(limit=MAX_PAGES * 120)
            log.info("Chrono24 %s: %d listings", brand, len(results))
            for r in results:
                img_url = ""
                imgs = r.get("image_urls") or []
                if imgs:
                    img_url = imgs[0]
                raw_url = r.get("url", "")
                full_url = abs_url(raw_url, "https://www.chrono24.com")
                listings.append(Listing(
                    title=r.get("title", "Unknown"),
                    price=r.get("price", "—"),
                    image_url=img_url,
                    listing_url=full_url,
                    source="Chrono24",
                    brand=brand,
                ))
        except Exception as exc:
            log.error("Chrono24 %s failed: %s", brand, exc)
        time.sleep(2)

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

                listings.append(Listing(
                    title=title, price=price, image_url=img_url,
                    listing_url=listing_url, source=source_name, brand=brand,
                ))
            page += 1
            time.sleep(0.5)

    return deduplicate(listings)


# ── WatchFinder ────────────────────────────────────────────────────────────────
def scrape_watchfinder(session: requests.Session) -> list[Listing]:
    listings: list[Listing] = []
    BASE = "https://www.watchfinder.com"
    queries = [
        ("FP Journe",  f"{BASE}/search?q=F.P.+Journe"),
        ("De Bethune", f"{BASE}/search?q=De+Bethune"),
    ]
    for brand, url in queries:
        resp = fetch(url, session)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "lxml")
        cards = soup.select(
            ".product-card, .watch-card, "
            "article.product, [class*='product-item']"
        )
        log.info("WatchFinder %s: %d cards", brand, len(cards))
        for card in cards:
            a = card.find("a", href=True)
            if not a:
                continue
            href = abs_url(a["href"], BASE)
            title_el = card.select_one("h2, h3, .product-title, [class*='title']")
            title = title_el.get_text(" ", strip=True) if title_el else "Unknown"
            price_el = card.select_one(".price, [class*='price']")
            price = price_el.get_text(" ", strip=True) if price_el else "—"
            img_url = best_img(card.find("img"))
            listings.append(Listing(
                title=title, price=price, image_url=img_url,
                listing_url=href, source="WatchFinder", brand=brand,
            ))
        time.sleep(1)
    return deduplicate(listings)


# ── WatchBox ───────────────────────────────────────────────────────────────────
def scrape_watchbox(session: requests.Session) -> list[Listing]:
    listings: list[Listing] = []
    BASE = "https://www.thewatchbox.com"
    queries = [
        ("FP Journe",  f"{BASE}/search?q=fp+journe"),
        ("De Bethune", f"{BASE}/search?q=de+bethune"),
    ]
    for brand, url in queries:
        resp = fetch(url, session)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "lxml")
        cards = soup.select(
            ".product-card, .watch-item, "
            "[class*='product-grid-item'], [class*='listing-card']"
        )
        log.info("WatchBox %s: %d cards", brand, len(cards))
        for card in cards:
            a = card.find("a", href=True)
            if not a:
                continue
            href = abs_url(a["href"], BASE)
            title_el = card.select_one("h2, h3, [class*='name'], [class*='title']")
            title = title_el.get_text(" ", strip=True) if title_el else "Unknown"
            price_el = card.select_one("[class*='price']")
            price = price_el.get_text(" ", strip=True) if price_el else "—"
            img_url = best_img(card.find("img"))
            listings.append(Listing(
                title=title, price=price, image_url=img_url,
                listing_url=href, source="WatchBox", brand=brand,
            ))
        time.sleep(1)
    return deduplicate(listings)


# ── European Watch Company ─────────────────────────────────────────────────────
def scrape_european_watch_co(session: requests.Session) -> list[Listing]:
    listings: list[Listing] = []
    BASE = "https://www.europeanwatch.com"
    queries = [
        ("FP Journe",  f"{BASE}/search?q=fp+journe"),
        ("De Bethune", f"{BASE}/search?q=de+bethune"),
    ]
    for brand, url in queries:
        resp = fetch(url, session)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "lxml")
        cards = soup.select(".product-item, .grid-item, [class*='product']")
        log.info("European Watch Co %s: %d cards", brand, len(cards))
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
                listing_url=href, source="European Watch Co.", brand=brand,
            ))
        time.sleep(1)
    return deduplicate(listings)


# ── Bezel ──────────────────────────────────────────────────────────────────────
def scrape_bezel(session: requests.Session) -> list[Listing]:
    """
    Bezel (shop.getbezel.com) is a fully JS-rendered React/Plasmic app —
    requests+BS4 returns an empty shell. We use Playwright (headless Chromium)
    to execute the JavaScript and pull the rendered listing cards.

    Brand listing pages:
      https://shop.getbezel.com/explore/fp-journe
      https://shop.getbezel.com/explore/de-bethune
    Individual listing URLs follow: /watches/{brand}/{model}/ref-{ref}/id-{id}
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
    except ImportError:
        log.warning(
            "Playwright not installed — skipping Bezel. "
            "Run: pip install playwright && playwright install chromium"
        )
        return []

    listings: list[Listing] = []
    pages_to_visit = [
        ("FP Journe",  "https://shop.getbezel.com/explore/fp-journe"),
        ("De Bethune", "https://shop.getbezel.com/explore/de-bethune"),
    ]

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=HEADERS["User-Agent"])
        page = ctx.new_page()

        for brand, url in pages_to_visit:
            try:
                page.goto(url, wait_until="networkidle", timeout=45_000)
                # Wait until at least one watch link renders
                page.wait_for_selector('a[href*="/watches/"]', timeout=20_000)

                # Extract every listing card via JS — avoids fragile class-name selectors
                items = page.evaluate("""() => {
                    const seen = new Set();
                    const results = [];
                    document.querySelectorAll('a[href*="/watches/"]').forEach(a => {
                        const href = a.href;
                        if (seen.has(href)) return;
                        seen.add(href);
                        // Walk up to find a card-like container
                        const card = a.closest('li, article, section, [class*="card"], [class*="item"], [class*="tile"]') || a;
                        const img = card.querySelector('img');
                        // Collect all leaf-node text snippets
                        const texts = [];
                        card.querySelectorAll('*').forEach(el => {
                            if (el.children.length === 0) {
                                const t = el.textContent.trim();
                                if (t) texts.push(t);
                            }
                        });
                        results.push({
                            url:      href,
                            imageUrl: img ? (img.src || img.dataset.src || img.dataset.lazySrc || '') : '',
                            texts:    texts,
                        });
                    });
                    return results;
                }""")

                log.info("Bezel %s: %d listings", brand, len(items))

                for item in items:
                    texts = item.get("texts", [])
                    # First non-trivial text is usually the title
                    title = next((t for t in texts if len(t) > 6), "Unknown")
                    # Price: contains $ and at least one digit
                    price = next(
                        (t for t in texts if "$" in t and any(c.isdigit() for c in t)),
                        "—"
                    )
                    listings.append(Listing(
                        title=title,
                        price=price,
                        image_url=item.get("imageUrl", ""),
                        listing_url=item.get("url", ""),
                        source="Bezel",
                        brand=brand,
                    ))

            except PwTimeout:
                log.warning("Bezel timed out for %s — page may not have loaded", brand)
            except Exception as exc:
                log.warning("Bezel error for %s: %s", brand, exc)

        browser.close()

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
    ("Hodinkee Shop",          lambda s: scrape_shopify_store(
        s, "https://shop.hodinkee.com", "Hodinkee Shop"
    )),
    ("WatchFinder",            scrape_watchfinder),
    ("WatchBox",               scrape_watchbox),
    ("European Watch Co.",     scrape_european_watch_co),
    ("Bezel",                  scrape_bezel),
    ("1stDibs",                scrape_1stdibs),
    ("Watches of Switzerland", scrape_watches_of_switzerland),
]


def gather_all(only_source: Optional[str] = None) -> tuple[list[Listing], list[dict]]:
    """
    Run all scrapers (or just one if only_source is given).
    only_source is matched case-insensitively as a substring, e.g. "chrono" matches "Chrono24".
    """
    session = make_session()
    all_listings: list[Listing] = []
    stats: list[dict] = []

    for name, scraper_fn in SCRAPERS:
        if only_source and only_source.lower() not in name.lower():
            continue
        log.info("── Scraping %s …", name)
        try:
            results = scraper_fn(session)
            new = [r for r in results if r.dedup_key() not in {x.dedup_key() for x in all_listings}]
            all_listings.extend(new)
            stats.append({"source": name, "count": len(results), "error": None})
            log.info("   → %d listings (%d after global dedup)", len(results), len(new))
        except Exception as exc:
            log.error("Scraper %s crashed: %s", name, exc)
            stats.append({"source": name, "count": 0, "error": str(exc)})

    return all_listings, stats


def print_console_summary(listings: list[Listing], stats: list[dict]) -> None:
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
            # Truncate title to fit terminal width
            title = l.title if len(l.title) <= 44 else l.title[:41] + "…"
            price = l.price.ljust(12)
            source = l.source[:18]
            print(f"  {title:<44}  {price}  {source}")
        print()

    print("  Source breakdown:")
    for s in stats:
        status = f"{s['count']} listings" if s["count"] > 0 else "0 listings"
        err    = f"  ⚠ {s['error']}" if s["error"] else ""
        print(f"    {s['source']:<26}  {status}{err}")
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
        '</tr>'
        '</thead>'
        '<tbody>' + "".join(rows) + "</tbody>"
        "</table>"
    )


def stats_table_html(stats: list[dict]) -> str:
    rows = []
    for s in stats:
        color = "#2a6b2a" if s["count"] > 0 else "#bbb"
        err = (
            f' <span style="color:#b94040;font-size:11px;">({escape(str(s["error"]))})</span>'
            if s["error"] else ""
        )
        rows.append(
            f'<tr>'
            f'<td style="padding:5px 8px;">{escape(s["source"])}</td>'
            f'<td style="padding:5px 8px;font-weight:600;color:{color};">{s["count"]}{err}</td>'
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


def build_email(
    listings: list[Listing], stats: list[dict]
) -> tuple[str, str, str]:
    today = date.today().strftime("%B %d, %Y")
    fpj = [l for l in listings if l.brand == "FP Journe"]
    db  = [l for l in listings if l.brand == "De Bethune"]
    total = len(listings)

    subject = (
        f"Watch Listings — {today} "
        f"({len(fpj)} FPJ · {len(db)} DB)"
    )

    # ── Plain text ──────────────────────────────────────────────────────────
    lines = [f"Watch Listings Digest — {today}", f"{total} total listings\n"]
    for brand_name, brand_listings in [("F.P. Journe", fpj), ("De Bethune", db)]:
        lines.append(f"── {brand_name} ({len(brand_listings)}) ──")
        for l in brand_listings:
            lines.append(f"  {l.title}")
            lines.append(f"  {l.price} | {l.source}")
            lines.append(f"  {l.listing_url}\n")
    plain = "\n".join(lines)

    # ── HTML ────────────────────────────────────────────────────────────────
    active_sources = len([s for s in stats if s["count"] > 0])

    def brand_section(name: str, brand_listings: list[Listing]) -> str:
        count_str = f"{len(brand_listings)} listing{'s' if len(brand_listings) != 1 else ''}"
        return (
            f'<h3 style="margin:28px 0 10px;color:#1a3550;font-size:18px;'
            f'border-bottom:2px solid #e0d8cc;padding-bottom:6px;">'
            f'{escape(name)} '
            f'<span style="font-size:13px;font-weight:normal;color:#aaa;">'
            f'{count_str}</span></h3>'
            + listing_table_html(brand_listings)
        )

    html = f"""<!DOCTYPE html>
<html>
<body style="font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;
             max-width:900px;margin:auto;color:#222;padding:24px;
             background:#fff;">

  <!-- Header -->
  <div style="border-bottom:3px solid #1a3550;padding-bottom:14px;margin-bottom:4px;">
    <h2 style="margin:0;font-size:24px;color:#1a3550;letter-spacing:-.3px;">
      Watch Listings Digest
    </h2>
    <p style="margin:5px 0 0;color:#999;font-size:13px;">
      {escape(today)} &mdash;
      <strong style="color:#444;">{total}</strong> listing{'s' if total != 1 else ''}
      across <strong style="color:#444;">{active_sources}</strong> source{'s' if active_sources != 1 else ''}
    </p>
  </div>

  {brand_section("F.P. Journe", fpj)}
  {brand_section("De Bethune", db)}

  <!-- Source stats -->
  <div style="margin-top:40px;border-top:1px solid #e8e8e8;padding-top:16px;">
    <p style="font-size:12px;color:#bbb;margin:0 0 8px;">Source breakdown</p>
    {stats_table_html(stats)}
  </div>

</body>
</html>"""

    return subject, plain, html


def send_email(listings: list[Listing], stats: list[dict]) -> None:
    subject, plain, html = build_email(listings, stats)
    resend.Emails.send({
        "from": RESEND_FROM,
        "to": RECIPIENT,
        "subject": subject,
        "html": html,
        "text": plain,
    })
    log.info("Email sent → %s | %d listing(s)", RECIPIENT, len(listings))


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

    listings, stats = gather_all(only_source=args.source)

    fpj_count = sum(1 for l in listings if l.brand == "FP Journe")
    db_count  = sum(1 for l in listings if l.brand == "De Bethune")
    log.info("Total: %d listings (%d FPJ, %d DB)", len(listings), fpj_count, db_count)

    # Always print console summary when not doing a plain production run
    if skip_email:
        print_console_summary(listings, stats)

    if args.preview:
        _, _, html = build_email(listings, stats)
        out_path = pathlib.Path(args.out).resolve()
        out_path.write_text(html, encoding="utf-8")
        log.info("Preview written → %s", out_path)
        # Open in default browser (cross-platform)
        webbrowser.open(out_path.as_uri())
        log.info("Opened in browser.")
    elif not skip_email:
        send_email(listings, stats)

    log.info("Done.")
