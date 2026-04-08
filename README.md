# Watch Listing Monitor

A daily automated scraper that tracks pre-owned **F.P. Journe** and **De Bethune** listings across the major watch marketplaces and auction houses. Each morning it sends two brand-specific email digests — one per brand — with photos, titles, prices, and direct links to every active listing found.

Built and maintained by [The 1916 Company](https://www.1916company.com).

---

## What It Does

- Scrapes 13 sources every day at 7:30 AM Eastern via GitHub Actions
- Sends two HTML email digests (one F.P. Journe, one De Bethune) with a photo grid of available listings
- Tracks upcoming auction lots separately (Phillips) with estimates, sale dates, and locations
- Deduplicates listings across sources so the same watch doesn't appear twice
- Filters out sold-out listings automatically
- Persists all listings to a Supabase PostgreSQL database with price history tracking and inactive marking

---

## Sources

| Source | Method |
|---|---|
| **Chrono24** | Playwright + Bright Data residential proxy — De Bethune only (see note below) |
| **eBay** | eBay Browse API (OAuth) |
| **A Collected Man** | Shopify `/products.json` |
| **Wrist Aficionado** | Shopify `/products.json` |
| **G&G Timepieces** | Shopify `/products.json` |
| **WatchX NYC** | Shopify `/products.json` |
| **Hodinkee Shop** | Shopify `/products.json` |
| **WatchFinder** | HTML scraping (BeautifulSoup) |
| **European Watch Co.** | Next.js RSC JSON extraction |
| **WristCheck** | Playwright + SSR title fetch |
| **Bezel** | Playwright + `__NEXT_DATA__` JSON |
| **1stDibs** | HTML scraping (BeautifulSoup) |
| **Watches of Switzerland** | HTML scraping (BeautifulSoup) |
| **Phillips** *(auctions)* | React hydration JSON extraction |
| **Sotheby's** *(auctions)* | Algolia index JSON |
| **Christie's** *(auctions)* | Playwright + Bright Data proxy + `__NEXT_DATA__` fallback |
| **Barnebys** *(auctions)* | `__redux` embedded JSON (React/Algolia search) |

> **Note on Chrono24:** Chrono24 surfaces ~900 FP Journe and ~96 De Bethune listings, but per internal intel approximately 70% are broker placeholder listings — the watch is not held inventory; the dealer will source it through their network if they get a hit. Including Chrono24 would flood the digest with noise and undermine the signal from sources where the watch actually exists. It remains in the codebase but is intentionally not connected.

---

## Auction Source Pipeline

Sources ranked by implementation priority. Phillips, Sotheby's, and Christie's are live.

| Priority | Source | Coverage | Approach |
|---|---|---|---|
| 1 | ~~**Barnebys**~~ | ✅ Live — Europe-wide aggregator | `__redux` embedded JSON |
| 2 | **LiveAuctioneers** | US aggregator, hundreds of small houses | XHR JSON (React SPA) — confirmed FPJ/DB lots |
| 3 | **Invaluable** | Broadest small-house coverage globally | Reverse-engineer search XHR — no public API |
| 4 | **Antiquorum** | Watch specialist — different buyer pool, pricing signal | XHR on catalog.antiquorum.swiss |
| 5 | **Drouot** | French market | Server-rendered HTML |

**Skipped:** Heritage Auctions (actively anti-scrape, overlapping US audience), Fellows (near-zero FPJ/De Bethune presence).

---

## Architecture

```
scraper.py
├── Scraper functions       One per source, returns list[Listing]
├── gather_all()            Runs all scrapers sequentially, global dedup
├── build_email(brand, …)   Renders HTML email for one brand
└── send_emails(…)          Fires two Resend API calls (FPJ + DB)
```

**Data model:**

```python
@dataclass
class Listing:
    title:            str   # e.g. "2024 F.P. Journe Octa Automatique Reserve Lune"
    price:            str   # e.g. "$42,500"
    image_url:        str
    listing_url:      str
    source:           str   # e.g. "Bezel"
    brand:            str   # "FP Journe" | "De Bethune"
    reference_number: str   # e.g. "FPJ-39-RG" — populated where available

@dataclass
class AuctionLot:
    title:         str   # "F.P. Journe Centigraphe Souverain"
    estimate:      str   # "$80,000 – $160,000"
    sale_date:     str   # "May 12, 2026"
    sale_location: str   # "New York" | "Geneva" | "Hong Kong"
    image_url:     str
    lot_url:       str
    brand:         str
```

**JS-rendered sites** (Bezel, WristCheck) use Playwright headless Chromium, installed once per Actions run. All other sources use `requests` + BeautifulSoup or structured JSON APIs, keeping the run well under the 20-minute job timeout.

---

## Setup

### 1. Clone & install dependencies (local)

```bash
git clone https://github.com/kedinger1/watch-scraping.git
cd watch-scraping
pip install -r requirements.txt
playwright install chromium --with-deps
```

### 2. Environment variables

| Variable | Description |
|---|---|
| `RESEND_API_KEY` | [Resend](https://resend.com) API key |
| `RESEND_FROM` | Verified sender, e.g. `Watch Monitor <watch@1916co.com>` |
| `RECIPIENT_EMAIL` | Destination address for the digests |
| `EBAY_CLIENT_ID` | eBay Developer App Client ID |
| `EBAY_CLIENT_SECRET` | eBay Developer App Client Secret |
| `SUPABASE_URL` | Supabase project URL, e.g. `https://xxxx.supabase.co` |
| `SUPABASE_KEY` | Supabase **service role** secret key (server-side writes) |

For GitHub Actions, add all seven as **repository secrets** under
`Settings → Secrets and variables → Actions`.

### 3. Schedule

The workflow runs automatically every day at **7:30 AM Eastern**.

> GitHub Actions cron runs in UTC and has no DST awareness. The cron needs a manual update twice a year to hold clock time:
> - **Mid-March (clocks spring forward to EDT, UTC−4):** set cron to `30 11 * * *`
> - **Early November (clocks fall back to EST, UTC−5):** set cron to `30 12 * * *`
You can also trigger it manually via the **"Run workflow"** button in the Actions tab.

---

## Local Development

Run the full scraper without sending email:

```bash
python scraper.py --no-email
```

Open a live HTML preview in your browser (saves `preview-fpj.html` + `preview-db.html`):

```bash
python scraper.py --preview
```

Test a single source:

```bash
python scraper.py --source bezel --preview
python scraper.py --source wristcheck
python scraper.py --source chrono24 --no-email
```

List all available source names:

```bash
python scraper.py --list-sources
```

> **Note:** `--source` and `--preview` do not require `RESEND_API_KEY` to be set.

---

## Database

Listings are persisted to Supabase PostgreSQL on every full run. Run the following SQL once in your Supabase SQL editor to create the schema:

```sql
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
```

**What gets written each run:**
- Every active listing is upserted (insert or update) keyed on its URL
- A `price_history` row is written whenever a listing's price changes (or it's first seen)
- Listings that were previously active but not seen this run are marked `is_active = false`

Reference numbers are captured from sources that expose them: Bezel (`referenceNumber`), WatchFinder (`ModelNumber`), Shopify (variant SKU), and European Watch Co. (reference field).

---

## Stack

- **Python 3.12**
- [`requests`](https://docs.python-requests.org) + [`beautifulsoup4`](https://www.crummy.com/software/BeautifulSoup/) — HTTP and HTML parsing
- [`playwright`](https://playwright.dev/python/) — headless Chromium for JS-rendered sites
- [`playwright-stealth`](https://pypi.org/project/playwright-stealth/) — patches Playwright to bypass bot detection (used for Chrono24, Bezel, WristCheck)
- [`resend`](https://resend.com/docs) — transactional email delivery
- [`supabase`](https://supabase.com/docs/reference/python) — PostgreSQL persistence (listings + price history)
- **GitHub Actions** — scheduling and execution (free tier)
