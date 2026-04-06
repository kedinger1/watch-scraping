# Watch Listing Monitor

A daily automated scraper that tracks pre-owned **F.P. Journe** and **De Bethune** listings across the major watch marketplaces and auction houses. Each morning it sends two brand-specific email digests — one per brand — with photos, titles, prices, and direct links to every active listing found.

Built and maintained by [The 1916 Company](https://www.1916company.com).

---

## What It Does

- Scrapes 14 sources every day at 8:00 AM Mountain Time via GitHub Actions
- Sends two HTML email digests (one F.P. Journe, one De Bethune) with a photo grid of available listings
- Tracks upcoming auction lots separately (Phillips) with estimates, sale dates, and locations
- Deduplicates listings across sources so the same watch doesn't appear twice
- Filters out sold-out listings automatically

---

## Sources

| Source | Method |
|---|---|
| **Chrono24** | Playwright + stealth (Cloudflare bypass) |
| **eBay** | eBay Browse API (OAuth) |
| **A Collected Man** | Shopify `/products.json` |
| **Wrist Aficionado** | Shopify `/products.json` |
| **G&G Timepieces** | Shopify `/products.json` |
| **WatchX NYC** | Shopify `/products.json` |
| **Hodinkee Shop** | Shopify `/products.json` |
| **WatchFinder** | HTML scraping (BeautifulSoup) |
| **WatchBox** | HTML scraping (BeautifulSoup) |
| **European Watch Co.** | Next.js RSC JSON extraction |
| **WristCheck** | Playwright + SSR title fetch |
| **Bezel** | Playwright + `__NEXT_DATA__` JSON |
| **1stDibs** | HTML scraping (BeautifulSoup) |
| **Watches of Switzerland** | HTML scraping (BeautifulSoup) |
| **Phillips** *(auctions)* | React hydration JSON extraction |

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
    title:       str   # e.g. "2024 F.P. Journe Octa Automatique Reserve Lune"
    price:       str   # e.g. "$42,500"
    image_url:   str
    listing_url: str
    source:      str   # e.g. "Bezel"
    brand:       str   # "FP Journe" | "De Bethune"

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

For GitHub Actions, add all five as **repository secrets** under
`Settings → Secrets and variables → Actions`.

### 3. Schedule

The workflow runs automatically every **Monday at 7:30 AM Eastern**.

> GitHub Actions cron runs in UTC and has no DST awareness. The cron needs a manual update twice a year to hold clock time:
> - **Mid-March (clocks spring forward to EDT, UTC−4):** set cron to `30 11 * * 1`
> - **Early November (clocks fall back to EST, UTC−5):** set cron to `30 12 * * 1`
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

## Stack

- **Python 3.12**
- [`requests`](https://docs.python-requests.org) + [`beautifulsoup4`](https://www.crummy.com/software/BeautifulSoup/) — HTTP and HTML parsing
- [`playwright`](https://playwright.dev/python/) — headless Chromium for JS-rendered sites
- [`playwright-stealth`](https://pypi.org/project/playwright-stealth/) — patches Playwright to bypass bot detection (used for Chrono24, Bezel, WristCheck)
- [`resend`](https://resend.com/docs) — transactional email delivery
- **GitHub Actions** — scheduling and execution (free tier)
