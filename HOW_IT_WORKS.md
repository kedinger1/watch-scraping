# Watch Monitor — How It Works

We run an automated system that tracks every F.P. Journe, De Bethune, Greubel Forsey, and Daniel Roth watch listed for sale or coming up at auction — across the internet, every day, without lifting a finger.

---

## What It Does

**Every morning at 7:30 AM**, a bot wakes up and scrapes ~15 marketplaces for pre-owned listings. It sends a digest email per brand with photos, prices, and direct links.

**Every Monday at 10:00 AM**, a second bot scans the major auction houses for upcoming lots. It sends an alert the moment a new sale is discovered, and again when a sale closes within 48 hours.

---

## Marketplaces Monitored (Daily)

| Source | Notes |
|---|---|
| Chrono24 | Via residential proxy to bypass bot protection |
| eBay | Via official Browse API |
| A Collected Man | |
| Wrist Aficionado | |
| G&G Timepieces | |
| WatchX NYC | |
| Hodinkee Shop | |
| WatchFinder | Via Algolia search API |
| European Watch Co. | |
| WristCheck | |
| Bezel | |
| 1stDibs | |
| Watches of Switzerland | |

## Auction Houses Monitored (Weekly)

Phillips · Sotheby's · Christie's · Loupe This · Invaluable · Barnebys · LiveAuctioneers · Antiquorum

---

## The Emails

- **One email per brand** — FPJ, DB, GF, and DR each get their own digest
- **New listings are highlighted** in gold so you can spot fresh inventory instantly
- **Auction alerts** fire immediately when a new sale appears, with lot photos and estimates
- **Closing reminders** go out when a sale is within 48 hours

---

## Behind the Scenes

```
GitHub Actions (cloud)
  │
  ├── scrape.yml         runs daily @ 7:30 AM EDT
  │     └── scraper.py   hits all marketplaces → saves to Supabase → sends emails
  │
  └── scrape-auctions.yml   runs weekly Monday @ 10 AM EDT
        └── scraper.py --auctions-only
              hits all auction houses → saves to Supabase → sends alerts
```

- **Python** scraper running in GitHub Actions (free CI/CD — no server needed)
- **Supabase** (Postgres) stores all listings and auction lots with timestamps
- **Resend** delivers the emails
- **Netlify** hosts the live auction dashboard at [auctionmonitoring.netlify.app](https://auctionmonitoring.netlify.app)
- **Bright Data** residential proxy lets us scrape Chrono24, Christie's, and Invaluable without getting blocked

---

## The Auction Dashboard

A live webpage showing all upcoming lots across every monitored auction house — grouped by sale, with photos and estimates. Pulls directly from the database so it's always current after each Monday run.

→ **[auctionmonitoring.netlify.app](https://auctionmonitoring.netlify.app)**

---

## What Gets Filtered Out

- Our own inventory (The 1916 Company / WatchBox listings are excluded)
- Closed auctions and sold listings
- Duplicate listings of the same watch across multiple platforms
- Non-watch lots that happen to mention a maker's name (e.g. F.P. Journe cufflinks)

---

## Questions / Changes

The scraper code lives at [github.com/kedinger1/watch-scraping](https://github.com/kedinger1/watch-scraping). Adding a new marketplace, brand, or auction house is typically a small code change.
