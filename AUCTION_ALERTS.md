# Auction Alert System — Design Plan

## Two triggered emails per lot (no weekly digest)

### Email 1 — New Lot Detected
Fires immediately when a new lot appears in `auction_lots` that wasn't present on the previous run.

**Subject:** `New FPJ lot — Répétition Souveraine, €400K–800K, Monaco Legend, closes Apr 25`

Content:
- Photo, title, estimate, auction house, sale date / close date
- Direct lot URL
- Registration deadline note where relevant (Invaluable requires 48hr advance)
- Link to full upcoming auctions page (see below)

### Email 2 — 48-Hour Close Reminder
Fires when a lot's `sale_date_end` (or `sale_date`) is within 48 hours and the lot is still active.

**Subject:** `Closes Thursday — FPJ Répétition Souveraine, Monaco Legend`

Content: same as Email 1, with urgency callout at top.

---

## "All Upcoming Auctions" Page

A simple hosted page (could be a static HTML file served from GitHub Pages, or a Supabase-backed page) showing all active `auction_lots` rows where `is_upcoming = true`, sorted by close date.

- George gets this link at the bottom of every alert email
- Gives him confidence that coverage is comprehensive even between alerts
- No login, no dashboard — just a clean read-only list

**Fields to show:** Brand, title, estimate, auction house, sale date open → close, lot link

---

## Implementation Notes

- New-lot detection: compare current `auction_lots` against a `first_seen_at` timestamp or a separate `alerted_new` boolean column
- 48hr reminder: query `auction_lots` where `sale_date_end` BETWEEN now AND now+48hr AND `alerted_close` IS NULL, then set `alerted_close = now`
- Both emails send via Resend, same infrastructure as daily digest
- Add `alerted_new` and `alerted_close` boolean columns to `auction_lots` Supabase table
- Triggered from the auction scrape job (weekly) — but 48hr reminders need a separate daily check, could run inside the existing daily scrape job

---

## Status
- [ ] Add `alerted_new` + `alerted_close` columns to `auction_lots`
- [ ] Build new-lot detection + email
- [ ] Build 48hr close reminder check + email
- [ ] Build "all upcoming auctions" page
- [ ] Wire 48hr check into daily scrape job
