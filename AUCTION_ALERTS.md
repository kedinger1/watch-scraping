# Auction Alert System — Design Plan

## Two triggered emails per sale event (not per lot)

The meaningful unit is the **sale event**, not the individual lot. George doesn't need 16 emails for Sotheby's Important Watches 4 — he needs one email saying "Sotheby's has a sale coming with 15 FPJ + 1 DB."

---

### Email 1 — New Sale Event Detected
Fires once when a new `sale_name` + `auction_house` combination appears in `auction_lots` for the first time.

**Subject:** `Sotheby's — Important Watches 4, Hong Kong, Apr 24 — 15 FPJ · 1 DB`

Content:
- Sale name, auction house, location, open → close date
- FPJ lots listed with title, estimate, lot URL (photo + one-liner)
- DB lots listed the same way
- Registration deadline note where relevant (Invaluable requires 48hr advance)
- Link to full upcoming auctions page (see below)

**Trigger logic:** Query `auction_lots` grouped by `(auction_house, sale_name)` where `alerted_new IS NULL`. Fire one email per new group, then mark all rows in that group `alerted_new = now`.

---

### Email 2 — 48-Hour Close Reminder
Fires once per sale event when the sale is closing within 48 hours and hasn't been reminded yet.

**Subject:** `Closes Thursday — Sotheby's Important Watches 4, Hong Kong (15 FPJ · 1 DB)`

Content: same as Email 1, urgency callout at top ("Sale closes in ~X hours").

**Trigger logic:** Query `auction_lots` grouped by `(auction_house, sale_name)` where `sale_date` (or `sale_date_end`) is within 48 hours AND `alerted_close IS NULL`. Fire one email per group, mark `alerted_close = now`.

---

## "All Upcoming Auctions" Page

A simple hosted page showing all `auction_lots` where `is_upcoming = true`, grouped by sale event and sorted by close date.

- George gets this link at the bottom of every alert email
- Gives him a complete picture of what's coming even between alerts
- No login, no dashboard — clean read-only list

**Layout:** One card per sale event showing auction house, sale name, location, date range, FPJ count, DB count, and expandable lot list.

---

## Supabase Schema Changes Needed

```sql
ALTER TABLE auction_lots ADD COLUMN IF NOT EXISTS alerted_new   TIMESTAMPTZ;
ALTER TABLE auction_lots ADD COLUMN IF NOT EXISTS alerted_close TIMESTAMPTZ;
```

---

## Implementation Notes

- Group key: `(auction_house, sale_name)` — uniquely identifies a sale event across all sources
- Edge case: if a sale adds new lots after the first alert (e.g. Sotheby's publishes lots in waves), only alert on the first detection. Don't re-alert on incremental additions.
- 48hr reminder fires from the **daily scrape job** (scrape.yml), not the weekly auction job — so it catches close dates regardless of when the weekly ran
- Both emails via Resend, same infrastructure as daily digest

---

## Status
- [ ] Add `alerted_new` + `alerted_close` columns to `auction_lots`
- [ ] Build new sale event detection + Email 1
- [ ] Build 48hr close reminder + Email 2
- [ ] Build "all upcoming auctions" page
- [ ] Wire 48hr check into daily scrape job (scrape.yml)
