# Plaid & Open Banking

**Page type:** detail page (platform-API layout: h1 + subtitle + verified badge, one two-column obj-table row — text left 45%, payload + canvas right 55% — then an official-references list)
**HTML title tag:** Plaid & Open Banking — Platform APIs

**Subtitle:** Lets you read a user's bank accounts, balances, and transactions — with that user's permission, which they can withdraw at any time.

**Verified badge:** Last verified: August 2026

## Overview

**What you can get**

- Bank accounts and their current balances
- Transactions, with a cleaned-up merchant name and a spending category added by Plaid
- Loan and credit-card details: rates, minimum payments, remaining balance
- Investment holdings and trades
- Income and identity information as held by the bank

**Key point (callout):** The bank only sends a cryptic descriptor like `SQ *BLUE BOTTLE 41 SAN FRANCISC`. The friendly merchant name and the category are Plaid's guesses from that string — model output, not bank data. The guesses carry an error rate that is not uniform across merchants, and they can change retroactively when Plaid updates its model.

**Watch out for**

- Users can revoke access at any time — or just change their bank password. Your data feed simply stops, and the users who stay connected are not a random sample.
- A pending transaction gets a brand-new ID when it posts, and its amount can change or the transaction can vanish entirely. Naive upserts double-count.
- Transactions can be removed after you stored them — the sync endpoint reports these removals in a `removed` list alongside additions. Your store must handle deletes, not just inserts.
- Balance is a point-in-time reading. There is no balance history — if you want a series, you must sample it yourself going forward.

### Payload example

**Payload note (italic, above the block):** One synced transaction. Only the raw name, amount, and date come from the bank — the merchant name and category are Plaid's inference, and it says so with a confidence level.

```
{
  "amount": 6.33,
  "iso_currency_code": "USD",
  "date": "2026-08-14",
  "pending": false,

  "name": "SQ *BLUE BOTTLE 41 SAN FRANCISC",
  "merchant_name": "Blue Bottle Coffee",

  "personal_finance_category": {
    "primary": "FOOD_AND_DRINK",
    "detailed": "FOOD_AND_DRINK_COFFEE",
    "confidence_level": "VERY_HIGH"
  }
}
```

### Visualization (canvas `confChart`, responsive width × 380)

Bar chart: distribution of Plaid `confidence_level` values across five bands, with a dashed threshold marking where labels become guesses.

- **Title (bold 13px `#1a5276`, top center):** "Categorization confidence — an inference, reported as such".
- **Subtitle (italic 10px `#888`, centered):** "Shape is illustrative. The point is that a confidence_level field exists at all: the vendor is not claiming certainty."
- **Data (label, share, bar color):** VERY_HIGH 46% `#27ae60`; HIGH 29% `#1a5276`; MEDIUM 15% `#e67e22`; LOW 7% `#e74c3c`; UNKNOWN 3% `#8e44ad`.
- **Axes:** y from 0% to 50% with gridlines every 10% (light gray `#eee`, labels `#666`); x baseline gray `#999`; margins top 62, bottom 96, left 56, right 24. Bars 50% of group width, drawn at 0.85 alpha, bold percentage value label in `#2c3e50` above each bar, band label below the baseline.
- **Threshold marker:** vertical dashed red line (`#e74c3c`, dash 5/4, width 1.4) at the left edge of the MEDIUM group, from just below the top of the plot to the baseline, with italic red label to its right: "below this line the label is a guess".
- **Footer (three 11px `#666` lines, left-aligned at bottom):** "A spend-by-category chart built without filtering or weighting by confidence" / "presents the low-confidence tail with the same visual authority as the rest." / "Carry confidence through the aggregation, or state that you dropped it."

## Official API References

- [Plaid API Reference](https://plaid.com/docs/api/) — endpoint-level reference including `/transactions/sync`, `/accounts/get`, and Item management
- [Transactions](https://plaid.com/docs/transactions/) — product guide covering sync, enrichment fields, and pending-to-posted behaviour

## Regeneration instructions

- **Layout:** platform-API detail page: h1, `.subtitle` paragraph, `.verified` inline badge, `<h2>Overview</h2>` with a bottom border, one `.obj-table` row — left `<td>` (45%) holds `.section-label` headings ("What you can get", "Watch out for") with bullet lists and a `.key-point` callout between them; right `<td>` (55%) holds a `.payload-note` italic paragraph, a `pre.payload` JSON block, and the canvas — then `<h2>Official API References</h2>` with a link list.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; `.verified` 0.8em `#888` with `1px solid #e0e0e0` border, radius 4px, padding 2px 10px; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; `.section-label` bold `#1a5276`; `.obj-table` cells `1px solid #e0e0e0`, padding 16px, vertical-align top; li/p 0.93em; links `#1a5276`; `pre.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, radius 4px; `.key-point` same background/left border, padding 10px 14px, 0.93em; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** `display: block; width: 100%`, height attribute 380; drawn responsively from `getBoundingClientRect().width`, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray text `#666`/`#888`/`#2c3e50`, bar fill rgba(26,82,118,0.35).
