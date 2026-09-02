# Amazon Selling Partner & Ads

**Page type:** detail page (platform-API layout: h1 + subtitle + verified badge, one two-column obj-table row — text left 45%, payload + canvas right 55% — then an official-references list)
**HTML title tag:** Amazon Selling Partner & Ads — Platform APIs

**Subtitle:** Lets a seller pull their orders, inventory, fees, and payouts from Amazon — and, through a separate ads API, their advertising performance.

**Verified badge:** Last verified: August 2026

## Overview

**What you can get**

- Orders and their items, with status and totals
- Warehouse (FBA) inventory: sellable, reserved, inbound
- Fees, refunds, and settlements — what Amazon actually paid you
- Bulk reports covering sales, traffic, returns, and storage
- Ad campaigns and the actual search terms shoppers typed (separate ads API)

**Key point (callout):** The numbers keep changing after the fact. Returns post late, fees get adjusted, and ad conversions accrue for days after the click — so the same report re-requested a week later legitimately returns different numbers. A recent period is not comparable to a settled one; keep the generation date on every extract and never overwrite an old one.

**Watch out for**

- Most data comes through slow, asynchronous reports: request, wait (seconds to tens of minutes), then download a compressed file — not instant API calls.
- Seller data and ads data are two separate systems with separate credentials and no shared key. Joining ad spend to sales is a join you build yourself, with your own assumptions.
- You do not get buyer identity by default. Customer-level analysis that is trivial on Shopify is largely unavailable — Amazon owns the customer.
- Fee estimates are a calculator, not your fees. Only the settlement report shows what you were actually charged.

### Payload example

**Payload note (italic, above the block):** The report pattern both APIs share: request, poll, then download a file — here nearly eight minutes later, via a link that expires in five.

```
// 1. request a report
{ "reportType": "GET_FLAT_FILE_ALL_ORDERS_...",
  "dataStartTime": "2026-07-01T00:00:00Z",
  "dataEndTime":   "2026-07-31T23:59:59Z" }
// -> { "reportId": "50919019226" }

// 2. poll until done
{ "processingStatus": "DONE",
  "createdTime":         "2026-08-22T10:00:04Z",
  "processingEndTime":   "2026-08-22T10:07:52Z",
  "reportDocumentId": "amzn1.tortuga.4.eu.T1FSAMPLE" }

// 3. get the download link, then fetch + gunzip
{ "url": "https://...s3.amazonaws.com/...&X-Amz-Expires=300",
  "compressionAlgorithm": "GZIP" }
```

### Visualization (canvas `latencyChart`, responsive width × 380)

Two-series line chart with dots: how the same report's numbers restate as it is regenerated more days after the reporting date.

- **Title (bold 13px `#1a5276`, top center):** "Restatement: the same report date, regenerated on later days".
- **Subtitle (italic 10px `#888`, centered):** "Illustrative. Attributed sales accrue over the window; returns and fee adjustments arrive late."
- **X values (days since the reporting date, labeled "+1d" … "+30d"):** `[1, 2, 3, 5, 7, 10, 14, 21, 30]`, equally spaced; x-axis caption centered below the labels: "days between the reporting date and the day the report was generated".
- **Series 1 — attributed sales (green `#27ae60`, rises toward the settled figure):** `[0.58, 0.71, 0.80, 0.89, 0.96, 0.99, 1.00, 1.00, 1.00]`.
- **Series 2 — net units (red `#e74c3c`, falls as returns post):** `[1.00, 1.00, 0.99, 0.97, 0.955, 0.94, 0.928, 0.921, 0.920]`.
- **Axes:** y from 50% to 106% with 8 gridlines labeled as whole percentages (light gray `#eee` lines, labels `#666`); rotated y-axis label "value relative to the settled figure" in 10px `#666`; x baseline gray `#999`; margins top 62, bottom 96, left 58, right 26. Lines width 2.2 with 3.4px-radius dots at every point.
- **Settled reference:** horizontal dashed line (rgba(26,82,118,0.35), dash 4/4, width 1.4) at 100%.
- **Callout on the earliest read:** vertical dashed orange line (`#e67e22`, dash 3/3, width 1.2) spanning between the two series at day +1, with italic orange label to its right: "a next-day pull understates ad performance and overstates net units".
- **Legend (color swatch bars + 11px `#2c3e50` text, bottom left):** green — "attributed sales (7d window still filling)"; red — "net units after returns and adjustments post".
- **Footer (italic 10px `#666`, left-aligned, bottom):** "ROAS computed from a next-day pull is biased in both numerator and denominator, in opposite directions."

## Official API References

- [Selling Partner API Documentation](https://developer-docs.amazon.com/sp-api/) — the SP-API docs hub: orders, finances, FBA inventory, reports, notifications
- [Amazon Ads API Documentation](https://advertising.amazon.com/API/docs/en-us) — campaigns, Reporting v3, Attribution, and Marketing Stream

## Regeneration instructions

- **Layout:** platform-API detail page: h1, `.subtitle` paragraph, `.verified` inline badge, `<h2>Overview</h2>` with a bottom border, one `.obj-table` row — left `<td>` (45%) holds `.section-label` headings ("What you can get", "Watch out for") with bullet lists and a `.key-point` callout between them; right `<td>` (55%) holds a `.payload-note` italic paragraph, a `pre.payload` JSON block, and the canvas — then `<h2>Official API References</h2>` with a link list.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; `.verified` 0.8em `#888` with `1px solid #e0e0e0` border, radius 4px, padding 2px 10px; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; `.section-label` bold `#1a5276`; `.obj-table` cells `1px solid #e0e0e0`, padding 16px, vertical-align top; li/p 0.93em; links `#1a5276`; `pre.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, radius 4px; `.key-point` same background/left border, padding 10px 14px, 0.93em; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** `display: block; width: 100%`, height attribute 380; drawn responsively from `getBoundingClientRect().width`, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#888`/`#2c3e50`, reference line rgba(26,82,118,0.35).
