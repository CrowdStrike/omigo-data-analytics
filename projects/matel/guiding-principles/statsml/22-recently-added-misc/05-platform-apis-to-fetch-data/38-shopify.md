# Shopify

**Page type:** detail page (platform-API layout: h1 + subtitle + verified badge, one two-column obj-table row — text left 45%, payload + canvas right 55% — then an official-references list)
**HTML title tag:** Shopify — Platform APIs

**Subtitle:** Lets you pull orders, customers, products, and inventory out of a Shopify store.

**Verified badge:** Last verified: August 2026

## Overview

**What you can get**

- Orders with their line items, discounts, taxes, and refunds
- Customers and their order history
- Abandoned checkouts — carts that reached checkout but never converted
- Products, variants, and current inventory per location
- Fulfillments and shipment tracking

**Key point (callout):** By default an app only sees roughly the last 60 days of orders. Older orders need a separate, explicitly granted permission — and the API does not tell you rows were withheld. A backfill that "works" but returns nothing older than two months is almost always this, not a bug.

**Watch out for**

- Orders change after creation — refunds, edits, cancellations rewrite totals. Re-sync on last-updated time, not created time, and filter out test orders.
- Requests draw from a per-shop cost budget that refills at a steady rate. Asking for huge pages drains it faster — smaller pages often give more sustained throughput.
- Full history extracts should use bulk operations, which return a flat file you reassemble. One bulk query and one bulk mutation can run at the same time per shop per app — but not two of the same kind.
- Inventory is a now-only number. There is no stock history; if you want one, sample it yourself going forward.

### Payload example

**Payload note (italic, above the block):** Every GraphQL response reports what your query cost and how much budget remains — the numbers a well-behaved client paces itself by.

```
"extensions": {
  "cost": {
    "requestedQueryCost": 302,
    "actualQueryCost": 47,
    "throttleStatus": {
      "maximumAvailable": 2000.0,
      "currentlyAvailable": 1953,
      "restoreRate": 100.0
    }
  }
}
```

### Visualization (canvas `bucketChart`, responsive width × 380)

Two-series simulated line chart: bucket level over time for a burst client vs a paced client under Shopify's cost-based leaky bucket.

- **Title (bold 13px `#1a5276`, top center):** "Leaky bucket under burst vs paced load".
- **Subtitle (italic 10px `#888`, centered):** "Simulated. Capacity and restore rate are per-shop and plan-dependent; the mechanism is what matters."
- **Simulation (data generated at draw time, 41 points, ticks 0–40):** capacity CAP = 2000, restore = 100 points per tick, burst query cost = 260, paced query cost = 95. Both buckets start at 2000; each tick the current level is recorded, then the bucket restores (capped at 2000), then: the burst client always attempts a 260-cost query (spends if the bucket holds ≥ 260, otherwise it is throttled); the paced client spends 95 only if doing so keeps the bucket at or above 35% of capacity (700). Result: the red burst line drains steeply to the throttle floor and sawtooths there; the green paced line settles into a stable high band.
- **Axes:** y from 0 to 2000 with gridlines at 0/500/1000/1500/2000 (light gray `#eee`, labels `#666`); rotated y-axis label "currentlyAvailable (points)" in 10px `#666`; x baseline gray `#999`; x-axis caption centered below: "time (bucket refills at restoreRate = 100/tick)"; margins top 60, bottom 84, left 58, right 22.
- **Throttle floor band:** the region below y = 260 filled rgba(231,76,60,0.08), topped by a horizontal dashed red line (`#e74c3c`, dash 4/4, width 1) at y = 260, with italic red label just below it: "below requestedQueryCost → THROTTLED".
- **Series:** paced client `#27ae60` width 2.2; burst client `#e74c3c` width 2.2; area under the burst line filled rgba(26,82,118,0.35) at 0.25 alpha to emphasize depletion.
- **Legend (color swatch bars + 11px `#2c3e50` text, bottom left):** red — "burst client — first: 250 every request (cost 260)"; green — "paced client — smaller pages, reads throttleStatus (cost 95)".
- **Footer (italic 10px `#666`, left-aligned, bottom):** "The burst client drains to the floor and thereafter completes roughly one query per 3 ticks. Bigger pages bought less throughput."

## Official API References

- [GraphQL Admin API](https://shopify.dev/docs/api/admin-graphql) — the primary API reference: orders, customers, products, fulfillment orders
- [Rate Limits](https://shopify.dev/docs/api/usage/rate-limits) — the cost-based leaky bucket, `throttleStatus`, and REST call limits

## Regeneration instructions

- **Layout:** platform-API detail page: h1, `.subtitle` paragraph, `.verified` inline badge, `<h2>Overview</h2>` with a bottom border, one `.obj-table` row — left `<td>` (45%) holds `.section-label` headings ("What you can get", "Watch out for") with bullet lists and a `.key-point` callout between them; right `<td>` (55%) holds a `.payload-note` italic paragraph, a `pre.payload` JSON block, and the canvas — then `<h2>Official API References</h2>` with a link list.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; `.verified` 0.8em `#888` with `1px solid #e0e0e0` border, radius 4px, padding 2px 10px; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; `.section-label` bold `#1a5276`; `.obj-table` cells `1px solid #e0e0e0`, padding 16px, vertical-align top; li/p 0.93em; links `#1a5276`; `pre.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, radius 4px; `.key-point` same background/left border, padding 10px 14px, 0.93em; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** `display: block; width: 100%`, height attribute 380; drawn responsively from `getBoundingClientRect().width`, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize; the two series are computed by the in-page simulation described above, not hardcoded arrays.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#888`/`#2c3e50`, depletion fill rgba(26,82,118,0.35), throttle band rgba(231,76,60,0.08).
