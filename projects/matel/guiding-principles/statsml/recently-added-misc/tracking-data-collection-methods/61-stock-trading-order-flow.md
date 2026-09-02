# Tracking Data: Stock Trading & Order Flow

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Stock Trading & Order Flow

**Subtitle:** Commission-free brokers route retail orders to wholesale market makers, which pay for that flow. The order is visible to the market maker before it is filled.

## Section 1: What is it?

The order goes to a wholesaler, not to a public exchange.

- **Routing:** a commission-free broker sends it to a wholesale market maker
- **Payment for order flow:** the market maker pays for that flow, disclosed in regulatory filings
- **Internalisation:** the fill comes from its own inventory, not from an exchange match
- **Where the money is:** the spread between what it pays and what it sells at
- **In return:** the fill is typically slightly better than the public quote

### Visualization (canvas `c1`, 720×320)

Flow diagram: the order reaches a wholesale market maker before the market.

- **Three boxes on one horizontal axis (vertically centered around h/2−30):**
  - Left, blue `#2a78d6` filled rect 100×40 at x=30: white bold 17px "YOU".
  - Middle, magenta `#d55181` filled rect 160×50 at x=270: white bold 15px "MARKET MAKER" plus 13px "(sees the order before it fills)".
  - Right, green `#008300` filled rect 120×40 at x=560: white bold 17px "MARKET".
- **Arrow 1 (You → market maker):** orange `#d95926`, width 3, with arrowhead; label above in 14px orange: "Order routed here".
- **Arrow 2 (market maker → market):** green `#008300`, width 3, with arrowhead; label above in bold 15px magenta: "Delay: milliseconds".
- **Bottom:** dashed gray (`#6b7280`, dash 5/5, width 1.5) horizontal line spanning box-to-box below the row, with 14px gray caption: "Simplified view: order → market, with no intermediary".

## Section 2: What does it collect?

- **Every buy and sell order** with exact timing, to the millisecond
- **Position sizes** and portfolio composition
- **Trading patterns** and frequency
- **Watchlist additions**
- **Selling during market drops**, and reaction time to breaking news
- **Total amount invested**
- **Risk tolerance**, inferred from behaviour
- **App session frequency**

**Key point (callout):** **Only three fields are observed:** `exec_price` and the two timestamps. `improvement_bps` is a difference against a `reference_price` somebody had to choose.

**Key point (callout):** **The benchmark moves the answer:** the quote at receipt, at execution, and at the midpoint are different numbers on a moving book, and 77 ms is long enough for them to diverge. Swap the benchmark and the same fill scores differently.

**Key point (callout):** **So comparison needs a common rule:** comparing two brokers is only meaningful if both computed it the same way — which is why the reporting rules specify the reference rather than leaving it to the firm.

### Visualization (canvas `c2`, 720×320)

Event timeline: the life of one retail order, showing the market maker's millisecond advantage.

- **Timeline base:** horizontal line in `#e5e9ef`, width 3, from x=60 to x=660 at mid-height.
- **Five events**, each a colored dot (radius 6) on the line with a colored vertical stem (±40px, width 1.5) and a centered 14px multi-line label, alternating above/below:
  - x=100, blue `#2a78d6`, above: "You click BUY"
  - x=200, orange `#d95926`, below: "Order routed to" / "market maker"
  - x=320, magenta `#d55181`, above: "Market maker sees" / "the order"
  - x=430, magenta `#d55181`, below: "Fills from inventory" / "or hedges"
  - x=560, blue `#2a78d6`, above: "Order fills at the" / "quoted or better price"
- **Bottom line (bold 15px magenta, centered):** "Per-trade margin is small; the volume is what makes it material".

### Example payload (below canvas `c2`, right column)

Visible caption above the block (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// A broker's internal order record is not published.
// Field names are reconstruction; NBBO and the
// standard order attributes are spec terms.
{
  // ── present in the raw order record ──
  "order_id":     "o_1d7c…",
  "symbol":       "XYZ",
  "side":         "buy",
  "order_type":   "market",
  "received_ts":  "2026-08-22T14:31:07.412Z",
  "exec_ts":      "2026-08-22T14:31:07.489Z",
  "venue":        "wholesaler-B",
  "exec_price":   "…",

  // ── inferred / derived downstream ──
  "reference_price": "…",     // NBBO midpoint at
                              // which timestamp?
  "benchmark":       "nbbo_midpoint_at_receipt",
  "improvement_bps": "…"      // exec vs reference
}
```

## Section 3: Why is it collected?

**Label (`.lbl-purpose`):** Stated purpose

- **Funds commission-free trading**
- **Delivers price improvement** relative to the public quote

**Label (`.lbl-effect`):** Additional consequence

- Retail flow is worth paying for because it is, on average, **less informed** — a large fund's order may signal that the price is about to move; a retail order **usually does not**
- That **predictability** is what makes the flow profitable to fill

**Key point (callout):** **The contested question:** not whether price improvement happens — it does — but whether what is passed back is as large as competitive routing would have produced. An empirical question, and why the disclosures exist.

**Key point (callout):** **One venue is not a portfolio:** the record exists only where an order reached this app, so holdings and trades at another broker are absent rather than zero. A risk profile inferred from it describes activity at one venue, not an investor.

### Visualization (canvas `c3`, 720×320)

Stacked horizontal proportion bar: what the app can see of one investor's holdings, and the risk label each view implies. The absent part is not zero, it is unobserved.

- **Title (bold 14px `#1a5276`, top center):** "One household portfolio, and the slice that reaches this app".
- **Subtitle (12px `#6b7280`):** "percent of total invested — the risk profile is fitted to the left-hand block alone".
- **Bar:** from x=46 to x=674, y=92, height 46, split into four proportional slices (illustrative household portfolio, percent of total; only the first slice ever generated an order at this app):
  - 14% "single stocks and options here", orange `#d95926`, seen — fill tint alpha 0.42, solid stroke width 2, bold label
  - 47% "index funds at another broker", blue `#2a78d6`, unseen — fill tint alpha 0.12, dashed stroke (dash 4/3) width 1
  - 28% "workplace pension", aqua `#199e70`, unseen — same dashed treatment
  - 11% "cash and savings", violet `#4a3aa7`, unseen — same dashed treatment
- **In-slice labels:** bold 13px percent value ("14%", "47%", "28%", "11%") in each slice's hue, centered.
- **Slice name labels:** below the bar, staggered on two levels (20px / 38px below) with thin tinted leader lines from each slice center; seen slice's label bold, others regular 12px, each in its hue, clamped within canvas margins.
- **Two verdicts above the bar:** an orange bracket line (width 2) over the seen slice only, with left-aligned bold 12px orange text: "observed  →  “high risk tolerance”"; right-aligned bold 12px blue text: "unobserved, and not zero  →  the portfolio is mostly conservative".
- **Bottom captions (centered):** italic 12px `#2c3e50`: "The profile describes activity at one venue, not an investor."; italic 11px `#6b7280`: "Illustrative split — the proportions show the shape, not a surveyed household."

## Regeneration instructions

- **Layout:** tracking detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` paragraph, bullets, `.lbl` labels and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600; `.lede` 0.95em.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `strong` in `#1a5276`.
- **Labels:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload style:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** intrinsic 720×320 each; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Charts use a shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`, a `tint(hex, alpha)` helper for translucent fills, and a rounded-rect path helper. Red is reserved for alarm states and not used here. All chart data is hardcoded literal arrays (no Math.random).
- **Project palette reference:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
