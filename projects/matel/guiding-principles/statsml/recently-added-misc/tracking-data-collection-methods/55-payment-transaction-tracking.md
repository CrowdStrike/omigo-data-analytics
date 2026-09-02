# Tracking Data: Payment & Transaction Tracking

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Payment & Transaction Tracking

**Subtitle:** A card payment is authorised by passing a record between several parties. Each keeps a copy, and each copy carries a category code and a descriptor string built for a statement line.

## What is it?

A record passed between parties so each can decide whether to approve.

- **Route:** terminal to acquirer, across the card network, to the issuing bank, and back
- **Each hop retains it** — every one needed the record to decide
- **Settlement is separate,** and later, so a transaction can appear, change or disappear after the fact
- **Built for authorisation, not analysis** — a numeric category code plus a short descriptor string for a statement line

**Merchant name is a guess:** the field is not supplied, so the name in a spending app is usually parsed out of the descriptor string.

### Visualization (canvas `c1`, 720×320)

Flow diagram: one authorisation record in the center, dashed arrows radiating to the five parties on the path, plus a settlement footer band.

- **Title (bold 15px, ink `#1a5276`, top center):** "One authorisation, passed hop by hop".
- **Center record:** ink `#1a5276` rounded rectangle 80×50 (radius 8) centered at (w/2, 128), white bold 13px text "AUTH" / "REQUEST".
- **Five recipients, each with a dashed line (width 2, dash 4/3) from the center, an arrowhead at the recipient end, and a bold 14px label, all in that hop's hue:**
  - "1 Terminal" at (360, 48), blue `#2a78d6`
  - "2 Acquirer" at (132, 76), green `#008300`
  - "3 Card network" at (588, 76), violet `#4a3aa7`
  - "4 Issuing bank" at (132, 190), orange `#d95926`
  - "5 Merchant" at (588, 190), aqua `#199e70`
- **Footer band (y=232, height 66, full width minus 20px margins):** background magenta tint `rgba(213,81,129,0.10)` with a solid 4px magenta `#d55181` left edge. Bold 13px text `#2c3e50` over two lines: "Each of the five hops must see the record to approve or decline it," / "so each can retain a copy of the same authorisation." Third line in 13px magenta `#d55181`: "Settlement (magenta) is a later, separate step — which is why a row can change after it appears."

## What does it collect?

- **Amount and currency,** in minor units
- **Authorisation timestamp,** and a later settlement timestamp
- **Merchant category code** — a coarse ISO classification
- **Descriptor string,** formatted for a statement line
- **Card entry mode** and the last four digits
- **Acquirer reference** for the transaction
- **Derived:** merchant name and id, parsed from the descriptor
- **Derived:** a category label and a recurring-payment flag

**Merchant detail sits below the split:** the descriptor was built for a statement line — facilitator prefixes, truncation, inconsistent spacing. Any per-merchant figure depends on a parsing step with its own error rate, visible here as `parse_conf`.

**Coarse and inconsistent:** two records for the same shop can land under different merchant IDs, and one MCC covers a wide range of businesses.

### Visualization (canvas `c2`, 720×320)

Calendar heatmap: spending density over 5 weeks × 7 days, single-hue blue ramp, weekend rows marked in violet, with a two-item footer legend strip.

- **Title (bold 15px, ink `#1a5276`, top center):** "Recurring structure in a card stream — illustrative, not recorded data".
- **Grid:** 5 week columns × 7 day rows; cells 70×26 (drawn 66×22 with 4px gutters), origin (80, 40). Day labels right-aligned at x=70, bold 13px — "Mon", "Tue", "Wed", "Thu", "Fri" in blue `#2a78d6`; "Sat", "Sun" in violet `#4a3aa7`.
- **Data (rows = weeks, columns = Mon…Sun, values are intensities 0–1):**
  - Week 1: `[0.2, 0.3, 0.1, 0.4, 0.9, 0.1, 0.2]`
  - Week 2: `[0.8, 0.5, 0.3, 0.2, 0.6, 0.7, 0.3]` (payday Fri week 1)
  - Week 3: `[0.3, 0.2, 0.1, 0.2, 0.3, 0.8, 0.9]` (weekend splurge)
  - Week 4: `[0.2, 0.1, 0.1, 0.1, 0.9, 0.4, 0.3]` (payday)
  - Week 5: `[0.7, 0.6, 0.4, 0.2, 0.3, 0.8, 0.7]`
- **Cell fill:** blue `#2a78d6` at alpha = value × 0.75 (single-hue magnitude ramp). Cell outline: light gray `#e5e9ef` for weekday rows, violet tint `rgba(74,58,167,0.55)` for Sat/Sun rows.
- **Ramp legend (right of grid):** bold 12px blue label "card total", four 16×14 swatches at alphas 0.15/0.35/0.55/0.75 outlined `#e5e9ef`, muted 12px caption "low → high"; below it bold 12px violet two-line note "violet rows —" / "Sat and Sun".
- **Footer strip (y=232, height 66, background `#e5e9ef`):** green `#008300` 9×9 swatch beside bold 13px "A regular cycle is structure the record carries — the cause of it is not."; orange `#d95926` 9×9 swatch beside bold 13px "A pale cell means little card activity, not little spending —" and a regular 13px continuation "cash leaves no row at all, so a category total is a lower bound of unknown tightness."

### Payload (under canvas `c2`)

Caption (italic, gray): "Sample payload — illustrative structure, not real captured data."

```
{
  // ── standard fields carried on a card transaction ──
  "amount":       { "value": 4137, "currency": "USD" },  // minor units
  "auth_ts":      "2026-08-22T19:02:11Z",
  "mcc":          "5812",          // ISO 18245 category: eating places
  "descriptor":   "SQ *THE CORNER  AUSTIN     TX",
  "card_last4":   "…4417",
  "entry_mode":   "contactless",
  "acquirer_ref": "7710…",
  "settled_ts":   null,            // authorised, not yet settled

  // ── inferred / plausible, added downstream ──
  "merchant_name":  "The Corner",  // parsed out of descriptor
  "merchant_id":    "m_88213",     // assigned by the normalisation step
  "category_label": "Restaurants",
  "is_recurring":   false,
  "parse_conf":     0.71
}
```

## Why is it collected?

**Stated purpose** (label pill)

- **Authorise, clear and settle** — each step needs the record
- **Fraud detection** on the same data, and genuinely effective: a card used in two distant places within minutes is a pattern only the full history reveals

**Additional consequence** (label pill)

- The same history supports **aggregate spending products and audience segments**
- The **category code** does most of that work — built to route a charge, it doubles as a coarse proxy for the kind of place a person went

**"Aggregated" and "anonymous" are different claims:** aggregation only protects while a cell holds enough people, and a merchant-plus-day cell in a small town may hold one. Stripping the name also leaves the pattern, and a spending pattern over time is close to unique — a published result in the re-identification literature, not an inference from this page.

### Visualization (canvas `c3`, 720×320)

Horizontal bar chart on a log-10 x-axis: people left in one cell of an "aggregated" report as breakdown dimensions are added.

- **Title (bold 14px, ink `#1a5276`, center):** "People left in one cell of an "aggregated" report". Subtitle (12px muted `#6b7280`): "each row adds one breakdown to the same released figures".
- **Bars (5 rows, height 22, 12px gaps, starting y=66; left row labels right-aligned 13px `#2c3e50` in the form "A × B"; bold 13px value labels just right of each bar end):**
  - "City × month" — ~90,000 (n=90000)
  - "District × month" — ~7,000 (n=7000)
  - "District × day" — ~240 (n=240)
  - "Merchant × day" — ~30 (n=30)
  - "Merchant × day, small town" — 1 (n=1)
- **Scale:** log10 x from 0.7 to 200,000; plot area from x=214 to w−96; baseline at y=240. Vertical gridlines in `#e5e9ef` with 11px muted tick labels at 1, 100, 10k, 200k.
- **Bar colors:** blue `#2a78d6` (fill alpha 0.30, stroke width 1.2) for cells with n>1; the n=1 bar in orange `#d95926` (fill alpha 0.45), its value label also orange.
- **Annotations:** bold 12px orange line below the axis at x=214: "a cell of one is a single person's spending, still labelled aggregated". Centered italic 11px muted caption at the bottom: "Illustrative counts — log scale so the collapse fits. Not measured from any scheme."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` paragraph + bullets + `.key-point` callouts, right `<td>` (55%, text-align center) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption and `.payload` `<pre>` block (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; bullets 0.93em with bold lead terms (`li b`) in `#1a5276`. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width`/`height` attributes (720×320); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own attributes. Include helpers: `tint(hex, a)` producing an rgba tint of a palette token, and a rounded-rect path helper `rr()`.
- **Palette:** page charts use the tracking-set validated categorical palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` for headings/axes, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and does not appear. Site-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
