# Tracking Data: Two-Sided Marketplace Events

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Two-Sided Marketplace Events

**Subtitle:** The platform does not own the goods. Every row carries who bought and which seller supplied it, and that second column changes what the data can answer.

## What is it?

A store that stocks nothing — it matches buyers to independent sellers and takes a fee.

- **A single retailer records** who did what to which product
- **A marketplace adds one column:** which seller's listing was involved

**One search is one record:** the platform logs the whole results page — every listing shown, in the order shown — not just the one that got clicked.

### Visualization (canvas `c1`, 720×320)

Combined bar-and-line chart: relevance by rank as bars, click share by rank as a line, both expressed relative to their own position-1 value so they share one unit and the comparison is of shape.

- **Title (bold 14px, ink `#1a5276`, top center):** "Being seen decays much faster than being relevant".
- **Data (positions 1–8, illustrative, relative to position 1 = 1.00):**
  - Relevance bars: `[1.00, 0.94, 0.90, 0.84, 0.79, 0.72, 0.65, 0.59]`
  - Click share line: `[1.00, 0.55, 0.35, 0.26, 0.18, 0.13, 0.10, 0.07]`
  - Sponsored flags: positions 1 and 4 are paid placements.
- **Plot area:** x=76, y=68, 576×176; baseline at the bottom; 8 equal slots, bars 30px wide centered per slot; x labels "pos 1"…"pos 8" (12px muted). Horizontal gridlines in `#e5e9ef` with right-aligned muted y labels at 0.00, 0.25, 0.50, 0.75, 1.00. Axes stroked ink `#1a5276` width 1.4.
- **Bars:** organic listings filled blue tint `rgba(42,120,214,0.35)` with blue `#2a78d6` stroke; sponsored positions (1 and 4) filled orange tint `rgba(217,89,38,0.45)` with orange `#d95926` stroke.
- **Line:** magenta `#d55181`, width 2.2, with 3.2px-radius dots at each position.
- **Legend (12px, at y≈48 above the plot):** blue swatch "how relevant the listing is"; orange swatch "paid placement"; magenta line sample "share of clicks it gets".
- **Y-axis caption (rotated vertical, 12px muted):** "relative to position 1".
- **Captions (bottom center):** 13px `#2c3e50` "A listing at rank 8 is still fairly relevant, but gets almost none of the clicks."; italic 11px muted "Illustrative shape — both series scaled to their own position-1 value."

## What does it collect?

- **What was shown:** the query, each listing, its rank, and whether it was a paid placement
- **What was looked at:** which listing was opened, and for how long
- **What was bought:** each line of the order with its own seller and price

**Rank has to be stored when it is shown:** a click on the top result is partly caused by being on top. Without the rank recorded at serve time, a good listing and a well-placed one look the same afterwards.

**The order is not the unit:** a basket from two sellers becomes two rows downstream, because fees, delivery promises and returns are per seller, not per order.

### Visualization (canvas `c2`, 720×320)

Split diagram: one order box on the left fanning out via elbow connectors to two per-seller rows on the right.

- **Title (bold 14px, ink `#1a5276`, top center):** "One basket, two sellers, two downstream rows".
- **Order box (left):** 168×86 at (34, 118), filled blue tint `rgba(42,120,214,0.16)`, blue `#2a78d6` stroke width 1.4. Inside, centered: bold 13px blue "one order"; 12px monospace `#2c3e50` "ORD-2026-8841027"; 12px muted "2 items, one payment".
- **Seller rows (right, each 356×86 at x=330):**
  - Top row at y=74, violet `#4a3aa7`: fill violet tint 0.12, violet stroke; bold 13px "seller slr_44812"; 12px text "shipped by the platform"; 12px muted "platform fee schedule, platform delivery promise".
  - Bottom row at y=190, orange `#d95926`: fill orange tint 0.12, orange stroke; bold 13px "seller slr_90233"; 12px text "shipped by the seller"; 12px muted "different fee, different delivery promise, own returns".
- **Connectors:** elbow lines (width 1.5) in each row's hue from the order box's right edge, via a shared mid-x, to each row's left edge, ending in a small filled triangular arrowhead.
- **Captions (bottom center):** 13px `#2c3e50` "Counting orders and counting seller performance are two different queries."; italic 11px muted "Schematic — ids invented for the illustration."

### Payload (under canvas `c2`)

Caption (italic, gray): "Sample payload — illustrative structure, not real captured data."

```
{
  // ── the identity envelope, on every event ──
  "event_name": "search_results_served",
  "event_ts": "2026-08-19T14:02:11.482Z",
  "session_id_masked": "EXAMPLE_SESSION_ID_redacted",  // Note: masked for illustration; real IDs use typed prefix + alphanumeric
  "anonymous_id_masked": "EXAMPLE_ANON_ID_redacted",   // Note: masked for illustration; user_id null until sign-in

  "query": "noise cancelling headphones",
  "result_count": 4128,
  // every listing shown, with rank captured at serve time
  "items": [
    { "item_id": "B0C7R9K2LM", "seller_id": "slr_44812",
      "position": 1, "price": 248.00, "is_sponsored": true },
    { "item_id": "B09TT4XQ7P", "seller_id": "slr_10236",
      "position": 2, "price": 279.99, "is_sponsored": false },
    { "item_id": "B0BZK1V8HD", "seller_id": "slr_90233",
      "position": 3, "price": 189.50, "is_sponsored": false }
  ]
  // a later item_view points back at this event id,
  // so query and rank stay recoverable from the purchase
}
```

## Why is it collected?

**Stated purpose** (label pill)

- **Running the market** — ordering results, charging fees, paying sellers, and catching fraudulent listings

**Additional consequence** (label pill)

- The seller column lets the platform **rank and score sellers against each other** using data no individual seller can see
- Paid and unpaid placements sit in the same log, so **the platform can price attention** it also controls

**Any per-seller rate needs the impressions, not just the sales:** two sellers with identical order counts are not performing alike if one was shown ten times as often. The denominator only exists if the seller was recorded at impression time.

### Visualization (canvas `c3`, 720×320)

Paired horizontal bars: two sellers with equal orders but very different impression counts, showing the rate as the only comparable figure.

- **Title (bold 14px, ink `#1a5276`, top center):** "Same sales, very different exposure".
- **Data (illustrative, hardcoded):** seller A — shown 3,000 times, 12 orders, blue `#2a78d6`; seller B — shown 400 times, 12 orders, aqua `#199e70`. Scale max 3,200.
- **Layout:** bars start at x=128, max width 420; rows at y=62 and y=166 (104px apart); "shown" bar height 26, "orders" bar height 14 below it (8px gap). Seller names right-aligned bold 13px in each seller's hue.
- **"Shown" bars:** width proportional to impressions, filled in the seller's hue at tint 0.28 with a solid stroke; 12px label right of bar end: "shown 3,000 times" / "shown 400 times".
- **"Orders" bars:** magenta `#d55181`, width proportional to 12/3200 (minimum 2.5px so it stays visible — nearly invisible on the shared scale by design); label "12 orders".
- **Rate labels (bold 13px `#2c3e50`, right of each orders row):** "0.4% of the times it was shown" (seller A) and "3.0% of the times it was shown" (seller B).
- **Legend (12px, y≈262):** magenta swatch "orders (same for both)"; muted-tint swatch stroked `#6b7280` "times shown (differs 7.5x)".
- **Captions (bottom center):** 13px `#2c3e50` "Ranked on orders they tie; ranked on the rate, seller B is far ahead."; italic 11px muted "Illustrative counts."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` paragraph + bullets + `.key-point` callouts, right `<td>` (55%, text-align center) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption and `.payload` `<pre>` block (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; bullets 0.93em with bold lead terms (`li b`) in `#1a5276`; inline `code` monospace 0.9em on `#f4f6f7` background, 1px 4px padding, 2px radius. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width`/`height` attributes (720×320); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own attributes. Include a `tint(hex, a)` helper producing an rgba tint of a palette token.
- **Palette:** page charts use the tracking-set validated categorical palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` for headings/axes, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and does not appear. Site-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
