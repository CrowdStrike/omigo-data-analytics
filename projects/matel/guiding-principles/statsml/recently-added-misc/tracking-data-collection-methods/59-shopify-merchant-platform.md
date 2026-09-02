# Tracking Data: Shopify Merchant Platform

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Shopify Merchant Platform

**Subtitle:** Many independent stores run on the same hosted software. One purchase is recorded by the store and by the platform underneath it, at different scopes.

## Section 1: What is it?

Not a marketplace and not a retailer — the software a store is built on.

- **Merchant view:** its own store only
- **Platform view:** the same kind of event across every store running on it

**Key point (callout):** **Scope, not secrecy:** both collectors are expected. The difference is how many stores each one can see, which is what makes platform-level benchmarks possible and merchant-level ones not.

### Visualization (canvas `c1`, 720×320)

Schematic diagram: two side-by-side panels showing the same four-store grid, illuminated differently by viewer scope.

- **Title (bold 14px, `#1a5276`, top center):** "One checkout on one store, seen at two scopes".
- **Panels:** rounded rects (radius 6), 286×168 at y=48; left panel at x=46 titled "Merchant analytics" in blue `#2a78d6`; right panel at x=388 titled "Platform analytics" in violet `#4a3aa7`. Panel fill: hue at alpha 0.05; stroke: hue, width 1.4.
- **Inside each panel:** a 2×2 grid of store boxes (118×46, radius 4, 14px/12px gaps), labeled "this store", "store B", "store C", "store D". Lit boxes: fill hue at alpha 0.35, stroke hue width 1.3, bold 12px name in hue, sub-label "visible" 11px in hue. Unlit boxes: white fill, stroke `#e5e9ef`, name in muted gray `#6b7280`, sub-label "not visible" in `#e5e9ef`.
  - Merchant panel lights box 0 only ("this store"); platform panel lights all four.
- **Panel notes (12px `#6b7280`, below each panel):** "its own store" / "every store on the platform".
- **Bottom line (13px `#2c3e50`, centered at y=280):** "Same event, same fields — the platform view spans stores the merchant cannot see."
- **Caption (italic 11px `#6b7280`, centered at y=300):** "Schematic — four stores stand in for many."

## Section 2: What does it collect?

- **The order:** items, quantities, price, tax, discount code
- **The visit:** browser, device size, IP, and the link the shopper arrived on
- **History with that store:** how many past orders, how much spent

It arrives two ways, and the two disagree:

- **Browser pixel** — fires from the shopper's page; lost to ad blockers, closed tabs, failed requests, with no retry
- **Server webhook** — sent after the order is committed; signed and retried

**Key point (callout):** **Trust the webhook for money:** pixel counts run below webhook counts by an unknown margin. Use the pixel as behavioural signal and watch the ratio between the two; both records carry the same checkout token, so the gap can be measured per checkout.

### Visualization (canvas `c2`, 720×320)

Grouped (paired) bar chart: browser pixel counts sitting below server webhook counts across six days.

- **Title (bold 14px `#1a5276`, top center):** "The two transports do not agree".
- **Data (hardcoded, illustrative):** labels `['Mon','Tue','Wed','Thu','Fri','Sat']`; webhook `[412, 448, 391, 505, 530, 476]`; pixel `[337, 361, 318, 402, 421, 379]`.
- **Chart area:** x=62, y=62, width 600, height 176. Y scale 0–600 with gridlines and right-aligned labels every 150 (`#e5e9ef` gridlines width 0.6, labels 12px `#6b7280`). L-shaped axes in `#1a5276`, width 1.4.
- **Bars:** 30px wide per bar, paired around each group center with 3px offset. Webhook bar (left of pair): fill `#2a78d6` at alpha 0.35, stroke `#2a78d6`. Pixel bar (right): fill `#d95926` at alpha 0.75, stroke `#d95926`.
- **Gap labels:** above each pixel bar, 11px in orange `#d95926`, showing the negative difference: "-75", "-87", "-73", "-103", "-109", "-97".
- **Day labels:** 12px `#6b7280` below the baseline.
- **Legend (12px, swatches 12×12 at y=38):** blue "server webhook (signed, retried)"; orange "browser pixel (no retry)".
- **Caption (italic 11px `#6b7280`, centered at y=300):** "Illustrative counts — the direction is the point, not the size of the gap."

### Example payload (below canvas `c2`, right column)

Visible caption above the block (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
{
  // ── documented in the Admin API order object ──
  "id": 5218837402113,
  "order_number": 10482,
  "created_at": "2026-08-19T14:24:51-04:00",
  "total_price": "302.19",
  "total_tax": "17.69",
  "financial_status": "paid",
  "fulfillment_status": null,
  // same token appears in the browser pixel event
  "checkout_token_masked": "EXAMPLE_TOKEN_redacted",  // Note: masked for illustration; real tokens are ~22 char alphanumeric
  "line_items": [
    { "sku": "NSC-HLM-M-BLK", "title": "Trailhead Helmet",
      "quantity": 1, "price": "129.00", "vendor": "Trailhead Gear" }
  ],
  "customer": { "id": 6733491802177, "orders_count": 4,
                "total_spent": "1148.72" },
  "shipping_address": { "city": "Portland", "zip": "97214" },
  "client_details": { "browser_ip": "73.14.208.61",
                      "user_agent": "Chrome/128 (Macintosh)" },
  "landing_site": "/collections/helmets?utm_source=google",
  "discount_codes": [ { "code": "RIDE10", "amount": "31.61" } ]
}
```

## Section 3: Why is it collected?

**Label (`.lbl-purpose`):** Stated purpose

- **Running the store** — fulfilling orders, tax, refunds, and telling the merchant which ads paid off

**Label (`.lbl-effect`):** Additional consequence

- The platform holds the **same event across unrelated stores**, so it can benchmark one merchant against the rest
- A shared checkout account **recognises the shopper at a store they have never used**
- Each installed app granted order access **holds its own copy** of the order

**Key point (callout):** **Customer ids are per-store:** the same shopper carries unrelated ids at two stores, so joining stores on customer id is invalid. The platform can link them on the checkout account; a merchant cannot.

### Visualization (canvas `c3`, 720×320)

Schematic diagram: same shopper at two stores — a merchant-level join fails, the platform layer beneath links them.

- **Title (bold 14px `#1a5276`, top center):** "One shopper, two unrelated customer ids".
- **Store boxes:** two rounded rects (276×78, radius 5, y=44): left at x=42 named "northsidecycles" with monospace line "customer_id 6733491802177"; right at x=402 named "harbourcoffee" with "customer_id 4102855190034". Fill blue `#2a78d6` at alpha 0.12, stroke blue width 1.3; store name bold 13px blue; id 12px monospace `#2c3e50`; sub-label 11px `#6b7280`: "issued by this store only".
- **Failed join:** dashed orange (`#d95926`, dash 5/4, width 1.6) horizontal line between the boxes at mid-height, with a white circle (radius 11, orange stroke) containing an orange X at the midpoint. Orange labels: bold 12px two lines above — "join on" / "customer id"; 11px below — "does not hold".
- **Platform layer:** rounded rect at x=42, y=190, 636×68, radius 5; fill violet `#4a3aa7` at alpha 0.12, stroke violet width 1.3. Violet arrows drop from each store box center down into the layer (line plus filled triangle head).
- **Layer text:** bold 13px violet centered: "Shared checkout account, held by the platform"; below it 12px `#2c3e50`: "keyed on the email and phone the shopper reuses  →  one shopper, both stores".
- **Caption (italic 11px `#6b7280`, centered at y=300):** "Store names and ids are invented for the illustration."

## Regeneration instructions

- **Layout:** tracking detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` paragraphs, bullets, `.lbl` labels and one `.key-point` callout; right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600; `.lede` 0.95em.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `strong` in `#1a5276`.
- **Labels:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload style:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** intrinsic 720×320 each; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Charts use a shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`, a `tint(hex, alpha)` helper for translucent fills, and a rounded-rect path helper. Red is reserved for alarm states and not used here. All chart data is hardcoded literal arrays (no Math.random).
- **Project palette reference:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
