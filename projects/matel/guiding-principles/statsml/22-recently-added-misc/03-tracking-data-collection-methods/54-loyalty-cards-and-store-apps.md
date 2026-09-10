# Tracking Data: Loyalty Cards & Store Apps

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Loyalty Cards & Store Apps

**Subtitle:** A discount in exchange for presenting a persistent identifier at the register. The identifier is what makes baskets from separate visits joinable into one history.

## What is it?

A discount attached to an account identifier presented at checkout.

- **Examples:** Target Circle, CVS ExtraCare, grocery chain cards
- **Without the card** a basket is a standalone transaction
- **With it,** baskets from separate visits join into one series under one key
- **Not the only join:** payment card hashes do the same job, and so does basket composition on its own

**What the card adds:** the join becomes cheap and reliable, not newly possible.

### Visualization (canvas `c1`, 720×320)

Exchange diagram: shopper on the left, loyalty card in the center, retailer on the right, with two labeled arrows showing what each side gets.

- **Shopper (left, x=100, vertical center):** bold 14px label "SHOPPER" in blue `#2a78d6` above a person icon — circle radius 20 filled `rgba(42,120,214,0.35)`, stroked blue `#2a78d6` width 2, with a 👤 glyph centered inside.
- **Loyalty card (center):** orange `#d95926` rounded rectangle 110×60 (radius 10) centered at page center, white bold 13px text "LOYALTY" / "CARD" and 11px "**** 1234" inside.
- **Retailer (right, x=600):** bold 14px label "RETAILER" in blue, rectangle 60×50 filled `rgba(42,120,214,0.35)` stroked blue width 2.
- **Green arrow (retailer→shopper direction, drawn from card's left edge to shopper), color `#008300`:** horizontal line at cy+45 with a triangular arrowhead pointing left, captioned below in 12px: "A discount at the register".
- **Blue arrow (shopper→retailer, from card's right edge to retailer), color `#2a78d6`:** filled block arrow at cy−45 pointing right, with bold 12px label above: "A key that joins separate baskets into one series".
- **Bottom note (center, 13px, muted gray `#6b7280`):** "The identifier is the mechanism, not the discount".

## What does it collect?

- **Every item purchased** — SKU-level detail
- **Exact time** and store location
- **Coupon and sale response**
- **Browse vs buy,** for app users
- **Visit frequency** and dwell time
- **Emailed-coupon redemption** — connects an email address to in-store behaviour

**The identifier is coarser than the data:** `loyalty_id` names a household account that more than one person uses, so the unit of observation is a household while the segment reads as a description of a person — two shoppers' preferences averaged into one profile that fits neither.

**Line items make it worse:** basket composition is distinctive enough to link baskets across visits even when the loyalty ID is absent, so dropping the ID does not undo the linkage.

### Visualization (canvas `c2`, 720×320)

Schematic mapping measured line items (left) to derived labels (middle) and a household silhouette (right).

- **Title (bold 15px, blue `#2a78d6`, top center):** "Measured line items → derived labels (schematic)".
- **Item boxes (left, x=40, 130×26 each, at y = 50, 85, 120, 155, 190):** filled `rgba(42,120,214,0.35)`, stroked blue `#2a78d6` width 1, 12px dark text `#2c3e50`. Items: "Organic Milk", "Diapers Size 3", "Wine $40", "Allergy Meds", "Protein Bars".
- **Dashed orange arrows (`#d95926`, width 1.5, dash 3/2)** from each item box (x=175) to x=320, each ending in a bold 12px orange derived label at x=325: "→ health-conscious?", "→ infant in household?", "→ income band?", "→ allergy sufferer?", "→ fitness interest?".
- **Household silhouette (right, centered at x=580):** head circle radius 30 at y=100 and body ellipse 40×55 at y=170, both filled `rgba(42,120,214,0.15)` and stroked in dashed blue (`#2a78d6`, width 2, dash 4/3). Inside, centered 11px blue text over four lines: "one segment" / "label, averaged" / "over everyone" / "on the account".
- **Labels:** bold 13px "Household account" at (580, 80); muted 11px `#6b7280` "(dashed = inferred, not stated)" at (580, 220).

### Payload (under canvas `c2`)

Caption (italic, gray): "Sample payload — illustrative structure, not real captured data."

```
// Reconstruction. GTIN is a published standard;
// the surrounding schema is generic.
{
  // ── inferred / plausible ──
  "basket_id":    "bk_5518…",
  "loyalty_id":   "LC-…8842",
  "account_type": "household",     // one ID, more than one shopper
  "store":        "s_214",
  "ts":           "2026-08-22T18:37:04Z",
  "lines": [
    { "gtin": "00012345678905", "qty": 2, "unit_cents": 349 },
    { "gtin": "00098765432109", "qty": 1, "unit_cents": 1299,
      "coupon_id": "cp_77", "coupon_src": "email" },
    { "gtin": "00055512300047", "qty": 1, "unit_cents": 599 }
  ],
  "tender":       "card",
  "card_hash":    "9ab4…",         // join key to the payment record

  // segment assigned from basket composition
  "segment":      "small_household_health_conscious"
}
```

## Why is it collected?

**Stated purpose** (label pill)

- **Targeted discounts** — a joined history lets a retailer send a coupon for a category someone actually buys
- **Demand forecasting** — store-level demand per item instead of a guess from aggregate sales

**Additional consequence** (label pill)

- **Unstated labels** — life stage, health interests, income band — inferred from basket composition, then stored beside measured line items and read downstream as though observed
- **Partial coverage** — a row exists only where the card was presented, so purchases at other chains and in cash are absent rather than zero

**Base rate:** inferring a rare state from common purchases mostly produces false positives — even from a classifier that flags nearly every true case, because the label is rare and the purchase pattern is not.

### Visualization (canvas `c3`, 720×320)

Base-rate bar: one horizontal bar of flagged households, split into correct vs wrong flags, with the arithmetic below.

- **Computed values:** N=10,000 card holders; prevalence 2% → 200 true cases; label catches 90% → 180 correct; fires on 8% of the remaining 9,800 → 784 wrong; 964 flagged total; correct share 19%.
- **Title (bold 14px, ink `#1a5276`, center):** "Of the households a rare-state label fires on". Subtitle (12px muted `#6b7280`): "10,000 card holders; 2 in 100 are in the state; the label catches 9 of every 10 of them".
- **Bar:** x=60 to w−60, y=72, height 54. Left segment (correct share, 180/964 of the width) filled `rgba(25,158,112,0.45)` (aqua); right segment filled `rgba(217,89,38,0.28)` (orange). Whole bar outlined light gray `#e5e9ef`; a bold ink `#1a5276` vertical divider line (width 2) at the split, extending 6px above and below the bar.
- **Bar labels:** bold 13px "180 correct" in aqua `#199e70` inside the left segment; "784 wrong" in orange `#d95926` at the start of the right segment; centered 12px muted caption below the bar: "964 households flagged".
- **Arithmetic table (three rows starting at x=148, y=176, 24px spacing, left-aligned text 13px `#2c3e50`, right-aligned bold values at x+420):**
  - "in the state, label fires" — 180 (aqua `#199e70`)
  - "not in the state, label fires" — 784 (orange `#d95926`)
  - "share of flagged that is correct" — 19% (ink `#1a5276`)
  - A thin light-gray `#e5e9ef` rule between the second and third rows.
- **Captions (bottom center):** italic 12px `#2c3e50` "The retailer sees the flagged set. It never sees which of those flags was wrong."; italic 11px muted `#6b7280` "Illustrative rates — the shape, not a measured programme."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` paragraph + bullets + `.key-point` callouts, right `<td>` (55%, text-align center) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption and `.payload` `<pre>` block (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; bullets 0.93em with bold lead terms (`li b`) in `#1a5276`. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width`/`height` attributes (720×320); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own attributes. Include a rounded-rect path helper `rr()` for the card shape.
- **Palette:** page charts use the tracking-set validated categorical palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` for headings/axes, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and does not appear. Site-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
