# E-Commerce / Payments

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** E-Commerce / Payments — Collect, Use, Keep, Return

**Subtitle:** Purchase history as a personality profile — life events, income band, price sensitivity.

**Disclaimer callout:** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** orders, shipping addresses, payment methods, reviews, wishlists, gift registries, the address book of people you ship to.
- **Incidental:** every product viewed and for how long, on-site searches, carts built and abandoned, email opens and clicks, device and network identifiers, when you check a price and how often you come back to it.
- **Inferred:** life events (pregnancy, illness, a move) read from purchase shifts; income band; household composition; price sensitivity and willingness-to-pay; a returns-behavior risk score.

**Key point (callout box):** Most surprising: the purchase timeline can flag a life event — a pregnancy, a diagnosis, a divorce — from category shifts alone, before you have told anyone.

### Visualization (canvas `c1`, 720×400)

Grouped horizontal bar chart: assumed vs realistic collection extent, two bars per row.

- **Title (bold 13px `#1a5276`, top center):** "What people assume is collected vs realistic extent (illustrative)".
- **Legend (top, at x≈180 and x≈280, 14×10 swatches):** "assumed" — fill `rgba(26,82,118,0.35)`; "realistic extent" — fill `rgba(231,76,60,0.55)`. Legend text 11px `#2c3e50`.
- **Rows** (label, assumed %, realistic %): Order history 90/95; On-site searches 50/90; Every product viewed + dwell 20/85; Abandoned carts 25/85; Price-sensitivity signals 5/70; Gift-recipient address graph 10/75; Life-event inference 5/80; Returns-behavior score 5/60.
- **Layout:** right-aligned labels at x=215 (11px `#2c3e50`), bars start at x=225, max width 430px (scale 0–100), bar height 11px, assumed bar on top, realistic bar 3px below, group spacing 40px, first group at y=52.
- **Caption (bottom center, 10px `#999`):** "Numbers are illustrative — they show the shape of the gap, not measured values."

## How it gets used

- **Provide the service:** order fulfillment, payment processing, fraud screening.
- **Rank / recommend:** product ranking, "frequently bought together", reorder nudges timed to your consumption rate.
- **Offer calibration:** coupons and discounts sized to your inferred price sensitivity — hesitant browsers may see offers loyal buyers never do.
- **Ad targeting / measurement:** hashed identifiers matched with ad networks; purchases close the loop on ads seen elsewhere.
- **Model training:** demand forecasting, pricing, fraud and returns-risk models.
- **Sharing:** marketplace sellers, payment processors, delivery partners, ad and affiliate networks.

### Visualization (canvas `c2`, 720×360)

Hub-and-spoke flow diagram: source boxes → central "Buyer profile" hub → use boxes, with arrows.

- **Title (bold 13px `#1a5276`, top center):** "From raw signals to uses".
- **Source boxes (left column, 150×36 at x=25, y = 55/120/185/250):** Purchases, Browsing & carts, Searches & wishlists, Payments & returns. Style: `#1a5276` stroke (1.5px), same color fill at 12% alpha, bold 11px centered labels in `#1a5276`.
- **Hub box (x=275, y=130, 160×80):** stroke `#e67e22` 2px, fill `#e67e22` at 12% alpha; bold 12px `#e67e22` label "Buyer profile"; below it 10px `#7d5a29` text "segments · sensitivity · risk".
- **Use boxes (right column, 190×34 at x=510):** Fulfillment & payments (`#27ae60`, y=45); Ranking & recommendations (`#2980b9`, y=95); Calibrated offers & coupons (`#8e44ad`, y=145); Ad targeting & measurement (`#e74c3c`, y=195); Demand & fraud models (`#2980b9`, y=245); Seller / partner sharing (`#e67e22`, y=295). Each stroked/filled in its color (12% alpha fill), bold 11px label in its color.
- **Arrows:** gray `#bbb` (1.5px, filled triangular heads) from each source box to the hub's left-middle; colored arrows (each use box's color) from the hub's right-middle to each use box.

## How long it's kept

- **Active account:** full order and browsing history for the life of the account.
- **After deletion:** a backup tail of weeks to months before purging begins.
- **Transaction records:** kept for years after deletion — tax, accounting, and anti-fraud law require it.
- **Payment / chargeback data:** long retention under financial regulation, outside your control.
- **Fraud scores and de-identified aggregates:** effectively indefinite — the longest retention lands on copies stripped of direct identifiers, while raw identifiable records get the shorter windows. The catch: a purchase history stripped of PII can still be re-identified.

### Visualization (canvas `c3`, 720×340)

Horizontal retention-timeline bars per data category with an "account deleted" marker line.

- **Title (bold 13px `#1a5276`, top center):** "Retention by data category (illustrative)".
- **Axis:** bars start at x=220, axis width 460px; timeline baseline (thin `#999` line) at y=278 with 10px `#666` labels "account opens" (left-aligned at axis start) and "indefinite →" (right-aligned at axis end) at y=310.
- **Rows** (label, bar length as fraction of axis, color, note): Browsing clickstream 0.50 `#2980b9` "purged after tail"; Abandoned carts / wishlists 0.52 `#2980b9` (no note); Order history 0.60 `#2980b9` "backup tail"; Transaction records (tax law) 0.90 `#e67e22` "years past deletion"; Fraud / chargeback scores 1.0 `#e74c3c` "indefinite"; De-identified aggregates 1.0 `#e74c3c` "indefinite".
- **Bar style:** height 18px, gap 18px, first at y=45; fill in row color at 45% alpha, 1px stroke in row color; full-length (1.0) bars end in a filled triangular arrowhead pointing right. Notes in 10px `#666` just right of the bar end (or right-aligned inside the bar for full-length bars). Labels right-aligned at x=210, 11px `#2c3e50`.
- **Marker:** vertical dashed red line (`#e74c3c`, 2px, dash 5/4) at 45% of the axis (x≈427), from y=38 to y=280, labeled below in bold 11px `#e74c3c` centered: "account deleted".

## What you get back

- **Included:** order history, addresses, reviews, wishlists, messages with sellers, profile and settings.
- **Excluded:** inferred segments (income band, life events, household), price-sensitivity and willingness-to-pay estimates, returns-risk score, the browsing / abandonment clickstream in full detail, hashed identifiers already shared with ad networks, internal logs.

**Key point (callout box):** The asymmetry: the export is a receipt pile. The commercially valuable object — the buyer model built on top of those receipts — is not in the file.

### Visualization (canvas `c4`, 720×340)

Two side-by-side comparison panels: export contents vs retained data.

- **Title (bold 13px `#1a5276`, top center):** "The data export: what comes back vs what stays behind".
- **Left panel (x=30, y=40, 320×240, green `#27ae60` — 2px stroke, 8% alpha fill), bold 13px title "IN THE EXPORT",** items (11px `#2c3e50`, centered, 22px spacing): Order history & receipts / Addresses & payment methods / Reviews & wishlists / Messages with sellers / Profile & settings.
- **Right panel (x=375, y=40, 320×275, red `#e74c3c`), bold 13px title "EXISTS BUT NOT RETURNED",** items: Income band / life-event segments; Price-sensitivity estimates; Returns-risk score; Full browsing clickstream; Hashed IDs shared with ad networks; Household-composition inference; Internal logs & fraud models.
- **Caption (bottom center, 10px `#999`):** "The export is the receipt pile; the buyer model built from it is not in the file."

## Regeneration instructions

- **Layout:** detail page using `.obj-table` — full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` callout, right `<td>` (55%, `text-align: center`) holds the canvas. Cell borders `1px solid #e0e0e0`, padding 16px.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em with 6px bottom margin.
- **Callouts:** `.disclaimer` — background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`. `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; a shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Canvases are `display: block; margin: 0 auto`.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, red bar fill `rgba(231,76,60,0.55)`, gray text `#666`/`#999`/`#2c3e50`. No nav bar, no back/home links.
- Note: in regenerated HTML, any card/grid links referencing this page use the `.html` extension.
