# Pricing Psychology & Anchor Manipulation

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** 119. Pricing Psychology & Anchor Manipulation

**Subtitle:** Psychological price points, decoys, and fake anchors create cliffs and clusters that break smooth-demand and continuous-distribution assumptions.

## The $19.99 vs $20 Psychological Cliff

- 0.05% price difference yields 30% conversion difference
- Left-digit bias dominates rational price evaluation
- Consumers perceive $19.99 as "in the teens" not "basically $20"

**Example:** A/B test shows $19.99 converts at 42% while $20.00 converts at 29% — a 0.05% price change creating a 30% behavior change that no smooth demand curve predicts.

### Visualization (canvas `c1`, 720×300)

Horizontal bar comparison of conversion rate at two price points, with annotation panel.

- **Title (17px, `#1a5276`, at 20,25):** "Conversion Rate by Price Point".
- **Bars:** $19.99 bar green `#27ae60` at (80,50), 180×40, white value label "42%" inside; $20.00 bar red `#e74c3c` at (80,110), 125×40, white label "29%". Row labels "$19.99" and "$20.00" in `#333` to the left of the bars.
- **Divider:** vertical dashed line `#2980b9` (dash 4/4) from (300,45) to (300,160).
- **Annotations (14px, `#c0392b`, x=320):** "Price diff: 0.05%" (y=80), "Conversion diff: 30%" (y=100), "← Psychological cliff!" (y=130).
- **Legend:** gray `#7f8c8d` 12×12 square at (500,55) with `#333` text "Rational model predicts ~0% diff".

## Decoy Products With Zero Sales Shifting Purchases

- The Economist: print-only $125 exists ONLY to make print+digital $125 look like a deal
- Remove decoy → revenue drops 40%
- A product nobody buys changes what everyone else buys

**Example:** Three options: digital $59, print $125, print+digital $125. The print-only option gets 0% of sales but makes print+digital share jump from 32% to 84%.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart comparing choice shares with and without the decoy option.

- **Title (17px, `#1a5276`):** "The Economist Decoy Effect".
- **Categories (x = 80 + i×200):** labels "Digital $59", "Print $125", "Print+Digital $125" (13px, `#333`, below baseline at y=188).
- **Data:** with decoy `[16, 0, 84]` (blue `#2980b9` bars, 35px wide); without decoy `[68, 0, 32]` (orange `#e67e22` bars, 35px wide, offset +45px). Bar heights scale 1.5px per percent, baseline y=170; percent value labels above each bar.
- **Legend (x=550):** blue swatch "With decoy", orange swatch "Without decoy".

## Fake "Original Price" Anchoring

- $299 crossed out — product never actually sold at $299
- Anchor creates perceived savings that don't exist
- FTC requires "was" prices to be genuine, yet enforcement is lax

**Example:** Mattress listed as "$1,200 — now $599!" but sales data shows it has NEVER sold above $620. The $1,200 anchor exists purely to manufacture perceived value.

### Visualization (canvas `c3`, 720×300)

Line chart of actual sale prices vs a dashed fake anchor line above.

- **Title (17px, `#1a5276`):** "Fake Anchor Price vs Actual Transaction History".
- **Anchor line:** horizontal dashed red `#e74c3c` (dash 6/4) from (60,50) to (650,50), labeled in red 14px: '"Was $299" (never sold here)' at (500,45).
- **Actual price series:** blue `#2980b9` line (width 2) through 12 points `[155, 148, 160, 152, 149, 158, 145, 150, 153, 147, 155, 149]`, x = 80 + i×50, y = 180 − (price − 130)×3; blue label "Actual sale prices: $145-$160 range" at (200,180).
- **Gap bracket:** gray `#7f8c8d` vertical bracket at x=400 from y=55 to y=105 with tick ends, labeled (13px gray) "Gap = manufactured \"savings\"" at (300,100).

## Bundle Manipulation — Manufactured Savings

- Items worth $30+$20 separately, bundled at $79 "saving $21"
- You'd never buy both items individually
- Bundle creates demand for the combination that doesn't exist organically

**Example:** Separately: phone case $30 (70% would buy) + screen protector $20 (40% would buy). Bundled at $79 "save $21 vs buying both!" — but only 15% of customers would have bought both anyway.

### Visualization (canvas `c4`, 720×300)

Three stacked horizontal intent bars plus a red takeaway line.

- **Title (17px, `#1a5276`):** "Bundle vs Separate Purchase Intent".
- **Bars (14px labels):** green `#27ae60` bar 250×30 at (60,55) with white text "Case $30 — 70% would buy"; blue `#3498db` bar 145×30 at (60,95) with white text "Protector $20 — 40%"; purple `#9b59b6` bar 55×30 at (60,135) with white text "Both" and `#333` text beside it: "15% would buy both = actual addressable market".
- **Divider:** dashed red `#e74c3c` (dash 4/3) horizontal line from (60,175) to (680,175).
- **Takeaway (red, y=193):** "Bundle at $79 \"save $21\" — but 85% would never have spent $50 on both!"

## "Compare At" Manufactured Baselines

- Retailers create fictional comparison prices from non-existent competitors
- "Compare at $89" — compared to whom? Nobody sells it at $89
- Outlet stores carry items manufactured specifically for outlet pricing

**Example:** Factory outlet tags show "Original $120 / Our price $49" but the item was designed for the outlet, never stocked in mainline stores, and cost $12 to manufacture.

### Visualization (canvas `c5`, 720×300)

Grouped bar chart of four items with three bars each: compare-at price, outlet price, manufacturing cost.

- **Title (17px, `#1a5276`):** '"Compare At" Price vs Reality'.
- **Items (x = 80 + i×160, item name centered below at y=185, 12px):** Jacket, Bag, Shoes, Shirt.
- **Data:** compare-at `[120, 89, 150, 75]` (red `#e74c3c`, 30px wide, height = value px, baseline y=170); actual `[49, 35, 59, 29]` (orange `#f39c12`, offset +35px); cost `[12, 8, 14, 6]` (green `#27ae60`, offset +70px, height = value×3 — 3× scale).
- **Legend (12px, x=500):** red swatch '"Compare at"', orange swatch "Outlet price", green swatch "Mfg cost (3x scale)".

## Price Clustering at .99/.95 Creating Distribution Spikes

- Price distributions show massive spikes at .99 and .95 endings
- Creates artificial clustering that violates continuous distribution assumptions
- Statistical models assuming smooth price distributions fail catastrophically

**Example:** Histogram of 10,000 products: 47% end in .99, 18% end in .95, remaining 35% spread across other endings. Any regression treating price as continuous misses this structure.

### Visualization (canvas `c6`, 720×300)

Histogram of price endings with highlighted spike bars.

- **Title (17px, `#1a5276`):** "Price Ending Distribution (10,000 Products)".
- **Bins:** endings `['.00','.09','.19','.25','.29','.39','.49','.50','.59','.69','.79','.89','.95','.99']` with percentages `[5, 1, 1, 2, 2, 1, 4, 3, 1, 1, 1, 2, 18, 47]`. Bars 36px wide at x = 50 + i×46, height = pct×3, baseline y=165.
- **Colors:** `.99` bar red `#e74c3c`, `.95` bar orange `#e67e22`, all others blue `#3498db`.
- **Labels:** ending labels rotated −0.5 rad below bars (10px); percent labels (11px) above bars with pct > 3.
- **Annotation (12px, `#7f8c8d`, at 400,60):** "← Massive spikes violate continuous assumptions".

## Free Shipping Threshold Cart Value Clustering

- $25 free shipping threshold causes cart values to cluster at $25-$28
- Customers add unnecessary items to hit threshold
- Cart value distribution becomes bimodal, not normal

**Example:** e-commerce platform $25 threshold creates visible spike: 22% of carts land between $25.00-$27.99 vs 6% expected under uniform distribution. Customers add $4 items they don't want to save $5.99 shipping.

### Visualization (canvas `c7`, 720×300)

Density curve of cart values with a highlighted spike band at the threshold.

- **Title (17px, `#1a5276`):** "Cart Value Distribution (Free Shipping at $25)".
- **Curve:** blue `#2980b9` line (width 2) through points (x,y): (50,140), (100,135), (150,130), (200,128), (230,125), (250,80), (270,55), (290,60), (310,70), (340,110), (380,120), (430,130), (500,135), (580,140), (650,145) — dip near x=270 is the spike peak (lower y = higher density).
- **Highlight band:** translucent red `rgba(231,76,60,0.2)` rect at (245,40), 60×130; vertical dashed red `#e74c3c` (dash 4/3) line at x=260 from y=40 to y=170.
- **Annotations (13px):** red "$25 threshold" at (235,185); red "22% of carts cluster here" at (320,55); gray `#7f8c8d` "Expected ~6%" at (320,75).
- **Axis labels (12px, `#333`):** "$10" at (45,158), "$50" at (430,145), "$25-$28" at (250,38).

## Demand Curves With Psychological Cliffs

- Can't interpolate between $19.99 and $24.99 — demand is NOT a smooth slope
- Psychological price points create step functions, not curves
- Traditional elasticity models fail at boundary points

**Example:** Demand at $19.99 = 1000 units, at $20.01 = 680 units, at $24.99 = 650 units. The cliff is between $19.99 and $20.01, not spread across the $5 range. Linear interpolation gives wildly wrong estimates.

### Visualization (canvas `c8`, 720×300)

Step-function demand curve vs a dashed linear-model line on shared axes.

- **Title (17px, `#1a5276`):** "Demand: Actual Step Function vs Linear Model".
- **Axes:** black `#333` L-shaped axes — x from (60,170) to (660,170), y from (60,170) to (60,40).
- **Actual demand (blue `#2980b9`, width 3):** step path (80,50) → (280,52) → (282,120) → (450,118) → (452,135) → (600,133) → (602,155) → (650,155).
- **Linear model (red `#e74c3c`, width 2, dash 6/4):** straight line from (80,50) to (650,155).
- **X labels (12px, `#333`):** "$19.99" (250,185), "$20.01" (285,185), "$24.99" (430,185), "$29.99" (590,185). **Y labels:** "1000" (25,55), "680" (30,123).
- **Legend (x=400):** blue swatch "Actual (cliffs)", red swatch "Linear model (wrong)".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a single-row full-width table; left `<td>` (40%) holds `.obj-title` + bullet list + bold-labeled example paragraph, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; bullets 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** all 8 canvases declared `width="720" height="300"`; a shared IIFE loops over all canvases, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Default chart font 17px -apple-system.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c` (dark red `#c0392b`), orange `#e67e22`/`#f39c12`, purple `#9b59b6`, gray `#7f8c8d`/`#555`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions.
