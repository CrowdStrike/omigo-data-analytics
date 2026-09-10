# Movie Theaters — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, main histogram canvas center 31%, insight canvas right 31%, one table per section)
**HTML title tag:** Movie Theaters — Distribution Patterns

**Subtitle:** Distribution shapes from theater pricing, seating, and timing decisions

## Popcorn Pricing (The Medium Is the Decoy)

**Label:** DECOY EFFECT (color `#795548`)

Spikes at Small ($5/10oz) and Large ($8/24oz), with only a sliver at Medium ($7/12oz). The classic decoy reading: Medium has the worst price-per-ounce ($0.58/oz vs $0.50 for Small and $0.33 for Large) and exists mainly to make Large look like a deal.

- Medium = decoy ($0.58/oz — worst of the three)
- Small and Large get 95% of sales in this simulation
- $1 more for 2x the popcorn → Large "wins"
- Remove the decoy and Large loses its "bargain" frame

### Visualization (canvas `canvas1`, 420×340)

Trimodal purchase-price histogram.

- **Data:** seeded RNG mulberry32(42) shared across the page; 800 draws normal(5, 0.3) (Small) + 100 draws normal(7, 0.2) (Medium) + 1100 draws normal(8, 0.3) (Large).
- **Bins/axes:** 40 bins over x 3.5–9.5; x ticks formatted "$4"…"$9"; margins top 35 / right 20 / bottom 40 / left 50; axes `#999`; white background.
- **Title (bold 13px, `#1a5276`):** "Popcorn Size Purchased ($)". **X label:** "Price ($)".
- **Bars:** fill `rgba(192,57,43,0.5)`, stroke `#c0392b`.
- **Density overlay:** Gaussian-kernel smoothed counts (sigma 1.5 bins), line `#1a5276` width 2 with SE band `rgba(230,126,34,0.22)` (1.96·smoothed/√effN, effN clamped 30–200).

### Visualization (canvas `canvas1b`, 400×340)

Price-per-ounce bar chart exposing the decoy.

- **Title (bold 13px, `#c0392b`):** "Price per Ounce — The Decoy Exposed".
- **Bars (name, price, oz, color):** Small $5 / 10oz `#2980b9`; Medium $7 / 12oz `#e74c3c`; Large $8 / 24oz `#27ae60`. Bar height = ($/oz) scaled to max 0.70; 50px-wide bars at thirds of the plot, alpha 0.75 fill with 2px matching stroke.
- **Labels:** below each bar, bold 12px in the bar color: size name and "$5 / 10oz" etc.; above each bar, bold 14px `#333` "$0.50/oz", "$0.58/oz", "$0.33/oz" (computed from price/oz).
- **Annotation:** red `#e74c3c` arrow to the worst-$/oz bar (index computed from the size data — Medium) with bold "DECOY!" and 10px lines "Worst value — exists to" / "make Large look cheap".
- **Axes:** L-frame `#999`; y-axis caption "$/oz" top-left; margins top 40 / right 20 / bottom 50 / left 55.

## Seat Selection (Center-Back Sweet Spot)

**Label:** NORMAL CONVERGENCE (color `#2980b9`)

Seat preference forms a tight 2D normal centered at row 7-8, center column. Consistent with viewers independently converging on the same audio/visual sweet spot — no coordination needed to produce a single sharp peak.

- Peak at row 7-8, center column (audio + visual sweet spot)
- Edges and front rows avoided (neck strain, distortion)
- Tight spread — most mass within ~2 rows of the peak
- Theaters place "premium" pricing where the density peaks

### Visualization (canvas `canvas2`, 420×340)

Row-preference histogram.

- **Data:** 2000 draws normal(7.5, 1.8).
- **Bins/axes:** 30 bins over x 1–15; x ticks as rounded integers.
- **Title:** "Preferred Row Number". **X label:** "Row".
- **Bars:** fill `rgba(41,128,185,0.5)`, stroke `#2980b9`; same density overlay as canvas1.

### Visualization (canvas `canvas2b`, 400×340)

2D seat heatmap.

- **Title (bold 13px, `#2980b9`):** "Seat Selection Heatmap (Row × Column)".
- **Grid:** 12 rows × 16 columns; cell heat from a 2D Gaussian centered at row 7.5, col 8 with sigmas 2.2 (rows) and 3.5 (cols); rows drawn bottom-up so row 1 sits at the bottom adjacent to the screen; cell color `rgba(red,50,blue,alpha)` where red = 255·intensity, blue = 80 + 120·(1−intensity), alpha = 0.3 + 0.6·intensity; margins top 35 / right 15 / bottom 30 / left 35.
- **Annotations:** yellow `#f1c40f` circle (radius 18, 3px stroke) at the Gaussian peak, position computed from peakRow/peakCol, with bold label "← SWEET SPOT"; centered caption "SCREEN" below the grid; row labels "R1" (bottom, nearest the screen), "R4", "R7", "R10" (upward) on the left (every 3rd row).

## Showtime Attendance (Event vs Routine)

**Label:** BIMODAL MARKET (color `#27ae60`)

Opening spike plus a flat long tail of weekday viewers. Consistent with two distinct audiences — "event" viewers who show up in the first days and routine viewers spread evenly after — with an average that describes neither.

- Opening spike: event viewers (one reading: price-insensitive)
- Week 2+: flat routine traffic (deal-seekers, matinee)
- "Average" attendance describes neither population
- ~1/3 of all attendance lands in the first 3 days here

### Visualization (canvas `canvas3`, 420×340)

Attendance-by-day histogram: spike plus flat tail.

- **Data:** 1200 draws normal(2, 1) kept if ≥ 0 (opening spike) + 1800 draws uniform 3–30 (flat routine tail).
- **Bins/axes:** 35 bins over x 0–30; integer x tick labels.
- **Title:** "Days After Release (Attendance)". **X label:** "Day".
- **Bars:** fill `rgba(142,68,173,0.5)`, stroke `#8e44ad`; same density overlay.

### Visualization (canvas `canvas3b`, 400×340)

Stacked area chart of the two audiences.

- **Title (bold 13px, `#8e44ad`):** "Two Markets: Event vs Routine".
- **Data (30 days):** event viewers = 100·exp(−0.5·day) (exponential decay); routine = 15 + 5·rng() (flat with noise); y scaled to day-0 total.
- **Areas:** routine layer on the bottom filled `rgba(41,128,185,0.6)`; event layer stacked on top filled `rgba(231,76,60,0.6)`.
- **Labels:** bold 11px red "EVENT VIEWERS" with 9px "(must see day 1, price-insensitive)" top-left; bold 11px blue `#2980b9` "ROUTINE VIEWERS" with 9px "(deal-seekers, matinee)" lower-middle; bold 10px `#333` right-aligned "~1/3 of attendance in first 3 days".
- **Axes:** baseline only, x labels "Day 1", "Day 15", "Day 30"; margins top 35 / right 15 / bottom 40 / left 45.

## Concession Timing (80% in Last 5 Minutes)

**Label:** PANIC BUYING (color `#e74c3c`)

Concession purchases spike violently in the 5 minutes before showtime — 80% of simulated purchases land there. One reading: queue-time pressure drives impulse add-ons that a calm browser would skip.

- 80% of purchases within 5 min of showtime
- Purchases 10+ minutes out are sparse and flat
- One explanation: queue anxiety → impulse candy/drink add-ons
- Staffing has to absorb the surge, not the average

### Visualization (canvas `canvas4`, 420×340)

Purchase-timing histogram (minutes before showtime).

- **Data:** 2000 draws normal(−2, 1.5) kept if ≤ 0 (last-minute spike) + 400 draws uniform −30 to −5 (sparse early buyers).
- **Bins/axes:** 35 bins over x −30 to 0; x ticks formatted "-30m"…"0m".
- **Title:** "Minutes Before Showtime (Purchase)". **X label:** "Minutes Before Show".
- **Bars:** fill `rgba(230,126,34,0.5)`, stroke `#e67e22`; same density overlay.

### Visualization (canvas `canvas4b`, 400×340)

Minute-by-minute arrival-rate bars with a panic zone.

- **Title (bold 13px, `#e67e22`):** "Purchase Rate — The Panic Zone".
- **Bars:** 30 per-minute counts computed from the canvas4 data, drawn reversed (30 min ago at left, showtime at right); last-5-minute bars `rgba(231,76,60,0.75)`, earlier bars `rgba(230,126,34,0.45)`.
- **Panic zone:** last 5 minutes shaded `rgba(231,76,60,0.1)` with a dashed red `#e74c3c` vertical boundary; bold red labels "PANIC" / "ZONE" and bold 10px "80% buy here" with a red down-arrow.
- **Axes:** x labels "-30m", "-15m", "SHOW"; margins top 35 / right 15 / bottom 40 / left 45.

## Ticket Price (Cliff at Round Numbers)

**Label:** PSYCHOLOGICAL THRESHOLD (color `#8e44ad`)

Willingness to pay is flat within price bands but drops sharply at each round-number boundary, and the cliffs deepen as price rises (−28%, −37%, −51% at $10, $15, $20 in the demand panel). Consistent with buyers bucketing prices into mental bands rather than reading them continuously.

- Demand cliff at $10, $15, $20 boundaries
- Crossing $10 sheds about a quarter of buyers here
- Consistent with matinee prices hugging the $10 ceiling
- Premium formats (IMAX) price past $20 — a different band, not the same curve

### Visualization (canvas `canvas5`, 420×340)

Willingness-to-pay histogram with band cliffs.

- **Data:** 2000 candidate draws uniform $5–25 with step rejection: keep 100% at ≤$10, 72% at $10–15 (reject p=0.28), 45% at $15–20 (reject p=0.55), 22% above $20 (reject p=0.78) — band retention matching the −28%/−37%/−51% cliffs in the companion chart.
- **Bins/axes:** 30 bins over x 5–25; x ticks formatted "$5"…"$25".
- **Title:** "Willingness to Pay (Ticket Price)". **X label:** "Price ($)".
- **Bars:** fill `rgba(39,174,96,0.5)`, stroke `#27ae60`; same density overlay.

### Visualization (canvas `canvas5b`, 400×340)

Step-function demand curve with annotated cliffs.

- **Title (bold 13px, `#27ae60`):** "Demand Cliffs at Psychological Boundaries".
- **Curve:** demand index over price $5–25 in $0.50 steps: 100 below $10, 72 at $10–15, 45 at $15–20, 22 above $20, plus ±2.5 uniform noise; green `#27ae60` line width 3; y scale 0–105.
- **Cliff annotations (price, drop label, color):** $10 "−28%" `#e67e22`; $15 "−37%" `#e74c3c`; $20 "−51%" `#c0392b`. Each has a dashed vertical line at its price, a bold "$10"/"$15"/"$20" x-axis label, the drop percentage stacked near the top, and a small filled down-arrow.
- **Axes:** L-frame `#999`; y labels "100%" and "0%"; top-left caption "Demand Index"; margins top 35 / right 20 / bottom 40 / left 50.

## Regeneration instructions

- **Layout:** h1 + `.subtitle` paragraph, then one `.obj-table` (full-width, `border-collapse: collapse`) per pitfall, single `<tr>` with three `<td>`s: text 38%, main canvas 31% centered, insight canvas 31% centered. Cell borders `1px solid #2980b9`, padding 12px. Each text cell: `.pitfall-label` span, `<h3>`, one `<p>`, one 4-item `<ul>`.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.subtitle` centered `#666` 0.95em; `.obj-table h3` 1.0em weight 700 `#1a5276`; p/li 14px, line-height 1.5–1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px. `canvas { width: 100%; height: auto; }`. No nav bar, no back/home links.
- **Label colors:** assigned by section index from the palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script.
- **Canvases:** intrinsic sizes 420×340 (main) and 400×340 (insight); backing store scaled by `window.devicePixelRatio`, CSS size fixed, `ctx.scale` back to logical coordinates. Data generated with seeded mulberry32(42) RNG shared sequentially across all charts plus a Box-Muller `randNormal(mean, std)` helper.
- **Shared histogram helper:** margins {top 35, right 20, bottom 40, left 50}, white background, bold 13px `#1a5276` title, `#999` axes, `#555` tick text; smoothed density line `#1a5276` width 2 with SE band `rgba(230,126,34,0.22)`; optional `xFormat` tick formatter and `min`/`max` range.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, purple `#8e44ad`, dark red `#c0392b`, yellow accent `#f1c40f`.
