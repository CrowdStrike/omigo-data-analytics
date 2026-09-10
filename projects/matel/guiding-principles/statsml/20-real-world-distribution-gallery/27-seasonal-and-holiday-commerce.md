# Seasonal & Holiday Commerce — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left ~38%, histogram canvas middle ~31%, insight canvas right ~31%, one table per section)
**HTML title tag:** Seasonal & Holiday Commerce — Distribution Patterns

**Subtitle:** 5 distributions that expose the extreme asymmetries hiding inside holiday retail data

## Black Friday Volume (Extreme Spike)

**Label:** 99TH PERCENTILE DAY (color `#795548`)

Normal day volume is flat and low. Black Friday concentrates roughly 8x the typical hourly volume into a 4-hour morning window. Logistics infrastructure sized for this ONE day sits idle 364 days/year — the distribution shape IS the capacity planning problem.

- Normal day: uniform 50-150 transactions/hour
- Black Friday hours 6-9: spike to ~800/hour average
- Peak hours ≈ 8x the typical hour, ~5x the busiest normal hour
- Excess capacity cost = 364 days × idle infrastructure

### Visualization (canvas `canvas1`, 420×340)

Histogram of Black Friday hourly transaction volume by hour of day.

- **Title (bold 13px, `#1a5276`, top center):** "Black Friday — Hourly Transaction Volume".
- **Data:** synthetic samples generated with seeded RNG (mulberry32, seed 42). For each of 24 hours, an hourly count is drawn — hours 6-9: Normal(mean 800, sd 100); other hours: uniform 50-150 — then each hour contributes `count/10` samples placed at `hour + U(0,0.9)`, so bin height is proportional to hourly volume.
- **Bins/axes:** 24 bins over x range 0-24; x labels at 6 ticks formatted as "Nh" (0h…24h); x-axis label "Hour of Day"; y-axis unlabeled (counts), gray `#999` L-shaped axes; margins top 35, right 20, bottom 40, left 50.
- **Bars:** fill `rgba(241,196,15,0.6)`, border `#f39c12` (0.5px).
- **Overlay:** Gaussian-smoothed density line (sigma 1.5 bins) in `#1a5276` width 2, with 95% SE band filled `rgba(230,126,34,0.22)` (effective N clamped 30-200).

### Visualization (canvas `canvas1b`, 400×340)

Line-comparison chart: normal day vs Black Friday hourly volume over 24 hours.

- **Title (bold 13px, `#f39c12`, top center):** "Normal Day vs Black Friday".
- **Axes:** L-shaped gray `#999` axes; y scale 0-1000 with right-aligned labels "1000", "500", "0"; x labels "0h", "6h", "12h", "18h", "24h"; padding top 40, right 20, bottom 50, left 50.
- **Normal day series:** flat line at 100, dashed (6/4), `#2980b9`, width 2.
- **Black Friday series:** solid `#e74c3c`, width 3; value 100 at all hours except hours 6-9 at 800, with hours 7 and 8 at 950.
- **Shaded area:** region between the Black Friday curve and the flat 100 line filled `rgba(231,76,60,0.12)`.
- **Annotations:** near the peak (hour 8, y≈950), bold 11px `#c0392b` two-line text "Logistics built for" / "this ONE day" with a small arrow pointing down-left to the peak; centered at 75% width near baseline, 10px `rgba(231,76,60,0.8)` two-line label "Excess capacity" / "364 days/year".
- **Legend (below x-axis):** dashed `#2980b9` swatch + bold 10px "Normal Day"; solid `#e74c3c` swatch + "Black Friday (~8x)".

## Gift Card Redemption (Bimodal + Breakage)

**Label:** BREAKAGE REVENUE (color `#2980b9`)

Gift cards create a trimodal distribution: immediate redeemers (week 1), delayed redeemers (6+ months), and never-redeemed (shown at day 365). The never-redeemed spike IS the profit center — industry-wide breakage is worth billions a year.

- 45% redeemed within first week
- 20% redeemed after 6+ months
- 35% NEVER redeemed = pure profit
- Breakage revenue = billions/year industry-wide

### Visualization (canvas `canvas2`, 420×340)

Histogram of gift card redemption timing in days.

- **Title:** "Gift Card Redemption (Days; 365 = never)".
- **Data:** 900 samples Normal(mean 3, sd 2) clamped to [0,365] (week-1 redeemers); 400 samples Normal(mean 240, sd 60) clamped to [0,365] (6+ month redeemers); 700 samples fixed at exactly 365 (never redeemed).
- **Bins/axes:** 40 bins over x range 0-365; x labels formatted "Nd"; x-axis label "Days"; same shared histogram layout as canvas1.
- **Bars:** fill `rgba(39,174,96,0.5)`, border `#27ae60`; same `#1a5276` smoothed density line with `rgba(230,126,34,0.22)` SE band.

### Visualization (canvas `canvas2b`, 400×340)

Waterfall diagram: where the gift card money goes.

- **Title (bold 13px, `#27ae60`, top center):** "Gift Card Waterfall — Where the Money Goes".
- **Steps (4 bars, each 0.7 of a w/5 column slot, 75% alpha fill + 2px stroke in its color):**
  - "100 Cards / Sold": value 100, color `#2980b9` (full-height starting bar).
  - "45 Redeemed / Week 1": −45, color `#27ae60` (drop from 100 to 55).
  - "20 Redeemed / 6+ Months": −20, color `#f39c12` (drop from 55 to 35).
  - "35 NEVER / REDEEMED": −35, color `#e74c3c` (drop from 35 to 0).
- **Scale:** y max 100; two-line bold 10px labels below each bar in the bar's color; dashed gray `#999` connector lines between consecutive bars.
- **Annotation:** the last bar's column is overlaid with a full-height box filled `rgba(231,76,60,0.1)` with dashed `#e74c3c` 2px border (dash 4/3), captioned inside at top in bold 11px `#e74c3c`: "= PURE" / "PROFIT".
- **Bottom caption (bold 10px, `#c0392b`, centered):** "Breakage revenue = billions/year industry-wide".

## Post-Holiday Return Wave (Spike + Decay)

**Label:** PREDICTABLE WAVE (color `#27ae60`)

Returns spike massively Dec 26-28 then decay exponentially over 30 days. About half of all returns land in the first 3 days. A shape this regular is what makes pre-scheduling staff to the curve possible.

- ~50% of returns happen in first 3 days
- Exponential decay τ ≈ 5 days after spike
- A regular, recurring wave — spike then decay
- Staffing can be scheduled to the curve

### Visualization (canvas `canvas3`, 420×340)

Histogram of return timing (days after Dec 25).

- **Title:** "Post-Holiday Returns (Days After Dec 25)".
- **Data:** 1500 samples Normal(mean 2, sd 1) kept if in [0,30] (the spike); 1000 samples 3 + Exponential(mean 5) (i.e. `3 - 5·ln(U)`) kept if in [3,30] (the decay tail).
- **Bins/axes:** 30 bins over x range 0-30; x labels formatted "Nd"; x-axis label "Days After Christmas".
- **Bars:** fill `rgba(231,76,60,0.5)`, border `#e74c3c`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas3b`, 400×340)

Overlay line chart: return volume vs staff scheduled over 30 days.

- **Title (bold 13px, `#e74c3c`, top center):** "Return Volume vs Staff Scheduled".
- **Axes:** gray `#999` L-shaped axes; y max 100 (unlabeled); x labels "Dec 26" (left), "Jan 10" (middle), "Jan 25" (right); padding top 40, right 20, bottom 50, left 50.
- **Return volume series:** days 0-2: `100 − 15·d` (100, 85, 70); days 3+: `70·exp(−0.25·(d−2))`; drawn solid `#2980b9` width 3 with area under the curve filled `rgba(52,152,219,0.2)`.
- **Staff series:** `0.9 × return volume + 5` per day; dashed (8/4) `#e67e22` width 2.5.
- **Annotation:** bold 11px `#1a5276` two-line text at ~40% width near the top: "If you know the shape," / "you can staff it", with a 2px `#1a5276` arrow pointing down-left to the curve.
- **Legend (below x-axis):** solid `#2980b9` swatch + bold 10px "Return Volume"; dashed `#e67e22` swatch + "Staff Scheduled".

## Inventory Pre-Build (Asymmetric Triangle)

**Label:** ASYMMETRIC BUILD (color `#e74c3c`)

Inventory builds linearly Sept-Dec (4 months of slow ramp) then collapses to zero on Dec 26 in a single day. The distribution is a sawtooth — slow accumulation, instant liquidation. Cash flow is this shape inverted.

- Linear build: Sept → Dec (4 months)
- Cliff collapse: Dec 26 (1 day)
- Asymmetry ratio: 120:1 (build:collapse)
- Cash flow = this shape inverted

### Visualization (canvas `canvas4`, 420×340)

Histogram of inventory level by month offset from Sept 1.

- **Title:** "Inventory Level (Months from Sept 1)".
- **Data:** 1500 samples from a rising triangular distribution on [0,4] via `4·sqrt(U)` (mode at 4 = Dec); plus 100 samples at `4 + U(0,0.15)` — a tiny sliver at the right edge representing the post-Dec-26 collapse.
- **Bins/axes:** 30 bins over x range 0-5; x labels one-decimal values; x-axis label "Months (Sept=0, Dec=4)".
- **Bars:** fill `rgba(41,128,185,0.5)`, border `#2980b9`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas4b`, 400×340)

Time-series schematic: linear ramp then vertical cliff.

- **Title (bold 13px, `#2980b9`, top center):** "Inventory Build → Cliff Collapse".
- **Axes:** gray `#999` L-shaped axes; padding top 40, right 20, bottom 50, left 50; x labels "Sept 1" (left edge), "Oct" (33% of ramp), "Nov" (66% of ramp), "Dec 25" (82% of width), "Dec 26" (just past ramp end); rotated y-axis label "Inventory Level" in gray.
- **Ramp:** solid `#27ae60` line width 3 from the origin (Sept 1, zero) rising linearly to near the top at 82% width (Dec 25); area under the ramp filled `rgba(39,174,96,0.15)`.
- **Cliff:** vertical `#e74c3c` line width 4 at Dec 26 dropping from the peak to the baseline, with a 6px-wide full-height band shaded `rgba(231,76,60,0.2)` behind it.
- **Annotations:** centered on the ramp in bold 11px `#27ae60`: "4 months to build" with a horizontal green arrow along the ramp; to the right of the cliff in bold 11px `#e74c3c`: "1 day to" / "collapse" with a red arrow pointing at the cliff.
- **Bottom caption (bold 10px, `#555`, centered below x labels):** "Cash flow = this shape INVERTED".

## Doorbuster Cannibalization (Bimodal Basket)

**Label:** TWO POPULATIONS (color `#8e44ad`)

One promotion, two opposite basket profiles: a doorbuster-only cluster ($15 avg, loss-leader items) and a full-trip cluster ($120 avg). The mean basket of ~$60 describes NEITHER population — it's a statistical ghost.

- Doorbuster hunters: $15 avg, buy loss leader only
- Full-trip shoppers: $120 avg, healthy margin
- Mean = $60 describes nobody
- Same promo, two very different basket profiles

### Visualization (canvas `canvas5`, 420×340)

Bimodal histogram of basket size on doorbuster promotion day.

- **Title:** "Basket Size — Doorbuster Promotion Day".
- **Data:** 1000 samples Normal(mean 15, sd 5) kept if > 0 (doorbuster-only shoppers); 800 samples Normal(mean 120, sd 35) kept if in (0,200] (full-trip shoppers).
- **Bins/axes:** 35 bins over x range 0-200; x labels formatted "$N"; x-axis label "Basket Total ($)".
- **Bars:** fill `rgba(142,68,173,0.5)`, border `#8e44ad`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas5b`, 400×340)

Two-population diagram: opposing customer groups connected by a double arrow.

- **Title (bold 13px, `#8e44ad`, top center):** "Same Promotion — OPPOSITE Customers".
- **Left group (centered at 15% width):** box 30% of width × 55% of height, filled `rgba(231,76,60,0.12)` with `#e74c3c` 2px border, containing a 4×5 grid of 6px-radius red `#e74c3c` circles (people icons). Labels below in `#e74c3c`: bold 11px "DOORBUSTER" / "HUNTERS", 10px "$15 avg", bold 10px "NEGATIVE margin".
- **Right group (centered at 78% width):** same-size box filled `rgba(39,174,96,0.12)` with `#27ae60` 2px border, containing a 3×4 grid of 7px-radius green `#27ae60` circles. Labels below in `#27ae60`: bold 11px "FULL-TRIP" / "SHOPPERS", 10px "$120 avg", bold 10px "healthy margin".
- **Connector:** horizontal double-headed arrow in `#8e44ad` width 3 between the boxes at mid-height, with three-line bold 9px `#8e44ad` label above/around it: "Same promotion" / "attracts OPPOSITE" / "value customers".
- **Bottom caption (bold 12px, `#1a5276`, centered):** "Is the loss leader worth it?"

## Regeneration instructions

- **Layout:** one `.obj-table` per section (full-width, border-collapse), each with a single `<tr>` of three `<td>`s: first 38% (text: `.pitfall-label` span, `<h3>` title, `<p>` paragraph, `<ul>` bullets), second 31% centered (histogram canvas), third 31% centered (insight canvas). Section order as above.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `.subtitle` centered `#666` 0.95em; table cell borders `1px solid #2980b9`, padding 12px; h3 `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px. No nav bar, no back/home links.
- **Pitfall label colors:** assigned by index from the cycling palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script that sets each `.pitfall-label`'s color.
- **Canvases:** intrinsic sizes as given (420×340 histograms, 400×340 insight charts), CSS `width: 100%; height: auto`; every canvas scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Shared histogram helper:** white background, centered bold 13px `#1a5276` title, gray `#999` L axes (margins 35/20/40/50), per-bin bars with 1px gap, Gaussian-smoothed (sigma 1.5 bins) density line `#1a5276` width 2 over a 95% SE band filled `rgba(230,126,34,0.22)`, 6 x-tick labels 11px `#555` with optional 12px `#333` x-axis label. Data generated with seeded mulberry32(42) RNG and Box-Muller normal sampler.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`, yellow `rgba(241,196,15,0.6)`.
