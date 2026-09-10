# Advertising & AdTech — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, histogram canvas middle 31%, insight canvas right 31%, one table per section)
**HTML title tag:** Advertising & AdTech — Distribution Patterns

## Click-Through Rate — Most Ads Get Ignored (Beta shape)

**Pitfall label:** ALMOST ALL VALUES CRAMMED NEAR ZERO (color `#795548`)

Across the simulated ads, the median CTR is about 1.1% — roughly 99 in 100 impressions scroll right past. Values are bounded at 0, pile up at the low end, and stretch into a long right tail: 95% of ads sit below ~4%. The kicker: going from 1.0% to 1.5% sounds like nothing, but that's 50% more clicks.

- The typical ad gets about one click per hundred views — most impressions are ignored
- The rare ad in the top 5% (above ~4% CTR) found a hook that defeats the "scroll past" reflex
- Tiny-sounding absolute improvements are massive in relative terms (+0.5pt on 1% = +50% clicks)
- Values are trapped between 0% and 100% and bunch near zero — a Beta shape, not a bell curve

### Visualization (canvas `canvas1`, 420×340)

Histogram of CTR across ads, extreme left-bunched Beta.

- **Data:** 5000 samples of Beta(1.2, 80) × 100 (percent scale), generated via Marsaglia-Tsang gamma sampling with seeded RNG mulberry32(101).
- **Chart:** 60 bins over x range 0–8. Bars filled `rgba(243,156,18,0.35)` stroked `#1a5276` (0.5px). Gaussian-smoothed density line (sigma 1.5 bins) in `#d68910` (2px) with 95% SE band filled `rgba(243,156,18,0.18)`. White background, light gray axes `#ccc` with `#eee` horizontal gridlines, 5 rounded-count y ticks, 6 x ticks; margins top 40 / right 20 / bottom 50 / left 55.
- **Title (bold 13px `#1a5276`, top center):** "CTR Distribution Across Ads (Beta(1.2, 80))"
- **X-axis label:** "Click-Through Rate (%)"

### Visualization (canvas `canvas1b`, 400×340)

ECDF of CTR with percentile markers.

- **Title (bold 12px `#1a5276`):** "ECDF: How Fast CTR Saturates".
- **Curve:** ECDF of sorted CTR data over x 0–5%, stroke `#e67e22` width 2.5. Margins top 35 / right 15 / bottom 45 / left 50; y ticks 0%–100% by 20% with `#eee` gridlines; x ticks 0.0%–5.0% by 1.0%; x-axis title "CTR (%)".
- **Percentile markers:** at the 50th, 90th, 95th, 99th percentiles — vertical dashed line (dash 3/3) from baseline to curve, 4px dot on the curve, bold 10px label "{P}% at {value}%". Colors: 50% `#27ae60`, 90% `#2980b9`, 95% `#8e44ad`, 99% `#e74c3c`.
- **Annotation (bold 11px `#c0392b`, centered near baseline):** "95% of ads below {p95}% CTR" (p95 ≈ 4%).

## Cost-Per-Click — The Price Moves With Who's Bidding (Log-normal shape)

**Pitfall label:** LOPSIDED HILL THAT SLIDES LEFT AND RIGHT (color `#2980b9`)

Imagine an auction where the price of a click forms a hill tilted to the right. On weekdays, business-focused advertisers flood in and push the whole hill rightward (more expensive). On weekends, those advertisers back off and the hill slides left (cheaper). Where the hill sits on a given day is a readable signal of who is competing in the auction — here the weekday shift is roughly +65% at every percentile.

- The shape is a hill with a long tail stretching toward expensive clicks
- Weekdays: business advertisers bid aggressively, prices go up across the board
- Weekends: those bidders vanish, everything gets cheaper
- Where the hill sits on any given day tells you who is competing in the auction

### Visualization (canvas `canvas2`, 420×340)

Overlaid histograms of weekday vs weekend CPC (both log-normal).

- **Data:** 3000 samples each, seed 202. Weekday: `exp(0.7 + 0.6·N(0,1))` dollars. Weekend: `exp(0.2 + 0.6·N(0,1))` dollars.
- **Chart:** 50 bins over x range 0–8, shared count scale. Weekday bars `rgba(41,128,185,0.35)` stroked `#1a5276`; weekend overlay bars `rgba(39,174,96,0.30)` stroked `#27ae60`. Smoothed density line `#d68910` + SE band `rgba(243,156,18,0.18)` (computed on the weekday series, shared helper). Legend (top right): blue swatch "Weekday (Mon-Fri)", green swatch "Weekend (Sat-Sun)".
- **Title:** "CPC Distribution — Weekday vs Weekend"
- **X-axis label:** "Cost Per Click ($)"

### Visualization (canvas `canvas2b`, 400×340)

Lollipop / dumbbell chart of the percentile shift from weekend to weekday CPC.

- **Title:** "Percentile Shift: Weekday vs Weekend CPC".
- **Rows:** percentiles P10, P25, P50, P75, P90, P95 (one horizontal row each, labeled at left). For each row: green dot `rgba(39,174,96,0.8)` at the weekend value, blue dot `rgba(41,128,185,0.8)` at the weekday value (5px radius), joined by a red connector `rgba(231,76,60,0.7)` (2px) ending in a red arrowhead at the weekday dot; bold 9px `#c0392b` "+{shift}%" label above the midpoint (≈+65% at every percentile).
- **Axes:** x 0–$6 with "$0"–"$6" ticks and light `#f0f0f0` vertical gridlines; x-axis title "Cost Per Click ($)".
- **Legend (top left):** green dot "Weekend", blue dot "Weekday".

## Attribution Lag — Pay-Cycle Bumps Hidden in the Data (Exponential + spikes)

**Pitfall label:** FAST DROP-OFF WITH SURPRISE BUMPS LATER (color `#27ae60`)

Just over half of the simulated conversions land within a day of the click — the impulse crowd — and the rate drops off fast after that. Then two bumps appear, near day 7 and day 30. One explanation: pay cycles — people click, can't afford it yet, and come back when money arrives. Whatever the cause, a 7-day window captures about two-thirds of conversions here; a 30-day window captures over 90%.

- Over half of conversions arrive within the first day — fast, impulse-like buying
- Interest drops off rapidly after that — an exponential-style decay
- Bumps at day 7 and day 30 — consistent with paycheck timing
- A 7-day tracking window misses roughly a quarter of what a 30-day window catches

### Visualization (canvas `canvas3`, 420×340)

Histogram of conversion lag in days: exponential decay plus paycheck spikes.

- **Data:** 5000 samples, seed 303. 55% exponential base: `Exp(λ=0.15)` hours converted to days (÷24). 20% spike near day 7: `7 + 0.8·N(0,1)` (positive only). 15% spike near day 30: `30 + 1.2·N(0,1)` (positive only). 10% uniform background scatter over 0–35 days.
- **Chart:** 70 bins over x range 0–35. Bars `rgba(231,76,60,0.35)` stroked `#1a5276`. Smoothed density line `#d68910` + SE band `rgba(243,156,18,0.18)` (shared helper).
- **Title:** "Attribution Lag — Exponential + Paycheck Spikes"
- **X-axis label:** "Days After Click"

### Visualization (canvas `canvas3b`, 400×340)

Cumulative capture curve with attribution-window cutoffs.

- **Title:** "Cumulative Conversions Captured".
- **Curve:** cumulative fraction of conversions captured vs attribution window (0–35 days, 0.5-day steps), stroke `#e74c3c` width 2.5, area under curve filled `rgba(231,76,60,0.15)`. Y ticks 0%–100% by 20% with `#eee` gridlines; x ticks 0d–35d in 5-day steps; x-axis title "Attribution Window (days)".
- **Window markers:** at 1, 7, 14, 30 days — vertical dashed line (dash 4/3) from baseline to curve, 4px dot, bold 9px label "{window}: {pct}%". Colors: 1-day `#27ae60`, 7-day `#2980b9`, 14-day `#8e44ad`, 30-day `#e67e22`.
- **Annotation (bold 11px `#c0392b`, top center):** "7d window misses {N}% more conversions" (difference between 30-day and 7-day capture, ≈25%).

## Ad Frequency — Response Collapses Past a Point (Geometric + cliff)

**Pitfall label:** STEADY DECLINE THEN A SUDDEN WALL (color `#e74c3c`)

Each additional exposure to the same ad engages fewer people — a gradual geometric decline. But the decline doesn't fade smoothly to zero: at some repetition count, response collapses, consistent with the ad becoming background noise. In this simulation the banner curve collapses after the 3rd view and the video curve after the 7th — the exact numbers vary by format and audience; the cliff shape is the point.

- Fewer people engage each additional time they see the ad — gradual decline at first
- Then a collapse: response drops to near zero within an exposure or two
- In this model, banner hits the wall around 3 views and video around 7 — illustrative, not universal constants
- Averaging CTR across frequencies hides the cliff — split by exposure count

### Visualization (canvas `canvas4`, 420×340)

Histogram of engaged ad frequency: geometric decay truncated by a fatigue cliff.

- **Data:** seed 404. 4000 draws from Geometric(p=0.35) (`floor(log(1-u)/log(0.65)) + 1`), keeping only k ≤ 6 (the fatigue cliff at 6, blending banner ~3 and video ~7); plus 50 extra values uniformly in 7–9 to show the drop-off past the cliff.
- **Chart:** 12 bins over x range 0.5–10.5. Bars `rgba(142,68,173,0.35)` stroked `#1a5276`. Smoothed density line `#d68910` + SE band `rgba(243,156,18,0.18)` (shared helper).
- **Title:** "Ad Frequency — Geometric Decay + Fatigue Cliff"
- **X-axis label:** "Number of Times User Saw Ad"

### Visualization (canvas `canvas4b`, 400×340)

Two CTR-decay curves by format with cliff markers and a wasted-spend zone.

- **Title:** "CTR Decay by Format: The Fatigue Cliff".
- **Data (fixed arrays), frequencies 1x–10x:**
  - Banner CTR (%): `[2.8, 2.1, 1.4, 0.5, 0.15, 0.05, 0.02, 0.01, 0.01, 0.01]` — line + 3px dots in `#e74c3c`, width 2.5
  - Video CTR (%): `[3.5, 3.2, 2.9, 2.5, 2.0, 1.4, 0.6, 0.1, 0.03, 0.01]` — line + 3px dots in `#2980b9`, width 2.5
- **Axes:** y 0%–4% (ticks each 1%) with `#eee` gridlines; x ticks "1x"–"10x"; x-axis title "Ad Frequency (impressions)".
- **Cliff markers:** vertical dashed red line (`#e74c3c`, dash 5/3) at 3x with bold red two-line label "Banner cliff" / "(3x)", and the region right of 3x tinted `rgba(231,76,60,0.1)`; vertical dashed blue line (`#2980b9`, dash 5/3) at 7x with bold blue label "Video cliff" / "(7x)".
- **Zone label:** bold 13px "WASTED SPEND" in `rgba(231,76,60,0.6)` centered at ~75% width, mid-height.
- **Legend (bottom left):** red line swatch "Banner", blue line swatch "Video".

## Revenue Per Impression — A Few Slots Pay for Everything (Zero-heavy + extreme tail)

**Pitfall label:** TONS OF PENNIES, A FEW JACKPOTS (color `#8e44ad`)

Most simulated ad slots earn a few cents per thousand impressions — some likely below what they cost to serve. A thin premium slice earns hundreds of times the median slot. The Lorenz curve makes the concentration visible: the top 5% of slots produce nearly half the revenue, while the bottom 60% produce about 5% — the same shape as wealth inequality.

- The majority of ad slots earn pennies per thousand views — near-worthless individually
- Premium slots earn hundreds of times more than the typical slot
- Top 5% of slots generate close to half the revenue; bottom 60% around 5%
- A handful of goldmine slots effectively subsidize all the low-earning ones

### Visualization (canvas `canvas5`, 420×340)

Histogram of eCPM: zero-inflated mass plus Pareto tail.

- **Data:** 6000 samples, seed 505. 60% remnant inventory near $0.01: `0.01 + |0.02·N(0,1)|`. 25% mid-tier: `0.1 + |0.3·N(0,1)|`. 15% Pareto tail (premium): `0.5/u^(1/1.5)` (x_m=0.5, alpha=1.5), capped at 30 for display.
- **Chart:** 80 bins over x range 0–15. Bars `rgba(39,174,96,0.35)` stroked `#1a5276`. Smoothed density line `#d68910` + SE band `rgba(243,156,18,0.18)` (shared helper).
- **Title:** "eCPM Distribution — Zero-Inflated Pareto"
- **X-axis label:** "Revenue Per 1000 Impressions ($)"

### Visualization (canvas `canvas5b`, 400×340)

Lorenz curve of ad-slot revenue concentration.

- **Title:** "Lorenz Curve: Ad Revenue Inequality".
- **Equality line:** gray dashed diagonal (`#999`, dash 4/4) with rotated 9px `#999` label "Perfect Equality" along it.
- **Lorenz curve:** cumulative revenue share vs cumulative slot share (sorted ascending, ~200 sampled points, closed at (1,1)), stroke `#27ae60` width 2.5; area between equality and Lorenz filled `rgba(39,174,96,0.2)`.
- **Annotations:**
  - Red 5px dot at the 60%-of-slots point with dashed red guide lines to both axes; bold 10px `#c0392b` two-line label "Bottom 60% of slots" / "= only {N}% revenue" (≈5%).
  - Bold 11px `#1a5276` top center: "Top 5% earns {N}% of revenue" (≈ near half).
  - Bold 10px `#8e44ad` bottom right: "Gini = {computed}".
- **Axes:** x and y both 0%–100% by 20% with `#eee` gridlines; x-axis title "% of Ad Slots (ranked low to high)".

## Regeneration instructions

- **Layout:** one `.obj-table` (full-width, border-collapse) per section, single `<tr>` with three `<td>`: left 38% text (`.pitfall-label` span, `h3`, paragraph, `ul`), middle 31% centered canvas (420×340), right 31% centered insight canvas (400×340). Cell borders `1px solid #2980b9`, padding 12px. Includes viewport meta tag.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `h3` in cells `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase with 0.5px letter-spacing. Canvas CSS `width: 100%; height: auto`. No nav bar, no back/home links.
- **Pitfall label colors:** assigned by a small script cycling through `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` in document order.
- **Data generation:** seeded RNG `mulberry32(seed)` per section (seeds 101, 202, 303, 404, 505), Box-Muller `randn()`, inverse-CDF `randExp(lambda)`, Marsaglia-Tsang gamma for Beta sampling.
- **Histogram helper:** shared `drawHistogram(canvasId, data, options)` — options bins/color/strokeColor/title/xLabel/min/max plus optional overlayData/overlayColor/overlayStroke/legendLabels for two-series histograms; white plot background; margins top 40 / right 20 / bottom 50 / left 55; title bold 13px `#1a5276` centered; light `#ccc` axes with `#eee` gridlines, 5 y ticks, 6 x ticks (smart decimal formatting); plus a Gaussian-smoothed density line (`#d68910`, sigma 1.5 bins) with a 95% SE band (`rgba(243,156,18,0.18)`, effective n clamped to [30, 200]) drawn on the primary series.
- **Canvas scaling:** all canvases set `max-width` to the intrinsic width, size the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and `ctx.scale` by that combined factor.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`, purple `#8e44ad`, amber `#f39c12`/`#d68910`, dark red `#c0392b`, gray text `#666`/`#333`.
- Note: regenerated HTML pages link nowhere (detail page); any grid page linking here uses the `.html` extension.
