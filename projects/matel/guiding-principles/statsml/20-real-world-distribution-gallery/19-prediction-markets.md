# Prediction Markets — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, main canvas center 31%, insight canvas right 31%, one table per section)
**HTML title tag:** Prediction Markets — Distribution Patterns

## Contract Price Before Expiry (Info Arrival)

**Label:** U-SHAPED BETA (color `#795548`)

Early in a contract's life = flat/uniform (no information). As expiry approaches, distribution becomes U-shaped Beta with mass piling at 0¢ and 100¢. The flat → U-shaped transition tracks information arriving in the market. One reading: fast U-formation suggests an efficient market, and a sudden U can hint at leaked information.

- Early: flat/uniform (maximum uncertainty)
- Late: U-shaped (market has resolved to near-certain)
- Transition speed reads as a market-efficiency proxy
- Sudden U-formation = possible insider trading signal

### Visualization (canvas `canvas1`, 420×340)

U-shaped Beta histogram.

- **Data:** seeded RNG mulberry32(42) shared across the page; 5000 draws from Beta(0.3, 0.3) via two gamma variates g1/(g1+g2) (Ahrens-Dieter for shape<1, Marsaglia-Tsang otherwise), scaled to 0–100 cents.
- **Bins/axes:** 40 bins over the data range; x ticks at 5 values (one decimal); y "Frequency"; axes `#333`; margins top 40 / right 30 / bottom 50 / left 60; white background.
- **Title (bold 14px, `#1a5276`):** "U-Shaped Beta: Contract Prices Near Expiry". **X label:** "Contract Price (cents)".
- **Bars:** fill `rgba(41,128,185,0.4)`, stroke `#1a5276`.
- **Density overlay:** Gaussian-kernel smoothed counts (sigma 1.5 bins), line `#1a5276` width 2 with SE band `rgba(41,128,185,0.2)` (1.96·smoothed/√effN, effN clamped 30–200).

### Visualization (canvas `canvas1b`, 400×340)

ECDF family showing information arrival over time.

- **Title (bold 12px, `#1a5276`):** "ECDF: Information Arrival Over Time".
- **Stages (800 Beta samples each, scaled to 0–100¢):** Beta(1.0, 1.0) in `rgba(39,174,96,0.8)` labeled "T-30 days (uniform)"; Beta(0.6, 0.6) in `rgba(230,126,34,0.8)` labeled "T-7 days"; Beta(0.3, 0.3) in `rgba(231,76,60,0.8)` labeled "T-1 day (resolved)". Each drawn as an ECDF line, width 2.5.
- **Annotations (bold 10px `#e74c3c`):** "← Mass at 0¢" near top-left of plot; "Mass at 100¢ →" right-aligned at mid-height.
- **Legend:** color swatch dashes + labels, upper right area.
- **Axes:** x ticks "0¢", "25¢", "50¢", "75¢", "100¢"; grid lines `#eee` at quarters; axis titles "Contract Price (cents)" and rotated "Cumulative Prob."; margins top 38 / right 20 / bottom 48 / left 50.

## Calibration Curve (Favorite–Longshot Bias)

**Label:** S-SHAPED BIAS (color `#2980b9`)

Predicted probability vs actual frequency should be a 45° line. The simulated curve bends into an S: near-calibrated around 50¢, compressed at the extremes — contracts priced 90¢ resolve YES ~95% of the time, while 10¢ longshots win only ~5%. One explanation: traders overpay for lottery-like longshots. The gap between the two curves maps where contrarian bets would profit.

- Should be linear 45° (perfect calibration)
- Simulated: near-calibrated at 50¢, compressed at extremes
- Priced 90¢ → resolves ~95%; priced 10¢ → resolves ~5%
- Gap between curves = where a contrarian edge would sit

### Visualization (canvas `canvas2`, 420×340)

Line chart: perfect vs actual calibration.

- **Data:** 101 points over x 0–1. Ideal: y = x. Actual: y = x + 0.12·(2x−1)·x·(1−x) + 0.08·(2x−1)³, clamped to [0,1], with y=0 at x=0 and y=1 at x=1.
- **Series:** "Perfect (45°)" in `#aaa`, width 1.5, dash [5,5]; "Actual (S-curve)" in `#e74c3c`, width 2.5. Legend dashes top-right.
- **Title (bold 14px, `#1a5276`):** "Calibration: Predicted vs Actual Frequency". **Axes:** x "Predicted Probability" and y "Actual Frequency", both 0–1 formatted as percentages "0%"…"100%"; `#eee` horizontal gridlines at quarters; axes `#333`; margins top 40 / right 30 / bottom 50 / left 60.

### Visualization (canvas `canvas2b`, 400×340)

Edge (actual − predicted) area chart marking profit zones.

- **Title (bold 12px, `#1a5276`):** "Tradeable Edge: Profit per Contrarian Bet".
- **Curve:** edge = actual − predicted from the same S-curve formula, 51 points, clamped to [−0.05, +0.05]; line `#1a5276` width 2.5; dashed `#999` zero line.
- **Fills:** positive edge regions filled `rgba(39,174,96,0.6)` (buy opportunity); negative regions filled `rgba(231,76,60,0.6)` (sell opportunity).
- **Annotations (bold 10px):** green "BUY 90%+" / "(+5% edge)" with a small green down-arrow at ~90% x; red "SELL <25%" / "(longshots overpriced)" at ~12% x.
- **Axes:** x ticks "0%", "25%", "50%", "75%", "100%" with title "Market Predicted Probability"; y ticks "+5%", "0%", "-5%" with rotated title "Edge (Actual - Predicted)"; margins top 38 / right 20 / bottom 48 / left 50.

## Bid-Ask Spread (Info Asymmetry at Extremes)

**Label:** U-SHAPED SPREAD (color `#27ae60`)

Spread is tightest at 50¢ (balanced opinion, high liquidity), widest near 5¢ and 95¢. Counterintuitive at first — one explanation is adverse selection: at 95¢, whoever still disagrees with consensus may know something, so market makers widen quotes. Spread width at the extremes reads as an information-asymmetry gauge.

- Tightest at 50% (balanced opinion, high liquidity)
- Widest at extremes (who disagrees with 95%?)
- One reading: U-shape gauges information asymmetry
- Wide spread at 95% = "someone might know something"

### Visualization (canvas `canvas3`, 420×340)

Line chart of spread vs contract probability.

- **Data:** 101 points over p 0–1: spread = 2 + 6·(2p−1)² + 0.5·max(0, p−0.5)^1.5·10 (slight high-end asymmetry from adverse selection) + 0.3·randn() noise, floored at 0.5.
- **Series:** single line "Bid-Ask Spread" in `#1a5276`, width 2.5.
- **Title (bold 14px, `#1a5276`):** "Bid-Ask Spread vs Contract Probability". **Axes:** x "Contract Probability" 0–1 formatted "0%"…"100%"; y "Spread (cents)" 0–12 formatted "0¢"…"12¢"; same line-chart frame as canvas2.

### Visualization (canvas `canvas3b`, 400×340)

Heatmap strips of adverse-selection intensity by participant type.

- **Title (bold 12px, `#1a5276`):** "Adverse Selection Intensity Map". **Subtitle (10px `#666`):** "Who trades against you at each price level?".
- **Rows (4 strips, 40 cells each over p 0–1, intensity functions):** "Informed Insiders" 0.1 + 0.8·(2p−1)²; "Noise Traders" 0.7 − 0.4·(2p−1)²; "Market Makers" 0.5 + 0.1·cos(πp); "Contrarians" 0.2 + 0.6·|2p−1|. Row labels right-aligned 10px `#333`; strip borders `#ccc`.
- **Color scale:** three-segment gradient from cool blue through olive to hot orange-red (e.g. low ≈ rgb(26,82,180) → mid ≈ rgb(126,162,120) → high ≈ rgb(230,36,60), alpha 0.8).
- **Annotations:** bold red "← DANGER" pointing at the insider hot zone near 95% on the first strip; gradient legend bar (100px) bottom center between 9px labels "Low activity" and "High activity".
- **Axes:** x ticks "0%", "25%", "50%", "75%", "100%" with title "Contract Probability"; margins top 38 / right 20 / bottom 60 / left 50.

## Volume Before Resolution (Surprise Magnitude)

**Label:** JUMP PROCESS (color `#e74c3c`)

Flat baseline volume, then exponential explosion in final hours/minutes before resolution. The spike-to-baseline ratio offers one way to quantify how "surprising" the outcome was: an expected resolution produces a moderate spike, while a shock produces a massive one as the market scrambles to reprice.

- Flat baseline during contract life
- Exponential explosion in final hours
- Spike-to-baseline ratio as a surprise gauge
- Large spike suggests the outcome contradicted consensus

### Visualization (canvas `canvas4`, 420×340)

Time-series line chart of trading volume.

- **Data:** 100 time points; named parameters baseline 50, riseGain 80, spikeExp 5, spikeScale 20 (shared with the canvas4b waterfall). For t<0.8: baseline + 8·randn() (flat). 0.8≤t<0.9: linear rise to +riseGain with 10·randn() noise. t≥0.9: baseline + riseGain + exp(progress·spikeExp)·spikeScale + 15·randn() (exponential explosion). Floored at 5.
- **Series:** "Trading Volume" red `#e74c3c` width 2; "Baseline" green `#27ae60` width 1.5, dash [4,4], horizontal at 50.
- **Title (bold 14px, `#1a5276`):** "Volume Spike Before Resolution (Jump Process)". **Axes:** x 0–100 formatted as hours-remaining "100h"…"0h" with title "Time (hours before resolution)"; y 0 to 1.1×max volume with title "Volume (contracts)"; same line-chart frame.

### Visualization (canvas `canvas4b`, 400×340)

Waterfall decomposition of volume by phase.

- **Title (bold 12px, `#1a5276`):** "Waterfall: Volume Decomposition by Phase".
- **Phases (label, value, fill):** values derived from the canvas4 generation parameters, not hardcoded — "Baseline (organic)" +baseline (= +50) `rgba(41,128,185,0.7)`; "News leaks (gradual rise)" +riseGain (= +80) `rgba(230,126,34,0.7)`; "Resolution scramble" +round(exp(spikeExp)·spikeScale) (= +2968) `rgba(192,57,43,0.8)`; "Post-settle (decay)" −(baseline + riseGain + spikePeak) (= −3098) `rgba(39,174,96,0.7)`; "Final" total bar (= 0) `rgba(26,82,118,0.8)`. Each two-line label below its bar (9px `#555`).
- **Geometry:** cumulative floating bars against y 0 to ceil(1.08·peakCum/100)·100 (= 3400), `#333` 0.5px outlines, dashed `#999` connectors between bars; bold 10px value labels above bars ("+50", "+80", "+2968", "-3098", final total).
- **Annotation:** red `#e74c3c` horizontal bracket over the rise + scramble bars with bold label "← Surprise = Nx baseline →", where N = (baseline + riseGain + spikePeak)/baseline rendered with toFixed(0) (= 62x) — matches the primary chart's spike-to-baseline ratio.
- **Axes:** y ticks at quarters of the computed max, rotated title "Cumulative Volume"; `#eee` gridlines; margins top 38 / right 20 / bottom 55 / left 55.

## Trader P&L (Domain-Specific Skill)

**Label:** DOMAIN-STRATIFIED NORMAL (color `#8e44ad`)

Stratify simulated P&L by domain expertise and it splits: generalists sit in a tight bell at -4% (consistent with fee drag, no edge), while specialists are bimodal — roughly a third cluster near +5% in their niche, the rest still near -3%. Consistent with prediction skill being domain-specific rather than general; skill behaves like a latent mixture variable.

- Generalists: tight normal at -4% (no edge over fees)
- Specialists: bimodal — ~1/3 near +5% in their niche
- Most specialists still sit near -3%
- One reading: skill shows up domain-by-domain, not in general

### Visualization (canvas `canvas5`, 420×340)

Overlaid histograms of generalist vs specialist P&L.

- **Data:** Generalists: 3000 draws of −4 + 1.5·randn(). Specialists: 2000 draws, p=0.35 → 5 + 2·randn() (niche winners), else −3 + 1.8·randn().
- **Bins/axes:** 50 shared bins over x −12 to 14; y scaled to the joint max count.
- **Title (bold 14px, `#1a5276`):** "Trader P&L: Generalists vs Domain Specialists". **X label:** "P&L (%)" with tick labels "−12%"…"14%" plus a dashed `#333` vertical zero line labeled "0%". **Y label:** "Frequency".
- **Series:** generalists fill `rgba(41,128,185,0.4)` stroke `#1a5276`; specialists overlaid fill `rgba(230,126,34,0.4)` stroke `#e67e22`.
- **Legend (top-right):** swatch `rgba(26,82,118,0.5)` + "Generalists (n=3000)" in `#1a5276`; swatch `rgba(230,126,34,0.5)` + "Specialists (n=2000)" in `#e67e22`.

### Visualization (canvas `canvas5b`, 400×340)

Radar chart of domain-specific skill profiles.

- **Title (bold 12px, `#1a5276`):** "Radar: Skill is Domain-Specific".
- **Axes (6 spokes):** Politics, Crypto, Sports, Science, Finance, Culture; 4 concentric `#ddd` rings; `#aaa` spokes; bold 10px `#333` labels outside.
- **Profiles (values per domain 0–1, stroke / fill):** "Generalist" [0.35, 0.30, 0.32, 0.28, 0.33, 0.31] `rgba(41,128,185,0.7)` / `rgba(41,128,185,0.15)`; "Poly Specialist" [0.85, 0.15, 0.20, 0.70, 0.25, 0.18] `rgba(231,76,60,0.8)` / `rgba(231,76,60,0.15)`; "Crypto Specialist" [0.20, 0.90, 0.15, 0.25, 0.75, 0.12] `rgba(39,174,96,0.8)` / `rgba(39,174,96,0.15)`. Polygons stroked width 2.5 with 3px vertex dots.
- **Legend (bottom-left):** color dash + label per profile.
- **Annotations:** bold 9px red "SPIKE = EDGE" bottom-right; 9px `#1a5276` "Flat = no edge (generalist)" bottom-left.

## Regeneration instructions

- **Layout:** one `.obj-table` (full-width, `border-collapse: collapse`) per pitfall, single `<tr>` with three `<td>`s: text 38%, main canvas 31% centered, insight canvas 31% centered. Cell borders `1px solid #2980b9`, padding 12px. Each text cell: `.pitfall-label` span, `<h3>`, one `<p>`, one 4-item `<ul>`.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.obj-table h3` 1.0em weight 700 `#1a5276`; p/li 14px, line-height 1.5–1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px. `canvas { width: 100%; height: auto; }`. No nav bar, no back/home links.
- **Label colors:** assigned by section index from the palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script.
- **Canvases:** intrinsic sizes 420×340 (main) and 400×340 (insight); set `max-width` to the intrinsic width, size the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and `ctx.scale` by that combined factor. Data generated with seeded mulberry32(42) RNG shared sequentially across all charts, plus Box-Muller `randn()`, `randExp(lambda)`, and a gamma sampler (Ahrens-Dieter for shape<1, Marsaglia-Tsang otherwise) for Beta variates.
- **Shared helpers:** `drawHistogram` (margins {top 40, right 30, bottom 50, left 60}, `#333` axes, smoothed density line `#1a5276` with band `rgba(41,128,185,0.2)`) and `drawLineChart` (same margins, `#eee` gridlines at quarters, per-dataset color/width/dash/legend, configurable x/y ranges and tick formatters).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, purple `#8e44ad`, dark red `#c0392b`, gray `#aaa`/`#999`.
