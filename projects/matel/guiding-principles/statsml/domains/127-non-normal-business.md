# Non-Normal Distributions as Core Business Metrics

**Page type:** detail page (h2 heading per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 127. Non-Normal Distributions as Core Business Metrics

**Subtitle:** Core business metrics are zero-inflated, bimodal, heavy-tailed, or power-law — so means and t-tests describe nobody.

## Revenue Per Session — 90% Zero

**Obj-title:** Revenue Per Session — 90% Zero

- 90% of sessions generate $0 (didn't buy)
- 9% small purchases, 1% large orders
- Mean is meaningless; t-test assumptions violated

**Example:** "Average revenue per session = $2.40" — but 90% see $0 and the 1% who spend $200+ drive the entire average.

### Visualization (canvas `c1`, declared 720×300, script renders at 720×200)

Histogram: huge spike at $0 followed by a long decaying tail.

- **Title (17px `#1a5276`):** "Revenue Per Session Distribution" at (10,25). Axis frame `#2980b9` width 1: (60,40)–(60,175)–(680,175).
- **Zero spike:** `#e74c3c` bar at (70,45) 40×130, labeled "$0 (90%)" in 11px `#333` at (65,190).
- **Tail bars (`#2980b9`, 30px wide at x = 130 + i*38, height = value×8):** `[30, 22, 15, 10, 7, 4, 3, 2, 1.5, 1, 0.8, 0.5, 0.3, 0.2]`.
- **X labels (11px `#333`):** "$1-10" at (150,190); "$50+" at (400,190); "$200+" at (600,190).
- **Annotation (12px `#e74c3c`):** "Mean = $2.40 (represents nobody)" at (400,40).

## Transaction Amounts — Bimodal

**Obj-title:** Transaction Amounts — Bimodal

- Everyday small purchases cluster around $10-50
- Rare large purchases at $500-5000
- Can't average across two distinct populations

**Example:** "Average transaction = $85" represents neither the $25 coffee-run crowd nor the $800 electronics buyers.

### Visualization (canvas `c2`, declared 720×300, script renders at 720×200)

Bimodal histogram with a mean marker falling in the empty valley between the two modes.

- **Title (17px `#1a5276`):** "Transaction Amounts: Bimodal" at (10,25). Axis frame `#2980b9` width 1: (60,40)–(60,175)–(680,175).
- **Small-purchase mode (`#2980b9`, 24px wide at x = 70 + i*28, height = value×1.4):** `[10, 40, 70, 85, 90, 75, 50, 30, 15, 5]`, labeled "$10-50" (12px `#333`) at (130,190).
- **Large-purchase mode (`#e67e22`, 16px wide at x = 380 + i*20, height = value×1.4):** `[0, 0, 0, 0, 0, 3, 8, 20, 35, 45, 40, 25, 12, 5]`, labeled "$500-5000" at (440,190).
- **Mean marker:** vertical dashed `#e74c3c` (dash 4/4, width 2) at x=330 from y=40 to y=175, labeled "Mean = $85 (nobody here)" in 12px `#e74c3c` at (300,38).

## Session Duration — Zero-Inflated + Heavy Tail

**Obj-title:** Session Duration — Zero-Inflated + Heavy Tail

- Bounces: 0-3 seconds (40%+ of sessions)
- Normal browsing: 1-5 minutes
- Left tab open: 30+ minutes (not real engagement)

**Example:** "Average session = 3 min" represents nobody — most are 5 seconds or 25 minutes with nothing in between.

### Visualization (canvas `c3`, declared 720×300, script renders at 720×200)

Histogram: bounce spike, mid-range hump, and a low flat far tail.

- **Title (17px `#1a5276`):** "Session Duration Distribution" at (10,25). Axis frame `#2980b9` width 1: (60,40)–(60,175)–(680,175).
- **Bounce spike:** `#e74c3c` bar at (70,55) 35×120, labeled "Bounce" (10px `#333`) at (70,190) and "0-3s (40%)" at (63,50).
- **Mid bars (`#2980b9`, 25px wide at x = 130 + i*30, height = value×2.2):** `[20, 35, 50, 45, 35, 25, 15, 8, 4, 2, 1]`, labeled "1-5 min" (11px `#333`) at (200,190).
- **Tail bars (`#e67e22`):** five short rects at (500,160) 25×15, (535,162) 25×13, (570,163) 25×12, (605,165) 25×10, (640,166) 25×9; labeled "30+ min (tab left open)" in 11px `#e67e22` at (520,155).
- **Annotation (12px `#e74c3c`):** "\"Avg = 3 min\" represents nobody" at (350,40).

## Time to Purchase — Right-Skewed Censored

**Obj-title:** Time to Purchase — Right-Skewed Censored

- Some never buy (can't observe infinity)
- Of those who buy: highly right-skewed
- Survival analysis needed, not averages

**Example:** "Average time to first purchase = 14 days" ignores that 70% never purchase at all. Conditional mean meaningless without survival framing.

### Visualization (canvas `c4`, declared 720×300, script renders at 720×200)

Survival curve flattening at a 70% censoring asymptote.

- **Title (17px `#1a5276`):** "Time to Purchase (Survival Curve)" at (10,25). Axis frame `#2980b9` width 1: (80,40)–(80,175)–(680,175). Y labels (11px `#666`): "100%" at y=50, "70%" at y=90, "0%" at y=175; x label "Days" at (370,192).
- **Survival curve:** `#1a5276` width 2.5, starting at (80,47), quadratic curves through (200,55)→(250,70) and (350,82)→(450,87), then a straight line to (680,90) — flattening near the 70% line.
- **Censoring line:** horizontal dashed `#e74c3c` (dash 4/4, width 1.5) at y=90 from x=80 to x=680.
- **Annotations (12px):** "70% NEVER purchase (censored)" in `#e74c3c` at (400,105); "Survival: % who haven't bought yet" in `#1a5276` at (400,60).

## Rating Scores — J-Shaped Distribution

**Obj-title:** Rating Scores — J-Shaped Distribution

- Mostly 5 stars + cluster of 1 stars
- Almost no 2, 3, or 4 star ratings
- Mean of 4.2 hides bipolarity completely

**Example:** Product has mean 4.2 stars. Reality: 70% give 5 stars, 20% give 1 star, 10% give 2-4. Polarizing product looks "good."

### Visualization (canvas `c5`, declared 720×300, script renders at 720×200)

Bar chart of star-rating percentages showing the J shape, with the mean marked in the empty middle.

- **Title (17px `#1a5276`):** "Rating Distribution (J-Shape)" at (10,25). Axis frame `#2980b9` width 1: (80,40)–(80,175)–(500,175).
- **Bars (55px wide at x = 110 + i*75, height = pct×1.8, baseline y=175):** 1 star 20% `#e74c3c`; 2 star 3% `#e67e22`; 3 star 3% `#f39c12`; 4 star 4% `#27ae60`; 5 star 70% `#2980b9`. Labels "1 star"…"5 star" (13px `#333`) below, percentage values above each bar.
- **Mean marker:** vertical dashed `#e74c3c` (dash 4/4, width 2) at x = 110 + 3.2×75 (i.e. between 4 and 5 stars) from y=40 to y=175, labeled "Mean = 4.2" (13px `#e74c3c`); "(hides bipolarity)" at (520,100).

## Support Ticket Resolution Time — Log-Normal

**Obj-title:** Support Ticket Resolution Time — Log-Normal

- Most tickets resolved in ~2 hours
- Some take 2 weeks (extreme tail)
- p50 vs p99 = 100× different

**Example:** Median resolution = 2 hours, mean = 18 hours, p99 = 336 hours. "Average resolution time" is misleading for 95% of customers.

### Visualization (canvas `c6`, declared 720×300, script renders at 720×200)

Log-scale histogram of resolution time with p50 and mean markers far apart.

- **Title (17px `#1a5276`):** "Ticket Resolution Time (Log Scale)" at (10,25). Axis frame `#2980b9` width 1: (60,40)–(60,170)–(680,170).
- **Bars (`#2980b9`, 28px wide at x = 70 + i*33, height = value×1.3, baseline y=170):** `[5, 25, 70, 95, 80, 50, 30, 18, 10, 6, 4, 3, 2, 1.5, 1, 0.8, 0.5, 0.3]`.
- **X labels (11px `#333`):** "1h" at (80,185); "4h" at (180,185); "24h" at (300,185); "1wk" at (430,185); "2wk+" at (560,185).
- **Markers:** vertical dashed `#27ae60` line at x=136 labeled "p50=2h" (12px `#27ae60` at (100,38)); vertical dashed `#e74c3c` line at x=280 labeled "mean=18h" (12px `#e74c3c` at (250,38)); both from y=40 to y=170, dash 4/4.
- **Annotation (12px `#666`):** "p99=336h (100x p50!)" at (500,55).

## Ad Spend ROI — Zero-Inflated + Extreme Returns

**Obj-title:** Ad Spend ROI — Zero-Inflated + Extreme Returns

- Most campaigns return 0-1× (break even or lose)
- Occasional campaign returns 100×
- Mean ROI driven by 1% of campaigns

**Example:** "Average campaign ROI = 5×" driven entirely by 2 viral campaigns. Median campaign ROI = 0.8× (net loss).

### Visualization (canvas `c7`, declared 720×300, script renders at 720×200)

Histogram of campaign ROI: tall bars near 0× shrinking rapidly, with a tiny isolated bar at 100×.

- **Title (17px `#1a5276`):** "Campaign ROI Distribution" at (10,25). Axis frame `#2980b9` width 1: (60,40)–(60,170)–(680,170).
- **Bars (50px wide, baseline y=170):** 0x — `#e74c3c` at (80,60) height 110; 0.5x — `#e67e22` at (140,85) height 85; 1x — `#f39c12` at (200,110) height 60; 2x — `#2980b9` at (260,135) height 35; 3x — `#27ae60` at (320,150) height 20; isolated 100x — `#27ae60` at (560,155) 30×15.
- **X labels (11px `#333`):** "0x", "0.5x", "1x", "2x", "3x", "...", "100x".
- **Annotations (12px):** "Median ROI = 0.8x (loss!)" in `#e74c3c` at (420,80); "Mean ROI = 5x" in `#27ae60` at (420,100); "(driven by 1% of campaigns)" in `#666` at (420,116).

## Revenue Per Customer — Power-Law

**Obj-title:** Revenue Per Customer — Power-Law

- Whales dominate total revenue
- Removing top 1% changes "average" by 40%
- Most customers worth <$10, few worth >$10,000

**Example:** Mean revenue/customer = $150. Remove top 1% of customers: mean drops to $90. One whale ≈ 70 regular customers.

### Visualization (canvas `c8`, declared 720×300, script renders at 720×200)

Power-law decay curve with shaded area under it and a top-1% whale cutoff marker.

- **Title (17px `#1a5276`):** "Revenue Per Customer (Power-Law)" at (10,25). Axis frame `#2980b9` width 1: (60,40)–(60,175)–(680,175).
- **Curve:** `#1a5276` width 2.5, y = 175 − 125·(0.993)^(x−70) for x from 70 to 670 (steep drop then long flat tail); area under curve filled `rgba(41,128,185,0.3)`.
- **Whale cutoff:** vertical dashed `#e74c3c` (dash 4/4, width 1.5) at x=130 from y=40 to y=175, labeled "Top 1% (whales)" in 11px `#e74c3c` at (85,38).
- **Labels:** "Bottom 80%" in 11px `#333` at (400,165); "Remove top 1% -> mean drops 40%" in 12px `#666` at (350,60).

## Regeneration instructions

- **Layout:** for each pitfall: an `<h2>` section heading (1.4em, `#1a5276`, 2px solid `#2980b9` bottom border), then a `.obj-table` (full-width, border-collapse) containing one `<tr>`; left `<td>` (40%) holds `.obj-title` div + `<ul>` bullets + a `<p><strong>Example:</strong> ...</p>` paragraph; right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page style:** body -apple-system/BlinkMacSystemFont/'Segoe UI' sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; p 0.95em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`. A `.philosophy` class exists (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but is unused on this page. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"`; a shared setup loop over all canvases sets each backing store to 720×200 × `window.devicePixelRatio` to 720×200px, and calls `ctx.scale` so drawing stays in logical coordinates; per-chart IIFEs then draw. Title font 17px -apple-system, sans-serif.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, amber `#f39c12`, bar-area fill `rgba(41,128,185,0.3)`, grays `#666`/`#333`.
- Note: in regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
