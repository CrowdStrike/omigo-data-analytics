# Sports Betting Pitfalls

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Sports Betting Pitfalls - Domain-Specific Statistical Issues

**Subtitle:** Statistical traps that separate profitable bettors from losing ones

## Vig/Juice Hides Your Edge

**A 52%-Accurate Model Still Loses Money — Break-Even Is 52.4%**

- **The house cut:** The bookmaker takes a 4-10% margin on every bet placed.
- **Break-even bar:** At standard -110 on both sides you must win 52.4% just to tie.
- **Why 52% loses:** A "52% accurate" model bleeds bankroll once the vig is subtracted.
- **Metric blind spot:** Accuracy metrics never subtract the house cut, so edges look real.
- **The real target:** You need an edge over 50% + vig, a wider gap than most realize.

### Visualization (canvas `canvas1`, 500×300)

Line chart: break-even win rate as a function of vig percentage, on a light `#f0f4f8` panel background.

- **Title (bold 15px `#1a5276`):** "Break-Even Win Rate vs Vig %".
- **Axes:** margins top 45 / right 30 / bottom 50 / left 60; L-shaped `#333` axes. X axis 0–15% ("Vig / Juice (%)", labels every 3%); y axis 50–58% ("Break-Even Win %", labels every 2% with `#ddd` gridlines).
- **Data (vig% → break-even win rate):** (0, 50), (2, 51), (4, 52), (5, 52.4), (6, 53), (8, 54), (10, 55), (12, 56), (15, 57.5); red line `#e74c3c`, width 3.
- **50% line:** dashed green (`#27ae60`, width 2, dash 5/5) along the x-axis baseline (y=50%).
- **Standard -110 marker:** red dot (radius 6) at vig=4.55%, y=52.4%; bold 12px `#c0392b` label: "Standard -110: need 52.4%".
- **Profit zone:** translucent green fill `rgba(39,174,96,0.1)` covering the region above 52.4%; 13px `#27ae60` label top-right: "PROFIT ZONE (above line)".

## Closing Line Value (CLV)

**48% Wins While Beating Closing Beats 60% Wins Without It**

- **What closing is:** The odds at game time, the market's final aggregated consensus.
- **How value is measured:** Betting -3 into a -5 close captures 2 points, win or lose.
- **The only proven predictor:** CLV is the sole validated long-term profitability signal.
- **The 60% trap:** 60% wins at consistently worse-than-closing numbers reverts downward.
- **The 48% surprise:** 48% wins while beating closing still profits over the long run.

### Visualization (canvas `canvas2`, 500×300)

Scatter plot with trend line: average CLV vs. long-term profit for simulated bettors, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "CLV vs Long-Term Profit (Simulated Bettors)".
- **Axes:** x "Average CLV (cents)" spanning −6 to +6 (tick labels "-6", "0", "+6"); y "Profit/Loss (units)" spanning −150 to +150; dashed gray (`#aaa`, dash 4/4) zero lines both horizontal and vertical through the center.
- **Points** (radius 5; green `#27ae60` if profit ≥ 0, red `#e74c3c` if negative), (clv, profit): (−5,−120), (−4,−85), (−3,−60), (−2,−40), (−1,−15), (0,−5), (1,20), (2,45), (3,70), (4,95), (5,130), (−4.5,−100), (−2.5,−55), (−0.5,−10), (0.5,8), (1.5,30), (2.5,55), (3.5,80), (−3.5,−70), (−1.5,−30), (1.2,25), (−0.8,−12), (4.2,105).
- **Trend line:** blue `#2980b9` width 2 from (−6, −150) to (+6, +150).
- **Annotation (bold 12px `#2980b9`, top-right):** "r = 0.95".

## Sharp vs Public Money

**$10K From a Sharp Moves the Line More Than $100K From the Public**

- **The mechanism:** Books move lines on who bets, not on total money arriving.
- **The asymmetry:** A proven-winner pro's $10,000 outweighs $100,000 of recreational money.
- **Hidden ratings:** Books internally rate every account; that rating is invisible to you.
- **Why movement data fails:** Line movement without the money's source is uninterpretable.
- **The useless stat:** "80% of money on Team A" says nothing absent sharp-vs-public split.

### Visualization (canvas `canvas3`, 500×300)

Bar chart comparing line movement caused by sharp vs. public money, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Line Movement: Sharp $10K vs Public $100K".
- **Axes:** y "Line Movement (points)", 0 to 3.5 scale with labels every 0.5 and light `#eee` gridlines.
- **Bars** (four, evenly spaced, value label bold 13px `#333` above each, 11px `#555` category label below):
  - "Sharp $10K" — 1.5 pts — `#e74c3c`
  - "Public $100K" — 0.5 pts — `#3498db`
  - "Sharp $50K" — 3.0 pts — `#c0392b`
  - "Public $500K" — 1.0 pts — `#2471a3`
- **Annotation (bold 12px `#c0392b`, top):** "WHO bets matters more than HOW MUCH".

## Injury/News Latency

**Lines Move in Seconds; Edge Half-Life Is Minutes, Not Days**

- **Automated speed:** Injury news hits the wire and automated systems move lines in seconds.
- **Stale on arrival:** A model on yesterday's roster is stale the moment it predicts.
- **Edge decay:** Information edge half-life is measured in minutes, not days.
- **The human loop:** Read report, run model, place bet — the market already fully adjusted.
- **What it takes:** Exploiting injury news requires sub-second reaction, beating the market.

### Visualization (canvas `canvas4`, 500×300)

Exponential decay curve of remaining information edge after injury news, with actor markers, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Information Edge Half-Life After Injury News".
- **Axes:** x "Time since announcement" with tick labels "0s", "10s", "30s", "1m", "5m", "15m", "60m" at fractional positions 0, 0.003, 0.008, 0.017, 0.083, 0.25, 1.0 of the axis; y "Edge remaining (%)" 0–100.
- **Curve:** edge = 100·e^(−5t) for t in [0,1], red `#e74c3c` line, width 3.
- **Actor markers** (dot radius 6 on the curve, bold 11px label plus 10px "N% edge left" sub-label):
  - "Automated bots" at t=0.003 — `#e74c3c` — "99% edge left"
  - "Sharp bettors" at t=0.017 — `#e67e22` — "92% edge left"
  - "Your model" at t=0.25 — `#3498db` — "29% edge left"
  - "Casual bettor" at t=0.8 — `#95a5a6` — "2% edge left"

## Model vs Market Efficiency

**NFL Spreads Land Within 2-3 Points — Your Model Must Beat That**

- **The benchmark:** NFL point spreads are accurate to within 2-3 points of outcomes.
- **What the line is:** Aggregated intelligence of thousands of sharps, models, and makers.
- **The bar:** Your model must beat that collective intelligence, not merely match it.
- **Correlation is not edge:** Tracking the closing line without beating it means no edge.
- **Backtest illusion:** Most "successful" models are the market line plus added noise.

### Visualization (canvas `canvas5`, 500×300)

Two overlaid Gaussian prediction-error curves (market vs. model), on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "NFL Spread Accuracy: Market vs Typical Model".
- **Axes:** x "Prediction Error (points)" from −15 to +15, tick labels every 4 from −12 to +12 ("+" prefix on positives); only the horizontal baseline is drawn.
- **Curves** (Gaussian densities scaled to chart height, width 2.5, labeled bold 12px near the top at the mean):
  - Market: mean 0, std 2.5, green `#27ae60`, label "Market (std=2.5)".
  - Your Model: mean 0.5, std 3.5, red `#e74c3c`, label "Your Model (std=3.5)".
- **Annotation (12px `#c0392b`, two centered lines near baseline):** "Model must be TIGHTER than market" / "to have any edge".

## In-Play / Live Betting Volatility

**The Live Market Priced the Touchdown 200 Milliseconds Before You Did**

- **Every possession repriced:** Live odds shift constantly; one touchdown flips 7+ points.
- **What the market absorbs:** Score, time remaining, and possession, all in real time.
- **Your latency:** Seeing "Team A scored" and updating repeats what the market did 200ms ago.
- **Not prediction, echo:** Modeling the final outcome mid-game re-derives priced-in news.
- **What real edges need:** Speed plus proprietary video and tracking feeds retail lacks.

### Visualization (canvas `canvas6`, 500×300)

Volatile live-spread line over game time with touchdown markers, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Live Spread Movement During NFL Game".
- **Axes:** x "Game Time" with quarter labels "Q1", "Q2", "Q3", "Q4" at 10%, 35%, 60%, 85%; y "Live Spread", scale ±14 around center.
- **Spread series** (blue `#2980b9` line, width 2, 30 points): [−3, −3, −3.5, −3.5, −2.5, 4, 4.5, 4, 3.5, −3, −3.5, −4, 3.5, 3, 2.5, −4.5, −5, −5.5, −6, −7, −7, −7.5, −8, −10, −10.5, −10, −9.5, −10, −11, −10.5].
- **TD markers** (red `#e74c3c` dots radius 4, 9px labels): index 5 "TD Away", index 9 "TD Home", index 12 "TD Away", index 15 "TD Home", index 23 "TD Home".
- **Opening line reference:** dashed orange (`#e67e22`, dash 4/4, width 1) horizontal line at −3, labeled 11px orange "Opening: -3" at right.

## Correlated Parlays

**Books Multiply Parlay Legs as Independent When They Are Not**

- **The correlated pair:** "Covers the spread" and "goes over the total" are not independent.
- **The mechanism:** A blowout both covers the spread and pushes the total higher.
- **The mispricing:** Books multiply leg odds naively, as if the legs were independent.
- **Where the edge is:** Outcomes co-occurring more than independent pricing implies.
- **Why it stays hard:** Proving the correlation needs samples hard to accumulate in practice.

### Visualization (canvas `canvas7`, 500×300)

2×2 contingency grid of independent vs. actual joint probabilities, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Parlay Leg Correlation: Cover + Over".
- **Grid:** 2×2 cells (~60% of chart width, 70% of height); column headers bold 13px `#1a5276` "Covers" / "Misses"; row headers "Over" / "Under". Each cell: fill = cell color at ~13% alpha (hex + "22"), 2px border in cell color; contents: 11px `#888` "Independent: X%", bold 14px cell-color "Actual: Y%":
  - Over/Covers: Independent 25%, Actual 32% — `#27ae60`
  - Over/Misses: Independent 25%, Actual 20% — `#f39c12`
  - Under/Covers: Independent 25%, Actual 20% — `#f39c12`
  - Under/Misses: Independent 25%, Actual 28% — `#e74c3c`
- **Right-side annotation:** bold 12px `#c0392b` "Correlation = +0.14", then 11px `#555` lines: "Cover + Over happen" / "together MORE than" / "independent pricing" / "suggests. This is the" / "parlay edge."

## Small Sample Sizes

**A 60% ATS Record Over 30 Games Has a 95% CI of [41%, 77%]**

- **The season limit:** NFL teams play only 16-17 games, so splits accumulate slowly.
- **What "8-2 at home" is:** n=10 spread across five seasons of different rosters and coaches.
- **The interval:** 60% ATS over 30 games gives a 95% CI of [41%, 77%] — zero skill fits.
- **What drives the splits:** Random variance explains almost all of these small-sample records.
- **Narrative as signal:** "Revenge games" and "off a bye" are stories, not statistical findings.

### Visualization (canvas `canvas8`, 500×300)

Horizontal 95% confidence-interval bars for ATS records at increasing sample sizes, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Confidence Interval: 8-2 ATS Record (n=10)".
- **Reference line:** dashed red vertical line (`#e74c3c`, width 2, dash 5/5) at 50%, labeled 11px red "50% (no skill)" below the axis.
- **CI bars** (30px tall, 20px gaps; Wald 95% CI = p̂ ± 1.96·√(p̂(1−p̂)/n) clipped to [0,1]; fill = red `#e74c3c` at ~27% alpha with red border if the lower bound is below 50%, else green `#27ae60` equivalents; blue `#1a5276` point-estimate dot radius 5 at p̂; 12px `#333` label above each bar; 10px `#888` percentage labels at each CI endpoint):
  - "8-2 (n=10)" — p̂=0.80, CI ≈ [55%, 100%]
  - "18-12 (n=30)" — p̂=0.60, CI ≈ [42%, 78%]
  - "56-44 (n=100)" — p̂=0.56, CI ≈ [46%, 66%]
  - "270-230 (n=500)" — p̂=0.54, CI ≈ [50%, 58%]
- **Bottom annotation (bold 12px `#c0392b`):** "Red bars: CI includes 50% = NOT significant".

## Regeneration instructions

- **Layout:** domains detail-page variant — h1, `.subtitle`, then one `<h2>` per pitfall (no id attributes) followed by a single-row `.obj-table`: left `<td>` (40%) with an `.obj-title` div holding a one-line punchline (not a repeat of the h2) plus a `<ul>` of 4-5 `<li>` labeled bullets (`<strong>Label:</strong> short phrase`, each fitting one line); right `<td>` (60%, centered) holding one canvas. No philosophy callout on this page, no thead, no nav, no badges.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; `.subtitle` `#666` 1.05em; `.obj-table` full width, collapsed borders, cells `1px solid #e0e0e0` with 20px 24px padding, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `ul { margin: 8px 0 8px 20px; font-size: 0.9em; color: #333; }` and `li { margin: 4px 0; }` for the bullet lists; unused `.philosophy` rule (background `#f0f4f8`, left border 4px `#2980b9`) present in CSS.
- **Canvases:** each declares intrinsic `width="500" height="300"` and is filled with a `#f0f4f8` panel background; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) at 500×300, and calls `ctx.scale` so drawing stays in logical coordinates, and sets a default 17px system-sans font (chart text uses 9-15px sizes).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`/`#3498db`/`#2471a3`, green `#27ae60`, red `#e74c3c`/`#c0392b`, orange `#e67e22`/`#f39c12`, gray `#95a5a6`/`#888`/`#555`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
