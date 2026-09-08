# Crypto Pitfalls

**Page type:** detail page (obj-table layout: one h2 + one-row table per pitfall, text left 50%, canvas right 50%)
**HTML title tag:** Crypto Pitfalls - Domain-Specific Statistical Issues

**Subtitle:** Data integrity and structural issues unique to cryptocurrency markets

## Wash Trading Inflating Volume

**Up to 95% of Reported Volume Never Actually Traded**

- **The mechanism:** Exchanges trade against themselves to look liquid and attract real traders.
- **The scale:** Independent estimates say 50-95% of reported volume is fabricated.
- **Indistinguishable:** Real liquidity and manufactured liquidity look identical in the feed.
- **What a model learns:** "High volume = significant move" fitted on invented numbers.
- **What to trust:** Only regulated exchanges report usable volume — and even those imperfectly.
- **In the chart:** Worst venue is 95% fake; the best regulated one is still 10% fake.
- **Aggregate effect:** 28 units reported against 8.4 real is 70% fake market-wide.

### Visualization (canvas `canvas1`, 500×300)

Grouped bar chart: reported vs estimated-real volume per exchange type, with "% fake" labels. All values are fixed constants — no generated data.

- **Background:** full-canvas fill `#f0f4f8`.
- **Title (bold 15px, `#1a5276`):** "Reported vs Real Volume by Exchange Type" at (90, 25).
- **Margins:** top 50, right 30, bottom 60, left 50; only the x-axis baseline is drawn (`#333`, 1px).
- **Data (scale max 12):** Unregulated Exchange A — reported 10, real 0.5; Unregulated Exchange B — reported 8, real 1.2; Semi-regulated Exchange C — reported 5, real 2.5; Regulated Exchange D — reported 3, real 2.4; US Regulated Exchange E — reported 2, real 1.8.
- **Bars:** per exchange, reported bar filled `#e74c3c88` with `#e74c3c` 1px stroke; real bar (2px to the right) solid `#27ae60`. Bar width = plot width / (5 exchanges × 3).
- **"% fake" labels:** bold 10px `#c0392b` above each reported bar, computed at render time as round((1 − real/reported)×100)% + " fake" (95%, 85%, 50%, 20%, 10%).
- **Aggregate label (bold 10px `#c0392b`, under the legend):** sums computed from the plotted bars — "Aggregate: 28 reported vs 8.4 real = 70% fake". Reported sum 10+8+5+3+2 = 28; real sum 0.5+1.2+2.5+2.4+1.8 = 8.4; 1 − 8.4/28 = 0.70.
- **Exchange names:** two-line 9px `#555` labels under each group.
- **Legend (top right):** `#e74c3c88` swatch "Reported"; `#27ae60` swatch "Estimated real" (11px `#333`).
- **Footnote (9px `#888`, top left):** "Illustrative Example".

## DEX vs CEX Data Incompatibility

**Two Columns Named "Price" That Measure Different Objects**

- **CEX price:** The last matched trade in a limit order book — an observed event.
- **DEX price:** The output of a pool formula (x*y=k) — a computed slope, not a trade.
- **Slippage differs:** DEX slippage comes from pool depth, CEX slippage from order book shape.
- **Why joining fails:** Different objects, so you cannot naively combine the two series.
- **Why the gap persists:** Arbitrage links them, but latency and gas costs leave a standing spread.

### Visualization (canvas `canvas2`, 500×300)

Side-by-side comparison diagram: CEX order book (left) vs DEX AMM curve (right).

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Price Discovery: CEX Order Book vs DEX AMM" at (90, 25). Margins: top 50, right 20, bottom 40, left 40.
- **Left panel (40% of plot width), heading bold 12px `#1a5276`:** "CEX: Order Book".
  - **Bids (green, drawn leftward from center, fill `#27ae6044`, price labels 9px `#27ae60`):** price/qty pairs 99.5/5, 99.0/8, 98.5/12, 98.0/15, 97.5/20.
  - **Asks (red, drawn rightward from center, fill `#e74c3c44`, price labels 9px `#e74c3c`):** 100.5/4, 101.0/7, 101.5/10, 102.0/14, 102.5/18.
  - Rows are 18px tall; qty scale max 20. Bold 10px `#333` label between books: "Spread: $1.00".
- **Right panel (40% of plot width, starting at 55%), heading bold 12px `#1a5276`:** "DEX: AMM Curve (x*y=k)".
  - Hyperbola y = 1/x plotted for x in [0.3, 3], stroke `#9b59b6` at 2.5px.
  - Current price point: filled `#9b59b6` dot (radius 5) at x=1 on the curve, labeled 10px "Price = slope".
- **Center annotation (`#c0392b`):** bold 12px "≠" plus 10px two-line text "Cannot" / "combine" between the panels.
- **Footnote (9px `#888`, below the plot):** "Illustrative Example".

## MEV (Maximal Extractable Value)

**Your Fill Price Is Set by an Adversary, Not by the Market**

- **The power:** Validators and miners reorder transactions inside a block for their own profit.
- **The sandwich:** A bot buys before your trade and sells after, taking value from your fill.
- **Not random:** Your placement in the block is adversarial, not a coin flip.
- **What it depends on:** Who else is in the block and how the validator ordered them.
- **Why models miss it:** Invisible in historical prices, fully real in live execution.
- **In the chart:** Your fill is $2004 against a $2000 pre-block price — $4/TKN worse.
- **Your cost:** $4 × 10 TKN = $40, a 0.20% execution penalty you never see in backtests.

### Visualization (canvas `canvas3`, 500×300)

Block diagram of a sandwich attack plus a price timeline below.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "MEV Sandwich Attack: Transaction Ordering" at (100, 25). Margins: top 50, right 30, bottom 40, left 40.
- **Block box:** `#333` 2px border rectangle occupying ~50% of plot height, labeled bold 12px `#333` above: "Block #17,284,591".
- **Transaction rows inside block (each a rounded-fill rect, fill = row color + "22" alpha, stroke = row color 1px, left label bold 12px in row color, right description 11px):**
  1. "MEV Bot: BUY 100 TKN" — `#e74c3c` — "(front-run)"
  2. "YOUR TX: BUY 10 TKN" — `#3498db` — "(victim: worse price)"
  3. "MEV Bot: SELL 100 TKN" — `#e74c3c` — "(back-run: profit)"
  4. "Other transactions..." — `#95a5a6` — (no description)
- **Price timeline (horizontal `#555` line 30px below block):** dots (`#333`, radius 4) at fractional positions with bold 11px price above and 9px label below: 0.1 → "$2000" / "Before"; 0.35 → "$2003" / "After front-run"; 0.6 → "$2004" / "Your fill (worse!)"; 0.85 → "$2001" / "After back-run".
- **Bottom annotations:** bold 11px `#c0392b` "You overpay $4/TKN x 10 TKN = $40. Bot buys avg $2001.50, sells avg $2002.50." then 10px `#555` "Bot gross = $1.00/TKN x 100 TKN = $100 (the rest comes from other txs in the block)." Arithmetic: victim pays 2004 vs a 2000 pre-block price, so 4 × 10 = $40; the bot's 100 TKN round trip at a $1.00/TKN average improvement is $100 gross before gas and priority fees.
- **Footnote (9px `#888`, top right of the block):** "Illustrative Example".

## Rug Pulls / Scam Tokens in Training Data

**~90% of New Tokens Are Fraudulent or Dead, So the Token Universe Is Poisoned**

- **The composition:** Rug pulls, honeypots, and pump-and-dumps are 80% of new tokens.
- **Plus the dead ones:** Another 10% are simply abandoned, taking the unusable share to 90%.
- **What "all tokens" means:** Training on the full universe means training mostly on fraud.
- **Misread pattern:** 1000x in a week then zero is coordinated fraud, not momentum.
- **Why filtering fails:** Excluding scams needs manual labeling that does not scale.
- **The choice:** Keep them and learn fraud patterns instead of legitimate market behavior.

### Visualization (canvas `canvas4`, 500×300)

Pie chart of new-token outcomes with legend, derived counts, and annotation. All values are fixed constants — no generated data.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "New Token Outcomes (Typical Month)" at (130, 25). Margins: top 50, right 30, bottom 40, left 50.
- **Cohort:** 1,000 tokens/day × 30 days = 30,000 tokens in the month. Every legend count is computed as round(30000 × pct/100), so the counts and the percentages reconcile by construction.
- **Pie (center at 35% plot width, 50% plot height; radius = 0.32 × min(plot w,h); slices start at −90°, white 2px separators):**
  - Rug pulls — 45% — 13,500 — `#e74c3c` — fraud
  - Honeypots — 20% — 6,000 — `#c0392b` — fraud
  - Pump & dump — 15% — 4,500 — `#e67e22` — fraud
  - Abandoned — 10% — 3,000 — `#f39c12` — counted as unusable
  - Legitimate — 10% — 3,000 — `#27ae60`
- **Legend (right side, 12px swatches, 11px `#333` text):** "<label> <pct>% = <count>" for each slice, count formatted with thousands separators.
- **Totals line (10px `#333`):** "Slices total 100%" — the percentage sum computed from the slice table (45+20+15+10+10 = 100).
- **Annotation (bold 11px `#c0392b`, two lines):** "27,000 of 30,000 = 90%" / "fraudulent or dead" — the numerator is the sum of the four unusable slices (13,500+6,000+4,500+3,000 = 27,000) computed in JS, not asserted.
- **Center label (bold white over the pie, three lines):** "~1000" / "tokens/day" / "(30,000/mo)".
- **Footnote (9px `#888`, top left):** "Illustrative Example".

## 24/7 Markets With No Official "Close"

**"Daily Return" Is an Artifact of the Cutoff You Chose**

- **No official close:** Crypto never stops trading, so no timestamp is canonically end-of-day.
- **Providers disagree:** Midnight UTC, 4 PM EST, or arbitrary exchange-specific cutoffs.
- **Consequence:** Different "daily" statistics for the same asset from different sources.
- **Concrete mismatch:** Data Provider A's daily candle will not match Exchange B's daily candle.
- **Spreads downstream:** Moving averages and daily volatility shift with the cutoff — rarely acknowledged.
- **In the chart:** Four cutoffs over one seeded 48h path give −0.31%, +1.20%, −0.47%, −0.95%.
- **The spread:** A 2.15-point gap in "the daily return" for the same asset, same data.

### Visualization (canvas `canvas5`, 500×300)

48-hour price line with four dashed vertical cutoff markers, each ending a trailing 24-hour window, plus a computed table of the four resulting "daily returns".

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** 'Four Cutoffs, Four "Daily Returns", One Path' at (100, 25). Margins: top 50, right 30, bottom 50, left 60. L-shaped axes `#333` 1px.
- **Price series (seeded, deterministic):** generator `rnd = lcg(20250341)` using the shared Park-Miller LCG (`s = (s * 16807) % 2147483647`, return `s / 2147483647`). 49 hourly points; `p` starts at 30000 and each step does `p += Math.sin(i * 0.8) * 200 + (rnd() - 0.5) * 300`. Y-scale from min−100 to max+100 (29442 to 30562 for this seed). Line `#2980b9`, 2px. Never `Math.random()`.
- **Cutoff markers (dashed [3,3] vertical lines, 1.5px, dot radius 5 on the price line, 10px labels below axis and "$<price>" above dot):** each cutoff hour h closes a window that opened at hour h−24.
  - hour 24 — "UTC 00:00" — `#e74c3c` — price $29,904
  - hour 32 — "UTC 08:00" — `#f39c12` — price $30,029
  - hour 40 — "UTC 16:00" — `#27ae60` — price $29,784
  - hour 45 — "EST 16:00" — `#9b59b6` — price $29,907
- **Returns table (top left, 10px, one row per cutoff in the cutoff's color):** each row prints `(prices[h] − prices[h−24]) / prices[h−24] × 100` computed in JS from the plotted points. For seed 20250341: UTC 00:00 −0.31%, UTC 08:00 +1.20%, UTC 16:00 −0.47%, EST 16:00 −0.95%.
- **Spread line (bold 10px `#c0392b`):** max return − min return computed from the four values above = 2.15 pts (1.20 − (−0.95)).
- **X-axis label (11px `#555`):** "Hour of a 48h window". **Footnote (9px `#888`, top right):** "Illustrative Example".

## Oracle Manipulation

**Some Historical Prices Were Faked on Purpose for One Block**

- **What depends on oracles:** Collateral values, liquidation thresholds, and swap rates in DeFi.
- **The attack:** A flash loan moves the oracle price, exploits the protocol, and repays atomically.
- **Data contamination:** Historical prices may include quotes manipulated for exactly that purpose.
- **The fingerprint:** A spike followed by an instant reversion, not a gradual move.
- **How to read it:** These are protocol-design exploits, not market signals to learn from.
- **In the chart:** The other 49 blocks average $999; the attack block prints $3,000.
- **The multiple:** 3.00× the surrounding mean, held for one block then gone.

### Visualization (canvas `canvas6`, 500×300)

Price-by-block line with a single one-block flash-loan spike.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Flash Loan Oracle Attack: Price Spike" at (120, 25). Margins: top 45, right 30, bottom 50, left 60. L-shaped axes `#333` 1px.
- **Data (seeded, deterministic):** generator `rnd = lcg(20250342)`. 50 blocks; each block is `1000 + (rnd() - 0.5) * 20` except block 25, which is `1000 * 3 = 3000` (the spike). Y-scale max 3200. Line `#2980b9`, 2px. Never `Math.random()`.
- **Computed baseline:** the mean of the 49 non-spike blocks, computed in JS from the plotted array — $999 for this seed (999.47 unrounded, range 990.6 to 1009.8). The spike multiple is `3000 / baseline` = 3.00×.
- **Spike marker:** `#e74c3c` filled dot (radius 6) at the spike; annotations to its right — bold 12px `#c0392b` "Flash loan attack", then 11px "Oracle reads $3000", "Mean of other blocks: $999", "3.00× for 1 block (~12s)". The dollar figure and the multiple are both printed from the computed values, not hardcoded.
- **Attack sequence (10px `#e74c3c`, one row near the bottom of the plot):** "1. Borrow $100M", "2. Manipulate oracle", "3. Exploit protocol", "4. Repay loan + profit".
- **X-axis label (12px `#555`):** "Block number". **Footnote (9px `#888`, top right):** "Illustrative Example".

## Airdrop/Farming Distortion

**Usage Metrics Are ~90% Farming Bots Chasing Free Tokens**

- **The incentive:** Protocols hand out free tokens, so users manufacture qualifying activity.
- **The method:** Hundreds of wallets, fake transactions, and artificial volume per person.
- **What inflates:** TVL, transaction count, unique addresses, and daily active users alike.
- **The cliff:** When the airdrop ends, the "users" disappear overnight.
- **What you actually measure:** The incentive program schedule, not genuine adoption.
- **In the chart:** Farming weeks average 897/wk against 110/wk normal — an 8.1× lift.
- **The drop:** Post-airdrop weeks average 93/wk, so 89.6% of the activity vanishes.

### Visualization (canvas `canvas7`, 500×300)

Activity time series showing normal usage, a farming spike, the airdrop, and a post-airdrop cliff.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Protocol Activity: Before/During/After Airdrop" at (80, 25). Margins: top 50, right 30, bottom 55, left 60. L-shaped axes `#333` 1px.
- **Data (30 weeks, seeded and deterministic):** generator `rnd = lcg(20250343)`. Weeks 0–7 normal `100 + rnd() * 20`; weeks 8–9 rumor phase `200 + week * 50`; weeks 10–19 farming `800 + rnd() * 200`; week 20 airdrop = 900 exactly; weeks 21–29 cliff `80 + rnd() * 30`. Y-scale = 1.1 × max. Line `#9b59b6`, 2.5px. Never `Math.random()`.
- **Phase means computed in JS from the plotted array (seed 20250343):** normal (weeks 0–7) = 110/wk; farming (weeks 10–19) = 897/wk; post-airdrop (weeks 21–29) = 93/wk.
- **Farming-phase background band:** weeks 10–20 shaded `#e74c3c15`.
- **Annotations (all figures printed from the computed means):** 11px `#555` "Normal: 110/wk" (bottom left of plot); 11px `#e74c3c` "Farming: 897/wk" / "(8.1× normal)" at top of shaded band, the multiple = farming mean / normal mean.
- **Airdrop marker:** dashed [3,3] vertical `#27ae60` 2px line at week 20, labeled bold 11px `#27ae60` "Airdrop".
- **Cliff annotation (bold 11px `#c0392b` plus a 10px detail line):** "89.6% of activity vanishes" / "897/wk -> 93/wk", where the percentage = (1 − post mean / farming mean) × 100 computed at render time. The claim is about activity volume, not a headcount of wallets — the series plots daily active addresses per week, so no wallet-count total is asserted.
- **Axis labels (12px `#555`):** x "Weeks"; y (rotated) "Daily Active Addresses". **Footnote (9px `#888`, top right):** "Illustrative Example".

## Regulatory Event Risk

**One Announcement Moves the Whole Market 20%+ in Hours**

- **The triggers:** A regulator announcement, a country ban, or charges filed against an exchange.
- **No predictor:** No feature in the model anticipates regulatory action at all.
- **The shape:** Binary, unpredictable events whose impact hits the entire market at once.
- **Why fitting fails:** A model trained on normal conditions cannot handle regime breaks.
- **Not a tail risk:** These are structural breaks, not draws from a normal distribution.
- **In the chart:** Calm days have 0.89% daily sd, so a −20% day is a 22 sd event.
- **What that implies:** Under a normal fit, 22 sd is impossible — yet it happened three times.

### Visualization (canvas `canvas8`, 500×300)

Token A price path with three regulatory shock markers and realized statistics computed from the plotted path.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Token A Price: Regulatory Announcements" at (110, 25). Margins: top 45, right 30, bottom 50, left 60. L-shaped axes `#333` 1px.
- **Data (120 days, seeded and deterministic):** generator `rnd = lcg(20250344)`. Path starts at 40000; each day `ret = 0.002 + (rnd() - 0.5) * 0.03`, overridden on shock days: day 25 → −0.20 ("Country ban"), day 55 → −0.15 ("Regulator suit"), day 90 → +0.25 ("ETF approval"); `price[i] = price[i-1] * (1 + ret)`. Y-scale min×0.95 to max×1.05. Line `#f39c12`, 2px. Never `Math.random()`.
- **Realized (not design) statistics, computed in JS over the 116 non-shock days:** mean calm return 0.319%/day, sample sd 0.890%/day. The design parameters are mean 0.2% and a ±1.5% uniform band (sd 0.866%); the realized path differs, so the chart prints the realized figures. Path low $32,046, high $48,957, final $48,957.
- **Shock markers:** filled dots (radius 6) — `#e74c3c` for drops, `#27ae60` for the gain — with bold 10px label above and 10px "<±pct>% in hours" below, formatted with `toFixed(0)` so the strings read "-20% in hours", "-15% in hours", "+25% in hours".
- **Statistics annotations:** bold 10px `#c0392b` "Calm-day sd: 0.89%/day -> worst shock = 22 sd" (22.5 = 0.20 / 0.00890, printed with `toFixed(0)`); 10px `#555` "Realized max drawdown: 27.8%", computed as the largest peak-to-trough decline over the plotted path.
- **Bottom annotation (bold 11px `#c0392b`):** "No feature predicts these. Binary. Unpredictable. Market-wide."
- **X-axis label (12px `#555`):** "Days". **Footnote (9px `#888`, top right):** "Illustrative Example".

## Regeneration instructions

- **Layout:** standard domains detail page. h1, `.subtitle` paragraph, then per pitfall an `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border, padding-bottom 8px) followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (the punchline line) + a `<ul>` of labeled `<li>` bullets (`<strong>Label:</strong> phrase`), right `<td>` (60%, centered) holds the canvas. Even table rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `ul` margin `8px 0 8px 20px`, 0.9em `#333`; `li` margin `4px 0`; `strong` `#1a5276`; `.philosophy` callout style available (background `#f0f4f8`, left border 4px `#2980b9`) but unused. No nav bar, no back/home links.
- **Canvases:** each declared `<canvas id="canvasN" width="500" height="300">`; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) at 500×300px, and calls `ctx.scale` so drawing stays in logical coordinates, and sets base font 17px system sans-serif. Each chart is an IIFE painting on a `#f0f4f8` background.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark red `#c0392b`, secondary blue `#2980b9`/`#3498db`, purple `#9b59b6`, amber `#f39c12`, gray `#555`/`#333`.
- **Determinism rule (mandatory):** No chart may call `Math.random()`. The script defines one shared seeded Park-Miller LCG immediately after `setupCanvas`, verbatim:

  ```js
  // Seeded Park-Miller LCG — deterministic, never Math.random()
  function lcg(seed) {
      var s = seed;
      return function () { s = (s * 16807) % 2147483647; return s / 2147483647; };
  }
  ```

  Charts 5, 6, 7, and 8 each take their own generator with a distinct fixed seed: `lcg(20250341)` (chart 5 price walk), `lcg(20250342)` (chart 6 block prices), `lcg(20250343)` (chart 7 activity phases), `lcg(20250344)` (chart 8 return series). Charts 1–4 use fixed constants and need no generator. Changing a seed changes every figure quoted in this spec and in the page prose, so re-derive them if a seed is ever changed.
- **Computed-label rule (mandatory):** every statistic drawn next to generated data — percentages, means, multiples, standard deviations, drawdowns, spike multiples, aggregate sums — is computed in JS from the plotted array at render time and printed from that value. No statistic beside generated data is hardcoded. Design parameters (e.g. "returns are 0.2% ± 1.5%") are never printed as if they were realized statistics.
- **Illustrative labelling:** every chart carries a 9px `#888` "Illustrative Example" footnote drawn by a shared `illustrative(ctx, x, y)` helper. No real token, exchange, project, or regulator is named anywhere on the page — venues are "Exchange A"–"Exchange E", the asset is "Token A", the data source is "Data Provider A", and shock labels are "Country ban" / "Regulator suit" / "ETF approval".
