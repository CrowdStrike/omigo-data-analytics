# Hedge Fund Pitfalls

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Hedge Fund Pitfalls - Domain-Specific Statistical Issues

**Subtitle:** Quantitative strategy traps that destroy alpha in production

## Alpha Decay Once Discovered

**Alpha Half-Life Runs 6 Months for Momentum, 48 for Structural Arb**

- **The mechanism:** A fund trades the signal, participants adjust, and the edge disappears.
- **Decay speed:** Months for simple momentum, years for complex structural arbitrage.
- **Backtest lie:** It earns historical alpha that no longer exists once anyone trades it.
- **Discovery date:** Every published anomaly's returns decline sharply after that date.
- **Papers as obituaries:** An academic paper announcing an edge is the edge's obituary.

### Visualization (canvas `canvas1`, 500×300)

Three exponential alpha-decay curves for strategies of different complexity, on a light `#f0f4f8` panel background.

- **Title (bold 15px `#1a5276`):** "Alpha Decay After Signal Discovery".
- **Axes:** margins top 45 / right 30 / bottom 50 / left 60; L-shaped `#333` axes. X "Months After Discovery", 0–60 with labels every 12; y "Alpha Remaining (%)", 0–100.
- **Curves** (alpha = 100·e^(−0.693·m/halfLife), width 2.5):
  - "Simple momentum (t½=6mo)" — `#e74c3c`
  - "Statistical arb (t½=18mo)" — `#f39c12`
  - "Structural arb (t½=48mo)" — `#27ae60`
- **Discovery marker:** dashed gray vertical line (`#555`, dash 4/4) at month 0.
- **Legend:** top-right, 12×12 color swatches with 11px `#333` labels as above.

## Capacity Constraints

**Sharpe 3.2 at $10M AUM Falls to 0.2 at $5B — Same Strategy**

- **The mechanism:** Your own buy order moves the price up before you finish buying.
- **Capacity ceiling:** Sharpe falls monotonically with AUM; near $500M impact exceeds alpha.
- **Blind backtest:** It models no market impact, because historically you weren't trading.
- **Inverse pairing:** The highest-Sharpe strategies typically have the lowest capacity.
- **Misread cause:** Growing funds blame the strategy when they simply outgrew it.

### Visualization (canvas `canvas2`, 500×300)

Downward-sloping Sharpe-vs-AUM curve on a log x-scale, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Sharpe Ratio vs AUM (Strategy Capacity)".
- **Axes:** x "AUM ($M, log scale)" with tick labels "$10M", "$100M", "$1B", "$5B" positioned by log10(v)/log10(5000) over 90% of chart width; y "Sharpe Ratio" 0–3.5 with labels 0.0–3.0.
- **Curve** (blue `#2980b9` line, width 3), (AUM $M, Sharpe): (10, 3.2), (50, 2.8), (100, 2.4), (250, 1.9), (500, 1.4), (1000, 0.9), (2000, 0.5), (5000, 0.2).
- **Capacity ceiling:** dashed red vertical line (`#e74c3c`, width 2, dash 5/5) at $500M, labeled bold 11px red "Capacity ceiling" with 11px sub-line "(market impact > alpha)".

## Factor Crowding

**In Crisis, Crowded Factor Correlations Spike Toward 1.0**

- **How it crowds:** Everyone discovers "momentum," everyone trades it, it becomes one trade.
- **The unwind:** One fund liquidates under stress and the whole crowded position blows out.
- **Simultaneous losses:** Nobody escapes, because every fund held the identical position.
- **2007 quant crisis:** Market-neutral funds lost 20%+ in days as correlations spiked.
- **Illusory hedge:** Diversifying across crowded factors is not diversification at all.

### Visualization (canvas `canvas3`, 500×300)

Two side-by-side 4×4 correlation heatmaps (normal vs. crisis), on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Factor Correlations: Normal vs Crisis".
- **Matrix headers (bold 12px):** "Normal Period" in `#27ae60` (left), "Crisis Period" in `#e74c3c` (right); factor row/column labels (10px `#333`): "Mom", "Val", "Qual", "Size"; 35px cells.
- **Normal-period matrix values (rows Mom/Val/Qual/Size):**
  - [1.0, −0.2, 0.1, 0.05]
  - [−0.2, 1.0, 0.15, 0.1]
  - [0.1, 0.15, 1.0, −0.1]
  - [0.05, 0.1, −0.1, 1.0]
- **Crisis-period matrix values:**
  - [1.0, 0.85, 0.9, 0.8]
  - [0.85, 1.0, 0.88, 0.82]
  - [0.9, 0.88, 1.0, 0.87]
  - [0.8, 0.82, 0.87, 1.0]
- **Cell coloring:** green-tinted ramp for positive values (rgb(255−200v, 255−100v, 255−200v)), blue-tinted for negatives; value text 10px, white when v > 0.7 else `#333`, one decimal place.
- **Arrow:** black arrow (`#333`, width 2) between the two matrices.
- **Bottom annotation (bold 12px `#c0392b`):** "In crisis: all correlations → 1.0 (diversification vanishes)".

## Backtest Overfitting / Multiple Testing

**Test 1,000 Strategies and ~50 Clear Sharpe 2 by Chance Alone**

- **The arithmetic:** Try 1,000 backtests, and roughly 50 beat Sharpe 2 on luck.
- **Live outcome:** Of those 50 "discoveries," 48 were noise dressed as historical signal.
- **Moving bar:** After 1,000 tries you need Sharpe 3+, not 2, to survive the correction.
- **The maxim:** "The best backtest is the one you never trade."
- **What shops miss:** Few count the hypotheses they implicitly tested while iterating.

### Visualization (canvas `canvas4`, 500×300)

Scatter of backtest Sharpe vs. live Sharpe showing regression to the mean, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "1000 Random Strategies: Backtest vs Live Sharpe".
- **Axes:** x "Backtest Sharpe" spanning ~0.5–4.5; y "Live Sharpe" spanning ~−1.5 to +2.5.
- **Points:** 80 seeded pseudo-random dots (radius 4): backtest Sharpe uniform 1.5–4.0, live Sharpe ≈ 0.2×backtest plus noise ±0.75 (mostly near 0); semi-transparent green `#27ae6088` when live > 0.5, else semi-transparent red `#e74c3c88`.
- **Expected line:** dashed green 45-degree line (`#27ae60`, width 1.5, dash 5/5) from bottom-left to top-right, labeled 11px "Expected (no overfitting)".
- **Reality line:** nearly flat red line (`#e74c3c`, width 2.5) near live ≈ 0, labeled 11px red "Reality (massive regression)".

## Execution Slippage

**8% in Backtest Becomes 3% Live — a 5% Execution Gap**

- **The assumption:** Backtests fill at the observed close price, costlessly.
- **The reality:** Your order takes 30ms, moves the market, and fills only partially.
- **Adverse selection:** The counterparties willing to trade with you know something.
- **The circularity:** That observed price existed only because you did not trade.
- **Imperfect fix:** Market impact models exist but are imprecise for illiquid instruments.

### Visualization (canvas `canvas5`, 500×300)

Waterfall chart from backtest return down to live return, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Returns: Backtest vs After Execution Costs".
- **Waterfall bars** (six, bold 12px `#333` signed value label above each, 10px `#555` two-line name labels below the baseline):
  - "Backtest Return" +8% — green `#27ae60` (anchor bar)
  - "Market Impact" −2.5% — red `#e74c3c`
  - "Spread Costs" −1.2% — red `#e74c3c`
  - "Partial Fills" −0.8% — red `#e74c3c`
  - "Adverse Selection" −0.5% — red `#e74c3c`
  - "Live Return" +3% — blue `#2980b9` (anchor bar)
- Deduction bars step down cumulatively from the 8% level; scale ±10% around the mid-height zero line.
- **Top annotation (bold 13px `#c0392b`, centered):** "5% gap = reality check".

## Data Snooping from Strategy Iteration

**The "Regime Filter" Exists Only Because You Saw the 2015 Drawdown**

- **The sequence:** Spot a 2015 drawdown, add a filter, and the backtest looks great.
- **Why it fails:** The filter was chosen after seeing the loss — that is fitting to noise.
- **Out-of-sample:** It doesn't help, since future drawdowns won't resemble 2015's.
- **Compounding effect:** Each backtest-driven edit adds overfit; more iterations, more rot.
- **What the curve hides:** The final equity curve is the product of dozens of such choices.

### Visualization (canvas `canvas6`, 500×300)

Two equity curves over 96 months (8 years): honest original vs. overfit iteration, with an in-sample/out-of-sample divider, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Strategy Iteration: Overfitting to History".
- **Series** (both start at 100, monthly compounding, drawn as lines width 2, y scaled to 1.1× the max equity):
  - V1 (blue `#3498db`): +0.5%/month baseline, except months 36–42 at −3%/month (the 2015 drawdown).
  - V5 (red `#e74c3c`): +0.5%/month baseline, months 36–42 "fixed" to +0.2%/month, but months 72–80 at −4%/month (fails in the new regime).
- **Divider:** dashed gray vertical line (`#555`, dash 4/4) at month 60, with 11px `#555` labels "In-Sample" (left) and "Out-of-Sample" (right).
- **Legend** (top-right, 12×12 swatches, 11px `#333`): blue "V1: Original (honest)"; red "V5: After \"fixing\" (overfit)".

## Regime Detection

**You Cannot Detect a Regime Change Until the Loss Has Happened**

- **The case:** Value investing worked from 1945 to 2020, then failed for a decade.
- **The detection lag:** Confirming a new regime statistically requires the losses as data.
- **Post-hoc label:** "Regime" is a narrative applied after the model stopped working.
- **No forecast:** There is no reliable way to predict a regime change in advance.
- **Unfalsifiable:** Any failure can be relabeled a "regime change," so it explains nothing.

### Visualization (canvas `canvas7`, 500×300)

Bar chart of value-factor returns by decade with a regime-change marker, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Value Factor Returns by Decade".
- **Bars** (around a zero baseline at 60% of chart height, scale ±6%; 10px decade labels below, bold 11px signed value labels at bar ends):
  - 1950s +4.2% — green `#27ae60`
  - 1960s +3.8% — green
  - 1970s +5.1% — green
  - 1980s +3.5% — green
  - 1990s +2.8% — green
  - 2000s +3.2% — green
  - 2010s −1.5% — red `#e74c3c`
  - 2020s −0.8% — red
- **Regime marker:** dashed red vertical line (`#e74c3c`, width 2, dash 3/3) before the 2010s bar; bold 12px red label "\"Regime change\"" with 11px sub-line "(detected only after losses)".

## Leverage Amplifies Noise

**Sharpe 0.5 at 1x Is Still Sharpe 0.5 at 3x — Only Ruin Scales**

- **The upside:** 2x leverage turns an 8% return into 16%, which is the selling point.
- **The symmetry:** The same 2x turns a 4% drawdown into 8% — noise scales with signal.
- **SNR unchanged:** Leverage moves magnitude only; the ratio of signal to noise is fixed.
- **Ruin threshold:** At 3x a 2-sigma event wipes you out instead of costing a bad quarter.
- **Blowup math:** Given the true return distribution, many blowups were arithmetically due.

### Visualization (canvas `canvas8`, 500×300)

Grouped signal-vs-noise bar chart at four leverage levels, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Signal vs Noise at Different Leverage Levels".
- **Groups** (x labels bold 12px: "1x", "2x", "3x", "5x"; scale max 35): signal bars (green `#27ae60`) values [4, 8, 12, 20]; noise bars (semi-transparent red `#e74c3c88`) values [6, 12, 18, 30]; 10px `#555` "SNR: 0.67" label under each group.
- **Legend** (top-right, 12×12 swatches, 11px `#333`): green "Signal", translucent red "Noise".
- **Bottom annotation (bold 12px `#c0392b`):** "SNR stays constant = 0.67 at ALL leverage levels".

## Regeneration instructions

- **Layout:** domains detail-page variant — h1, `.subtitle`, then one `<h2>` per pitfall (no id attributes) followed by a single-row `.obj-table`: left `<td>` (40%) with an `.obj-title` div holding a one-line punchline (not a repeat of the h2) followed by a `<ul>` of 4-5 `<li>` labeled bullets, each `<strong>Label:</strong> short phrase` fitting one line; right `<td>` (60%, centered) holding one canvas. No philosophy callout on this page, no thead, no nav, no badges.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; `.subtitle` `#666` 1.05em; `ul { margin: 8px 0 8px 20px; font-size: 0.9em; color: #333; }` and `li { margin: 4px 0; }`; `strong` in `#1a5276`; `.obj-table` full width, collapsed borders, cells `1px solid #e0e0e0` with 20px 24px padding, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; unused `.philosophy` rule (background `#f0f4f8`, left border 4px `#2980b9`) present in CSS.
- **Canvases:** each declares intrinsic `width="500" height="300"` and is filled with a `#f0f4f8` panel background; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) at 500×300, and calls `ctx.scale` so drawing stays in logical coordinates, and sets a default 17px system-sans font (chart text uses 10-15px sizes).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`/`#c0392b`, orange `#f39c12`, gray `#555`/`#888`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
