# Stock Market Pitfalls

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Stock Market Pitfalls - Domain-Specific Statistical Issues

**Subtitle:** Data quality and structural issues that invalidate equity market analysis

## Survivorship Bias in Indices

**Backtests on Today's Constituents Overstate Returns by 1-2% a Year**

- **What changes:** The S&P 500 today is not the S&P 500 of 2000 — members are swapped continually.
- **The mechanism:** Failed companies are removed and successful ones added retroactively.
- **Who's missing:** Bankruptcies, distressed acquisitions, and delistings never enter the sample.
- **The size:** Historical index returns inflate by 1-2% annually once losers are excluded.
- **Why it matters:** The upward bias is systematic, so those returns were never investable.

### Visualization (canvas `canvas1`, 500×300)

Two compound-growth curves over 30 years (survivors-only vs. actual), on a light `#f0f4f8` panel background.

- **Title (bold 15px `#1a5276`):** "S&P 500: With vs Without Survivorship Bias".
- **Axes:** margins top 45 / right 30 / bottom 50 / left 60; L-shaped `#333` axes; x label "Years".
- **Series** (both start at 100, compounded annually for 30 years, y normalized to the biased endpoint at 90% chart height, line width 2.5):
  - Biased (survivors only): 10.5%/yr — red `#e74c3c`.
  - Actual (including delisted): 8.8%/yr — green `#27ae60`.
- **Gap annotation:** translucent red block (`#e74c3c33`) between the two endpoints near the right edge; bold 12px `#c0392b` two-line label: "~40% gap" / "after 30 yrs".
- **Legend** (top-left, 12×12 swatches, 11px `#333`): red "Survivors only (10.5%/yr)"; green "Including delisted (8.8%/yr)".

## Earnings Restatements / Point-in-Time Data

**Your Database Says $1.50; the Market Only Knew $2.00 That Day**

- **The sequence:** A company reports $2.00 EPS, then later restates the same quarter to $1.50.
- **The mismatch:** Your database stores $1.50, but the decision date only had $2.00 available.
- **What it creates:** Look-ahead bias — the model "knew" the corrected number before it was public.
- **The fix, and its cost:** Point-in-time databases preserve what was known when, but are expensive and rare.
- **Default state:** Free sources ship final restated values, so history is quietly contaminated.

### Visualization (canvas `canvas2`, 500×300)

Horizontal timeline of four events with callout boxes above/below, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Point-in-Time vs Restated Data".
- **Timeline:** black arrow (`#333`, width 2) across the middle, labeled 12px `#555` "Time →" at the right end.
- **Events** (each: colored dot radius 6 on the timeline, dotted connector (dash 2/2) to a 90×35 label box filled with the event color at ~8% alpha and bordered 1px in the color, 11px two-line label text in the color):
  - At 10% width, box above: "Q1 Report:" / "EPS = $2.00" — `#2980b9`
  - At 35% width, box below: "Model uses" / "$2.00 (correct)" — `#27ae60`
  - At 60% width, box above: "Restatement:" / "EPS → $1.50" — `#e74c3c`
  - At 85% width, box below: "Database shows" / "$1.50 for Q1!" — `#e74c3c`
- **Warning (bold 12px `#c0392b`, bottom):** "LOOK-AHEAD BIAS: model \"knows\" restatement before it happens".

## Market Microstructure Noise

**Below One Second, "Price" Is a Noisy Estimate, Not a Signal**

- **Bid-ask bounce:** The printed price flips between bid and ask with no news behind it.
- **Discretization:** Prices round to tick sizes, adding jumps that carry no information.
- **Hidden state:** Undisplayed orders and exchange routing both move what you observe.
- **What OHLC hides:** A "flat" bar can conceal wild intra-bar swings entirely.
- **What to assume:** Treat observed prices as noisy estimates of true consensus value.

### Visualization (canvas `canvas3`, 500×300)

Tick-level price line bouncing between bid and ask reference lines, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Bid-Ask Bounce: What OHLC Bars Hide".
- **Data:** 60 simulated ticks around mid = $100 with spread $0.02: each tick has small random drift (±0.0025) and randomly lands at bid (mid − 0.01) or ask (mid + 0.01); jagged blue line `#2980b9`, width 1.5, y scaled to the tick range.
- **Reference lines** (dashed, dash 3/3, width 1): bid line green `#27ae60` labeled 11px "Bid: $99.99"; ask line red `#e74c3c` labeled "Ask: $100.01".
- **OHLC bar:** thin orange bar (`#f39c12`, 3px wide) at the right edge spanning most of the chart height, labeled 10px orange "OHLC" / "bar".
- **X caption (12px `#555`):** "1-second window (60 ticks)".
- **Annotation (bold 11px `#c0392b`, bottom-left inside plot):** "Bounce between bid/ask = noise, not signal".

## HFT Front-Running / Adverse Selection

**Adverse Selection Eats 30-50% of the Alpha Your Signal Showed**

- **When you're wrong:** Your resting limit order gets picked off by HFT firms instantly.
- **When you're right:** Faster traders cancel the opposing side, so your order never fills.
- **The result:** Actual fills are adversely selected — worse expected returns than the signal.
- **The size:** The signal-return to filled-return gap can run 30-50% of the alpha.
- **Why unseen:** Backtests priced at mid or close never show this at all.

### Visualization (canvas `canvas4`, 500×300)

Two overlaid Gaussian return distributions (theoretical signals vs. actual fills), on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Adverse Selection: Filled vs Unfilled Orders".
- **Axes:** x "Post-trade return (%)" spanning −5 to +5; dashed gray vertical zero line (`#aaa`, dash 3/3) labeled 11px `#888` "0% return"; only the horizontal baseline is drawn.
- **Curves** (unnormalized Gaussians peaking at 80% of chart height, width 2.5, bold 12px labels near the top at the mean):
  - "All signals (theoretical)": mean +1.0, std 1.2 — green `#27ae60`.
  - "Actually filled orders": mean −0.3, std 1.2 — red `#e74c3c`.
- **Gap arrow:** black leftward arrow (`#333`, width 1.5) from mean +1.0 to mean −0.3 at ~45% height, labeled bold 11px "Adverse selection gap".

## After-Hours / Pre-Market Gaps

**A Stop-Loss at $95 Never Triggers When the Open Gaps to $85**

- **The sequence:** Close $100, earnings at 4:30 PM, next open $85 — price skips the stop.
- **What breaks:** Any model assuming continuous prices — stops, trailing stops, intraday sizing.
- **How much is overnight:** Roughly 30-40% of total stock returns occur outside trading hours.
- **The data gap:** Models ignoring overnight gaps are fitting an incomplete return series.
- **Risk consequence:** Tail risk gets dramatically underestimated as a result.

### Visualization (canvas `canvas5`, 500×300)

Two-day price line with an overnight gap that skips a stop-loss level, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Overnight Gap: Stop-Loss Failure".
- **Axes:** y range $82–$103, tick labels "$85, $90, $95, $100" (10px `#888`); L-shaped `#333` axes.
- **Series** (blue `#2980b9`, width 2.5, 16 slots across the width):
  - Day 1 (slots 0-7): [100, 100.5, 101, 100.8, 100.2, 100.5, 100.3, 100].
  - Day 2 (slots 8-15): [85, 84, 85.5, 86, 85.5, 86.5, 87, 87.5].
- **Gap band:** translucent red rectangle (`#e74c3c22`) full chart height between slots 7 and 8; bold 12px `#c0392b` label at mid-height: "$15 GAP".
- **Stop-loss line:** dashed red horizontal line (`#e74c3c`, width 2, dash 5/5) at $95, labeled bold 12px "Stop-loss: $95" and 11px "NEVER TRIGGERED".
- **X annotations (11px `#555`):** "Day 1: Close $100" (left), "Overnight" (at the gap), "Day 2: Open $85" (right).

## Dividend Adjustments

**Every Ex-Dividend Date Looks Like a Crash in Unadjusted Prices**

- **The mechanic:** A stock paying a $2 dividend opens about $2 lower on the ex-date.
- **Not a signal:** The drop is a mechanical adjustment, not information about the company.
- **If unadjusted:** Skipping dividend and split adjustment makes each ex-date read as a crash.
- **Cumulative damage:** $2/quarter for 10 years shows a phantom $80 "decline".
- **What flips:** Trends, support levels, and returns all differ between adjusted and raw series.

### Visualization (canvas `canvas6`, 500×300)

Adjusted vs. unadjusted price paths over 20 quarters with ex-date tick markers, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Adjusted vs Unadjusted Price (Quarterly $2 Div)".
- **Series** (both start at 100, 2% growth per quarter for 20 quarters, width 2.5):
  - Adjusted: smooth compounding curve — green `#27ae60`.
  - Unadjusted: same growth but minus $2 each quarter (sawtooth decline) — red `#e74c3c`.
- Y scaled from 0.9× the unadjusted minimum to 1.1× the adjusted maximum; x label "Quarters".
- **Ex-date markers:** small red tick marks below the axis every 4 quarters, each labeled 9px "Ex".
- **Legend** (top-left, 12×12 swatches, 11px `#333`): green "Dividend-adjusted (true return)"; red "Unadjusted (looks like decline)".

## Index Reconstitution Effects

**Index Addition Buys 3-5% of Return That Is Pure Plumbing**

- **The forced flow:** On S&P 500 addition, every index fund must buy the name at once.
- **The move:** The stock jumps 3-5% in the days surrounding the addition.
- **Not fundamental:** It is a predictable short-term return from structural flow, not value.
- **The reversal:** The move often reverts over the weeks after the effective date.
- **The mirror case:** Deletions create equally predictable selling pressure the other way.
- **Why dangerous:** Phantom signals mimic momentum or value but are purely structural.

### Visualization (canvas `canvas7`, 500×300)

Event-study price line around an S&P 500 addition: run-up, peak, and slow reversion, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Stock Price Around S&P 500 Addition".
- **Axes:** x "Days relative to effective date" spanning −25 to +65; y range $98–$107.
- **Series** (blue `#2980b9`, width 2.5), (day, price): (−20, 100), (−15, 100.5), (−10, 101), (−5, 101), (−3, 103), (−2, 104), (−1, 104.5), (0, 105), (1, 104.5), (2, 104), (3, 103.5), (5, 103), (10, 102.5), (15, 102), (20, 101.5), (30, 101), (40, 100.5), (50, 100.2), (60, 100).
- **Markers:** dashed red vertical line (dash 3/3, width 1.5) at day −5 labeled 10px red "Announce"; dashed green vertical line at day 0 labeled green "Effective".
- **Annotations:** bold 12px `#c0392b` "+5% (index fund buying)" near the peak; 11px `#555` "Gradual reversion over 60 days" lower right.

## Corporate Actions

**Without a Corporate Actions Database, Long-Term Price Series Are Fiction**

- **What redefines "price":** Mergers, spinoffs, buybacks, splits, rights issues, special dividends.
- **Split example:** A 2:1 split halves the price overnight and is not a crash.
- **Spinoff example:** A new entity appears while the parent drops by the spinoff's value.
- **The requirement:** Every corporate action must be applied or the series is discontinuous.
- **Identity problems:** Tickers get reused; companies merge and change names over time.

### Visualization (canvas `canvas8`, 500×300)

Timeline of five corporate actions, each shown as before/after price dots joined by a short connector, on `#f0f4f8` panel.

- **Title (bold 15px `#1a5276`):** "Price Discontinuities from Corporate Actions".
- **Baseline:** horizontal `#333` axis at the bottom; price scale 0–$220.
- **Actions** (at fractional x positions; colored dots radius 5 for before/after prices with 10px "$N" labels, a colored connector line between them when both exist, bold 10px multi-line label below the axis in the action color):
  - x=0.1: $200 → $100 — "2:1 Split" — `#3498db`
  - x=0.3: $110 → $95 — "Spinoff" / "($15 value)" — `#9b59b6`
  - x=0.5: $102 → $99 — "Special" / "Dividend $3" — `#27ae60`
  - x=0.7: $105 → (delisted, no after-dot) — "Acquired" / "(delisted)" — `#e74c3c`
  - x=0.9: (no before-dot) → $45 — "New ticker" / "(same co.)" — `#f39c12`
- **Warning (bold 11px `#c0392b`, top):** "Each action breaks price continuity — raw data is UNUSABLE without adjustments".

## Regeneration instructions

- **Layout:** domains detail-page variant — h1, `.subtitle`, then one `<h2>` per pitfall (no id attributes) followed by a single-row `.obj-table`: left `<td>` (40%) with an `.obj-title` div holding a one-line punchline (not a repeat of the h2) plus a `<ul>` of 4-6 `<li>` labeled bullets (each `<strong>Label:</strong> short phrase.` fitting one line); right `<td>` (60%, centered) holding one canvas. No philosophy callout on this page, no thead, no nav, no badges.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; `.subtitle` `#666` 1.05em; `.obj-table` full width, collapsed borders, cells `1px solid #e0e0e0` with 20px 24px padding, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; bullet styling `ul { margin: 8px 0 8px 20px; font-size: 0.9em; color: #333; }` and `li { margin: 4px 0; }`; `strong` in `#1a5276`; unused `.philosophy` rule (background `#f0f4f8`, left border 4px `#2980b9`) present in CSS.
- **Canvases:** each declares intrinsic `width="500" height="300"` and is filled with a `#f0f4f8` panel background; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) at 500×300, and calls `ctx.scale` so drawing stays in logical coordinates, and sets a default 17px system-sans font (chart text uses 9-15px sizes).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`/`#c0392b`, orange `#f39c12`, purple `#9b59b6`, gray `#555`/`#888`/`#333`/`#aaa`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
