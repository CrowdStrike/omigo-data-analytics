# Prediction Markets Pitfalls

**Page type:** detail page (obj-table layout: one h2 + one-row table per pitfall, text left 50%, canvas right 50%)
**HTML title tag:** Prediction Markets Pitfalls - Domain-Specific Statistical Issues

**Subtitle:** Why prediction market prices are noisier and less informative than they appear

**Determinism rule for this page:** every generated series uses the shared seeded Park-Miller LCG
`lcg(seed)` (`s = (s * 16807) % 2147483647; return s / 2147483647`). `Math.random()` must never appear
in a chart. Every number printed on a chart is computed from the plotted values at render time —
no hardcoded statistics. Seeds: chart 1 = `20253592`, chart 7 = `20250357`, chart 8 = `20250383`.
Charts 2–6 are fully deterministic (closed-form curves or fixed arrays), so they need no generator.

## Thin Liquidity = Noisy Prices

**A $200 Bet Moves a $500 Book from 60% to 72%**

- **The setup:** A market displays "60% probability" backed by only $500 in the order book.
- **The mechanism:** One $200 bet pushes the price to 72% with no new information involved.
- **What it really is:** Not a wisdom-of-crowds estimate — the opinion of whoever bet last.
- **The fix:** Liquidity-weighted confidence — 60% on $5M is meaningful, 60% on $500 is noise.
- **At scale:** Most books are thin, so prices invite manipulation and random fluctuation.

### Visualization (canvas `canvas1`, 500×300)

Three seeded price paths at different liquidity levels around a fixed true probability.

- **Background:** full-canvas fill `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Price Stability vs Market Liquidity" at (130, 25). Margins: top 50, right 30, bottom 55, left 60. L-shaped axes `#333` 1px.
- **Generator:** one `lcg(20253592)` shared by all three paths, drawn in the order thin → mid → deep.
- **Series formula:** 30 steps, `path[0] = 60`, `path[i] = clamp(path[i-1] + (rnd() - 0.5) * vol, 10, 90)`; y-scale 0–100.
  - `#e74c3c`, vol 24 — "$500 liquidity" — spans 44.4–78.2, **sd 8.4** (computed)
  - `#f39c12`, vol 6 — "$50K liquidity" — spans 56.0–64.1, **sd 2.2** (computed)
  - `#27ae60`, vol 2 — "$5M liquidity" — spans 57.8–61.8, **sd 1.0** (computed)
  - All lines 2px. No path touches the clamp bounds, so the walk is unbiased.
- **Biggest-jump annotation (computed at render):** scan the thin path for the largest single-step move, mark it with a 4px `#c0392b` dot and label bold 10px `#c0392b` "One $200 bet: 60% → 72%" using the rounded before/after values. With this seed the largest step is index 7→8, 60.04 → 72.01, i.e. **60% → 72%** — the headline figure is produced by the data, not asserted.
- **True probability reference:** dashed [5,5] horizontal `#555` 1.5px line at 60, labeled 11px `#555` "True prob: 60%".
- **Legend (top left):** 12px color swatches with 11px `#333` labels; each label appends the path's standard deviation computed from its own plotted points — "$500 liquidity (sd 8.4)", "$50K liquidity (sd 2.2)", "$5M liquidity (sd 1.0)".
- **Axis labels (12px `#555`):** x "Time (same event, different liquidity)"; y (rotated) "Market Price (%)".

## Binary vs Continuous Outcome Mismatch

**A 50% Price on "Inflation > 3%" Fits Both E[inf] = 3.0% and E[inf] = 2.5%**

- **The mismatch:** Markets resolve YES/NO, but the underlying quantity is usually continuous.
- **What the price means:** Probability of crossing a threshold, not the expected inflation level.
- **Information lost:** Binarization discards the entire shape of the belief distribution.
- **Two fits, one price:** A tight bell at 3.0% and a two-peak belief at 1% / 4% both price at 50%.
- **No reconstruction:** You cannot recover the full distribution without strong assumptions.
- **Net effect:** Resolution criteria compress continuous reality into a coin flip.

### Visualization (canvas `canvas2`, 500×300)

Two belief densities with a shared binary threshold, priced identically despite different shapes and different means.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Information Loss: Continuous → Binary" at (120, 25). Margins: top 50, right 30, bottom 55, left 60. Baseline x-axis only (`#333` 1px).
- **Threshold:** dashed [4,4] vertical `#e74c3c` 2px line at x=3 (of a 0–6% axis), labeled bold 12px `#e74c3c` "Threshold: 3%".
- **Densities (2px strokes, `nrm(x,m,s) = exp(−0.5·((x−m)/s)²)/(s·2.5)`):**
  - Scenario A (`#2980b9`): unimodal, `nrm(x, 3.0, 0.5)`.
  - Scenario B (`#9b59b6`): bimodal, `0.5·nrm(x, 1.0, 0.4) + 0.5·nrm(x, 4.0, 0.4)`.
- **Computed statistics (numerical integration over 0–6 at dx = 0.005, printed at render):** A gives P(>3%) = **50%**, E[inf] = **3.00%**; B gives P(>3%) = **50%**, E[inf] = **2.51%**. The earlier normal(2.5, 1.2) curve was replaced because it integrates to P(>3%) = 34%, which contradicted the "~50%" caption.
- **Bottom annotations (12px `#333`, two lines, numbers substituted from the integrals):** "Both distributions → same market price (50%)" / "But E[inf] = 3.00% vs 2.51% — very different beliefs".
- **Legend (top right, 11px `#333`, values computed):** `#2980b9` swatch "A: E=3.00%, P(>3%)=50%"; `#9b59b6` swatch "B: E=2.51%, P(>3%)=50%".
- **X-axis:** 11px `#888` tick labels "0%"–"6%" at each integer; 12px `#555` axis title "Inflation Rate".

## Market Manipulation by Small Capital

**$5K Moves a $50K Market by 10 Percentage Points**

- **The lever:** In a $50K total volume market, $5K shifts the price ten points.
- **The playbook:** Buy early, move the price, let followers read the move as a "signal," exit at profit.
- **The worse case:** Manipulate purely to mislead observers using the price as an information source.
- **Why it pays:** External gain, not trading profit, is the motive when prices drive decisions.
- **The consequence:** Any market feeding decision-making carries an enormous manipulation incentive.

### Visualization (canvas `canvas3`, 500×300)

Pump-and-revert price path with staged manipulation markers. Fixed array — no generator needed.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "$5K Moves a $50K Market: Manipulation Path" at (95, 25). Margins: top 50, right 30, bottom 50, left 60. L-shaped axes `#333` 1px.
- **Price data (20 points, y mapped from the 30–70 range):** `[40, 40, 42, 45, 50, 55, 58, 60, 62, 63, 64, 63, 60, 55, 48, 42, 40, 40, 39, 40]`. Line `#e74c3c`, 2.5px. Range check: min 39, max 64 — both inside the 30–70 plot window.
- **Arithmetic that must close:** index 0 = 40 (true value), index 4 = 50, so the price impact of the $5K buy is exactly **10 points**, matching the headline. Peak = 64 at index 10. Exit at index 11 = 63, a gross gain of **50%** on a 42¢ fill.
- **Stage markers (filled dots radius 5, two-line 10px labels; every percentage in a label is read from the array at render, never typed):**
  - index 2 — `#e74c3c` — "Manipulator buys" / "$5K at 42%" (42 = `steps[2]`)
  - index 4 — `#e67e22` — "Impact:" / "+10 pts" (`steps[4] − steps[0]`)
  - index 9 — `#f39c12` — "Followers pile in" / '"signal detected!"'
  - index 11 — `#27ae60` — "Manipulator sells" / "at 63% → +50%" (`steps[11]`, and the gross return `(steps[11] − steps[2]) / steps[2]`)
  - index 16 — `#555` — "Reverts to" / "true value: 40%" (`steps[16]`)
- **True value line:** dashed [4,4] horizontal `#27ae60` 1.5px at `steps[0]`, labeled 11px `#27ae60` "True probability: 40%" with the value read from the array.
- **Axis labels (12px `#555`):** x "Time"; y (rotated) "Market Price (%)".

## Information Asymmetry Timing

**A Move from 30% to 40% Is Indistinguishable from Noise**

- **The sequence:** Insider bets, price moves, public follows the "signal," insider exits at profit.
- **What you observe:** A price that already embeds insider action you cannot separate out.
- **The unanswerable question:** Was 30%→40% genuine public information or one informed bettor?
- **Why price alone fails:** The tape carries no attribution for who moved it or why.
- **Timing penalty:** By the time you read the market, the information is already extracted.

### Visualization (canvas `canvas4`, 500×300)

Rising price path segmented into four shaded phases from insider entry to resolution. Fixed array — no generator needed.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Insider Trading Pattern in Prediction Markets" at (95, 25). Margins: top 50, right 30, bottom 50, left 60. L-shaped axes `#333` 1px.
- **Price data (29 points, y-scale 0–100):** `[30, 30, 31, 32, 35, 40, 45, 50, 55, 58, 62, 65, 68, 72, 75, 78, 80, 82, 85, 88, 90, 92, 94, 95, 96, 97, 98, 99, 100]`. Line `#2980b9`, 2.5px. Monotone non-decreasing, ends at exactly 100 (resolution).
- **Phase bands (fill = phase color + "15" alpha; two-line 10px labels in phase color; the endpoint percentages in every label are read from the array at render):**
  - indices 0–5 — `#e74c3c` — "Insider buys" / "(30→40%)" (`prices[0]`→`prices[5]`)
  - indices 5–14 — `#f39c12` — "Public follows" / "(40→75%)" (`prices[5]`→`prices[14]`)
  - indices 14–20 — `#27ae60` — "Insider exits" / "(75→90%)" (`prices[14]`→`prices[20]`)
  - indices 20–28 — `#555` — "Event resolves" / "(90→100%)" (`prices[20]`→`prices[28]`)
- **Bottom annotation (bold 11px `#c0392b`, two lines, endpoints substituted from the array):** "Q: Was the move from 30→40% signal or noise?" / "A: You cannot tell from price alone."
- **Y-axis label (12px `#555`, rotated):** "Market Price (%)".

## Event Resolution Ambiguity

**Your Model's Target and the Contract's Resolution Are Different Events**

- **Criteria you don't write:** "Will X happen by year end?" never says what counts as X.
- **Who defines it:** Market makers set resolution rules that may not match your prediction target.
- **Edge-case cost:** Disputes, frozen markets, and ambiguous outcomes follow from vague criteria.
- **Definition-dependent:** "Will there be a recession?" resolves differently under each definition.
- **Seemingly clear:** "Candidate wins?" breaks on a recount, legal challenge, or contested result.
- **Invisible premium:** Resolution risk is priced into the market but absent from external models.

### Visualization (canvas `canvas5`, 500×300)

Boxed diagram: one question, three conflicting resolution interpretations, plus a model-prediction box. Static text — no generator, no computed statistics.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Resolution Ambiguity: Same Event, Different Outcomes" at (60, 25). Margins: top 50, right 20, bottom 40, left 40.
- **Question box:** fill `#1a527622`, bold 13px `#1a5276` text: '"Will there be a recession this year?"'.
- **Three interpretation boxes (equal width, height 90; fill/border by result — YES `#27ae60`+`#27ae6022`, NO `#e74c3c`+`#e74c3c22`, ??? `#f39c12`+`#f39c1222`; source bold 11px `#333`, result bold 16px in border color, note 10px `#555`):**
  - "Official arbiter" — "NO" — "Declared a year late"
  - "GDP rule (2 Qs neg)" — "YES" — "Q1 & Q2 negative"
  - "Contract definition" — "???" — "Depends on the venue"
- **Model box (fill `#2980b922`, border `#2980b9` 2px):** bold 12px `#2980b9` "Your model predicts: P(recession) = 65%", then 11px `#c0392b` "But which definition? Your P(recession) ≠ market's P(recession)".
- **Footer (9px `#888`):** "Illustrative Example — figures are constructed, not measured."

## Regulated Venue: Contract Approval Limits

**Your Best Edge May Sit on a Topic That Cannot Be Legally Traded**

- **The gate:** A regulated venue needs its regulator to sign off on each contract type.
- **Mid-flight risk:** Some markets shut down mid-contract; sensitive events may be barred outright.
- **What restricts coverage:** Regulation, not information availability, decides the contract universe.
- **Censored view:** You see only approved events — coverage is structurally incomplete.
- **The statistical name:** Selection bias in what is tradeable, not in what is predictable.

### Visualization (canvas `canvas6`, 500×300)

Two-column approved-vs-blocked comparison panel. Static text — no generator, no computed statistics.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Regulated Venue: What You Can vs Cannot Trade" at (105, 25). Margins: top 50, right 20, bottom 30, left 40. Columns are each 45% of plot width with a 10% gap.
- **Approved column (fill `#27ae6022`, border `#27ae60` 2px, heading bold 13px `#27ae60` "Regulator Approved"; items 11px `#333` prefixed "✓ "):** Weather events, Economic indicators, Central bank rates, Box office results, Award shows, Company earnings.
- **Blocked column (fill `#e74c3c22`, border `#e74c3c` 2px, heading bold 13px `#e74c3c` "Blocked / Restricted"; items 11px `#333` prefixed "✗ "):** Political contests, Armed conflict, Named individuals, Sports (most), Crypto prices, Public health events.
- **Bottom annotation (bold 11px `#c0392b`):** "Selection bias: tradeable events ≠ predictable events".
- **Naming rule:** no venue, regulator, or agency is named — "regulated venue" and "regulator" only.

## On-Chain Venue: Blockchain Settlement Delays

**A $50 Bet Can Cost $30 in Gas Exactly When You Need to Trade**

- **The friction:** Bets settle on-chain, so gas spikes during volatile events block quick exits.
- **Concrete cost:** At the modelled peak of 287 gwei, one trade costs about $30 in gas.
- **Fee vs stake:** That is 60% of a $50 stake — the edge is gone before the bet is placed.
- **Price breaks down:** High-gas prices do not equal true probability — trading is too expensive.
- **Update failure:** The market cannot re-price efficiently while fees exceed the edge.
- **Worst timing:** Prices go stale precisely when information is moving fastest.

### Visualization (canvas `canvas7`, 500×300)

Dual-series line chart: gas price vs implied market staleness over 24 hours.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Gas Price vs Market Efficiency (On-Chain Venue)" at (95, 25). Margins: top 50, right 30, bottom 55, left 60. L-shaped axes `#333` 1px.
- **Generator:** `lcg(20250357)`, one generator for both series, consumed in interleaved order (gas value then staleness value, per hour).
- **Series formula (24 hourly points, i = 0…23):**
  - `gas[i] = (i >= 10 && i <= 14) ? 200 + rnd() * 100 : 30 + rnd() * 15`
  - `stale[i] = gas[i] > 100 ? 30 + rnd() * 10 : 2 + rnd() * 3`
- **Computed values with this seed:** calm hours span **32–45 gwei** (mean 40); the spike window hours 10–14 spans **203–287 gwei**, peaking at **287 gwei at hour 13**. Staleness is **2.1–4.9 min** when calm and **32–39 min** in the spike, peaking at 39 min. Axis maxima 350 gwei and 45 min both clear the realised peaks.
- **Cost annotation (computed at render, bold 10px `#c0392b`):** fee = `gas × 1e-9 × 150,000 gas units × $700 per token`; at the realised peak this evaluates to **$30**, printed as "Peak: 287 gwei = $30 per trade" alongside "= 60% of a $50 stake" (`fee / 50`). The prose $30 figure is therefore the chart's own arithmetic, not an assertion.
- **Correlation label (computed at render):** Pearson r between the gas and staleness arrays, printed 10px `#555` as "r(gas, staleness) = 0.98".
- **Series:** gas price solid `#e74c3c` 2px; staleness dashed [4,4] `#9b59b6` 2px. Both plotted with x = `i / (hours − 1)`.
- **High-gas zone:** hours 10–14 shaded `#e74c3c15`, labeled bold 10px `#e74c3c` "High gas:" / "market frozen".
- **Legend (top left):** `#e74c3c` swatch "Gas price (gwei)"; `#9b59b6` swatch "Price staleness (min)" (11px `#333`).
- **X-axis label (12px `#555`):** "Hour of day". **Footer (9px `#888`):** "Illustrative Example — 150k gas units at $700/token."

## Correlation Across Markets

**Being Right About Mispricing Does Not Mean You Can Collect**

- **The linkage:** "Party A wins Chamber A" resolving YES should lift "Party A wins Chamber B" too.
- **The gap:** Related markets are genuinely correlated but priced independently of each other.
- **Theory:** Arbitrage exists — buy the correlated outcomes underpriced relative to one another.
- **Practice:** Simultaneous multi-market execution is hard and capital locks up until resolution.
- **Extra unknown:** The correlation structure itself is uncertain, so the mispricing persists.

### Visualization (canvas `canvas8`, 500×300)

Network diagram of six linked markets. **Every `r=` label is a Pearson correlation computed from the two
node price series at render time — no correlation coefficient is invented.** The previous version printed
`(0.3 + Math.random() * 0.5).toFixed(2)`, a coefficient no data produced; the fix is to build the series
the labels claim to describe.

- **Generator:** `lcg(20250383)`. A shared latent factor drives all six markets, which is what makes them
  genuinely correlated: `f[0] = 0`, `f[t] = clamp(0.90·f[t-1] + (rnd() − 0.5)·0.09, −0.18, 0.18)` over T = 60 steps.
- **Per-market series:** `p[t] = clamp(base + load·f[t] + (rnd() − 0.5)·noise, 0.02, 0.98)`; the clamp never
  binds at this seed, so no series is distorted. Parameters (base, load, noise):
  - m0 (0.48, +1.00, 0.06) — m1 (0.55, +0.90, 0.07) — m2 (0.52, +0.80, 0.08)
  - m3 (0.34, +0.65, 0.10) — m4 (0.62, **−0.60**, 0.09) — m5 (0.42, +0.55, 0.11)
  - The negative load on m4 gives the diagram one genuinely negative edge rather than an all-positive fake.
- **Probability closure:** each node's printed YES price is `p[T−1]`, rounded; NO is `100 − YES` with no vig,
  so every displayed pair sums to exactly 100%. Computed finals: m0 45/55, m1 53/47, m2 48/52, m3 32/68,
  m4 63/37, m5 43/57. All lie strictly inside (0, 100).
- **Nodes (circles radius 30, fill = node color + "33" alpha, stroke = node color 2px, two-line bold 10px labels in node color; positions as fractions of plot area; the percentage is the computed final, not typed):**
  - (0.5, 0.15) "Top office" / "YES: 45%" — `#e74c3c` (m0)
  - (0.15, 0.55) "Chamber A" / "YES: 53%" — `#e67e22` (m1)
  - (0.5, 0.55) "Chamber B" / "YES: 48%" — `#f39c12` (m2)
  - (0.85, 0.55) "Tax bill" / "YES: 32%" — `#9b59b6` (m3)
  - (0.3, 0.9) "Shutdown avoided" / "YES: 63%" — `#2980b9` (m4)
  - (0.7, 0.9) "Defense +10%" / "YES: 43%" — `#27ae60` (m5)
- **Edges (`#ccc` 1.5px) between node pairs, each midpoint labeled 9px with the computed Pearson r** (`#c0392b` when negative, `#888` when positive):
  - 0–1 r = **0.84** · 0–2 r = **0.75** · 0–3 r = **0.57** · 1–2 r = **0.71**
  - 1–4 r = **−0.60** · 2–3 r = **0.55** · 2–5 r = **0.41** · 3–5 r = **0.48**
- **Bottom annotations (bold 11px `#c0392b` then 9px `#888`):** "Each priced independently — correlation not arbitraged" / "Illustrative Example — r computed from 60 seeded price paths; NO = 100 − YES."
- **Naming rule:** no party, person, or venue is named; "Party A", "Chamber A/B" and generic policy outcomes only.

## Regeneration instructions

- **Layout:** standard domains detail page. h1, `.subtitle` paragraph, then per pitfall an `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border, padding-bottom 8px) followed by a one-row `.obj-table`: left `<td>` (50%) holds `.obj-title` (a one-line punchline) plus a `<ul>` of labeled `<li>` bullets (`<strong>Label:</strong>` + short phrase, one line each), right `<td>` (50%, centered) holds the canvas. Even table rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `ul` margin `8px 0 8px 20px`, 0.9em `#333`; `li` margin `4px 0`; `strong` `#1a5276`; `.philosophy` callout style available (background `#f0f4f8`, left border 4px `#2980b9`) but unused. No nav bar, no back/home links.
- **Canvases:** each declared `<canvas id="canvasN" width="500" height="300">`; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) at 500×300px, and calls `ctx.scale` so drawing stays in logical coordinates, and sets base font 17px system sans-serif. Each chart is an IIFE painting on a `#f0f4f8` background.
- **Shared helpers, declared right after `setupCanvas`:** `lcg(seed)` (seeded Park-Miller LCG — deterministic, never `Math.random()`), `stdev(a)` (population standard deviation), and `pearson(xs, ys)` (Pearson correlation). Charts call these instead of restating the formulas.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark red `#c0392b`, secondary blue `#2980b9`, purple `#9b59b6`, amber `#f39c12`, gray `#555`/`#333`/`#888`.
- **No randomness at render:** `Math.random()` must not appear anywhere in the page. Charts 1, 7 and 8 use the fixed seeds listed at the top; charts 2–6 are closed-form or fixed arrays. Any statistic shown as a label is computed from the plotted data in JS.
- **Naming:** no real venues, regulators, parties, people, or elections. Generic placeholders only ("regulated venue", "on-chain venue", "Party A", "Chamber A"). Constructed figures are footnoted "Illustrative Example".
