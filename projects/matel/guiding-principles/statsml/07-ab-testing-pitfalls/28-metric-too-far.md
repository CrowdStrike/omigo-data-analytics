# Metric Too Far

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Metric Too Far — A/B Testing Pitfalls

**Subtitle:** Power — Measuring a micro-feature against a macro-metric guarantees inconclusive results regardless of feature quality.

## Section 1: Causal Dilution

- Each step between a feature change and the top-line metric introduces noise: other features, user behavior variation, and external factors all dilute the signal.
- A feature that perfectly improves tooltip clarity for 3% of users → those users complete tasks 10% faster → but only 5% of task completions lead to purchases → purchase amount varies ±40% → GMV signal from this feature: **~0.015% change** in total GMV.
- Detecting 0.015% with p<0.05 and 80% power requires **~70M users in each arm**. That is not feasible.
- The test will **always** come back "no significant difference" — not because the feature failed, but because you asked the wrong question.
- This is a **power problem disguised as an effectiveness problem**. Teams conclude "the feature doesn't work" when in reality the measurement instrument was incapable of detecting it.

### Visualization (canvas `canvas1`, 720×400)

Causal chain diagram: five circular nodes connected by decaying signal waves inside widening gray noise bands, plus a signal-strength bar chart below.

- **Title (bold 14px `#1a5276`, centered, y=28):** "Signal Dilution Across Causal Chain".
- **Nodes (circles, radius 32, at y=160):** label / x / signal strength: "Feature Change" x=70 signal 1.0 (green `#27ae60`); "Direct Effect" x=200 signal 0.7 (blue `#1a5276`); "Behavior Change" x=340 signal 0.35 (blue); "Intermediate Metric" x=480 signal 0.12 (blue); "Top-Line Metric" x=620 signal 0.02 (red `#e74c3c`). Circles filled at 15% alpha of their color, stroked 2.5px; two-line bold 11px labels centered in each.
- **Connections:** between consecutive nodes, a sine-wave signal line (`#1a5276`, width 2, sin frequency 0.15, amplitude interpolating between node signals ×28px) inside a gray noise band `rgba(200,200,200,0.3)` that widens toward the next node; small blue triangle arrowhead at each connection end.
- **Signal strength bars (below, baseline y=340):** small centered label "Signal Strength" (11px `#666`) at y=250; one bar per node (40 wide, height = signal×80), fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 1; percentage labels below each bar (10px `#333`): 100%, 70%, 35%, 12%, 2%.
- **Noise label (italic 11px `#e74c3c`, right-aligned):** "Signal buried in noise".
- **Bottom annotation (12px `#e74c3c`, centered, y=H−15):** "At the top-line metric, the feature's signal is undetectable (0.015% in ±40% noise)".

## Section 2: Metric Proximity Principle

- **Rule:** measure the FIRST thing your feature changes, not the LAST thing you hope it eventually affects.
- Feature → proximate metric (high sensitivity, fast) vs distal metric (low sensitivity, slow, noisy).
- The proxy must be validated: show historically that proxy improvements correlate with top-line improvements (one-time observational study).
- Once validated, individual feature tests measure the proxy. Periodic holdback experiments on bundles of features measure aggregate top-line impact.
- **Examples:** Search relevance improvement → measure ranking position clicks, NOT revenue. Checkout button color → measure conversion rate at that step, NOT total GMV.
- Connects to pitfall #27 (testing-to-justify): when the metric is impossible to move, the test becomes theater, and teams resort to p-hacking to find "something significant."

### Visualization (canvas `canvas2`, 720×400)

Two stacked scenario boxes comparing power analysis at distal vs proximate metric.

- **Title (bold 14px `#1a5276`, centered, y=28):** "Same Feature, Different Measurement Points".
- **Subtitle (12px `#666`, centered, y=48):** "Power analysis: p < 0.05, 80% power, identical real effect".
- **Scenario A box (rounded 640×130 at x=40, y=80, radius 8):** fill `rgba(231,76,60,0.06)`, stroke `#e74c3c` width 2. Header bold 13px `#e74c3c`: "Scenario A: Measure GMV (distal metric)". Stat rows (label 12px `#555`, value bold 12px `#333`): "Required N per arm:" "70,000,000 users"; "Test duration:" "~18 months (if even possible)"; "Effect to detect:" "0.015% GMV change"; "Noise level:" "±40% variance in purchase amounts". Right-aligned verdict: bold 16px `#e74c3c` "INCONCLUSIVE" and 11px "\"No significant difference detected\"".
- **Scenario B box (same geometry at y=230):** fill `rgba(39,174,96,0.06)`, stroke `#27ae60`. Header bold 13px `#27ae60`: "Scenario B: Measure Proxy — Task Completion Rate (proximate metric)". Stat rows: "Required N per arm:" "50,000 users"; "Test duration:" "~2 weeks"; "Effect to detect:" "8.2% improvement in task completion"; "Noise level:" "±12% variance (binary outcome)". Verdict: bold 16px `#27ae60` "DETECTED: +8.2%" and 11px "p = 0.003, statistically significant".
- **Bottom annotation (bold 12px `#1a5276`, centered, y=H−15):** "Same feature. Same real effect. The only difference is where you point the measurement."

## Section 3: Real Example: Clickbait Headlines at News Sites

- News sites that graded headline tests purely on clicks found that sensational, exaggerated headlines won the test almost every time. Clicks were standing in for reader value, but the two had drifted so far apart that the "winning" headline was often actively worse for the reader.
- Readers who felt tricked by a headline left the page quickly and trusted the site a little less each time, so over the following months paid-subscription cancellations crept up even while the click numbers looked great.
- Several publishers responded by moving the test metric closer to real value — for example, whether readers actually finished the article or came back the next week (engagement metrics instead of click-through rate).

### Visualization (canvas `canvas3`, 720×320)

Split two-panel bar chart: clicks measured vs subscribers kept, separated by a dashed divider.

- **Title (bold 16px `#2a2a2a`, centered, y=26):** "Clickbait Headline: Wins the Test, Loses the Readers".
- **Geometry:** baseline y=230, max bar height 140, bar width 90.
- **Left panel** — header bold 14px `#1a5276` at (190, 58): "What the test measured: clicks". Bars: Honest at x=100, 66% of max height, fill `rgba(26,82,118,0.35)`, stroke `#1a5276`; Clickbait at x=220, full height, fill `rgba(230,126,34,0.35)`, stroke `#e67e22`, with bold 14px `#e67e22` "test winner" above. Labels below (14px `#333`): "Honest", "Clickbait".
- **Right panel** — header bold 14px `#1a5276` at (545, 58): "What mattered: subscribers kept". Bars: Honest at x=450, 95% of max height, fill `rgba(39,174,96,0.35)`, stroke `#27ae60`; Clickbait at x=570, 62% of max height, fill `rgba(231,76,60,0.25)`, stroke `#e74c3c`, with bold 14px `#e74c3c` "real loser" above. Labels: "Honest", "Clickbait".
- **Divider:** vertical dashed `#ddd` line (dash 4/4) at x=W/2 from y=45 to baseline+25.
- **Takeaway (bold 15px `#e74c3c`, centered, y=H−15):** "Clicks were a proxy so far from reader value that winning the test meant losing the business".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section (three rows in one table); left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `ul` 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); canvas ids on this page are `canvas1`, `canvas2`, `canvas3`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#333`.
