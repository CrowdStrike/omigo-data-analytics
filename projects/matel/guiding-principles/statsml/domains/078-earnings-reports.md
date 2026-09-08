# Domain Pitfalls: Earnings Reports

**Page type:** detail page (h2 section headings, each followed by a one-row two-column obj-table: text left 50%, canvas right 50%)
**HTML title tag:** Domain Pitfalls: Earnings Reports

**Subtitle:** Common statistical and analytical pitfalls when analyzing corporate earnings data

## GAAP vs Non-GAAP Manipulation

**Obj-title:** GAAP vs Non-GAAP Manipulation

- GAAP net income: -$200M; "adjusted" non-GAAP: +$50M.
- Difference: stock-based compensation "excluded" ($150M), restructuring ($50M), acquisition costs ($50M).
- Every quarter "one-time" charges that recur every quarter.

### Visualization (canvas `chart1`, 720×200 — declared 720×300 in markup, resized to 720×200 by the setup helper)

Waterfall (bridge) chart from GAAP loss to non-GAAP profit.

- **Title (17px `#1a5276`, at 220,18):** "GAAP to Non-GAAP Bridge".
- **Zero line:** dashed `#bdc3c7` horizontal line (dash 4/3) at y=140, labeled "$0" (11px `#7f8c8d`); axis unit "$M" (12px `#5d6d7e`, top left).
- **Bars (80px wide, 30px gap, from x=60, value scale 0.4 px per $M):**
  - "GAAP" −200 → bar drawn downward from zero, `#c0392b`, value label "-$200M" below.
  - "+SBC" +150 → bridge bar upward from running total, `#e67e22`, label "+$150M".
  - "+Restruct." +50 → `#f39c12`, label "+$50M".
  - "+Acq. Costs" +50 → `#d4ac0d`, label "+$50M".
  - "Non-GAAP" +50 → final bar above zero, `#27ae60`, label "+$50M".
- **Connectors:** dotted `#95a5a6` lines (dash 2/2) linking each bridge bar's top to the next bar.
- **Text:** bar name labels 13px/11px `#2c3e50`, centered.

## Guidance Sandbagging

**Obj-title:** Guidance Sandbagging

- Company guides "revenue $980M-$1020M"; analysts estimate $1000M; actual: $1010M → "beat expectations by 1%!"
- The game: set bar low, step over it, declare victory.
- The "beat" is manufactured, not earned.

### Visualization (canvas `chart2`, 720×200 — declared 720×300, resized by setup helper)

Three-line chart over 8 quarters: guidance (low), consensus estimates, and actuals that always "beat".

- **Title (17px `#1a5276`, top right):** "Manufactured Beats".
- **X labels:** Q1, Q2, Q3, Q4, Q1, Q2, Q3, Q4 (11px `#7f8c8d`).
- **Y scale:** $930M-$1020M; gridlines and labels at $940M, $960M, $980M, $1000M, $1020M (`#ecf0f1` gridlines, 11px `#7f8c8d` labels). Plot area padding: left 55, right 30, top 35, bottom 35.
- **Series (width 2):**
  - Guidance (low), dashed red `#e74c3c` (dash 5/3): [940, 955, 960, 970, 975, 980, 985, 980]; a `rgba(231,76,60,0.1)` band fills from the guidance line up to guidance+40.
  - Consensus Est., solid orange `#f39c12`: [950, 965, 970, 982, 988, 995, 998, 1000].
  - Actual, solid green `#27ae60` with 4px green dots: [960, 972, 980, 990, 995, 1002, 1005, 1010].
- **Legend (top left, 12px `#2c3e50` with color swatches):** "Guidance (low)", "Consensus Est.", "Actual (always \"beats\")".

## Revenue Recognition Timing

**Obj-title:** Revenue Recognition Timing

- Q4 looks weak? Pull forward Q1 deals into Q4 with aggressive recognition.
- Q1 now looks weak → pull Q2; musical chairs of revenue timing.
- Any single quarter is manipulated by ±10% via recognition games.

### Visualization (canvas `chart3`, 720×200 — declared 720×300, resized by setup helper)

Bar chart of reported quarterly revenue (colored by pull direction) with a dashed organic-trend line.

- **X labels:** Q1-24, Q2-24, Q3-24, Q4-24, Q1-25, Q2-25, Q3-25, Q4-25 (10px `#2c3e50`).
- **Y scale:** $220M-$300M; gridlines/labels at $230M, $250M, $270M, $290M (`#ecf0f1`, 11px `#7f8c8d`). Padding: left 55, right 30, top 30, bottom 40.
- **Reported bars (40px wide, centered on each quarter):** [250, 240, 255, 280, 245, 238, 260, 285] $M, value labels above each bar.
- **Pull direction (drives bar color):** pulled values [0, −8, 5, 22, −7, −15, 6, 24]; bars with pulled ≥ 0 filled `rgba(39,174,96,0.3)` with `#27ae60` outline; pulled < 0 filled `rgba(231,76,60,0.3)` with `#e74c3c` outline.
- **Organic trend line:** dashed `#1a5276` (dash 6/3, width 2): [252, 252, 253, 254, 255, 256, 257, 258].
- **Legend (top, 12px):** `#1a5276` "--- True organic growth trend"; `#27ae60` "■ Revenue pulled in"; `#e74c3c` "■ Revenue pulled away".

## EPS Inflated by Buybacks

**Obj-title:** EPS Inflated by Buybacks

- Company earns same $1B profit, but bought back 10% of shares.
- EPS "grew" 11% while actual earnings grew 0%.
- Per-share metrics improve while the underlying business performance itself does not improve at all.
- Buybacks financed by debt count as financial engineering, not real growth in earnings power.

### Visualization (canvas `chart4`, 720×200 — declared 720×300, resized by setup helper)

Dual-axis line chart 2020-2025: EPS rising (left axis) while shares outstanding shrink (right axis) and earnings stay flat.

- **X labels:** 2020, 2021, 2022, 2023, 2024, 2025 (11px `#7f8c8d`). Padding: left 55, right 55, top 30, bottom 35.
- **Data:** earnings flat [1000, 1000, 1000, 1000, 1000, 1000] $M; shares outstanding [1000, 920, 850, 780, 720, 660] M; EPS = earnings/shares = [1.00, 1.09, 1.18, 1.28, 1.39, 1.52].
- **Left axis (EPS, green labels `#27ae60`):** $0.80-$1.80, gridline labels $1.00-$1.60 in $0.20 steps (`#ecf0f1` gridlines).
- **Right axis (shares, red labels `#e74c3c`):** 600M-1100M, labels 700M-1000M in 100M steps.
- **Series:** EPS — solid green `#27ae60`, width 3, trending up. Shares — dashed red `#e74c3c` (dash 5/3, width 2), trending down.
- **Flat earnings marker:** dashed gray `#7f8c8d` horizontal line (dash 3/3, width 1.5) at 65% chart height, labeled centered 11px `#7f8c8d` "Total Earnings: flat at $1B".
- **Legend (top, 12px):** green "— EPS (\"growth\": +52%)"; red "--- Shares Outstanding (-34%)"; gray "Actual profit growth: 0%".

## Pro-Forma Adjustments

**Obj-title:** Pro-Forma Adjustments

- Remove "non-recurring" items: restructuring every year, litigation every year, impairments every year.
- At some point "adjusted earnings excluding everything bad" ≠ actual earnings.
- Pro-forma is earnings in an imaginary world.

### Visualization (canvas `chart5`, 720×200 — declared 720×300, resized by setup helper)

Diverging two-line chart 2020-2025: pro-forma earnings climbing while GAAP earnings fall, with the widening gap shaded.

- **X labels:** 2020-2025 (11px `#7f8c8d`). Y scale $300M-$1700M; gridlines/labels at $400M, $800M, $1200M, $1600M (`#ecf0f1`, 11px `#7f8c8d`). Padding: left 55, right 30, top 30, bottom 35.
- **Series (width 2.5, 4px dots at every point):**
  - Pro-Forma "Adjusted" Earnings — green `#27ae60`: [1200, 1280, 1350, 1400, 1480, 1550].
  - GAAP Actual Earnings — dark red `#c0392b`: [800, 650, 700, 550, 600, 450].
- **Gap fill:** `rgba(231,76,60,0.08)` region between the two lines; annotation 12px `#c0392b` "Growing gap: $880M" placed at the 2024 midpoint (offset right).
- **Legend (top, 12px):** green "— Pro-Forma \"Adjusted\" Earnings"; dark red "— GAAP Actual Earnings"; gray `#7f8c8d` "\"Non-recurring\" every year".

## Conference Call Qualitative Signal

**Obj-title:** Conference Call Qualitative Signal

- CEO tone, hesitation patterns, word choice changes quarter-over-quarter.
- NLP on earnings calls: works until companies start coaching executives on "AI-detectable language patterns".
- Adversarial adaptation to NLP sentiment tools.

### Visualization (canvas `chart6`, 720×200 — declared 720×300, resized by setup helper)

Three-line chart over 8 quarters: NLP sentiment diverging from actual performance as predictive correlation collapses after AI coaching begins.

- **X labels:** Q1-23, Q2-23, Q3-23, Q4-23, Q1-24, Q2-24, Q3-24, Q4-24 (11px `#7f8c8d`). Padding: left 55, right 55, top 30, bottom 35.
- **Left axis (0-1 scale, labels 0.2-0.8 in 0.2 steps, 11px `#7f8c8d`, `#ecf0f1` gridlines).** **Right axis:** correlation as percent, purple `#8e44ad` labels 20%-80%.
- **Series:**
  - NLP Sentiment — blue `#2980b9`, width 2: [0.72, 0.68, 0.55, 0.45, 0.60, 0.75, 0.78, 0.80].
  - Actual Performance — orange `#e67e22`, width 2: [0.65, 0.58, 0.40, 0.35, 0.50, 0.52, 0.48, 0.45].
  - Predictive Correlation — dashed purple `#8e44ad` (dash 5/3, width 2.5): [0.85, 0.82, 0.80, 0.78, 0.60, 0.45, 0.30, 0.15].
- **Coaching zone:** `rgba(142,68,173,0.05)` shaded region from Q1-24 onward, labeled centered 11px `#8e44ad` "AI coaching begins".
- **Legend (top, 12px):** blue "— NLP Sentiment"; orange "— Actual Performance"; purple "--- Predictive Correlation (collapsing)".

## Regeneration instructions

- **Layout:** standard domains detail page (139-style): h1, `.subtitle` paragraph, then per pitfall an unnumbered `<h2>` followed by a one-row `.obj-table` — left `<td>` (40%) with `.obj-title` + `<ul>` bullets, right `<td>` (60%, centered) with one `<canvas>` (ids `chart1`-`chart6`). No thead, no nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-table` cells border `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout style defined but unused.
- **Canvas:** markup declares `width="720" height="300"`; a shared `setupCanvas(id)` helper overrides to 720×200 CSS pixels and scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), default font 17px system sans-serif.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, gold `#d4ac0d`, dark red `#c0392b`, purple `#8e44ad`, grays `#7f8c8d`/`#95a5a6`/`#2c3e50`/`#5d6d7e`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
