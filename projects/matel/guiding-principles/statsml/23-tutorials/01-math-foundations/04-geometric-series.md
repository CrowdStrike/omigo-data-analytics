# Geometric Series

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Geometric Series

**Subtitle:** Add a number, then a fixed fraction of it, then a fraction of that, forever — the infinite total is just first term ÷ (1 − ratio)

## The Cashback Coupon That Never Quite Ends

**Tags:** `core idea` (blue), `fixed ratio` (green), `running total` (orange)

- **The deal** — a cafe returns 20% of every purchase as store credit you can spend again
- **Round after round** — $100 spent earns $20 credit, the $20 earns $4, the $4 earns $0.80
- **Shrinking fast** — every round is exactly 0.2 times the one before; that fixed ratio is the key
- **It stops growing** — the running total climbs 100, 120, 124, 124.80 and never passes $125
- **The name** — a sum where each term is the last one times a fixed ratio is a geometric series

*Example (italic):* A $100 gift card at this cafe really buys $125 of coffee — the extra $25 is the geometric series at work.

**Key point:** Infinitely many rounds can still add up to a finite number, as long as each round is a fixed fraction of the one before. Here: 100 ÷ (1 − 0.2) = $125.

### Visualization (canvas `c1`, 720×300)

Dual-panel chart: each cashback round's amount as bars (left) and the running total leveling off at the $125 limit (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One $100 Purchase, 20% Back in Credit Each Round".
- **Data:** rounds `['spend', 'r1', 'r2', 'r3', 'r4']`; amounts `[100, 20, 4, 0.80, 0.16]` with labels "$100", "$20", "$4", "$0.80", "$0.16"; running totals `[100, 120, 124, 124.80, 124.96]`.
- **Left panel (amounts):** axis origin x=55, width 280, baseline y=240, chart height 170, y scale 0–100; five bars fill `rgba(42,120,214,0.45)` (bar width = panel/5, 6px inset, minimum 2px height); bold 12px blue `#2a78d6` dollar label above each bar; round labels 12px `#444` below baseline; orange `#d95926` bold 12px annotation "each bar = 0.2 × the last" at (panel center + 30, baseY − 90); caption 12px `#444` "money spent per round" at bottom.
- **Right panel (running total):** axis origin x=400, width 280, same baseline/height, y scale 0–130; magenta `#d55181` dashed (5/4) 2px horizontal limit line at $125 with right-aligned bold 12px label "$125 = 100 ÷ (1 − 0.2)" above it; green `#008300` 3px line through the five totals (points inset 25px from each end) with 4px green dots; round labels 12px `#444` below baseline; green bold 12px annotation "100 → 120 → 124 → 124.80 → 124.96" centered at baseY − 40; caption "running total never passes $125".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The One Sum to Know by Heart

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The formula** — a + ar + ar² + ... = a / (1 − r), true whenever the ratio r is below 1
- **Plug in** — the cafe: a = $100 and r = 0.2, so the total is 100 / 0.8 = $125 exactly
- **Halves** — 1 + 1/2 + 1/4 + 1/8 + ... = 1 / (1 − 0.5) = 2, the most famous case
- **Near one** — r = 0.9 gives 1/(1 − 0.9) = 10; the closer r is to 1, the bigger the total
- **Fast settle** — with r = 0.2 the first three terms already reach 1.24 of the 1.25 total

*Example (italic):* Starting from a = 1, ratio 0.2 settles at 1.25 within four terms, while ratio 0.9 is still climbing toward 10 after sixteen.

**Key point:** One formula covers every case: total = first term ÷ (1 − ratio). Memorize it — everything else on this page is that line wearing different clothes.

### Visualization (canvas `c2`, 720×300)

Single-panel line chart of the partial sums of 1 + r + r² + ... for three ratios (0.2, 0.5, 0.9) over 16 terms, each with its dashed limit line.

- **Title (bold 15px, `#1a5276`, top center):** "Running Total of 1 + r + r² + … for Three Ratios".
- **Data (hardcoded 16-element partial-sum arrays):**
  - r = 0.2: `[1, 1.2, 1.24, 1.248, 1.25, 1.25, 1.25, 1.25, 1.25, 1.25, 1.25, 1.25, 1.25, 1.25, 1.25, 1.25]`
  - r = 0.5: `[1, 1.5, 1.75, 1.875, 1.938, 1.969, 1.984, 1.992, 1.996, 1.998, 1.999, 2.0, 2.0, 2.0, 2.0, 2.0]`
  - r = 0.9: `[1, 1.9, 2.71, 3.439, 4.095, 4.686, 5.217, 5.695, 6.126, 6.513, 6.862, 7.176, 7.458, 7.712, 7.941, 8.147]`
- **Axes:** origin x=60, width 600, baseline y=250, chart height 190, y scale 0–11; x tick labels "1", "4", "7", "10", "13", "16" (every 3rd term, 12px `#444`); y tick labels 0–10 in steps of 2, right-aligned at x−8; x-axis caption "number of terms added" bottom center.
- **Limit lines (dashed 5/4, 1.5px, right-aligned bold 12px label above each):** magenta `#d55181` at 10 "limit 10 = 1/(1−0.9)"; green `#008300` at 2 "limit 2 = 1/(1−0.5)"; blue `#2a78d6` at 1.25 "limit 1.25 = 1/(1−0.2)".
- **Series:** 3px lines with 3px dots — r=0.2 blue `#2a78d6`, r=0.5 green `#008300`, r=0.9 magenta `#d55181`.
- **Annotations (left-aligned):** magenta bold 13px "r = 0.9 still climbing at 8.15" at (lx+260, baseY−155); green bold 12px "r = 0.5 settles at 2" at (lx+190, baseY−48); blue bold 12px "r = 0.2 settles at 1.25 by term 4" at (lx+190, baseY−8).

## Retries, PageRank, Annuities — Three Costumes

**Tags:** `where it's used` (blue), `same skeleton` (green)

- **Retries** — if 20% of calls fail and failures retry, expected attempts = 1/(1 − 0.2) = 1.25
- **PageRank** — damping 0.85 lets score keep flowing hop after hop; multiplier = 1/0.15 ≈ 6.67
- **Annuities** — $1,000 a year forever, discounted 5% a year, is worth 1,000/0.05 = $20,000 now
- **Same skeleton** — each one is first term ÷ (1 − ratio); only the story changes
- **Spot the ratio** — find "the same fraction survives each round" and the formula is ready

*Example (italic):* A capacity planner who forgets the retry series provisions for 1.0 requests per call and runs 25% hot.

**Key point:** Whenever each round keeps a fixed fraction of the last — failed calls, damped link score, discounted dollars — the infinite total collapses to one division.

### Visualization (canvas `c3`, 720×300)

Three side-by-side bar panels showing the first few rounds of a retry series, a PageRank damping series, and a discounted annuity, each with its closed-form total underneath.

- **Title (bold 15px, `#1a5276`, top center):** "Three Costumes, One Formula: first term ÷ (1 − ratio)".
- **Shared panel geometry:** baseline y=235, chart height 140; each panel has an L-shaped `#999` axis, a bold 13px `#1a5276` panel title 12px above the chart top, 11px `#444` bar labels below the baseline, and a bold 12px `#1a5276` total line at baseY+34; bars are 4px inset with minimum 2px height.
- **Panel 1 (retries):** origin x=45, width 190; bars `[1, 0.2, 0.04, 0.008]` labeled "1", "0.2", "0.04", ".008"; fill `rgba(42,120,214,0.5)`; title "retries: 20% fail"; total "expected 1.25 attempts"; y scale max 1.05.
- **Panel 2 (PageRank):** origin x=270, width 190; bars `[1, 0.85, 0.72, 0.61, 0.52]` labeled "1", ".85", ".72", ".61", ".52"; fill `rgba(0,131,0,0.45)`; title "PageRank: damping 0.85"; total "multiplier 1/0.15 ≈ 6.67"; y scale max 1.05.
- **Panel 3 (annuity):** origin x=495, width 190; bars `[952, 907, 864, 823, 784]` labeled "$952", "$907", "$864", "$823", "$784"; fill `rgba(217,89,38,0.5)`; title "annuity: $1,000/yr at 5%"; total "worth $20,000 today"; y scale max 1000.
- **Caption (12px `#6b7280`, bottom center):** "bars = the first few rounds of each series; totals include every round to infinity".

## It Only Works When the Ratio Is Below One

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The trap** — a/(1 − r) is meaningless when r ≥ 1; the sum just grows forever
- **r = 1.1** — terms grow 10% each round: 1, 1.1, 1.21; after 20 rounds the total tops 57
- **r = 1 exactly** — 1 + 1 + 1 + ... never settles, and the formula would divide by zero
- **Finite cutoff** — stopping after n terms uses a(1 − rⁿ)/(1 − r), which works for any r
- **Sanity check** — before quoting a/(1 − r), confirm each round really shrinks by a fixed factor

*Example (italic):* Retries that spawn 1.1 new calls each on average are not a series to sum — they are an outage in progress.

**Common mistake:** Applying total = a/(1 − r) without checking r < 1. Below one the rounds fade and the total settles; at or above one it diverges and no finite answer exists.

### Visualization (canvas `c4`, 720×300)

Dual-panel line chart: partial sums for r = 0.8 converging to 5 (left) vs r = 1.1 diverging past 57 (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Ratio 0.8 Levels Off at 5 — Ratio 1.1 Never Stops".
- **Data (hardcoded 20-element partial-sum arrays):**
  - r = 0.8: `[1, 1.8, 2.44, 2.952, 3.362, 3.689, 3.951, 4.161, 4.329, 4.463, 4.571, 4.656, 4.725, 4.780, 4.824, 4.859, 4.887, 4.910, 4.928, 4.942]`
  - r = 1.1: `[1, 2.1, 3.31, 4.641, 6.105, 7.716, 9.487, 11.436, 13.580, 15.937, 18.531, 21.384, 24.523, 27.975, 31.772, 35.950, 40.545, 45.599, 51.159, 57.275]`
- **Left panel (r = 0.8):** axis origin x=55, width 280, baseline y=240, chart height 170, y scale 0–6; green `#008300` dashed (5/4) 2px limit line at 5 with left-aligned bold 12px label "limit 5 = 1/(1−0.8)"; blue `#2a78d6` 3px line through the 20 partial sums (no dots); blue bold 12px annotation "total settles: formula valid" centered at baseY−25; caption 12px `#444` "r = 0.8, terms 1 → 20" at bottom.
- **Right panel (r = 1.1):** axis origin x=400, width 280, same baseline/height, y scale 0–60; orange `#d95926` 3px line through the 20 partial sums (no dots, no limit line); orange bold 12px right-aligned annotation "20 rounds in, total tops 57" near the top right; red `#e74c3c` bold 13px centered annotation "no limit exists — a/(1−r) does not apply" at baseY−25; caption "r = 1.1, terms 1 → 20".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
