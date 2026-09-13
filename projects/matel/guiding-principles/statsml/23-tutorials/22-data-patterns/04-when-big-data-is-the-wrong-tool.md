# When "Big Data" Is the Wrong Tool

**Page type:** detail page (tutorial topic page: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** When "Big Data" Is the Wrong Tool

**Subtitle:** Distributed systems add cost and complexity — often a sample, a summary, or one beefy machine answers the question faster

## Two Billion Receipts, One Simple Question

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — a supermarket chain has 2 billion receipts; the boss asks the average basket
- **The reflex** — "2 billion rows is big data, spin up the cluster"
- **The shortcut** — a random sample of 100,000 receipts fits in any spreadsheet
- **The result** — full data says $23.47; three separate samples say $23.41, $23.52, $23.44
- **The lesson** — the size of the answer you need, not the size of the data, picks the tool

*Example:* A chef tastes one spoonful of the soup, not the whole pot — a stirred pot makes the spoonful enough.

**Key point:** A big dataset does not make a big question — averages, shares, and trends are answered by a well-mixed sample.

### Visualization (canvas `c1`, 720×300)

Bar chart: full-data average vs three 100k-sample averages.

- **Title (bold 16px, `#1a5276`, top center):** "Average Basket: All 2 Billion Receipts vs 100,000-Receipt Samples".
- **Data:** labels `['all 2 billion', 'sample 1', 'sample 2', 'sample 3']`; values `[23.47, 23.41, 23.52, 23.44]`; colors: first bar blue `#2a78d6`, the three samples green `#008300`.
- **Axes:** padding top 55, bottom 62, left 75, right 30; y scale max 26; muted baseline; bar width 105, equal gaps; bold 13px value labels "$23.47" etc. above bars, 12px category labels below.
- **Reference line:** dashed blue `#2a78d6` horizontal line (dash 6/4, width 1.5) across the chart at the true average 23.47.
- **Annotations:** green bold 13px above the chart "every sample lands within 6 cents of the full answer"; muted 12px at bottom "illustrative numbers — 20,000x less data, same decision".

## How Wrong Can the Spoonful Be? Check It by Hand

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **Receipts vary** — basket totals spread around the mean by about $18 (the std dev)
- **The formula** — a sample average misses by about 2 × 18 ÷ √n
- **n = 100** — √100 = 10, so 2 × 18 ÷ 10 = ±$3.60 — too sloppy
- **n = 10,000** — √10,000 = 100, so 2 × 18 ÷ 100 = ±$0.36 — already fine
- **n = 100,000** — √100,000 ≈ 316, so 2 × 18 ÷ 316 = ±$0.11 — pennies
- **Diminishing returns** — 10x more receipts only shrinks the miss by about 3x

*Example:* Going from 100,000 receipts to all 2 billion moves the answer by less than a dime.

**Key point:** Sample error shrinks with the square root of n — a modest sample already pins an average to pocket change.

### Visualization (canvas `c2`, 720×300)

Bar chart of expected miss (±2 SE) vs sample size, sd = $18.

- **Title (bold 16px, `#1a5276`, top center):** "How Far Off the Sample Average Can Be (2 x 18 / √n)".
- **Data:** n labels `['100', '1,000', '10,000', '100,000']`; miss values `[3.60, 1.14, 0.36, 0.11]`.
- **Axes:** padding top 55, bottom 62, left 80, right 40; y scale max 4; muted baseline; bar width 100, equal gaps, minimum bar height 4px.
- **Colors:** first two bars orange `#d95926`; last two bars green `#008300`.
- **Labels:** bold 13px "±$3.60", "±$1.14", "±$0.36", "±$0.11" above bars; n values 12px below.
- **Annotations:** muted 12px x-caption "receipts in the sample (n)"; green bold 13px "10,000 receipts already pin the average to ±36 cents"; muted 12px "each 10x more data buys only ~3x less error".

## The Bill for the Cluster Nobody Needed

**Tags:** `cost of complexity` (orange), `where it's used` (blue)

- **Sample on a laptop** — minutes of work, tools the whole team already knows
- **One beefy machine** — renting a 512 GB-RAM server crunches far more than people think
- **A cluster** — weeks of setup, new tools to learn, and someone on call when it breaks
- **Complexity compounds** — every later question now pays the cluster tax too
- **Precomputed summaries** — daily totals per store often answer next month's questions free

*Example:* The team spent six weeks building a pipeline for a number an intern's sample had already nailed.

**Key point:** Distributed systems are a standing cost in money, skills, and upkeep — pay it only when no simpler tool can answer.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of time-to-answer by approach, log scale.

- **Title (bold 16px, `#1a5276`, top center):** "Time to Answer \"What Is the Average Basket?\"".
- **Layout:** bars start at x=235, right padding 100, first row top y=62, row height 50, bar height 28. Length is log10(seconds) mapped from logMin=2 to logMax=7 (100 s to 10^7 s, ~4 months).
- **Rows** (label right-aligned in bold 13px `#2c3e50` left of bar; time note in bar color right of bar):
  - "sample on a laptop" — 600 s, note "10 min", green `#008300`
  - "one beefy rented machine" — 7200 s, note "2 hours", blue `#2a78d6`
  - "build a cluster pipeline" — 3,628,800 s, note "6 weeks", orange `#d95926`
- **Annotations (centered):** orange bold 13px "same $23.47, three price tags — and the cluster keeps costing after the answer"; muted 12px "illustrative times, log scale".

## The Confusion: When You Really Do Need All the Rows

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The overcorrection** — "sampling always works" is as wrong as "always use the cluster"
- **Rare events break it** — a fraud pattern hitting 1 in 100,000 receipts is nearly invisible
- **Do the math** — all 2 billion receipts hold ~20,000 cases; a 100,000 sample holds ~1
- **Per-person detail** — serving every single customer their own history needs every row
- **The question test** — averages and trends: sample; needles and per-user lookups: all rows

*Example:* The average-basket question needed a spoonful; the fraud hunt needed the whole pot, receipt by receipt.

**Key point:** Sample when the answer is an aggregate; keep every row when the answer is a needle or a per-person record.

### Visualization (canvas `c4`, 720×300)

Two-panel dot diagram: fraud cases visible in full data vs in a sample, split by a dashed vertical divider at x=360 (`#e5e9ef`, dash 4/3).

- **Title (bold 16px, `#1a5276`, top center):** "Fraud at 1 in 100,000 Receipts: Cases You Get to See".
- **Left panel:** heading bold 13px `#2c3e50` "all 2,000,000,000 receipts"; a dense 40×10 block of violet `#4a3aa7` 4px dots (cell pitch 5.5px) starting at x=85, y=80. Below: violet bold 14px "~20,000 fraud cases to study"; muted 12px "(each dot stands for 50 cases)"; green `#008300` bold 13px "enough to learn the pattern".
- **Right panel:** heading bold 13px "a 100,000-receipt sample"; a single magenta `#d55181` 4px dot at (542, 130) circled by a magenta 12px-radius ring. Below: magenta bold 14px "~1 fraud case expected"; muted 12px "100,000 x 1/100,000 = 1"; magenta bold 13px "one case teaches you nothing".
- **Bottom caption (ink `#1a5276` bold 13px, center):** "sampling keeps the average and throws away the needles".

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` (one `<tr>`; left `td.text-col` 50% width, right `td.viz-col` 50% width, cells padded 12px, no cell borders).
- **Text column structure:** `.tags` row of colored pill spans (`.tag` — 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each starting with `<b>` in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) opening with `<strong>Key point:</strong>`. Bullets use HTML entities where needed (×, ÷, √, ≈, ±).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box; }`; h1 2rem `#1a5276`; subtitle `#666` 0.95rem. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
- **Canvas:** each canvas declared `width="720" height="300"`, CSS `width:100%`, border `1px solid #e0e0e0`, radius 4px; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
