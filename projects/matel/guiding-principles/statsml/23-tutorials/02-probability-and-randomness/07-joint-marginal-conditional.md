# Joint, Marginal, Conditional

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Joint, Marginal, Conditional

**Subtitle:** One table of two variables answers three different questions — divide a cell by the grand total, an edge by the grand total, or a cell by its edge

## One Coffee Shop Log, Three Questions

**Tags:** `core idea` (blue), `two-variable table` (green), `three readings` (orange)

- **The log** — a coffee shop tags 200 orders by drink (hot/iced) and time (morning/afternoon)
- **The table** — counts: 90 hot-morning, 30 hot-afternoon, 10 iced-morning, 70 iced-afternoon
- **Joint** — an inside cell over 200: P(hot and morning) = 90/200 = 45% of all orders
- **Marginal** — an edge total over 200: P(hot) = 120/200 = 60%, ignoring time entirely
- **Conditional** — a cell over its edge: P(hot given morning) = 90/100 = 90% of morning orders

*Example (italic):* Of all 200 orders, 45% were hot-and-morning, 60% were hot — but 90% of the morning orders were hot.

**Key point:** Joint, marginal, and conditional are not three tables — they are three ways to divide the same cell counts.

### Visualization (canvas `c1`, 720×300)

Annotated 2×2 count table with margins, plus a right-side legend mapping each table region to its reading.

- **Title (bold 15px, `#1a5276`, top center):** "200 Coffee Orders: One Table, Three Ways to Read It".
- **Data:** joint counts hot-morning 90, hot-afternoon 30, iced-morning 10, iced-afternoon 70; row totals hot 120, iced 80; column totals morning 100, afternoon 100; grand total 200.
- **Table grid:** origin x=100, y=60; column headers "morning", "afternoon", "total" bold 12px `#444` (columns 120px, 120px, 90px wide); row headers "hot", "iced", "total" bold 12px `#444`; header row 30px, two data rows 52px each, totals row 40px; 1px `#bdc3c7` cell borders.
- **Joint cells (90, 30, 10, 70):** fill `rgba(42,120,214,0.15)`, counts bold 16px blue `#2a78d6` centered.
- **Margin cells (120, 80, 100, 100):** fill `rgba(0,131,0,0.12)`, counts bold 15px green `#008300`.
- **Grand total (200):** no fill, bold 16px ink `#1a5276`.
- **Right-side legend (x=475, three stacked blocks, each two lines bold 12px):** blue `#2a78d6` "joint: cell ÷ 200" / "P(hot & morning) = 90/200 = 45%"; green `#008300` "marginal: edge ÷ 200" / "P(hot) = 120/200 = 60%"; orange `#d95926` "conditional: cell ÷ its edge" / "P(hot | morning) = 90/100 = 90%".
- **Caption (12px `#444`, bottom left):** "counts are illustrative".

## Dividing by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Grand total** — every joint probability divides by 200: 90/200, 30/200, 10/200, 70/200
- **Row sums** — hot row: 90 + 30 = 120; iced row: 10 + 70 = 80; together they add back to 200
- **Column sums** — morning: 90 + 10 = 100; afternoon: 30 + 70 = 100; again 200 in total
- **Joint check** — the four joint probabilities 45% + 15% + 5% + 35% sum to exactly 100%
- **Conditional check** — P(hot|morning) + P(iced|morning) = 90/100 + 10/100 = 100%

*Example (italic):* P(iced and afternoon) = 70/200 = 35%, but P(iced given afternoon) = 70/100 = 70% — same cell, different denominator.

**Key point:** Only the denominator changes: cell over 200 is joint, edge over 200 is marginal, cell over its edge is conditional.

### Visualization (canvas `c2`, 720×300)

Three mini-table panels showing the identical 2×2 table with different regions shaded as numerator and denominator, with the division written under each.

- **Title (bold 15px, `#1a5276`, top center):** "Same Table, Three Divisions".
- **Data (all three panels):** the same counts 90, 30 / 10, 70 with margins 120, 80 / 100, 100 and grand total 200.
- **Panels:** three mini tables, each 190px wide × 130px tall, at x=35, x=265, x=495, top y=70; cell counts 12px `#444`, 1px `#bdc3c7` borders; panel heading bold 13px above each table.
- **Panel 1 "joint" (blue):** hot-morning cell (90) filled `rgba(42,120,214,0.5)` as numerator; all four joint cells outlined 2px blue `#2a78d6` as the 200 denominator; below, bold 13px blue "90 / 200 = 45%".
- **Panel 2 "marginal" (green):** hot row-total cell (120) filled `rgba(0,131,0,0.4)`; all four joint cells outlined 2px green `#008300` as the 200 denominator; below, bold 13px green "120 / 200 = 60%".
- **Panel 3 "conditional" (orange):** hot-morning cell (90) filled `rgba(217,89,38,0.5)` as numerator; the morning column (90 and 10) outlined 2px orange `#d95926` as the 100 denominator; below, bold 13px orange "90 / 100 = 90%".
- **Takeaway (bold 13px ink `#1a5276`, bottom center y=280):** "numerator shaded, denominator outlined — the denominator is the whole trick".

## Why the Conditional Flips the Stock Plan

**Tags:** `where it's used` (blue), `segments` (orange)

- **The overall rate** — 60% hot across the day suggests one warm-drink-heavy stocking plan
- **By time** — mornings run 90% hot (90 of 100); afternoons run only 30% hot (30 of 100)
- **The decision** — brewing from the 60% marginal over-ices mornings and over-brews afternoons
- **The pattern** — segments can hold wildly different conditionals behind one calm marginal
- **In practice** — churn, CTR, and conversion all hide per-segment conditionals under one marginal

*Example (italic):* An afternoon-only barista who trusts the 60% overall figure brews twice the hot coffee that the 30% afternoon conditional supports.

**Key point:** A marginal is an average over segments; whenever segments differ, report the conditional per segment, not the single marginal.

### Visualization (canvas `c3`, 720×300)

Three 100%-stacked bars comparing the hot/iced split by segment against the all-day marginal.

- **Title (bold 15px, `#1a5276`, top center):** "Share of Hot Drinks: Morning vs Afternoon vs All Day".
- **Data:** morning 90 hot / 10 iced (of 100); afternoon 30 hot / 70 iced (of 100); all day 120 hot / 80 iced (of 200).
- **Bars:** three vertical 100%-stacked bars, width 90px, centered at x=150, x=350, x=550; baseline y=250, full height 170 (= 100%); hot segment on bottom fill `rgba(217,89,38,0.55)`, iced segment on top fill `rgba(42,120,214,0.5)`.
- **Segment labels (bold 13px, inside each segment, white or `#444` for contrast):** morning "90% hot" / "10% iced"; afternoon "30% hot" / "70% iced"; all day "60% hot" / "40% iced".
- **X-axis labels (bold 12px `#444`, below baseline):** "morning (100)", "afternoon (100)", "all day (200)".
- **Legend (12px, top right):** orange swatch "hot", blue swatch "iced".
- **Annotation (bold 13px green `#008300`, above the all-day bar with a short arrow to it):** "one 60% marginal hides a 90% vs 30% split".
- **Caption (12px `#444`, bottom):** "each bar rescaled to 100% of its own orders".

## Which Way Does the Bar Point?

**Tags:** `common mistake` (red), `direction` (orange)

- **Two directions** — P(hot given morning) and P(morning given hot) share numerator 90 but differ
- **First one** — among the 100 morning orders, 90 are hot: P(hot given morning) = 90%
- **Second one** — among the 120 hot orders, 90 are morning: P(morning given hot) = 75%
- **Why they differ** — the denominators are different crowds: 100 morning orders vs 120 hot ones
- **Famous version** — mixing up P(disease given positive test) with P(positive given disease)

*Example (italic):* "90% of morning orders are hot" does not mean "90% of hot orders happen in the morning" — that figure is 75%.

**Common mistake:** Swapping the condition. P(A given B) equals P(B given A) only when the two marginals match — here they are 100 and 120, so the answers must differ.

### Visualization (canvas `c4`, 720×300)

Two horizontal denominator bars drawn to the same per-order scale, showing the identical 90 shared orders against two different-sized denominator crowds.

- **Title (bold 15px, `#1a5276`, top center):** "Same 90 Orders, Two Different Questions".
- **Data:** bar A = the 100 morning orders, 90 of them hot; bar B = the 120 hot orders, 90 of them morning; shared scale 4px per order, both bars starting at x=70.
- **Bar A (y=95, 26px tall):** total width 400px (100 orders) with 1px `#999` outline; left 360px (90 orders) filled `rgba(217,89,38,0.55)`, remaining 40px filled `#eceff3`; heading bold 12px `#444` above: "the 100 morning orders"; bold 13px orange `#d95926` label right of the bar: "P(hot | morning) = 90/100 = 90%".
- **Bar B (y=185, 26px tall):** total width 480px (120 orders), same outline; left 360px (90 orders) filled `rgba(42,120,214,0.55)`, remaining 120px filled `#eceff3`; heading "the 120 hot orders"; bold 13px blue `#2a78d6` label: "P(morning | hot) = 90/120 = 75%".
- **Shared-orders note (bold 12px ink `#1a5276`, between the bars with thin connector lines to both shaded regions):** "the same 90 hot-morning orders".
- **Takeaway (bold 13px magenta `#d55181`, centered y=280):** "same numerator, different crowd in the denominator — never swap the condition".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays/counts — no `Math.random()`. In regenerated HTML, any card links would use `.html` extensions (this page has no links).
