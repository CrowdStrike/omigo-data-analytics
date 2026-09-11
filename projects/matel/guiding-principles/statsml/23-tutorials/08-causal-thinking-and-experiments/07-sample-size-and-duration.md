# Sample Size & Duration

**Page type:** detail page (tutorial layout: h2 card-sections, two-column table 50% text / 50% viz per section)
**HTML title tag:** Sample Size &amp; Duration

**Subtitle:** Decide how many users you need before you start — on small samples, chance wobble can fake or hide a real lift

## How Much Does 3% Wobble on Its Own?

Tags: `core idea` (blue), `running example` (green)

- **The target** — detect a +0.3 point lift on the checkout page: 3.0% → 3.3%
- **Chance wobble** — 10,000 users at a true 3% rate: expect 300 buyers, get 265-335 routinely
- **As a rate** — that is roughly ±0.34 points of wobble with nothing changed at all
- **The problem** — the wobble (±0.34pt) is bigger than the lift we hunt (+0.3pt)
- **The fix** — more users: wobble shrinks with the square root of the sample size

*Example (italic):* Twenty groups of 10,000 saw the SAME blue button; their rates ranged from 2.66% to 3.35%.

**Key point:** If chance alone produces ±0.34 points, a +0.3 point reading from 10,000 users proves nothing yet.

### Visualization (canvas `c1`, 720×300)

Dot plot: 20 identical groups' conversion rates scattering around the true 3.0% rate.

- **Title (bold 16px, `#1a5276`, centered, y=24):** "20 Groups of 10,000 — Same Blue Button, Different Rates".
- **Data (hardcoded illustrative rates, one dot per group, evenly spaced along x):** `[3.12, 2.81, 3.05, 2.66, 3.35, 2.94, 3.22, 2.88, 3.02, 3.18, 2.73, 3.09, 2.97, 3.28, 2.85, 3.06, 3.31, 2.79, 2.99, 3.15]`.
- **Axes:** y from 2.5% to 3.5%, mute 12px tick labels at 2.6, 2.8, 3.0, 3.2, 3.4 ("%"-suffixed); padding top 56 / bottom 56 / left 64 / right 30; `#999` L-axes.
- **Chance band:** filled rgba(42,120,214,0.10) rectangle spanning 2.66% to 3.34%.
- **True-rate line:** dashed ink (`#1a5276`, dash 6/4, 1.5px) horizontal line at 3.0%, labeled bold ink 12px "true rate 3.0%" above its left end.
- **Dots:** blue `#2a78d6`, radius 5.
- **Annotations:** bold orange (`#d95926`) 13px centered near the bottom: "spread is chance alone: ±0.34pt at n = 10,000 — bigger than the +0.3pt we hunt"; mute 12px x-axis caption: "group number (each 10,000 users, illustrative draws)".

## The Sample-Size Calculation

Tags: `worked example` (green), `rule of thumb` (blue)

- **The recipe** — n per arm ≈ 16 × p × (1−p) / lift² (standard 80% power, 5% false alarm)
- **Plug in** — 16 × 0.03 × 0.97 = 0.466
- **Divide** — 0.466 / (0.003)² = 0.466 / 0.000009 ≈ 51,700 users per arm
- **Both arms** — about 103,000 visitors total for a fair shot at seeing +0.3pt
- **The shape** — lift² sits in the denominator: chase half the lift, need 4x the users

*Example (italic):* To detect +0.1pt instead of +0.3pt, n jumps from ~52,000 to ~466,000 per arm — 9x.

**Key point:** The calculation happens before launch: rate you have (3%), smallest lift you care about (+0.3pt), out comes n (~52,000 per arm).

### Visualization (canvas `c2`, 720×300)

Bar chart: users needed per arm versus the lift you want to detect (3% base rate).

- **Title (bold 16px, `#1a5276`, centered, y=24):** "Users Needed per Arm vs Lift You Want to Detect (3% base)".
- **Data (n = 16 × 0.03 × 0.97 / lift², in thousands):** lifts `['+0.1pt', '+0.15pt', '+0.2pt', '+0.3pt', '+0.5pt', '+1.0pt']` with n values `[466, 207, 116, 52, 19, 4.7]` labeled `['466k', '207k', '116k', '52k', '19k', '4.7k']`; y-scale max 500 with mute labels "400k" and "200k".
- **Axes:** padding top 56 / bottom 62 / left 64 / right 30; `#999` L-axes; mute 12px lift labels below bars.
- **Bars:** 76px wide; the "+0.3pt" bar highlighted orange `#d95926` at 85% alpha, all others blue `#2a78d6` at 60% alpha; bold 12px value labels above bars (orange for the highlighted one).
- **Annotations:** bold orange 13px above the +0.3pt bar: "our test: ~52k per arm"; bold ink 13px centered near the top: "halve the lift you chase → 4x the users (lift² in the denominator)"; mute 12px x-axis caption: "smallest lift worth detecting".

## Duration: Run Whole Weeks

Tags: `rule of thumb` (blue), `running example` (green)

- **Arithmetic** — 15,000 visitors/day means 7,500 per arm: 51,700 / 7,500 ≈ 6.9 days
- **Round up** — 6.9 days of traffic → run a full 7 days (or 14), never stop at 5
- **Weekly rhythm** — weekdays convert near 2.6%, weekends near 4.1% (illustrative)
- **Partial weeks lie** — stop on a Friday and you mostly measured weekday shoppers
- **Whole weeks** — Monday-to-Sunday blocks give every shopper type its fair share

*Example (italic):* A Tue-Fri test said +0.5pt for green; the full-week rerun said +0.1pt — weekend shoppers disagreed.

**Key point:** Duration = (n per arm ÷ daily visitors per arm), rounded UP to whole weeks.

### Visualization (canvas `c3`, 720×300)

Bar chart: conversion rate by day of week, weekends highlighted, with a Friday cut line.

- **Title (bold 16px, `#1a5276`, centered, y=24):** "Conversion by Day of Week — Weekends Are a Different Crowd".
- **Data (illustrative):** days `['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']` with rates `[2.5, 2.6, 2.6, 2.7, 2.9, 4.2, 4.0]`; y-scale max 5% with mute labels "4%" and "2%".
- **Axes:** padding top 56 / bottom 58 / left 64 / right 30; `#999` L-axes; mute 12px day labels below bars.
- **Bars:** 66px wide, 72% alpha; weekdays blue `#2a78d6`, weekend (Sat, Sun) aqua `#199e70`; bold 12px colored value labels above bars ("2.5%" … "4%").
- **Cut line:** dashed red (`#e74c3c`, dash 6/4, 2px) vertical line between Friday and Saturday; bold red 13px right-aligned labels beside it: "stop here on Friday and" / "weekend shoppers never voted".
- **Caption (mute 12px, bottom center):** "rates illustrative".

## The Early-Stop Temptation

Tags: `common mistake` (red)

- **The scene** — day 4 of 14, the dashboard flashes "significant!" — tempting to stop
- **The trap** — checking daily and stopping at the first green light inflates false wins
- **The rule** — compute n up front, run to n in whole weeks, read the answer once
- **Elsewhere** — peeking and its cousins fill their own pitfalls pages; here: just don't stop early

*Example (italic):* On a no-change A/A test, a team that checked daily "found" a winner by day 5.

**Key point:** The sample size chosen up front is a promise — the test is not done until n is reached.

### Visualization (canvas `c4`, 720×300)

Line chart: the daily p-value of a no-change (A/A) test wandering over 14 days, dipping below 0.05 once.

- **Title (bold 16px, `#1a5276`, centered, y=24):** "A No-Change Test, Checked Daily: the p-value Wanders".
- **Data (p-value by day 1–14):** `[0.61, 0.32, 0.11, 0.04, 0.09, 0.22, 0.16, 0.31, 0.25, 0.44, 0.38, 0.52, 0.47, 0.41]`; y-scale 0 to 0.7 with mute labels "0.6", "0.3", "0.05".
- **Axes:** padding top 56 / bottom 56 / left 64 / right 30; `#999` L-axes; mute 12px day numbers 1–14 under each point.
- **Alpha line:** dashed red (`#e74c3c`, dash 6/4, 1.5px) horizontal line at 0.05.
- **Series:** violet (`#4a3aa7`) 3px connected line; dots radius 4 in violet, except day 4 (p=0.04) which is red and radius 6.
- **Annotations:** bold red 13px left-aligned next to day 4: "day 4: p = 0.04 — "ship it!" would be wrong"; bold violet 13px right-aligned near the last point: "day 14 answer: p = 0.41, nothing there".
- **Caption (mute 12px, bottom center):** "day of test (illustrative A/A run)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). Page: `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, four `.card-section` blocks each with `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with one `<tr>`: `.text-col` (50%) and `.viz-col` (50%) holding a 720×300 canvas.
- **Text column structure:** `.tags` pill row (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem bold pills, 10px radius), `<ul>` (0.92rem) of one-line bullets opening with `<b>` (`#1a5276`), italic `.example` (`#555`, 0.9rem), `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem). Bullets use HTML entities for math symbols (&rarr;, &plusmn;, &asymp;, &times;, &minus;, &sup2;, &divide;).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px.
- **Canvas palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic `width`/`height` attributes (720×300); shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded — no `Math.random()`; invented numbers labeled "illustrative" in captions. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
