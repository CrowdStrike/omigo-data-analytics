# Natural Experiments

**Page type:** detail page (tutorial page: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%, tag pills above bullets)
**HTML title tag:** Natural Experiments

**Subtitle:** Sometimes the world randomizes for you — an accident splits people into treated and untreated groups you never designed.

## The rollout quirk: one city's fee changed a month early

Tags: `core idea` (blue), `lucky accident` (green)

- **The plan** — a delivery app raises its fee from $2 to $3 everywhere in April
- **The quirk** — a config mistake ships the new fee to City A a month early, in March
- **The gift** — for one month, City A lives with the new fee while City B lives without it
- **Why it works** — the quirk had nothing to do with either city's customers or demand
- **The comparison** — City B's Feb→March change shows how A would have moved without the fee

*Example:* Nobody designed an experiment — a deploy script did, and it split cities as blindly as a coin flip.

**Key point:** A natural experiment is an accident that assigns treatment for reasons unrelated to the outcome. The analysis is yours; the randomization was free.

### Visualization (canvas `c1`, 720×300)

Two-line timeline chart: each city's weekly orders drop the month its fee changes.

- **Title (bold 16px, ink `#1a5276`, top center):** "Each City Drops the Month Its Fee Changes".
- **Axes:** L-shaped `#999` axes; padding top 55, bottom 50, left 75, right 150; x = months Dec–Apr (5 points), y range 8,000–10,400.
- **Series (line width 3, dots radius 4.5):** City A blue `#2a78d6`: `[9900, 9950, 10000, 8800, 8850]`; City B violet `#4a3aa7`: `[9400, 9450, 9500, 9600, 8400]`.
- **Fee-change markers:** vertical dashed (dash 5/4, width 1.5) lines: orange `#d95926` at Mar with bold orange 12px label above "A's fee: $2 → $3"; aqua `#199e70` at Apr with bold aqua 12px label above "B's fee: $2 → $3".
- **Series labels (bold 12px, near the left ends of the lines):** blue "City A (early, by accident)"; violet "City B (on schedule)".
- **Annotation (bold magenta `#d55181` 13px, right side, three lines):** "March: same world," / "different fee —" / "a free experiment".
- 12px month labels below the axis.

## Difference-in-differences on the back of an envelope

Tags: `worked example` (green), `by hand` (blue)

- **City A** — weekly orders: 10,000 in Feb → 8,800 in March; change = −1,200
- **City B** — weekly orders: 9,500 in Feb → 9,600 in March; change = +100
- **B's job** — its +100 captures season, weather, marketing — everything except the fee
- **The subtraction** — fee effect = (−1,200) − (+100) = −1,300 orders (≈ −13%)
- **Two differences** — one across time, one across cities; hence the name

*Example:* Four numbers and two subtractions — the whole estimate fits on a sticky note.

**Key point:** The comparison city removes what would have happened anyway. The key assumption: without the fee change, A and B would have moved in parallel.

### Visualization (canvas `c2`, 720×300)

2×2 grouped bars (Feb/Mar for each city) with the diff-in-diff subtraction written out on the right.

- **Title (bold 16px, ink, top center):** "Two Differences, One Effect".
- **Axes:** horizontal `#999` baseline; padding top 60, bottom 62, left 65, right 250; y scale 0–11,000; bar width 62.
- **Groups (Feb bar at 0.35 alpha, Mar bar at 0.75 alpha, bold 12px value labels above, "Feb"/"Mar" below, bold colored group label, then a bold 13px change line — red `#e74c3c` if negative, green `#008300` if positive):**
  - "City A (fee changed)", blue `#2a78d6`: Feb 10,000, Mar 8,800; "change: −1,200".
  - "City B (unchanged)", violet `#4a3aa7`: Feb 9,500, Mar 9,600; "change: +100".
- **Right-side arithmetic block:** bold ink 14px "fee effect ="; bold red 14px "(−1,200)"; "−" in text color; bold green "(+100)"; a horizontal rule; bold magenta `#d55181` 15px "= −1,300 orders"; mute 12px "≈ −13% of A's volume".

## Why it beats the comparisons you would have made instead

Tags: `where it's used` (blue), `no A/B possible` (orange)

- **Naive option 1** — A's March vs A's Feb: −1,200, but blames the fee for seasonal shifts too
- **Naive option 2** — A vs B in March only: −800, but the cities differed before any fee change
- **Diff-in-diff** — −1,300, using each flawed comparison to cancel the other's flaw
- **When you need this** — prices, laws, outages, city launches: things you cannot A/B test
- **What to hunt for** — staggered rollouts, eligibility cutoffs, config accidents, weather shocks

*Example:* You cannot randomly charge half a city $3 and the other half $2 — but the rollout quirk effectively did.

**Key point:** When randomizing is impossible, the next best thing is finding a split the world already made for arbitrary reasons — then analyzing it like the experiment it accidentally is.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: three estimates of the fee effect side by side.

- **Title (bold 16px, ink, top center):** "Three Ways to Estimate the Fee Effect".
- **Layout:** left label gutter 280px with a vertical `#999` axis line; rows start y=60, row height 60, bar height 26; bar length scaled to |value| with max 1,400; fills at 0.7 alpha; right-aligned bold 12px row labels, bold 13px colored value labels after each bar, mute 12px note under each bar.
- **Rows:**
  - "A: Mar vs Feb (before/after)": −1,200, yellow `#c98500`; note "blames the fee for seasonality too".
  - "A vs B, March only": −800, aqua `#199e70`; note "cities differed before the fee did".
  - "difference-in-differences": −1,300, green `#008300`; note "each comparison cancels the other's flaw".
- **Caption (bold magenta `#d55181` 13px, bottom center):** "orders lost per week — the naive answers bracket the honest one for different wrong reasons".

## The confusion: the accident must really be an accident

Tags: `common mistake` (red), `check the assignment` (orange)

- **The danger** — if A got the fee early BECAUSE it was underperforming, the comparison breaks
- **Then the drop** — mixes the fee effect with the decline that triggered the early rollout
- **The check** — plot both cities BEFORE the change; their trends should run parallel
- **Passed here** — Dec–Feb, both cities drift up ~+50/month; the quirk looks clean
- **Always ask** — who or what decided the timing, and could that reason touch the outcome?

*Example:* "We tried the fee in our weakest market first" is a business decision — and the death of the natural experiment.

**Key point:** Pre-trend plots are the lie detector. If the groups were already diverging before treatment, the "accident" was carrying information about the outcome.

### Visualization (canvas `c4`, 720×300)

Two side-by-side pre-trend panels (clean vs broken), separated by a dashed light-gray (`#bdc3c7`, dash 4/3) vertical divider at x=360.

- **Title (bold 16px, ink, top center):** "Pre-Trends Tell You If the Accident Was Really Random".
- **Each panel:** 260px wide, plot y=60–210, y range 8,200–11,000; x = Dec, Jan, Feb, Mar (12px labels); horizontal `#999` baseline; vertical dashed orange `#d95926` (dash 4/4, width 1.5) fee-change marker between Feb and Mar; City A line blue `#2a78d6` width 3, City B line violet `#4a3aa7` width 3 with data `[9400, 9450, 9500, 9600]` in both panels; bold 13px panel title above, bold 12px verdict line below.
  - **Left panel (x0=55):** title green `#008300` "quirk: parallel before the change"; City A data `[9900, 9950, 10000, 8800]`; verdict green: "clean — B is a fair stand-in for A".
  - **Right panel (x0=410):** title red `#e74c3c` "\"weakest market first\": already falling"; City A data `[10600, 10300, 10000, 8800]`; verdict red: "broken — drop mixes fee + decline".
- **Caption (mute `#6b7280` 12px, bottom center):** "blue: City A    purple: City B    orange dashes: A's fee change".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` in `#666` 0.95rem, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%) holding the canvas.
- **Left column structure:** `.tags` row of colored pill spans (0.72rem, 600 weight, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem) beginning with `<strong>Key point:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal reset; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`; shared `setup(id)` helper with fixed 720×300 logical size that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
