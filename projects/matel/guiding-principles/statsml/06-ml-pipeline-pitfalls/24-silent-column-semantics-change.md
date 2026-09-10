# Pitfall: Silent Column Semantics Change

**Page type:** detail page (card-sections with h2 headers, two-column layout table per section: text left 45%, canvas right 55%)
**HTML title tag:** Silent Column Semantics Change

**Subtitle:** Column name stays the same but meaning or encoding changes over time

## The Problem

Tags: `the trap` (red), `semantics` (blue)

- **Same name, new meaning** — upstream changes redefine a column without renaming it
- **Definition change** — a revenue column switches from gross to net, measuring a new quantity
- **Encoding change** — country codes move from ISO-2 to ISO-3, making every category unseen
- **Type change** — a boolean flips from 1/0 integers to true/false strings, breaking numerics
- **Timezone change** — timestamps shift from UTC to local, moving every time-derived feature

*Example:* Upstream switches "days_active" from calendar to business days, and the churn model underestimates activity by roughly 28%.

**Impact:** The column still exists and holds plausible values, so predictions go wrong without any error being raised.

### Visualization (canvas `c1`, 720×300)

Timeline diagram showing semantic drift between training and production periods.

- **Title (bold 14px, top center, `#1a5276`):** "Timeline: Same Column Name, Different Meanings".
- **Layout:** timeline band spans left=60 to right=660, top=80 to bottom=260; horizontal gray (`#999`, width 2) axis line through the vertical middle.
- **Training block:** rectangle from x=60, width 250, full band height; fill `rgba(26,82,118,0.3)`, stroke `#1a5276` width 2. Labels centered inside near top: bold 12px "TRAINING", then 11px "revenue = gross".
- **Change point:** vertical dashed red line (`#e74c3c`, width 3, dash 8/4) at x=310 extending 10px above/below the band; above it two lines of bold 11px red text: "SEMANTIC" / "CHANGE".
- **Production block:** rectangle from x=310 to x=660, full band height; fill `rgba(231,76,60,0.3)`, stroke `#e74c3c` width 2. Labels centered inside: bold 12px "PRODUCTION", then 11px "revenue = net (changed!)" and "Model still expects gross".
- **Timeline labels (10px gray `#666`, below the mid axis):** "Jan 2025" (left area), "Jun 2025" (at change point), "Dec 2025" (right area).
- **Bottom annotation (bold 10px red, centered):** "Silent failure: column exists, values plausible, predictions wrong".

## Why It Happens

Tags: `root cause` (orange), `contracts` (blue)

- **Definitions drift** — teams rename columns rarely but change their meanings frequently
- **Unversioned improvement** — an upstream team improves a metric without versioning it
- **No contracts** — no schema registry or data contract blocks the change from shipping
- **Name as contract** — the name is treated as the interface, but the real contract is the meaning
- **Buried changes** — the change hides in ETL code and is never communicated to the ML team

*Example:* An e-commerce site changes "price" from pre-tax to post-tax, shifting values about 10% higher, and the model underpredicts demand.

**Root Cause:** Definitions evolve faster than names, so without explicit data contracts semantic changes propagate silently.

### Visualization (canvas `c2`, 720×300)

Before/after table comparison of a categorical encoding change.

- **Title (bold 14px, top center, `#1a5276`):** "Encoding Change Example: Country Code Format".
- **Two mini-tables**, each 280px wide, rows 28px tall, starting at y=70; left table at x=60, right table at x=380.
- **Left table (BEFORE):** heading "BEFORE (ISO-2)" in bold 13px green `#27ae60`; header row filled `#1a5276` with white bold 11px text "country_code"; data rows (alternating `#f8f9fa` / white, border `#e0e0e0`): `US — 1200 rows`, `GB — 450 rows`, `DE — 380 rows`, `CA — 290 rows` (code left-aligned, count right-aligned, 11px `#2c3e50`). Caption below in 10px green: "Model trained on this".
- **Right table (AFTER):** heading "AFTER (ISO-3)" in bold 13px red `#e74c3c`; same header row "country_code"; data rows (alternating `#ffe5e5` / white): `USA — 1200 rows`, `GBR — 450 rows`, `DEU — 380 rows`, `CAN — 290 rows`. Caption below in 10px red: "Production sees this (all unknown!)".
- **Arrow:** solid red (`#e74c3c`, width 2) horizontal arrow with filled triangular head pointing from the left table to the right table at mid-row height; above it bold 10px red text centered between tables: "Model treats all as UNKNOWN".

## The Correct Approach

Tags: `the fix` (green), `versioning` (blue)

- **Treat as breaking** — semantic changes require explicit versioning, like any API break
- **Schema registry** — keep semantic descriptions and version history for every column
- **Distribution checks** — track mean, variance, percentiles, cardinality, and null rates
- **Alerting** — flag shifts beyond expected bounds; two sigma is a starting point, not a rule
- **Versioned columns** — introduce a new named column when the meaning must change

*Example:* The team creates "revenue_net", deprecates "revenue_gross" with a 6-month sunset, and the ML team retrains before cutover.

**Fix:** Version columns when semantics change and run distribution checks on every pipeline run, treating semantic drift as an incident that requires a retrain.

### Visualization (canvas `c3`, 720×300)

Left: mean-over-time drift chart with alert zone; right: monitoring checklist.

- **Title (bold 14px, top center, `#1a5276`):** "Detection via Distribution Monitoring".
- **Chart area:** left=60, right=340, top=70, bottom=240; gray (`#999`) L-shaped axes. X-axis label "Time →" centered below (11px `#666`); rotated y-axis label "Mean Value".
- **Baseline series:** green (`#27ae60`, width 2.5) wavy line over the first half of the x-range at y ≈ bottom−80 with ±5px sine wobble (`sin(i*0.5)`, i = 0..50).
- **Shifted series:** red (`#e74c3c`, width 2.5) wavy line over the second half at y ≈ bottom−130 with the same ±5px wobble (i = 50..100) — a visible upward level shift.
- **Alert zone:** right half of the chart area shaded `rgba(231,76,60,0.1)` with dashed red border (`#e74c3c`, width 2, dash 5/3); bold 10px red label near its top: "ALERT: Distribution shift".
- **Series labels (10px):** green "Baseline" over the first-half line; red "Semantic change" over the second-half line.
- **Checklist (right side, starting x=400, y=70):** heading bold 12px `#1a5276` "Monitor These Stats:", then six items, each with a green (`#27ae60`) checkbox square (14×14) containing a green checkmark, item text 11px `#2c3e50`, 22px row spacing:
  - Mean / Median
  - Std deviation
  - Percentiles (p5, p95)
  - Unique value count
  - Null percentage
  - Value range (min/max)
- **Footer under checklist (bold 10px orange `#e67e22`):** "Alert on shift (e.g. 2 sigma — tune per feature)".

## Regeneration instructions

- **Layout:** repeated `.card-section` blocks, one per section. Each has an `<h2>` (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) followed by a `table.layout` (full width, border-collapse) with a single `<tr>`: left `td.text-col` (45%) containing `.tags` pills, a `<ul>` of labeled bullets, a `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) containing one `<canvas width="720" height="300">`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `2px solid #2980b9` bottom border. `.subtitle` `#666` 0.95rem. `ul` 0.92rem with `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; intrinsic size 720×300, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
