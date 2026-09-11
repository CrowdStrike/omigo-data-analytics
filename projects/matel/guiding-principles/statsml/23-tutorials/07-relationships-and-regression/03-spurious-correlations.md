# Spurious Correlations

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table — text left 50%, canvas right 50%)
**HTML title tag:** Spurious Correlations

**Subtitle:** Test enough pairs of metrics and some will correlate strongly by pure luck — the more you look, the more ghosts you find

## A Dashboard With 1,000 Metrics

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — a company dashboard tracks 1,000 metrics, 30 days of data each
- **Pairs explode** — 1,000 metrics make 499,500 possible metric pairs
- **Pretend none are related** — luck alone still spreads their r values out
- **Most land near 0** — but the spread has tails, and the tails are not empty
- **Thousands of ghosts** — a few thousand unrelated pairs land beyond |r| > 0.5

*Example:* "Signups in Brazil" and "average order weight" hit r = 0.93 — and share nothing at all.

**Core idea:** a strong correlation is only surprising if you looked at ONE pair — across half a million pairs, strong correlations are guaranteed to appear by chance.

### Visualization (canvas `c1`, 720×300)

Histogram of r values across 499,500 unrelated metric pairs, with the tails highlighted red.

- **Title (bold 15px, `#1a5276`, top center):** "r Across 499,500 UNRELATED Metric Pairs (30 days each, illustrative)".
- **Bins (width 0.1, lower edges −0.8 to +0.7):** lo = `[-0.8, -0.7, -0.6, -0.5, -0.4, -0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7]`; counts in thousands of pairs = `[0.05, 0.3, 1.5, 6, 18.8, 43.9, 77, 102.2, 102.2, 77, 43.9, 18.8, 6, 1.5, 0.3, 0.05]` (bell-shaped, illustrative).
- **Axes:** y 0 to 110k (labels "0k"–"100k" every 25k); x tick labels every other bin edge from −0.8 to +0.8 (minus rendered as "−"); L-shaped gray `#999` axes; padding top 50, bottom 58, left 66, right 24. Axis titles 12px `#444`: "correlation r of the pair" (bottom), rotated "number of pairs" (left).
- **Bars:** central bins translucent blue `rgba(42,120,214,0.4)`; tail bins (lower edge ≤ −0.6 or ≥ +0.5) solid red `#e74c3c`; tail bins with counts ≥ 0.3k get red 12px count labels above them (e.g. "1,500", "300").
- **Annotation (red `#e74c3c`, bold 13px, two lines, upper right at ~66% width):** "≈ 3,700 pairs beyond |r| > 0.5" / "— from pure noise".

## Count the Ghosts by Hand

**Tags:** `worked example` (green), `by hand` (blue)

- **Pairs formula** — n metrics give n × (n − 1) / 2 pairs to compare
- **Ten metrics** — 10 × 9 / 2 = 45 pairs, drawn as a triangle grid
- **The luck rate** — say an unrelated pair "looks correlated" 1 time in 20 (5%)
- **Expected ghosts** — 45 × 0.05 ≈ 2 pairs will fool you even here
- **Now scale up** — 100 metrics → ~250 ghosts; 1,000 metrics → ~25,000

*Example:* Even a modest 10-metric dashboard is expected to contain about 2 fake "relationships".

**The math:** ghosts grow with the square of the metric count — doubling your dashboard roughly quadruples the number of accidental correlations.

### Visualization (canvas `c2`, 720×300)

Left: lower-triangle grid of the 45 metric pairs with 2 ghost cells; right: scale-up ladder of three dashboard sizes.

- **Title (bold 15px, `#1a5276`):** "10 Metrics = 45 Pairs — About 2 Are Ghosts".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3) at x=340.
- **Left triangle grid:** 21px cells starting at (78, 52); rows M2–M10 (right-aligned 11px labels), columns M1–M9 (labels below); each of the 45 lower-triangle cells filled translucent blue `rgba(42,120,214,0.25)` except 2 fixed ghost cells filled red `#e74c3c` (row M6/col M3 and row M9/col M7, i.e. grid positions (5,2) and (8,6)).
- **Grid annotations (bold 12px, left-aligned beside the triangle):** blue `#2a78d6` "45 cells = 45 pairs"; red two lines "2 red cells: strong r," / "no real link (5% luck)".
- **Right ladder (heading ink bold 13px "Same 5% luck rate, bigger dashboard:"):** three 310×38 boxes (background `#f8f9fa`, border `#e5e9ef` 1px; last box red `#e74c3c` 2px border) at x=380, stacked from y=80 with 52px spacing. Each row: bold ink metric count, `#444` pairs, bold red ghosts:
  - "10 metrics" / "45 pairs" / "~2 ghosts"
  - "100 metrics" / "4,950 pairs" / "~250 ghosts"
  - "1,000 metrics" / "499,500 pairs" / "~25,000 ghosts"
- **Bottom annotation (red bold 13px at x=380):** "ghosts grow with the SQUARE of the metric count".

## The Ghost That Vanished

**Tags:** `running example` (green), `common mistake` (red)

- **The find** — over 12 months, signups and order weight track with r = 0.93
- **The story writes itself** — someone will invent a reason they "must" be linked
- **The test** — keep watching for 6 fresh months the correlation never saw
- **The collapse** — on the new months, r flips to −0.49
- **Why** — luck made the past match; luck has no reason to repeat

*Example:* Both series simply drifted upward for a year — any two drifting metrics correlate.

**The test that matters:** a real relationship keeps holding on data it has never seen; a lucky one falls apart the moment you stop looking backward.

### Visualization (canvas `c3`, 720×300)

Dual-axis line chart of two series over 18 months; the last 6 months are shaded as fresh data where the correlation collapses.

- **Title (bold 15px, `#1a5276`):** "Daily Signups vs Avg Order Weight — Then 6 Fresh Months".
- **Data (18 monthly points):** signups = `[42, 47, 43, 49, 46, 52, 50, 55, 52, 58, 56, 61, 63, 60, 66, 64, 69, 71]`; weight = `[3.3, 3.4, 3.5, 3.6, 3.4, 3.9, 3.8, 3.9, 4.1, 4.1, 4.0, 4.4, 4.2, 4.0, 4.1, 3.9, 4.0, 3.8]`.
- **Axes:** x labels "m1", "m3", … "m17" (every other month); left y for signups, scale 35–75 (labels 40–70 every 10, blue `#2a78d6`); right y for weight, scale 3.0–4.6 (labels 3.2–4.4 every 0.4, violet `#4a3aa7`); L-shaped gray axes; padding top 50, bottom 50, left 60, right 64.
- **Fresh-months shading:** translucent yellow-orange `rgba(201,133,0,0.10)` rectangle over months ~12.5 onward (the final 6 months).
- **Series:** signups blue `#2a78d6` line width 3; weight violet `#4a3aa7` line width 3. In-plot legend bold 12px upper-left: blue "signups/day", violet "order weight (kg)".
- **Annotations (bold 13px):** green `#008300` "found: r = 0.93" centered near the bottom around month 6; red `#e74c3c` "fresh data: r = −0.49" near the top over month ~15; orange `#d95926` 12px caption below the x-axis at month ~15: "months the \"finding\" never saw".

## Why Dredging Dashboards Finds Ghosts

**Tags:** `where it's used` (orange), `common mistake` (red)

- **Dredging** — scanning every metric pair for "interesting" links is a ghost generator
- **Selection does the lying** — you only report the pairs that passed, never the 499,000 that didn't
- **Hypothesis first** — decide what should correlate and why BEFORE computing r
- **Retest on fresh data** — of 20 lucky pairs found in January, ~1 survives February
- **Discount by search size** — a finding from a big scan needs a much higher bar

*Example:* An analyst who scans 1,000 metrics will always find something to present — that is the problem.

**Common mistake:** treating a correlation you SEARCHED for like one you PREDICTED — the same r = 0.9 is strong evidence in the second case and near-noise in the first.

### Visualization (canvas `c4`, 720×300)

Grid of 20 circles (the January "discoveries") retested in February: 19 crossed out, 1 checkmarked survivor.

- **Title (bold 15px, `#1a5276`):** "Retest the 20 \"Discoveries\" on February's Data".
- **Layout:** 20 circles (radius 17) in 2 rows of 10, starting at (66, 60), 66px horizontal spacing, 78px between rows.
- **Failures (19 circles):** fill `rgba(107,114,128,0.12)`, muted `#6b7280` 1.2px outline, red `#e74c3c` X mark (width 2) inside.
- **Survivor (1 circle, index 13 — row 2, 4th position):** fill `rgba(0,131,0,0.15)`, green `#008300` 2.5px outline, green checkmark inside.
- **Captions (centered):** `#444` 13px "each circle = one metric pair that looked strongly correlated in January"; red bold 14px "19 of 20 fail the February retest — they were luck"; green bold 14px "luck doesn't repeat; real signals do".

## Regeneration instructions

- **Template:** tutorials topic-page layout. Page: `<h1>` + `.subtitle`, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, bottom border 2px solid `#2980b9`) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%) holding one canvas.
- **Text column structure per section:** `.tags` row of colored pill spans (0.72rem, 600 weight, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (`li b` in `#1a5276`); one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) starting with a `<strong>` label.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each declared 720×300 intrinsic; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Hardcode all data arrays (no `Math.random()`); illustrative data is labeled as such in chart titles. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (tutorials `P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette accents: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange (red `#e74c3c` is used directly in these charts for ghost/failure highlights).
- In regenerated HTML, any card links use `.html` extensions (this page has none).
