# Dynamic Time Warping

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Dynamic Time Warping

**Subtitle:** Two curves can trace the same shape at different speeds — DTW stretches and squeezes the time axis so the shapes line up, then measures how far apart they really are

## Two Mornings, Same Rush, Different Clock

**Tags:** `core idea` (blue), `sequence alignment` (green), `elastic matching` (orange)

- **The shop** — a coffee shop logs cups per hour, 7am–1pm; Monday sells 10, 30, 60, 40, 20, 15, 10
- **Tuesday** — the exact same rush arrives one hour late: 10, 12, 30, 60, 40, 20, 12
- **Rigid pairing** — comparing hour-by-hour stacks up absolute differences totalling 95 cups
- **Elastic pairing** — let Monday 8am match Tuesday 9am, peak to peak; the total drops to 7 cups
- **Definition (after example)** — DTW is the smallest total gap over all order-preserving pairings

*Example (italic):* Hour-by-hour, Monday 9am (60 cups) is compared to Tuesday 9am (30 cups) — a 30-cup "error" that vanishes once the peaks are matched.

**Key point:** DTW compares shapes, not clocks. If two sequences differ only in timing, DTW reports them as nearly identical; a rigid hour-by-hour comparison reports them as very different.

### Visualization (canvas `c1`, 720×300)

Dual-panel line chart: the same two daily curves paired rigidly (left) and paired by DTW (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Monday vs Tuesday Cups: Rigid Pairing vs DTW Pairing".
- **Data:** hour labels `7, 8, 9, 10, 11, 12, 1`; Monday `[10, 30, 60, 40, 20, 15, 10]`; Tuesday `[10, 12, 30, 60, 40, 20, 12]`.
- **Left panel (rigid):** axis origin x=55, width 280, baseline y=240, chart height 175, y scale 0–70; Monday as blue `#2a78d6` 3px line with 4px dots, Tuesday as orange `#d95926` 3px line with 4px dots; grey `#bdc3c7` 1.5px vertical connectors between same-hour points; hour labels 12px `#444` below baseline; magenta `#d55181` bold 12px annotation near the 9am gap "peak vs shoulder: gap 30"; caption 12px `#444` "hour-by-hour: total gap 95 cups".
- **Right panel (DTW):** axis origin x=400, width 280, same baseline/height and lines; grey connectors follow the DTW pairing `(7,7) (7,8) (8,9) (9,10) (10,11) (11,12) (12,1) (1,1)` (Monday hour, Tuesday hour), so most connectors slant one step right; green `#008300` bold 13px annotation "peaks matched: total gap 7 cups"; caption "DTW pairing: stretch, don't force".
- **Legend (top right of each panel, 12px):** blue square "Mon", orange square "Tue".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Warping Five Points by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Tiny series** — A = 1, 3, 5, 3, 1 and B = 1, 1, 3, 5, 3: same bump, B lags one step
- **Cell cost** — each grid cell holds |a − b| for one pairing, e.g. A's 5 vs B's 3 costs 2
- **Fill rule** — running total = own cost + cheapest of the three neighbors: left, below, diagonal
- **Read the corner** — the top-right cell holds the DTW distance: 2 for this pair
- **Compare** — the rigid position-by-position total is 0+2+2+2+2 = 8, four times worse

*Example (italic):* The cheapest path pairs A's peak 5 with B's peak 5 (cost 0) instead of A's 5 with B's 3 (cost 2) — the grid finds that swap automatically.

**Key point:** DTW is a shortest-path problem on a grid: every monotone staircase from corner to corner is one legal alignment, and the running-total rule finds the cheapest one without trying them all.

### Visualization (canvas `c2`, 720×300)

Cost grid: a 5×5 table of running totals with the optimal warping path highlighted, sequence A up the left side and sequence B along the bottom.

- **Title (bold 15px, `#1a5276`, top center):** "The DTW Grid for A = 1,3,5,3,1 and B = 1,1,3,5,3".
- **Grid geometry:** 5 columns × 5 rows of 44px cells, grid origin bottom-left at x=200, baseline y=255; 1px `#e5e9ef` cell borders.
- **Axis labels:** B values `1, 1, 3, 5, 3` centered 12px `#444` below each column; A values `1, 3, 5, 3, 1` bottom row upward, 12px `#444` left of each row; axis captions bold 12px ink `#1a5276`: "B (lags) →" below, "A ↑" rotated or stacked at left.
- **Cell values (running totals, rows bottom→top for A = 1, 3, 5, 3, 1):** row A=1: `[0, 0, 2, 6, 8]`; row A=3: `[2, 2, 0, 2, 2]`; row A=5: `[6, 6, 2, 0, 2]`; row A=3: `[8, 8, 2, 2, 0]`; row A=1: `[8, 8, 4, 6, 2]`; values bold 13px, `#2c3e50` on plain cells.
- **Path highlight:** cells (row, col) counted from bottom-left = (1,1), (1,2), (2,3), (3,4), (4,5), (5,5) filled `rgba(0,131,0,0.18)` with 2px `#008300` border and green values; the corner cell (5,5) additionally gets a 3px `#008300` border.
- **Annotations:** green bold 13px arrow-note at top-right cell "DTW distance = 2"; magenta `#d55181` bold 12px note right of the grid, two lines: "rigid diagonal total = 8" / "warped path total = 2"; grey 11px note below-right "each cell = |a − b| + min(left, below, diagonal)".

## Finding the Shifted Twin

**Tags:** `where it's used` (blue), `clustering` (orange)

- **Three days** — Monday 10, 30, 60, 40, 20, 15, 10; Tuesday (rush 1h late); a decline day 40, 35, 30, 25, 20, 15, 10
- **Rigid distance** — Monday–Tuesday scores 95 but Monday–decline scores 80: the wrong twin wins
- **DTW distance** — Monday–Tuesday drops to 7 while Monday–decline stays at 75: shape wins
- **Where it appears** — clustering daily curves, matching spoken words, heartbeats, gestures, steps
- **Why it matters** — any speed or phase wobble makes rigid distance cluster by timing, not shape

*Example (italic):* A k-means run on rigid distances files Tuesday's shifted rush next to the decline day; the same run on DTW distances groups the two rush days together.

**Key point:** Whenever the "same event" can arrive early or late — rushes, heartbeats, spoken syllables — rigid point-by-point distance picks the wrong neighbors, and DTW is the standard fix.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart comparing rigid vs DTW distance for the two candidate pairs, with three small inset sparklines of the day shapes above the bars.

- **Title (bold 15px, `#1a5276`, top center):** "Which Day Is Monday's Twin? Rigid Distance vs DTW".
- **Data:** pairs "Mon vs Tue (shifted rush)" and "Mon vs Decline day"; rigid distances `[95, 80]`; DTW distances `[7, 75]`.
- **Sparklines (top strip, y=45–85):** three 90px-wide mini polylines, 2px, labeled 11px `#444` below: Monday `[10,30,60,40,20,15,10]` in blue `#2a78d6` at x=80, Tuesday `[10,12,30,60,40,20,12]` in green `#008300` at x=310, Decline `[40,35,30,25,20,15,10]` in orange `#d95926` at x=540.
- **Bars:** axis origin x=90, baseline y=250, chart height 130, y scale 0–100; two groups centered at x=250 and x=520; in each group a rigid bar fill `rgba(42,120,214,0.45)` and a DTW bar fill `rgba(0,131,0,0.4)`, each 60px wide with 12px gap; bold 13px value labels above each bar (95, 7, 80, 75); group labels 12px `#444` below baseline.
- **Annotations:** magenta `#d55181` bold 13px above the first group "rigid says Tuesday is FAR (95)"; green `#008300` bold 13px beside its DTW bar "DTW says twin (7)".
- **Legend (top right, 12px):** blue square "rigid, hour-by-hour", green square "DTW".
- **Caption (12px `#444`, bottom center):** "illustrative cups-per-hour data; lower = more similar".

## When Warping Goes Too Far

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Only time bends** — DTW fixes when, not how much: a 60-cup peak vs a 120-cup peak stays far apart
- **Normalize first** — z-score each series before DTW, or level and scale differences dominate
- **Over-warping** — unconstrained DTW can pin one point to many, matching noise to noise
- **Use a window** — a band that caps the shift (say ±2 hours) blocks absurd alignments and runs faster
- **Not a metric** — DTW breaks the triangle inequality, so metric-assuming tricks can misbehave

*Example (italic):* Without a window, a one-hour Monday spike matched a three-hour Tuesday spike with distance near 0 — one Monday point had been stretched across Tuesday's entire spike.

**Common mistake:** Treating DTW as a fix for every mismatch. It only bends the time axis — differences in level, scale, or shape pass straight through, and unlimited bending manufactures similarity that is not there.

### Visualization (canvas `c4`, 720×300)

Dual panel: an over-warped alignment where one point absorbs a whole spike (left), and a DTW grid with a warping-window band (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Two Guardrails: Watch for Over-Warping, Add a Window".
- **Left panel (over-warping):** series A `[10, 10, 50, 10, 10]` drawn as a blue `#2a78d6` 3px line with 5px dots along y=95 (x from 60 to 320); series B `[10, 50, 50, 50, 10]` as an orange `#d95926` 3px line with 5px dots along y=205; grey `#bdc3c7` 1.5px connectors for the pairing (1,1), (2,1), (3,2), (3,3), (3,4), (4,5), (5,5) so A's single 50 fans out to B's three 50s; magenta `#d55181` bold 12px annotation with arrow to the fan "1 point stretched over 3 — distance 0, similarity fake"; captions 11px `#444` "A: one-hour spike" and "B: three-hour spike".
- **Right panel (window):** a 7×7 grid of 24px cells, origin bottom-left x=430, baseline y=250; cells with |row − col| ≤ 2 filled `rgba(25,158,112,0.25)` (aqua band), cells outside filled `rgba(107,114,128,0.12)` (grey, forbidden); dashed `#1a5276` (dash 4/3) diagonal line corner to corner; aqua `#199e70` bold 12px label above "allowed: shift ≤ 2 steps"; grey `#6b7280` 11px label at top-left corner region "blocked"; caption 12px `#444` below "a Sakoe–Chiba band keeps warping honest".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays — no `Math.random()`. The DTW grid values, path cells, and all distances (95, 7, 8, 2, 80, 75) are precomputed above and must be drawn exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
