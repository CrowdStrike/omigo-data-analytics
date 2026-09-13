# T-Digest

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** T-Digest

**Subtitle:** A t-digest keeps a few tiny summary buckets instead of every number, and can still answer "what's the median delivery time?" or "what's the 95th percentile?" about a stream it never stored

## A Pizza Shop That Can't Keep Every Ticket

**Tags:** `core idea` (blue), `streaming` (green), `centroids` (orange)

- **The shop** — a pizza shop times every delivery all day and wants the median and the slowest 5%
- **The problem** — thousands of tickets pile up; keeping every single time just to sort them is wasteful
- **The trick** — keep a short list of clusters, each just two numbers: an average time and a count
- **Squeeze the middle** — nearby middling times merge into big clusters; 18, 20, 22 min become "20 min × 3"
- **Protect the edges** — the fastest and slowest deliveries stay as tiny clusters, some holding one ticket
- **The name** — this cluster list is a t-digest: coarse in the middle, sharp exactly where percentiles live

*Example (italic):* Twelve deliveries — 14, 16, 18, 20, 22, 22, 24, 26, 28, 30, 38, 52 minutes — collapse into six clusters, and the lone 52-minute disaster keeps its own row.

**Key point:** A t-digest replaces the raw stream with a handful of (average, count) clusters, kept deliberately small near the extremes so tail percentiles stay accurate.

### Visualization (canvas `c1`, 720×300)

Two-row number-line chart: the day's 12 raw delivery times as dots on top, collapsing via connector lines into 6 centroid circles (sized by count) below, on a shared minutes axis.

- **Title (bold 15px, `#1a5276`, top center):** "12 Delivery Times Squeezed Into 6 Clusters".
- **Axis:** horizontal 2px `#999` line at y=250 from x=60 to x=680; x scale = minutes, 10 at x=60 to 55 at x=680; tick labels "10", "15", ..., "55" every 5 minutes (12px `#444`) below the line, axis caption "delivery time (minutes)" 12px `#444` centered at y=290.
- **Raw row (y=105):** twelve 6px blue `#2a78d6` dots at minutes `[14, 16, 18, 20, 22, 22, 24, 26, 28, 30, 38, 52]` (the two 22s stacked, second dot at y=93); 12px `#444` row label "raw tickets" at x=62, y=80.
- **Centroid row (y=195):** six circles at mean minutes `[15, 20, 24, 29, 38, 52]` with counts `[2, 3, 3, 2, 1, 1]`; radius = 5px + 3px per count (so 11, 14, 14, 11, 8, 8); fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` stroke; bold 12px `#1a5276` label under each: "15×2", "20×3", "24×3", "29×2", "38×1", "52×1"; 12px `#444` row label "t-digest clusters" at x=62, y=170.
- **Connectors:** thin 1px `#6b7280` lines from each raw dot down to its cluster circle (14,16→15; 18,20,22→20; 22,24,26→24; 28,30→29; 38→38; 52→52).
- **Annotation (bold 12px orange `#d95926`, near x=470, y=140):** two lines: "middle gets merged," / "tails keep their own rows".
- **Caption (12px `#444`, bottom right):** "illustrative — one small day of deliveries".

## Reading the Median From Six Summary Rows

**Tags:** `worked example` (blue), `interpolation` (green)

- **The list** — six clusters: 15×2, 20×3, 24×3, 29×2, 38×1, 52×1; total count 2+3+3+2+1+1 = 12
- **Mid-ranks** — each average sits at the middle of its ranks: 15 at rank 1.5, 20 at rank 4, 24 at rank 7
- **The target** — the median of 12 deliveries is halfway between rank 6 and rank 7, so rank 6.5
- **Walk and interpolate** — rank 6.5 lands between 20 (rank 4) and 24 (rank 7): 20 + (2.5/3)×4 ≈ 23.3 min
- **Check it** — the true median of the raw list is (22+24)/2 = 23 min; the digest says 23.3, off by 0.3
- **The tail is exact** — the worst delivery, 52 min, is its own cluster, so the maximum has zero error

*Example (italic):* Six rows on a sticky note reproduce the median of twelve deliveries to within a third of a minute — 23.3 estimated versus 23 true.

**Key point:** Percentiles come from walking the clusters' cumulative counts and interpolating between averages — no raw ticket is ever consulted again.

### Visualization (canvas `c2`, 720×300)

Single-panel rank-vs-minutes plot: the six centroids as dots at their mid-ranks joined by a line, with dashed guide lines showing rank 6.5 being read off as 23.3 minutes, and the true median 23 marked.

- **Title (bold 15px, `#1a5276`, top center):** "Reading Rank 6.5 Off the Cluster Line: ≈ 23.3 min".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x axis = rank 0 to 13, tick labels "0", "2", ..., "12" every 2 (12px `#444`), caption "rank (out of 12 deliveries)" 12px `#444` at bottom center; y axis = minutes 0 to 60, tick labels "0", "10", ..., "60" every 10 (12px `#444`), light `#e5e9ef` gridlines at each.
- **Cluster line:** blue `#2a78d6` 3px line through the mid-rank points `(1.5, 15), (4, 20), (7, 24), (9.5, 29), (11, 38), (12, 52)`; 7px blue dots at each; 11px `#444` label near each dot with its cluster ("15×2", "20×3", "24×3", "29×2", "38×1", "52×1"), staggered to avoid overlap.
- **Read-off guides:** vertical dashed orange `#d95926` (dash 4/3) line from the baseline at rank 6.5 up to the cluster line, then horizontal dashed orange line left to the y axis; bold 13px orange label "median rank 6.5 → 23.3 min" just below the elbow.
- **True median marker:** short green `#008300` 3px tick on the y axis at 23, bold 12px green label "true: 23" to its right.
- **Annotation (bold 12px green `#008300`, near rank 8.5, minutes 12):** "off by 0.3 min — from six rows, not twelve tickets".

## A Day of 40,000 Deliveries in 60 Rows

**Tags:** `where it's used` (blue), `memory` (green), `dashboards` (orange)

- **Real scale** — a delivery app logs 40,000 orders a day; a t-digest summarizes them in about 60 clusters
- **Same answers** — the digest reports the median as 23.2 min against a true 23, and p95 as 43.6 against 44
- **Sharp tails** — the smallest clusters sit at the extremes, so p95 and p99 alarms stay accurate
- **One pass** — each new ticket updates the nearest cluster and is thrown away; nothing is ever re-sorted
- **Everywhere** — latency dashboards, load-time monitors, and database engines all lean on this structure

*Example (italic):* A dashboard shows live p50, p95, and p99 delivery times all day while holding 60 little (average, count) pairs instead of 40,000 tickets.

**Key point:** A t-digest turns "store and sort everything" into "keep ~60 rows", and the percentile curve it reports is nearly indistinguishable from the exact one.

### Visualization (canvas `c3`, 720×300)

Single-panel percentile-curve overlay: the exact percentile curve of the full day (green, thick) and the 60-cluster digest's estimate (blue, dashed) lying almost on top of each other, with p50 and p95 called out.

- **Title (bold 15px, `#1a5276`, top center):** "Exact vs T-Digest Percentile Curve — 40,000 Tickets vs 60 Rows".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = percentile 0 to 100, tick labels "0", "25", "50", "75", "95", "100" (12px `#444`), caption "percentile" at bottom center; y = minutes 0 to 60, tick labels "0"–"60" every 10 (12px `#444`), light `#e5e9ef` gridlines.
- **Exact curve:** green `#008300` 4px line through percentile points `[1, 5, 10, 25, 50, 75, 90, 95, 99]` with minutes `[11, 14, 16, 19, 23, 28, 34, 44, 58]`; 12px green label "exact (all 40,000)" near percentile 30, above the curve.
- **Digest curve:** blue `#2a78d6` 2px dashed (dash 6/4) line through the same percentiles with minutes `[11, 14, 16, 19.2, 23.2, 28.3, 34.4, 43.6, 57.5]`; 12px blue label "t-digest (60 rows)" near percentile 65, below the curve.
- **Call-out markers:** vertical dashed `#6b7280` (dash 4/3) lines at percentile 50 and 95 from baseline to the curves; bold 12px `#444` labels "p50: 23 vs 23.2" and "p95: 44 vs 43.6" above each, staggered heights.
- **Annotation (bold 13px violet `#4a3aa7`, near percentile 20, minutes 50):** two lines: "two curves, one shape —" / "the raw data was never kept".
- **Caption (12px `#444`, bottom right):** "illustrative — a simulated day of deliveries".

## You Can't Average Percentiles — But You Can Merge Digests

**Tags:** `common mistake` (red), `merging` (orange)

- **Two stores** — store A has 100 deliveries with p95 = 30 min; store B has 900 with p95 = 50 min
- **The shortcut** — averaging the two p95s gives (30+50)/2 = 40 min, and it is simply wrong
- **Why it fails** — a percentile is a rank in one combined pile, and B's 900 tickets dominate that pile
- **The right way** — merge the two digests' cluster lists into one, then read p95 from it: about 49 min
- **The mistake** — company dashboards that average per-server p95s understate the real tail by minutes

*Example (italic):* The chain's real p95 is about 49 minutes, but the averaged dashboard says 40 — nine minutes of slow deliveries hidden by one innocent-looking mean.

**Common mistake:** Averaging percentiles from separate machines or stores. Percentiles don't average — but t-digests merge, and the merged digest gives the combined percentile correctly.

### Visualization (canvas `c4`, 720×300)

Four-bar chart on a minutes axis: each store's own p95, the wrong averaged answer in red, and the correct merged-digest answer in green.

- **Title (bold 15px, `#1a5276`, top center):** "Combined p95: Averaging Says 40, Merging Digests Says 49".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = minutes 0 to 60, tick labels "0"–"60" every 10 (12px `#444`), light `#e5e9ef` gridlines; no x-axis numbers, bar labels instead.
- **Bars (80px wide, centered at x = 150, 290, 430, 570):** heights for values `[30, 50, 40, 49]` —
  - "store A p95" 30 min, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` stroke;
  - "store B p95" 50 min, same blue style;
  - "average of p95s" 40 min, fill `rgba(231,76,60,0.25)`, 2px `#e74c3c` stroke;
  - "merged digest p95" 49 min, fill `rgba(0,131,0,0.25)`, 2px `#008300` stroke.
- **Bar labels:** two-line 12px `#444` labels under each bar; bold 13px value labels above each bar top ("30", "50", "40", "49") in the bar's stroke color.
- **Count tags:** 11px `#6b7280` labels inside the two blue bars: "100 deliveries", "900 deliveries".
- **Annotation (bold 13px magenta `#d55181`, spanning above bars 3–4 near y=70):** "9 minutes of slow tail hidden by averaging".
- **Caption (12px `#444`, bottom right):** "illustrative — two stores, one chain".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all delivery times, cluster means/counts, mid-rank points, percentile curves, and bar values are the hardcoded arrays above (no randomness); c1/c2 numbers derive from the twelve-value list `[14, 16, 18, 20, 22, 22, 24, 26, 28, 30, 38, 52]`, c3/c4 numbers are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
