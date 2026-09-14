# Memoization & Lazy Initialization

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Memoization & Lazy Initialization

**Subtitle:** Compute once, on first use — delay expensive work until someone actually asks for it, save the answer, and hand back the saved copy every time after

## The Dashboard That Computes on First Click

**Tags:** `core idea` (blue), `compute once` (green), `on first use` (orange)

- **The shop** — a coffee shop's dashboard has 8 reports, each built by scanning a 12,000-row orders table
- **The cost** — building any one report takes 3 seconds of number-crunching
- **Lazy init** — the dashboard opens instantly at 6am; no report is computed until its tab is clicked
- **Memoization** — the first click on "average order" pays the 3 seconds, then stores the result
- **The reuse** — every later click on that tab returns the stored $4.20 with no recomputation

*Example (italic):* The manager clicks "average order" at 7:05am, waits 3 seconds, sees $4.20 — and every click after that shows $4.20 instantly from the cache.

**Key point:** Lazy initialization decides *when* to compute (on first use, not at startup); memoization decides *how often* (once) — together: compute once, on first use.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the first click pays the full compute path; every later click short-circuits straight from the cache.

- **Title (bold 15px, `#1a5276`, top center):** "First Click Pays 3 Seconds — Every Later Click Pays Nothing".
- **Row 1 (y=95), label 12px `#444` at x=20:** "first click"; blue `#2a78d6` rounded box at x=130 labeled "orders table 12,000 rows" (12px), 3px arrow to an orange `#d95926` box at x=330 labeled "compute avg — 3 s", 3px arrow to a green `#008300` box at x=530 labeled "cache: $4.20 → screen".
- **Row 2 (y=205), label:** "every later click"; green box at x=130 labeled "cache: $4.20", 3px arrow straight to a blue box at x=530 labeled "screen — instant", with bold 12px green "✓ no recompute" above the arrow midpoint.
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the 3-second scan happens exactly once, and only if someone asks".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Twelve Views, Only Three Computations

**Tags:** `worked example` (blue), `cache hits` (green)

- **The day** — staff open reports 12 times before lunch, but only 3 distinct ones: A, B, C
- **The order** — the click sequence is A, A, B, A, C, B, B, A, C, A, B, C
- **The latencies** — per-view seconds: 3, 0, 3, 0, 3, 0, 0, 0, 0, 0, 0, 0 (a spike only on each first view)
- **Hand-check** — 3 first views × 3 s = 9 s of compute; without memoization it is 12 × 3 s = 36 s
- **Full day** — 40 views of the same 3 reports: still 9 s memoized vs 120 s recomputing every time

*Example (italic):* View 4 is report A again — the cache answers in 0 seconds, so the running compute total stays at 6 s instead of climbing to 12 s.

**Key point:** Memoized cost scales with the number of *distinct* inputs (3 reports = 9 s), not the number of calls (12 views, or 40 — the compute bill is the same 9 s).

### Visualization (canvas `c2`, 720×300)

Bar chart of the 12 views in click order: tall orange bars where a report is computed for the first time, tiny green stubs for cache hits.

- **Title (bold 15px, `#1a5276`, top center):** "12 Views, 3 Computations: Spikes Only on First Sight".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = views 1–12 with 12px `#444` labels "A A B A C B B A C A B C" under the bars; y = seconds 0 to 3, gridlines `#e5e9ef` at 1 and 2, 12px `#444` tick labels.
- **Bars:** 12 bars, 30px wide, spaced 50px apart starting x=75; heights from seconds `[3, 0, 3, 0, 3, 0, 0, 0, 0, 0, 0, 0]` (3 s = 180px); computed views (1, 3, 5) solid orange `#d95926`; cache hits drawn as 4px-tall green `#008300` stubs on the baseline.
- **Labels:** bold 12px orange "compute 3 s" above bar 1; bold 12px green "cache hits" centered over the stubs around view 8.
- **Annotation (bold 13px ink `#1a5276`, upper right near y=70):** "total compute: 9 s memoized vs 36 s recomputing".
- **Caption (12px `#444`, bottom right):** "view sequence and timings illustrative".

## From lru_cache to Lazy-Loaded Models

**Tags:** `where it's used` (blue), `data science` (green)

- **One decorator** — Python's `functools.lru_cache` memoizes a pure function in a single line
- **Model loading** — serving code loads a large model on the first predict call, not at process start
- **Lazy frames** — Spark and Polars build a query plan lazily and compute only when results are asked for
- **Cached property** — `functools.cached_property` computes an object attribute once, on first access
- **Feature reuse** — memoizing per-user feature vectors avoids recomputing them for every model in a batch

*Example (italic):* A service that eagerly built all 8 coffee-shop reports at open would spend 24 s before showing anything; lazy + memoized, the whole day costs 9 s.

**Key point:** The pattern shows up wherever a result is expensive, asked for repeatedly, and not always needed — pay on first use, then read the cached copy.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: total compute seconds for the coffee-shop day under three strategies — recompute every view, eager-at-startup, lazy + memoized.

- **Title (bold 15px, `#1a5276`, top center):** "One Day of Dashboard Views: 120 s vs 24 s vs 9 s".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440 (120 s = 440px, so ~3.67 px per second); left-aligned 12px `#444` row labels at x=20.
- **Rows (top to bottom at y = 80, 145, 210):**
  - "no cache — recompute all 40 views": magenta `#d55181` bar width 440, 11px label "120 s" at bar end
  - "eager — all 8 reports at open": blue `#2a78d6` bar width 88, with an orange `#d95926` overlay segment width 55 at its right end, 11px label "24 s (15 s on 5 reports nobody opened)"
  - "lazy + memoized — 3 first views": green `#008300` bar width 33, 11px label "9 s"
- **Bar style:** 22px tall, fills solid at 0.85 alpha, 4px radius.
- **Annotation (bold 13px green `#008300`, right side near y=250):** "13× less compute, and nothing built that nobody asked for".
- **Caption (12px `#444`, bottom right):** "seconds from the worked example; illustrative".

## The Cache That Still Shows 7am's Number

**Tags:** `common mistake` (red), `stale cache` (orange)

- **The trap** — memoization assumes the answer never changes; the orders table grows all day
- **The drift** — $4.20 cached at 7:05am is wrong by lunch, when sandwiches push the true average to $6.80
- **The symptom** — the dashboard is fast and confidently displays a number 62% off
- **The fix** — invalidate the cache when new orders land, or give entries a time-to-live
- **The rule** — memoize pure functions of their inputs; anything reading changing data needs an expiry plan

*Example (italic):* At 12:30pm the tab still shows the cached $4.20 while the true average order is $6.80 — fast, stale, and trusted because it loaded instantly.

**Common mistake:** Treating memoization as free speed. Caching a value that depends on changing data trades a 3-second wait for a silently wrong answer — cache invalidation is the real design work.

### Visualization (canvas `c4`, 720×300)

Line chart across the morning: the true average order value drifts upward while the cached value stays flat at the 7am number; the gap is the error.

- **Title (bold 15px, `#1a5276`, top center):** "True Average Climbs, Cached Value Sleeps at $4.20".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "7am" to "2pm" with 12px `#444` tick labels each hour (8 points, ~85px apart); y = average order $4 to $7, gridlines `#e5e9ef` at $5 and $6 ($4 → y=245, $7 → y=65, 60px per dollar).
- **True line:** green `#008300` 3px line through hours `[7, 8, 9, 10, 11, 12, 13, 14]`, dollars `[4.20, 4.35, 4.60, 5.10, 5.90, 6.80, 6.50, 6.20]`.
- **Cached line:** red `#e74c3c` 3px dashed (dash 6/4) flat line at $4.20 across the full width, 12px red label "cached at 7:05am" near its left end.
- **Gap marker:** vertical dashed `#6b7280` (dash 4/3) segment at noon between $4.20 and $6.80, bold 12px `#6b7280` label "$2.60 stale gap".
- **Annotation (bold 13px red `#e74c3c`, near 1pm, y=64):** "fast answer, wrong by 62%".
- **Caption (12px `#444`, bottom right):** "order values illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the coffee-shop numbers are invented and labeled illustrative — 8 reports × 3 s each; view latencies `[3,0,3,0,3,0,0,0,0,0,0,0]` for sequence A,A,B,A,C,B,B,A,C,A,B,C; day totals 120 / 24 / 9 s; average-order drift `[4.20,4.35,4.60,5.10,5.90,6.80,6.50,6.20]` over hours 7–14 with the cache flat at 4.20.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
