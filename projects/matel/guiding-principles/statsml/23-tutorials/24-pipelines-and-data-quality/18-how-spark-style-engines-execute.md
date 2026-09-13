# How Spark-Style Engines Execute

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** How Spark-Style Engines Execute

**Subtitle:** Distributed dataframe engines don't run your code line by line — they collect it into a lazy plan, cut the plan into stages at every shuffle, and the shuffle is where the money goes

## Four Lines of Code, Zero Rows Read

**Tags:** `core idea` (blue), `lazy evaluation` (green), `distributed` (orange)

- **The job** — revenue per city from a 40M-row orders table, stored as 8 partitions of 5M rows each
- **The code** — read the table, filter to year 2025, select (city, revenue), group by city and sum
- **The surprise** — after all four lines run, the engine has read exactly zero rows from disk
- **The plan** — each line only appends a step to a query plan; these steps are called transformations
- **The trigger** — an action (collect, write, count) hands the whole plan to the scheduler at once

*Example (italic):* The read/filter/select/groupBy lines return in milliseconds; the `collect()` on line five is where the cluster finally spins up and the 40M rows get touched.

**Key point:** Transformations are lazy — they build a plan, not results. Nothing executes until an action asks for an answer, which lets the engine see the whole plan before choosing how to run it.

### Visualization (canvas `c1`, 720×300)

Flow diagram: four transformation boxes accumulating into a dashed "plan" region with a running "rows read: 0" counter, then an action box that fires execution.

- **Title (bold 15px, `#1a5276`, top center):** "Transformations Build a Plan — the Action Runs It".
- **Plan region:** dashed 1.5px `#6b7280` rounded rectangle (dash 6/4) from (30, 55) to (560, 150), 12px `#6b7280` label "lazy plan — nothing executed" at its top-left inside edge.
- **Transformation boxes (inside region, y=85, height 42, width 115, 8px radius, fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border, 12px `#2c3e50` centered text):** at x = 45 "read orders", x = 175 "filter year=2025", x = 305 "select city, rev", x = 435 "groupBy city .sum" — 2px `#2a78d6` arrows between consecutive boxes.
- **Counter (bold 13px `#d95926`, at x=45, y=180):** "rows read so far: 0" with a thin `#d95926` underline.
- **Action box:** green fill `rgba(0,131,0,0.12)`, 2px `#008300` border, rounded, at (560, 200) width 130 height 44, bold 12px `#008300` centered text "collect() — ACTION"; 3px `#008300` arrow from the plan region's right edge down into it.
- **Annotation (bold 13px green `#008300`, near x=330, y=235):** "only now do 40M rows get read".
- **Caption (12px `#444`, bottom left):** "row counts illustrative".

## Where Stage 1 Ends: Reshuffling Rows by City

**Tags:** `worked example` (blue), `shuffle` (red), `stages` (green)

- **Narrow steps** — filter and select need only their own partition, so all 8 tasks run independently
- **The wide step** — summing per city needs every "London" row together, but London sits in all 8 partitions
- **The cut** — the planner splits the plan into Stage 1 (scan/filter/select) and Stage 2 (sum) at the groupBy
- **The reshuffle** — each Stage 1 task hashes city into 4 output buckets: hash(city) % 4 picks the target
- **Hand-check** — 8 map tasks × 4 buckets = 32 shuffle blocks; each Stage 2 task fetches its 8 over the network

*Example (italic):* hash("London") % 4 = 2, so every one of the 8 Stage 1 tasks writes its London rows into bucket 2 — Stage 2's task 2 fetches those 8 blocks and is the only task that ever sums London.

**Key point:** A wide dependency (groupBy, join) forces a shuffle: every task repartitions its output by key and ships it across the network, and the plan breaks into a new stage on the far side.

### Visualization (canvas `c2`, 720×300)

Bipartite flow diagram: 8 Stage 1 map tasks on the left, 4 Stage 2 reduce partitions on the right, all 32 shuffle edges drawn faint with the 8 London edges highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "The Shuffle: 8 Map Tasks × 4 Buckets = 32 Blocks Over the Network".
- **Left column:** 8 boxes at x=95, width 110, height 20, at y = 52, 79, 106, 133, 160, 187, 214, 241; fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border, 11px `#2c3e50` text "map task 1" … "map task 8"; bold 12px `#1a5276` column header "Stage 1 (8 tasks)" at (95, 40).
- **Right column:** 4 boxes at x=515, width 130, height 34, at y = 58, 115, 172, 229; fill `rgba(0,131,0,0.12)`, 1.5px `#008300` border, 11px `#2c3e50` two-line text "bucket 0" … "bucket 3" with second line "sum task"; bold 12px `#008300` column header "Stage 2 (4 tasks)" at (515, 40).
- **Edges:** straight 1px `rgba(107,114,128,0.25)` lines from each map box's right-center to each bucket box's left-center (32 total).
- **Highlight:** the 8 edges into bucket 2 redrawn 2.5px `#2a78d6`; bold 12px `#2a78d6` label "all London rows → hash % 4 = 2" at (300, 285).
- **Mid label (12px `#d95926`, rotated 0°, centered at x=360, y=52):** "network".
- **Caption (12px `#444`, bottom right):** "4 reduce partitions illustrative".

## Why the Shuffle Is the Whole Bill

**Tags:** `where it's used` (blue), `job cost` (orange), `pipelining` (green)

- **Free chaining** — narrow ops pipeline: each task filters and selects a row in one pass, no wait, no I/O
- **The heavy part** — the shuffle writes 6M surviving rows to disk, ships them, and re-reads them sorted
- **The split (illustrative)** — Stage 1 pipelined scan takes 12s; the shuffle takes 85s; Stage 2 sums in 3s
- **The share** — 85s of a 100s job is the shuffle: disk write, network transfer, and fetch dominate
- **The instinct** — tuning a Spark-style job usually means shuffling fewer bytes, not computing faster

*Example (italic):* Cutting the 6M shuffled rows in half would save more time than making the 12s scan infinitely fast — the shuffle's 85s is 85% of the 100s job.

**Key point:** Narrow transformations fuse into one pass over each partition, so they're nearly free; the shuffle pays for disk, network, and coordination, which is why it dominates job cost.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: time spent per phase of the 100-second job, one bar per phase, widths proportional to seconds.

- **Title (bold 15px, `#1a5276`, top center):** "Where the 100 Seconds Go (illustrative timings)".
- **Layout:** left-aligned 12px `#444` row labels at x=20, bars start at x=185, scale 5.2 px/second, bar height 26.
- **Rows (top to bottom):**
  - y=85: "Stage 1: scan+filter+select (12s)" — blue `#2a78d6` bar, fill `rgba(42,120,214,0.30)`, 2px border, width 62
  - y=150: "Shuffle: write + network + fetch (85s)" — red `#e74c3c` bar, fill `rgba(231,76,60,0.20)`, 2px border, width 442
  - y=215: "Stage 2: per-city sum (3s)" — green `#008300` bar, fill `rgba(0,131,0,0.20)`, 2px border, width 16
- **Value labels:** bold 12px, same hue as each bar, "12s" / "85s" / "3s" just right of each bar end.
- **Annotation (bold 13px red `#e74c3c`, at x=320, y=130):** "85% of the job is moving rows, not computing".
- **Caption (12px `#444`, bottom right):** "timings illustrative; proportions are the point".

## Filtering After the Shuffle Ships Rows for Nothing

**Tags:** `common mistake` (red), `shuffle size` (orange)

- **The mistake** — grouping by (city, year) first and filtering to 2025 after the shuffle, not before
- **The cost** — the shuffle now repartitions all 40M rows by city instead of the 6M that survive the filter
- **The ratio** — 40M vs 6M shuffled rows is 6.7× the disk, network, and fetch work for the same answer
- **The safety net** — good optimizers push filters below the shuffle, but UDF filters often block the push
- **The habit** — shrink data before every wide step: filter early, drop unused columns, pre-aggregate

*Example (italic):* Moving the year filter above the groupBy drops the shuffle from 40M rows to 6M — the result is identical, but 34M rows never touch the network.

**Common mistake:** Assuming line order is free because execution is lazy. The plan's shape decides how many rows cross the shuffle, and a filter placed after a wide step can multiply job cost several-fold.

### Visualization (canvas `c4`, 720×300)

Two-row before/after bar chart: rows crossing the shuffle when the filter comes after the groupBy vs before it.

- **Title (bold 15px, `#1a5276`, top center):** "Rows Crossing the Shuffle: Filter After vs Filter Before".
- **Layout:** left-aligned 12px `#444` row labels at x=20, bars start at x=210, scale 11.5 px per million rows, bar height 34.
- **Row 1 (y=95):** label "filter AFTER groupBy"; red `#e74c3c` bar, fill `rgba(231,76,60,0.20)`, 2px border, width 460 (40M rows); bold 12px red label "40M rows shuffled" inside the bar's right end.
- **Row 2 (y=185):** label "filter BEFORE groupBy"; green `#008300` bar, fill `rgba(0,131,0,0.20)`, 2px border, width 69 (6M rows); bold 12px green label "6M rows shuffled" just right of the bar.
- **Bracket:** thin dashed `#6b7280` vertical guides (dash 4/3) at the two bar ends near y=140 with bold 13px `#d95926` label "6.7× less network traffic" at (350, 250).
- **Caption (12px `#444`, bottom right):** "row counts illustrative; same final answer both ways".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all geometry and values are the hardcoded numbers above (no randomness); the running example is one job throughout — 40M-row orders table in 8 partitions, filter to 2025 keeps 6M rows, groupBy city into 4 buckets, stage timings 12s / 85s / 3s of a 100s job — all invented and labeled illustrative; the 32-block count (8 map tasks × 4 buckets) and the 6.7× ratio (40M / 6M) follow arithmetically from those numbers; the execution semantics (lazy transformations, action-triggered execution, stage cuts at wide dependencies, hash-partitioned shuffle, pipelined narrow ops) are documented Spark behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
