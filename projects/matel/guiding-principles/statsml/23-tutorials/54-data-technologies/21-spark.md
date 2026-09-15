# Spark

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Spark

**Subtitle:** Spark writes down your whole pipeline as a plan first and runs nothing until you ask for a result — then executes it in memory across a cluster

## The Pipeline That Doesn't Run Yet

**Tags:** `core idea` (blue), `lazy evaluation` (green), `DAG` (orange)

- **The job** — filter 10M order rows to 2025, join to 50k customers, sum revenue per region
- **The surprise** — all three lines return instantly; Spark has computed nothing at all
- **The plan** — each transformation only adds a node to a DAG: filter → join → aggregate
- **The action** — only `.show()` (or count, write) tells Spark a result is actually needed
- **The payoff** — seeing the whole DAG first lets the Catalyst optimizer rearrange it before running

*Example (italic):* The filter/join/aggregate lines finish in milliseconds; the `.show()` at the end is where all 42 seconds of real work happen.

**Key point:** Transformations are lazy — they build a DAG of intended work; nothing executes until an action asks for a result, so Spark optimizes the whole plan, not one step at a time.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the running example: three dashed "plan only" transformation nodes building a DAG, then a solid action node that triggers execution, with a work meter that stays at zero until the action.

- **Title (bold 15px, `#1a5276`, top center):** "Three Transformations Build a Plan — the Action Runs It".
- **DAG row (boxes centered on y=110):** four rounded boxes left to right at x = 40, 210, 380, 550, each 140px wide, 44px tall, 8px radius, 12px `#2c3e50` text; first three have dashed 2px `#2a78d6` borders, fill `rgba(42,120,214,0.10)`, labels "filter(year=2025)", "join(customers)", "groupBy(region).sum"; fourth has solid 3px `#008300` border, fill `rgba(0,131,0,0.12)`, label ".show()  — action".
- **Arrows:** 2px `#6b7280` arrows between consecutive boxes at y=110.
- **Stage labels (11px `#6b7280`, above first three boxes):** "plan node" over each; bold 12px green `#008300` "execution starts here" above the action box.
- **Work meter (bar at y=210, x from 60 to 660, 16px tall, `#e5e9ef` background):** green `#008300` fill segment only under the action box's x-range (x 550–660); 12px `#444` labels "work done: 0s" at x=70 and bold 12px green "42s of compute" at x=545, y=250.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "the code ran top to bottom, but the cluster ran nothing until the last line".
- **Caption (12px `#444`, bottom right):** "runtimes illustrative".

## Counting the Stages by Hand

**Tags:** `worked example` (blue), `shuffles` (green), `driver & executors` (orange)

- **The split** — a driver process plans the job; 4 executors × 2 cores run 8 tasks at a time
- **Narrow steps** — the filter touches each row where it sits, so it stays inside one stage
- **Wide steps** — join and groupBy must move rows between machines: each one is a shuffle
- **Hand-check** — 2 shuffle boundaries make 3 stages here; each shuffled input adds its own stage
- **The default** — shuffled data lands in 200 partitions (Spark's documented default), run as 200 tasks

*Example (italic):* Stage 1 scans and filters 10M rows down to 4M, Stage 2 shuffles them to join customers, Stage 3 shuffles again to sum 6 regions — 3 stages, 2 shuffles; a small customers table would be broadcast instead, skipping that shuffle.

**Key point:** Spark cuts the DAG into stages at shuffle boundaries; inside a stage, work runs as parallel tasks on executors with no data movement — the shuffles between stages are where the cost lives.

### Visualization (canvas `c2`, 720×300)

Stage diagram of the same pipeline: three stage blocks separated by two dashed shuffle boundaries, with row counts shrinking left to right and a task lane showing parallel executors.

- **Title (bold 15px, `#1a5276`, top center):** "2 Shuffles Cut the DAG into 3 Stages".
- **Stage blocks (y=70, 56px tall, 8px radius):** blue `rgba(42,120,214,0.15)` boxes with 2px `#2a78d6` borders at x=50 width 170 ("Stage 1: scan + filter / 10M → 4M rows"), x=280 width 170 ("Stage 2: join customers / 4M rows"), x=510 width 170 ("Stage 3: sum by region / → 6 rows"), 12px `#2c3e50` two-line labels.
- **Shuffle boundaries:** vertical dashed `#d95926` (dash 5/4) lines at x=250 and x=480 from y=55 to y=215, each with bold 12px orange `#d95926` label "shuffle" at its top.
- **Task lane (y=160 to 215):** 12px `#444` label "8 tasks in parallel (4 executors × 2 cores)" at x=50, y=150; under Stage 1, 8 small green `#008300` bars (10px tall, staggered x 50–210) for its tasks; under Stages 2–3, 11px `#6b7280` note "200 shuffle partitions (default)" centered in each stage's x-range at y=190.
- **Annotation (bold 13px magenta `#d55181`, centered near y=255):** "count the wide steps: 2 shuffles → 3 stages, no cluster needed to know it".
- **Caption (12px `#444`, bottom right):** "row counts illustrative; one stage per shuffle boundary here".

## Why Memory Beat MapReduce

**Tags:** `where it's used` (blue), `in-memory` (green), `one engine` (orange)

- **The origin** — Spark came out of Berkeley's AMPLab, first as RDDs, later the DataFrame API
- **The old cost** — MapReduce writes results to disk after every step; iterative jobs pay it every loop
- **The new trick** — Spark keeps intermediate data in memory, so loop 2 starts where loop 1 ended
- **The result** — on iterative work like ML training, that beat MapReduce by an order of magnitude
- **One surface** — the same engine and API run batch, SQL, structured streaming, and MLlib models
- **The steward** — Spark's creators founded Databricks, the project's main commercial steward

*Example (italic):* Ten training iterations at ~110s each on disk cost 1,100s; Spark pays ~80s once to load, then ~6s per pass in memory — 134s total, roughly 8× faster (illustrative).

**Key point:** The order-of-magnitude win on iterative work came from skipping the disk between steps — and because one engine covers batch, SQL, streaming, and ML, the same DAG machinery serves all four.

### Visualization (canvas `c3`, 720×300)

Line chart of per-iteration runtime over 10 training iterations: MapReduce flat and high (disk every pass), Spark high once then dropping to a low in-memory floor.

- **Title (bold 15px, `#1a5276`, top center):** "10 Training Iterations: Disk Every Pass vs Memory After the First".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = iteration 1 to 10, 12px `#444` tick labels at 1/3/5/7/10; y = seconds per iteration 0 to 120, gridlines `#e5e9ef` at 30/60/90.
- **MapReduce line:** red `#e74c3c` 3px line through iterations `[1,2,3,4,5,6,7,8,9,10]`, seconds `[110, 112, 109, 111, 110, 113, 108, 110, 111, 110]` — flat near the top.
- **Spark line:** green `#008300` 3px line through the same iterations, seconds `[80, 6, 6, 6, 6, 6, 6, 6, 6, 6]` — one tall first point, then a low floor.
- **Point markers:** 4px radius filled circles on both lines at every iteration.
- **Labels:** bold 12px red `#e74c3c` "disk between every step — 1,100s total" near iteration 5, y=75; bold 12px green `#008300` "in memory after load — 134s total" near iteration 5, y=200.
- **Annotation (bold 13px violet `#4a3aa7`, near iteration 7, y=130):** "~8× on iterative work".
- **Caption (12px `#444`, bottom right):** "timings illustrative; order-of-magnitude claim documented".

## Lazy Doesn't Mean Cached

**Tags:** `common mistake` (red), `recomputation` (orange)

- **The confusion** — people assume a DataFrame they built once holds its result like a variable
- **The reality** — a DataFrame is a plan; every action replays the whole DAG from the source
- **The bill** — three actions on the uncached pipeline run the 42s filter-join-aggregate three times
- **The fix** — `.cache()` marks the result to be kept in executor memory after the first action
- **The nuance** — cache is itself lazy: nothing is stored until the next action materializes it

*Example (italic):* Uncached, a `.count()`, a `.show()`, and a `.write()` cost 42s + 42s + 42s = 126s; cached, the first pays 42s and the next two take ~3s each — 48s total (illustrative).

**Common mistake:** Treating a lazy DataFrame as a stored result. Laziness means Spark remembers the recipe, not the dish — without `.cache()`, every action cooks the whole DAG from scratch.

### Visualization (canvas `c4`, 720×300)

Grouped horizontal bar chart: time paid by three successive actions on the same pipeline, uncached (three full replays) vs cached (one full run, then two fast reads).

- **Title (bold 15px, `#1a5276`, top center):** "Three Actions, Same Pipeline: Replay Everything vs Cache Once".
- **Layout:** bars extend right from a 2px `#999` baseline at x=230, max width 420 (42s = 420px, so 10px per second); left-aligned 12px `#444` row labels at x=20.
- **Uncached rows (y = 60, 95, 130):** labels "action 1 — 42s", "action 2 — 42s", "action 3 — 42s"; red `#e74c3c` bars, fill `rgba(231,76,60,0.30)` with 2px solid edge, widths 420, 420, 420; bold 12px red total "126s" at the right of row 3.
- **Cached rows (y = 185, 220, 255):** labels "action 1 — 42s (fills cache)", "action 2 — 3s", "action 3 — 3s"; green `#008300` bars, fill `rgba(0,131,0,0.30)` with 2px solid edge, widths 420, 30, 30; bold 12px green total "48s" at the right of row 3.
- **Group headers (bold 13px, at x=230):** red `#e74c3c` "uncached" at y=45; green `#008300` "with .cache()" at y=170.
- **Bar style:** 16px tall, 11px `#444` width labels at bar ends where the total labels don't already cover them.
- **Annotation (bold 13px orange `#d95926`, right side near y=155):** "a DataFrame is a recipe, not leftovers".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); row counts, per-iteration timings, and action costs are invented and labeled illustrative; the 200-shuffle-partition default is an exact documented fact, and the order-of-magnitude-over-MapReduce claim on iterative work is publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
