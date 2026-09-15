# Airflow

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Airflow

**Subtitle:** Airflow runs data pipelines on a schedule — one Python file declares the tasks and their order as a DAG, and a scheduler runs it every night, retries failures, and can replay the past

## The Pipeline That Wakes Up at 2am

**Tags:** `core idea` (blue), `DAGs` (green), `Airflow` (orange)

- **The pipeline** — a shop's nightly job: extract orders → load warehouse → transform → publish dashboard
- **The DAG** — one Python file lists the tasks and the arrows between them (a directed acyclic graph)
- **The scheduler** — reads `schedule="0 2 * * *"` and creates each night's run by itself, no human clicks
- **Operators & sensors** — prebuilt task types run SQL or submit jobs; sensors wait for a file to appear
- **The grid UI** — a wall of green and red squares, one per task per day, that every data engineer knows
- **Born at Airbnb** — open-sourced in 2015; now every major cloud sells a managed version

*Example (italic):* At 2:00am the scheduler creates tonight's run on its own; publish cannot start until transform turns green.

**Key point:** An Airflow workflow is just Python declaring tasks and dependencies as a DAG — the scheduler decides when things run, the workers do the running.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the nightly DAG's five tasks (a sensor plus the four pipeline steps), with a one-line Python strip below it.

- **Title (bold 15px, `#1a5276`, top center):** "One Python File, One Nightly DAG".
- **Task boxes (118px wide, 46px tall, 8px radius, fill `rgba(42,120,214,0.15)`, border 2px `#2a78d6`, two-line 11px `#2c3e50` text, centered on y=120), left edges at x = 16, 156, 296, 436, 576, joined by 3px `#6b7280` arrows in the 22px gaps:**
  - "wait_for_export / (sensor)"
  - "extract / orders API"
  - "load / to warehouse"
  - "transform / SQL"
  - "publish / dashboard"
- **Python strip (12px monospace `#444`, centered at y=210):** `sensor >> extract >> load >> transform >> publish   # schedule="0 2 * * *", retries=2`.
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=250):** "the scheduler does the clicking — a new run appears every night at 2:00am".
- **Caption (12px `#444`, bottom right):** "pipeline illustrative".

## The 2:07am Failure That Fixed Itself

**Tags:** `worked example` (blue), `retries` (green)

- **The run** — extract pulls 52,000 order rows between 2:00 and 2:04am and turns green
- **The failure** — load's attempt 1 dies at 2:07am on a dropped warehouse connection
- **The config** — the task declares `retries=2` with a 5-minute retry delay; no human is paged
- **The retry** — attempt 2 starts at 2:12am and finishes green at 2:16am
- **The finish** — transform runs 2:16–2:22, the dashboard publishes at 2:23am, fresh before anyone wakes

*Example (italic):* One dropped connection cost 8 minutes — 3 failing plus 5 waiting — and the dashboard was still fresh by 2:23am.

**Key point:** Retries are declared, not hand-coded — the scheduler reruns a failed task automatically, and downstream tasks simply wait until it turns green.

### Visualization (canvas `c2`, 720×300)

Gantt-style timeline of tonight's run: one row per task, bars placed by clock time, with the failed load attempt in red and its retry in green.

- **Title (bold 15px, `#1a5276`, top center):** "Tonight's Run: Load Fails at 2:07, Retries, All Green by 2:23".
- **Axes:** row labels ("extract", "load", "transform", "publish") 12px `#444` right-aligned at x=105; plot from x=115, width 560 (22.4px per minute); x = minutes after 2:00am, 0 to 25, tick labels "2:00"–"2:25" every 5 minutes (12px `#444` under baseline y=250); vertical gridlines `#e5e9ef` at each tick.
- **Bars (22px tall, centered on row lines y = 85, 130, 175, 220):**
  - extract: minutes `[0, 4]`, fill `rgba(0,131,0,0.35)`, border 2px `#008300`
  - load attempt 1: minutes `[4, 7]`, solid red `#e74c3c`, bold 11px red label "✗ attempt 1" above the bar
  - retry wait: dashed `#6b7280` (dash 4/3) horizontal line on the load row, minutes `[7, 12]`, 11px `#6b7280` label "5-min retry delay" above
  - load attempt 2: minutes `[12, 16]`, fill `rgba(0,131,0,0.35)`, border 2px `#008300`, bold 11px green label "✓ attempt 2" above
  - transform: minutes `[16, 22]`, same green style; publish: minutes `[22, 23]`, same green style
- **Annotation (bold 13px orange `#d95926`, near x=17 minutes, y=55):** "8 minutes lost, zero humans woken".
- **Caption (12px `#444`, bottom right):** "times illustrative".

## Four Red Squares and One Backfill

**Tags:** `where it's used` (blue), `backfill` (green)

- **The heartbeat** — nearly every table a data scientist queries was filled by a nightly DAG run like this
- **Run per date** — each run is stamped with its data date; Monday's run fills Monday's partition
- **The bug** — a bad schema change makes transform crash four nights running: Mon–Thu squares turn red
- **The fix + backfill** — the fix merges Thursday; Friday one backfill command replays the four dates
- **No overlap** — each replayed run fills only its own day, so history is repaired date by date

*Example (italic):* By Friday 9am the backfill has rerun Mon–Thu; the dashboard's four missing days are back, each filled by its own dated run.

**Key point:** Because every run is tied to a data date, missed history is repaired by replaying dates — backfill is a built-in feature, not an emergency script.

### Visualization (canvas `c3`, 720×300)

Airflow grid view: seven day-columns by four task-rows of colored squares — the load and extract rows all green, the transform row red Mon–Thu, the publish row grayed where its upstream failed.

- **Title (bold 15px, `#1a5276`, top center):** "The Grid Every Data Engineer Knows: Four Red Days, One Backfill".
- **Annotation (bold 12px orange `#d95926`, centered at y=48):** "fix merged Thursday — Friday's backfill replays Mon–Thu".
- **Layout:** day labels "Mon"–"Sun" 12px `#444` centered at y=68 over each column; squares 34×34, left edges at x = 130 + i×80 for i = 0..6 (130, 210, 290, 370, 450, 530, 610); row labels ("extract", "load", "transform", "publish") 12px `#444` right-aligned at x=118, row top edges y = 78, 122, 166, 210.
- **Square states:**
  - extract row: all 7 green, fill `rgba(0,131,0,0.35)`, border 2px `#008300`
  - load row: all 7 green, same style
  - transform row: Mon–Thu red, fill `rgba(231,76,60,0.35)`, border 2px `#e74c3c`, bold 13px `#e74c3c` "✗" centered in each; Fri–Sun green
  - publish row: Mon–Thu gray, fill `rgba(107,114,128,0.18)`, border 2px `#6b7280` (upstream failed); Fri–Sun green
- **Legend (12px `#444`, row at y=272, starting x=130):** green swatch "success", red swatch "failed", gray swatch "upstream failed" (12×12px squares, 6px gap before each label).
- **Caption (12px `#444`, bottom right at y=272):** "states illustrative".

## Workers That Coordinate, Not Compute

**Tags:** `common mistake` (red), `pushdown` (orange)

- **The temptation** — write transform as pandas in a Python task: `df = read_sql("SELECT * FROM raw")`
- **The reality** — workers are small VMs built to coordinate many tasks, not to hold big dataframes
- **The blowup** — at 60M rows the dataframe outgrows the worker and the task dies; retries just repeat it
- **The fix** — push the work down: run the SQL in the warehouse or submit a Spark job, then only wait
- **The rule** — Airflow should tell big engines what to do and watch for green, not lift the data itself

*Example (italic):* At 20M rows the worker's pandas takes 38 minutes where the warehouse takes 3 — and at 60M rows the worker simply runs out of memory.

**Common mistake:** Doing the heavy lifting inside Airflow workers. They are an orchestration layer — a large transform belongs in the warehouse or Spark, with Airflow issuing the command and watching the square turn green.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: transform runtime as the table grows, pandas on the worker (red, exploding then crashing) vs the same SQL pushed to the warehouse (green, flat).

- **Title (bold 15px, `#1a5276`, top center):** "Same Transform, Two Homes: the Worker Chokes, the Warehouse Shrugs".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = runtime in minutes 0 to 60, gridlines `#e5e9ef` at 15/30/45 with 11px `#6b7280` labels; x = three groups centered at x = 170, 370, 570 with 12px `#444` labels "5M rows", "20M rows", "60M rows" under the baseline.
- **Worker bars (left of each center: left edges at center−48, 44px wide):** runtimes minutes `[9, 38, crash]`, fill `rgba(231,76,60,0.30)`, border 2px `#e74c3c`; 11px `#444` value labels "9 min", "38 min" above the first two; the third bar rises to the plot top (y=65) with a dashed `#e74c3c` top edge and bold 12px red label "✗ OOM — never finishes" above it at y=58.
- **Warehouse bars (right of each center: left edges at center+4, 44px wide):** runtimes minutes `[1, 3, 7]`, fill `rgba(0,131,0,0.30)`, border 2px `#008300`, 11px `#444` value labels "1 min", "3 min", "7 min" above.
- **Legend (12px `#444`, top left at x=75, lines at y=62 and y=80):** red swatch "pandas on the worker", green swatch "pushed to the warehouse" (12×12px squares).
- **Annotation (bold 13px magenta `#d55181`, near x=330, y=110):** "the worker coordinates; the warehouse computes".
- **Caption (12px `#444`, bottom right):** "runtimes illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); task times (the 2:00–2:23am run, the 5-minute retry delay), the 52,000 extracted rows, the Mon–Thu failure pattern, and the runtime bars (`[9, 38, crash]` vs `[1, 3, 7]` minutes at 5M/20M/60M rows) are invented and labeled illustrative; Airflow facts (Airbnb origin, 2015 open-sourcing, scheduler/retry/backfill behavior, operators and sensors, managed cloud offerings) are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
