# Query Determinism & Time-Sensitive Queries

**Page type:** detail page (backlog-style 2-column layout: `.card-section` per topic, each with a text column ~45% left and a canvas column ~55% right)
**HTML title tag:** Query Determinism & Time-Sensitive Queries

**Subtitle:** A query that reads the wall clock, an unordered top-N, or a table that is still being written is not a function of its parameters — it is a function of when you happened to run it. (Status pill: TO DISCUSS)

## 1. Relative Time Windows: The Core Anti-Pattern

**What goes wrong.** `SELECT * FROM events WHERE ts > NOW() - INTERVAL '1 hour'` run at 10:00 reads 09:00–10:00. The same query retried at 10:05 reads 09:05–10:05. Different rows, same SQL, no error, pipeline reports success.

- **Silent recovery corruption** — the usual trigger is not a deliberate rerun but automatic retry after a partial write; the window shifts and nobody looks, because the retry "succeeded."
- **Unreproducible analysis** — "I ran this yesterday and got X" can never be reproduced.
- **Race conditions** — two stages running seconds apart see different "last 1 hour" windows.
- **Permanently missed rows** — events from 50 minutes ago that land after the query ran are never picked up.
- **Undebuggable** — you cannot replay a failed step against the data it originally saw.

**The fix: pre-constructed absolute ranges.** Compute the window once, store it, pass it as parameters — `window_start = '2026-08-03T09:00:00Z'`, `window_end = '2026-08-03T10:00:00Z'`, then `WHERE ts >= :window_start AND ts < :window_end`. Retries, backfills and downstream stages all share the same boundaries, the audit log records exactly what was read, and replay with the same parameters gives the same output.

Key-point callout (red accent): **Impact:** the query must be a pure function of its parameters. The orchestrator computes the boundaries; the query never reads the clock.

Example line (italic): Separate "when to run" from "what range to query" — a schedule is not a filter.

### Visualization (canvas `c1`, 720×300)

Timeline diagram with two tracks comparing a wall-clock query vs a parameterized query across two runs.

- **Title (bold 14px `#1a5276`, top center):** "Same Query, Two Runs: The Window Silently Shifts".
- **Time axis:** minutes 0–75 mapped from x=70 to x=690 (representing 08:55–10:10). Vertical light-gray (`#eee`) gridlines from y=46 to y=258 at minutes 5, 20, 35, 50, 65 labeled "09:00", "09:15", "09:30", "09:45", "10:00" in 10px gray `#999` above (y=41).
- **Track A header (bold 11px red `#e74c3c`, left at x=70, y=62):** "ts > NOW() - INTERVAL '1 hour'   (reads the clock)".
- **Track A bars** (16px tall, 9px labels inside at left):
  - y=70, minutes 5–65, fill `rgba(26,82,118,0.35)`, stroke `#1a5276`, label "run @10:00  ->  09:00-10:00" in `#1a5276`.
  - y=94, minutes 10–70, fill `rgba(231,76,60,0.25)`, stroke `#e74c3c`, label "retry @10:05  ->  09:05-10:05" in `#e74c3c`.
- **Drift markers:** dashed red (dash 3/3) horizontal segments at y=114 spanning minutes 5–10 and 65–70; centered 9px red captions at y=128: "09:00-09:05 dropped" (at minute 7.5) and "10:00-10:05 added" (near minute 67.5).
- **Separator:** dashed gray `#ccc` (dash 4/4) horizontal line at y=142 across the plot.
- **Track B header (bold 11px green `#27ae60`, x=70, y=162):** "ts >= :window_start AND ts < :window_end   (pure parameters)".
- **Track B bars** (fill `rgba(39,174,96,0.30)`, stroke `#27ae60`, labels in `#1a5276`):
  - y=170, minutes 5–65, label "run @10:00  ->  09:00-10:00".
  - y=194, minutes 5–65, label "retry @10:05  ->  09:00-10:00 (identical)".
- **Green note (10px `#27ae60`, left at x=70, y=232):** "same parameters -> same rows -> replayable, auditable, backfillable".
- **Caption (11px gray `#888`, bottom center):** "Both runs report success — only the parameterized query returns the same data".

## 2. Other Sources of Non-Determinism

**Engine-level.** Even with a fixed window, the answer can move:

- **Unordered results** — `LIMIT 100` without a total `ORDER BY` returns whichever rows the scan reached first.
- **Concurrent writes and isolation level** — a query running mid-ingestion sees a partial batch; visibility guarantees differ by isolation mode.
- **Eventual consistency** — replica reads (Cassandra, DynamoDB) can serve stale rows.
- **Schema evolution** — a column added mid-pipeline changes NULLability for historical rows.
- **Planner variability** — partition pruning and join order shift with runtime statistics; float aggregation order across shards changes the low bits of a `SUM`.

**Time-related anti-patterns.**

- `WHERE date = CURDATE()` — breaks past midnight, backfill impossible.
- `ORDER BY ts DESC LIMIT 1` — "latest record" changes every second, so it is a non-deterministic join key.
- `DATEDIFF(NOW(), created_at) < 30` — the "active user" definition drifts continuously.
- Cron at `:00` querying "last hour" — a firing delay of a few seconds leaks the previous window in.
- Event time vs processing time — `processed_at` misses reprocessed events, `event_time` catches them but duplicates on replay.
- Timezone-naive timestamps — "last 24 hours" across a DST boundary is 23 or 25 hours.

Key-point callout (red accent): **The shared signature:** the result depends on something outside the parameter list — the clock, the scan order, the replica, or the plan.

### Visualization (canvas `c2`, 720×300)

Two side-by-side row-list panels showing a top-N tie break-down.

- **Title (bold 14px `#1a5276`, top center):** "ORDER BY score DESC LIMIT 3 — Two Runs, Two Answers".
- **Panels:** two panels 280px wide, gap 50px, starting at x=55; row height 26px, rows start at y=70; panel titles (bold 12px, centered at y=58): "Run 1 — scan order A" in `#1a5276`, "Run 2 — scan order B" in `#e74c3c`.
- **Panel 1 rows (id, score):** row 4187 / 95, row 4102 / 93, row 4210 / 91, row 4055 / 91, row 4331 / 91, row 4008 / 88. Result line (bold 10px `#1a5276`, centered below rows): "returns 4187, 4102, 4210".
- **Panel 2 rows:** row 4187 / 95, row 4102 / 93, row 4055 / 91, row 4331 / 91, row 4210 / 91, row 4008 / 88. Result line (bold 10px `#e74c3c`): "returns 4187, 4102, 4055".
- **Row styling:** each row is a 22px-tall outlined box (`#ccc` stroke); rows with score 91 (the tie group) get fill `rgba(230,126,34,0.12)` and stroke `#e67e22`; row id in 11px `#2c3e50` left-aligned, "score N" right-aligned (`#e67e22` for tie rows, `#2c3e50` otherwise).
- **LIMIT cut line:** dashed red `#e74c3c` (dash 5/3, width 1.5) horizontal line after the third row of each panel, extended 6px past the panel edges, labeled "LIMIT 3 cut" in 9px red right-aligned above the line.
- **Orange note (10px `#e67e22`, centered below panels):** "shaded rows = tie group at score 91: their relative order is not defined by the query".
- **Caption (11px gray `#888`, bottom center):** "A top-N is only reproducible if ORDER BY is a total order — add a unique tiebreak".

## 3. Design Rules and Frameworks That Get It Right

**Rules.** Determinism is a property you design in, not a bug you fix later:

- **Parameterize every time boundary** — no wall-clock function calls inside the query.
- **Separate scheduling from windowing** — "run hourly" is not "query the last hour."
- **Idempotent writes** — rerunning with the same parameters upserts the same output, never duplicates.
- **Watermarks for late data** — do not close a window until a grace period says no more events are coming.
- **Explicit total ORDER BY** — every pipeline-feeding query needs a tiebreak column, not just a sort key.
- **Log the parameters** — each run records its window boundaries, query hash and row count.

Key-point callout (red accent): **The common pattern:** the system computes a deterministic boundary, passes it as a value, and the query stays pure. "When did this run" is metadata, not a filter.

### Visualization (canvas `c3`, 720×300)

Event-arrival timeline showing late data, window end and watermark/grace boundaries.

- **Title (bold 14px `#1a5276`, top center):** "Late Arrivals: When Can the 09:00-10:00 Window Close?".
- **Axis:** arrival-time minutes 0–40 mapped from x=60 to x=690 (representing 09:50–10:30); horizontal gray `#ccc` axis at y=200 with 5px ticks and 10px `#999` labels "09:50", "10:00", "10:10", "10:20", "10:30" at minutes 0/10/20/30/40. Axis caption (10px `#666`, centered at y=234): "arrival (processing) time of events belonging to the 09:00-10:00 window".
- **Legend (10px, y≈40-48):** green `#27ae60` swatch "arrived before window end"; orange `#e67e22` swatch "late, inside grace period"; red `#e74c3c` swatch "after grace — dropped or reprocessed" (legend text in `#2c3e50`).
- **Boundary lines:** vertical dashed (dash 4/3, width 1.5) lines from y=88 to y=206: at minute 10 in `#1a5276` labeled "window end 10:00 — naive query runs here" (label at y=70); at minute 20 in `#e67e22` labeled "grace closes 10:10 — watermark" (label at y=84).
- **Events:** dots (radius 4.5, white 1px outline) at arrival minutes `[0.5, 2, 3.5, 5, 6, 7.5, 9, 9.7, 10.5, 12.8, 16, 26, 33]`, staggered vertically at y = 108 + (i mod 4)×21; color green `#27ae60` if minute < 10, orange `#e67e22` if 10–20, red `#e74c3c` after 20.
- **Run comparison (bold 10px, left at x=60):** "query at 10:00  ->  8 rows (misses every late arrival)" in red at y=254; "query at 10:10 after grace  ->  11 rows, stable on replay" in green at y=270.
- **Caption (11px gray `#888`, bottom center):** "Determinism needs two pinned values: the window, and when you stop waiting".

### Comparison table (full-width `.compare` table below the 2-col row)

| Framework | Mechanism that preserves determinism |
|-----------|--------------------------------------|
| Apache Airflow | `execution_date` is a fixed logical timestamp, not "now" — tasks receive their window as parameters. |
| Apache Flink | Event-time processing with watermarks — late data and window closure are explicit, not incidental. |
| dbt | Incremental models can lean on `run_started_at`, but the reproducible practice is an explicit `var('start_date')`. |
| Spark Structured Streaming | Watermark-based triggers — window boundaries are data-driven rather than wall-clock driven. |

## 4. Open Questions and Tradeoffs

Not every consumer wants the same rung of the ladder, which is where the design tension lives:

- Should the pipeline lint queries for non-deterministic functions (`NOW()`, `CURDATE()`, `RANDOM()`) and warn or block?
- Dashboards want the freshest possible data; pipelines want reproducibility. Same store, opposite requirements.
- Late data: close the window and accept a gap, or reprocess? Each gives a different determinism guarantee.
- Monitoring (#22) inherits this — a non-deterministic alerting query produces unreliable alerts.
- Backfill is the acid test: a deterministic query replays for any historical window, a relative one cannot.

Questions callout (orange accent): **Working position:** pipelines and monitoring must sit on the deterministic rungs; wall-clock queries are allowed only in read-only dashboards where nothing downstream depends on the answer being stable.

Example line (italic): A useful audit question per query: if I run this again tomorrow with the same parameters, what could legitimately change the answer?

### Visualization (canvas `c4`, 720×300)

Vertical ladder diagram of four determinism rungs with badges and an upward arrow.

- **Title (bold 14px `#1a5276`, top center):** "The Determinism Ladder: What \"Same Query, Same Answer\" Requires".
- **Rungs:** four boxes 420px wide × 44px tall at x=150, starting y=52, vertical step 52px, fill `rgba(26,82,118,0.06)`, 2px stroke in the rung color. Each shows bold 12px title (rung color), 10px example line (`#555`), and a 10px badge to the right of the box (rung color); top to bottom:
  1. "Fully deterministic" — example "absolute params + pinned snapshot + total ORDER BY" — color `#27ae60` — badge "replayable".
  2. "Deterministic given a snapshot" — example "absolute window, but the table keeps changing underneath" — color `#1a5276` — badge "replay if pinned".
  3. "Wall-clock / session dependent" — example "NOW(), CURDATE(), CURRENT_USER" — color `#e67e22` — badge "not replayable", with 9px gray `#888` note below: "dashboards live here".
  4. "Truly random" — example "RANDOM(), unseeded sampling" — color `#e74c3c` — badge "random by design".
- **Arrow:** vertical 2px `#1a5276` arrow at x=128 pointing up alongside the ladder, with rotated 11px `#1a5276` label "reproducibility increases".
- **Caption (11px gray `#888`, bottom center):** "Pipelines and alerts belong on the upper rungs; lower rungs are read-only conveniences".

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col text/viz layout). Page: h1, `.subtitle` paragraph containing an inline `.status` pill, then one `.card-section` per numbered topic. Each `.card-section` has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one `<tr>`: left `td.text-col` (45%) with paragraphs, `<ul>` bullets, `.key-point`/`.questions` callouts and `.example` lines; right `td.viz-col` (55%) with the canvas. Section 3 additionally has a full-width `table.compare` below its layout table.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `strong` in `#1a5276`.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.questions` — same but left border `3px solid #e67e22`. `.example` — italic, `#555`, 0.9rem.
- **Inline code:** background `#f8f9fa`, border `1px solid #e0e0e0`, padding 1px 5px, radius 3px, 0.82em, color `#1a5276`.
- **Status pill:** `.status` inline-block, background `#f8f9fa`, border `1px solid #e0e0e0`, color `#1a5276`, padding 2px 10px, radius 12px, 0.8rem bold.
- **Compare table:** `table.compare` full-width, 0.9rem; `th` background `#1a5276` white text left-aligned padding 8px 10px 0.85rem; `td` border `1px solid #e0e0e0` padding 8px 10px top-aligned; even rows `#f8f9fa`.
- **Canvas:** intrinsic 720×300 per chart, CSS `width: 100%`, border `1px solid #e0e0e0` radius 4px; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, grays `#666`/`#888`/`#999`/`#ccc`.
- Detail pages have no nav bar and no back/home links; any card links in regenerated HTML grids use `.html` extensions.
