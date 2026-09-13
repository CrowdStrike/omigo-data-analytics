# Scheduling & Dependencies

**Page type:** detail page (tutorial page: card-sections, each with a two-column layout table — text left 50% with tag pills / bullets / example / key-point, canvas right 50%)
**HTML title tag:** Scheduling & Dependencies

**Subtitle:** A pipeline is a chain of jobs — run each one when its parents finish, not at a time you guessed

## Four Jobs, One Chain

Tags: `core idea` (blue), `running example` (green)

- **Extract** — 2:00am: copy yesterday's 12,480 orders out of the app database
- **Clean** — needs the extract: drop test rows, fix types → 12,448 clean orders
- **Aggregate** — needs clean orders: roll up to revenue per city → 214 rows
- **Report** — needs the aggregates: render the morning dashboard
- **The shape** — each job waits on its parents: a DAG (a chain with no loops)

*Example:* The report doesn't care what time it is — it cares that city_revenue finished.

**Key point:** a pipeline is a dependency graph. The real schedule answers "after what?", not just "at what time?".

### Visualization (canvas `c1`, 720×300)

Flow diagram: a four-node dependency chain drawn as boxes connected by arrows.

- **Title (bold 16px, `#1a5276`, top center):** "The Dependency Chain Behind One Morning Dashboard".
- **Boxes:** four 140×80 boxes centered vertically at y=130, fill `#f8f9fa`, 2px colored border, at x = 30, 210, 390, 570:
  - "extract" (border/label `#2a78d6`), line 2 "2:00–2:40am", line 3 "12,480 rows"
  - "clean" (border/label `#008300`), "after extract", "12,448 rows"
  - "aggregate" (border/label `#d95926`), "after clean", "214 city rows"
  - "report" (border/label `#4a3aa7`), "after aggregate", "dashboard"
  - Box label bold 14px in the box color; the two sub-lines 12px in `#2c3e50`.
- **Arrows:** gray (`#6b7280`) filled-head arrows between consecutive boxes at mid height; "waits for" (12px `#6b7280`) centered under each arrow at x = 190, 370, 550, y = 162.
- **Annotations:** bold 13px `#199e70` centered at y=232: "only the first job has a clock time — everything else has a parent"; 12px `#6b7280` at y=258: "arrows point from parent to child: this shape is a DAG (directed, no loops)".

## The Night the Extract Ran Late

Tags: `worked example` (green), `failure mode` (red)

- **Normal night** — extract done 2:40, clean 3:10, aggregate 3:25, report ready 3:40
- **Guess-time schedule** — clean is simply "run at 3:00am", trusting extract is done
- **Bad night** — a slow app database pushes the extract's finish from 2:40 to 4:50
- **The 3:00am clean** — runs anyway, reads the previous day's stale rows, "succeeds"
- **Morning result** — the dashboard shows Monday's numbers labeled as Tuesday

*Example:* Every job showed green in the morning — but "ran" is not "ran on the right data".

**Key point:** a fixed-time schedule encodes a guess about how long parents take. The failure isn't a crash — it's a confident report built on stale data.

### Visualization (canvas `c2`, 720×300)

Two mini Gantt timelines (normal night vs slow night) under fixed-time scheduling.

- **Title (bold 15px, `#1a5276`, top center):** "Fixed-Time Night: Clean Fires at 3:00am Whether Extract Finished or Not".
- **Time axis:** 2:00am to 5:30am mapped to x = 120..680; vertical gridlines (`#e5e9ef`) at 2, 3, 4, 5 o'clock with labels "2:00am"…"5:00am" (12px `#6b7280`) at y=278.
- **Bars:** 22px tall, fill = bar color at 35% alpha, 2px stroke of same color, bold 12px label in bar color to the right of each bar.
- **Row label "normal night"** (bold 13px `#2c3e50` at left, y≈66); bars: extract 2:00–2:40 (`#2a78d6`, label "extract") at y=54; clean 3:00–3:10 (`#008300`, "clean 3:00") at y=82; report 3:25–3:40 (`#4a3aa7`, "report 3:40 ✓") at y=110.
- **Row label "slow night"** (y≈172); bars: extract 2:00–4:50 (`#2a78d6`, "extract finishes 4:50") at y=160; clean 3:00–3:10 in magenta `#d55181` at y=188 with label "clean 3:00 — reads stale rows, still \"succeeds\""; report 3:25–3:40 magenta at y=216 with label "report 3:40 — yesterday's numbers, today's date".
- **Bottom annotation (bold 13px `#d55181`, centered, y=258):** "no crash, all jobs green — and the dashboard is wrong".

## Run on DONE, Not on a Clock

Tags: `rule of thumb` (green), `where it's used` (blue)

- **Trigger on completion** — clean starts when extract signals success, whenever that is
- **Late, not wrong** — on the slow night the report lands 5:50am instead of 3:40, correct
- **One late parent** — delays every descendant: the whole tree shifts by two hours
- **Schedulers encode this** — workflow tools exist to express "after X, run Y"
- **Alert on lateness** — since delay cascades, page someone when the chain misses 7am

*Example:* The 2h10m extract delay became a 2h10m report delay — annoying, visible, and correct beats punctual and wrong.

**Rule of thumb:** run when upstream is DONE. A late report is a nuisance; a wrong report that looks on-time is a trap.

### Visualization (canvas `c3`, 720×300)

Cascading Gantt chart: four dependency-triggered jobs, each starting when the previous finishes.

- **Title (bold 15px, `#1a5276`, top center):** "Trigger-on-Done Night: One Late Parent Shifts the Whole Tree".
- **Time axis:** 2:00am to 6:30am mapped to x = 120..680; vertical gridlines (`#e5e9ef`) at 2, 3, 4, 5, 6 o'clock with labels "2:00am"…"6:00am" at y=268.
- **Rows** (bars 22px, fill color at 35% alpha + 2px stroke; row labels bold 12px `#2c3e50` at left; note in bar color right of bar):
  - y=56: "extract" 2:00–4:50 (`#2a78d6`), note "slow: 4:50"
  - y=104: "clean" 4:50–5:20 (`#008300`), note "starts on DONE"
  - y=152: "aggregate" 5:20–5:35 (`#d95926`), no note
  - y=200: "report" 5:35–5:50 (`#4a3aa7`), note "ready 5:50 — correct"
- **Handoff arrows:** gray (`#6b7280`) arrows from each bar's end down to the next bar's start.
- **Bottom annotation (bold 13px `#008300`, centered, y=290):** "+2h10m late, 100% correct — the delay is visible instead of hidden inside a wrong report".

## The Confusion: A Bigger Time Gap Is Still a Guess

Tags: `common confusion` (red), `design trap` (orange)

- **The instinct** — after the incident, move clean from 3:00am to 4:00am "to be safe"
- **Still a guess** — the next slow night finishes at 4:50 and blows through the margin
- **Padding wastes mornings** — every normal night now finishes an hour later for nothing
- **Facts vs hopes** — "clean needs extract" is a fact; "3am is enough" is a hope
- **Backfill bonus** — dependency-triggered chains also rerun cleanly for past days

*Example:* The team moved the job 3:00 → 4:00 → 5:00 over a year, chasing rare slow nights they could have simply waited for.

**Common mistake:** you can't fix a dependency problem with a bigger time gap — encode the dependency itself and the guessing ends.

### Visualization (canvas `c4`, 720×300)

Histogram of extract finish times over a year, with two dashed cutoff lines.

- **Title (bold 15px, `#1a5276`, top center):** "365 Nights of Extract Finish Times vs the Guessed Cutoffs".
- **Data:** half-hour buckets ["2:30", "3:00", "3:30", "4:00", "4:30", "5:00", "5:30"] with night counts `[228, 96, 22, 10, 5, 3, 1]`; x labels "by 2:30" … "by 5:30".
- **Axes:** padding top 52 / bottom 62 / left 66 / right 26; y scale 0–240 with gridlines and labels at 0, 100, 200 (12px `#6b7280`); L-shaped axis in `#999`.
- **Bars:** 66px wide; first two buckets blue (fill `rgba(42,120,214,0.35)`, stroke `#2a78d6`), buckets 3–4 yellow (fill `rgba(201,133,0,0.35)`, stroke `#c98500`), buckets 5–7 magenta (fill `rgba(213,81,129,0.40)`, stroke `#d55181`); count value (12px `#2c3e50`) above each bar.
- **Cutoff lines:** vertical dashed (dash 6/4, 2px) before bucket 3 in orange `#d95926` labeled "old guess: clean at 3:00", and before bucket 5 in violet `#4a3aa7` labeled "new guess: 4:00" (bold 12px, above the plot).
- **Bottom annotation (bold 13px `#d55181`, centered):** "41 nights beat the 3:00 guess; 9 still beat the 4:00 guess — the tail always wins".
- **Caption (12px `#6b7280`, bottom right):** "illustrative year".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%). Text cell order: `.tags` pill row, `<ul>` bullets (each starting with `<b>bold term</b>` in `#1a5276`), italic `.example` paragraph, `.key-point` callout.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = bg `rgba(26,82,118,0.12)` / `#1a5276`, green = `rgba(39,174,96,0.15)` / `#27ae60`, red = `rgba(231,76,60,0.12)` / `#e74c3c`, orange = `rgba(230,126,34,0.15)` / `#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** each declared 720×300 intrinsic; a shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); shared `arrow()` helper draws filled-head arrows. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
