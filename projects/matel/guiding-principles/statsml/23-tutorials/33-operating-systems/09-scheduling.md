# Scheduling

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Scheduling

**Subtitle:** The CPU runs one task at a time — scheduling is the rule for who runs next, and the rule decides who waits seconds and who waits forever

## One Espresso Machine, a Line of Orders

**Tags:** `core idea` (blue), `one CPU, many tasks` (green), `time slicing` (orange)

- **The machine** — one espresso machine makes one drink at a time, exactly like one CPU core
- **The line** — three orders wait: a 10-minute frappé, a 1-minute espresso, a 1-minute tea
- **The rule** — scheduling is just the barista's rule for picking which order to work on next
- **Time slicing** — the barista works 2 minutes on an order, then rotates to the next in line
- **The effect** — small orders escape fast; the frappé finishes a little later than before

*Example (italic):* With 2-minute turns, the espresso is done at minute 3 and the tea at minute 4 — the frappé slips from minute 10 to 12.

**Key point:** A scheduler is the rule for who gets the one machine next; round-robin slicing trades a little total speed for much fairer waits.

### Visualization (canvas `c1`, 720×300)

Gantt-style timeline of round-robin with a 2-minute slice: three horizontal rows (one per order) showing exactly when each order holds the machine.

- **Title (bold 15px, `#1a5276`, top center):** "Round Robin, 2-Minute Slices: Small Orders Escape Early".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 12 (50px per minute), 12px `#444` tick labels every 2 minutes; light `#e5e9ef` vertical gridlines at each tick.
- **Rows (26px-tall bars, left-aligned 12px `#444` row labels at x=8):** "frappé (10m)" at y=90, "espresso (1m)" at y=150, "tea (1m)" at y=210.
- **Frappé segments:** blue `#2a78d6` fill `rgba(42,120,214,0.8)`, minute spans `[[0,2],[4,12]]` — one slice, then the rest once the line is empty.
- **Espresso segment:** green `#008300`, minute span `[[2,3]]`.
- **Tea segment:** aqua `#199e70`, minute span `[[3,4]]`.
- **Finish markers:** bold 12px labels at each bar's right end: "done 12" (blue), "done 3" (green), "done 4" (aqua).
- **Annotation (bold 13px green `#008300`, near minute 6, y=60):** "espresso done at minute 3, not 11".
- **Caption (12px `#444`, bottom right):** "prep times illustrative".

## Frappé First or Frappé Last: the Waiting Math

**Tags:** `worked example` (blue), `average wait` (green)

- **The orders** — frappé needs 10 minutes, espresso 1, tea 1; they arrived in that order
- **First-come** — frappé runs first, so waits are 0, 10, and 11 minutes for the three orders
- **The average** — (0 + 10 + 11) / 3 = 7 minutes of average wait under first-come-first-served
- **Shortest-first** — espresso, then tea, then frappé: waits become 0, 1, and 2 minutes
- **The average again** — (0 + 1 + 2) / 3 = 1 minute — same drinks, 7× less average waiting
- **The catch** — shortest-first must know prep times, and it always shoves the big order last

*Example (italic):* Reordering three drinks cuts the average wait from 7 minutes to 1 without making any single drink faster.

**Key point:** Order changes nothing about total work (12 machine-minutes either way) but changes average wait dramatically — that is the whole game of scheduling.

### Visualization (canvas `c2`, 720×300)

Two stacked Gantt rows on one shared minute axis: the first-come order on top, the shortest-first order below, with each policy's average wait printed beside it.

- **Title (bold 15px, `#1a5276`, top center):** "Same 12 Minutes of Work, Average Wait 7 min vs 1 min".
- **Axes:** origin x=60, baseline y=245, plot width 600; x = minutes 0 to 12 (50px per minute), 12px `#444` tick labels every 2 minutes, `#e5e9ef` vertical gridlines.
- **Row 1 (bar top y=90, 30px tall), label 12px `#444` at x=8:** "first-come"; segments frappé blue `#2a78d6` `[0,10]`, espresso green `#008300` `[10,11]`, tea aqua `#199e70` `[11,12]`; 11px white in-bar labels "frappé", "E", "T".
- **Row 2 (bar top y=175, 30px tall), label:** "shortest-first"; segments espresso green `[0,1]`, tea aqua `[1,2]`, frappé blue `[2,12]`.
- **Wait callouts (bold 12px `#1a5276`, right-aligned at x=712 just above each row):** "avg wait 7 min" (row 1), "avg wait 1 min" (row 2).
- **Annotation (bold 13px magenta `#d55181`, centered near y=140):** "waits: 0+10+11 vs 0+1+2 — order is everything".
- **Caption (12px `#444`, bottom right):** "prep times illustrative; averages exact for these numbers".

## The Shared GPU Has a Barista Too

**Tags:** `where it's used` (blue), `priority` (orange), `clusters` (green)

- **Same problem** — a shared GPU, a Spark cluster, or a team job queue all pick "who runs next"
- **The clash** — a 60-minute training job and a 2-minute dashboard query want the same machine
- **FIFO default** — behind the training job, the 2-minute query returns after 62 minutes
- **Priority lane** — mark queries interactive: the query runs first and returns in 2 minutes
- **The trade** — the training job still finishes at minute 62; only the small job's wait collapses
- **Everyday knobs** — Unix nice values, Kubernetes priority classes, and queue tiers are this dial

*Example (italic):* A dashboard query stuck behind model training returns in 62 minutes under FIFO but in 2 minutes with an interactive priority lane.

**Key point:** Whenever many jobs share one resource, some scheduling policy — often a silent FIFO default — is deciding your wait; priorities let you say whose time matters most.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of completion times for the two jobs under FIFO vs priority scheduling, on a shared minutes axis.

- **Title (bold 15px, `#1a5276`, top center):** "2-Minute Query Behind 60-Minute Training: FIFO vs Priority".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 7px per minute (max width ~440 at 62 min); left-aligned 12px `#444` row labels at x=20.
- **Rows (14px-tall bars at y = 70, 110, 180, 220):**
  - "FIFO — training job": blue `#2a78d6` fill `rgba(42,120,214,0.30)` bar width 420 (60 min running), 11px label "done 60"
  - "FIFO — dashboard query": hollow orange `#d95926` 1.5px outline width 420 (60 min waiting) then solid orange bar width 14 (2 min running), 11px label "done 62"
  - "priority — dashboard query": green `#008300` solid bar width 14 (2 min running), 11px label "done 2"
  - "priority — training job": hollow blue `#2a78d6` 1.5px outline width 14 (2 min waiting) then blue fill `rgba(42,120,214,0.30)` bar width 420 (60 min running), 11px label "done 62"
- **Group separators:** 12px bold `#1a5276` group headers "FIFO" at y=50 and "priority" at y=160, thin `#e5e9ef` divider line at y=145.
- **Annotation (bold 13px green `#008300`, near x=280, y=250):** "query wait 60 min → 0; training barely notices".
- **Caption (12px `#444`, bottom right):** "hollow = waiting, solid = running; job lengths illustrative".

## Priority Without Aging Starves the Report

**Tags:** `common mistake` (red), `starvation` (orange), `aging` (green)

- **Pure priority** — always run the highest-priority ready job and ignore everything below it
- **The stream** — a 10-minute priority-5 job arrives every 10 minutes, keeping the machine busy
- **The victim** — a priority-1 nightly report is ready at minute 0 and is never chosen
- **Starvation** — a job that is always runnable but never picked; its wait has no upper bound
- **Aging** — raise a waiting job's priority by 1 every 10 minutes it sits in the queue
- **The fix in action** — the report climbs 1→5, ties the newcomers at minute 40, and finally runs

*Example (italic):* With +1 priority per 10 minutes waited, the report reaches priority 5 and runs at minute 40 — without aging it waits forever on a busy machine.

**Common mistake:** Adding priorities without aging. It feels like a fairness fix, but on a machine that is always busy it quietly guarantees the lowest-priority job never runs.

### Visualization (canvas `c4`, 720×300)

Line chart of the report job's effective priority over time: a flat starved line without aging vs a climbing staircase with aging that crosses the priority-5 bar at minute 40.

- **Title (bold 15px, `#1a5276`, top center):** "Aging Rescues the Priority-1 Report at Minute 40".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 50, 12px `#444` tick labels every 10 minutes; y = effective priority 1 to 5, gridlines `#e5e9ef` at 2/3/4.
- **High-priority bar:** horizontal dashed `#6b7280` (dash 4/3) line at priority 5, 12px `#6b7280` label "priority-5 jobs keep arriving" above it.
- **Arrival ticks:** small violet `#4a3aa7` down-arrows at the top of the plot at minutes `[0, 10, 20, 30]`, 11px violet label "new p5 job" near the first.
- **With-aging line:** green `#008300` 3px staircase through minutes `[0, 10, 20, 30, 40]`, priorities `[1, 2, 3, 4, 5]`; solid green dot at (40, 5) with bold 12px green label "report runs".
- **Without-aging line:** orange `#d95926` 3px flat line at priority 1 from minute 0 to 50, bold 12px orange label "stuck at 1 — never runs" near minute 42.
- **Annotation (bold 13px green `#008300`, near minute 15, y=80):** "+1 priority per 10 min waited".
- **Caption (12px `#444`, bottom right):** "arrival pattern and aging rate illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); drink prep times (10/1/1 min), the round-robin slices, the GPU job lengths (60/2 min), and the aging staircase (`[1,2,3,4,5]` at minutes `[0,10,20,30,40]`) are invented and labeled illustrative; the average waits (7 min vs 1 min) and completion times (62 vs 2 min) follow exactly from those numbers by hand arithmetic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
