# Thrashing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Thrashing

**Subtitle:** When memory is over-committed, the machine spends nearly all its time swapping pages in and out and almost none doing real work — busy doing nothing

## The Desk With Four Folder Slots

**Tags:** `core idea` (blue), `memory` (green), `page swaps` (orange)

- **The desk** — a clerk's desk holds exactly 4 folders; every other folder lives in the archive room
- **The rule** — writing one step of a report needs that report's 2 folders open on the desk
- **The trip** — a folder not on the desk means a 2-minute walk to the archive to swap it in
- **The overload** — with too many reports open at once, every step evicts folders the next step needs
- **The name** — the clerk is thrashing: always moving folders, almost never writing

*Example (italic):* With 8 reports assigned at once, the clerk spends the whole afternoon in the hallway and finishes half a page.

**Key point:** Thrashing is when a system spends nearly all its time swapping memory in and out and almost none of it doing the actual work.

### Visualization (canvas `c1`, 720×300)

Flow diagram: the small desk (fast memory, 4 folder slots) beside the big archive room (slow storage), with swap arrows between them.

- **Title (bold 15px, `#1a5276`, top center):** "One Desk, Four Folder Slots, and a Long Hallway".
- **Desk box:** rounded box at x=60, y=80, width 230, height 150, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 13px `#1a5276` label "Desk — 4 folder slots" at its top; inside, a 2×2 grid of folder slots (each 90×42, fill `rgba(42,120,214,0.30)`, 12px `#2c3e50` labels "R1 f1", "R1 f2", "R2 f1", "R2 f2").
- **Archive box:** rounded box at x=460, y=80, width 210, height 150, fill `rgba(107,114,128,0.12)`, 2px `#6b7280` border, bold 13px `#6b7280` label "Archive — every other folder"; inside, three small stacked folder rectangles (70×24, fill `rgba(107,114,128,0.25)`).
- **Arrows:** orange `#d95926` 3px arrow from archive to desk at y=130 with bold 12px orange label "swap in — 2 min per folder"; mute `#6b7280` 2px arrow from desk to archive at y=190 with 12px mute label "swap out".
- **Annotation (bold 13px orange `#d95926`, centered near y=265):** "writing needs the folder on the desk — the walking is pure overhead".
- **Caption (12px `#444`, bottom right):** "layout schematic; times illustrative".

## Twenty Minutes of Work That Takes a Hundred

**Tags:** `worked example` (blue), `hand-check` (green)

- **The setup** — 2 reports, one 1-minute step each in turn: all 4 folders fit, so no archive trips at all
- **The clean run** — 20 steps of writing take 20 minutes, and every minute is useful work
- **The overload** — 4 reports need 8 folders on a 4-slot desk; each turn starts with 2 folders missing
- **The cost** — each step now pays 2 fetches × 2 min + 1 min of writing = 5 minutes per step
- **The total** — the same 20 steps take 100 minutes, and only 20 of them are actual writing
- **The ratio** — useful work falls from 100% to 20% just by doubling the number of open reports

*Example (italic):* Doubling the workload from 2 reports to 4 made the same 20 steps take 5× longer — 20 minutes became 100.

**Key point:** Thrashing math is brutal: the work itself stays the same size while swap time explodes, so throughput collapses even though nothing got harder.

### Visualization (canvas `c2`, 720×300)

Two horizontal stacked bars comparing where the minutes go: 2 reports (all writing) vs 4 reports (mostly fetching), on a shared 0–100 minute axis.

- **Title (bold 15px, `#1a5276`, top center):** "Same 20 Steps: 20 Minutes vs 100 Minutes".
- **Axis:** origin x=100, bars extend right, scale 6 px per minute (0–100 min spans 600px); 12px `#444` tick labels at 0/20/40/60/80/100 min along a 2px `#999` baseline at y=245, gridlines `#e5e9ef` rising from each tick.
- **Row 1 (bar top y=100, 36px tall), 12px `#444` label "2 reports" at x=20:** one green `#008300` segment width 120 (20 min of writing), 12px white inner label "20 min — all writing".
- **Row 2 (bar top y=170, 36px tall), label "4 reports":** green `#008300` segment width 120 (20 min writing, inner label "20 writing") followed by orange `#d95926` segment width 480 (80 min fetching, 12px white inner label "80 min walking to the archive").
- **Annotation (bold 13px magenta `#d55181`, above row 1 near x=380, y=80):** "useful work: 100% → 20%".
- **Caption (12px `#444`, bottom right):** "minutes from the worked example; illustrative".

## RAM, Disk, and the Cliff in the Curve

**Tags:** `where it's used` (blue), `working set` (green), `page faults` (orange)

- **The mapping** — RAM is the desk, disk is the archive, a folder is a page, the trip is a page fault
- **The gap** — a page fault served from disk costs thousands of times more than a RAM access
- **The trigger** — too many processes share RAM, so no process keeps its working set resident
- **The cliff** — in the chart, useful work peaks at 88% with 6 processes and collapses to 5% at 10
- **The data scientist** — a join needing 12 GB of working memory on an 8 GB laptop churns the disk for hours
- **The symptom** — the machine feels frozen while the disk stays busy and the fans spin

*Example (italic):* A dataframe join that needs 12 GB on an 8 GB laptop keeps pushing out pages it will need again seconds later, over and over.

**Key point:** Every layer of computing has a thrashing point: the moment the working set stops fitting in the fast layer, time goes to moving data instead of computing on it.

### Visualization (canvas `c3`, 720×300)

Line chart of the classic thrashing curve: useful work rises as processes are added, peaks when RAM is full, then falls off a cliff.

- **Title (bold 15px, `#1a5276`, top center):** "Adding Work Helps — Until RAM Runs Out".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = concurrent processes 1 to 10 with 12px `#444` tick labels at each integer; y = useful work % 0 to 100, gridlines `#e5e9ef` at 25/50/75, 12px `#444` labels.
- **Curve:** blue `#2a78d6` 3px line with 4px dots through processes `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, useful work % `[20, 38, 55, 70, 82, 88, 62, 30, 12, 5]` — smooth rise to a peak at 6, steep collapse after.
- **Peak marker:** vertical dashed `#6b7280` (dash 4/3) line at x=6 with 12px `#6b7280` label "RAM runs out" at its top.
- **Annotation (bold 13px red `#e74c3c`, near x=8.5, y=100):** "more work in flight, less work done".
- **Caption (12px `#444`, bottom right):** "curve shape classic; percentages illustrative".

## Busy Is Not the Same as Productive

**Tags:** `common mistake` (red), `utilization` (orange)

- **The illusion** — the machine shows near-100% activity, so it looks like it is running at full speed
- **The reality** — the disk is 95% busy moving pages while the CPU does useful work only 8% of the time
- **The wrong fix** — buying a faster CPU speeds up the 8%, not the 92% spent waiting on swaps
- **The right fix** — run fewer things at once, or add RAM, so the working set fits again
- **Not all swapping** — an occasional page fault is normal; thrashing is when faults dominate the time
- **Admission control** — operating systems suspend whole processes on purpose to break the spiral

*Example (italic):* Suspending 3 of the 4 overloaded reports feels like giving up, but the remaining one finishes sooner than all four were going to.

**Common mistake:** Reading "busy" as "productive". In thrashing, utilization is high precisely because throughput is near zero — the fix is less load or more memory, never a faster worker.

### Visualization (canvas `c4`, 720×300)

Two horizontal bars on a shared 0–100% axis: what the dashboard shows (disk busy) vs what actually matters (useful work), exposing the gap.

- **Title (bold 15px, `#1a5276`, top center):** "The Dashboard Lies: 95% Busy, 8% Productive".
- **Axis:** origin x=230, bars extend right, scale 4.4 px per percent (0–100% spans 440px); 12px `#444` tick labels at 0/25/50/75/100% along a 2px `#999` baseline at y=245.
- **Row 1 (bar top y=100, 36px tall), 12px `#444` right-aligned label "disk busy (looks like work)" ending at x=220:** orange `#d95926` bar width 418 (95%), bold 12px orange label "95%" at the bar end.
- **Row 2 (bar top y=170, 36px tall), label "useful work (actual progress)":** blue `#2a78d6` bar width 35 (8%), bold 12px blue label "8%" at the bar end.
- **Annotation (bold 13px red `#e74c3c`, centered near x=430, y=225):** "the gap between the bars is thrashing".
- **Caption (12px `#444`, bottom right):** "percentages illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the desk/folder economics (4 slots, 2 folders per report, 1-min step, 2-min fetch → 20 min vs 100 min, 100% vs 20% useful) are invented and labeled illustrative; the thrashing curve `[20, 38, 55, 70, 82, 88, 62, 30, 12, 5]` over processes 1–10 and the 95%-busy / 8%-useful pair are illustrative renderings of the classic textbook shape.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
