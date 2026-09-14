# Readers-Writers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Readers-Writers

**Subtitle:** Many people can read the same data at the same time, but a writer needs it alone — like a chalkboard menu the whole queue reads while one manager rewrites a price

## The Chalkboard Menu Everyone Reads at Once

**Tags:** `core idea` (blue), `shared reads` (green), `exclusive write` (orange)

- **The board** — a coffee shop's one chalkboard menu; twenty customers a minute glance at it, one manager updates prices
- **Reads share** — any number of customers can read the same board at the same moment without bothering each other
- **Writes exclude** — while the manager erases and rewrites a price, nobody should be reading a half-written number
- **The torn read** — a customer who reads mid-rewrite sees "$" and a smudge and orders at the wrong price
- **The name** — this many-read / one-write pattern is the readers-writers problem, and a read-write lock is its standard fix

*Example (italic):* At noon, 6 customers read the board simultaneously with no conflict; at 12:03 the manager takes it alone for 4 seconds to change one price.

**Key point:** Readers-writers means readers may share the data freely among themselves, but a writer requires exclusive access — a read-write lock grants "many readers OR one writer", never both.

### Visualization (canvas `c1`, 720×300)

Timeline lane chart of 20 seconds at the board: reader bars overlapping freely in parallel lanes, then one writer bar standing completely alone.

- **Title (bold 15px, `#1a5276`, top center):** "Readers Overlap Freely; the Writer Gets the Board Alone".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 20 (30px per second), 12px `#444` tick labels every 5s; six horizontal lanes at y = 75, 103, 131, 159, 187, 215, separated by gridlines `#e5e9ef`.
- **Reader bars:** blue fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, 20px tall, one per lane, at seconds `[start, end]`: `[0,2]`, `[0.3,2.3]`, `[0.6,2.6]`, `[0.9,2.9]`, `[1.2,3.2]`, `[1.5,3.5]` (all six overlap around second 1.5-2), then two post-write readers `[17,19]` (lane 1) and `[17.5,19.5]` (lane 2); each labeled "read" in 11px `#2a78d6`.
- **Writer bar:** orange solid `#d95926`, 20px tall, lane 3, at seconds `[12,16]`, bold 12px white label "WRITE"; no other bar overlaps seconds 12–16.
- **Exclusive zone:** vertical dashed `#6b7280` (dash 4/3) lines at x = 12s and 16s, 12px `#6b7280` label "board locked" between them at the top.
- **Annotation (bold 13px `#2a78d6`, near x=3s, y=60):** "6 reads at once, zero conflicts".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Timing the Lunch Rush: 44 Seconds vs 14

**Tags:** `worked example` (blue), `hand-check` (green)

- **The minute** — one lunch-rush minute brings 18 customer glances at 2 seconds each and 2 price updates at 4 seconds each
- **One-at-a-time** — a plain lock serializes everything: 18×2 + 2×4 = 36 + 8 = 44 seconds of board time
- **Readers share** — 6 customers fit in front of the board, so 18 glances run as 3 batches: 3×2 = 6 seconds
- **Writers alone** — the 2 updates still run solo either way: 2×4 = 8 seconds, unchanged by sharing
- **The total** — 6 + 8 = 14 seconds versus 44 — letting reads overlap saves 30 seconds with identical work

*Example (italic):* The same 20 requests occupy the board for 44 seconds under a plain lock and 14 seconds under a read-write lock — the writes cost 8 seconds in both, only the reads compress.

**Key point:** A read-write lock speeds up only the read side — 36 seconds of reading becomes 6 — while write time stays exactly the same, so it pays off precisely when reads dominate.

### Visualization (canvas `c2`, 720×300)

Two horizontal stacked bars comparing total board time: plain lock (reads 36s + writes 8s) vs read-write lock (reads 6s + writes 8s), on a shared seconds axis.

- **Title (bold 15px, `#1a5276`, top center):** "Same 20 Requests: 44s Serialized, 14s with Shared Reads".
- **Axis:** horizontal 2px `#999` baseline at y=245 from x=180 to x=650; scale 10px per second, 12px `#444` tick labels at 0/10/20/30/40s; left-aligned 12px `#444` row labels at x=20.
- **Row 1 (y=95), "plain lock":** blue `rgba(42,120,214,0.30)` segment width 360 (reads, 36s) with 2px `#2a78d6` border, then orange solid `#d95926` segment width 80 (writes, 8s); 12px labels "reads 36s" and "writes 8s" inside/above; bold 12px `#2c3e50` total "44s" at bar end.
- **Row 2 (y=185), "read-write lock":** blue segment width 60 (reads, 6s), orange segment width 80 (writes, 8s); labels "reads 6s", "writes 8s"; bold 12px `#008300` total "14s" at bar end.
- **Bar style:** 34px tall, no gaps between segments.
- **Annotation (bold 13px green `#008300`, near x=420, y=185):** "only the reads compress — writes cost 8s either way".
- **Caption (12px `#444`, bottom right):** "glance and update times illustrative; arithmetic exact".

## Dashboards Read While the Nightly Job Writes

**Tags:** `where it's used` (blue), `databases` (green), `pipelines` (orange)

- **The dashboard** — 40 analysts refresh a sales dashboard all day; one ETL job rewrites the table each night
- **Databases** — shared vs exclusive locks (and MVCC snapshots) are the readers-writers pattern under every SELECT and UPDATE
- **Feature stores** — 100 model servers read feature values while one training job publishes the new version
- **Config objects** — every request thread reads a config; a deploy writes it — the classic in-process read-write lock
- **Without it** — reads either block each other for no reason, or a read races the write and returns half-updated rows

*Example (italic):* An analyst who queries mid-load sees January totals with February's rows half-inserted — a torn read at table scale.

**Key point:** Whenever many consumers read a shared table, cache, or config while one process updates it, you are inside the readers-writers problem whether you named it or not — so schedule the writer where the readers are fewest.

### Visualization (canvas `c3`, 720×300)

Line chart of dashboard read queries per hour across a 24-hour day, with the nightly write window shaded where reads bottom out.

- **Title (bold 15px, `#1a5276`, top center):** "One Writer per Day: Put It Where the Readers Aren't".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hour 0 to 23 with 12px `#444` tick labels every 4 hours ("0h" to "20h"); y = queries/hour 0 to 80, gridlines `#e5e9ef` at 20/40/60.
- **Reads line:** blue `#2a78d6` 3px line with 3px dots through hourly reads `[4, 2, 1, 1, 2, 3, 8, 20, 45, 60, 70, 66, 58, 62, 68, 64, 55, 40, 25, 15, 10, 8, 6, 5]` (hours 0–23).
- **Write window:** orange band `rgba(217,89,38,0.15)` from hour 2 to hour 4, full plot height, with 2px dashed `#d95926` edges and bold 12px `#d95926` label "ETL writes 2–4am" at its top.
- **Peak marker:** 12px `#6b7280` label "70 reads/hr at 10am" beside the hour-10 point.
- **Annotation (bold 13px orange `#d95926`, near hour 5, y=90):** "1–2 reads/hr in the write window vs 70 at peak".
- **Caption (12px `#444`, bottom right):** "hourly query counts illustrative".

## The Manager Who Never Gets the Chalk

**Tags:** `common mistake` (red), `starvation` (orange)

- **Reader preference** — the naive rule "let any reader in whenever a reader is already inside" feels harmless
- **The stream** — at lunch a new customer starts reading every few seconds, so someone is always at the board
- **The wait** — the manager, waiting for a moment with zero readers, never gets the chalk: writer starvation
- **The stale price** — the whole queue keeps reading yesterday's price while today's correction sits waiting
- **The fix** — writer-preference or fair (FIFO) locks: once a writer queues, newly arriving readers wait behind it

*Example (italic):* The manager arrives at 12:00:05 to fix a price; under reader-preference the active-reader count never touches zero, and 55 seconds later the fix is still waiting.

**Common mistake:** Assuming "readers can't hurt anything" — an endless stream of overlapping readers can block the writer forever, so a real read-write lock must decide who yields, and reader-preference silently starves writers.

### Visualization (canvas `c4`, 720×300)

Step line of the active-reader count over 60 seconds that never touches zero, with a marker for the writer's arrival and a growing red "still waiting" bar beneath.

- **Title (bold 15px, `#1a5276`, top center):** "Reader-Preference: the Count Never Hits 0, the Writer Never Starts".
- **Axes:** origin x=60, baseline y=225, plot width 600, plot height 160; x = seconds 0 to 60 with 12px `#444` tick labels every 10s; y = active readers 0 to 6, gridlines `#e5e9ef` at 2/4/6.
- **Reader step line:** blue `#2a78d6` 3px step line through seconds `[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]`, active readers `[2, 3, 4, 3, 5, 4, 3, 4, 5, 4, 3, 4, 3]` — minimum value 2, never 0.
- **Zero line callout:** 2px dashed `#008300` horizontal line at readers=0 with 12px `#008300` label "writer needs this: 0 readers" at its left end.
- **Writer arrival:** vertical dashed `#6b7280` (dash 4/3) line at x=5s, 12px `#6b7280` label "writer arrives 12:00:05" at its top.
- **Waiting bar:** red solid `#e74c3c`, 12px tall, at y=250, spanning seconds 5 to 60, bold 12px `#e74c3c` label "writer still waiting: 55s and counting" below its right end (baseline y=274).
- **Annotation (bold 13px red `#e74c3c`, near x=30s, y=70):** "overlapping readers starve the writer".
- **Caption (12px `#444`, bottom right):** "reader counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); reader/writer intervals in c1, the 18×2s + 2×4s lunch-rush workload in c2 (44s vs 14s totals are exact arithmetic on those invented times), the 24 hourly query counts in c3, and the 13-point active-reader step series in c4 are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
