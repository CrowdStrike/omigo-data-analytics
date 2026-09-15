# The Life of an Analytics Event

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Life of an Analytics Event

**Subtitle:** One "Add to cart" tap travels eight hops — SDK, beacon, collector, enricher, sessionizer, warehouse, model, dashboard — and it can die or double at every one of them

## One Tap at 2:14pm on a Tuesday

**Tags:** `core idea` (blue), `pipeline` (green), `eight hops` (orange)

- **The tap** — a shopper taps "Add to cart" at 2:14:00pm; the cart service records it instantly
- **The event** — the SDK builds a JSON: event name, timestamp, anonymous id, product properties
- **The batch** — events queue on the device and flush every 10 seconds to save battery and requests
- **The beacon** — a POST to the collection endpoint; one timeout, one retry, accepted on try two
- **The pipeline** — collector validates, queues to a stream; consumers enrich, sessionize, load
- **The clock** — the tap reaches the dashboard at 6:00am Wednesday: 15 h 46 min after it happened

*Example (italic):* The PM who opens the dashboard at 9:00am Wednesday is reading a tap that happened at 2:14pm Tuesday — after eight separate hops.

**Key point:** An analytics event is not written to the dashboard — it travels a multi-hop pipeline, and every hop adds delay plus a chance to lose or duplicate it.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: eight pipeline stages as boxes, arrows labeled with per-hop latencies, arrival clock under each box; hop latencies sum exactly to the end-to-end total.

- **Title (bold 15px, `#1a5276`, top center):** "One Tap's Journey: 2:14pm Tuesday to the 6:00am Dashboard".
- **Row 1 (boxes at y=70, height 46, width 140, radius 8, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border), x = 20, 200, 380, 560:** "SDK builds event / tap 2:14:00pm", "beacon POST / arrives 2:14:12", "collector → stream / 2:14:15", "enrich geo+device / 2:15:00" — two-line 12px `#2c3e50` labels.
- **Row 2 (boxes at y=185, same style), x = 20, 200, 380, 560:** "sessionize 30-min rule / 2:45pm", "warehouse batch load / 3:00pm", "dbt daily model / 5:20am Wed", "dashboard refresh / 6:00am Wed" — the dashboard box uses green fill `rgba(0,131,0,0.12)` and 2px `#008300` border.
- **Arrows:** seven 2px `#6b7280` arrows between the eight boxes, each with a bold 11px `#1a5276` hop-latency label above it. Row 1 arrows: "+12 s (10 s batch + retry)", "+3 s", "+45 s"; elbow connector from the row-1 last box down to the row-2 first box: "+30 min (session closes)"; row 2 arrows: "+15 min (batch load)", "+14 h 20 m (5am dbt run)", "+40 min (cache refresh)". Full hop sequence: +12 s, +3 s, +45 s, +30 min, +15 min, +14 h 20 m, +40 min — matching each box's arrival clock.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=275):** "eight hops, 15 h 46 min total — the big waits are the session window and the nightly model".
- **Caption (12px `#444`, bottom right):** "latencies illustrative; 1 min + 30 + 15 + 860 + 40 min sums exactly to 15 h 46 min".

## 1,000 Taps In, 872 on the Dashboard

**Tags:** `worked example` (blue), `exact arithmetic` (green), `loss modes` (orange)

- **Server truth** — the cart service logs 1,000 add-to-carts on Tuesday; this is the reference count
- **Ad blockers** — 80 shoppers block the SDK, so no event is ever created: 1,000 − 80 = 920
- **Lost in transit** — 30 tabs close before the 10-second flush: 920 − 30 = 890 beacons arrive
- **Schema rejects** — 25 events fail validation (bad timestamp, missing id): 890 − 25 = 865
- **Late batches** — 10 offline events arrive after the daily cutoff: 865 − 10 = 855 land today
- **Retry duplicates** — 17 retried beacons are counted twice: 855 + 17 = 872 on the dashboard

*Example (italic):* The dashboard reads 872 for Tuesday against 1,000 real taps — 87.2% of server truth, and every step of the gap is accounted for.

**Key point:** The dashboard number is the output of a subtraction-and-addition chain — 1,000 − 80 − 30 − 25 − 10 + 17 = 872 — not a direct count of taps.

### Visualization (canvas `c2`, 720×300)

Horizontal funnel bar chart: event count surviving at each hop, with red loss labels between bars and a red duplicate segment inflating the final bar.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Taps In, 872 on the Dashboard: Exact Loss at Every Hop".
- **Rows (bars 22px tall at y = 60, 95, 130, 165, 200, 235), left-aligned 12px `#444` labels at x=20:** "server truth 1,000", "SDK events 920", "beacons arrived 890", "passed validation 865", "landed today 855", "dashboard shows 872".
- **Bars:** start x=170, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge; pixel widths = count × 0.40: `400, 368, 356, 346, 342, 349`; the last bar is drawn as blue width 342 plus a solid red `#e74c3c` segment of width 7 at its right end.
- **Loss labels (bold 12px red `#e74c3c`, just right of each bar end, between consecutive rows):** "−80 ad blockers", "−30 lost in transit", "−25 schema rejects", "−10 late (counted tomorrow)", and on the last row "+17 retry duplicates" beside the red segment.
- **Annotation (bold 13px magenta `#d55181`, bottom right near y=270):** "872 = 1,000 − 80 − 30 − 25 − 10 + 17".
- **Caption (12px `#444`, bottom left):** "loss counts illustrative; arithmetic exact".

## Why the Dashboard Never Matches the Server

**Tags:** `where it's used` (blue), `reconciliation` (green)

- **Two systems** — the cart service counts what happened; analytics counts what survived the pipe
- **Skewed loss** — ad-blocker use varies by audience, so the undercount differs across segments
- **Trends survive** — a steady ~13% loss cancels out of week-over-week and A/B comparisons
- **Levels don't** — conversion or revenue-per-visitor built on raw event counts is silently biased
- **Reconcile** — compare one metric to a server-side source monthly to learn your actual loss rate

*Example (italic):* Finance asks why analytics shows 872 add-to-carts while the order system shows 1,000 — both are right about what they measure.

**Key point:** Use the dashboard for direction and comparison, server-side data for absolute counts — and measure the gap between them instead of assuming it is zero.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: seven days of add-to-carts, server truth vs dashboard count side by side, showing a parallel gap that leaves the weekly shape intact.

- **Title (bold 15px, `#1a5276`, top center):** "A Week of Add-to-Carts: Server Truth vs Dashboard".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 180; y = 0 to 1,500 with gridlines `#e5e9ef` at 500 and 1,000 (12px `#444` labels); x = day labels "Mon"–"Sun" centered under each group (12px `#444`).
- **Bars:** for day i (0–6), server bar at x = 75 + i×86 width 30, dashboard bar at x = 109 + i×86 width 30; heights = value × 0.12 px above the baseline.
- **Server series (blue fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` edge):** `[1000, 1040, 980, 1100, 1210, 1490, 1380]`.
- **Dashboard series (green fill `rgba(0,131,0,0.25)`, 2px `#008300` edge):** `[872, 907, 855, 959, 1055, 1299, 1203]`.
- **Legend (12px, top right inside plot):** blue swatch "server truth", green swatch "dashboard".
- **Annotation (bold 13px orange `#d95926`, near x=200, y=70):** "gap steady at ~13% — trends trustworthy, levels not".
- **Caption (12px `#444`, bottom right):** "weekly counts illustrative; dashboard ≈ 0.87 × server by construction".

## Reading a Dashboard Count as Exact Truth

**Tags:** `common mistake` (red), `late data` (orange)

- **The mistake** — reading Wednesday 6am's 872 as a final, exact count of Tuesday's taps
- **Numbers move** — the 10 late offline events backfill over two days: 872 → 879 → 882
- **Never truth** — the settled 882 still carries 17 duplicates and misses 135 blocked or lost events
- **The alarm** — a "drop" read at 6am is often late data still in flight, not user behavior
- **The fix** — freeze reports only after the settle window; annotate dashboards with known loss modes

*Example (italic):* The 6:00am read says 872; by Friday the same Tuesday shows 882 — anyone who screenshotted Wednesday's number is quoting a stale count.

**Common mistake:** Treating a dashboard count as an exact fact. It is a pipeline output with known loss and duplication modes — quote it with its settle window and reconciliation gap, never as truth.

### Visualization (canvas `c4`, 720×300)

Line chart: the same Tuesday's count as read on five successive mornings, settling upward as late batches land, against a dashed server-truth reference line it never reaches.

- **Title (bold 15px, `#1a5276`, top center):** "Tuesday's Count as Read Each Morning: 872 → 879 → 882".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y zoomed to 840–1,020 (map: y = 245 − (value − 840) × 1 px), gridlines `#e5e9ef` at 880, 920, 960, 1,000 with 12px `#444` labels; x = five morning reads labeled "Wed", "Thu", "Fri", "Sat", "Sun" (12px `#444`), evenly spaced 120px apart starting x=100.
- **Reading line:** blue `#2a78d6` 3px line with 5px filled dots through readings `[872, 879, 882, 882, 882]`, 12px `#1a5276` value labels above each dot.
- **Truth line:** dashed `#6b7280` (dash 6/4) horizontal line at value 1,000, 12px `#6b7280` label "server truth 1,000" above its right end.
- **Backfill labels (bold 12px green `#008300`, above the first two segments):** "+7 late" between Wed and Thu, "+3 late" between Thu and Fri.
- **Annotation (bold 13px red `#e74c3c`, near x=380, y=110):** "gap never closes: 882 = 1,000 − 135 lost + 17 dupes".
- **Caption (12px `#444`, bottom right):** "y-axis zoomed 840–1,020; settle values exact given this page's loss counts".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); hop latencies, funnel loss counts, and weekly totals are invented and labeled illustrative; the latency sum (15 h 46 min), the funnel chain (1,000 − 80 − 30 − 25 − 10 + 17 = 872), the settle sequence (872 + 7 + 3 = 882), and 882 = 1,000 − 135 + 17 are exact arithmetic and must stay internally consistent across text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
