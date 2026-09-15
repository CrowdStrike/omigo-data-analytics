# Druid & Pinot

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Druid & Pinot

**Subtitle:** Real-time OLAP stores that answer aggregations over event streams in under a second, on data that is only seconds old — the combination neither a warehouse nor a stream delivers

## The Ops Dashboard Watching the Last Five Minutes

**Tags:** `core idea` (blue), `real-time OLAP` (green), `event streams` (orange)

- **The dashboard** — a website ops team watches clicks sliced by country, refreshed every few seconds
- **The window** — every refresh asks: click counts per country over the last 5 minutes, right now
- **The stream** — click events pour into a Kafka topic; Druid and Pinot ingest straight from it
- **The freshness** — a click at 2:00:00pm is queryable on the dashboard about three seconds later
- **The warehouse gap** — a batch-loaded warehouse would show that same click an hour late

*Example (italic):* Traffic from Germany collapses at 2:03pm after a bad deploy; the on-call sees the dip within seconds and rolls back — with hourly batch loads the drop would surface at 3pm.

**Key point:** Druid and Pinot exist for exactly one job: sub-second aggregations over event streams that are fresh to the last few seconds, ingested directly from Kafka.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram tracing one click event: the batch-warehouse path (visible in an hour) vs the streaming OLAP path (visible in seconds).

- **Title (bold 15px, `#1a5276`, top center):** "One Click at 2:00:00pm: When Can the Dashboard See It?".
- **Row 1 (y=95), label 12px `#444` at x=20:** "warehouse path"; blue `#2a78d6` rounded box at x=150 labeled "click 2:00:00pm" (12px), 3px arrow to a mute `#6b7280` box at x=340 labeled "batch file (waits)", 3px arrow to an orange `#d95926` box at x=530 labeled "hourly load 3:00pm" with bold 12px orange "visible ~60 min later".
- **Row 2 (y=205), label:** "Druid / Pinot path"; blue box at x=130 "click 2:00:00pm", arrow to a blue box at x=310 "Kafka topic +0.2s", arrow to a green `#008300` box at x=480 "ingested +2s", arrow to a green box at x=620 "queryable +3s" with bold 12px green "✓".
- **Box style:** 110–150px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(107,114,128,0.12)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, centered near y=270):** "fresh to the last few seconds — that is the whole point".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Slicing Five Minutes of Clicks by Country

**Tags:** `worked example` (blue), `time segments` (green), `inverted index` (orange)

- **The layout** — events land in columnar, time-partitioned segments, one per minute, indexed on write
- **The query** — clicks by country over the last 5 minutes = touch only the 5 newest segments
- **The index** — an inverted index on the country column jumps straight to each country's rows
- **Pre-aggregation** — rollup at ingest stores per-minute counts per country, not raw click rows
- **Hand-check** — US per minute: 8,200 + 8,500 + 8,900 + 8,600 + 8,800 = 43,000 clicks (exact)
- **The slice** — US 43,000, India 27,500, Germany 12,300, Brazil 9,800, Japan 7,400 = 100,000 total (exact)

*Example (italic):* At 2:05pm the dashboard shows the 5-minute country slice — 100,000 clicks led by US at 43,000 — computed from just five 1-minute segments in about 90 ms.

**Key point:** Time partitioning prunes the scan to the queried window and the inverted index prunes it to each country's pre-aggregated rows — that double pruning is why the slice takes milliseconds, not minutes.

### Visualization (canvas `c2`, 720×300)

Bar chart of clicks per country over the last 5 minutes, with the US sum spelled out as the hand-check.

- **Title (bold 15px, `#1a5276`, top center):** "Clicks by Country, Last 5 Minutes: Five Segments, One GROUP BY".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = clicks 0 to 50,000, gridlines `#e5e9ef` at 15,000 / 30,000 / 45,000 with 12px `#444` labels "15k" / "30k" / "45k"; x = five bars centered at x = 130, 250, 370, 490, 610.
- **Bars:** width 80, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; heights from values `[43000, 27500, 12300, 9800, 7400]`; 12px `#444` value labels above each bar ("43,000", "27,500", "12,300", "9,800", "7,400"); 12px `#444` tick labels below: "US", "India", "Germany", "Brazil", "Japan"; the US bar outlined 2px green `#008300`.
- **Annotation (bold 13px green `#008300`, upper right near x=420, y=55):** "US = 8,200+8,500+8,900+8,600+8,800 = 43,000".
- **Annotation 2 (bold 12px violet `#4a3aa7`, near x=420, y=80):** "whole slice: 100,000 clicks in ~90 ms".
- **Caption (12px `#444`, bottom right):** "counts and latency illustrative; sums exact".

## The Niche Between the Warehouse and the Stream

**Tags:** `where it's used` (blue), `user-facing analytics` (green)

- **Druid's origin** — built at Metamarkets to power interactive ad-analytics dashboards
- **Pinot's origin** — built at LinkedIn for "who viewed your profile", analytics shown to every member
- **Warehouses** — answer aggregations fast, but are batch-loaded: the newest rows are hours old
- **Streams** — Kafka is fresh to the millisecond, but a topic is a pipe: you can't GROUP BY a pipe
- **Concurrency** — 3,000 open ops dashboards refreshing every 10s fire 300 queries/sec, all day (exact)

*Example (italic):* "Who viewed your profile" runs an analytics query for each of millions of members opening the page — a concurrency load no analyst-facing warehouse is built to take.

**Key point:** The niche is fresh AND fast AND concurrent: seconds-old data, sub-second answers, thousands of simultaneous queries — both systems were open sourced by companies that hit that exact wall.

### Visualization (canvas `c3`, 720×300)

Quadrant scatter: data staleness (x, fresher to the left) vs aggregation-query latency (y, faster at the bottom), placing the Kafka stream, the warehouse, and Druid/Pinot.

- **Title (bold 15px, `#1a5276`, top center):** "Fresh AND Fast: the Gap Druid and Pinot Fill".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x-axis 12px `#444` labels "seconds", "minutes", "hours" at x = 150, 360, 570 (staleness of newest queryable row, log-feel by hardcoded pixel positions); y-axis 12px `#444` labels "ms", "seconds", "no ad-hoc query" at y = 220, 150, 80 (latency of a slice-by-country aggregation).
- **Quadrant shading:** light green `rgba(0,131,0,0.06)` rectangle over the fresh-and-fast corner (x 70–260, y 190–245).
- **Points (10px radius filled circles, bold 12px labels beside each):**
  - Druid / Pinot: green `#008300` at (150, 215), label "Druid / Pinot" to the right
  - Kafka stream: blue `#2a78d6` at (130, 90), label "Kafka — fresh, but a pipe: no GROUP BY" to the right
  - Warehouse: orange `#d95926` at (540, 160), label "warehouse — fast enough, hours stale" to the left
- **Annotation (bold 13px green `#008300`, inside the shaded corner near x=90, y=185):** "live dashboards live here".
- **Caption (12px `#444`, bottom right):** "positions schematic, illustrative".

## Not a Faster Warehouse

**Tags:** `common mistake` (red), `right tool` (orange)

- **The confusion** — teams read "sub-second SQL" and try to replace the warehouse with Druid or Pinot
- **The trade** — segments, rollup, and inverted indexes are pre-built for anticipated aggregation shapes
- **What suffers** — large arbitrary joins and exploratory full-history SQL are the warehouse's job
- **The pairing** — the common architecture runs both: stream to Druid/Pinot, batch to the warehouse
- **The tell** — if the query has no time window and no dashboard behind it, it belongs elsewhere

*Example (italic):* The ops team's "last 5 minutes by country" slice takes 90 ms in Pinot, but the analyst's 6-month join against the customer table still runs in the warehouse.

**Common mistake:** Treating Druid or Pinot as a general-purpose warehouse. They buy sub-second latency by pre-organizing data around time-windowed aggregations — ad-hoc joins over full history is the workload they deliberately gave up.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: two query shapes, each run on the streaming OLAP store and on the warehouse, showing where each system wins.

- **Title (bold 15px, `#1a5276`, top center):** "Two Queries, Two Right Answers".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 420; log-feel by hardcoded pixel widths, not a real log axis.
- **Group 1 (rows at y = 80 and 115), 12px `#444` group label at x=20, y=70:** "last-5-min clicks by country":
  - "Druid / Pinot — 0.09 s": green `#008300` bar width 12
  - "warehouse — 8 s": orange `#d95926` bar width 220
- **Group 2 (rows at y = 185 and 220), group label at x=20, y=175:** "6-month ad-hoc join to customer table":
  - "warehouse — 40 s": green `#008300` bar width 300
  - "Druid / Pinot": no bar; bold 12px red `#e74c3c` label at x=260 "not the workload it is built for"
- **Bar style:** 14px tall, solid fill, 11px `#444` seconds label at each bar end; row labels 12px `#444` right-aligned ending at x=240.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "run both: stream for the dashboard, warehouse for the analyst".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); click counts, timings, and quadrant positions are invented and labeled illustrative; the US per-minute sum (8,200+8,500+8,900+8,600+8,800 = 43,000), the five-country total (43,000+27,500+12,300+9,800+7,400 = 100,000), and the concurrency arithmetic (3,000 dashboards / 10s refresh = 300 queries/sec) are exact. Origins (Druid at Metamarkets for ad analytics, Pinot at LinkedIn for "who viewed your profile") are publicly documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
