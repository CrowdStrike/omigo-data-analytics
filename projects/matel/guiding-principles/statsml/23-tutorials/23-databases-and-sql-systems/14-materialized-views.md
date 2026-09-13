# Materialized Views

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Materialized Views

**Subtitle:** A materialized view stores a query's answer as a real table you refresh on a schedule — reads become instant, but the answer is only as fresh as the last refresh

## The Dashboard That Adds Up 50 Million Receipts

**Tags:** `core idea` (blue), `precompute` (green), `orders table` (orange)

- **The table** — a coffee-shop chain's orders table holds 50 million rows, one per receipt
- **The query** — the revenue dashboard runs `SUM(amount) GROUP BY day`, scanning all 50M rows
- **The wait** — every dashboard load repeats that scan and takes about 12 seconds
- **The copy** — a materialized view stores the query's result: just 365 rows, one per day
- **The trade** — reads drop to 0.05 seconds, but the view is only as fresh as its last refresh
- **The definition** — a materialized view is a saved query result kept as a physical table

*Example (italic):* The hourly refresh scans the 50M rows once at 2:00pm; every dashboard load until 3:00pm just reads the 365 precomputed rows.

**Key point:** Precompute vs recompute — a materialized view pays the big query once per refresh instead of once per read, and staleness is the price.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: a live query recomputing from the base table vs a dashboard reading a precomputed materialized view, shown as boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Recompute Every Load vs Precompute Once, Read Many Times".
- **Row 1 (y=95), label 12px `#444` at x=20:** "live query"; blue `#2a78d6` rounded box at x=140 labeled "dashboard load" (12px), 3px arrow to an orange `#d95926` box at x=340 labeled "scan 50M rows", arrow to a box at x=560 labeled "answer in 12 s" with bold 12px orange "every single load".
- **Row 2 (y=205), label:** "materialized view"; green `#008300` rounded box at x=140 labeled "hourly refresh: scan 50M once", 3px arrow to a blue box at x=380 labeled "view: 365 rows", arrow to a green box at x=560 labeled "answer in 0.05 s".
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, centered near y=270):** "the 50M-row scan happens once an hour, not once a load".
- **Caption (12px `#444`, bottom right):** "row counts and timings illustrative".

## Counting the Rows: 10 Billion vs 1.2 Billion a Day

**Tags:** `worked example` (blue), `query cost` (green), `staleness` (orange)

- **The loads** — analysts open the dashboard 200 times a day across the company
- **Recompute cost** — 200 loads × 50M rows = 10,000M (10 billion) rows scanned per day
- **Refresh cost** — 24 hourly refreshes × 50M rows = 1,200M (1.2 billion) rows per day
- **Read cost** — 200 loads × 365 view rows = 73,000 rows, small enough to ignore
- **Hand-check** — 10,000M ÷ 1,200M ≈ 8.3× fewer rows scanned with the view
- **The price** — hourly refresh means the numbers are up to 60 minutes stale, 30 on average

*Example (italic):* An order placed at 2:10pm is missing from every dashboard load until the 3:00pm refresh — at most a 60-minute wait.

**Key point:** The view wins whenever reads outnumber refreshes — here 200 reads vs 24 refreshes cuts daily scanning 8.3×, at the cost of a one-hour staleness window.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: total rows scanned per day under recompute vs precompute, with the arithmetic written on each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Rows Scanned per Day: 200 Live Loads vs 24 Refreshes + 200 Reads".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, max width 440; widths proportional to rows scanned.
- **Rows (top to bottom at y = 90, 170), each with a left-aligned 12px `#444` label at x=20:**
  - "live query — 200 × 50M": orange `#d95926` bar width 440, 12px `#444` label "10,000M rows/day" at bar end
  - "materialized view — 24 × 50M + 200 × 365": green `#008300` bar width 53, 12px `#444` label "1,200M rows/day" at bar end
- **Bar style:** 26px tall, fills `rgba(217,89,38,0.30)` / `rgba(0,131,0,0.30)` with solid 2px borders in the same hues.
- **Staleness note (12px `#6b7280`, under the green bar at y=210):** "price: answers up to 60 min old (avg 30)".
- **Annotation (bold 13px green `#008300`, right side near y=140):** "8.3× fewer rows scanned".
- **Caption (12px `#444`, bottom right):** "load counts illustrative; arithmetic exact".

## Fast Dashboards, Smaller Bills, Lighter Refreshes

**Tags:** `where it's used` (blue), `warehouse cost` (green), `incremental refresh` (orange)

- **Latency** — a 12-second dashboard gets abandoned; a 0.05-second one gets used all day
- **Warehouse cost** — cloud warehouses bill by data scanned, so 8.3× fewer rows is a smaller bill
- **Hot aggregates** — daily revenue, top products, active users: the same heavy query, read constantly
- **Incremental refresh** — instead of rescanning all 50M rows, apply only the new rows since last time
- **The math** — the chain adds ~60,000 orders an hour, so an incremental refresh reads 60k, not 50M
- **The ratio** — 50,000,000 ÷ 60,000 ≈ 833× less work per refresh when only changes are applied

*Example (italic):* The 3:00pm incremental refresh reads only the 60,000 orders placed since 2:00pm and adds them into the 365-row view.

**Key point:** Materialized views turn a per-read cost into a per-refresh cost, and incremental refresh shrinks the per-refresh cost to just the rows that changed.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: rows read by one hourly refresh under three strategies — no view (every load pays), full refresh, incremental refresh.

- **Title (bold 15px, `#1a5276`, top center):** "Rows Read per Hourly Refresh: Full Rescan vs Incremental".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 420; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 80, 150, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "no view — every load rescans": orange `#d95926` bar width 420, 11px label "50M × every load"
  - "full refresh — rescan hourly": blue `#2a78d6` bar width 300, 11px label "50M once/hour"
  - "incremental — new rows only": green `#008300` bar width 34, 11px label "60k once/hour"
- **Bar style:** 22px tall, fills `rgba(217,89,38,0.30)` / `rgba(42,120,214,0.30)` / `rgba(0,131,0,0.30)`, 11px width labels at bar ends.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=250):** "incremental refresh: 833× less work than a full rescan".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic (log-feel), row counts illustrative".

## The Report That Froze in March

**Tags:** `common mistake` (red), `stale data` (orange)

- **The mistake** — treating a materialized view as always-current; it only knows its last refresh
- **The silent failure** — the refresh job dies on March 31 and nobody wires up an alert
- **The frozen view** — the dashboard still loads instantly, proudly showing March's numbers in June
- **Why it hides** — a stale view returns fast, clean, plausible numbers; nothing looks broken
- **The fix** — show a "last refreshed" timestamp on the dashboard and alert when refreshes fail
- **The check** — compare the view's total against a live query on a sample day, once in a while

*Example (italic):* Revenue climbs from $1.10M to $1.31M a day between March and June, but the frozen dashboard reports $1.10M all spring — and loads in 0.05 s the whole time.

**Common mistake:** Confusing fast with fresh. A materialized view answers instantly no matter how old its contents are — staleness is invisible unless you surface the refresh time.

### Visualization (canvas `c4`, 720×300)

Line chart over six months: actual daily revenue rising vs what the dashboard shows after the refresh job silently dies at the end of March.

- **Title (bold 15px, `#1a5276`, top center):** "Refresh Job Dies March 31: the Dashboard Freezes, Reality Doesn't".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months Jan to Jun, 12px `#444` tick labels "Jan"–"Jun"; y = daily revenue $0.9M to $1.4M, gridlines `#e5e9ef` at 1.0/1.1/1.2/1.3.
- **Actual line:** green `#008300` 3px line through months `[Jan, Feb, Mar, Apr, May, Jun]`, revenue `[1.00, 1.05, 1.10, 1.18, 1.24, 1.31]` ($M/day).
- **Dashboard line:** red `#e74c3c` 3px dashed (dash 6/4) line, same months, revenue `[1.00, 1.05, 1.10, 1.10, 1.10, 1.10]` — tracks actual through March, then flat.
- **Failure marker:** vertical dashed `#6b7280` (dash 4/3) line at March, 12px `#6b7280` label "last successful refresh" at its top.
- **Labels:** bold 12px green "actual revenue" near May above the green line; bold 12px red "what the dashboard shows" near May below the red line.
- **Annotation (bold 13px red `#e74c3c`, near Jun, y=90):** "instant answers, three months old".
- **Caption (12px `#444`, bottom right):** "revenue figures illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 50M-row table, 12 s / 0.05 s timings, 200 loads/day, 60k orders/hour, and revenue series `[1.00, 1.05, 1.10, 1.18, 1.24, 1.31]` vs frozen `[1.00, 1.05, 1.10, 1.10, 1.10, 1.10]` are invented and labeled illustrative; the derived figures (10,000M vs 1,200M rows/day, 8.3×, 833×, 60-min staleness window) follow exactly from that arithmetic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
