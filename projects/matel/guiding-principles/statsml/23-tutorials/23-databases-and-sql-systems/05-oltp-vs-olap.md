# OLTP vs OLAP

**Page type:** detail page (tutorial page: h2 card-sections, each a two-column table.layout — text left 50%, canvas right 50%)
**HTML title tag:** OLTP vs OLAP

**Subtitle:** Running the shop touches one row at a time; studying the shop scans everything — two jobs so different they get two databases

## Running the Shop vs Studying the Shop

Tags: `core idea` (blue), `running example` (green)

- **Running it (OLTP)** — insert order 1009, look up customer 3: one row per query
- **Studying it (OLAP)** — "average order value by month, all history": every row per query
- **OLTP traffic** — thousands of tiny queries a day, each needing milliseconds
- **OLAP traffic** — a handful of huge queries a day, each allowed seconds or minutes
- **Same data** — the same 8 orders serve both; only the access pattern differs

*Example (italic):* The checkout writes one order row; the quarterly review reads all of them — the till and the telescope.

**Key point:** OLTP = many tiny touches (transactions). OLAP = few giant scans (analysis). The workload, not the data, is what differs.

### Visualization (canvas `c1`, 720×300)

Split-panel diagram: the same 8-row orders table shown twice — OLTP highlights one row, OLAP highlights all rows.

- **Title (bold 15px `#1a5276`, top center):** "Same 8 Orders, Two Very Different Queries".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3) at x=360 from y=38 to y=286.
- **Orders data** (order_id, cust, amount): 1001/1/$8, 1002/2/$15, 1003/1/$5, 1004/3/$8, 1005/2/$12, 1006/4/$15, 1007/1/$12, 1008/9/$5.
- **Left panel — heading (bold 13px `#2c3e50`, centered at x=180):** "OLTP: fetch order 1004". Table drawn at (75, 62), columns order_id/cust/amount (widths 70/50/66, row height 20), header `#1a5276` white bold text, alternating white/`#f4f6f8` rows; only the 1004 row highlighted `rgba(39,174,96,0.25)` and bold. Below: bold 12px green `#008300` "1 row touched" (y=262), muted 12px "milliseconds; thousands of these a day" (y=280).
- **Right panel — heading:** "OLAP: AVG(amount) by month" (centered x=540). Same table at (435, 62) with all 8 rows highlighted `rgba(42,120,214,0.15)`. Below: bold 12px blue `#2a78d6` "all 8 rows touched" (y=262), muted 12px "seconds at scale; a few of these a day" (y=280).

## One Report, Every Row: Average Order by Month

Tags: `worked example` (green)

- **The question** — is the average order getting bigger or smaller month by month?
- **January** — orders of $8, $15, $5: average $9.33
- **February** — orders of $8, $12, $15: average $11.67
- **March** — orders of $12, $5: average $8.50
- **The cost** — all 8 rows were read to produce 3 numbers; at scale, all 80 million

SQL block:

```sql
SELECT month, AVG(amount)
FROM   orders
GROUP  BY month;   -- reads every row, returns 3
```

*Example (italic):* No index saves you here — an average needs every amount, so the scan is the work, not a failure to avoid it.

**Key point:** Analytics compresses many rows into few numbers. Reading "everything" is the job description, so OLAP engines are built to scan fast.

### Visualization (canvas `c2`, 720×300)

Table-to-bars diagram: 8-row month-colored orders table, a GROUP BY arrow, then three monthly average bars.

- **Title (bold 15px `#1a5276`, top center):** "8 Rows In, 3 Numbers Out".
- **Left table** at (50, 48), columns order_id/amount/month (widths 70/66/60, row height 24), rows: 1001/$8/Jan, 1002/$15/Jan, 1003/$5/Jan, 1004/$8/Feb, 1005/$12/Feb, 1006/$15/Feb, 1007/$12/Mar, 1008/$5/Mar. Row backgrounds by month: Jan `rgba(42,120,214,0.15)`, Feb `rgba(25,158,112,0.18)`, Mar `rgba(201,133,0,0.18)`.
- **Arrow:** ink `#1a5276` 2.5px right-pointing arrow from (270,160) to (330,160), with bold 12px label "GROUP BY" above it (centered x=300, y=144).
- **Bar chart** (plot x=370, width 300, baseline y=224, chart height 140, y-scale max 14, bar width 64):
  - Jan: $9.33, blue `#2a78d6`, calc caption "(8+15+5)/3".
  - Feb: $11.67, aqua `#199e70`, calc caption "(8+12+15)/3".
  - Mar: $8.50, yellow `#c98500`, calc caption "(12+5)/2".
  - Value labels bold 13px `#2c3e50` above each bar ("$9.33" etc.), month labels 12px below baseline, calc formulas 11px muted below those. Thin gray `#999` baseline.
- **Chart heading (bold 13px orange `#d95926`, centered over bars, y=60):** "average order value by month".
- **Bottom caption (12px muted, centered, y=282):** "every row was read to build these three bars".

## Rows on Disk vs Columns on Disk

Tags: `core idea` (blue), `where it's used` (orange)

- **Row store (OLTP)** — each order's five values sit together: perfect for "fetch order 1004"
- **Column store (OLAP)** — all amounts sit together, all dates together
- **The report needs** — amount and month: 2 of the 5 columns
- **Row store cost** — reads whole rows anyway; 3 columns of waste per row
- **Column store cost** — rebuilding one full order means visiting 5 separate places

*Example (italic):* Warehouses like the column layout so much that "columnar" is practically a synonym for "analytics database".

**Key point:** Storage layout is a bet on the workload: rows bet on "one whole thing at a time", columns bet on "one attribute across everything".

### Visualization (canvas `c3`, 720×300)

Split-panel cell-grid diagram comparing row store and column store reads (8 rows × 5 columns: id, cust, prod, amt, date; the report needs amt and date).

- **Title (bold 15px `#1a5276`, top center):** "What the AVG-by-Month Report Actually Reads".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3) at x=360 from y=38 to y=286.
- **Left panel — heading (bold 13px `#2c3e50`, centered x=180):** "row store: whole rows come off disk". Grid of 8×5 cells starting at (60, 66), cell 48px wide (46px drawn) × 20px tall with 2px vertical gaps; needed columns (amt, date) filled `rgba(42,120,214,0.30)`, the other three filled `rgba(231,76,60,0.10)` (pink). Column labels (11px muted) under the grid: id, cust, prod, amt, date. Captions: bold 12px red `#e74c3c` "reads 40 cells to use 16" (y=272), 11px muted "(pink cells: hauled off disk, then ignored)" (y=288).
- **Right panel — heading (centered x=540):** "column store: only the needed columns". Same grid starting at (420, 66); needed columns filled `rgba(39,174,96,0.35)`, others `#f0f2f5` (grey). Column labels: needed ones bold 11px green `#008300`, others 11px muted. Captions: bold 12px green "reads 16 cells, uses 16" (y=272), 11px muted "(grey columns never leave the disk)" (y=288).

## One Database Doing Both Jobs Does Both Badly

Tags: `common mistake` (red), `why it matters` (orange)

- **The setup** — analysts run their big scans on the same database as the checkout
- **The symptom** — every Monday 9am, checkout slows just as the weekly report runs
- **Why** — one giant scan hogs disk and memory that a thousand tiny queries needed
- **The fix** — copy data out on a schedule; shop on OLTP, reports on the warehouse
- **The price** — the warehouse lags the shop by minutes or hours; know your copy's age

*Example (italic):* A data scientist's innocent SELECT over all history once made real customers wait at a real checkout.

**Common mistake:** Pointing analytics at the production database. It works until the first big query — separate the till from the telescope.

### Visualization (canvas `c4`, 720×300)

Line chart: checkout response time by hour spiking during the report window (illustrative).

- **Title (bold 15px `#1a5276`, top center):** "Checkout Response Time on Report Day (illustrative)".
- **Axes:** gray `#999` L-shaped axes; padding top 52, bottom 52, left 70, right 30. Y gridlines (`#e5e9ef`) and right-aligned 12px muted labels at 0, 100, 200, 300, 400 ms; y-scale max 450.
- **X labels (12px `#2c3e50`, centered under points):** 6am, 7, 8, 9, 10, 11, 12pm, 1, 2.
- **Data (ms):** `[22, 20, 24, 380, 410, 30, 22, 25, 21]`.
- **Report window band:** `rgba(231,76,60,0.08)` fill spanning the 9–11 hour slots (points 3 to 5), full chart height.
- **Series:** blue `#2a78d6` line, 3px, with 4px-radius dots at each point; dots red `#e74c3c` when ms > 100 (the 9 and 10 points), otherwise blue.
- **Annotations:** bold 13px red, centered near the top of the band (two lines): "9–11am: the weekly all-history report runs" / "here — checkout waits 20x longer". Bold 12px blue, left-aligned right of the band: "~22 ms all day otherwise".
- **Bottom caption (12px muted, centered, y=292):** "one scan-everything query competing with thousands of one-row queries".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border), then `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each opening with `<b>` term in `#1a5276`), an optional `.sql` `<pre>` block, an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300 with `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `ul` 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem; `.sql` background `#f8f9fa`, left border 3px solid `#1a5276`, ui-monospace 0.8rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Palette:** primary blue `#1a5276` (ink), green `#27ae60`, red `#e74c3c`, orange `#e67e22`; chart palette object P: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Canvas:** intrinsic 720×300 attributes; scale by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). A shared `drawTable(ctx,x,y,cols,widths,rows,opt)` helper draws data tables with `#1a5276` header (white bold 12px text), alternating white/`#f4f6f8` rows, optional per-row background/bold overrides, and `#cfd8e0` 1px outer border. A shared ORDERS array holds the 8 order rows. No nav bar, no back/home links. In regenerated HTML, any card links use .html extensions.
