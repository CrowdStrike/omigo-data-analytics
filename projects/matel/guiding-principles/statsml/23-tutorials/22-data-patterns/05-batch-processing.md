# Batch Processing

**Page type:** detail page (tutorial topic page: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%; one section adds a monospace payload block under its canvas)
**HTML title tag:** Batch Processing

**Subtitle:** Let the data pile up all day, then process the whole pile at once on a schedule — like a truck that leaves once a night, full

## One Truck a Night: The 2am Report Job

**Tags:** `core idea` (blue), `running example` (green)

- **All day** — a shop's website collects orders; by midnight, 500,000 have piled up
- **Nothing yet** — during the day, no reports are computed; orders just accumulate
- **At 2:00am** — a scheduled job wakes up and reads all 500,000 orders in one go
- **By 3:15am** — it has written the daily revenue report; then it goes back to sleep
- **The truck model** — one full truck at night beats 500,000 bicycle couriers all day

*Example:* The 9am dashboard shows yesterday's sales — computed once, at 2am, from the whole pile.

**Key point:** Batch means collect first, process later, all at once — the schedule, not the data, decides when work happens.

### Visualization (canvas `c1`, 720×300)

Cumulative area/line chart: orders piling up over 24 hours, then a highlighted job window after midnight.

- **Title (bold 15px, `#1a5276`, top center):** "Orders Pile Up All Day — Nothing Is Processed Until 2:00am".
- **Data:** hourly orders (thousands), 24 values summing to 500: `[6,4,3,2,2,3,8,14,20,25,28,32,34,32,30,28,27,29,34,38,36,30,21,14]`, plotted as the cumulative sum.
- **Axes:** padding top 52, bottom 56, left 62, right 24; x spans 0 to 27.5 hours (00:00 through 03:30 next day); y scale max 520 (thousands); L-shaped axes in `#999`. X ticks (12px `#222`): 00:00, 06:00, 12:00, 18:00, 24:00, 03:15 (at hour 27.25). X-axis caption "time (next day after 24:00)" in `#444`; rotated y-axis label "orders collected (thousands)".
- **Job window:** shaded rectangle `rgba(217,89,38,0.15)` from hour 26 to 27.25 (2:00am–3:15am), full chart height; orange `#d95926` bold 13px labels inside near top: "job runs" / "2:00-3:15".
- **Cumulative curve:** blue `#2a78d6` line width 3, rising to 500 at hour 24 then flat to 27.5; fill under curve `rgba(42,120,214,0.12)`.
- **Midnight marker:** dashed `#bbb` vertical line at hour 24 (dash 5/4).
- **Annotations:** blue bold 12px right-aligned near the top of the curve "500,000 orders by midnight"; muted 12px "daytime: collect only" at around hour 7 mid-height.

## What the Job Actually Does With 500,000 Rows

**Tags:** `worked example` (green), `core idea` (blue)

- **Input** — yesterday's orders table: 500,000 rows, one per order, frozen at midnight
- **Step 1** — group the rows by store: 120 stores, each gets its own pile
- **Step 2** — sum the amounts in each pile: store 17's 4,210 orders add up to $71,320
- **Output** — a tiny table: 120 rows of (store, orders, revenue) that feeds the dashboard
- **Do it by hand** — same as sorting receipts into folders, then totaling each folder

*Example:* 500,000 input rows shrink to 120 output rows — batch jobs usually summarize, not copy.

**Key point:** A batch job is an ordinary computation over a frozen pile — read everything, aggregate, write the result.

### Visualization (canvas `c2`, 720×300)

Flow diagram: input table → GROUP BY step → output summary table, connected by orange arrows.

- **Title (bold 15px, `#1a5276`, top center):** "One Pass Over the Pile: 500,000 Rows In, 120 Rows Out".
- **Input box:** x=30, y=55, 200×190, fill `rgba(42,120,214,0.08)`, stroke blue `#2a78d6`; blue bold 13px heading "orders (yesterday)"; five monospace 11px rows in `#444`: "#483726  store 17  $18.50", "#483727  store 52  $64.00", "#483728  store 17   $9.75", "#483729  store 03  $22.00", "#483730  store 52   $5.25"; italic muted 11px "... 500,000 rows ..."; blue bold 12px "frozen at midnight".
- **Group step box:** x=292, y=105, 140×90, fill `rgba(217,89,38,0.10)`, stroke orange `#d95926`; orange bold 13px "GROUP BY store"; 12px `#444` lines "sum(amount)" and "count(*)". Orange arrows in and out at mid-height.
- **Output box:** x=494, y=68, 200×164, fill `rgba(0,131,0,0.08)`, stroke green `#008300`; green bold 13px heading "store_daily_revenue"; three monospace 11px rows: "store 03   1,882   $30,140", "store 17   4,210   $71,320", "store 52   3,077   $48,610"; italic muted 11px "... 120 rows, one per store ..."; green bold 12px "feeds the 9am dashboard".
- **Caption (orange bold 13px, bottom center):** "the whole job: read everything once, aggregate, write a small result".

### Payload block (under canvas `c2`)

Italic `.payload-note`: "Three of the 500,000 input rows — illustrative records."

Monospace `.payload` pre block (verbatim):

```
{ "order_id": 483726, "store": 17, "time": "2026-08-23T13:42:09", "amount": 18.50 }
{ "order_id": 483727, "store": 52, "time": "2026-08-23T13:42:11", "amount":  64.00 }
{ "order_id": 483728, "store": 17, "time": "2026-08-23T13:42:14", "amount":   9.75 }
                                 ...497 thousand more...
--- after the 2am job: one output row per store ---
{ "store": 17, "date": "2026-08-23", "orders": 4210, "revenue": 71320.00 }
```

## Why Batch Is the Right Default More Often Than It Sounds

**Tags:** `where it's used` (blue), `rule of thumb` (blue)

- **Cheap** — one machine for 75 minutes a night, instead of servers running all day
- **Simple** — plain code over a frozen input; no clocks, queues, or ordering puzzles
- **Always stale** — the answer is hours old by design; that is the price of the truck
- **Ask the question** — "who needs this number sooner than tomorrow morning?"
- **Usual answer** — reports, invoices, and model training are all fine a day late

*Example:* Monthly invoices tolerate 30 days of staleness; only the fraud check needs 0.2 seconds.

**Key point:** Most uses of data tolerate hours of staleness — start with batch, and earn your way into anything fancier.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of staleness tolerance per use case on a log scale, with a nightly-batch threshold line.

- **Title (bold 15px, `#1a5276`, top center):** "How Stale Can the Answer Be? (log scale)".
- **Layout:** padding top 52, bottom 46, left 190, right 30; x is log10(seconds) from 0.1 to 10,000,000; bar height 24, evenly spaced rows.
- **Rows** (label right-aligned 12px `#222`; bars filled at 0.6 alpha with 1.5px solid stroke in the same color; bold 12px duration label right of each bar):
  - "fraud check at swipe" — 0.2 s, magenta `#d55181`, label "0.2 s"
  - "exec dashboard" — 86,400 s, blue `#2a78d6`, label "1 day"
  - "ML training data" — 86,400 s, aqua `#199e70`, label "1 day"
  - "weekly email digest" — 604,800 s, violet `#4a3aa7`, label "7 days"
  - "monthly invoices" — 2,592,000 s, green `#008300`, label "30 days"
- **Threshold:** dashed orange `#d95926` vertical line (dash 6/4, width 2) at 86,400 s, labeled above in orange bold 12px "nightly batch: at most ~24h stale".
- **Axis caption (12px `#444`):** "staleness the use case tolerates → (longer is easier)".
- **Annotations:** green bold 13px "4 of 5 needs sit at or right of the line — batch is enough"; magenta bold 12px "only this one needs streaming" just below the fraud-check bar.

## The Confusion: "Stale" Doesn't Mean "Broken"

**Tags:** `common mistake` (red), `best practice` (green)

- **Not a bug** — a dashboard showing yesterday's sales is batch working exactly as designed
- **Frozen input** — yesterday's 500,000 orders never change after midnight
- **Safe reruns** — if the 2am run crashes, rerun at 6:30am and get the identical report
- **Same in, same out** — that rerun-safety is the superpower streaming struggles to match
- **Real bug** — the job silently not running at all; monitor "did it finish?", not "is it fresh?"

*Example:* The 2am run crashed; the 6:30am rerun read the same frozen pile and produced the same $8.4M total.

**Key point:** Because the input is frozen, a batch job can fail and simply run again — staleness buys you the right to retry.

### Visualization (canvas `c4`, 720×300)

Timeline diagram: crash at 2:00, rerun at 6:30, identical output — with a frozen-input band above the axis.

- **Title (bold 15px, `#1a5276`, top center):** "The Input Is Frozen, So a Rerun Gives the Identical Answer".
- **Timeline:** horizontal axis at y=150 spanning hours 0–8 (left pad 60, right pad 30), 2px `#999`; ticks with 12px labels: 00:00, 02:00, 04:00, 06:30, 08:00.
- **Frozen input band:** rectangle from x(0) to x(8), y=48, height 40, fill `rgba(42,120,214,0.10)`, stroke blue `#2a78d6` 1.5px; centered blue bold 12px text: "yesterday's 500,000 orders — frozen, never change after midnight".
- **Run markers:** magenta `#d55181` 7px dot on the axis at 02:00 with bold 13px labels above "run 1 starts 02:00" / "✖ crashes"; green `#008300` 7px dot at 06:30 with labels "rerun 06:30" / "✔ finishes". Dashed muted connectors from both dots up to the frozen band.
- **Output boxes (190×52 at y=200, fill `rgba(0,131,0,0.08)`):** under 02:00, stroked muted `#6b7280`, labeled "report it WOULD have made" plus bold 13px `#222` "total revenue: $8.4M"; under 06:30, stroked green, labeled "report it DID make" plus "total revenue: $8.4M"; solid muted connector from the 06:30 dot down to its box.
- **Caption (orange `#d95926` bold 13px, bottom center):** "same frozen input → same output: retrying is always safe".

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` (one `<tr>`; left `td.text-col` 50% width, right `td.viz-col` 50% width, cells padded 12px, no cell borders). Section 2's viz cell holds the canvas, then the `.payload-note` and `.payload` pre block.
- **Text column structure:** `.tags` row of colored pill spans (`.tag` — 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each starting with `<b>` in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) opening with `<strong>Key point:</strong>`.
- **Payload styles:** `.payload` — background `#f8f9fa`, left border 3px solid `#1a5276`, padding 10px, ui-monospace/Menlo 0.78em, `white-space: pre`, `overflow-x: auto`, line-height 1.45, left-aligned. `.payload-note` — 0.82em, `#666`, italic, left-aligned, margin 12px 0 -6px 0.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box; }`; h1 2rem `#1a5276`; subtitle `#666` 0.95rem. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
- **Canvas:** each canvas declared `width="720" height="300"`, CSS `width:100%`, border `1px solid #e0e0e0`, radius 4px; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
