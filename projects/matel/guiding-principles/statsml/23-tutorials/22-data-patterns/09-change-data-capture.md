# Change Data Capture

**Page type:** detail page (tutorial card-section layout: one h2 per section, two-column `table.layout` with 50% text / 50% viz)
**HTML title tag:** Change Data Capture

**Subtitle:** Instead of re-copying the whole database every night, tail its change log — every insert, update, and delete becomes an event you can stream anywhere

## Stop Re-Copying Five Million Rows Every Night

**Tags:** `core idea` (blue), `running example` (green)

- **The task** — keep the analytics warehouse in sync with the orders database
- **Old way** — every night, SELECT all 5,000,000 rows and reload the warehouse copy
- **The waste** — only 40,000 rows actually changed today; 99.2% of the copy was unchanged
- **The observation** — the database already writes every change to an internal log before applying it
- **CDC** — read that change log and ship only the 40,000 changes, as they happen

**Example (italic):** Like syncing a phrasebook by sending the 3 edited pages, not re-printing the whole book nightly.

**Key point:** Change Data Capture ships the diffs — the database's own change log already contains exactly what changed, in order.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart on a log scale comparing rows shipped per day.

- **Title (bold 15px ink `#1a5276`, top center):** "Rows Shipped per Day to Keep the Warehouse in Sync (log scale)"
- **Scale:** bar length is log10-scaled from 1,000 (min) to 10,000,000 (max) across the chart width; left padding 200px, right 130px. Thin gray `#999` vertical baseline at the left edge of the bars (x=200, from y=70 to y=230).
- **Bars (40px tall; fill in bar color at 0.55 alpha, 2px solid stroke; row label right-aligned bold 13px `#222` to the left; value label bold 13px in bar color to the right of the bar end):**
  | y | label | value | value text | color |
  |---|-------|-------|-----------|-------|
  | 90 | nightly full re-copy | 5,000,000 | 5,000,000 rows | `#d55181` (magenta) |
  | 170 | CDC: changes only | 40,000 | 40,000 events | `#008300` (green) |
- **Captions (bottom center):** bold 13px orange `#d95926` "99.2% of the nightly copy was rows that had not changed" (y=252); 12px mute `#6b7280` "and the copy arrives hours late — the change events arrive in seconds" (y=274).

## Three Database Operations, Three Events

**Tags:** `worked example` (green), `core idea` (blue)

- **09:14 INSERT** — order 1001 is created ($64, status "placed") → an insert event, with the new row
- **11:02 UPDATE** — order 1001 ships → an update event carrying BOTH the before and after row
- **11:41 DELETE** — test order 998 is removed → a delete event, with the row that vanished
- **Seconds later** — each event is applied to the warehouse; it now mirrors the database
- **In order** — the log preserves the sequence, so the warehouse never sees "shipped" before "placed"

**Example (italic):** The warehouse learned about the 11:02 shipment at 11:02:04 — not at 2am the next day.

**Key point:** One row operation becomes one event — insert carries the new row, update usually carries before AND after (log config dependent), delete carries what was removed.

### Visualization (canvas `c2`, 720×300)

Three-column flow diagram: database operation → change-log event → warehouse apply, one row per operation with arrows between boxes.

- **Title (bold 15px ink, top center):** "Each Row Operation Becomes One Change Event"
- **Column headers (bold 13px, y=52):** "orders database" (blue `#2a78d6`, x=130), "change log (events)" (ink `#1a5276`, x=380), "warehouse applies" (green `#008300`, x=622).
- **Rows (left box 190×52 fill `rgba(42,120,214,0.06)` with the row's color border, bold 12px op label + 12px `#444` description; middle box 206×52 fill `rgba(26,82,118,0.05)` ink border with monospace 11px `#222` event text; right box 174×52 fill `rgba(0,131,0,0.05)` green border with 12px `#444` text; colored arrows connect the boxes):**
  | y-center | op | database | change event | warehouse | color |
  |----------|----|----------|-------------|-----------|-------|
  | 90 | INSERT 09:14 | order 1001 created | insert: {1001, placed, $64} | row added 09:14:03 | `#199e70` (aqua) |
  | 160 | UPDATE 11:02 | 1001 placed → shipped | update: before + after | row updated 11:02:04 | `#4a3aa7` (violet) |
  | 230 | DELETE 11:41 | test order 998 removed | delete: {998, ...} | row removed 11:41:02 | `#d55181` (magenta) |
- **Caption (bold 13px orange, bottom center, y=285):** 'applied in log order, seconds behind the database — never "shipped" before "placed"'

**Payload note (italic, below canvas):** The 11:02 update as a change event — illustrative structure.

**Payload block (monospace, `#f8f9fa` background, left border 3px solid `#1a5276`):**

```
{ "op": "update",
  "table": "orders",
  "ts": "2026-08-24T11:02:00Z",
  "log_position": 88213472,
  "before": { "order_id": 1001, "status": "placed",  "amount": 64.00 },
  "after":  { "order_id": 1001, "status": "shipped", "amount": 64.00 } }
// insert events have "before": null
// delete events have "after": null
```

## The Bridge From Database-World to Stream-World

**Tags:** `where it's used` (blue), `best practice` (green)

- **Two worlds** — the app speaks "rows in a database"; analytics tooling speaks "events on a stream"
- **The bridge** — CDC turns every row change into a stream event with zero app-code changes
- **One log, many readers** — the same change stream feeds the warehouse, the search index, and the cache
- **Fresh warehouse** — tables lag the database by seconds, not by a nightly truck
- **Data science angle** — features computed on the change stream instead of on yesterday's snapshot

**Example (italic):** The team added a search index as a second reader of the same change stream — the orders app never knew.

**Key point:** CDC is how an ordinary database joins the streaming world — its change log, published, IS an event stream.

### Visualization (canvas `c3`, 720×300)

Left-to-right pipeline diagram: app → database → CDC reader → fan-out to three stream consumers.

- **Title (bold 15px ink, top center):** "The Bridge: Row Changes In, Stream Events Out"
- **App box:** (25, 120), 100×60, fill `rgba(107,114,128,0.08)`, mute `#6b7280` border; bold 12px mute "orders app", 11px `#444` "untouched". Mute arrow to the database.
- **Database box:** (169, 105), 150×90, fill `rgba(42,120,214,0.08)`, blue `#2a78d6` border; bold 13px blue "orders database", 12px `#444` "writes every change" / "to its internal log". Blue arrow to the CDC reader.
- **CDC reader box:** (363, 118), 110×64, fill `rgba(217,89,38,0.10)`, orange `#d95926` border; bold 13px orange "CDC reader", 11px `#444` "tails the log".
- **Fan-out consumers (156×52 boxes at x=539, fill `rgba(0,131,0,0.04)`, colored borders; bold 13px label + 12px `#444` sub; colored arrow from CDC reader (473,150) to each):**
  | y-center | label | sub | color |
  |----------|-------|-----|-------|
  | 70 | warehouse | seconds fresh | `#008300` (green) |
  | 150 | search index | added later | `#199e70` (aqua) |
  | 230 | cache | invalidated per change | `#4a3aa7` (violet) |
- **Annotation:** bold 12px ink two lines at (505, 92/108): "one change stream," / "many readers".
- **Caption (bold 13px orange, bottom center, y=282):** "database-world on the left, stream-world on the right — CDC is the bridge, app code untouched"

## The Confusion: Tailing the Log Is Not Polling the Table

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The lookalike** — "SELECT * WHERE updated_at > last check" every hour feels like CDC, but is not
- **Misses deletes** — a deleted row does not appear in any SELECT; polling never learns it is gone
- **Misses hops** — placed → shipped → delivered within one hour: polling sees only "delivered"
- **Hammers the DB** — every poll queries the table again; the log is read once, sequentially
- **The log has it all** — every change, including deletes and intermediate states, in exact order

**Example (italic):** The hourly poll at 12:00 saw order 1001 as "delivered" — the "shipped" step and deleted order 998 never showed up.

**Key point:** Polling samples the table's current state; CDC replays its full history — deletes and in-between states only exist in the log.

### Visualization (canvas `c4`, 720×300)

Two-lane timeline comparing what the change log sees vs what an hourly poll sees over one hour; x-axis is minutes after 11:00 (0 to 70), left padding 170px, right 40px.

- **Title (bold 15px ink, top center):** "One Busy Hour: What the Log Sees vs What the Poll Sees"
- **Changes during the hour:** 1001 shipped at 11:02 (aqua `#199e70`), 1001 delivered at 11:24 (violet `#4a3aa7`), 998 DELETED at 11:41 (magenta `#d55181`).
- **Lane 1 (y=90), labeled "change log sees" (bold 12px green `#008300`) with "(CDC)" (11px `#444`) below:** light grid line `#e5e9ef`; a 7px colored dot per change with its bold 12px label above; bold 12px green annotation to the right: "all 3 changes, in order".
- **Lane 2 (y=190), labeled "hourly poll sees" (bold 12px magenta `#d55181`) with "(updated_at > 11:00)" (11px `#444`) below:** a vertical dashed (6/4) magenta line at the 12:00 mark labeled "poll runs at 12:00" (bold 12px magenta, top); a single violet dot at 12:00 labeled '1001: "delivered"'; two bold 12px red `#e74c3c` miss annotations below the lane: "✖ \"shipped\" step invisible" and "✖ delete of 998 invisible".
- **Time ticks (12px `#222`, y=262):** 11:00, 11:20, 11:40, 12:00.
- **Caption (bold 13px orange `#d95926`, bottom center, y=288):** "polling samples the end state; the log kept the whole story"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray line, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `td.text-col` (50%) holding tags/bullets/example/key-point and `td.viz-col` (50%) holding the canvas (plus `.payload-note` and `.payload` pre-block in section 2).
- **Text column structure:** `.tags` row of colored pill spans (0.72rem bold, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); 5 one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius. `.payload` monospace 0.78em, `#f8f9fa` background, left border 3px solid `#1a5276`. No nav bar, no back/home links.
- **Canvases:** intrinsic 720×300; shared `setup(id)` helper scales backing store by `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates; shared `boxAt` and `arrowTo` helpers draw bordered boxes and arrowheaded lines. All data hardcoded.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card/grid links use `.html` extensions (this page has none — no cross-page links).
