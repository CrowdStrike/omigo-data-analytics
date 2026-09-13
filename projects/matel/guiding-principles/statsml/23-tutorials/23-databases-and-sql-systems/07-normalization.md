# Normalization

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Normalization

**Subtitle:** Normalization splits a table so every fact is stored exactly once — and warehouses deliberately glue it back together to make reads fast

## The Coffee Shop Sheet With 47 Copies of One Address

**Tags:** `core idea` (blue), `one fact, one place` (green), `orders table` (orange)

- **The sheet** — a coffee shop logs every order in one spreadsheet: customer, address, items, prices
- **Repeating groups** — items squeeze into item1/item2/item3 columns; a 4-item order doesn't fit
- **The copies** — Mia Chen has 47 orders, so "12 Oak St" is typed on 47 separate rows
- **The move** — Mia moves; fixing her address means editing all 47 rows without missing one
- **The anomaly** — miss 3 rows and the shop has two addresses on file, both looking official

*Example (italic):* A refund clerk picks row 1044, reads the stale address, and mails Mia's refund to the old house.

**Key point:** Normalization means splitting tables so every fact is stored exactly once — the messy sheet stores one fact 47 times, and every copy is a chance to disagree.

### Visualization (canvas `c1`, 720×300)

A drawn spreadsheet: 8 sample rows of the flat orders sheet with the address column visibly repeated on every row and the repeating item columns half-empty.

- **Title (bold 15px, `#1a5276`, top center):** "One Flat Orders Sheet: the Same Address on Every Row".
- **Grid:** header row at y=70, then 8 data rows 22px tall; thin `#e5e9ef` row lines; column x positions 30 / 105 / 195 / 320 / 500 / 610; bold 12px `#1a5276` headers "order", "date", "customer", "address", "item1", "item2".
- **Data (12px `#2c3e50`):** orders `[1039, 1040, 1041, 1042, 1043, 1044, 1045, 1046]`; dates `["Mar 3","Mar 4","Mar 6","Mar 9","Mar 11","Mar 12","Mar 14","Mar 15"]`; customer "Mia Chen" on all 8 rows; item1 `["latte","muffin","latte","latte","cold brew","latte","muffin","latte"]`; item2 `["","","scone","muffin","","scone","",""]` — blanks show the repeating-group waste.
- **Address column:** every cell "12 Oak St" in magenta `#d55181` on a `rgba(213,81,129,0.12)` fill spanning the column.
- **Annotation (bold 13px magenta `#d55181`, right-aligned above the table, y≈48):** "one fact, 47 copies (8 rows shown)".
- **Caption (12px `#444`, bottom right):** "orders illustrative".

## Splitting the Sheet: 1NF, 2NF, 3NF

**Tags:** `worked example` (blue), `three steps` (green)

- **1NF** — one item per row: order 1042 (latte, muffin, cold brew) becomes 3 rows, no itemN columns
- **The cost** — Mia's 47 orders explode into 61 item rows, and each still carries her address
- **2NF** — customer and address depend on the order alone, not (order, item): move them to an orders table
- **Prices too** — the latte's 4.50 was retyped on every row; 2NF gives it one row in an items table
- **3NF** — address depends on the customer, not the order: move it once more, into a customers table
- **The payoff** — Mia's address now lives in 1 row; her move is a single edit instead of 47

*Example (italic):* Rows to edit when Mia moves: flat sheet 47, after 1NF 61, after 2NF 47, after 3NF exactly 1.

**Key point:** Each normal form removes one kind of duplication — 1NF repeating groups, 2NF facts about part of the key, 3NF facts about another fact — until every fact has one home.

### Visualization (canvas `c2`, 720×300)

Bar chart: number of rows that must be edited to change Mia's address at each normalization stage — it gets worse before it gets better.

- **Title (bold 15px, `#1a5276`, top center):** "Rows to Edit When Mia Moves: 47 → 61 → 47 → 1".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = rows to edit 0 to 70, gridlines `#e5e9ef` at 20/40/60 with 12px `#444` labels.
- **Bars:** four bars 90px wide, centered at x = 135, 285, 435, 585; stage labels 12px `#444` below baseline `["flat sheet", "1NF", "2NF", "3NF"]`; heights from values `[47, 61, 47, 1]`; fills magenta `#d55181`, orange `#d95926`, yellow `#c98500`, green `#008300`.
- **Value labels:** bold 13px, each bar's color, centered above each bar: "47", "61", "47", "1".
- **Annotation (bold 13px green `#008300`, above the 3NF bar, y≈120):** "3NF: change the address in exactly 1 row".
- **Caption (12px `#444`, bottom right):** "row counts from the worked example; illustrative".

## Why Warehouses Glue It Back Together

**Tags:** `where it's used` (blue), `reads vs writes` (green), `star schema` (orange)

- **Two jobs** — the checkout app writes orders all day; the analytics team reads millions of rows
- **Writes love 3NF** — each sale inserts small rows, and an address change touches one place
- **Reads hate joins** — the monthly sales report on the 3NF schema must join 5 tables to answer
- **Star schema** — warehouses copy customer and item facts into wide tables around one fact table
- **Safe copies** — a pipeline rebuilds the copies nightly, so no human ever edits 47 rows by hand
- **The trade** — extra storage buys the report a 2-join query: 42s drops to 6s in the example

*Example (italic):* The shop's warehouse joins the fact table to a customer dimension in 2 hops; the monthly report runs in 6s instead of 42s.

**Key point:** Normalization protects writes; denormalization serves reads. A star schema is deliberate, machine-maintained duplication — the anomaly risk is handled by the pipeline, not by hand.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: the same monthly report on the normalized 3NF schema vs the star schema — tables joined and query seconds.

- **Title (bold 15px, `#1a5276`, top center):** "Same Report, Two Schemas: Joins and Seconds".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, max width 440; left-aligned 12px `#444` row labels at x=20.
- **Rows (14px-tall bars, top to bottom at y = 75, 110, 175, 210), pixel widths hardcoded:**
  - "3NF — tables joined: 5": blue `#2a78d6` bar width 300
  - "star — tables joined: 2": green `#008300` bar width 120
  - "3NF — query time: 42s": blue `#2a78d6` bar width 420
  - "star — query time: 6s": green `#008300` bar width 60
- **Value labels:** 11px `#444` at each bar end: "5", "2", "42s", "6s".
- **Annotation (bold 13px aqua `#199e70`, right side near y=250):** "same rows, 7× faster — the copies are the price".
- **Caption (12px `#444`, bottom right):** "seconds illustrative; join counts from the schema".

## Normalizing the Wrong Database

**Tags:** `common mistake` (red), `OLTP vs OLAP` (orange)

- **Mistake one** — normalizing an analytics table to death: analysts face five-join queries and give up
- **Mistake two** — denormalizing the live checkout database "for speed": the 47-row anomaly returns
- **The test** — ask who writes: many small app edits → normalize; append-only pipeline loads → widen
- **Half-measures** — caching one denormalized column in the live database quietly makes two sources of truth
- **The rule** — normalize where humans and apps write; denormalize where machines rebuild and people read

*Example (italic):* The shop copies "address" onto live order rows to skip a join; two months later 3 refunds ship to Mia's old house.

**Common mistake:** Treating normalization as a virtue scale — "more normal = better design". It is a tool for write-heavy tables; applied to a read-only warehouse it just taxes every query.

### Visualization (canvas `c4`, 720×300)

2×2 quadrant diagram: workload (writes vs reads) against design (normalized vs denormalized), with the two good matches in green and the two mismatches in red.

- **Title (bold 15px, `#1a5276`, top center):** "Match the Design to the Workload".
- **Grid:** vertical divider 1px `#e5e9ef` at x=400, horizontal at y=165; plot area from y=70 to y=260; column headers bold 13px `#1a5276` at y=62: "normalized (3NF)" centered at x=250, "denormalized (wide)" centered at x=555.
- **Row labels (12px `#444`, left edge x=15):** "checkout app (many writes)" at y≈115, "sales reports (reads only)" at y≈215.
- **Cells:** rounded boxes ~230×60, 8px radius, 12px `#2c3e50` text:
  - top-left, fill `rgba(0,131,0,0.12)`: "✓ one edit updates every order" with bold green `#008300` check
  - top-right, fill `rgba(231,76,60,0.12)`: "✗ the 47-row anomaly returns" with bold red `#e74c3c` cross
  - bottom-left, fill `rgba(231,76,60,0.12)`: "✗ every report is a five-join puzzle" with bold red `#e74c3c` cross
  - bottom-right, fill `rgba(0,131,0,0.12)`: "✓ one wide table, fast scans" with bold green `#008300` check
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=285):** "normalize where you write, denormalize where you read".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the coffee-shop orders, Mia's 47 orders, the edit counts `[47, 61, 47, 1]`, the join counts (5 vs 2), and the query times (42s vs 6s) are invented and labeled illustrative; bar pixel widths in c3 are schematic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
