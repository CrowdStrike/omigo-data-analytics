# MongoDB

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** MongoDB

**Subtitle:** MongoDB stores records as JSON-like documents — an order carries its line items nested inside it, shaped the way the application code already sees it

## The Order That Fits in One Document

**Tags:** `core idea` (blue), `document database` (green), `JSON` (orange)

- **The order** — customer Ana buys a mug, a tee, and a lamp; the shop must store order #4712
- **The relational way** — three tables: `orders`, `order_items`, `products`, stitched by two joins
- **The document way** — one document: the order with its three line items nested inside it
- **BSON** — MongoDB stores documents as BSON, a binary form of JSON with real dates and numbers
- **Flexible schema** — the next order can add a `gift_note` field; no ALTER TABLE, no migration
- **The match** — the document looks exactly like the object the checkout code already builds

*Example (italic):* Order #4712 is one document — `{_id: 4712, customer: "Ana", total: 93, items: [mug, tee, lamp]}` — nothing to join back together.

**Key point:** A document database stores each record as a self-contained JSON-like document — data that a relational design would spread across joined tables simply nests inside one object.

### Visualization (canvas `c1`, 720×300)

Side-by-side diagram: order #4712 as three joined relational tables (left) vs one nested document (right).

- **Title (bold 15px, `#1a5276`, top center):** "Order #4712: Three Joined Tables or One Document".
- **Left panel (x 20–345), label 12px `#444` at (30, 52):** "relational: three tables, two joins".
  - **orders table:** box at (30, 62), 190px wide; 18px header row fill `rgba(42,120,214,0.15)`, 11px `#2c3e50` text "orders (id, cust, total)"; one 20px data row "4712 | Ana | 93", 1px `#e5e9ef` borders.
  - **order_items table:** box at (30, 128), 190px wide; header "order_items (id, sku, qty)"; three 20px data rows "4712 | sku-11 | 2", "4712 | sku-27 | 1", "4712 | sku-30 | 1".
  - **products table:** box at (30, 232), 190px wide; header "products (sku, name, price)"; one 20px data row "sku-11 | mug | 12" plus 11px `#6b7280` text "… 2 more rows" beneath at (35, 285).
  - **Join lines:** 2px `#d95926` bracket from orders to order_items with bold 11px `#d95926` label "JOIN on id" at (232, 118); bracket from order_items to products with label "JOIN on sku" at (232, 222).
- **Right panel (x 375–700), label 12px `#444` at (385, 52):** "document: everything nested". One rounded box at (385, 62), 310px wide, 200px tall, fill `rgba(0,131,0,0.10)`, 2px `#008300` border, 8px radius; 12px monospace `#2c3e50` lines inside: `{ _id: 4712, customer: "Ana",`, `  date: "2026-03-12", total: 93,`, `  items: [`, `   {sku:"sku-11", name:"mug",  qty:2, price:12},`, `   {sku:"sku-27", name:"tee",  qty:1, price:19},`, `   {sku:"sku-30", name:"lamp", qty:1, price:50}`, `  ] }`.
- **Annotation (bold 12px green `#008300`, at (385, 280)):** "one object, zero joins".
- **Caption (12px `#444`, bottom right):** "order contents illustrative".

## Reading Order #4712 by Hand

**Tags:** `worked example` (blue), `one lookup` (green)

- **The task** — render Ana's confirmation page: the order header plus all three line items
- **Relational reads** — 1 row from `orders`, 3 from `order_items`, 3 from `products`: 7 rows, 3 tables
- **The joins** — two of them: `order_items` to `orders` on id, `order_items` to `products` on sku
- **Document read** — one lookup by `_id: 4712` returns one document; everything is already inside
- **Hand-check** — the total: 2×12 + 1×19 + 1×50 = 93, matching the stored `total: 93` (exact)
- **Locality** — the whole order sits together on disk instead of scattered across three tables

*Example (italic):* The confirmation page costs 3 tables, 7 rows, and 2 joins relationally — or exactly 1 document read in MongoDB.

**Key point:** Reads that match the document's shape collapse to a single lookup — the join work was done once, at write time, by nesting.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: what one confirmation-page read touches — relational (blue) vs document (green) — across tables touched, rows fetched, and joins.

- **Title (bold 15px, `#1a5276`, top center):** "One Order Read: 3 Tables, 7 Rows, 2 Joins vs 1 Document".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = count 0 to 8, gridlines `#e5e9ef` at 2/4/6/8 with 12px `#444` tick labels; three category groups with 13px `#444` labels centered under the baseline: "tables / collections" at x=170, "rows / docs fetched" at x=380, "joins" at x=590.
- **Bars:** each group has a blue bar (fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border) and a green bar (fill `rgba(0,131,0,0.25)`, 2px `#008300` border), 50px wide, 8px apart, pair centered on the group x. Values (heights at 22.5px per unit): relational `[3, 7, 2]` → 67/157/45px; document `[1, 1, 0]` → 22/22/0px. Zero-height join bar drawn as a bold 12px green "0" sitting on the baseline.
- **Value labels:** bold 12px in the bar's border color, 6px above each bar top: "3", "7", "2", "1", "1", "0".
- **Legend (12px `#2c3e50`, top right at x=520, y=52):** blue swatch "relational", green swatch "document".
- **Annotation (bold 13px green `#008300`, near x=300, y=80):** "the page needs 1 lookup, not 2 joins".
- **Caption (12px `#444`, bottom right):** "counts exact for this example order".

## Replica Sets, Sharding, and Growing Up

**Tags:** `where it's used` (blue), `scaling` (green), `history` (orange)

- **Replica sets** — the data lives on several servers; if the primary dies, an election promotes a secondary
- **Sharding** — a collection splits across machines by a shard key, spreading writes horizontally
- **The rough years** — early versions drew public criticism for lax write-durability defaults
- **WiredTiger** — the storage engine that became the default in 2015, answering the durability critiques
- **Transactions** — multi-document ACID transactions landed in version 4.0 (2018)
- **Atlas** — the managed cloud service (launched 2016) that now fronts much of MongoDB's business

*Example (italic):* A 3-node replica set (illustrative) loses its primary at 2:14am; a secondary is elected and writes resume in seconds — no pager, no restore.

**Key point:** MongoDB's pitch matured from "easy for developers" into a full distributed database: replication for failover, sharding for scale, transactions for the cases one document can't cover, and Atlas to run it all for you.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline of publicly documented milestones, 2008–2019, with alternating labels above and below the line.

- **Title (bold 15px, `#1a5276`, top center):** "From Durability Criticism to Managed Cloud: the Milestones".
- **Timeline:** 3px `#1a5276` horizontal line at y=160 from x=60 to x=680; x maps year 2008 → 60 and 2019 → 680 (≈56.4px per year); small 12px `#6b7280` year ticks "2008" and "2019" at the ends.
- **Milestone dots (7px radius) with bold 12px labels and a thin 1px `#6b7280` leader line to the dot:**
  - 2009 (x=116, blue `#2a78d6`), label above at y=112: "2009 — first release"
  - 2010 (x=173, red `#e74c3c`), label below at y=205: "≈2010 — durability criticism era"
  - 2015 (x=455, green `#008300`), label above at y=112: "2015 — WiredTiger default"
  - 2016 (x=512, aqua `#199e70`), label below at y=205: "2016 — Atlas launches"
  - 2018 (x=624, violet `#4a3aa7`), label above at y=130: "2018 — transactions (4.0)"
- **Annotation (bold 13px `#4a3aa7`, centered near x=370, y=262):** "each release answered the loudest objection of the years before".
- **Caption (12px `#444`, bottom right):** "milestone years as publicly documented".

## The Shape You Save Is the Shape You Read

**Tags:** `common mistake` (red), `analytics` (orange)

- **The flip side** — nesting optimizes one read shape; questions that cut across documents must unpack it
- **The question** — "how many mugs (sku-11) sold this quarter?" spans every order document
- **In SQL** — one line: sum `qty` from `order_items` where sku matches; the table is already flat
- **In MongoDB** — a pipeline: `$match` the dates, `$unwind` items, `$match` the sku, `$group` the sum
- **The scale** — 120,000 orders × ~3 items unwind into ~360,000 item rows first (illustrative)
- **The mistake** — choosing the document model from the write path alone, ignoring the reports you'll run

*Example (italic):* The order page reads 1 document, but the quarterly sku report unwinds ~360,000 nested items out of 120,000 orders (illustrative) before it can even group.

**Common mistake:** Believing documents make everything simpler. Nesting pre-joins the data for one access pattern — every query that cuts across that shape pays, at read time, the unpacking cost the joins used to pay.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the quarterly sku question as one flat SQL aggregate vs a four-stage document pipeline.

- **Title (bold 15px, `#1a5276`, top center):** "Units of sku-11 This Quarter: Flat Table vs Nested Documents".
- **Row 1 (boxes centered on y=95), label 12px `#444` at x=20:** "SQL"; one green rounded box 330px wide, 44px tall at x=110, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 8px radius, 12px `#2c3e50` text "SELECT SUM(qty) FROM order_items WHERE sku='sku-11'"; 3px `#6b7280` arrow to a green box 90px wide at x=500 labeled "done ✓". Bold 12px green `#008300` annotation at (110, 148): "1 step — order_items is already flat".
- **Row 2 (boxes centered on y=210), label:** "MongoDB"; four rounded boxes 128px wide, 44px tall at x = 110, 258, 406, 554, fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border, 8px radius, 12px `#2c3e50` text: "$match quarter", "$unwind items", "$match sku-11", "$group sum qty"; 3px `#6b7280` arrows between boxes. Bold 12px orange `#d95926` annotation at (110, 263): "4 stages — ~360,000 items unwound from 120,000 orders (illustrative)".
- **Annotation (bold 13px magenta `#d55181`, right-aligned near (690, 60)):** "the nesting that made the read easy is what analytics must unpack".
- **Caption (12px `#444`, bottom right):** "pipeline stage counts exact; document counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded arrays and counts above (no randomness); order #4712's contents, the 120,000-order / 360,000-item analytics scale, and the replica-set anecdote are invented and labeled illustrative; the read-cost counts (3 tables, 7 rows, 2 joins vs 1 document) and the order total (2×12 + 19 + 50 = 93) are exact for the example and must stay consistent between text and charts; milestone years (2009 release, 2015 WiredTiger default, 2016 Atlas, 2018 transactions in 4.0) are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
