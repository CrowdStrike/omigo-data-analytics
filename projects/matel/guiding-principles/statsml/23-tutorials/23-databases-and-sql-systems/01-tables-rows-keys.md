# Tables, Rows, Keys

**Page type:** detail page (tutorial card-sections: h2 per section, two-column `table.layout` with text left 50% / canvas right 50%; first section uses the 3-column 38/31/31 variant with two canvases)
**HTML title tag:** Tables, Rows, Keys

**Subtitle:** A database is a few small tables; every row has its own ID, and rows point at each other using those IDs

**Shared running-example data (used across all canvases):**

- CUSTOMERS: `[1, Asha, Pune], [2, Ben, Delhi], [3, Chloe, Mumbai], [4, Dev, Pune], [5, Ella, Delhi]`
- PRODUCTS: `[101, Mug, $8], [102, T-shirt, $15], [103, Notebook, $5], [104, Poster, $12]`
- ORDERS: `[1001, 1, 101, $8, Jan 03], [1002, 2, 102, $15, Jan 05], [1003, 1, 103, $5, Jan 09], [1004, 3, 101, $8, Feb 02], [1005, 2, 104, $12, Feb 06], [1006, 4, 102, $15, Feb 14], [1007, 1, 104, $12, Mar 01], [1008, 9, 103, $5, Mar 04]`

## One Tiny Shop, Three Tables

**Tags:** `core idea` (blue), `running example` (green)

- **The shop** — 5 customers, 4 products, 8 orders: everything it knows fits in three tables
- **A table** — one kind of thing: people in customers, items in products, sales in orders
- **A row** — one of those things: "3, Chloe, Mumbai" is one customer, top to bottom
- **A column** — one fact about every row: each customer has a city, each order an amount
- **Stored once** — Asha's name lives in one row of customers, however often she buys

*Example:* Order 1004 reads: sale number 1004, by customer 3, of product 101, for $8, on Feb 02 — five facts, one row.

**Key point:** A database is not one big spreadsheet. It is several small tables, each holding one kind of thing exactly once.

### Visualization (canvas `c1a`, 420×340)

Two rendered data tables stacked vertically (drawn with a shared `drawTable` helper: ink `#1a5276` header band with white bold 12px column names, alternating row fills `#fff`/`#f4f6f8`, border `#cfd8e0`).

- **Heading 1 (bold 15px `#1a5276`, centered):** "customers — one row per person" — table with columns customer_id / name / city holding all 5 CUSTOMERS rows.
- **Heading 2:** "products — one row per item" — table with columns product_id / name / price holding all 4 PRODUCTS rows.
- **Caption (muted `#6b7280`, bottom center):** "each table holds one kind of thing"

### Visualization (canvas `c1b`, 420×340)

One rendered data table.

- **Heading (bold 15px `#1a5276`, centered):** "orders — one row per sale" — table with columns order_id / customer_id / product_id / amount / date holding all 8 ORDERS rows; the customer_id column is highlighted with overlay `rgba(42,120,214,0.10)`.
- **Annotation (bold 13px orange `#d95926`, centered, two lines):** "customer_id and product_id are pointers" / "into the other two tables"
- **Caption (muted `#6b7280`):** "no name, no price is copied in here"

## The Primary Key: Every Row's Own ID

**Tags:** `core idea` (blue), `rule of thumb` (green)

- **customer_id** — each customer gets a number at signup: Asha is 1, Ben is 2, Chloe is 3
- **Unique** — the database refuses a second row with id 3; no two rows share an id
- **Permanent** — Chloe can change her name or city; her id 3 stays for life
- **Why not names?** — two Chloes can exist, and names change; ids do neither
- **Every table has one** — customer_id, product_id, order_id: the same idea three times

*Example:* Chloe marries and becomes "Chloe K." — row 3 changes its name cell, and nothing else in the shop needs to know.

**Key point:** The primary key is the row's identity: a value that is never blank, never repeated, and never changed.

### Visualization (canvas `c2`, 720×300)

Rendered customers table with highlighted key column, plus two operation boxes (refused insert / allowed update).

- **Title (bold 15px `#1a5276`, top center):** "The Primary Key: Never Blank, Never Repeated, Never Changed"
- **Left:** customers table (customer_id / name / city, all 5 rows), customer_id column highlighted `rgba(39,174,96,0.18)`; below it bold green `#008300` label "primary key column" and muted "5 rows, 5 different ids".
- **Right, operation 1:** red `#e74c3c` box (270×34, fill `rgba(231,76,60,0.08)`) containing "INSERT  3  |  Chris  |  Delhi"; below, bold 14px red: "✕  REFUSED — id 3 is already taken".
- **Right, operation 2:** green `#008300` box (fill `rgba(39,174,96,0.08)`) containing "UPDATE row 3: name → 'Chloe K.'"; below, bold 14px green: "✓  ALLOWED — facts change, the id never does".
- **Annotation (bold 13px orange `#d95926`):** "the id is the row's identity; everything else is just a fact about it"

## The Foreign Key: Orders Point at Customers

**Tags:** `core idea` (blue), `where it's used` (orange)

- **The pointer** — order 1001 stores customer_id 1: "this sale belongs to customer 1"
- **Follow it** — look up 1 in customers and you get the rest: Asha, Pune
- **Reused** — orders 1001, 1003, 1007 all store 1: three sales, still one Asha row
- **Broken pointer** — order 1008 stores customer_id 9, and no customer 9 exists
- **The guard** — a foreign key constraint tells the database to refuse pointers to nowhere

*Example:* This shop never declared the constraint, so order 1008 slipped in pointing at a customer who was deleted.

**Key point:** A foreign key is just another table's primary key, copied into a row as a pointer.

### Visualization (canvas `c3`, 720×300)

Two rendered tables joined by per-row arrows: orders (left) pointing to customers (right).

- **Title (bold 15px `#1a5276`, top center):** "Every Order Points at One Customer Row"
- **Left table:** orders as order_id / customer_id / amount (8 rows), customer_id column highlighted `rgba(42,120,214,0.10)`. **Right table:** customers as customer_id / name (5 rows).
- **Arrows:** one 1.6px line per order from its row to its customer's row, arrowhead at the customer side, colored by customer: 1 blue `#2a78d6`, 2 green `#008300`, 3 magenta `#d55181`, 4 aqua `#199e70`.
- **Broken pointer:** order 1008 (customer_id 9) gets a red `#e74c3c` dashed (5/4) line to empty space with bold red label "? customer 9 missing — broken pointer".
- **Annotations:** bold 12px blue, two lines: "3 arrows land on Asha —" / "3 sales, one customer row"; muted 12px: "Ella (5): no arrows arrive — a customer with no orders"

## Why Copying the Name In Would Rot

**Tags:** `common mistake` (red), `why it matters` (orange)

- **The shortcut** — skip the pointer and write "Asha" straight into each of her orders
- **Copies drift** — she has 3 orders; a rename must now update 3 rows, not 1
- **The rot** — miss one and the table holds both "Asha" and "Asha K." forever
- **Counts break** — grouping sales by name now sees two customers where there is one
- **The fix** — store the name once in customers; every order just points at id 1

*Example:* A data scientist counting distinct names gets 6 customers out of a 5-customer shop — and reports growth that never happened.

**Common mistake:** Duplicating facts across rows. Every copy is one more place the truth can go stale — keys exist so each fact lives in one place.

### Visualization (canvas `c4`, 720×300)

Side-by-side rename comparison, split by a vertical dashed `#bdc3c7` divider at x=360.

- **Title (bold 15px `#1a5276`, top center):** "Rename Asha: Hunt 3 Copies vs Update 1 Row"
- **Left — heading "name copied into her orders":** rendered table order_id / customer_name / amount with rows `[1001, Asha, $8], [1003, Asha, $5], [1007, Asha K., $12]`; the two stale "Asha" rows in red `#e74c3c` text, the updated row bold green `#008300`. Bold red caption, two lines: 'rename reached 1 of 3 copies:' / '"Asha" and "Asha K." both live on'; muted caption: "COUNT(DISTINCT name) now says 2 customers".
- **Right — heading "orders keep only the pointer":** table order_id / customer_id with rows `[1001, 1], [1003, 1], [1007, 1]`, plus a one-row table id / name = `[1, Asha K.]` (bold, row fill `rgba(39,174,96,0.15)`); three green 1.6px lines from the order rows converging on that single row. Bold green caption, two lines: "rename touched exactly 1 row —" / 'all 3 orders see "Asha K." at once'; muted caption: "one fact, one place: nothing can disagree".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`, social-graph reference skeleton). Body: `<h1>` (no index number), `.subtitle`, then four `.card-section` blocks, each `<h2>` + `table.layout`. Section 1 uses the 3-column variant: `<td class="text-col3">` (38%) plus two `<td class="viz-col3">` (31% each) holding canvases `c1a`/`c1b` (420×340). Sections 2–4 use `<td class="text-col">` (50%) / `<td class="viz-col">` (50%) with one 720×300 canvas.
- **Left column structure:** `.tags` pill row first, then a `<ul>` of bullets each opening with `<b>bold term</b>` (bold terms render `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem). A `.sql` block style exists (monospace, `#f8f9fa`, left border `3px solid #1a5276`) though this page has no SQL blocks.
- **Tag pill styles:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** `setup(id)` reads each canvas's declared width/height attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. A shared `drawTable(ctx, x, y, cols, widths, rows, opt)` helper renders mini data tables (ink `#1a5276` header, white bold header text, alternating `#fff`/`#f4f6f8` rows, `#cfd8e0` border, optional column highlight, per-row color/bold/background overrides) and returns row-center geometry for arrows. All data arrays are hardcoded literals — no `Math.random()`.
- **Chart palette (`P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; alarm red `#e74c3c`. Project palette anchors: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
