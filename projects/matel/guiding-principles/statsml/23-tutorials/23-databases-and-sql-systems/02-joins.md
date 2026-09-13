# Joins

**Page type:** detail page (tutorial card-sections: h2 per section, two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Joins

**Subtitle:** Orders store only a customer_id — a join follows that pointer for every row at once and glues the two tables together

**Shared running-example data (used across canvases):**

- CUSTOMERS: `[1, Asha], [2, Ben], [3, Chloe], [4, Dev], [5, Ella]`
- ORDERS: `[1001, 1, $8], [1002, 2, $15], [1003, 1, $5], [1004, 3, $8], [1005, 2, $12], [1006, 4, $15], [1007, 1, $12], [1008, 9, $5]`

## Who Placed Order 1004? Match on customer_id

**Tags:** `core idea` (blue), `running example` (green)

- **The question** — orders knows amounts, customers knows names; reports need both
- **By hand** — order 1004 says customer_id 3; find 3 in customers: Chloe, Mumbai
- **A join** — the same lookup, done for all 8 orders in one statement
- **The meeting point** — ON orders.customer_id = customers.customer_id
- **Two loose ends** — Ella (id 5) has no orders; order 1008 points at a missing customer 9

*Example:* The shop's tiny data already contains both classic join headaches: a customer with no orders and an orphaned order.

**Key point:** A join is a row-by-row lookup along the foreign key. Everything else about joins is deciding what to do with rows that find no partner.

### Visualization (canvas `c1`, 720×300)

Two rendered tables with per-row match lines (drawn with a shared `drawTable` helper: ink `#1a5276` header band, alternating `#fff`/`#f4f6f8` rows, `#cfd8e0` border).

- **Title (bold 15px `#1a5276`, top center):** "Matching Every Order to Its Customer"
- **Left table:** orders as order_id / customer_id / amount (all 8 ORDERS rows). **Right table:** customers as customer_id / name (all 5 rows).
- **Match lines:** one 1.6px line per order to its customer row, colored by customer: 1 blue `#2a78d6`, 2 green `#008300`, 3 magenta `#d55181`, 4 aqua `#199e70`.
- **Orphan:** order 1008 (customer_id 9) gets a red `#e74c3c` dashed (5/4) line to empty space with bold red label "order 1008 → customer 9: no partner (orphan)".
- **Annotations:** bold 12px orange `#d95926`, two lines near the customers table: "Ella: no line arrives —" / "a customer with no orders"; bold 13px ink label between the tables: "ON customer_id = customer_id"

## Inner Join: Keep Only the Matches

**Tags:** `worked example` (green), `core idea` (blue)

- **The rule** — a row comes out only if both sides found a partner
- **7 of 8 survive** — orders 1001–1007 all find their customer
- **1008 vanishes** — customer 9 does not exist, so the row is silently dropped
- **Ella is absent too** — she has no order row to pair with
- **No error, no warning** — the $5 of order 1008 just left the report

SQL block (verbatim):

```sql
SELECT o.order_id, c.name, o.amount
FROM   orders o
JOIN   customers c
  ON   o.customer_id = c.customer_id;   -- 7 rows
```

*Example:* Total revenue is $80 in orders, but only $75 after the inner join — the difference walked out with order 1008.

**Key point:** Inner join means "matches only". Unmatched rows disappear without any error message.

### Visualization (canvas `c2`, 720×300)

Rendered result table plus a struck-out dropped row and side notes.

- **Title (bold 15px `#1a5276`, top center):** "Inner Join Result: 8 Orders In, 7 Rows Out"
- **Result table (order_id / name / amount, 7 rows):** `[1001, Asha, $8], [1002, Ben, $15], [1003, Asha, $5], [1004, Chloe, $8], [1005, Ben, $12], [1006, Dev, $15], [1007, Asha, $12]`.
- **Dropped row (below the table):** red-tinted band `rgba(231,76,60,0.07)` with red text "1008        ?             $5" struck through by a red `#e74c3c` line; bold 14px red beside it: "dropped: customer 9 not found".
- **Side notes (right of table):** bold 13px `#2c3e50` "row count:  orders 8  →  result 7"; bold 13px orange `#d95926` "revenue:  $80 in orders  →  $75 here"; muted 12px `#6b7280` lines: "no error was raised — the row and its $5" / "simply do not appear in the output" and "Ella appears nowhere either: she had no" / "order row to pair with".

## Left Join: Keep Everyone on the Left

**Tags:** `worked example` (green), `core idea` (blue)

- **The rule** — every left-table row comes out, matched or not
- **Left = customers** — all 5 people appear, even order-less Ella
- **No partner?** — the order columns are filled with NULL, meaning "nothing there"
- **Ella's row** — Ella | NULL | NULL: exactly the customer a win-back email wants
- **Direction matters** — orders LEFT JOIN customers would instead rescue order 1008

SQL block (verbatim):

```sql
SELECT c.name, o.order_id, o.amount
FROM   customers c
LEFT JOIN orders o
  ON   o.customer_id = c.customer_id;   -- 8 rows
```

*Example:* "Which customers never bought anything?" is a left join plus WHERE o.order_id IS NULL — the answer here is Ella.

**Key point:** Use a left join when losing unmatched rows would change the answer — which side is "left" decides who is safe.

### Visualization (canvas `c3`, 720×300)

Rendered result table with a highlighted NULL row and side notes.

- **Title (bold 15px `#1a5276`, top center):** "Left Join Result: All 5 Customers Kept, Gaps Filled with NULL"
- **Result table (name / order_id / amount, 8 rows):** `[Asha, 1001, $8], [Asha, 1003, $5], [Asha, 1007, $12], [Ben, 1002, $15], [Ben, 1005, $12], [Chloe, 1004, $8], [Dev, 1006, $15], [Ella, NULL, NULL]` — the Ella row bold with background `rgba(230,126,34,0.18)`.
- **Side notes:** bold 14px orange `#d95926`, two lines: "Ella kept: order columns are NULL —" / '"no order exists", not "zero dollars"'; bold 13px `#2c3e50` "row count:  customers 5  →  result 8"; muted 12px lines: "3 Asha rows + 2 Ben + 1 Chloe + 1 Dev + 1 Ella", "customers with several orders repeat;" / "customers with none survive with NULLs"; bold 12px red `#e74c3c`: "order 1008 is still lost — orders is not the left table here".

## Count Your Rows Before and After

**Tags:** `common mistake` (red), `why it matters` (orange)

- **Rows lost** — 8 orders in, 7 out: an inner join quietly dropped revenue
- **Rows gained** — join orders to a table with 2 rows per order and you get 16 rows
- **Fan-out warning** — joining on a non-unique key multiplies rows and double-counts every sum
- **The habit** — run COUNT(*) on inputs and output; explain any difference before trusting sums
- **Most join bugs** — are not wrong matches, they are wrong row counts

*Example:* After joining orders to a delivery-attempts table, revenue "doubled" to $160 overnight — every order matched two attempt rows.

**Rule of thumb:** A join changed your row count and you can't say why? Stop — your averages and totals are already wrong.

### Visualization (canvas `c4`, 720×300)

Bar chart: row counts for four join variants.

- **Title (bold 15px `#1a5276`, top center):** "The Cheapest Join Bug Detector: COUNT(*)"
- **Bars (88px wide, baseline y=234, chart height 165px, y max 18):**
  - "orders" / "(input)" = 8, blue `#2a78d6`
  - "INNER JOIN" / "customers" = 7, violet `#4a3aa7`
  - "customers" / "LEFT JOIN orders" = 8, green `#008300`
  - "orders JOIN" / "delivery_attempts" = 16, red `#e74c3c`
- **Reference line:** dashed (5/4) gridline `#e5e9ef` at 8 rows, right-aligned muted label "8 rows".
- **Labels:** bold 13px "N rows" above each bar; bar name and muted sub-label below the baseline.
- **Annotations:** bold 13px red near the top right: "fan-out: 2 attempt rows per order → 16 rows, sums doubled"; bold 12px violet over the second bar: "7: one row lost"; muted 12px bottom center: "same 8 orders, four different row counts — the count is the first thing to check".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`, social-graph reference skeleton). Body: `<h1>` (no index number), `.subtitle`, then four `.card-section` blocks, each `<h2>` + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one 720×300 canvas.
- **Left column structure:** `.tags` pill row first, then a `<ul>` of bullets each opening with `<b>bold term</b>` (bold terms render `#1a5276`), optional `<pre class="sql">` block (monospace 0.8rem, background `#f8f9fa`, left border `3px solid #1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem). Sections 2 and 3 contain the SQL blocks shown above.
- **Tag pill styles:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** `setup(id)` reads each canvas's declared width/height attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. A shared `drawTable(ctx, x, y, cols, widths, rows, opt)` helper renders mini data tables (ink `#1a5276` header, white bold header text, alternating `#fff`/`#f4f6f8` rows, `#cfd8e0` border, per-row color/bold/background overrides) and returns row-center geometry for match lines. All data arrays are hardcoded literals — no `Math.random()`.
- **Chart palette (`P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; alarm red `#e74c3c`. Project palette anchors: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
