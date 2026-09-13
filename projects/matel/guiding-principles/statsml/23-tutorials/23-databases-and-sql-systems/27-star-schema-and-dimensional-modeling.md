# Star Schema & Dimensional Modeling

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Star Schema & Dimensional Modeling

**Subtitle:** A warehouse keeps every measurement in one big fact table and the who/what/where/when in small dimension tables around it — drawn out, it looks like a star

## One Sales Table, Four Describing Tables

**Tags:** `core idea` (blue), `facts & dimensions` (green), `data warehouse` (orange)

- **The chain** — a coffee chain with many stores wants one place to answer any sales question
- **The fact** — every receipt line becomes one row in a SALES table: keys, amount, quantity
- **The dimensions** — DATE, STORE, PRODUCT, CUSTOMER tables each describe one key on that row
- **The split** — facts are numbers you add up; dimensions are words you filter and group by
- **The name** — the fact table sits in the middle, dimensions around it: a star schema
- **The grain** — the rule "one fact row = one receipt line" is called the grain of the table

*Example (italic):* A $12 latte sale is one SALES row holding date_key, store_key, product_key, customer_key, amount 12 — the words "Maple St" and "latte" live in the dimensions.

**Key point:** Dimensional modeling splits data into one wide, tall fact table of measurements and a handful of small dimension tables of descriptions, joined by simple keys.

### Visualization (canvas `c1`, 720×300)

Star diagram: the SALES fact table box in the center with four dimension boxes at the corners, connected by key lines.

- **Title (bold 15px, `#1a5276`, top center):** "The Star: One Fact Table, Four Dimensions Around It".
- **Fact box (center):** rounded box at x=270, y=105, width 180, height 90, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; bold 13px `#1a5276` header "SALES (fact)" and 11px `#2c3e50` lines "date_key · store_key", "product_key · customer_key", "amount · quantity".
- **Dimension boxes (corners), each 150×52, 8px radius, 12px `#2c3e50` text, bold 12px header:** DATE at (60, 45) fill `rgba(0,131,0,0.12)` border `#008300`, lines "day, month, year"; STORE at (510, 45) fill `rgba(217,89,38,0.12)` border `#d95926`, lines "name, city, region"; PRODUCT at (60, 205) fill `rgba(74,58,167,0.12)` border `#4a3aa7`, lines "name, size, category"; CUSTOMER at (510, 205) fill `rgba(213,81,129,0.12)` border `#d55181`, lines "name, loyalty tier".
- **Connectors:** 2px `#6b7280` straight lines from each dimension box's inner edge to the nearest corner of the fact box; small 11px `#6b7280` "key" label at each line's midpoint.
- **Annotation (bold 13px blue `#2a78d6`, bottom center near y=285):** "facts you add up, dimensions you group by".
- **Caption (12px `#444`, bottom right):** "schema schematic — columns abridged".

## Revenue by Region by Month, By Hand

**Tags:** `worked example` (blue), `grain` (green), `two joins` (orange)

- **The question** — the CFO asks: revenue by region by month, for January and February
- **The facts** — 8 SALES rows with amounts 12, 8, 15, 10, 9, 14, 11, 6 dollars across the two months
- **Join 1** — store_key to STORE: store 1 "Maple St" is region North, store 2 "Harbor Rd" is South
- **Join 2** — date_key to DATE: each row picks up its month, Jan or Feb
- **The group-by** — North-Jan 12+15=27, South-Jan 8+10=18, North-Feb 9+11=20, South-Feb 14+6=20
- **Grain check** — one row per receipt line, so SUM(amount) is safe: 27+18+20+20 = $85 total

*Example (italic):* `SELECT region, month, SUM(amount) FROM sales JOIN store JOIN date GROUP BY region, month` turns 8 fact rows into 4 answer rows.

**Key point:** Every star-schema question is the same move — join the fact table to the dimensions you need, then group and sum; the grain tells you the sum is trustworthy.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart of the hand-computed answer: revenue by month on the x-axis, one bar per region within each month.

- **Title (bold 15px, `#1a5276`, top center):** "8 Fact Rows Become 4 Answer Rows: Revenue by Region by Month".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; y = dollars 0 to 30, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels; x = two month groups "January" and "February" centered at x=230 and x=490 (13px `#444` labels below baseline).
- **Bars (width 70, gap 20 within a group):** January — North `#2a78d6` height for 27, South `#199e70` height for 18; February — North `#2a78d6` for 20, South `#199e70` for 20; bold 12px value labels "$27", "$18", "$20", "$20" above each bar in the bar's color.
- **Legend (top right, 12px):** blue swatch "North (Maple St)", aqua swatch "South (Harbor Rd)".
- **Annotation (bold 13px violet `#4a3aa7`, near x=300, y=70):** "two joins + one group-by answers the CFO".
- **Caption (12px `#444`, bottom right):** "amounts illustrative — totals match the text".

## Why Warehouses Are Drawn as Stars

**Tags:** `where it's used` (blue), `BI tools` (green), `conformed dimensions` (orange)

- **BI tools** — dashboard tools auto-detect stars: dimensions become filters, facts become metrics
- **Few joins** — every question is fact-to-dimension hops; no chain of six joins to reach "region"
- **Fast scans** — the warehouse scans one tall fact table and joins to tiny lookup tables
- **Shared language** — a conformed DATE or STORE dimension is reused by sales, inventory, staffing facts
- **Cross-mart answers** — shared dimensions let "sales per staffed hour by store" join two fact tables

*Example (italic):* Because sales and inventory facts share the same STORE dimension, "region" means exactly the same store list in both dashboards.

**Key point:** The star shape is a contract — BI tools, query optimizers, and analysts all assume it, and conformed dimensions keep every data mart speaking the same language.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: number of joins needed to answer "revenue by region by month" under three schema styles.

- **Title (bold 15px, `#1a5276`, top center):** "Joins Needed for One Business Question, by Schema Style".
- **Axis:** bars start at x=250, max width 400; x scale 0 to 10 joins, gridlines `#e5e9ef` every 2 joins with 11px `#6b7280` tick labels along y=255.
- **Rows (14px-tall bars at y = 80, 140, 200), left-aligned 12px `#444` labels at x=20:**
  - "star schema (fact + 2 dims)": green `#008300` bar width 80 (2 joins), 12px green value label "2" at bar end
  - "snowflaked star": yellow `#c98500` bar width 200 (5 joins), value label "5"
  - "fully normalized (3NF)": orange `#d95926` bar width 360 (9 joins), value label "9"
- **Annotation (bold 13px green `#008300`, right side near y=80):** "two hops — every question looks like this".
- **Caption (12px `#444`, bottom right):** "join counts illustrative for one typical question".

## Mixed Grain and the Snowflake Trap

**Tags:** `common mistake` (red), `mixed grain` (orange), `snowflaking` (blue)

- **Mixed grain** — someone adds daily-summary rows into the receipt-line fact table "for convenience"
- **Double count** — SUM(amount) now adds each dollar twice: the $85 total reports as $170
- **The fix** — one fact table per grain; summaries get their own table or a view, never mixed in
- **Snowflaking** — normalizing dimensions into sub-tables (store → city → region) re-adds join chains
- **The trade** — snowflakes save a little storage but cost query speed and BI-tool friendliness
- **Rule of thumb** — keep dimensions flat and slightly redundant; redundancy here is a feature

*Example (italic):* After summary rows sneak into SALES, the CFO's January total doubles overnight with no code change — the grain, not the query, broke.

**Common mistake:** Declaring the grain once and then violating it. Every fact table needs one sentence — "one row per receipt line" — and every insert must obey it.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a clean single-grain fact table summing correctly vs a mixed-grain table double counting.

- **Title (bold 15px, `#1a5276`, top center):** "One Grain Sums Right, Mixed Grain Counts Twice".
- **Row 1 (y=95), label 12px `#444` at x=20:** "one grain"; blue `#2a78d6` rounded box at x=150 labeled "8 receipt-line rows" (12px), 3px arrow to a green `#008300` box at x=430 labeled "SUM(amount) = $85" with bold 12px green "✓ matches the tills".
- **Row 2 (y=205), label:** "mixed grain"; blue box at x=150 labeled "8 line rows + summary rows", 3px arrow to a red `#e74c3c` box at x=430 labeled "SUM(amount) = $170" with bold 12px red "✗ every dollar counted twice".
- **Box style:** 170–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the query didn't change — the grain did".
- **Caption (12px `#444`, bottom right):** "dollar totals illustrative — $85 matches the worked example".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 8 sale amounts (12, 8, 15, 10, 9, 14, 11, 6), the grouped totals (27 / 18 / 20 / 20, total $85, doubled $170), and the join counts (2 / 5 / 9) are invented and labeled illustrative; the c2 bar values must match the section 2 hand computation exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
