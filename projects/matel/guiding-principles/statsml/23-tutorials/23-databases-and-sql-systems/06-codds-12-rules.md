# Codd's 12 Rules

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Codd's 12 Rules

**Subtitle:** In 1985 E.F. Codd published a checklist for what a database must do to call itself "relational" — the core demand: ask for data by value, never by where it lives

## Finding a Latte Without Following Pointers

**Tags:** `core idea` (blue), `by value, not address` (green), `1985` (orange)

- **The old way** — 1970s databases stored records in linked chains; programs walked pointers hop by hop
- **The question** — "what drink was order 1042?" meant navigating customer → order → next → next
- **The change** — Codd's model: data lives in plain tables, and you ask by value: order_id = 1042
- **The checklist** — in 1985 Codd published 12 rules (plus a rule zero) defining "truly relational"
- **The target** — vendors were stamping "relational" on pointer databases; the rules called them out

*Example (italic):* In a coffee shop's orders table, order 1042 is found by asking for order_id 1042 — not by walking a pointer chain that starts at the customer's record.

**Key point:** "Relational" originally meant one thing: every fact is a value in a table, reachable by table name, key, and column alone — never by a physical address or a pointer path.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: finding order 1042's drink by pointer navigation (top) vs by a value-based query (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways to Find Order 1042's Drink: Walk Pointers vs Ask by Value".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "1970s: navigate"; four rounded boxes 110×40 (8px radius, fill `rgba(217,89,38,0.12)`, 2px `#d95926` border, 12px `#2c3e50` text) left-anchored at x=130, 275, 420, 565 labeled "customer #7", "order 1040", "order 1041", "order 1042 · latte"; 3px `#d95926` arrows between; bold 12px orange `#d95926` label "3 hops — the program follows pointers" at (x=280, y=60).
- **Row 2 (boxes centered on y=215), label 12px `#444` at x=20:** "relational: ask"; blue box 190×40 (fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) at x=130 labeled "WHERE order_id = 1042", 3px `#2a78d6` arrow to a green box 160×40 (fill `rgba(0,131,0,0.12)`, 2px `#008300` border) at x=430 labeled "drink = latte ✓".
- **Annotation (bold 13px green `#008300`, near x=430, y=265):** "the query names the value, not the path".
- **Caption (12px `#444`, bottom right):** "orders and pointer chain illustrative".

## Twelve Rules, Four Promises

**Tags:** `worked example` (blue), `rules 1–12` (green)

- **Tables all the way down** — rules 1, 2, 4: data, and the catalog describing it, are just rows
- **Guaranteed access (rule 2)** — (orders, key 1042, column drink) names exactly one value: latte
- **One language** — rules 5, 6, 7: one language queries, updates, and manages views, whole sets at once
- **Independence** — rules 8, 9, 11: queries survive disk moves, schema splits, even distribution
- **Integrity in the catalog (rule 10)** — "price > 0" is stored in the database, not in app code

*Example (italic):* The DBA moves the orders table to a faster disk and adds an index (rule 8); `SELECT drink FROM orders WHERE order_id = 1042` still returns latte, unchanged.

**Key point:** The 12 rules reduce to four promises — everything is a table, one set-at-a-time language, storage details never leak into queries, and integrity rules live in the catalog.

### Visualization (canvas `c2`, 720×300)

Grouped diagram: four theme bands, each holding its rule-number chips and a one-line promise; rule 0 shown as a banner under the title.

- **Title (bold 15px, `#1a5276`, top center):** "The 12 Rules Group into Four Promises".
- **Rule 0 banner (12px `#6b7280`, centered, y=48):** "Rule 0 — the system must manage data entirely through its relational features".
- **Bands (rows at y = 90, 138, 186, 234), each:** bold 13px theme-colored label at x=20; three rule chips at x = 250, 292, 334 (rounded 34×24, 4px radius, solid theme fill, bold 12px white number centered); one 12px `#444` description starting at x=386.
  - Blue `#2a78d6` "Everything is a table", chips `[1, 2, 4]` — "catalog is rows too; (table, key, column) finds any value"
  - Aqua `#199e70` "One language", chips `[5, 6, 7]` — "one language handles whole sets; views act like tables"
  - Violet `#4a3aa7` "Independence", chips `[8, 9, 11]` — "storage/schema/distribution changes never break queries"
  - Magenta `#d55181` "Integrity built in", chips `[3, 10, 12]` — "uniform NULLs; rules live in the catalog; no back doors"
- **Annotation (bold 13px ink `#1a5276`, centered, y=278):** "twelve rules, one demand: nothing depends on how data is stored".
- **Caption (12px `#444`, bottom right):** "grouping is a common reading of the 1985 rules".

## The Yardstick Every SQL Database Chased

**Tags:** `where it's used` (blue), `history` (orange)

- **The test** — through the late 1980s, buyers scored candidate databases against the 12 rules
- **The convergence** — chasing one checklist is why the big SQL databases feel interchangeable
- **The bar** — Codd himself judged that in 1985 no shipping product satisfied all twelve
- **The rebellion** — key-value stores dropped the query language; document stores dropped flat tables
- **The trade** — NoSQL broke specific rules deliberately, trading Codd's guarantees for speed or scale

*Example (italic):* A key-value store fetches order 1042 instantly by key but cannot answer "which orders were lattes?" without scanning everything — access kept, query language dropped.

**Key point:** The rules explain both why SQL databases all look alike and exactly what you give up when you pick a store that skips them.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: an illustrative count of Codd's rules honored, by store type, on a 0–12 scale.

- **Title (bold 15px, `#1a5276`, top center):** "Rules Honored out of 12: SQL Kept the Checklist, NoSQL Chose Its Breaks".
- **Axis:** 2px `#999` vertical baseline at x=210, bars extend right at 38px per rule; vertical gridlines `#e5e9ef` at rule counts 4, 8, 12 (x = 362, 514, 666) with 11px `#6b7280` tick labels "4", "8", "12" below the bottom bar.
- **Rows (bars 18px tall, centered on y = 80, 128, 176, 224), each with a left-aligned 12px `#444` label at x=20 and an 11px count label at the bar end:**
  - "classic SQL database": blue `#2a78d6` bar, rules 11, width 418 — count "11" after the bar; white right-aligned note "duplicate rows, NULL quirks" inside the bar
  - "columnar warehouse": aqua `#199e70` bar, rules 10, width 380 — "10"
  - "document store": orange `#d95926` bar, rules 5, width 190 — "5"
  - "key-value store": magenta `#d55181` bar, rules 2, width 76 — "2"
- **Annotation (bold 13px violet `#4a3aa7`, near x=380, y=252):** "NoSQL didn't fail the test — it opted out of it".
- **Caption (12px `#444`, bottom right):** "rule counts illustrative — scoring any real product is contested".

## Relational Doesn't Mean "Has Relationships"

**Tags:** `common mistake` (red), `naming` (orange)

- **The guess** — most people assume "relational" means tables related to each other by foreign keys
- **The truth** — a "relation" is Codd's math word for a table: a set of rows over named columns
- **The irony** — foreign keys exist in the model, but they are not what the name refers to
- **A single table** — one lone orders table with no foreign keys at all is still fully relational
- **SQL slips** — SQL itself bends the math: it allows duplicate rows, which a true set never has

*Example (italic):* The coffee shop's single orders table, joined to nothing, is a relation; a pointer-linked web of "related" records is exactly what Codd was replacing.

**Common mistake:** Reading "relational" as "relationships between tables." The name comes from the mathematical relation — the table itself — not from foreign keys or joins.

### Visualization (canvas `c4`, 720×300)

Two-panel diagram: the popular reading (two tables joined by a foreign-key arrow) vs Codd's meaning (a single table drawn as a set of rows).

- **Title (bold 15px, `#1a5276`, top center):** "'Relational' Names the Table Itself, Not the Lines Between Tables".
- **Divider:** vertical dashed `#6b7280` (dash 4/3) line at x=360 from y=55 to y=265.
- **Left panel, label bold 13px `#6b7280` at (60, 70):** "what people assume"; blue rounded box 110×40 (fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) at (70, 95) labeled "customers"; second blue box 110×40 at (200, 175) labeled "orders"; 3px `#2a78d6` arrow between them with 12px `#444` label "foreign key"; bold 12px red `#e74c3c` line at (60, 250): "✗ not the source of the name".
- **Right panel, label bold 13px `#6b7280` at (400, 70):** "what Codd meant"; a mini table at (420, 90), 240×130, 2px `#008300` border, header row fill `rgba(0,131,0,0.12)` with 12px bold `#2c3e50` "order_id | drink | price", then three 12px `#2c3e50` data rows from the hardcoded array `[[1041, "mocha", 5.00], [1042, "latte", 4.50], [1043, "tea", 3.00]]`, row separators 1px `#e5e9ef`; bold 12px green `#008300` line at (420, 250): "✓ a relation = a set of rows".
- **Annotation (bold 13px magenta `#d55181`, centered, y=282):** "one table, zero joins — still 100% relational".
- **Caption (12px `#444`, bottom right):** "menu prices illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the coffee-shop orders (1040–1043, latte/mocha/tea, prices) and the rules-honored counts (11 / 10 / 5 / 2) are invented and labeled illustrative; the rule numbers, their four-theme grouping, rule 0's wording, and the 1985 publication facts (E.F. Codd, Computerworld) are documented history.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
