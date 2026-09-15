# Trino / Presto

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Trino / Presto

**Subtitle:** Trino is a SQL engine with no storage of its own — connectors let one query join tables that live in completely different systems

## One Query Over Data It Doesn't Own

**Tags:** `core idea` (blue), `federated SQL` (green), `no storage` (orange)

- **The setup** — an orders table lives as Parquet files in a data lake; customers live in Postgres
- **The old answer** — copy one table into the other system overnight, then query the copy
- **The engine** — Trino, born as Presto at Facebook for interactive SQL on HDFS, stores nothing itself
- **Connectors** — each connector teaches Trino to read one system: lake files, Postgres, MySQL, Kafka
- **The trick** — one `SELECT ... JOIN` names tables by catalog, and Trino fetches from both live

*Example (italic):* `SELECT ... FROM lake.sales.orders o JOIN pg.public.customers c ON o.customer_id = c.id` runs as one statement — no copy ever made.

**Key point:** Trino is a pure query engine: it owns no data, only connectors — so one SQL statement can join tables that live in different systems.

### Visualization (canvas `c1`, 720×300)

Hub-and-spoke diagram: one SQL statement enters the Trino engine, which fans out through connectors to four different storage systems.

- **Title (bold 15px, `#1a5276`, top center):** "One SQL Statement, Four Systems, Zero Copies".
- **Query box (top center):** rounded box at x=250, y=48, 220×32, fill `rgba(74,58,167,0.12)`, 2px violet `#4a3aa7` border, 12px `#2c3e50` text "SELECT ... JOIN ... (one statement)".
- **Engine box (center):** rounded box at x=230, y=110, 260×52, fill `rgba(42,120,214,0.15)`, 2px blue `#2a78d6` border, bold 13px `#1a5276` line "Trino query engine", 11px `#6b7280` line "no storage of its own".
- **Arrow (query → engine):** 3px `#4a3aa7` vertical arrow from (360, 80) to (360, 110).
- **Source boxes (bottom row, y=215, each 150×46, 8px radius, 12px `#2c3e50` labels):** "data lake (Parquet)" at x=25 fill `rgba(0,131,0,0.12)` border `#008300`; "PostgreSQL" at x=200 fill `rgba(42,120,214,0.15)` border `#2a78d6`; "MySQL" at x=375 fill `rgba(201,133,0,0.15)` border `#c98500`; "Kafka" at x=550 fill `rgba(217,89,38,0.15)` border `#d95926`.
- **Connector arrows:** 2px `#6b7280` lines from the engine box bottom (y=162) to each source box top (y=215), one 11px `#6b7280` label "connector" centered on the leftmost arrow.
- **Annotation (bold 13px green `#008300`, right side near y=178):** "the data never moves in advance".
- **Caption (12px `#444`, bottom right):** "systems shown are typical connectors".

## Joining the Lake to Postgres by Hand

**Tags:** `worked example` (blue), `cross-system join` (green)

- **Orders (lake)** — 6 rows of (order, customer, amount): 101/c1/$40, 102/c2/$25, 103/c1/$60, 104/c3/$80, 105/c2/$35, 106/c4/$20
- **Customers (Postgres)** — 4 rows: c1 East, c2 West, c3 East, c4 West
- **The query** — join on customer id, group by region, sum the amounts
- **Hand-check East** — c1 and c3 are East: 40 + 60 + 80 = $180
- **Hand-check West** — c2 and c4 are West: 25 + 35 + 20 = $80
- **The run** — Trino scans Parquet for orders, asks Postgres for customers, joins in its own memory

*Example (italic):* One statement returns East $180 and West $80 (totalling $260) even though no single database holds both tables.

**Key point:** The join happens inside Trino's workers, in memory — each source system only ships its own rows, and the result exists nowhere but the query output.

### Visualization (canvas `c2`, 720×300)

Left half: the two source tables as boxes flowing into a join node. Right half: the joined result as a bar chart of revenue by region.

- **Title (bold 15px, `#1a5276`, top center):** "Lake Orders + Postgres Customers → Revenue by Region".
- **Orders box (left top):** rounded box at x=25, y=62, 195×100, fill `rgba(0,131,0,0.10)`, 2px `#008300` border; bold 12px `#008300` header "lake.sales.orders"; 11px monospace `#2c3e50` rows "101 c1 $40", "102 c2 $25", "103 c1 $60", "104 c3 $80", "105 c2 $35", "106 c4 $20" (two columns of three).
- **Customers box (left bottom):** rounded box at x=25, y=182, 195×82, fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border; bold 12px `#2a78d6` header "pg.public.customers"; 11px monospace rows "c1 East", "c2 West", "c3 East", "c4 West" (two columns of two).
- **Join node:** small rounded box at x=265, y=140, 110×40, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, bold 12px `#4a3aa7` text "JOIN in Trino"; 2px `#6b7280` arrows from both table boxes into its left edge, one 2px `#4a3aa7` arrow from its right edge to the bar chart.
- **Bar chart (right):** baseline 2px `#999` at y=245 from x=430 to x=690; y gridlines `#e5e9ef` at $60/$120/$180 with 11px `#6b7280` labels at x=425 right-aligned; East bar at x=460 width 80, height 160 (=$180), fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; West bar at x=585 width 80, height 71 (=$80), fill `rgba(0,131,0,0.30)`, 2px `#008300` border; bold 13px value labels "$180" and "$80" above the bars, 12px `#444` category labels "East", "West" below the baseline.
- **Annotation (bold 12px violet `#4a3aa7`, near x=430, y=70):** "result lives only in the query output".
- **Caption (12px `#444`, bottom right):** "amounts illustrative; sums exact".

## Why an Engine Without Storage Took Over

**Tags:** `where it's used` (blue), `interactive speed` (green)

- **The origin** — Presto replaced batch MapReduce at Facebook so analysts could query HDFS interactively
- **The execution** — in-memory, pipelined MPP: stages stream rows to each other, nothing hits disk between steps
- **The payoff** — exploratory joins across systems return in seconds, not as tomorrow's ETL output
- **The fork** — Presto's creators left Facebook in 2018 and their fork was renamed Trino in 2020
- **In the wild** — Amazon Athena runs on this engine; many lakehouse stacks use Trino as the SQL front door

*Example (italic):* The same lake-to-Postgres join answered by a nightly ETL copy is 24 hours stale; Trino answers it against live data in about 9 seconds (illustrative).

**Key point:** Pipelined in-memory execution plus federation means the freshest possible answer at interactive speed — the two things batch ETL pipelines give up.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: time-to-answer for the same cross-system join under three approaches, from nightly ETL to federated Trino.

- **Title (bold 15px, `#1a5276`, top center):** "Same Join Question, Three Ways to Wait".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 430; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 85, 150, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "nightly ETL copy — 24 h stale": red `#e74c3c` bar width 430, 11px red label "answer uses yesterday's data" at bar end
  - "batch MapReduce job — 12 min": orange `#d95926` bar width 230, 11px `#444` label "12 min" at bar end
  - "Trino federated query — 9 s": green `#008300` bar width 40, bold 11px green label "9 s, live data" at bar end
- **Bar style:** 22px tall, fills at 0.85 alpha of the row color, 1px solid border in the row color.
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "federation removes the copy step entirely".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic; timings illustrative".

## A Query Engine Is Not a Database

**Tags:** `common mistake` (red), `no storage` (orange)

- **The confusion** — Trino shows tables and runs SQL, so people assume it stores and protects the data
- **Who reads** — every scan is executed by the source system; Trino has no files and no indexes of its own
- **The blast radius** — a heavy federated join can hammer the production Postgres that serves your app
- **Pushdown helps** — connectors push filters down so Postgres returns 4 customer rows, not the whole table
- **The mistake** — pointing an unfiltered scan at a live operational database and stalling it for everyone

*Example (italic):* Without pushdown a 2,000,000-row customers table (illustrative) ships over the network to Trino just to keep 4 rows.

**Common mistake:** Treating Trino as a database. It owns no storage — dropping the lake files breaks the table, and every query's cost lands on the source systems doing the reading.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same filtered query with predicate pushdown (Postgres returns 4 rows) vs without it (2,000,000 rows cross the network).

- **Title (bold 15px, `#1a5276`, top center):** "Pushdown Decides Who Does the Work".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no pushdown"; blue `#2a78d6` rounded box at x=150 labeled "Postgres: full scan" (12px), 3px arrow labeled bold 12px red "2,000,000 rows over network" to a red `#e74c3c` box at x=470 labeled "Trino filters to 4" with bold 12px red "✗ source hammered".
- **Row 2 (y=205), label:** "filter pushed down"; green `#008300` rounded box at x=150 labeled "Postgres: WHERE id IN (...)", 3px arrow labeled bold 12px green "4 rows over network" to a green box at x=470 labeled "Trino joins 4 rows" with bold 12px green "✓".
- **Box style:** 160–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "Trino plans the query; the sources pay for the reads".
- **Caption (12px `#444`, bottom right):** "row counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); order amounts, timings, and row counts are invented and labeled illustrative; the region sums (East 40+60+80 = 180, West 25+35+20 = 80, total 260) are exact arithmetic on those illustrative rows and the chart's $180/$80 bars must match the text.
- **Facts:** stick to publicly documented history — Presto created at Facebook for interactive SQL on HDFS, creators forked in 2018 and the fork was renamed Trino in 2020, Amazon Athena runs on this engine; do not attribute undocumented behavior to any company.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
