# Hive

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Hive

**Subtitle:** Hive put a SQL layer over raw files on Hadoop so analysts could query them without writing Java — and its table catalog outlived the engine itself

## The Analysts Who Couldn't Write MapReduce

**Tags:** `core idea` (blue), `SQL over files` (green), `Hadoop` (orange)

- **The problem** — Facebook's data sat in Hadoop, and every question meant a hand-written Java MapReduce job
- **The bottleneck** — analysts knew SQL, not Java; each report queued behind an engineer
- **The fix** — Hive accepts a SQL-like query (HiveQL) and compiles it into MapReduce jobs for you
- **The cost** — a compiled query still launches batch jobs, so even small answers take minutes
- **The win** — a 6-line SQL query replaces a ~180-line Java job (line counts illustrative)

*Example (italic):* An analyst types `SELECT country, COUNT(*) FROM orders GROUP BY country`; Hive turns it into a MapReduce job and the answer arrives 4 minutes later instead of 4 days later.

**Key point:** Hive's core idea is a translation layer: you write SQL, it writes the MapReduce jobs — trading query speed for the ability to ask questions at all.

### Visualization (canvas `c1`, 720×300)

Two-row before/after flow diagram: answering one question without Hive (analyst → engineer → Java job → days) vs with Hive (analyst → HiveQL → compiled MapReduce → minutes).

- **Title (bold 15px, `#1a5276`, top center):** "One Question, Two Paths: Hand-Written Java vs Compiled SQL".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "before Hive"; rounded boxes left to right at x=110, 280, 450 labeled "analyst's question" (blue `#2a78d6`), "engineer writes ~180-line Java job" (orange `#d95926`), "MapReduce runs" (blue), joined by 3px `#6b7280` arrows; bold 13px red `#e74c3c` label at x=620: "answer in days".
- **Row 2 (boxes centered on y=215), label:** "with Hive"; boxes at x=110, 280, 450 labeled "analyst writes 6-line HiveQL" (green `#008300`), "Hive compiles to MapReduce" (blue), "MapReduce runs" (blue); bold 13px green `#008300` label at x=620: "answer in minutes".
- **Box style:** 140–160px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(217,89,38,0.15)`, 12px `#2c3e50` text wrapped to two lines.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "same engine underneath — Hive only changes who can ask".
- **Caption (12px `#444`, bottom right):** "line counts and turnaround times illustrative".

## A Table That Is Just a Folder of Files

**Tags:** `worked example` (blue), `schema-on-read` (green)

- **The files** — a directory `/logs/orders/` on HDFS holds 3 plain text files with 5, 4, and 3 rows
- **The table** — `CREATE EXTERNAL TABLE orders(id INT, country STRING, amount DOUBLE) LOCATION '/logs/orders/'`
- **No loading** — Hive copies nothing and checks nothing; the files stay exactly where they were
- **The query** — `SELECT country, COUNT(*) FROM orders GROUP BY country` compiles to one MapReduce job
- **Hand-check** — 12 rows total: 5 US + 4 DE + 3 IN; the job returns US=5, DE=4, IN=3
- **The overhead** — the job takes ~4 minutes for 12 rows: batch startup dominates (time illustrative)

*Example (italic):* Dropping a fourth file into `/logs/orders/` adds its rows to the table instantly — no INSERT, no import; the next query simply reads one more file.

**Key point:** Schema-on-read means the schema is applied when you query, not when you store — a Hive table is a description laid over files, not a container that holds data.

### Visualization (canvas `c2`, 720×300)

Two-panel chart: left, three HDFS file boxes stacking into a virtual table; right, the GROUP BY result as a bar chart whose counts match the files.

- **Title (bold 15px, `#1a5276`, top center):** "Three Files, One Table, One GROUP BY".
- **Left panel (x 20–330):** three rounded file boxes at y=80, 140, 200 (150px wide, 40px tall, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text) labeled "file-001.txt — 5 rows", "file-002.txt — 4 rows", "file-003.txt — 3 rows"; a dashed `#6b7280` (dash 4/3) bracket at x=200 gathering them into a green `#008300` box at x=250, y=140 labeled "orders (12 rows)"; 11px `#6b7280` caption under the boxes: "schema applied at read time".
- **Right panel (x 360–700):** vertical bar chart, baseline y=245, plot height 150; three bars 60px wide at x=420, 520, 620 with heights proportional to counts 5, 4, 3 (pixel heights 150, 120, 90); fills blue `rgba(42,120,214,0.35)` with 2px `#2a78d6` borders; 12px `#444` category labels "US", "DE", "IN" below the baseline and bold 13px `#1a5276` count labels "5", "4", "3" above each bar.
- **Annotation (bold 12px orange `#d95926`, above the bars near y=70):** "12 rows, ~4 min — startup, not data, sets the clock".
- **Caption (12px `#444`, bottom right):** "row counts exact for this example; runtime illustrative".

## The Component That Outlived the Product

**Tags:** `why it matters` (blue), `metastore` (green), `legacy` (orange)

- **The catalog** — the Hive Metastore is a small relational database mapping table names to schemas and file paths
- **The split** — the slow query engine and the catalog were separate pieces; only one aged badly
- **The retrofit** — Hive later swapped MapReduce for Tez or Spark underneath, but the exodus had begun
- **The displacement** — faster engines (Spark, Presto, Trino, Impala) answered the same queries in seconds
- **The survival** — those engines all chose to read the Hive Metastore rather than invent a new catalog
- **Today** — "a Hive table" usually means a Metastore entry queried by Spark or Trino, with Hive's engine never involved

*Example (italic):* A team runs every query through Trino and has never launched a Hive job — yet every table they query is defined in a Hive Metastore.

**Key point:** A well-placed component can outlive its product: the industry replaced Hive's engine but standardized on its catalog, because everyone rewrites the slow part and nobody wants to migrate the bookkeeping.

### Visualization (canvas `c3`, 720×300)

Hub-and-spoke diagram: the Hive Metastore at center, four modern engines connected to it, and the original Hive engine faded off to the side.

- **Title (bold 15px, `#1a5276`, top center):** "Everyone Left the Engine, Everyone Kept the Catalog".
- **Hub (center x=360, y=165):** rounded box 190px wide, 56px tall, fill `rgba(26,82,118,0.15)`, 2px `#1a5276` border, two-line bold 13px `#1a5276` text "Hive Metastore" / "table → schema → file path".
- **Engine boxes (130px wide, 40px tall, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px `#2c3e50` text)** at (x=110, y=85) "Spark", (x=110, y=245) "Presto", (x=610, y=85) "Trino", (x=610, y=245) "Impala", each joined to the hub by a 2px `#008300` line.
- **Faded engine:** gray box at (x=360, y=270), 150px wide, fill `rgba(107,114,128,0.12)`, dashed 2px `#6b7280` border, 12px `#6b7280` text "Hive engine (displaced)", joined to the hub by a dashed `#6b7280` (dash 4/3) line.
- **Annotation (bold 13px violet `#4a3aa7`, top right near x=560, y=55):** "the catalog outlived the query engine".
- **Caption (12px `#444`, bottom right):** "engines shown are the widely documented Metastore clients".

## It Looks Like a Database, But It Isn't One

**Tags:** `common mistake` (red), `schema-on-read` (orange)

- **The confusion** — the SQL syntax makes people expect database behavior: fast queries, indexes, checks on write
- **No gatekeeper** — a database rejects a malformed row at INSERT; Hive accepts any file into the folder
- **Silent damage** — a file whose columns are shifted parses without error and yields NULLs at query time
- **No shortcuts** — no OLTP indexes, only partition pruning trims scans; no fast path for one row
- **The mistake** — discovering weeks of bad rows only when a report suddenly shows NULL countries

*Example (italic):* A producer swaps two columns on March 1; a database would have rejected every row that day, but the Hive table absorbs 3 weeks of bad files before an analyst notices the NULLs on March 22.

**Common mistake:** Treating schema-on-read as free schema enforcement. Nothing validates data on the way in — the schema is only a lens at read time, so bad data ages quietly inside the table until a query exposes it.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a malformed row entering a database (rejected at write) vs entering a Hive table (accepted, surfacing as NULLs three weeks later).

- **Title (bold 15px, `#1a5276`, top center):** "Schema-on-Write Rejects Early, Schema-on-Read Fails Late".
- **Row 1 (boxes centered on y=100), label 12px `#444` at x=20:** "database"; blue `#2a78d6` rounded box at x=170 labeled "bad row arrives Mar 1" (12px), 3px arrow to a red `#e74c3c` box at x=400 labeled "INSERT rejected — error on day one" with bold 12px green `#008300` "✓ caught at write" at x=600.
- **Row 2 (boxes centered on y=210), label:** "Hive table"; blue box at x=170 "bad file lands Mar 1", 3px arrow to a gray box at x=380 (fill `rgba(107,114,128,0.12)`) labeled "sits unchecked 3 weeks", then arrow to a red box at x=580 labeled "NULLs in Mar 22 report" with bold 12px red `#e74c3c` "✗ caught at read".
- **Box style:** 150–170px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(107,114,128,0.12)`, 12px `#2c3e50` text wrapped to two lines.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the schema is a lens at read time, not a gate at write time".
- **Caption (12px `#444`, bottom right):** "dates and 3-week gap illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the worked-example row counts (5 / 4 / 3 files summing to 12, GROUP BY result US=5, DE=4, IN=3) are exact within the example and must match between text and chart; line counts (~180 vs 6), query runtime (~4 min), and the 3-week bad-data gap are invented and labeled illustrative; the Facebook origin, HiveQL-to-MapReduce (later Tez/Spark) compilation, schema-on-read, and Metastore usage by Spark/Presto/Trino/Impala are publicly documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
