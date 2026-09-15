# DuckDB

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** DuckDB

**Subtitle:** DuckDB is an analytical database that runs inside your program — as SQLite is to transactions, DuckDB is to analytics: no server, no cluster, just a library

## One Laptop, One Folder, No Server

**Tags:** `core idea` (blue), `in-process` (green), `SQLite analogy` (orange)

- **The folder** — an analyst has 24 monthly Parquet files of orders on a laptop: 180M rows, 9 GB on disk
- **The old way** — load a warehouse or a cluster to run one GROUP BY; setup takes longer than the question
- **The library** — DuckDB is `pip install duckdb`; the database engine runs inside the Python process
- **No server** — nothing to start, connect to, or administer — exactly how SQLite works for transactions
- **The twist** — SQLite is built for many small row updates; DuckDB is built for big scans and aggregates

*Example (italic):* The analyst types `duckdb.sql("SELECT ... FROM 'orders/*.parquet'")` and gets an answer — no import job, no cluster, no connection string.

**Key point:** DuckDB fills the empty quadrant: an embedded, in-process database aimed at analytics — as SQLite is to transactions, DuckDB is to analytics.

### Visualization (canvas `c1`, 720×300)

Quadrant chart placing four databases on two axes: embedded vs client-server (x) and transactional vs analytical (y); DuckDB highlighted in the embedded-analytical quadrant.

- **Title (bold 15px, `#1a5276`, top center):** "The Quadrant DuckDB Fills: Embedded + Analytical".
- **Axes:** horizontal 2px `#999` line at y=165 from x=60 to x=660; vertical 2px `#999` line at x=360 from y=50 to y=280; axis end labels 12px `#6b7280`: "runs inside your program" (left, x=65, y=158), "separate server" (right-aligned, x=655, y=158), "analytics — scans & aggregates" (top, centered at x=360, y=44), "transactions — single-row updates" (bottom, centered at x=360, y=294).
- **Quadrant boxes (150×44px, 8px radius, 13px bold labels centered):** DuckDB at (135, 85) — fill `rgba(0,131,0,0.15)`, 2px `#008300` border, text `#008300`; SQLite at (135, 210) — fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border, text `#2c3e50`; data warehouse at (500, 85) — same blue style; PostgreSQL at (500, 210) — same blue style.
- **Annotation (bold 13px green `#008300`, centered under the DuckDB box at y=145):** "no server, built for scans".
- **Caption (12px `#444`, bottom right):** "placement schematic".

## Grouping 180 Million Rows Without Loading Them

**Tags:** `worked example` (blue), `Parquet` (green)

- **The query** — `SELECT month, SUM(amount) FROM 'orders/*.parquet' GROUP BY month`, straight on the files
- **Hand-check** — 24 files × 7.5M rows each = 180M rows scanned (exact multiplication)
- **The pandas way** — `read_parquet` on all 24 files inflates 9 GB compressed to ~30 GB in RAM (illustrative)
- **The crash** — the laptop has 16 GB of RAM, so the load-everything approach dies before the GROUP BY
- **The stream** — DuckDB aggregates batch by batch: peak memory 1.2 GB, 8 seconds (illustrative)

*Example (italic):* Same laptop, same 24 files: pandas needs ~30 GB it doesn't have; DuckDB answers the monthly-revenue query using 1.2 GB.

**Key point:** DuckDB queries Parquet and CSV files in place — no loading step — and streams the scan, so the dataset never has to fit in memory.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart comparing peak memory for the same GROUP BY: pandas load-everything vs DuckDB streaming, with the laptop's 16 GB RAM as a dashed limit line.

- **Title (bold 15px, `#1a5276`, top center):** "Same Query, Same Laptop: Peak Memory to Group 180M Rows".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, x scale 0–32 GB over 440px (13.75 px/GB); 12px `#444` tick labels "0", "8 GB", "16 GB", "24 GB", "32 GB" under y=250 at x = 230/340/450/560/670.
- **Rows (left-aligned 12px `#444` labels at x=20):**
  - y=90: "pandas — load all 24 files": red `#e74c3c` bar width 412 (30 GB), bold 12px red label "~30 GB — crash" at bar end
  - y=170: "DuckDB — stream & aggregate": green `#008300` bar width 17 (1.2 GB), bold 12px green label "1.2 GB, 8 s" right of the bar
- **RAM line:** vertical dashed `#6b7280` (dash 4/3) line at x=450 from y=60 to y=245, 12px `#6b7280` label "laptop RAM 16 GB" above it at y=54.
- **Bar style:** 22px tall, solid fill, 4px corner radius.
- **Annotation (bold 13px violet `#4a3aa7`, near x=470, y=215):** "the dataset never has to fit in memory".
- **Caption (12px `#444`, bottom right):** "memory and timing illustrative; 24 × 7.5M = 180M rows exact".

## Why Data Scientists Reach for It

**Tags:** `where it's used` (blue), `columnar` (green)

- **The niche** — larger-than-memory local analysis: too big for pandas, too small to justify a cluster
- **Columnar** — data is stored and processed by column, so the query touches only the columns it names
- **The savings** — the revenue query reads 2 of 14 columns: ~1.3 GB of the 9 GB folder (illustrative)
- **Skipping files** — adding `WHERE year = 2025` lets Parquet metadata skip 12 files: ~0.65 GB read
- **Vectorized** — values are processed in batches of up to 2048 (exact engine default), not row by row

*Example (italic):* Full rows would mean 9 GB of reading; naming just `order_date` and `amount` cuts it to ~1.3 GB, and the 2025 filter halves that to ~0.65 GB.

**Key point:** A columnar, vectorized engine reads only the columns and files a query needs — that, in one library, is why DuckDB became the pandas alternative for local analytics.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of bytes read from the 9 GB folder under three query shapes: full-row scan, two named columns, two columns plus a year filter.

- **Title (bold 15px, `#1a5276`, top center):** "Bytes Read From the 9 GB Folder: Columns and Filters Shrink the Scan".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, x scale 0–9 GB over 440px (48.9 px/GB); no tick marks, width labels at bar ends instead.
- **Rows (left-aligned 12px `#444` labels at x=20):**
  - y=75: "SELECT * — all 14 columns": blue `#2a78d6` bar width 440, 12px `#444` label "9 GB" at end
  - y=145: "2 columns (date, amount)": aqua `#199e70` bar width 64, bold 12px aqua label "~1.3 GB" at end
  - y=215: "2 columns + WHERE year = 2025": green `#008300` bar width 32, bold 12px green label "~0.65 GB — 12 files skipped" at end
- **Bar style:** 24px tall, fills `rgba(42,120,214,0.30)` for the blue row, solid for the aqua and green rows.
- **Annotation (bold 13px magenta `#d55181`, right side near y=260):** "name your columns — the engine reads nothing else".
- **Caption (12px `#444`, bottom right):** "gigabytes illustrative; column-pruning behavior documented".

## It Is a Library, Not a Warehouse

**Tags:** `common mistake` (red), `not a server` (orange)

- **The confusion** — teams point five dashboards at one DuckDB file and expect a database server
- **What it is** — each process embeds its own engine; there is no shared service accepting connections
- **One writer** — like SQLite, a DuckDB database file allows a single writing process at a time
- **The right share** — keep the Parquet files on shared storage; every analyst queries them in-process
- **The boundary** — hundreds of concurrent users writing all day is warehouse territory, not DuckDB's

*Example (italic):* Five dashboards fighting over one `analytics.duckdb` file stall on the single-writer lock; five DuckDB processes reading the same Parquet folder work fine.

**Common mistake:** Treating DuckDB as a small data warehouse. It is an embedded engine for one process's analysis — share the files, not a running database.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: many clients hitting one DuckDB file as if it were a server (stall) vs each analyst running an in-process engine over shared Parquet files (works).

- **Title (bold 15px, `#1a5276`, top center):** "Share the Files, Not a Server".
- **Row 1 (y=95), label 12px `#444` at x=20:** "as a server ✗"; blue `#2a78d6` rounded box at x=150 labeled "5 dashboards, live writes" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "one .duckdb file — single writer" with bold 12px red "✗ writers queue and stall" beneath it at y=140.
- **Row 2 (y=205), label:** "as a library ✓"; blue box at x=150 labeled "Parquet folder on shared storage", 3px arrows fanning to two green `#008300` boxes at x=420 (y=185) "analyst A: in-process DuckDB" and (y=230) "analyst B: in-process DuckDB", bold 12px green "✓ each reads independently" at x=600, y=210.
- **Box style:** 170–190px wide, 38px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "the SQLite analogy cuts both ways: embedded strengths, embedded limits".

## Reading Remote Files Borrows Someone Else's Consistency

**Tags:** `remote storage` (blue), `borrowed guarantees` (orange), `stale scans` (red)

- **Local is simple** — a query over its own file sees one transactional snapshot, start to finish
- **Remote changes the picture** — pointing at Parquet in object storage puts a network inside the query plan
- **No coordination** — the engine never locks or versions remote files; it reads what the store returns
- **Two scans can differ** — a writer adding a file mid-scan makes the same SQL return different counts
- **Cross-region reads lag** — a mirrored bucket updates asynchronously, so an older file list is valid
- **Table formats fix this** — Iceberg or Delta hand the scan one committed manifest, not a live listing
- **The practical habit** — query an immutable snapshot or a committed version, never a folder in flux

*Example (italic):* A scan of `s3://.../events/` started a second before a loader adds `part-042.parquet` reports fewer rows than the identical query run a second later.

**Key point:** DuckDB's own guarantees stop at its file. Over remote storage its consistency is the store's consistency — and a plain folder listing offers none.

### Visualization (canvas `c5`, 720×300)

Two lanes over a shared timeline: a local-file query holding one snapshot, and two remote scans of the same prefix returning different counts while a loader adds a file.

- **Title (bold 15px `#1a5276`, top center):** "Same Query, Two Answers — Because the Folder Moved".
- **Time axis:** 2px `#6b7280` arrow at y=262 from x=110 to x=680, centered 12px label "time" at (395,282).
- **Loader event:** 2.5px dashed `#d95926` vertical line at x=400 from y=60 to y=255; bold 12px `#d95926` label "loader adds part-042.parquet" centered at (400,50).
- **Lane 1 — local file (y=105):** right-aligned 12px `#2c3e50` label "local .duckdb file" at x=100; a 4px `#008300` bar from x=140 to x=660; two 6px green dots at x=250 and x=520 with bold 12px `#008300` labels "12.0M rows" above each at y=86; bold 12px `#008300` note "one snapshot per query — unaffected" at (400,130), centered.
- **Lane 2 — remote prefix (y=200):** right-aligned label "remote Parquet prefix" at x=100; a 4px `#6b7280` bar from x=140 to x=400 then a 4px `#c98500` bar from x=400 to x=660; 6px `#6b7280` dot at x=250 labeled bold 12px `#6b7280` "12.0M rows" at (250,182); 6px `#c98500` dot at x=520 labeled bold 12px `#c98500` "12.4M rows" at (520,182).
- **Difference band:** rect x=400–660, y=170–232, fill `rgba(201,133,0,0.12)`, no stroke, drawn behind lane 2; bold 12px `#c98500` label "same SQL, 0.4M more rows" at (530,226), centered.
- **Annotation (bold 12px `#e74c3c`, centered at (400,248)):** "no error, no warning — the folder listing changed under the query".
- **Caption (12px `#444`, bottom left at (110,282)):** "row counts illustrative".


- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then five `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); file counts, row counts, gigabytes, memory, and timings are invented and labeled illustrative; 24 × 7.5M = 180M is exact arithmetic; the ~2048-value vector batch size, in-process/embedded architecture, direct Parquet/CSV querying, column pruning, and single-writer behavior are documented DuckDB facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
