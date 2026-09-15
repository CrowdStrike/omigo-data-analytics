# BigQuery

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** BigQuery

**Subtitle:** Google's serverless data warehouse — you write SQL, the service finds the machines, and the bill is the bytes your query reads

## A Warehouse With No Servers to Size

**Tags:** `core idea` (blue), `serverless` (green), `Dremel` (orange)

- **The analyst** — needs one number from a 2 TB `events` table: daily active users for last week
- **The old way** — a classic warehouse means sizing a cluster, provisioning it, and tuning it first
- **BigQuery** — Google's serverless warehouse, built on its internal Dremel query engine
- **No cluster** — the service allocates thousands of workers per query, then releases them
- **The storage** — the table lives as columnar files on Colossus, Google's distributed filesystem
- **The meter** — on-demand pricing charges by bytes scanned, not by machines or hours

*Example (italic):* The analyst opens a browser, pastes SQL against the 2 TB table, and gets an answer in seconds — nobody ever created or sized a cluster.

**Key point:** BigQuery separates storage from compute and hides the compute entirely — the only knob the analyst controls is how many bytes the query has to read.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: running one query on a classic self-managed warehouse (three setup steps before SQL) vs on BigQuery (SQL straight to answer, billed by bytes).

- **Title (bold 15px, `#1a5276`, top center):** "One Query, Two Worlds: Provision a Cluster vs Just Write SQL".
- **Row 1 (boxes centered on y=110), label 12px `#444` at x=20:** "classic warehouse"; three orange `#d95926` rounded boxes at x=140, x=330, x=520 labeled "size a cluster" / "provision & tune" / "run the query" (12px), joined by 3px `#6b7280` arrows; bold 12px orange label "capacity planning before any SQL" under the row at y=150.
- **Row 2 (boxes centered on y=215), label:** "BigQuery"; blue `#2a78d6` box at x=140 labeled "write SQL", 3px arrow to a green `#008300` box at x=330 labeled "service allocates workers (Dremel)", arrow to a green box at x=520 labeled "answer + bytes-scanned bill" with bold 12px green "✓" at its right.
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(217,89,38,0.12)` / `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "no machines to manage — the meter runs on bytes read".
- **Caption (12px `#444`, bottom right):** "flow schematic; table size illustrative".

## Forty Columns, but You Only Pay for Two

**Tags:** `worked example` (blue), `columnar storage` (green)

- **The table** — `events` holds 40 columns and 2,000 GB of data covering one year (illustrative)
- **Columnar files** — BigQuery stores each column separately, so a query reads only columns it names
- **SELECT star** — `SELECT *` touches all 40 column files: 2,000 GB scanned, $10.00 at $5/TB
- **Two columns** — `SELECT user_id, event_time` reads just those two files: 100 GB, $0.50
- **Hand-check** — 2,000 GB × $5 per 1,000 GB = $10.00; 100 GB × $5 per 1,000 GB = $0.50
- **Rows don't help** — both queries return the same rows; the 20× saving is purely column choice

*Example (italic):* The analyst reruns the same daily-active-users query naming only `user_id` and `event_time` — the bill drops from $10.00 to $0.50 and the answer is identical.

**Key point:** In a columnar store, `SELECT *` is a price decision, not a convenience — naming 2 of 40 columns cut this scan from 2,000 GB to 100 GB.

### Visualization (canvas `c2`, 720×300)

Column-strip diagram: 40 thin vertical strips representing the table's column files; the 2 strips the query names are highlighted green, the other 38 stay gray and unread.

- **Title (bold 15px, `#1a5276`, top center):** "SELECT user_id, event_time Reads 2 of 40 Column Files".
- **Strips:** 40 vertical rectangles starting at x=60, each 13px wide with a 2px gap (total span 600), top y=80, height 130; fill `rgba(107,114,128,0.25)` with 1px `#6b7280` border.
- **Highlighted strips:** indices 3 and 17 filled solid green `#008300`; bold 12px green labels "user_id" and "event_time" above them at y=72, connected by short 1px green leader lines.
- **Bottom labels (12px `#444`, y=232):** "40 column files — 2,000 GB total" centered under the strip row.
- **Annotation (bold 13px green `#008300`, centered at y=262):** "2 columns read = 100 GB scanned = $0.50, vs SELECT * = 2,000 GB = $10.00".
- **Caption (12px `#444`, bottom right):** "sizes and $5/TB rate illustrative; arithmetic exact".

## Partition Filters Turn a Table Scan Into a Week Scan

**Tags:** `where it's used` (blue), `partitioning` (green), `cost control` (orange)

- **Partitioning** — the table is split by day, so a date filter prunes whole days before the scan
- **The filter** — `WHERE event_date >= last 7 days` keeps 7 of 365 daily partitions
- **The scan** — 7/365 of the 100 GB two-column read is about 2 GB, so the query costs about $0.01
- **Clustering** — sorting within partitions by `user_id` lets BigQuery skip blocks inside each day
- **The habit** — dashboards that run hourly live or die by partition filters, not by query speed
- **Dry run** — BigQuery shows the bytes a query will scan before you run it, for free

*Example (italic):* The same query with a 7-day partition filter scans ~2 GB instead of 2,000 GB — a 1,000× cost drop from $10.00 to about $0.01.

**Key point:** Column selection and partition filtering multiply: 2 of 40 columns times 7 of 365 days took this query from 2,000 GB to ~2 GB scanned.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: bytes scanned (and dollar cost) for the three versions of the same query — SELECT *, two columns, two columns + 7-day partition filter.

- **Title (bold 15px, `#1a5276`, top center):** "Same Answer, Three Bills: 2,000 GB → 100 GB → 2 GB".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (bars 22px tall, centered at y = 90, 155, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "SELECT * (40 cols, full year)": red `#e74c3c` bar width 420, 12px bold red end label "2,000 GB — $10.00"
  - "2 columns, full year": blue `#2a78d6` bar width 190, 12px bold blue end label "100 GB — $0.50"
  - "2 columns + 7-day partition filter": green `#008300` bar width 36, 12px bold green end label "~2 GB — $0.01"
- **Bar fills:** red `rgba(231,76,60,0.30)` / blue `rgba(42,120,214,0.30)` / green `rgba(0,131,0,0.30)`, each with a 2px solid border in its line color.
- **Annotation (bold 13px violet `#4a3aa7`, near x=300, y=255):** "1,000× cheaper without touching the SQL result".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic; GB figures illustrative, cost arithmetic exact at $5/TB".

## LIMIT 10 Does Not Limit the Bill

**Tags:** `common mistake` (red), `LIMIT` (orange)

- **The instinct** — analysts add `LIMIT 10` to "peek cheaply", as they would on a row-store database
- **The reality** — LIMIT trims the rows returned, after the named columns are already fully scanned
- **The bill** — `SELECT * FROM events LIMIT 10` still scans all 2,000 GB and still costs $10.00
- **Free peeks** — the table preview and a dry run cost nothing; that is how you look before you pay
- **The fix** — cheap exploration names few columns and filters on the partition column

*Example (italic):* A "quick look" of `SELECT * ... LIMIT 10` bills 2,000 GB — the ten rows shown cost $10.00, while a two-column, one-week peek costs about $0.01.

**Common mistake:** Reading LIMIT as a cost control. BigQuery prices the scan, not the result set — only column selection and partition/cluster pruning shrink the bytes billed.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the LIMIT-10 "peek" still scanning the whole table vs a column-and-partition peek scanning almost nothing.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways to Peek at 10 Rows".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "SELECT * LIMIT 10"; blue `#2a78d6` rounded box at x=180 labeled "scan all 40 columns, full year" (12px), 3px arrow to a red `#e74c3c` box at x=440 labeled "2,000 GB billed — $10.00" with bold 12px red "✗ LIMIT applied after the scan" beneath at y=140.
- **Row 2 (boxes centered on y=215), label:** "2 cols + 7-day filter, LIMIT 10"; blue box at x=180 labeled "scan 2 columns, 7 partitions", 3px arrow to a green `#008300` box at x=440 labeled "~2 GB billed — $0.01" with bold 12px green "✓ same 10 rows on screen" beneath at y=250.
- **Box style:** 170–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=282):** "run the free dry run first — it prints the bytes before you spend them".
- **Caption (12px `#444`, bottom right):** "GB and dollar figures illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all figures are the hardcoded values above (no randomness); table size (2,000 GB), column-pair size (100 GB), 7-day scan (~2 GB), and the $5/TB rate are invented and labeled illustrative; the cost arithmetic (2,000 GB → $10.00, 100 GB → $0.50, ~2 GB → ~$0.01) and the 7/365 partition fraction are exact given those inputs. Facts stated as facts (Dremel engine, columnar storage on Colossus, per-bytes-scanned on-demand pricing, LIMIT not reducing bytes billed, free dry run) are publicly documented BigQuery behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
