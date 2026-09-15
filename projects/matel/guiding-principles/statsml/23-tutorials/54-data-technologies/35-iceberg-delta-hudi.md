# Iceberg, Delta, Hudi

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Iceberg, Delta, Hudi

**Subtitle:** Table formats put a database layer on top of plain files in a data lake — commits, versions, and safe concurrent writes for what used to be just a folder of Parquet

## The Table That Was Just a Folder

**Tags:** `core idea` (blue), `data lake` (green), `metadata layer` (orange)

- **The lake** — a retailer's `orders` table is 1,200 Parquet files in one S3 folder, and nothing more
- **No transactions** — a reader listing the folder mid-write sees 3 of a job's 5 new files
- **No safe concurrency** — two jobs writing at once silently overwrite or double each other's output
- **The fix** — Iceberg (from Netflix), Delta Lake (Databricks) and Hudi (Uber) add a metadata layer
- **The pointer** — the table is whatever the current snapshot's manifest lists; loose files mean nothing

*Example (italic):* An analyst's 2:00pm query used to catch half-written loads; with a table format it reads committed snapshot 510 — complete data or nothing.

**Key point:** A table format turns a folder of Parquet files into a real table by keeping a manifest of exactly which files belong to each committed version.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: a reader hitting a plain folder mid-write (sees partial data) vs a reader following the table format's snapshot pointer (sees all or nothing).

- **Title (bold 15px, `#1a5276`, top center):** "Plain Folder vs Table Format: What a Mid-Write Reader Sees".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "plain folder"; blue `#2a78d6` rounded box at x=140 labeled "writer: adding 5 files" (12px), 3px `#6b7280` arrow to a red `#e74c3c` box at x=400 labeled "reader lists folder — sees 3 of 5" with bold 12px red "✗ partial table" to its right at x=590.
- **Row 2 (boxes centered on y=215), label 12px `#444` at x=20:** "table format"; blue box at x=140 labeled "writer builds snapshot 511", 3px arrow to a green `#008300` box at x=350 labeled "atomic pointer swap 510→511", arrow to a green box at x=560 labeled "reader sees 510 or 511" with bold 12px green "✓" above it.
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, 1.5px borders in the box color.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "the manifest — not the folder listing — defines the table".
- **Caption (12px `#444`, bottom right):** "file counts illustrative".

## Two Jobs, One Table, No Corruption

**Tags:** `worked example` (blue), `ACID commit` (green), `optimistic concurrency` (orange)

- **Two writers** — at 2:00pm an hourly append job and a file-compaction job both start from snapshot 510
- **The append** — the append job writes 40,000 new order rows, then swaps the pointer 510 → 511 at t=45s
- **The conflict** — at t=50s the compaction job tries to commit and finds the pointer already moved
- **The retry** — it re-validates against 511 (no overlapping files), then swaps 511 → 512 at t=75s
- **The guarantee** — every reader saw exactly 510, 511, or 512 — never a mix, never a half-commit

*Example (italic):* Snapshot 511 holds 1.24M rows (1.20M + 40,000 appended); compaction's snapshot 512 holds the same 1.24M rows in ~300 files instead of 1,200.

**Key point:** ACID on a lake comes down to one atomic operation — swapping the current-snapshot pointer; when two writers race, one wins and the other retries against the new snapshot.

### Visualization (canvas `c2`, 720×300)

Two-lane commit timeline for the 90 seconds after 2:00pm: append job and compaction job racing to commit, with a pointer-value track showing the two atomic swaps.

- **Title (bold 15px, `#1a5276`, top center):** "Two Writers, One Pointer: Commit, Conflict, Retry".
- **Axes:** origin x=60, plot width 600; x = seconds 0 to 90, 12px `#444` tick labels "0s"–"90s" every 30s along a 2px `#999` baseline at y=250; three horizontal lanes with left labels (12px `#444` at x=20): "append job" at y=100, "compaction job" at y=160, "table pointer" at y=220.
- **Lane guides:** 1px `#e5e9ef` horizontal lines across the plot at each lane's y.
- **Append lane:** blue `#2a78d6` 6px dot at t=0 labeled 11px "reads @510"; green `#008300` 7px diamond at t=45 with bold 12px green label "commit → 511" above it.
- **Compaction lane:** blue 6px dot at t=0 labeled 11px "reads @510"; red `#e74c3c` bold 14px "✗" at t=50 with 12px red label "conflict: pointer moved" below-right; green 7px diamond at t=75 with bold 12px green label "retry → 512" above it.
- **Pointer lane:** stepped 3px `#4a3aa7` line: value "510" from t=0 to t=45, step up 12px at t=45 to "511" until t=75, step to "512" through t=90; 12px violet labels "510", "511", "512" on each segment.
- **Swap markers:** vertical dashed `#6b7280` (dash 4/3) lines at t=45 and t=75 spanning all three lanes.
- **Annotation (bold 13px violet `#4a3aa7`, near t=62, y=70):** "one winner per commit — the loser retries, never corrupts".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Time Travel After a Bad Load

**Tags:** `where it's used` (blue), `snapshots` (green), `lakehouse` (orange)

- **The bad load** — the 3:00am backfill runs twice, and snapshot 513 doubles the table to 2.48M rows
- **Time travel** — `SELECT ... FOR VERSION AS OF 512` still returns yesterday's clean 1.24M rows
- **The rollback** — repointing the table at snapshot 512 undoes the bad load in one metadata write
- **Schema evolution** — columns tracked by ID can be added, renamed or dropped with no file rewrite
- **Partition evolution** — Iceberg can switch day- to hour-partitioning; old files stay untouched
- **The lakehouse** — warehouse guarantees (ACID, versions, schema) on cheap, open lake storage

*Example (italic):* At 9:00am the on-call sees dashboards doubled, confirms with a query AS OF 512, and rolls back — no restore from backup, no rewriting 2.48M rows.

**Key point:** Because every snapshot is just a list of files, yesterday's table still exists after a bad load — recovery is repointing, not reloading.

### Visualization (canvas `c3`, 720×300)

Bar chart of row counts per snapshot: the doubled bad load stands out in red, with an arrow showing the time-travel query landing on the last clean snapshot.

- **Title (bold 15px, `#1a5276`, top center):** "Rows per Snapshot: the 3am Backfill Ran Twice".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = rows 0 to 2.5M, gridlines `#e5e9ef` at 0.5M/1.0M/1.5M/2.0M with 12px `#444` labels; x tick labels 12px `#444` under each bar: "snap 510", "snap 511", "snap 512", "snap 513".
- **Bars (90px wide, centered at x = 150, 290, 430, 570), heights from row counts `[1.20, 1.24, 1.24, 2.48]` millions:** snapshots 510–512 fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border; snapshot 513 fill `rgba(231,76,60,0.18)` with 2px `#e74c3c` border.
- **Value labels (bold 12px, bar color, above each bar):** "1.20M", "1.24M", "1.24M", "2.48M".
- **Clean-snapshot highlight:** 2.5px `#008300` outline around the snap-512 bar; bold 12px green label "AS OF 512 → 1.24M" at y=60 near x=380 with a 2px green arrow down to the 512 bar top.
- **Bad-load label (bold 12px red `#e74c3c`, above the 513 bar at y=30):** "backfill ran twice".
- **Rollback marker:** dashed 2px `#008300` (dash 5/4) arrow curving from the 513 bar top back to the 512 bar top, 11px green label "rollback" at its midpoint.
- **Caption (12px `#444`, bottom right):** "row counts illustrative".

## A Table Format Is Not a File Format

**Tags:** `common mistake` (red), `layers` (orange)

- **The confusion** — "we store Parquet" names a file format; Parquet says nothing about commits or versions
- **File format** — Parquet defines the bytes inside one file: columns, compression, encodings
- **Table format** — Iceberg, Delta and Hudi define which files form the table and how versions change
- **Not an engine** — the format is a spec plus libraries; Spark, Trino, Flink and Snowflake all read it
- **The consensus** — Iceberg emerged as the vendor-neutral pick, supported across major warehouses

*Example (italic):* A team "on Parquet" still corrupted its table under concurrent writers — the missing piece was the layer above the files, not the files themselves.

**Common mistake:** Treating "Parquet" as the table. Parquet is one file's internal layout; a table needs the commit-and-snapshot layer that Iceberg, Delta and Hudi provide on top.

### Visualization (canvas `c4`, 720×300)

Layer-stack diagram of the lakehouse: query engines on top, table format, file format, object store — with the table format highlighted as the layer this page is about.

- **Title (bold 15px, `#1a5276`, top center):** "Four Layers of a Lakehouse Table".
- **Stack (four rounded boxes, 440px wide, 40px tall, 8px radius, left edge x=140, centered text 12px `#2c3e50`), top to bottom at y = 55, 110, 165, 220:**
  - y=55: violet `#4a3aa7` border, fill `rgba(74,58,167,0.10)`, text "query engines — Spark, Trino, Flink, Snowflake"
  - y=110: green `#008300` 2.5px border, fill `rgba(0,131,0,0.12)`, bold text "table format — Iceberg / Delta / Hudi: commits, snapshots, schema"
  - y=165: blue `#2a78d6` border, fill `rgba(42,120,214,0.15)`, text "file format — Parquet: bytes of one file, no notion of a table"
  - y=220: mute `#6b7280` border, fill `rgba(107,114,128,0.10)`, text "object store — S3 / GCS / ADLS: no transactions, no safe rename"
- **Connectors:** short 2px `#6b7280` vertical arrows between adjacent boxes, centered at x=360.
- **Mistake marker (right side):** bold 13px red `#e74c3c` label "\"we use Parquet\" names only this layer" at x=595 vertically centered on the y=165 box, with a 2px red arrow pointing left to that box.
- **Highlight label (bold 13px green `#008300`, left of the y=110 box at x=20):** "the missing database layer".
- **Caption (12px `#444`, bottom right):** "layer diagram schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); file counts (1,200 / 300), row counts (1.20M / 1.24M / 2.48M and the `[1.20, 1.24, 1.24, 2.48]` bar array), snapshot IDs (510–513) and commit timings (t=45s / t=50s / t=75s) are invented and labeled illustrative; project origins (Iceberg — Netflix, Delta Lake — Databricks, Hudi — Uber) and the layer roles (Parquet = file format, Iceberg/Delta/Hudi = table format) are documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
