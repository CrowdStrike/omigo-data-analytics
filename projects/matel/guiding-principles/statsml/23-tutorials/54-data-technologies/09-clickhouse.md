# ClickHouse

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** ClickHouse

**Subtitle:** ClickHouse stores each column in its own file and scans them in CPU-sized blocks — so counting a billion pageviews by day takes under a second, not seven minutes

## Reading One Column Instead of the Whole Row

**Tags:** `core idea` (blue), `columnar storage` (green), `OLAP` (orange)

- **The table** — a pageviews table: 1 billion rows, 20 columns (timestamp, url, user, device, ...)
- **The question** — a dashboard asks "how many pageviews per day?" — it needs only the timestamp
- **Row store** — rows live together on disk, so counting by day drags all 20 columns past the CPU
- **Column store** — ClickHouse keeps each column in its own file; this query opens exactly one
- **The origin** — built at Yandex to power Metrica's web analytics over tables just like this one

*Example (italic):* The daily-count query touches the 4 GB timestamp column instead of the 200 GB table — 98% of the bytes never leave disk.

**Key point:** Columnar storage means a query pays only for the columns it uses — and analytics queries touch a few columns of a wide table, so the saving is enormous.

### Visualization (canvas `c1`, 720×300)

Two-panel layout diagram: the same table stored row-wise (every cell shaded — all read) vs column-wise (one column strip shaded — only it read), under the query text.

- **Title (bold 15px, `#1a5276`, top center):** "Same Table, Two Layouts: What One GROUP BY Must Read".
- **Query line (12px `#6b7280`, centered at y=52):** "SELECT day, count() FROM pageviews GROUP BY day".
- **Left panel ("row store", bold 13px `#1a5276` label at x=70, y=80):** 5 stacked row boxes (x=60, width 270, each 28px tall, 6px gap, starting y=95), each divided into 6 cells by 1px `#fff` lines; all cells filled `rgba(231,76,60,0.20)` with 1px `#e74c3c` borders; 12px `#e74c3c` caption below at y=280: "reads all 20 columns — 200 GB".
- **Right panel ("column store", label at x=400, y=80):** 6 vertical column strips (x=390, each 42 wide × 160 tall, 8px gap, y=95); strip 1 labeled "ts" (11px, top) filled `rgba(0,131,0,0.30)` with 2px `#008300` border; strips 2–6 filled `#f4f5f7` with 1px `#c8cdd4` borders, 11px `#6b7280` labels "url", "user", "dev", "ref", "..."; 12px `#008300` caption at y=280: "reads 1 column — 4 GB".
- **Annotation (bold 13px violet `#4a3aa7`, centered at x=360, y=68, between query line and panels):** "98% of the bytes never leave disk".
- **Caption (12px `#444`, bottom right):** "table sizes illustrative".

## Counting a Billion Rows by Hand

**Tags:** `worked example` (blue), `compression` (green), `vectorized` (orange)

- **The row math** — 1 billion rows × ~200 bytes each ≈ 200 GB a row store must scan for the count
- **The column math** — the timestamp column alone is 4 bytes × 1 billion rows = 4 GB (exact)
- **Compression** — sorted timestamps delta-encode brutally well: 4 GB shrinks ~40× to ~0.1 GB
- **The disk** — at an illustrative 500 MB/s, 200 GB takes ~400 s; 0.1 GB takes ~0.2 s
- **Vectorized** — the engine aggregates in CPU-friendly blocks; 8 cores count the values in ~0.2 s

*Example (italic):* The same GROUP BY answers in ~0.4 s on ClickHouse and ~400 s — nearly seven minutes — on the full row scan, a 1,000× gap from layout and compression alone.

**Key point:** Sub-second over a billion rows is not magic hardware — it is reading ~2,000× fewer bytes and crunching them in vectorized blocks instead of row by row.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of bytes read off disk for the one daily-count query: full row scan vs one raw column vs that column compressed.

- **Title (bold 15px, `#1a5276`, top center):** "Bytes Off Disk for One GROUP BY over 1 Billion Rows".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; 12px `#444` left-aligned row labels at x=20; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (bar centers at y = 85, 150, 215, bars 26px tall, bold 12px value labels at bar ends):**
  - "row store: all 20 columns": red `#e74c3c` fill `rgba(231,76,60,0.25)`, 2px `#e74c3c` border, width 440, label "200 GB → ~400 s" in `#e74c3c`
  - "columnar: timestamp only": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 150, label "4 GB (exact: 4 B × 1B rows)" in `#2a78d6`
  - "compressed on disk (delta + LZ4)": green `#008300` fill `rgba(0,131,0,0.30)`, width 30, label "~0.1 GB → ~0.4 s total" in `#008300`
- **Annotation (bold 13px violet `#4a3aa7`, near x=300, y=250):** "~2,000× fewer bytes turns 7 minutes into 0.4 s".
- **Caption (12px `#444`, bottom right):** "4 GB column size exact; other figures illustrative; bar widths schematic".

## MergeTree: Sorted Parts and an Index That Skips

**Tags:** `where it's used` (blue), `MergeTree` (green), `sparse index` (orange)

- **MergeTree** — the workhorse table engine: every insert batch writes a new sorted, immutable part
- **Background merges** — small parts continuously merge into bigger ones, hence the engine's name
- **Sparse index** — one entry per 8,192-row granule (the default): 1B rows need only ~122k entries
- **Skipping** — a WHERE on the last 7 days reads ~19M rows of granules and skips the other ~981M
- **Where it lives** — logs, telemetry, clickstreams, dashboards: append-heavy, aggregate-hungry data

*Example (italic):* Filtering the year-long table to the last 7 days touches 7/365 of the sorted data — about 19.2M of the 1 billion rows, under 2% of the granules.

**Key point:** MergeTree keeps rows sorted by the primary key, so range filters skip whole granules via a tiny in-memory index — that is where sub-second dashboards on modest hardware come from.

### Visualization (canvas `c3`, 720×300)

Strip diagram of the billion-row table as one sorted run of granules: index tick marks above, a thin green slice read at the right end, the gray remainder skipped.

- **Title (bold 15px, `#1a5276`, top center):** "Sparse Index: Read 7 Days, Skip the Other 358".
- **Strip:** rounded rect x=60, y=140, width 600, height 55, fill `#f4f5f7`, 2px `#c8cdd4` border, 6px radius — 1 billion rows sorted by date, "Jan 1" (12px `#444`) below the left edge, "Dec 31" below the right edge.
- **Read slice:** rightmost segment x=648 to x=660 (12px wide, 7/365 of 600) filled `rgba(0,131,0,0.45)` with 2px `#008300` border; bold 12px `#008300` label "read: ~19.2M rows" at (x=530, y=120) with a 2px `#008300` arrow to the slice.
- **Skipped label (bold 12px `#6b7280`, centered in the strip at x=330, y=172):** "skipped: ~981M rows — never touched".
- **Index ticks:** 21 short 2px `#2a78d6` vertical ticks along the strip top (every 30px from x=60 to x=660), 12px `#2a78d6` label "one index entry per 8,192 rows — ~122k total" centered at y=105.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=250):** "the whole index fits in memory; the data mostly stays on disk".
- **Caption (12px `#444`, bottom right):** "8,192-row granule is the documented default; 1B rows illustrative, skip arithmetic exact".

## A Speed Demon That Hates Edits

**Tags:** `common mistake` (red), `not OLTP` (orange)

- **The trap** — teams see the benchmark and try to run their main application database on it
- **Updates** — there is no cheap UPDATE: a mutation rewrites every part holding the row, in background
- **Deletes** — classic mutations rewrite parts; newer lightweight deletes only mark rows — neither is an OLTP op
- **Joins** — large joins hash the right-hand table into memory; OLTP-style many-way joins strain it
- **The fit** — append event streams and aggregate them; keep accounts and orders in Postgres

*Example (italic):* Fixing one user's mislabeled pageview rewrites the ~150M-row part that contains it — roughly 90 s of background work for a one-row edit (illustrative).

**Common mistake:** Judging ClickHouse as a general database. It is an OLAP engine: bulk appends and aggregations are its game; point updates, deletes, and transactional joins are the price.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of illustrative latencies for four operations on the billion-row table: the two it is built for (green) vs the two it is not (orange, red).

- **Title (bold 15px, `#1a5276`, top center):** "What It's Built For vs What It Tolerates".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 440; 12px `#444` left-aligned row labels at x=20; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (bar centers at y = 70, 120, 170, 220, bars 22px tall, bold 12px value labels at bar ends):**
  - "GROUP BY day over 1B rows": green `#008300` fill `rgba(0,131,0,0.30)`, width 40, label "0.4 s" in `#008300`
  - "batch INSERT of 1M rows": green fill `rgba(0,131,0,0.30)`, width 60, label "0.9 s" in `#008300`
  - "point lookup by user_id": orange `#d95926` fill `rgba(217,89,38,0.25)`, width 180, label "3 s — scans a whole column" in `#d95926`
  - "UPDATE one row (mutation)": red `#e74c3c` fill `rgba(231,76,60,0.25)`, 2px `#e74c3c` border, width 440, bold 12px red label "90 s — rewrites the part"
- **Annotation (bold 13px magenta `#d55181`, near x=300, y=260):** "appends and aggregations are the game; edits are not".
- **Caption (12px `#444`, bottom right):** "all timings illustrative; the ordering is the point".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness). Documented ClickHouse facts: born at Yandex for Metrica web analytics, columnar storage with per-column files, aggressive compression, vectorized execution, MergeTree engine with sorted immutable parts and background merges, sparse primary index with an 8,192-row default granule, mutations for UPDATE/DELETE. Invented and labeled illustrative: the 1B-row / 20-column / 200 GB table, 500 MB/s disk, 40× compression, all timings (0.4 / 0.9 / 3 / 90 / 400 s), the 150M-row part. Exact arithmetic given the stated inputs: 4 B × 1B rows = 4 GB, 1B / 8,192 ≈ 122k index entries, 7/365 of 1B ≈ 19.2M rows.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
