# Avro & ORC

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Avro & ORC

**Subtitle:** Avro stores events row by row for streams, ORC stores them column by column for Hive — and Parquet won the lake by being welcome everywhere

## One Click Event, Two Ways to Lay It on Disk

**Tags:** `core idea` (blue), `row vs column` (green), `file formats` (orange)

- **The event** — a shop logs every click as (user, page, ms): u2 hits /cart and it takes 340 ms
- **Avro (row)** — writes each event whole, one after another, with its schema stored as JSON up front
- **ORC (column)** — groups all users together, all pages together, all ms together, like Parquet
- **The fit** — streams append and read one event at a time; analytics scan one field over millions
- **Same data** — the four events below carry identical values, just arranged for different readers

*Example (italic):* The Kafka topic carrying the click and the lake table the analysts query hold the same four events — arranged in opposite directions.

**Key point:** Avro is row-oriented (whole records, schema travels as JSON); ORC and Parquet are columnar (one field scans cheaply) — the layout, not the data, is the difference.

### Visualization (canvas `c1`, 720×300)

Side-by-side layout diagram: the same four click events stored row-wise (Avro) on the left and column-wise (ORC/Parquet) on the right.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Four Events: Row Layout (Avro) vs Column Layout (ORC/Parquet)".
- **Data (hardcoded):** events `[["u1","/home",120], ["u2","/cart",340], ["u1","/pay",95], ["u3","/home",210]]`.
- **Divider:** vertical dashed `#6b7280` (dash 4/3) line at x=365 from y=55 to y=270.
- **Left panel header (bold 13px `#1a5276` at x=40, y=62):** "Avro — row by row". Four record rows at y = 78, 122, 166, 210; each row has three cells at x = 40 (width 96), x = 140 (width 96), x = 240 (width 70), 30px tall, 4px radius.
- **Right panel header (bold 13px `#1a5276` at x=400, y=62):** "ORC / Parquet — column by column". Three columns at x = 400, 500, 600, width 84, with 11px `#6b7280` head labels "user" / "page" / "ms" above; each column stacks four cells at y = 78, 108, 138, 168, 26px tall.
- **Cell colors by field (both panels):** user fill `rgba(42,120,214,0.25)` border `#2a78d6`; page fill `rgba(0,131,0,0.20)` border `#008300`; ms fill `rgba(217,89,38,0.20)` border `#d95926`; values in 12px `#2c3e50`, centered.
- **Annotations:** bold 12px blue `#2a78d6` at (40, 262) "one read = one whole event"; bold 12px violet `#4a3aa7` at (400, 225) "one read = one column, not every record".
- **Caption (12px `#444`, bottom right):** "events illustrative".

## Reading the Same Million Events Two Ways

**Tags:** `worked example` (blue), `bytes read` (green)

- **The file** — 1M click events, ~100 bytes each: a 100 MB day of traffic (illustrative)
- **Point read** — given event #4,217's offset, row layout hands you the whole record in ~100 bytes
- **Same read in Parquet** — touches every column chunk of one row group: ~6 MB for one event
- **Column scan** — "average ms for the day": Parquet reads only the ms column, ~4 MB compressed
- **Same scan in Avro** — decodes all 1M whole records: the full 100 MB, 25× more than Parquet

*Example (italic):* One question flips the winner — the point read costs Avro 100 bytes vs Parquet 6 MB; the column scan costs Parquet 4 MB vs Avro 100 MB.

**Key point:** Neither layout is faster in general: row wins record-at-a-time, column wins field-at-a-time. The byte counts are illustrative, but the direction of the asymmetry is exact.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart with two question groups: bytes read to fetch one whole event vs bytes read to scan one column, Avro against Parquet in each group.

- **Title (bold 15px, `#1a5276`, top center):** "Two Questions, Two Winners: Bytes Read from a 100 MB Day (illustrative)".
- **Axis:** vertical 2px `#999` baseline at x=210, bars extend right, max width 460; scale spans 100 B to 100 MB, so log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Group labels (bold 12px `#1a5276` at x=20):** "fetch one whole event" at y=62; "scan one column (avg ms)" at y=157.
- **Rows (bars 16px tall, left-aligned 12px `#444` row labels at x=20):**
  - y=80 "Avro — 100 B": green `#008300` bar width 4, 11px green width label "100 B" at bar end
  - y=115 "Parquet — 6 MB": orange `#d95926` bar width 150, 11px label "6 MB"
  - y=175 "Parquet — 4 MB": green `#008300` bar width 130, 11px label "4 MB"
  - y=210 "Avro — 100 MB": red `#e74c3c` bar width 460, 11px red label "100 MB — reads every record"
- **Annotation (bold 13px magenta `#d55181`, centered near y=262):** "the winner flips with the question — pick the layout per workload".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic, byte counts illustrative".

## Three Formats, Three Home Turfs — and Why Parquet Won

**Tags:** `where it's used` (blue), `ecosystem` (green), `Kafka & Hive` (orange)

- **Avro's home** — Kafka: the schema registry made Avro the standard for streamed records
- **Schema evolution** — Avro treats add-a-field and default-value rules as a first-class contract
- **ORC's home** — built for Hive at Hortonworks: ACID tables and excellent compression
- **Parquet's edge** — Spark writes it by default, pandas reads it built-in, every engine speaks it
- **The verdict** — ORC often compresses Hive tables better; Parquet won on breadth, not bytes

*Example (italic):* A team moving off Hive keeps its lake in Parquet because Spark, Trino, DuckDB, and pandas all read it without an extra library.

**Key point:** All three are solid formats. Parquet became the lake default because it is at least "supported" in every lake tool, while Avro owns the stream and ORC stayed strongest inside the Hive world.

### Visualization (canvas `c3`, 720×300)

Support matrix: five tool rows against three format columns, cells marked first-class / supported / poor fit.

- **Title (bold 15px, `#1a5276`, top center):** "Why Parquet Won: At Home Somewhere, Welcome Everywhere".
- **Column headers (bold 13px `#1a5276`, centered at x = 408, 518, 628, y=60):** "Avro", "ORC", "Parquet".
- **Rows (left-aligned 12px `#444` labels at x=20, cell rounded rects 96×26 with 4px radius centered on x = 408, 518, 628):** at y = 78, 116, 154, 192, 230:
  - "Kafka + schema registry": Avro green "standard", ORC red "—", Parquet red "—"
  - "Hive tables + ACID": Avro gray "tables only", ORC green "native + ACID", Parquet gray "tables only"
  - "Spark jobs": Avro gray "supported", ORC gray "supported", Parquet green "default write"
  - "pandas / Arrow": Avro red "extra lib", ORC gray "supported", Parquet green "built-in"
  - "Trino / DuckDB / engines": Avro gray "supported", ORC gray "supported", Parquet green "first-class"
- **Cell styles:** green fill `rgba(0,131,0,0.15)` border `#008300` bold 11px `#008300` text; gray fill `rgba(107,114,128,0.10)` border `#9aa2ad` 11px `#6b7280` text; red fill `rgba(231,76,60,0.10)` border `#e74c3c` 11px `#e74c3c` text.
- **Annotation (bold 13px green `#008300`, centered near y=278):** "Avro owns the stream; below it, Parquet is never worse than supported — breadth won the lake".
- **Caption (11px `#444`, bottom right, y=295):** "matrix schematic".

## Not One Format for the Whole Pipeline

**Tags:** `common mistake` (red), `stream vs lake` (orange)

- **The mistake** — forcing one format end to end because "we standardized on Parquet"
- **Streamed Parquet** — one file per event means 86,400 tiny files a day and no record append
- **Avro analytics** — a lake kept in Avro makes every dashboard decode whole records, not columns
- **The pattern** — Avro on the Kafka topic, a compaction job, Parquet (or ORC) in the lake
- **The bridge** — the schema registry keeps the stream schema and the lake schema evolving in step

*Example (italic):* The shop streams each click as an Avro message, then an hourly job rewrites the hour as one Parquet file for the analysts.

**Common mistake:** Treating format choice as a one-time standard. Row and column layouts answer different questions — healthy pipelines convert at the stream/lake boundary instead of forcing one format everywhere.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a single-format pipeline that drowns in tiny files vs a fit-for-purpose pipeline that converts Avro to Parquet at the boundary.

- **Title (bold 15px, `#1a5276`, top center):** "One Pipeline, Two Formats: Convert at the Stream/Lake Boundary".
- **Row 1 (boxes centered on y=100), label 12px `#444` at x=20:** "one format forced"; blue `#2a78d6` rounded box at x=140 labeled "click event" (12px), 3px arrow to a red `#e74c3c` box at x=330 labeled "1 Parquet file per event", 3px arrow to a red box at x=545 labeled "86,400 tiny files/day" with bold 12px red "✗ scans crawl" beneath it.
- **Row 2 (boxes centered on y=205), label:** "fit for purpose"; blue box at x=115 "click event", arrow to green `#008300` box at x=265 "Avro on Kafka", arrow to violet `#4a3aa7` box at x=430 "hourly compaction", arrow to green box at x=595 "1 Parquet file/hour" with bold 12px green "✓" beside it.
- **Box style:** 120–150px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)` / `rgba(74,58,167,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "rows for the stream, columns for the lake — convert once, in the middle".
- **Caption (11px `#444`, bottom right, y=293):** "file counts illustrative (one event per second per source)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and pixel widths above (no randomness); the four click events, the 100 MB / 100 B / 6 MB / 4 MB byte counts, and the 86,400 files/day are invented and labeled illustrative; the row-vs-column asymmetry direction and the format facts (Avro row-oriented with JSON schemas, ORC built for Hive with ACID, Parquet the Spark default) are documented, exact facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
