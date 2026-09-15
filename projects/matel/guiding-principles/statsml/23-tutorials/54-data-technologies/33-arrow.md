# Arrow

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Arrow

**Subtitle:** One in-memory columnar layout every tool agrees on — data moves between systems by sharing a pointer instead of copying and converting

## The Same Table, Copied at Every Hop

**Tags:** `core idea` (blue), `zero copy` (green), `Apache Arrow` (orange)

- **The pipeline** — a 2 GB orders table flows through four tools: Polars, pandas, DuckDB, an ML library
- **The tax** — without Arrow, each hop converts formats: Polars frames to pandas blocks to DuckDB vectors
- **The waste** — each conversion burns CPU serializing and deserializing the exact same values
- **The fix** — Arrow defines one columnar memory layout that all four tools understand natively
- **Zero copy** — a hop becomes a pointer handoff: the next tool reads the previous tool's memory

*Example (italic):* The 2 GB table crosses three hops on its way to the model; with Arrow, not a single byte is rewritten along the way.

**Key point:** Arrow is a standardized in-memory columnar format — once every tool lays columns out the same way, "transferring" data means sharing a buffer, not converting it.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the same four tools, top row copying and converting at every hop, bottom row all reading one shared Arrow buffer.

- **Title (bold 15px, `#1a5276`, top center):** "Four Tools, One 2 GB Table: Three Copies vs Zero".
- **Row 1 (boxes at y=70), label 12px `#444` at x=20, y=60:** "copy-and-convert"; four blue `#2a78d6` rounded boxes (100×36, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text) at x = 110, 260, 410, 560 labeled "Polars", "pandas", "DuckDB", "ML lib"; red `#e74c3c` 3px arrows between consecutive boxes, each with bold 11px red label "convert 2 GB" above the arrow.
- **Row 2 (boxes at y=175), label at x=20, y=165:** "with Arrow"; the same four boxes at the same x positions; one green `#008300` rounded box (220×34, fill `rgba(0,131,0,0.12)`) centered at x=250, y=240 labeled "one Arrow buffer (2 GB)"; grey `#6b7280` 2px lines from the bottom center of each tool box down to the buffer box.
- **Annotation (bold 12px green `#008300`, right side near x=490, y=250):** "0 bytes copied — all four read the same memory".
- **Caption (12px `#444`, bottom right):** "table size illustrative".

## Counting the Serialization Tax

**Tags:** `worked example` (blue), `memory traffic` (green)

- **The table** — 50M rows × 5 columns, 2 GB resident in RAM (illustrative)
- **One hop's cost** — copy-and-convert writes 2 GB out and reads 2 GB back in: 4 GB of memory traffic
- **Three hops** — 3 hops × 4 GB = 12 GB moved to relocate a table whose values never changed
- **Peak RAM** — mid-hop, the source and destination copies are both alive: 4 GB held for a 2 GB table
- **Hand-check** — 12 GB ÷ 2 GB means the pipeline rewrites the table 6 times end to end
- **The Arrow line** — every hop costs 0 bytes: all four tools point at the same buffer

*Example (italic):* Sending the 2 GB table through the pipeline shuffles 12 GB of memory the old way; with Arrow the running total stays at 0.

**Key point:** The serialization tax is hops × 2 × table size — with all four tools in one process sharing an Arrow buffer, no hop touches the data at all.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: memory traffic per hop, copy-and-convert (red, 4 GB each) vs Arrow (green, 0 GB each), with the 12 GB cumulative called out.

- **Title (bold 15px, `#1a5276`, top center):** "Memory Traffic per Hop: 4 GB Each vs 0 GB Each".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = GB moved 0 to 12 (15 px per GB), gridlines `#e5e9ef` at 3/6/9 with 12px `#444` tick labels; x = three hop groups centered at x=170, 360, 550 with 12px `#444` labels "Polars→pandas", "pandas→DuckDB", "DuckDB→ML lib" below the baseline.
- **Convert bars:** red `#e74c3c` bars, width 60, left bar of each group, heights 60px (= 4 GB) with 12px `#444` label "4 GB" on top of each.
- **Arrow bars:** green `#008300` bars, width 60, right bar of each group, drawn as a 3px stub on the baseline with 12px green label "0 GB" above.
- **Annotation (bold 13px red `#e74c3c`, near x=150, y=55):** "12 GB of traffic to move a 2 GB table".
- **Annotation (bold 12px green `#008300`, near x=390, y=95):** "Arrow: every hop is a pointer handoff".
- **Caption (12px `#444`, bottom right):** "illustrative — each hop writes 2 GB and reads 2 GB".

## The Layer Under Polars, DuckDB, and pandas 2.x

**Tags:** `where it's used` (blue), `ecosystem` (green)

- **Polars** — the DataFrame library's native memory model is Arrow; every column is an Arrow array
- **DuckDB** — queries pandas and Polars frames in place by scanning their Arrow buffers, no import step
- **pandas 2.x** — `dtype_backend="pyarrow"` stores columns as Arrow arrays instead of NumPy blocks
- **Spark** — `toPandas()` with the Arrow flag skips row-by-row pickling: 60 s drops to 3 s (illustrative)
- **Arrow Flight** — an RPC protocol that ships Arrow buffers over the network, replacing row-based ODBC
- **Memory maps** — an Arrow file on disk can be memory-mapped and read with zero parsing

*Example (italic):* A dashboard pulls 10M rows from a warehouse: 66 s over ODBC, 6 s over Arrow Flight — the rows were never re-encoded in flight (illustrative).

**Key point:** Arrow won by being the layer, not the tool — Polars, DuckDB, pandas, and Spark interoperate cheaply because they all agreed on the same bytes.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: two transfer scenarios, each shown with and without Arrow on a shared seconds scale.

- **Title (bold 15px, `#1a5276`, top center):** "Same Data, Same Wire — Skip the Re-encoding".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440 (scale: 440px = 66 s); left-aligned 12px `#444` row labels at x=20.
- **Rows (bar tops at y = 75, 115, 175, 215), bars 16px tall, 11px `#444` value labels at bar ends:**
  - "Spark→pandas, Arrow off — 60 s": red `#e74c3c` bar width 400
  - "Spark→pandas, Arrow on — 3 s": green `#008300` bar width 20, bold 11px green label "20× faster"
  - "10M-row fetch, ODBC — 66 s": red bar width 440
  - "10M-row fetch, Arrow Flight — 6 s": green bar width 40, bold 11px green label "11× faster"
- **Annotation (bold 13px magenta `#d55181`, near x=280, y=260):** "same trick both times: no serialize, no deserialize".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Arrow Is Not a File Format

**Tags:** `common mistake` (red), `Arrow vs Parquet` (orange)

- **The confusion** — Arrow and Parquet are both "columnar", so people treat them as interchangeable
- **Parquet** — an on-disk format: compressed and encoded, built to make files small
- **Arrow** — an in-memory format: uncompressed, fixed layout, built to be computed on directly
- **The cost** — every Parquet read pays a decompress-and-decode step; an Arrow buffer needs none
- **The pairing** — the standard pipeline is Parquet for storage, Arrow the moment data reaches RAM

*Example (italic):* Renaming `orders.parquet` to `orders.arrow` buys nothing — the compressed pages still have to be decoded into Arrow's layout before any tool can share them.

**Common mistake:** Expecting Parquet files to be zero-copy because they are columnar. Zero-copy is a property of Arrow's in-memory layout; Parquet deliberately trades it away for compression on disk.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: reading Parquet (decode step required) vs reading a memory-mapped Arrow file (pointer only).

- **Title (bold 15px, `#1a5276`, top center):** "Parquet Is for Storing, Arrow Is for Sharing".
- **Row 1 (boxes at y=85), label 12px `#444` at x=20:** "Parquet read"; blue `#2a78d6` rounded box (170×40, 8px radius, fill `rgba(42,120,214,0.15)`) at x=150 labeled "compressed pages on disk", 3px orange `#d95926` arrow labeled bold 12px orange "decompress + decode" to a blue box at x=470 labeled "in-memory table", with bold 12px orange "CPU work on every read" below the arrow.
- **Row 2 (boxes at y=195), label:** "Arrow read"; green `#008300` box (fill `rgba(0,131,0,0.12)`) at x=150 labeled "memory-mapped Arrow file", 3px green arrow labeled bold 12px green "pointer" to a green box at x=470 labeled "ready to compute", with bold 12px green "✓ no decode step" below the arrow.
- **Box style:** 150–180px wide, 40px tall, 8px radius, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "store as Parquet, share as Arrow — they solve different problems".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); the 2 GB table, per-hop 4 GB traffic, 12 GB total, and all timings (60 s/3 s, 66 s/6 s) are invented and labeled illustrative; the 12 GB = 3 hops × 4 GB arithmetic is exact given those inputs. Text numbers must match chart numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
