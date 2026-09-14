# CPU Caches & Locality

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** CPU Caches & Locality

**Subtitle:** The CPU keeps a tiny fast desk of recently used memory — read numbers in the order they are stored and eight arrive for the price of one

## One Coffee Order Arrives With Seven Neighbors

**Tags:** `core idea` (blue), `memory hierarchy` (green), `cache line` (orange)

- **The shop** — a coffee chain logs a year of order amounts as one long strip of numbers in memory
- **The desk** — the CPU keeps a tiny fast desk (the cache); the full strip lives in far-away RAM
- **The trip** — fetching one number from RAM takes ~100 ns; reading it off the desk takes ~1 ns
- **The tray** — RAM never sends one number; it sends a 64-byte cache line, eight amounts at once
- **The win** — ask for one amount and its seven strip-neighbors land on the desk free of charge

*Example (italic):* Summing eight neighboring amounts costs one 100 ns RAM trip plus seven 1 ns desk reads — not eight separate trips.

**Key point:** A cache is a small fast copy of recently used memory, filled a 64-byte line at a time — so touching data in the order it is laid out makes most reads nearly free. That habit is called locality.

### Visualization (canvas `c1`, 720×300)

Diagram: a memory strip of 16 amount cells with the first 8 bracketed as one cache line, above a desk-vs-warehouse latency sketch.

- **Title (bold 15px, `#1a5276`, top center):** "One RAM Trip Delivers a Whole Cache Line of Eight".
- **Memory strip:** 16 boxes 36px wide × 30px tall in a row starting at x=70, y=75; boxes 0–7 fill `rgba(42,120,214,0.25)` with 2px `#2a78d6` border, boxes 8–15 fill `rgba(107,114,128,0.08)` with 1px `#e5e9ef` border; 11px `#2c3e50` amount labels inside boxes 0–7: `[4.50, 3.25, 5.00, 2.75, 4.00, 6.50, 3.75, 5.25]`; box 2 gets a deeper fill `rgba(42,120,214,0.45)` and a 12px `#2c3e50` label below at y=125: "the one you asked for" with a short 2px `#2c3e50` arrow up to it.
- **Bracket:** thin 2px `#2a78d6` bracket spanning boxes 0–7 at y=62, bold 12px `#2a78d6` label above: "one cache line — 64 bytes = 8 amounts".
- **Lower sketch:** green `#008300` rounded box (150×42, fill `rgba(0,131,0,0.12)`) at x=140, y=195 labeled "cache (the desk)"; blue `#2a78d6` rounded box (170×42, fill `rgba(42,120,214,0.15)`) at x=420, y=195 labeled "RAM (the warehouse)"; double-headed 2px `#6b7280` arrow between them; bold 12px green "hit ≈ 1 ns" above the green box, bold 12px orange `#d95926` "miss ≈ 100 ns" above the arrow midpoint.
- **Annotation (bold 13px green `#008300`, right side near y=160):** "seven neighbors ride along free".
- **Caption (12px `#444`, bottom right):** "amounts illustrative; 1 ns / 100 ns are typical ballparks".

## Summing a Million Amounts, Two Loop Orders

**Tags:** `worked example` (blue), `row-major` (green), `hand-check` (orange)

- **The grid** — 1,000 shops × 1,000 days of daily totals, stored row-major: each shop's days sit side by side
- **Row order** — scan shop by shop: every 8 reads share a line, so 1,000,000 ÷ 8 = 125,000 RAM trips
- **Column order** — scan day by day across shops: each read lands 8,000 bytes away — idealized, 1,000,000 fresh trips
- **Hand-check** — at 100 ns a trip: 125,000 × 100 ns ≈ 12.5 ms versus 1,000,000 × 100 ns ≈ 100 ms
- **Same sum** — both loops add the same million numbers to the same total; only the visiting order changed

*Example (italic):* Swapping two nested loops turns a 100 ms sum into a 12.5 ms sum — an 8× speedup with zero algorithm change.

**Key point:** With a row-major layout, rows beat columns 8-to-1 because a row scan reuses each cache line fully and a column scan wastes seven-eighths of every tray.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: RAM trips (and time) for the row-order scan vs the column-order scan of the same 1,000,000-cell grid.

- **Title (bold 15px, `#1a5276`, top center):** "Same Million Numbers: 125,000 RAM Trips vs 1,000,000".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; no x gridlines (widths are proportional to trips, 1:8).
- **Rows (bar height 26px), each with a left-aligned 12px `#444` two-line label at x=20:**
  - y=95, "row order / (with the layout)": green `#008300` bar width 55, 12px `#2c3e50` label at bar end "125,000 trips ≈ 12.5 ms"
  - y=175, "column order / (against the layout)": orange `#d95926` bar width 440, 12px `#2c3e50` label just inside the bar end (white or above) "1,000,000 trips ≈ 100 ms"
- **Annotation (bold 13px ink `#1a5276`, centered near y=245):** "8× fewer trips, 8× faster — identical arithmetic".
- **Caption (12px `#444`, bottom right):** "100 ns per RAM trip, cache hits ignored — illustrative".

## Why Your DataFrame Cares About Layout

**Tags:** `where it's used` (blue), `columnar data` (green), `vectorization` (orange)

- **Column stores** — Parquet-style formats lay each column contiguously, so column scans get the full tray
- **The flip** — rows beat columns only because the layout was row-major; flip the layout and columns win
- **NumPy** — arrays scan fastest along the contiguous axis (C order: the last axis), same 8× logic
- **Row loops** — looping over a DataFrame row by row fights a columnar layout one strided read at a time
- **Pointer chasing** — linked lists and hash buckets scatter neighbors, so every hop is a fresh RAM trip

*Example (italic):* Averaging every column of the million-cell grid is a contiguous 12.5 ms scan in a columnar layout but a 100 ms strided crawl in a row-major one.

**Key point:** Locality is not "rows are magic" — the fast direction is whichever one matches the storage layout, and every fast data tool picks its layout to match its most common scan.

### Visualization (canvas `c3`, 720×300)

Grouped vertical bar chart: time to scan the whole grid by row vs by column, under a row-major layout and a columnar layout — the winner flips.

- **Title (bold 15px, `#1a5276`, top center):** "The Winner Is Whichever Direction Matches the Layout".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 175; y = scan time 0 to 100 ms, gridlines `#e5e9ef` at 25/50/75/100 with 12px `#444` labels; x = two groups centered at x=220 ("scan by row") and x=480 ("scan by column"), 12px `#444` labels below the baseline.
- **Bars (width 70, 12px gap inside a group), values in ms with 12px `#2c3e50` labels on top:**
  - scan by row: blue `#2a78d6` (row-major layout) `12.5`; aqua `#199e70` (columnar layout) `100`
  - scan by column: blue `100`; aqua `12.5`
- **Legend (12px, top right inside plot):** blue swatch "row-major layout", aqua swatch "columnar layout".
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=70):** "flip the layout and the fast direction flips too".
- **Caption (12px `#444`, bottom right):** "times from the worked example — illustrative".

## A Million Adds Is Not a Million Nanoseconds

**Tags:** `common mistake` (red), `big-O blind spot` (orange)

- **The trap** — both loops perform exactly 1,000,000 additions, so operation counts say they should tie
- **Big-O blind spot** — complexity counts operations, not the 100 ns memory stalls between them
- **The split** — the adds take ~1 ms in either loop; the slow loop then waits ~100 ms on RAM
- **Wrong fix** — micro-optimizing the arithmetic attacks the 1%, not the 99% spent waiting
- **Right fix** — change the access order (or the layout) so the tray of eight actually gets used

*Example (italic):* Profiled honestly, the column-order loop is ~1 ms of adding and ~100 ms of waiting — the CPU is idle 99% of the time.

**Common mistake:** Assuming equal work means equal time. On modern hardware, where you read usually costs more than what you compute — profile memory access patterns, not just instruction counts.

### Visualization (canvas `c4`, 720×300)

Stacked horizontal bar chart: each loop's runtime split into compute (adds) and memory wait — the compute sliver is identical and tiny.

- **Title (bold 15px, `#1a5276`, top center):** "Both Loops Do 1,000,000 Adds — the Wait Is the Difference".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440 (≈ 101 ms at ~4.36 px/ms); bar height 26px.
- **Rows, each with a left-aligned 12px `#444` label at x=20:**
  - y=95, "row order": green `#008300` compute segment width 4, then blue `#2a78d6` wait segment width 55; 12px `#2c3e50` end label "≈ 13.5 ms (1 adds + 12.5 wait)"
  - y=180, "column order": green compute segment width 4, then orange `#d95926` wait segment width 436; 12px `#2c3e50` label above the bar end "≈ 101 ms (1 adds + 100 wait)"
- **Legend (12px, below the title):** green swatch "compute (adds, ~1 ms)", gray-labeled note that the second segment is "memory wait".
- **Annotation (bold 13px magenta `#d55181`, centered near y=250):** "the adds are the thin green sliver — memory is the bill".
- **Caption (12px `#444`, bottom right):** "1 ns per add, 100 ns per RAM trip — illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the c1 amounts `[4.50, 3.25, 5.00, 2.75, 4.00, 6.50, 3.75, 5.25]` and all millisecond timings (12.5 / 100 / 13.5 / 101 / 1) are invented and labeled illustrative; the derived counts are exact arithmetic on the illustrative model (1,000 × 1,000 = 1,000,000 cells; 1,000,000 ÷ 8 = 125,000 line fetches; 125,000 × 100 ns = 12.5 ms; 1,000,000 × 100 ns = 100 ms); the 64-byte line, 8 doubles per line, and ~1 ns / ~100 ns latencies are standard ballpark hardware figures.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
