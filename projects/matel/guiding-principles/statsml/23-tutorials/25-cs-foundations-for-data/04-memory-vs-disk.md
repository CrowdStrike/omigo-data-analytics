# Memory vs Disk

**Page type:** detail page (tutorial layout: h1 + subtitle, then four `.card-section` blocks each with an h2 and a `table.layout` — text column 50% left with tag pills / bullets / example / key-point, viz column 50% right with one 720×300 canvas)
**HTML title tag:** Memory vs Disk

**Subtitle:** RAM answers in 100 nanoseconds, an SSD in 100 microseconds — scale that to human time and it's 1 second vs 17 minutes, which is why "does it fit in RAM?" decides everything

## Two Places Your Data Can Live

Tags: `core idea` (blue), `running example` (green)

- **The setup** — your program needs one customer record; where is it sitting?
- **In RAM** — fetched in ~100 nanoseconds (0.0000001 s)
- **On SSD** — a random read takes ~100 microseconds: 1,000× slower
- **Human scale** — stretch 100 ns into 1 second; the SSD read becomes ~17 minutes
- **The picture** — RAM is the desk in front of you; disk is the archive down the hall

*Example:* Imagine answering a question instantly vs walking to the archive for 17 minutes — per question.

**Key point:** RAM and disk aren't "fast and a bit slower" — they are three orders of magnitude apart, and code inherits that gap.

### Visualization (canvas `c1`, 720×300)

Two horizontal bars comparing one random read at human scale.

- **Title (bold 15px, `#1a5276`, centered):** "One Random Read, Stretched to Human Time (100 ns → 1 s)".
- **Rows (bars start at x=170, track width 440px, 26px tall, alpha 0.75, rows 80px apart from y=80; bold 13px `#333` left labels, bold 13px colored labels at bar ends):**
  - "RAM: 100 ns" — bar fraction 1/1000 of track (minimum 4px), blue `#2a78d6` — end label "human scale: 1 second".
  - "SSD: 100 µs" — full-width bar, orange `#d95926` — end label "human scale: ~17 minutes".
- **Annotation (bold orange 14px, centered, y=250):** "1,000× slower — the desk vs the archive down the hall".
- **Caption (gray `#6b7280` 12px, centered, y=278):** "ballpark figures for a random read; sequential reads are friendlier to disks".

## Fetching 10,000 Scattered Rows, Both Ways

Tags: `worked example` (green)

- **The task** — look up 10,000 customer records scattered across a big table
- **All in RAM** — 10,000 × 100 ns = 1 millisecond total
- **Each from SSD** — 10,000 × 100 µs = 1 full second total
- **Same 1,000×** — the per-read gap survives multiplication untouched
- **Scale it up** — 10 million scattered reads: 1 s from RAM, ~17 minutes from SSD

*Example:* A loop that does one disk read per row is the walking-to-the-archive loop.

**Key point:** Multiply the count by the cost of where the data lives — that one multiplication predicts most "why is this slow?" mysteries.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart (log-scale heights): RAM vs SSD total time at two lookup counts.

- **Title (bold 15px, `#1a5276`, centered):** "Total Time for Scattered Lookups: RAM vs SSD (log scale)".
- **Scale:** bar height maps log10(seconds) over range −3.5..3.5 to 150px (minimum 6px); baseline y=225 with thin `#999` line from x=60 to x=660; bars 70px wide, pair gap 20px, alpha 0.75; bold 13px value labels in bar color above bars; bold 12px `#333` group labels below.
- **Groups:**
  - "10,000 lookups" at x=140 — RAM 0.001 s (label "1 ms", blue `#2a78d6`); SSD 1 s (label "1 s", orange `#d95926`).
  - "10,000,000 lookups" at x=430 — RAM 1 s (label "1 s"); SSD 1,000 s (label "~17 min").
- **Legend (top-right, 12px swatches):** blue "from RAM"; orange "from SSD".
- **Bottom line (bold magenta `#d55181` 13px, centered, y=285):** "count × cost-of-where-it-lives: the 1,000× gap never averages away".

## Why Pandas Dies and Databases Don't

Tags: `where it's used` (blue), `common mistake` (red)

- **Pandas' deal** — load everything into RAM, then be blazingly fast on it
- **The overhead** — a 6 GB CSV often needs ~12 GB as a dataframe (types, indexes, copies)
- **The wall** — on an 8 GB laptop that load doesn't slow down — it dies: `MemoryError`
- **A database's deal** — keep data on disk, read only the pages a query needs, via indexes
- **The workarounds** — sample first, read in chunks, select columns, or push work to the DB

*Example:* The same query: pandas loads 6 GB to answer it; the database reads the 20 MB it needs.

**Common mistake:** "Pandas is faster than the database." Only while data fits in RAM — past the wall the comparison isn't slow, it's over.

### Visualization (canvas `c3`, 720×300)

Split panel: memory-needs bars vs a RAM ceiling on the left, the database's dodge steps on the right, separated by a vertical dashed light-gray divider (`#bdc3c7`, dash 4/3) at x=390.

- **Title (bold 15px, `#1a5276`, centered):** "The 8 GB Laptop Meets a 6 GB CSV".
- **Left panel (baseline y=240, height scale 170px over max 14 GB):**
  - Pandas bar (90px wide at x=100): 12 GB, fill `rgba(213,81,129,0.30)` with magenta `#d55181` 2px border; bold magenta 13px label "12 GB needed" above; `#333` 12px two-line caption below: "pandas dataframe" / "(6 GB CSV × ~2)"; bold 13px "MemoryError" above the bar (8px above the 13.4 GB level).
  - Database bar (90px wide at x=250): 0.4 GB tall (visual), fill `rgba(0,131,0,0.35)` with green `#008300` 2px border; bold green 13px label "~0.02 GB read" above; caption below: "database answering" / "the same query".
  - RAM ceiling: dashed red `#e74c3c` 2px horizontal line (dash 7/5) at the 8 GB level across the panel, labeled "8 GB RAM" in bold red 12px at its right end.
- **Right panel (from x=430):** heading "how the database dodges the wall:" in bold 13px `#444`, then four stacked 250×30 rounded-rectangle steps (tinted fill alpha 0.15, 2px colored border, bold 12px colored label, small gray connector ticks between them):
  1. "index finds the rows" — aqua `#199e70`.
  2. "reads only those disk pages" — blue `#2a78d6`.
  3. "streams results in batches" — violet `#4a3aa7`.
  4. "RAM used: a working set, not the table" — green `#008300`.
- **Bottom line (bold orange `#d95926` 13px, centered under the right panel, y=288):** "pandas: all or nothing — DB: only what the query needs".

## The Full Ladder: Cache → RAM → SSD → Network

Tags: `rule of thumb` (blue), `mental model` (orange)

- **CPU cache** — ~1 ns; human scale: a hundredth of a second
- **RAM** — ~100 ns; human scale: 1 second
- **SSD random read** — ~100 µs; human scale: ~17 minutes
- **Network round trip** — ~10 ms; human scale: ~28 hours
- **The rule** — each rung down costs roughly 100–1,000×; systems fight to stay high on the ladder

*Example:* Ballpark figures — exact numbers vary by hardware; the rung-to-rung ratios are the lesson.

**Key point:** Caching, indexing, batching, and "keep it in memory" are all the same move: climb the ladder, or make fewer trips down it.

### Visualization (canvas `c4`, 720×300)

Horizontal log-scale bar ladder of access latencies.

- **Title (bold 15px, `#1a5276`, centered):** "The Storage Ladder (log scale, one random access)".
- **Scale:** bars start at x=120, track width 330px mapping log10(ns) 0..7; rows 50px apart from y=54, bars 24px tall, alpha 0.75, minimum 5px; bold 12px `#333` left labels; bold 12px colored end labels formatted "<time>  →  <human scale>".
- **Rows:**
  - "CPU cache" — "~1 ns  →  a finger-snap (0.01 s)", log 0, aqua `#199e70`.
  - "RAM" — "~100 ns  →  1 second", log 2, blue `#2a78d6`.
  - "SSD" — "~100 µs  →  ~17 minutes", log 5, orange `#d95926`.
  - "network" — "~10 ms  →  ~28 hours", log 7, magenta `#d55181`.
- **Vertical gridlines (`#e5e9ef`) with gray tick labels below:** "1 ns" (0), "1 µs" (3), "1 ms" (6).
- **Bottom line (bold violet `#4a3aa7` 13px, centered, y=288):** "each rung down: roughly 100–1,000× — climb the ladder or make fewer trips".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` (full width, border-collapse) with `td.text-col` 50% and `td.viz-col` 50%, one canvas per section.
- **Text column structure:** `.tags` pill row (0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`), `<ul>` of one-line bullets with `<b>` lead terms in `#1a5276`, italic `.example` line (`#555`, 0.9rem), `.key-point` callout (background `#f8f9fa`, 3px `#e74c3c` left border, padding 8px 12px, 0.9rem). Inline `code`: ui-monospace, background `#f4f6f8`, padding 1px 4px, radius 3px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Page palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- No cross-page links; in regenerated HTML any card links elsewhere would use `.html` extensions.
