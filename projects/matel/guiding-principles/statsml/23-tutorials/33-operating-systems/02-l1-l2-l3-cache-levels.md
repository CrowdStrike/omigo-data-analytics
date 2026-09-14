# L1, L2, L3 — Cache Levels

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** L1, L2, L3 — Cache Levels

**Subtitle:** A CPU keeps copies of data on a ladder of ever-smaller, ever-faster shelves — L1, L2, L3 — so it rarely has to walk all the way to slow main memory

## The Barista's Four Shelves

**Tags:** `core idea` (blue), `latency ladder` (green), `memory hierarchy` (orange)

- **The barista** — a coffee shop barista fills orders all day and needs cups, lids, and beans constantly
- **The cup rack** — a tiny rack at arm's reach holds 8 cups; grabbing one takes about 1 second
- **The counter shelf** — a shelf behind her holds a few boxes; a turn-and-reach takes about 5 seconds
- **The back room** — a storeroom down the hall holds crates; a round trip takes about 20 seconds
- **The warehouse** — the supplier across town holds everything; a delivery run takes about 2 minutes
- **The definition** — L1, L2, L3 caches are the CPU's cup rack, counter shelf, and back room; RAM is the warehouse

*Example (italic):* When the cup rack runs out, the barista restocks it from the counter shelf — not from the warehouse — so the next 8 grabs are 1-second grabs again.

**Key point:** Each cache level trades size for speed: L1 is tiny but ~1 ns away, L3 is roomy but ~15 ns away, and RAM is huge but ~100 ns away — the CPU checks them in order, nearest first.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart of the latency ladder: four rows (L1, L2, L3, RAM), bar length = time to fetch, with the coffee-shop analogy label on each row.

- **Title (bold 15px, `#1a5276`, top center):** "The Latency Ladder: Each Step Down Is Several Times Slower".
- **Axis:** bars start at x=230, extend right, max width 440; 2px `#999` vertical baseline at x=230; bar width = 40 + 200·log10(latency in ns), i.e. a log scale.
- **Rows (top to bottom at y = 70, 118, 166, 214), each with a left-aligned 12px `#444` two-line label at x=20:**
  - "L1 — the cup rack": blue `#2a78d6` bar width 40, 12px `#2c3e50` label "1 ns" at bar end
  - "L2 — the counter shelf": aqua `#199e70` bar width 160, label "4 ns"
  - "L3 — the back room": yellow `#c98500` bar width 275, label "15 ns"
  - "RAM — the warehouse": orange `#d95926` bar width 440, label "100 ns"
- **Bar style:** 18px tall, fills at full color with `rgba` 0.85 alpha, 4px radius.
- **Size tags:** 11px `#6b7280` under each row label: "32 KB", "256 KB", "8 MB", "16 GB".
- **Annotation (bold 13px violet `#4a3aa7`, near x=300, y=250):** "100× between the top and bottom rungs".
- **Caption (12px `#444`, bottom right):** "latencies and sizes typical, illustrative; bar length on a log scale".

## Counting 100 Lookups by Hand

**Tags:** `worked example` (blue), `hit rates` (green)

- **The setup** — the CPU makes 100 data lookups; most land on a shelf that already has a copy
- **The split** — 90 hit L1, 6 miss to L2, 3 miss down to L3, and 1 goes all the way to RAM
- **The costs** — 1 ns per L1 hit, 4 ns per L2 hit, 15 ns per L3 hit, 100 ns for the RAM trip
- **Hand-check** — 90×1 + 6×4 + 3×15 + 1×100 = 90 + 24 + 45 + 100 = 259 ns for all 100 lookups
- **The average** — 259 ns ÷ 100 lookups = 2.59 ns each, barely above the pure-L1 time of 1 ns
- **No caches** — 100 lookups straight to RAM would take 100×100 = 10,000 ns, about 39× slower

*Example (italic):* The single RAM miss costs 100 ns — more time than all 90 L1 hits combined (90 ns) — yet the average stays low because misses are rare.

**Key point:** Average lookup time is a weighted sum of the ladder: with 90% of hits on the top rung, 100 lookups cost 259 ns instead of 10,000 ns — the hierarchy wins by making the slow trips rare.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart with two bars per level: lookup count (out of 100) and total time contributed (ns), showing the one RAM miss out-costing the 90 L1 hits.

- **Title (bold 15px, `#1a5276`, top center):** "100 Lookups: Where the Hits Land vs Where the Time Goes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = four level groups "L1", "L2", "L3", "RAM" centered at x = 135, 285, 435, 585 with 13px `#444` labels; y = 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Count bars (left in each pair, 44px wide):** blue `#2a78d6`, heights from `[90, 6, 3, 1]`, 12px `#2c3e50` value labels on top.
- **Time bars (right in each pair, 44px wide):** orange `#d95926`, heights from `[90, 24, 45, 100]` (ns), 12px `#2c3e50` value labels "90 ns", "24 ns", "45 ns", "100 ns" on top.
- **Legend (12px, top right):** blue swatch "lookups (of 100)", orange swatch "time (ns)".
- **Annotation (bold 13px magenta `#d55181`, near x=430, y=75):** "1 RAM miss costs more than 90 L1 hits".
- **Caption (12px `#444`, bottom right):** "hit split illustrative; totals sum to 259 ns".

## Why Your Loop Order Changes the Runtime

**Tags:** `where it's used` (blue), `data science` (green)

- **Cache lines** — memory moves in 64-byte chunks, so touching one number drags its 7 neighbors up too
- **Row-major** — a 4,000×4,000 array stores each row contiguously; row-by-row loops ride free neighbors
- **Column order** — column-by-column jumps 4,000 numbers each step, missing the cache on nearly every read
- **The gap** — summing the same array takes 20 ms row-wise vs 160 ms column-wise, an 8× difference
- **Vectorized wins** — NumPy and pandas columnar ops are fast partly because they stream cache-friendly
- **Same math** — both loops do 16 million additions; only the memory order differs

*Example (italic):* A data scientist's feature loop runs 8× faster after swapping two `for` lines — the arithmetic is identical, the cache traffic is not.

**Key point:** Caches reward touching memory in order: when your access pattern matches the storage layout, most reads are 1-ns L1 hits instead of 100-ns RAM trips.

### Visualization (canvas `c3`, 720×300)

Two-part chart: left, two vertical bars comparing row-wise vs column-wise sum time; right, a small grid schematic showing the two traversal paths over the same array.

- **Title (bold 15px, `#1a5276`, top center):** "Same 16M Additions, 8× Apart: Loop Order on a 4,000×4,000 Array".
- **Left panel (x 60–330):** baseline y=245, y-axis 0 to 160 ms with gridlines `#e5e9ef` at 40/80/120; green `#008300` bar at x=110 (width 70) height for 20 ms, orange `#d95926` bar at x=230 (width 70) height for 160 ms; 12px `#2c3e50` labels "row-wise 20 ms" and "column-wise 160 ms" above each bar.
- **Right panel (x 400–680):** two 5×5 grids of 22px cells stroked `#e5e9ef`, top grid at y=70 with a green `#008300` 2px arrow snaking left-to-right along rows labeled 12px green "walks the shelf", bottom grid at y=180 with an orange `#d95926` 2px arrow running top-to-bottom columns labeled 12px orange "jumps shelves every step".
- **Annotation (bold 13px violet `#4a3aa7`, near x=180, y=70):** "8× faster from loop order alone".
- **Caption (12px `#444`, bottom right):** "timings illustrative; 8× ratio typical for large arrays".

## The Benchmark That Fit in Cache

**Tags:** `common mistake` (red), `working set` (orange)

- **The mistake** — timing code on a small sample, then assuming production data runs at the same speed
- **The cliff** — throughput steps down each time the working set outgrows a cache level, not gradually
- **The test** — a 1 MB sample fits in the 8 MB L3, so every read is a cheap 15-ns-or-better hit
- **Production** — the real 128 MB table fits nowhere, so reads fall to RAM and throughput drops ~4.4×
- **Bigger ≠ faster** — L1 is small *because* it is fast; a huge L1 would be far away and slow
- **The check** — benchmark with data at least as large as production, or expect a surprise cliff

*Example (italic):* The notebook clocked 400M reads/sec on a 1 MB sample; the nightly job on 128 MB crawled at 90M — same code, working set past the last cache.

**Common mistake:** Believing performance scales smoothly with data size. It steps down a cliff at each cache boundary — a benchmark that fits in L3 tells you nothing about data that doesn't.

### Visualization (canvas `c4`, 720×300)

Step line chart: read throughput (millions of reads/sec) vs working-set size, with cliffs at the L1, L2, and L3 capacity boundaries.

- **Title (bold 15px, `#1a5276`, top center):** "Throughput Falls Off a Cliff at Each Cache Boundary".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = eight evenly spaced size ticks `["16K","32K","128K","256K","1M","8M","32M","128M"]` at 12px `#444` (log-feel via even spacing, not a real log axis); y = 0 to 1000 M reads/s, gridlines `#e5e9ef` at 250/500/750, 12px `#444` tick labels.
- **Throughput line:** blue `#2a78d6` 3px step line through the eight tick positions with values `[1000, 1000, 700, 700, 400, 400, 90, 90]`.
- **Boundary markers:** vertical dashed `#6b7280` (dash 4/3) lines between the 32K/128K, 256K/1M, and 8M/32M ticks, 11px `#6b7280` labels "L1 ends", "L2 ends", "L3 ends" at their tops.
- **Benchmark vs production dots:** green `#008300` 6px dot at the "1M" point labeled bold 12px green "benchmark: 400M/s"; orange `#d95926` 6px dot at the "128M" point labeled bold 12px orange "production: 90M/s".
- **Annotation (bold 13px magenta `#d55181`, near x=380, y=70):** "same code, 4.4× slower past the last cache".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative; step shape is the real behavior".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); latencies 1/4/15/100 ns and sizes 32 KB/256 KB/8 MB/16 GB are typical figures labeled illustrative; the worked example's hit split `[90, 6, 3, 1]` and time contributions `[90, 24, 45, 100]` must sum to 259 ns (average 2.59 ns, vs 10,000 ns all-RAM, ~39×); loop-order timings 20 ms vs 160 ms (8×); throughput steps `[1000, 1000, 700, 700, 400, 400, 90, 90]` M reads/s with benchmark 400M/s at 1 MB vs production 90M/s at 128 MB (4.4×).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
