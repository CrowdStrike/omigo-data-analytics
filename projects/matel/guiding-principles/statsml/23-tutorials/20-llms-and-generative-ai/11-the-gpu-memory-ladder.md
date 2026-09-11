# The GPU Memory Ladder

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The GPU Memory Ladder

**Subtitle:** A GPU is a tiny fast desk beside a huge slow bookshelf — the sizes and speeds of each memory layer, and how LLMs cut their math into tiles so every byte hauled to fast memory gets reused

## The Four-Rung Memory Ladder

**Tags:** `core idea` (blue), `memory ladder` (green)

- **The job** — Bob's 7B chatbot must read all 14 GB of its weights to produce each single token
- **Rung 1** — on-chip SRAM: ~40 MB at ~20 TB/s — the desk: tiny, everything within arm's reach
- **Rung 2** — GPU memory (HBM): 80 GB at ~3 TB/s — the bookshelf where the 14 GB of weights live
- **Rung 3** — CPU RAM: ~1 TB at ~60 GB/s over the PCIe cable — the basement across the hall
- **Rung 4** — SSD: many TB at ~6 GB/s — the warehouse; visit it mid-answer and the chat stalls

*Example (italic):* Each rung down is roughly 100–1000× bigger and 10–50× slower — and the weights sit one rung below where the math happens.

**Key point:** The GPU is a desk worker — arithmetic happens only at the desk (SRAM); everything else in the machine is fetching.

### Visualization (canvas `c1`, 720×300)

Inverted funnel of four memory rungs, widths growing downward with capacity, speed labels on the right and a plain-words nickname on the left.

- **Title (bold 15px, `#1a5276`, top center):** "The Four-Rung Memory Ladder (one GPU server)".
- **Bands (rounded rects 6px radius, height 40, vertically at tops y = `[52, 100, 148, 196]`, horizontally centered on x=350):**
  - Rung 1: width 160 (x=270), fill `rgba(0,131,0,0.12)`, 2px `#008300` border; bold 12px `#008300` centered label "on-chip SRAM — ~40 MB".
  - Rung 2: width 280 (x=210), fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border; two centered lines: bold 12px `#1a5276` "GPU memory (HBM) — 80 GB" (y+17) and 11px `#d95926` "Bob's 14 GB of weights live here" (y+32).
  - Rung 3: width 400 (x=150), fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border; bold 12px `#4a3aa7` centered label "CPU RAM — ~1 TB (across the PCIe cable)".
  - Rung 4: width 520 (x=90), fill `rgba(107,114,128,0.10)`, 2px `#6b7280` border; bold 12px `#6b7280` centered label "SSD — many TB".
- **Nicknames (12px `#6b7280`, left-aligned at x=14, vertically centered per band):** "the desk", "the bookshelf", "the basement", "the warehouse".
- **Speed labels (bold 12px, right-aligned at x=706, vertically centered per band, in each band's border color):** "~20 TB/s", "~3 TB/s", "~60 GB/s", "~6 GB/s".
- **Annotation (bold 12px orange `#d95926`, centered at y=266):** "each rung down: ~100–1000× more space, ~10–50× less speed".
- **Caption (11px `#444`, bottom right, y=292):** "sizes & speeds illustrative — one modern server".

## Tiling: Reuse Every Brick You Haul

**Tags:** `worked example` (blue), `tiling` (orange)

- **The task** — multiply two 8×8 grids of numbers; every output cell needs one row and one column
- **Naive** — fetch a fresh row + column per cell: 64 cells × 16 numbers = 1,024 slow fetches
- **Tiled** — haul 4×4 blocks to the desk once: 4 output blocks × 4 block-loads × 16 = 256 fetches
- **Check it** — 1,024 ÷ 256 = 4× less traffic, exactly the tile width; bigger tiles save even more
- **The limit** — tiles must fit the ~40 MB desk; choosing tile sizes is the craft of kernel writing

*Example (italic):* FlashAttention is this same trick applied to attention — the big score table is computed tile by tile at the desk and never visits the bookshelf.

**Key point:** Split the math into tiles that fit fast memory and reuse each tile fully — slow-memory traffic drops by the tile width.

### Visualization (canvas `c2`, 720×300)

Three panels: the naive fetch pattern on an 8×8 grid, the tiled pattern on the same grid, and a two-bar comparison of total fetches.

- **Title (bold 15px, `#1a5276`, top center):** "Same 8×8 Multiply: 1,024 Fetches Naive vs 256 Tiled".
- **Left grid (naive):** 8×8 grid of 17px cells, top-left at (40, 66); 1px `#e5e9ef` cell borders; row 3 (index 2) shaded `rgba(42,120,214,0.25)` and column 6 (index 5) shaded `rgba(213,81,129,0.25)`; their crossing cell filled `#d95926`. Caption below (11px `#6b7280`, centered x=108, two lines y=222/236): "naive: every output cell" / "re-fetches its row + column".
- **Middle grid (tiled):** same 8×8 grid, top-left at (270, 66); thick 2px `#008300` lines splitting it into four 4×4 blocks; top-left block shaded `rgba(0,131,0,0.20)`. Caption below (11px `#6b7280`, centered x=338, two lines y=222/236): "tiled: haul a 4×4 block once," / "reuse it four times at the desk".
- **Right bars:** baseline y=210, two bars 70px wide centered at x = `[540, 650]`; heights proportional to `[1024, 256]` with 1024 → 130px; naive bar `#6b7280`, tiled bar `#008300`; bold 13px value labels "1,024" and "256" above each bar in the bar's color; 12px `#444` labels "naive" / "tiled" below the baseline at y=228.
- **Annotation (bold 12px orange `#d95926`, centered at y=268):** "4× less traffic — exactly the tile width; GPUs do this on every layer".
- **Caption (11px `#444`, bottom right, y=292):** "count them yourself: 64×16 vs 4×4×16".

## Who Lives on Which Rung

**Tags:** `where it's used` (blue), `placement` (green), `offloading` (orange)

- **Weights** — permanent residents of HBM; read top to bottom once for every single token
- **KV cache** — the conversation's memory; grows in HBM with every token of every open chat
- **Activations** — the work in progress; streamed through SRAM tile by tile, barely touching HBM
- **Overflow** — when HBM fills, frameworks offload KV or weights to CPU RAM — a 50× slower rung
- **Why it matters** — serving cost, context limits, and batch size are all HBM real-estate math

*Example (italic):* One very long conversation's KV cache can outweigh the model itself — HBM fills up with memories, not weights.

**Key point:** HBM is prime real estate split between weights and the KV cache — "out of memory" almost always means this shelf ran out.

### Visualization (canvas `c3`, 720×300)

Floor plan of a GPU: SRAM strip on top, an HBM band holding the weights block and a growing KV-cache block, and an offload lane over PCIe to CPU RAM and SSD boxes outside.

- **Title (bold 15px, `#1a5276`, top center):** "Who Lives Where on the GPU".
- **GPU box:** rounded rect x=30, y=52, 430×210, 2px `#2a78d6` border, no fill; bold 13px `#2a78d6` label "GPU" at top-left inside (x=44, y=72).
- **SRAM strip:** rounded rect x=50, y=82, 390×32, fill `rgba(0,131,0,0.12)`, 2px `#008300` border; bold 12px `#008300` centered label "SRAM — activations tile through here".
- **HBM band:** rounded rect x=50, y=136, 390×110, fill `rgba(42,120,214,0.08)`, 2px `#2a78d6` border; bold 12px `#1a5276` label "HBM — 80 GB" at (x=64, y=156) left-aligned.
  - Weights block: rounded rect x=65, y=166, 150×66, fill `rgba(0,131,0,0.15)`, 2px `#008300` border; bold 12px `#008300` centered two lines "weights" / "14 GB".
  - KV block: rounded rect x=230, y=166, 190×66, fill `rgba(217,89,38,0.15)`, 2px `#d95926` border; bold 12px `#d95926` centered two lines "KV cache" / "grows every token →".
- **SRAM↔HBM arrow:** double-headed 2px `#6b7280` vertical arrow at x=245 between y=114 and y=136.
- **CPU RAM box:** rounded rect x=520, y=80, 170×56, fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border; bold 12px `#4a3aa7` centered label "CPU RAM — ~1 TB".
- **SSD box:** rounded rect x=520, y=186, 170×56, fill `rgba(107,114,128,0.10)`, 2px `#6b7280` border; bold 12px `#6b7280` centered label "SSD — many TB".
- **PCIe offload lane:** double-headed 2px `#d95926` horizontal arrow from (462, 155) to (518, 110) drawn as a straight segment; 11px `#d95926` label "PCIe — offload lane (50× slower)" centered at (480, 172) — place beneath the arrow, left-aligned at x=452 if centering collides with boxes.
- **Annotation (bold 12px orange `#d95926`, centered at y=280):** "out of memory = this shelf ran out — weights and memories compete for it".
- **Caption (11px `#444`, bottom right, y=296):** "capacities illustrative".

## More Horsepower Doesn't Move More Bricks

**Tags:** `common mistake` (red), `memory-bound` (orange)

- **The floor** — one token needs 14 GB read from HBM: 14 GB ÷ 3 TB/s ≈ 4.7 ms, math aside
- **The upgrade** — a GPU with double the TFLOPs but the same HBM speed: still ≈ 4.7 ms per token
- **What helps** — faster HBM, smaller weights (quantization), or batching more users per read
- **Not automatic** — CPU caches speed programs up invisibly; GPU tiling is written by hand
- **The mistake** — buying compute when the bottleneck is the pipe between shelf and desk

*Example (italic):* The spec sheet doubled the TFLOPs; the token timer didn't move — the bookshelf, not the desk, was setting the pace.

**Common mistake:** Reading GPU spec sheets by TFLOPs alone — for LLM serving, bytes per second usually matters more than math per second.

### Visualization (canvas `c4`, 720×300)

Bar chart: per-token time for the same model on three machines — baseline, doubled compute, doubled memory bandwidth — showing only the bandwidth upgrade moves the bar.

- **Title (bold 15px, `#1a5276`, top center):** "Three Machines, One Token Timer".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot top y=60; y = milliseconds per token 0 to 6 with light `#e5e9ef` gridlines at 1–6 and 12px `#444` tick labels.
- **Bars (90px wide, centered x = `[200, 390, 580]`):** "today" value 4.7 in gray `#6b7280`; "double the TFLOPs" value 4.7 in blue `#2a78d6`; "double the HBM speed" value 2.3 in green `#008300`; bold 13px value labels "4.7 ms", "4.7 ms", "2.3 ms" above each bar in the bar's color.
- **X labels:** 12px `#444` main label at y=263 ("today", "double compute", "double memory speed") and an 11px `#6b7280` spec line at y=279 ("100 TFLOPs · 3 TB/s", "200 TFLOPs · 3 TB/s", "100 TFLOPs · 6 TB/s").
- **Annotation (bold 13px orange `#d95926`, centered at (390, 85)):** "14 GB per token ÷ bandwidth = the floor — only the pipe upgrade moved it".
- **Caption (12px `#444`, bottom right):** "times illustrative: 14 GB ÷ 3 TB/s ≈ 4.7 ms".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all band widths, grid shading positions, fetch counts, and bar values are the hardcoded numbers above (no randomness); the fetch bars must read 1,024 vs 256 to match the text, and the token-time bars must read 4.7 / 4.7 / 2.3 ms to match 14 GB ÷ 3 or 6 TB/s.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
