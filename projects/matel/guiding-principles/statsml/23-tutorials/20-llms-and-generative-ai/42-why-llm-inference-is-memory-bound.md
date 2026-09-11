# Why LLM Inference Is Memory-Bound

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Why LLM Inference Is Memory-Bound

**Subtitle:** Producing one token takes very little math but a huge fetch — the chip spends almost all its time waiting for the model's weights to arrive from memory, not computing

## A One-Second Job at the End of a 99-Second Walk

**Tags:** `core idea` (blue), `fetch vs work` (green), `waiting` (orange)

- **The packer** — a warehouse packer tapes a box shut in 1 second flat, the fastest hands in the building
- **The aisle** — but every box sits at the far end of the aisle, and each cart trip there and back takes 99 seconds
- **The split** — of every 100 seconds, 1 goes to taping and 99 to walking; a faster taper changes almost nothing
- **The GPU** — an LLM chip is that packer: blistering at math, but every token needs the weights hauled in from memory first
- **The name** — a job whose pace is set by the fetching, not the working, is called memory-bound

*Example (italic):* Doubling the packer's taping speed saves half a second per box out of 100 — halving the aisle saves 49.5.

**Key point:** When fetching takes far longer than working, the fetch sets the speed — that is all "memory-bound" means.

### Visualization (canvas `c1`, 720×300)

Two horizontal stacked bars on a shared 0–100% "share of time" axis: the packer's box and the GPU's token, each split into a long fetch segment and a sliver of work, showing the two stories are the same picture.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Time Goes: 99% Fetching, 1% Working".
- **Axis:** horizontal 2px `#999` line at y=245 from x=180 to x=680 (width 500), scale 0 to 100%; 12px `#444` tick labels "0%", "25%", "50%", "75%", "100%".
- **Row 1 (bar center y=110, 34px tall), left-aligned 12px `#444` label at x=20:** "packer: one box (100 s)"; segment 0–99% fill `rgba(42,120,214,0.35)` with 12px blue `#2a78d6` label "walking the aisle — 99 s" centered inside; segment 99–100% fill orange `#d95926` with bold 12px orange label "tape 1 s" above the bar's right end, short pointer line down to the sliver.
- **Row 2 (bar center y=185, 34px tall), label at x=20:** "GPU: one token"; segment 0–99% fill `rgba(42,120,214,0.35)` with 12px blue label "fetching weights — 99% of the time" centered inside; segment 99–100% fill orange `#d95926` with bold 12px orange label "math 1%" below the bar's right end, pointer line up.
- **Annotation (bold 13px ink `#1a5276`, centered near x=430, y=55):** "same shape: the trip, not the job, sets the pace".
- **Caption (12px `#444`, bottom right):** "illustrative — GPU split worked out with real numbers in the next section".

## One Token on a 7-Billion-Weight Model, by Hand

**Tags:** `worked example` (blue), `back-of-envelope` (green)

- **The bytes** — 7 billion weights at 2 bytes each is 14 GB, and all of it is read to produce each token
- **The ops** — one token costs about 2 operations per weight: 14 billion operations of math
- **The fetch clock** — at 1,000 GB/s of memory bandwidth, reading 14 GB takes 14 ms
- **The math clock** — at 100 trillion ops/s, doing 14 billion operations takes 0.14 ms
- **The verdict** — the fetch is 100× slower, so the chip computes 1% of the time and waits the other 99%
- **The ceiling** — one token per 14 ms is at most about 71 tokens per second, set purely by memory

*Example (italic):* 14 GB ÷ 1,000 GB/s = 14 ms to fetch, but 14 billion ops ÷ 100 trillion ops/s = 0.14 ms to compute — a 100-to-1 wait.

**Key point:** Fetch 14 ms vs math 0.14 ms — token speed (≈71 tokens/s) is decided by the memory pipe, not the calculator.

### Visualization (canvas `c2`, 720×300)

Two horizontal bars on a shared milliseconds axis: the weight fetch spanning nearly the full width and the math a barely-visible sliver, making the 100× gap physically visible.

- **Title (bold 15px, `#1a5276`, top center):** "One Token's Clock: 14 ms of Fetching, 0.14 ms of Math".
- **Axis:** horizontal 2px `#999` line at y=245 from x=200 to x=680 (width 480), scale 0 to 15 ms; 12px `#444` tick labels "0", "5", "10", "15 ms" at 0, 5, 10, 15.
- **Row 1 (bar center y=115, 40px tall), left-aligned 12px `#444` label at x=20:** "reading 14 GB of weights"; bar from 0 to 14 ms, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; bold 13px blue `#2a78d6` value label "14 ms" just right of the bar end.
- **Row 2 (bar center y=190, 40px tall), label at x=20:** "doing 14 billion ops"; bar from 0 to 0.14 ms (about 4px wide — draw at least 4px so it stays visible), fill orange `#d95926`; bold 13px orange value label "0.14 ms" right of the sliver with a short pointer line.
- **Ceiling note:** dashed `#6b7280` (dash 4/3) vertical line at 14 ms from y=70 to the axis; 12px `#6b7280` label at its top: "1 token / 14 ms ≈ 71 tokens/s".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=218):** "the math fits inside 1% of the fetch".
- **Caption (12px `#444`, bottom right):** "illustrative round numbers: 14 GB model, 1,000 GB/s, 100 T ops/s".

## The Roofline: One Ratio Predicts the Speed

**Tags:** `where it's used` (blue), `arithmetic intensity` (green), `roofline` (orange)

- **The ratio** — arithmetic intensity = operations done per byte fetched; our token does 14 B ops on 14 GB = 1 op/byte
- **The ridge** — this chip can feed 100 ops per byte fetched (100 T ops/s ÷ 1,000 GB/s); below 100, memory rules
- **The roofline** — plot intensity against speed: a slanted memory line and a flat compute roof meet at the ridge
- **Batching** — serve 8 requests at once and each fetched weight gets used 8 times: intensity ≈ 8 ops/byte
- **Climbing** — batch 1 → 8 → 64 slides the dot up the slanted line; only past 100 does the flat roof take over

*Example (italic):* Batch 1 runs at 1 T ops/s (1% of peak), batch 8 at 8 T, batch 64 at 64 T, and batch 256 finally sits on the 100 T roof.

**Key point:** One number — ops per byte, placed on the roofline — tells you whether memory or compute sets your speed, before any benchmark runs.

### Visualization (canvas `c3`, 720×300)

Roofline chart on log-log axes: a slanted blue bandwidth line rising to the ridge, a flat green compute roof beyond it, and four dots for batch sizes 1, 8, 64, 256 climbing the slope onto the roof.

- **Title (bold 15px, `#1a5276`, top center):** "The Roofline: Slanted Memory Line, Flat Compute Roof, Ridge at 100".
- **Geometry:** plot origin x=70, baseline y=245, plot width 590, plot height 185; both axes log2. Map intensity I (0.5 to 512) to `x = 70 + 59 * log2(I / 0.5)`; map speed S in T ops/s (0.5 to 128... roof at 100 plots inside) to `y = 245 - 23.1 * log2(S / 0.5)`.
- **Axes labels:** x ticks at I = 1, 4, 16, 64, 256 with 12px `#444` labels; 12px `#444` axis caption "arithmetic intensity (ops per byte, log scale)" centered below; y ticks at S = 1, 10, 100 with 12px `#444` labels; rotated or top-left 12px `#444` caption "speed (trillion ops/s)".
- **Memory line:** blue `#2a78d6` 3px line from point (I=0.5, S=0.5) to the ridge (I=100, S=100); 12px blue label along it: "memory line: speed = 1 × intensity".
- **Compute roof:** green `#008300` 3px horizontal line from the ridge (I=100, S=100) to (I=512, S=100); 12px green label above its right half: "compute roof: 100 T ops/s".
- **Ridge marker:** dashed `#6b7280` (dash 4/3) vertical line at I=100 from the roof down to the baseline; 11px `#6b7280` label near the baseline: "ridge: 100 ops/byte".
- **Batch dots (7px filled circles) with bold 12px labels beside each:** batch 1 at (1, 1) orange `#d95926`, labeled "batch 1 — 1% of peak"; batch 8 at (8, 8) aqua `#199e70`, labeled "batch 8"; batch 64 at (64, 64) aqua `#199e70`, labeled "batch 64"; batch 256 at (256, 100) violet `#4a3aa7` sitting on the roof, labeled "batch 256 — roof".
- **Annotation (bold 13px violet `#4a3aa7`, near x=200, y=80):** "batching reuses each fetched byte — the dot climbs the slope".
- **Caption (12px `#444`, bottom right):** "illustrative machine: 1,000 GB/s, 100 T ops/s".

## Buying More TFLOPs Won't Fix It

**Tags:** `common mistake` (red), `what actually helps` (orange)

- **The mistake** — upgrading to a chip with twice the math speed and expecting twice the chat speed
- **The reality** — single-stream generation stays at 71 tokens/s, because the 14 GB fetch did not shrink
- **Bandwidth helps** — doubling memory bandwidth halves the fetch to 7 ms: about 143 tokens/s
- **Fewer bytes help** — 4-bit weights shrink 14 GB to 3.5 GB: a 3.5 ms fetch, about 286 tokens/s
- **Batching helps** — more users share each fetch, raising total throughput, though no single user's stream speeds up

*Example (italic):* Same model, same chip: shrinking weights to 4-bit quadrupled tokens/s, because the bytes — not the ops — were the bottleneck.

**Common mistake:** Reading a chip's TFLOPs spec as its chat speed. For token-by-token generation, memory bandwidth and the model's bytes are the specs that matter.

### Visualization (canvas `c4`, 720×300)

Four vertical bars of single-stream tokens per second: the baseline, a useless 2× compute upgrade at the same height, then 2× bandwidth and 4-bit weights rising past it.

- **Title (bold 15px, `#1a5276`, top center):** "What Moves Tokens/s: Bytes and Bandwidth, Not TFLOPs".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y = tokens per second 0 to 320, light `#e5e9ef` gridlines at 100, 200, 300 with 12px `#444` labels.
- **Bars (4 bars, 90px wide, evenly spaced across the plot), values `[71, 71, 143, 286]`, each with a bold 13px value label on top and a 12px `#444` two-line category label below the baseline:**
  - "baseline / (14 GB, 1,000 GB/s)": fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border, blue value label "71"
  - "2× compute / (same memory)": fill `rgba(107,114,128,0.30)`, 2px `#6b7280` border, mute value label "71 — no change"
  - "2× bandwidth / (2,000 GB/s)": fill `rgba(25,158,112,0.35)`, 2px `#199e70` border, aqua value label "143"
  - "4-bit weights / (3.5 GB)": fill `rgba(0,131,0,0.35)`, 2px `#008300` border, green value label "286"
- **Reference line:** dashed `#6b7280` (dash 4/3) horizontal line at 71 across the plot, 11px `#6b7280` label "baseline 71" at its left end.
- **Annotation (bold 13px magenta `#d55181`, near x=250, y=75):** "doubling the math speed bought exactly nothing".
- **Caption (12px `#444`, bottom right):** "illustrative — single-stream generation, one token at a time".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, dot positions, and timings are the hardcoded literals above (no randomness). One consistent illustrative machine everywhere: 14 GB of weights (7 B weights × 2 bytes), 1,000 GB/s bandwidth, 100 T ops/s peak — giving fetch 14 ms, math 0.14 ms, 71/143/286 tokens/s, ridge at 100 ops/byte. Text numbers and chart numbers must stay identical.
- **Log axes (c3):** compute pixel positions from the mapping formulas given in the c3 spec; do not draw a linear scale with log labels.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
