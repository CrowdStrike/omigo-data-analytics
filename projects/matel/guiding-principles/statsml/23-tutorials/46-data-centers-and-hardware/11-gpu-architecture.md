# GPU Architecture

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** GPU Architecture

**Subtitle:** A CPU makes a few cores fast at anything; a GPU makes thousands of weak cores fast at one thing — doing the same operation to different data

## One Layer, a Billion Identical Multiply-Adds

**Tags:** `core idea` (blue), `thousands of weak cores` (green), `deep learning` (orange)

- **The layer** — one neural-net layer multiplies two 1024×1024 matrices: about a billion multiply-adds
- **All identical** — every single one is the same operation, `a*b + c`, just on different numbers
- **The CPU bet** — 8 big cores, each loaded with big caches, branch prediction, out-of-order tricks
- **The GPU bet** — the same silicon buys 10,240 tiny cores that skip every one of those tricks
- **The catch** — the tiny cores are only fast when they all run the SAME instruction together

*Example (italic):* Split across 10,240 cores the billion multiply-adds are ~105,000 each; across 8 cores they are 134 million each.

**Key point:** A GPU trades per-core smartness for sheer core count — it wins only when the work is one operation repeated across huge amounts of data, which is exactly what deep learning is.

### Visualization (canvas `c1`, 720×300)

Side-by-side die schematic: the same silicon area spent two ways — a CPU die with 8 large cores plus cache machinery, a GPU die as a dense grid of tiny cores.

- **Title (bold 15px, `#1a5276`, top center):** "Same Silicon Budget: 8 Smart Cores vs 10,240 Simple Ones".
- **CPU die:** 2px `#1a5276` rounded rect at x=55, y=60, w=290, h=185; bold 13px `#1a5276` label centered below at y=268: "CPU: 8 cores + big cache".
  - 8 core blocks in 2 rows × 4 cols starting x=67, y=72, each 62×55 with 8px gaps, fill `rgba(42,120,214,0.25)`, 1px `#2a78d6` border, 11px `#2c3e50` "core" centered in each.
  - Cache strip at x=67, y=198, w=266, h=38, fill `rgba(201,133,0,0.20)`, 1px `#c98500` border, 12px label "L3 cache · branch prediction · out-of-order".
- **GPU die:** 2px `#1a5276` rounded rect at x=375, y=60, w=290, h=185; bold 13px `#1a5276` label centered below at y=268: "GPU: 10,240 cores (128 shown)".
  - Grid of 16 cols × 8 rows of 15×18 squares starting x=385, y=68, 2px gaps, fill `rgba(0,131,0,0.30)`, 0.5px `#008300` border, no per-square labels.
- **Caption (12px `#444`, bottom right):** "block sizes schematic; core counts illustrative".

## Lockstep in Warps of 32 — and the Cost of a Branch

**Tags:** `worked example` (blue), `SIMT` (green), `branch divergence` (red)

- **The warp** — GPU threads run in lockstep groups of 32 called warps: one instruction, 32 data lanes
- **Uniform code** — brightening 32 pixels the same way takes 8 instructions, each issued once for all 32
- **The branch** — `if pixel > 128 darken else brighten`: 18 threads go one way, 14 go the other
- **Divergence** — the warp must run BOTH paths, masking off the idle lanes: 8 + 8 = 16 issue slots
- **Hand-check** — same useful work, but the branchy version takes 16 / 8 = 2× as long per warp

*Example (italic):* The warp runs the darken path with 14 lanes parked, then the brighten path with 18 parked — every lane sits idle half the time.

**Key point:** SIMT means threads in a warp share one instruction stream; a divergent branch serializes the paths, so branchy code wastes the very parallelism the GPU is built from.

### Visualization (canvas `c2`, 720×300)

Two-row issue-slot timeline on a shared time scale: a uniform warp finishing in 8 slots vs a divergent warp needing 16 (both paths run back to back with lanes masked).

- **Title (bold 15px, `#1a5276`, top center):** "One Branch Doubles the Warp's Time: 8 Slots vs 16".
- **Time hint:** 12px `#6b7280` label "issue slots →" at x=180, y=68; slot pitch 32px (28px block + 4px gap) identical on both rows so lengths compare directly.
- **Row 1 (blocks at y=85, 28×36 each):** left label 12px `#444` at x=20, y=108: "uniform warp (32/32 active)"; 8 blocks starting x=180, fill `rgba(0,131,0,0.30)`, 1px `#008300` border; bold 12px green `#008300` label "done in 8 slots" at x=444, y=108.
- **Row 2 (blocks at y=180, 28×36 each):** left label "divergent warp": 16 blocks starting x=180; first 8 fill `rgba(42,120,214,0.35)` border `#2a78d6` with 11px `#2a78d6` bracket label above (y=172): "if path — 18 lanes on, 14 masked"; last 8 fill `rgba(217,89,38,0.30)` border `#d95926` with 11px `#d95926` label below (y=232): "else path — 14 on, 18 masked".
- **Finish marker:** vertical dashed `#6b7280` (dash 4/3) line at x=436 from y=80 to y=225 showing where the uniform warp already finished.
- **Annotation (bold 13px `#d95926`, centered near y=262):** "the warp pays for both paths — 2× slower".
- **Caption (12px `#444`, bottom right):** "18/14 split illustrative".

## Feeding Ten Thousand Cores: the Memory Story

**Tags:** `where it's used` (blue), `bandwidth` (green), `bottleneck` (orange)

- **The appetite** — 10,240 cores each doing a multiply-add per cycle starve on ordinary RAM
- **HBM** — GPUs bolt high-bandwidth memory onto the card: ~3,000 GB/s vs ~200 GB/s for CPU DDR
- **The on-ramp** — CPU↔GPU transfers cross PCIe at ~64 GB/s, nearly 50× slower than the HBM
- **The trap** — shipping 4 GB of inputs over PCIe costs ~63 ms, more than the 30 ms of GPU compute
- **Latency hiding** — a warp waiting on memory is parked; one of ~48 resident warps runs instead

*Example (italic):* A model that computes in 30 ms but pulls 4 GB of features over PCIe spends 63 ms on the transfer alone — the math was never the bottleneck.

**Key point:** GPUs pair with extreme-bandwidth memory and hide memory latency by oversubscribing warps; the real bottleneck is usually the slow CPU↔GPU link, so keep data on the card.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of the three bandwidths a batch of data meets: GPU on-card HBM, CPU main memory, and the PCIe link between them.

- **Title (bold 15px, `#1a5276`, top center):** "Three Roads: On-Card Memory Is Fast, the On-Ramp Is Not".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (bars 18px tall at y = 95, 155, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "GPU HBM (on card) — 3,000 GB/s": fill `rgba(0,131,0,0.30)`, 2px `#008300` border, width 430
  - "CPU DDR (main memory) — 200 GB/s": fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, width 285
  - "CPU↔GPU PCIe link — 64 GB/s": fill `rgba(217,89,38,0.35)`, 2px `#d95926` border, width 225
- **Bar-end labels:** 11px `#444` values "3,000" / "200" / "64" just right of each bar end.
- **Annotation (bold 13px `#d95926`, near y=260, centered):** "compute lives at 3,000; every batch arrives at 64 — keep data on the card".
- **Caption (12px `#444`, bottom right):** "bar lengths log scale; bandwidths order-of-magnitude, illustrative".

## When the GPU Loses

**Tags:** `common mistake` (red), `rule of thumb` (orange)

- **The mistake** — "just put it on the GPU" for code full of branches, pointers, and serial steps
- **Pointer chasing** — a linked-list walk is one load after another; 10,240 cores cannot help a chain
- **Branchy code** — heavy if/else splits every warp into serialized paths (the 2× cost compounds)
- **The scoreboard** — matmul: CPU 900 ms vs GPU 30 ms; list chase: CPU 40 ms vs GPU 220 ms
- **Rule of thumb** — GPUs win throughput-per-dollar on uniform parallel work; CPUs win latency and generality

*Example (italic):* The same GPU that beats the CPU 30× on the matrix multiply loses more than 5× to it on the linked-list walk.

**Common mistake:** Treating the GPU as a faster CPU. It is a different machine — a throughput engine that pays for its 30× matmul win by losing on anything serial, branchy, or transfer-bound.

### Visualization (canvas `c4`, 720×300)

Grouped vertical bar chart: CPU vs GPU wall-clock time on two tasks — a uniform matrix multiply and a serial, branchy linked-list traversal.

- **Title (bold 15px, `#1a5276`, top center):** "Same GPU, Opposite Results".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = time in ms, 0 to 1,000, gridlines `#e5e9ef` at 250/500/750 with 12px `#444` tick labels.
- **Bar scale:** height = value / 1000 × 180 px; all bars 60px wide, drawn up from y=245.
- **Group 1 — "matrix multiply (uniform)" (12px `#444` group label centered at x=250, y=265):** CPU bar at x=180, value 900 ms (height 162), fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; GPU bar at x=260, value 30 ms (height 5), fill `rgba(0,131,0,0.30)`, 2px `#008300` border.
- **Group 2 — "linked-list chase (serial, branchy)" (group label centered at x=530, y=265):** CPU bar at x=460, value 40 ms (height 7); GPU bar at x=540, value 220 ms (height 40); same CPU/GPU fills as group 1.
- **Value labels:** bold 12px `#2c3e50` "900 ms" / "30 ms" / "40 ms" / "220 ms" centered above each bar.
- **Verdict annotations:** bold 12px green `#008300` "GPU 30× faster" near x=250, y=60; bold 12px red `#e74c3c` "GPU 5× slower" near x=530, y=170.
- **Legend (top right, 11px):** blue swatch "CPU (8 cores)", green swatch "GPU (10,240 cores)".
- **Caption (12px `#444`, bottom right):** "times illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); core counts (8 / 10,240), the 18/14 lane split, resident-warp count (~48), and the CPU/GPU timings (900/30/40/220 ms) are invented and labeled illustrative; the warp size 32, the 8-vs-16-slot divergence arithmetic, the billion multiply-adds of a 1024³ matmul, and the 4 GB ÷ 64 GB/s ≈ 63 ms transfer math are exact; bandwidths (3,000 / 200 / 64 GB/s) are order-of-magnitude realistic and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
