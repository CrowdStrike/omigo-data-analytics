# TPUs & Custom Silicon

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** TPUs & Custom Silicon

**Subtitle:** A TPU gives up the ability to run any program so that nearly every transistor can do matrix math — hardware built for exactly one workload

## The Photo Service That Does One Thing a Billion Times

**Tags:** `core idea` (blue), `specialization spectrum` (green), `ASIC` (orange)

- **The workload** — a photo-tagging service runs the same neural-net matrix multiplies on every image
- **The CPU** — runs any program ever written; most of its area is control logic and cache, not math
- **The GPU** — runs uniform parallel work; thousands of small cores, still general across parallel jobs
- **The ASIC** — one workload burned into silicon; a TPU is an ASIC whose one job is matrix multiply
- **The spectrum** — each step trades flexibility for spending more of the chip on the actual arithmetic

*Example (italic):* The tagging service never runs a compiler, a database, or a game — chip area that could is pure overhead for it.

**Key point:** A domain-specific accelerator wins by deleting generality: when the workload is one operation repeated forever, every transistor not doing that operation is waste.

### Visualization (canvas `c1`, 720×300)

Stacked horizontal bar chart: for CPU, GPU, and TPU/ASIC, how the chip's area splits between arithmetic units and everything else (control, caches, scheduling) — illustrative proportions.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Transistors Go: Generality Costs Chip Area (illustrative)".
- **Rows (top to bottom at y = 85, 150, 215), each with a left-aligned 12px `#444` label at x=20:** "CPU — any program", "GPU — parallel programs", "TPU/ASIC — one workload".
- **Bars:** start x=190, total width 440, height 26; left segment = arithmetic (solid green `#008300`), right segment = control + cache + everything else (fill `rgba(42,120,214,0.30)`, 1px `#2a78d6` edge).
- **Splits (hardcoded pixel widths):** CPU green 88 / blue 352 ("20% math"); GPU green 242 / blue 198 ("55% math"); TPU green 396 / blue 44 ("90% math"). Percentage labels bold 12px white inside the green segment (CPU's "20% math" in green `#008300` just right of its segment if too narrow).
- **Legend (12px `#444`, top right):** green swatch "arithmetic", blue swatch "control + cache".
- **Annotation (bold 13px green `#008300`, below the TPU row near y=255):** "specialization = spend the chip on the math".
- **Caption (12px `#444`, bottom right):** "area splits illustrative".

## Partial Sums That Walk Across the Grid

**Tags:** `worked example` (blue), `systolic array` (green)

- **The grid** — a lattice of multiply-accumulate (MAC) cells; weights are loaded once and stay put
- **The flow** — inputs stream in from one edge; each cell multiplies, adds, passes the sum onward
- **Hand-check** — weights [1, 2, 0] meet inputs [2, 1, 3]: 1×2 = 2, then 2 + 2×1 = 4, then 4 + 0×3 = 4
- **No fetch** — the partial sum never touches memory between steps; it hops one neighbor per tick
- **At scale** — TPU v1's published array is 256×256 = 65,536 MACs peaking at 92 trillion ops/s

*Example (italic):* One 3-cell row computes the dot product 1×2 + 2×1 + 0×3 = 4 in three ticks with zero memory reads in between.

**Key point:** A systolic array turns matrix multiply into a flow — data pulses through a grid of MACs and partial sums pass neighbor to neighbor, so control logic and memory traffic almost vanish.

### Visualization (canvas `c2`, 720×300)

Flow diagram of one 3-cell systolic row computing the dot product [1,2,0]·[2,1,3] = 4: inputs drop in from the top, partial sums hop cell to cell left-to-right.

- **Title (bold 15px, `#1a5276`, top center):** "One Row of the Grid: the Partial Sum Hops Neighbor to Neighbor".
- **MAC cells:** three rounded boxes 130×54 (8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) at x = 140, 330, 520, all at y=130; two text lines each (12px `#2c3e50`, centered): cell 1 "w = 1" / "1×2 + 0 = 2", cell 2 "w = 2" / "2×1 + 2 = 4", cell 3 "w = 0" / "0×3 + 4 = 4".
- **Input arrows:** vertical 3px `#c98500` arrows from y=70 down to each box top, each with a bold 13px `#c98500` label above: "in: 2", "in: 1", "in: 3"; small 11px `#6b7280` note "inputs stream in" at top left near x=60, y=80.
- **Sum arrows:** horizontal 3px `#008300` arrows between boxes at mid-height y=157, bold 13px green labels "2" and "4" above each arrow; a final green arrow from cell 3 to x=690 labeled bold 13px "out: 4".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=235):** "no memory read between steps — the sum just moves one cell over".
- **Caption (12px `#444`, bottom right):** "one row shown; TPU v1's published grid is 256×256 MACs".

## The Published Numbers: What Saying No Bought

**Tags:** `where it's used` (blue), `perf per watt` (green), `reduced precision` (orange)

- **Published speed** — the TPU-v1 paper reports ~15–30× faster inference than its contemporary CPU/GPU
- **Published efficiency** — the same paper reports 30–80× better performance per watt on its benchmarks
- **The trick** — no branch predictors, no deep cache hierarchy; transistors go to MACs and buffers
- **Reduced precision** — v1 does inference in 8-bit integers; int8 MACs are far smaller than fp32 ones
- **The bf16 sequel** — later training TPUs kept the bargain with bfloat16: fp32 range in half the bits

*Example (italic):* The v1 chip ran inference in 8-bit integer arithmetic — precision was traded away as deliberately as generality was.

**Key point:** Specialization and reduced precision are one bargain: accept only matrix math at low precision, and the paper's order-of-magnitude perf-per-watt gains are what you get back.

### Visualization (canvas `c3`, 720×300)

Horizontal range-bar chart of the TPU-v1 paper's headline results versus its 2015-era CPU/GPU baseline: speed 15–30×, performance per watt 30–80×.

- **Title (bold 15px, `#1a5276`, top center):** "TPU v1 vs Contemporary CPU/GPU (published ranges, Jouppi et al. 2017)".
- **Axis:** horizontal 2px `#999` baseline line at y=250 from x=230 to x=680; linear scale 0–80× at 5.5 px per unit (x = 230 + value×5.5); 12px `#444` tick labels at 0, 20, 40, 60, 80 ("×" suffix); light `#e5e9ef` vertical gridlines at those ticks from y=60 to y=250.
- **Baseline marker:** vertical dashed `#6b7280` (dash 4/3) line at x=236 (1×), 12px `#6b7280` label "CPU/GPU baseline 1×" angled or placed above at y=55.
- **Rows (left-aligned 12px `#444` labels at x=20):**
  - y=115: "inference speed" — green `#008300` range bar 18px tall from x=313 (15×) to x=395 (30×), fill `rgba(0,131,0,0.30)` with 2px solid green ends; bold 12px green label "15–30× faster" right of the bar.
  - y=185: "performance per watt" — blue `#2a78d6` range bar 18px tall from x=395 (30×) to x=670 (80×), fill `rgba(42,120,214,0.30)` with 2px solid blue ends; bold 12px blue label "30–80×" centered above the bar.
- **Annotation (bold 13px magenta `#d55181`, near x=250, y=285):** "ranges as published; no numbers invented here".

## When the Workload Moves and the Silicon Can't

**Tags:** `common mistake` (red), `workload match` (orange)

- **The bet** — an accelerator freezes today's dominant operation into a chip that ships years later
- **The miss** — a model full of dynamic shapes, sparsity, or branching falls off the fast path
- **The pattern** — video encoders, crypto miners, inference chips: big, stable workloads earn silicon
- **The mistake** — assuming the headline speedup belongs to the chip rather than the chip–workload pair
- **The hedge** — vendors respin generations (v2, v3, …), chasing the workload as it drifts

*Example (italic):* A team benchmarks a dense CNN at 30× on the accelerator, then ships a sparse ranking model that gets 3× (illustrative).

**Common mistake:** Quoting an accelerator's speedup as a fixed property of the hardware. It is a property of the match between silicon and workload — change the dominant model architecture and the number changes with it.

### Visualization (canvas `c4`, 720×300)

Vertical bar chart: the same accelerator's speedup over CPU on a matched workload vs a mismatched one — illustrative numbers echoing the section's example.

- **Title (bold 15px, `#1a5276`, top center):** "Same Chip, Different Workload (illustrative)".
- **Axes:** origin x=110, baseline y=245, plot width 480, plot height 180; y = speedup over CPU 0–35×, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels "10×", "20×", "30×"; dashed `#6b7280` (dash 4/3) horizontal reference line at 1× with 11px `#6b7280` label "CPU = 1×" at its right end.
- **Bars (width 130):**
  - centered x=250: "dense matmul model (as benchmarked)" — green `#008300` fill `rgba(0,131,0,0.30)`, 2px green edge, height 154 px (30×), bold 13px green value label "30×" above the bar; 12px `#444` two-line category label under the axis.
  - centered x=490: "sparse / dynamic model (as shipped)" — red `#e74c3c` fill `rgba(231,76,60,0.12)`, 2px red edge, height 15 px (3×), bold 13px red value label "3×" above the bar; matching category label below.
- **Annotation (bold 13px orange `#d95926`, near x=370, y=65, above the 30× bar's value label):** "the speedup belongs to the chip–workload pair".
- **Caption (12px `#444`, bottom right):** "speedups illustrative — not from the TPU paper".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness). Chip-area splits (c1) and the 30×/3× workload-match bars (c4) are invented and labeled illustrative; the systolic hand-check (weights [1,2,0], inputs [2,1,3], output 4) is exact arithmetic; TPU figures (15–30× speed, 30–80× perf/watt, 256×256 array, 65,536 MACs, 92 trillion ops/s, 8-bit inference) are the published TPU-v1 paper's numbers and must not be altered.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
