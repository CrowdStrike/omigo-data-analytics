# Why GPUs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Why GPUs

**Subtitle:** Neural nets are almost entirely multiplying big grids of numbers — a CPU does a few multiplications at a time brilliantly, a GPU does thousands at once adequately

## A Neural Net Is Mostly One Job: Multiplying Grids

**Tags:** `core idea` (blue), `running example` (green)

- **The job** — a layer of 1,000 neurons reading 1,000 inputs is a 1,000 × 1,000 grid of weights
- **One pass** — multiply every input by every weight: 1,000,000 tiny multiplications
- **All independent** — no multiplication needs another one's answer; order does not matter
- **Stacked layers** — a deep net is just this grid multiply repeated, layer after layer
- **Perfect for a crowd** — a million independent tiny jobs is exactly what parallel hardware loves

*Example:* Grading 1,000,000 one-digit sums: one genius or a stadium of average helpers — the stadium finishes first.

**Key point:** Neural net work is a million independent tiny multiplications — the question is only how many you can do at the same time.

### Visualization (canvas `c1`, 720×300)

Grid sketch of independent multiplications with one highlighted cell.

- **Title (bold 15px `#1a5276`, top center):** "1,000 Inputs × 1,000 Neurons = a Grid of 1,000,000 Tiny Jobs".
- **Grid:** 10×10 cells starting at (130,60), 19px cells (2px gutters), each filled with one of four translucent colors cycling by `(row·3 + col·7) % 4`: `rgba(42,120,214,0.25)`, `rgba(25,158,112,0.25)`, `rgba(213,81,129,0.22)`, `rgba(201,133,0,0.22)`; whole grid outlined navy `#1a5276` 1.5px.
- **Highlighted cell:** cell at column 4, row 3 outlined orange `#d95926` 3px, with an orange pointer line to the right-side annotation.
- **Right-side annotation (left-aligned at x=350):** bold orange 12px "one cell = one multiplication:"; `#2c3e50` 13px "input no. 5  ×  weight (5, 4)"; bold green `#008300` 12px "needs no other cell's answer".
- **Grid caption (gray `#666` 12px, centered under grid):** "10 × 10 shown — the real grid is 1,000 × 1,000".
- **Lower-right annotation:** bold magenta `#d55181` 13px, two lines: "all 1,000,000 cells are independent —" / "they could all be computed at the same instant"; navy 12px line: "a deep net repeats this grid multiply, layer after layer".

## 8 Brilliant Workers vs 10,000 Adequate Ones

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **The CPU** — 8 cores, each does 1 multiplication per tick: 8 per tick, each core very fast
- **The GPU** — 10,000 lanes, each half as fast: think 10,000 per tick, but ticks twice as long
- **CPU total** — 1,000,000 ÷ 8 = 125,000 ticks to finish the grid
- **GPU total** — 1,000,000 ÷ 10,000 = 100 slow ticks = 200 CPU-tick equivalents
- **The ratio** — 125,000 vs 200: the GPU finishes this grid about 625× sooner

*Example:* Each GPU lane is the slower worker — but 10,000 slower workers beat 8 fast ones on a million tiny jobs.

**Key point:** The GPU wins not by being faster per multiplication — it is slower — but by doing thousands of them at once.

### Visualization (canvas `c2`, 720×300)

Two horizontal bars comparing ticks to finish.

- **Title (bold 15px `#1a5276`, top center):** "Time to Finish 1,000,000 Multiplications (in CPU ticks)".
- **Bars (34px tall, 0.75 alpha, from a vertical gray axis at x=220; scale max 125,000; minimum visible width 4px):**
  - Row 1 (y=110): label "CPU: 8 cores × 1 per tick", value 125,000, blue `#2a78d6`, value label "125,000 ticks", note "1,000,000 ÷ 8 = 125,000 ticks".
  - Row 2 (y=190): label "GPU: 10,000 lanes, half speed", value 200, green `#008300`, value label "200 ticks", note "100 slow ticks = 200 tick-equivalents".
- **Labels:** row labels bold `#444` 12px right-aligned left of axis; bold colored value right of each bar; gray note right of the bar, or right-aligned below the bar end when it would run off the canvas (CPU row).
- **Takeaway (bold 14px orange `#d95926`, bottom center):** "the GPU bar is ~625× shorter — even with each lane running at half speed".

## What This Buys a Data Scientist

**Tags:** `where it's used` (blue), `best practice` (green)

- **Training time** — a run that takes 50 hours on a CPU can finish in about an hour on a GPU
- **More experiments** — a 1-hour loop means you can try 8 ideas a day instead of 1 a week
- **Grid math dominates** — in deep nets, roughly 90%+ of compute time is these multiplies
- **Same for predictions** — serving a big model to many users is also batched grid math
- **Not only images** — language models, recommenders, speech: all grid multiplies inside

*Example:* The photo classifier from the batches tutorial: an epoch of 313 steps is 313 rounds of big grid multiplies.

**Key point:** Deep learning became practical when grid math moved to GPUs — the algorithms existed long before the speed did.

### Visualization (canvas `c3`, 720×300)

Two-panel chart split by a dashed divider at x=390: training-hours bars left, compute-share stacked bar right.

- **Title (bold 15px `#1a5276`, top center):** "One Training Run: 50 Hours Becomes About 1".
- **Divider:** vertical dashed `#bdc3c7` line (dash 4/3) at x=390.
- **Left panel (bars 80px wide, baseline y=230, height scale 150px for max 55; minimum visible height 5px):** "CPU" 50 hours in blue `#2a78d6` at x=100; "GPU" 1 hour in green `#008300` at x=235; bold colored value labels "50 hours" / "1 hour" above bars, bold `#222` name labels below; bold orange 12px caption, two lines: "8 experiments a day" / "instead of 1 a week (illustrative)".
- **Right panel (stacked bar at (430,70), 220×150):** top 90% filled `rgba(42,120,214,0.55)` labeled in white bold 14px "grid multiplies: ~90%"; bottom 10% filled `rgba(201,133,0,0.55)` with yellow (`#c98500`) bold label to the right "everything else: ~10%"; navy outline; bold navy 13px header above: "where deep-net compute time goes"; bold magenta 12px caption below, two lines: "speed up the 90% and the" / "whole run flies (illustrative)".

## The Confusion: a GPU Is Not a Faster CPU

**Tags:** `common mistake` (red), `trade-off` (orange)

- **One job at a time** — a single multiplication runs slower on a GPU lane than on a CPU core
- **Chains don't split** — if step 2 needs step 1's answer, 9,999 lanes stand idle
- **Our numbers** — a chain of 1,000,000 dependent steps: CPU 1,000,000 ticks, GPU 2,000,000
- **So pick by shape** — independent grid work goes to GPU; loops, branching logic stay on CPU
- **Everyday data work** — parsing files or cleaning rows with if-else logic rarely gains from a GPU

*Example:* A recipe where each step needs the last pot: hiring 10,000 cooks doesn't help — they queue behind one pot.

**Key point:** A GPU is a crowd, not a sprinter — it only wins when the work splits into thousands of independent pieces.

### Visualization (canvas `c4`, 720×300)

Two-panel diagram split by a dashed divider at x=360: parallel grid left, dependent chain right.

- **Title (bold 15px `#1a5276`, top center):** "Same Million Jobs, Two Shapes: the Crowd Only Helps One".
- **Left panel:** bold green 13px header "independent grid: all at once"; a 5×8 block of small green rectangles (`rgba(0,131,0,0.35)`, 26×18, starting at (70,75), 32/26px pitch); bold green caption "GPU: 1,000,000 ÷ 10,000 lanes → 200 tick-equivalents"; gray 12px "every job starts immediately".
- **Right panel:** bold orange 13px header "dependent chain: one at a time"; a horizontal chain of 6 orange rectangles (`rgba(217,89,38,0.35)`, 30×18 at y≈100, 48px pitch from x=405) connected by orange arrows; gray 12px "step 2 must wait for step 1's answer"; below, a 3×6 block of faint gray rectangles (`rgba(107,114,128,0.18)`) with bold gray (`#6b7280`) label "9,999 lanes idle"; bold orange 13px "GPU: 2,000,000 tick-equivalents — slower than the CPU".
- **Takeaway (bold 13px magenta `#d55181`, bottom center):** "the GPU wins on shape, not speed: split the work or lose the crowd".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (full width, border-collapse) with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%), both 12px padding, top-aligned.
- **Left column structure:** `.tags` row of colored pill spans (0.72rem, 600 weight, 2px 10px padding, 10px radius — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of bullets each opening with `<b>` term in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 8px 12px padding, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** each canvas declared `width="720" height="300"`, CSS `width:100%`, 1px `#e0e0e0` border, 4px radius; a shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** JS object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`. Doc palette anchors: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. All chart data hardcoded (no `Math.random()`).
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
