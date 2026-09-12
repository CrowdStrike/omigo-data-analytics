# Convolution

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Convolution

**Subtitle:** A tiny window of weights slides across the data and scores every position for one pattern — the network learns the weights, and the sliding finds the pattern anywhere

## A Jump Detector for Daily Sales

**Tags:** `core idea` (blue), `sliding window` (green), `filter` (orange)

- **The shop** — a store logs ten days of sales: 10, 12, 11, 12, 40, 42, 41, 43, 42, 44
- **The stencil** — a tiny 3-day pattern of weights [−1, 0, +1] describes what to look for
- **The slide** — the stencil moves one day at a time, scoring every 3-day stretch it covers
- **The score** — multiply each day by its weight and add; a big score means "pattern found here"
- **The result** — scores 1, 0, 29, 30, 1, 1, 1, 1 spike exactly where sales jumped, days 4 to 5

*Example (italic):* Sliding the [−1, 0, +1] stencil over the sales row scores 29 and 30 right at the day-4-to-5 jump, and near 0 everywhere else.

**Key point:** A convolution is one small set of weights slid across the data, producing a score for every position — a pattern detector on wheels.

### Visualization (canvas `c1`, 720×300)

Two vertically stacked, x-aligned panels: the ten-day sales bars on top, the eight convolution scores below, with the 3-day window highlighted on the sales row.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Days of Sales, One Sliding 3-Day Detector".
- **Data:** sales `[10, 12, 11, 12, 40, 42, 41, 43, 42, 44]` for days 1–10; kernel `[-1, 0, 1]`; scores `[1, 0, 29, 30, 1, 1, 1, 1]` for windows centered on days 2–9.
- **Top panel (sales):** origin x=60, width 620 (10 slots of 62px), baseline y=130, chart height 85, y scale 0–50; bars 40px wide centered per slot, fill `rgba(42,120,214,0.45)`, 1px `#2a78d6` stroke; 11px `#444` value labels above each bar.
- **Window highlight:** orange `#d95926` dashed (dash 5/3) 2px rectangle around the day 3–5 bars, from y=52 to y=134; bold 12px orange label above it: "3-day window × [−1, 0, +1]".
- **Bottom panel (scores):** baseline y=252, chart height 82 (tops from y=170), y scale 0–35; green bars fill `rgba(0,131,0,0.4)`, 1px `#008300` stroke, 40px wide, each centered under its window's middle day (days 2–9); day labels 1–10 in 12px `#444` below the baseline.
- **Annotation:** bold 13px green `#008300` near the two tall bars: "fires here: 29, 30 — the jump".
- **Caption (12px `#444`, bottom center):** "score = (−1)·first day + (0)·middle day + (+1)·last day of each window".

## Multiply, Add, Slide One Step

**Tags:** `worked example` (blue), `multiply & add` (green)

- **Window 1** — days 1–3 give (−1)(10) + (0)(12) + (1)(11) = 1, a "nothing here" score
- **Window 3** — days 3–5 give (−1)(11) + (0)(12) + (1)(40) = 29, and the detector fires
- **Window 5** — days 5–7 give (−1)(40) + (0)(42) + (1)(41) = 1, quiet again after the jump
- **One step** — each slide moves one day, so 10 days and a width-3 window give 8 scores
- **By hand** — three multiplications and two additions per window; all 8 scores are redoable

*Example (italic):* Window 3 covers sales 11, 12, 40; the weights −1, 0, +1 turn "low then high" into the big score 29.

**Key point:** Every output number is one multiply-and-add of the same 3 weights against one window of the data — nothing more.

### Visualization (canvas `c2`, 720×300)

Three horizontal computation strips, one per window position, each showing the sales values, the weights under them, the written-out arithmetic, and the resulting score box.

- **Title (bold 15px, `#1a5276`, top center):** "Multiply and Add: Three Window Positions by Hand".
- **Data (one strip per row):** window 1 — values `[10, 12, 11]`, arithmetic "(−1)(10) + (0)(12) + (1)(11)", score 1; window 3 — values `[11, 12, 40]`, arithmetic "(−1)(11) + (0)(12) + (1)(40)", score 29; window 5 — values `[40, 42, 41]`, arithmetic "(−1)(40) + (0)(42) + (1)(41)", score 1.
- **Strip rows:** centered at y=85, 160, 235; left label bold 12px `#1a5276` at x=20: "window 1 (days 1–3)", "window 3 (days 3–5)", "window 5 (days 5–7)".
- **Value boxes:** three 46×30 boxes starting x=170, gap 10; fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border, value bold 13px `#1a5276` centered; weight labels "×−1", "×0", "×+1" in bold 11px orange `#d95926` directly below each box.
- **Arithmetic text:** 12px `#444` at x=350 (vertically centered per strip), the arithmetic string for that window.
- **Score boxes:** 54×34 at x=610; window 3's box fill `rgba(0,131,0,0.5)` with white bold 15px "29"; windows 1 and 5 fill `#eef2f7`, 1px `#bdc3c7` border, `#444` bold 13px "1".
- **Annotation:** bold 12px magenta `#d55181` to the right of window 3's strip label area (near its score box, above it): "detector fires".
- **Caption (12px `#444`, bottom center):** "same 3 weights every time — only the window of data changes".

## From a Sales Row to a Photo

**Tags:** `where it's used` (blue), `images` (orange), `learned weights` (green)

- **Pictures** — an image is a grid of brightness numbers; here a 6×6 patch, dark 0s left, bright 9s right
- **2D stencil** — a 3×3 kernel with columns −1, 0, +1 slides across every position of the grid
- **Edge found** — the 4×4 output reads 27 along the dark-to-bright boundary and 0 on the flat halves
- **Learned** — a CNN starts these 9 weights random; gradient descent tunes them into a detector
- **Stacked** — early layers learn edges, later layers combine them into corners, textures, objects

*Example (italic):* The same 9-weight kernel scores 27 wherever dark meets bright — nobody hand-codes "edge"; the network learns weights that act like one.

**Key point:** In a CNN the sliding is fixed but the weights are parameters — the network learns what each window should look for.

### Visualization (canvas `c3`, 720×300)

Three-grid diagram: 6×6 input image, ∗ symbol, 3×3 kernel, = symbol, 4×4 output map, with the edge column lit up in the output.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Idea in 2D: a 3×3 Edge Kernel on a 6×6 Image".
- **Input grid:** every row is `[0, 0, 0, 9, 9, 9]`; 6×6 cells of 30px starting at x=55, y=70; cells with 0 fill `#eef2f7`, cells with 9 fill `rgba(42,120,214,0.55)`; values 11px centered, `#444` on light cells, white on blue cells; label bold 12px `#444` below: "input: dark | bright".
- **Kernel grid:** every row is `[-1, 0, 1]`; 3×3 cells of 32px starting at x=300, y=115; fill `rgba(217,89,38,0.12)`, 1.5px `#d95926` borders, values bold 12px `#d95926` centered; label bold 12px `#444` below: "3×3 kernel".
- **Output grid:** every row is `[0, 27, 27, 0]`; 4×4 cells of 34px starting at x=475, y=100; cells with 27 fill `rgba(0,131,0,0.5)` with white bold 12px "27", cells with 0 fill `#eef2f7` with `#444` 12px "0"; label bold 12px `#444` below: "output: edge map".
- **Operators:** bold 20px `#1a5276` "∗" centered between input and kernel (x≈272), "=" between kernel and output (x≈445), both at y≈165.
- **Annotation:** bold 13px green `#008300` under the output grid label: "27 lights up along the dark→bright edge".
- **Caption (12px `#444`, bottom center):** "in a CNN these 9 weights start random and are learned from data".

## The Detector Doesn't Care Where

**Tags:** `common mistake` (red), `weight sharing` (green)

- **The move** — shift the jump from day 5 to day 7: sales 10, 11, 12, 11, 12, 11, 40, 42, 41, 43
- **Same weights** — the identical [−1, 0, +1] detector now scores 28 and 31 at the new spot
- **No retraining** — the kernel never stored "day 5"; it stores the shape of a jump, not its place
- **Weight sharing** — one detector reused at 8 positions costs 3 weights, not 8 × 3 = 24
- **The trap** — assuming a network needs a separate detector per location; convolution reuses one

*Example (italic):* A dense layer that matched the jump at day 5 knows nothing about day 7; the convolution finds the moved jump with the same 3 weights.

**Common mistake:** Thinking convolution memorizes where a pattern appeared in training. It learns the pattern's shape; the sliding finds it at any position.

### Visualization (canvas `c4`, 720×300)

Two stacked, x-aligned panels, each pairing a thin sales line with green score bars, showing the score spike tracking the jump as it moves from day 5 to day 7.

- **Title (bold 15px, `#1a5276`, top center):** "Move the Jump — the Same 3 Weights Still Find It".
- **Data:** panel A sales `[10, 12, 11, 12, 40, 42, 41, 43, 42, 44]` with scores `[1, 0, 29, 30, 1, 1, 1, 1]`; panel B sales `[10, 11, 12, 11, 12, 11, 40, 42, 41, 43]` with scores `[2, 0, 0, 0, 28, 31, 1, 1]`; scores centered on days 2–9; both series scaled y 0–50 (sales) and 0–35 (score bars).
- **Shared x layout:** 10 slots of 62px starting x=60; day labels 1–10 in 12px `#444` below the bottom panel only.
- **Panel A (top):** baseline y=130, height 80; blue `#2a78d6` 2px sales line with 3px dots; green score bars fill `rgba(0,131,0,0.4)`, 26px wide, drawn up from the baseline on the 0–35 scale; label bold 12px `#444` at left (x=60, above panel): "jump at day 5"; bold 12px green labels "29 30" above the two tall bars.
- **Panel B (bottom):** baseline y=252, height 80, same construction; label "jump moved to day 7"; bold 12px green labels "28 31" above its two tall bars.
- **Annotation:** bold 12px orange `#d95926` in panel B, right of the spike: "spike moves with the jump".
- **Takeaway (bold 13px magenta `#d55181`, bottom center, y=292):** "one pattern, 3 shared weights, found at any position".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All chart data is hardcoded literal arrays (no `Math.random()`); the convolution scores are exact computations from the given signals and kernel.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
