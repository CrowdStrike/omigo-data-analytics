# Matrix Multiplication

**Page type:** detail page (tutorial layout: 4 card-sections, each h2 + two-column table.layout with text left 50% / canvas right 50%)
**HTML title tag:** Matrix Multiplication

**Subtitle:** Chaining two transformations into one — why row-times-column is the rule and why order matters

## Two Photo Edits, Saved as One Macro

**Tags:** `core idea` (blue), `running example` (green), `composition` (blue)

- **The edits** — rotate 90°: R = [[0, −1], [1, 0]]; then widen ×2: S = [[2, 0], [0, 1]]
- **Point by point** — corner (3, 2) rotates to (−2, 3), then stretches to (−4, 3)
- **The macro** — one matrix M = S·R = [[0, −2], [1, 0]] does both edits in a single step
- **Check it** — M applied to (3, 2): (0×3 − 2×2, 1×3 + 0×2) = (−4, 3): same landing spot
- **That IS multiplication** — the product of two matrices is the do-both-edits machine

*Example:* A photo editor "saves the two-step edit as one action" — matrix multiplication is that save button.

**Key point:** Multiplying matrices means chaining transformations: S·R is "apply R first, then S", collapsed into one matrix — read right to left.

### Visualization (canvas `c1`, 720×300)

Flow diagram: two-step pipeline (top row) vs one combined macro (bottom row), tracking point (3, 2).

- **Title (bold 15px, `#1a5276`, top center):** "Two Steps or One Macro: (3, 2) Lands at (−4, 3) Either Way".
- **Top row (y=62, box height 56):** boxes connected by gray (`#6b7280`) flow arrows:
  - Box "point / (3, 2)" at x=40, 92px wide — stroke blue `#2a78d6`, fill `rgba(42,120,214,0.08)`.
  - Box "R: rotate 90° / [[0,−1],[1,0]]" at x=172, 128px wide — violet `#4a3aa7`, fill `rgba(74,58,167,0.08)`.
  - Intermediate label "(−2, 3)" in bold violet above the arrow between R and S (x≈320).
  - Box "S: widen ×2 / [[2,0],[0,1]]" at x=340, 128px wide — orange `#d95926`, fill `rgba(217,89,38,0.08)`.
  - Box "result / (−4, 3)" at x=508, 100px wide — green `#008300`, fill `rgba(0,131,0,0.08)`.
  - Caption below row (gray `#444`, 12px): "two passes over every point".
- **Divider:** centered bold text "S·R means R acts first — read right to left" in `#1a5276` at y=160, above a dashed gray line (`#bdc3c7`, dash 4/3) from x=40 to x=680 at y=170.
- **Bottom row (y=190):** box "point / (3, 2)" (blue, x=40) → box "M = S·R / [[0,−2],[1,0]]" at x=236, 168px wide — magenta `#d55181`, fill `rgba(213,81,129,0.08)` → box "result / (−4, 3)" (green, x=508).
  - Caption below in bold magenta 13px: "one pass: (0×3 − 2×2, 1×3 + 0×2) = (−4, 3)".

## Row Times Column: Build S·R Entry by Entry

**Tags:** `worked example` (green), `row times column` (blue)

- **The rule** — entry (row i, col j) of S·R = (row i of S) · (column j of R): multiply, add
- **Top-left** — [2, 0] · [0, 1] = 2×0 + 0×1 = 0
- **Top-right** — [2, 0] · [−1, 0] = 2×(−1) + 0×0 = −2
- **Bottom row** — [0, 1] · [0, 1] = 1 and [0, 1] · [−1, 0] = 0, so M = [[0, −2], [1, 0]]
- **Why that rule** — each column of R says where a unit arrow went; each row of S re-mixes it

*Example:* Four little dot products fill the four slots — the same multiply-and-add recipe as matrix-times-point.

**Key point:** Row-times-column is not an arbitrary convention — it is exactly the arithmetic that makes M·point agree with "R first, then S" for every point.

### Visualization (canvas `c2`, 720×300)

Matrix equation diagram: three 2×2 grids showing S × R = M with row/column highlights, plus a side list of all four dot products.

- **Title (bold 15px, `#1a5276`, top center):** "Each Slot of the Macro Is One Row-Times-Column".
- **Three 2×2 grids** at y=70 (cell 44×36, values bold 16px `#333`, outer border `#1a5276` 1.5px, label bold 12px `#1a5276` below each grid):
  - S = [['2','0'],['0','1']] at x=70, label "S (second edit)", row 1 highlighted with orange `#d95926` at 18% alpha.
  - "×" between (bold 18px `#1a5276` at x=185).
  - R = [['0','−1'],['1','0']] at x=212, label "R (first edit)", column 2 highlighted with violet `#4a3aa7` at 18% alpha.
  - "=" at x=330.
  - M = [['0','−2'],['1','0']] at x=358, label "M = S·R", row 1 highlighted with magenta `#d55181` at 18% alpha.
- **Below grids (bold magenta 13px, centered at x=258):** "highlighted: row 1 of S × col 2 of R" / "= 2×(−1) + 0×0 = −2".
- **Right column (x=520, left-aligned):** heading "all four slots:" (bold 13px `#1a5276`), then lines: "[2,0]·[0,1] = 0" (`#333`), "[2,0]·[−1,0] = −2" (bold magenta), "[0,1]·[0,1] = 1" (`#333`), "[0,1]·[−1,0] = 0" (`#333`); then in bold green 12px `#008300`: "check: M·(3,2) = (−4,3)" / "= two-step answer ✓".
- **Bottom caption (bold violet 13px, centered):** "same multiply-and-add as matrix-times-point — done once per slot".

## Why Every ML Pipeline Is a Product of Matrices

**Tags:** `where it's used` (blue), `composition` (blue)

- **Neural nets** — every layer is a matrix; a 3-layer net is W3·W2·W1 plus squashes between
- **Preprocessing** — standardize (stretch) then PCA (rotate) is one product you could precompute
- **Speed** — collapsing a chain into one matrix turns many passes over data into a single pass
- **GPUs exist for this** — training and inference are mostly this one operation, repeated
- **Debugging lens** — "what does the whole pipeline do to a row?" = multiply its matrices once

*Example:* In the sketch below, 4 input features pass through a 4→3 matrix then a 3→2 matrix — 12 + 6 weights, one chained map.

**Key point:** Deep learning's core loop is matrix multiplication because stacking layers IS composing transformations — the chain rule of edits.

### Visualization (canvas `c3`, 720×300)

Neural network schematic: three node columns fully connected by edges.

- **Title (bold 15px, `#1a5276`, top center):** "A Neural Net Is a Chain of Matrix Multiplications (schematic)".
- **Node columns:** 4 nodes at x=140 (blue `#2a78d6`), 3 nodes at x=360 (aqua `#199e70`), 2 nodes at x=580 (green `#008300`); nodes are 11px-radius filled circles, vertically centered around y=150 with 44px spacing.
- **Column labels (gray `#444` 12px at y=262):** "4 input features", "3 hidden values", "2 outputs".
- **Edges:** every node in a column connected to every node in the next, thin light gray `#c5cdd6` 1px lines.
- **Weight-matrix labels:** bold 13px, centered between columns at y=56/74 — violet `#4a3aa7`: "W1: a 3×4 matrix" / "(12 weights)" at x=250; orange `#d95926`: "W2: a 2×3 matrix" / "(6 weights)" at x=470.
- **Bottom caption (bold magenta 13px, centered):** "output = W2 · (W1 · row) — two chained edits on every data row".

## The Order Trap: Rotate-Then-Stretch ≠ Stretch-Then-Rotate

**Tags:** `common mistake` (red), `order matters` (orange)

- **Path 1** — rotate then stretch: (1, 0) → (0, 1) → (0, 1): the stretch finds x = 0 to double
- **Path 2** — stretch then rotate: (1, 0) → (2, 0) → (0, 2): the doubled x gets turned upward
- **Different machines** — S·R = [[0, −2], [1, 0]] but R·S = [[0, −1], [2, 0]]: not equal
- **Unlike numbers** — 3×5 = 5×3 always, but A·B = B·A only by lucky accident
- **Read right to left** — in S·R the nearest matrix to the point (R) acts first

*Example:* Scaling features before PCA versus after PCA gives genuinely different components — same trap, real pipeline.

**Common mistake:** Assuming matrix products commute. Swapping the order swaps which edit sees the other's output — the same point can land somewhere else entirely.

### Visualization (canvas `c4`, 720×300)

Coordinate-plane diagram: the point (1, 0) traced through both operation orders, landing at different spots.

- **Title (bold 15px, `#1a5276`, top center):** "Same Two Edits, Swapped Order: (0, 1) vs (0, 2)".
- **Plane:** x range −0.6..2.6, y range −0.3..2.4 mapped to a 400×180 plot area at origin (120, 240); light grid lines `#e5e9ef` at integer x/y 0–2; gray `#999` axes; tick labels "1","2" on x below axis and "1","2" on y left of axis (gray `#444` 12px).
- **Start point:** blue `#2a78d6` 7px dot at (1, 0), labeled "start (1, 0)" below.
- **Path 1 (green `#008300`, 2.5px curved arrows, curve offset −18):** (1, 0) → (0, 1); endpoint dot; label "S·R ends at (0, 1)" bold green 12px near (0, 1).
- **Path 2 (orange `#d95926`, curve offset +22):** (1, 0) → (2, 0) → (0, 2); endpoint dot; label "R·S ends at (0, 2)" bold orange near (0, 2); "via (2, 0)" in plain orange 12px above (2, 0).
- **Legend (left-aligned at x=555):** bold green 12px "rotate, then stretch:" over "S·R = [[0,−2],[1,0]]" (`#333`); bold orange "stretch, then rotate:" over "R·S = [[0,−1],[2,0]]" (`#333`); then bold magenta 12px: "different matrices," / "different landing spots" / "— A·B ≠ B·A in general".

## Regeneration instructions

- **Template:** tutorials topic page (see `tutorials/CLAUDE.md`): `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` row of colored pill spans (0.72rem bold, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`); one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem) starting with `<strong>Key point:</strong>` or `<strong>Common mistake:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius.
- **Canvases:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart JS palette object: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Helper functions: `arrowHead`, `box` (bordered filled rectangle with centered bold 12px text lines), `flowArrow`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
