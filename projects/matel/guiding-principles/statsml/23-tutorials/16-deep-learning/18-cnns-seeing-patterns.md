# CNNs: Seeing Patterns

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** CNNs: Seeing Patterns

**Subtitle:** A network that slides small pattern detectors across an image — spotting edges first, then strokes, then whole objects

## A Tiny Stencil Slides Across a Handwritten 7

**Tags:** `core idea` (blue), `running example` (green), `images` (orange)

- **The task** — read a handwritten digit on an envelope: is this scribble a 7?
- **The stencil** — a 3×3 grid of weights, called a filter, tuned to one tiny pattern
- **The slide** — the same filter visits every 3×3 patch of the image, left to right, top to bottom
- **The score** — at each stop it outputs one number: big where its pattern appears, small elsewhere
- **The map** — the scores form a new grid showing WHERE the pattern lives in the image

*Example (italic):* A stencil shaped like a diagonal stroke lights up exactly along the slanted arm of the 7.

**Key point:** A convolution is one small pattern detector reused at every position — the output map says where the pattern was found.

### Visualization (canvas `c1`, 720×300)

Diagram: a 10×10 pixel grid of a handwritten "7", two highlighted 3×3 sliding windows, an arrow to an 8×8 response map.

- **Title (bold 15px, `#1a5276`, top center):** "The Same 3×3 Filter Visits Every Patch of the Digit".
- **Digit grid:** 10×10 cells, 21px each, starting at (60, 48). Ink cells (row, col) of the "7": `[1,1],[1,2],[1,3],[1,4],[1,5],[1,6],[1,7],[1,8],[2,7],[3,6],[4,6],[5,5],[6,5],[7,4],[8,4]` filled `#1a5276`; all other cells `#fafbfc`; cell borders `#e5e9ef` 1px.
- **Sliding windows:** two 3×3 outlines drawn 3px wide — orange `#d95926` at grid position (row 0, col 1) on the top bar; aqua `#199e70` at (row 6, col 7) on blank paper.
- **Grid caption (12px `#2c3e50`, centered below):** "a handwritten 7 (10×10 pixels)".
- **Arrow:** horizontal gray `#6b7280` arrow (line + filled triangle head) at y=150 from right of the digit grid to the response map; labels above/below the arrow (12px, centered): "slide +" and "score".
- **Response map:** 8×8 grid, 21px cells, to the right of the arrow. Each map cell fires based on how many ink pixels lie inside its 3×3 window: alpha 0.03 if 0 ink cells, 0.35 if 1–2, 0.85 if ≥3; fill `rgba(217,89,38,alpha)`; borders `#e5e9ef`.
- **Map caption (12px `#2c3e50`, two lines below map):** "response map: where the" / "pattern was found (schematic)".
- **Annotation (bold 13px magenta `#d55181`, left-aligned, right of the map, three lines):** "one stencil," / "64 stops," / "one map".

## One Filter Stop, Multiplied Out by Hand

**Tags:** `worked example` (green), `by hand` (blue)

- **The patch** — a 5×5 corner of the image: ink = 1, paper = 0, ink fills the left 3 columns
- **The filter** — columns of weights 1, 0, −1: it computes left column minus right column
- **One stop** — lay the filter on a patch, multiply the 9 pairs, add them up: one number
- **Middle stop** — left column ink (1+1+1 = 3), right column paper (0), so 3 − 0 = 3
- **First stop** — ink on both sides: 3 − 3 = 0; no edge, no response
- **Result map** — every row reads 0, 3, 3: the filter fires only at the ink-to-paper edge

*Example (italic):* Nine multiplications and one sum per stop — you can redo the whole 3×3 output map on paper.

**Key point:** "Big where left and right differ, zero where they match" — that is all a vertical-edge filter computes.

### Visualization (canvas `c2`, 720×300)

Worked-convolution diagram: patch grid ⊗ filter grid = output grid, with hand calculations at right.

- **Title (bold 15px `#1a5276`, top center):** "Patch × Filter = Output Map, Every Number Checkable".
- **Image patch:** 5×5 grid at (55, 62), 34px cells, values `[[1,1,1,0,0],[1,1,1,0,0],[1,1,1,0,0],[1,1,1,0,0],[1,1,1,0,0]]`; cells with value 1 filled `rgba(42,120,214,0.30)`, value 0 filled `#fafbfc`; each cell shows its number (bold 13px `#2c3e50` centered); borders `#e5e9ef`. Labels below: "image patch" (bold 12px `#2a78d6`) and "ink = 1, paper = 0" (12px `#6b7280`).
- **Operator:** "⊗" (bold 20px `#2c3e50`) at (268, 155).
- **Filter:** 3×3 grid at (300, 96), 34px cells, values `[[1,0,-1],[1,0,-1],[1,0,-1]]`; value 1 filled `rgba(0,131,0,0.25)`, value −1 filled `rgba(217,89,38,0.25)`, value 0 `#fafbfc`; numbers in each cell. Label below: "filter: left − right" (bold 12px `#008300`).
- **Operator:** "=" (bold 20px `#2c3e50`) at (438, 155).
- **Output map:** 3×3 grid at (468, 96), 34px cells, values `[[0,3,3],[0,3,3],[0,3,3]]`; value 3 filled `rgba(217,89,38,0.55)`, value 0 `#fafbfc`; numbers in each cell. Label below: "output map" (bold 12px `#d95926`).
- **Side annotations (bold 13px, left-aligned at x=596):** in magenta `#d55181`: "middle stop:" / "(1+1+1) − 0 = 3" / "first stop:" / "3 − 3 = 0"; then in orange `#d95926`: "3 marks the" / "ink-to-paper" / "edge".
- **Caption (12px `#6b7280`, bottom center):** "each output cell: lay the 3×3 filter on a 3×3 patch, multiply the 9 pairs, add".

## Edges First, Strokes Later, the Digit Last

**Tags:** `where it's used` (blue), `why it works` (green)

- **Stacking** — layer 1 finds edges; layer 2 combines edges into corners and short strokes
- **Going up** — layer 3 combines strokes into parts: a top bar, a slanted arm
- **The verdict** — the last layer sees "top bar + slanted arm" and scores the digit 7 highest
- **Anywhere** — because the filter slides, a 7 in the corner is found as easily as one centered
- **Beyond digits** — the same recipe reads photos, X-rays, satellite images, defect scans

*Example (italic):* No one programs "what a 7 looks like" — it emerges from edge maps feeding stroke maps feeding part maps.

**Key point:** Depth builds vocabulary: each layer describes the image in bigger pieces than the layer below it.

### Visualization (canvas `c3`, 720×300)

Pipeline diagram: four boxes connected by arrows, showing the feature hierarchy from edges to the digit.

- **Title (bold 15px `#1a5276`, top center):** "Each Layer Sees Bigger Pieces Than the One Below".
- **Boxes:** four 140×130 boxes starting at x=42, y=70, 40px gaps, fill `#fafbfc`, 2.5px colored borders. Labels below each box (bold 13px in the stage color, then 12px `#2c3e50`):
  1. "layer 1" / "edges" — blue `#2a78d6`; contents: four short scattered stroke segments (4px round-cap lines).
  2. "layer 2" / "corners, strokes" — aqua `#199e70`; contents: one right-angle corner and one slanted stroke.
  3. "layer 3" / "parts: bar, arm" — violet `#4a3aa7`; contents: a horizontal top bar and a separate slanted arm.
  4. "output" / "digit: 7" — orange `#d95926`; contents: the assembled "7" (top bar joined to slanted arm).
- **Arrows:** gray `#6b7280` arrows between consecutive boxes at mid-height.
- **Bottom annotations (centered):** bold 13px magenta `#d55181`: ""top bar + slanted arm, no bottom loop" → the 7 score wins"; then 12px `#6b7280`: "same idea scales from digits to faces, tumors, and street signs".

## The Confusion: Nobody Designs the Filters

**Tags:** `common mistake` (red), `weight sharing` (orange)

- **The mistake** — thinking engineers hand-draw each filter, like the edge stencil above
- **Reality** — the 9 weights start random and are learned from data, like any other weights
- **Edge filters emerge** — trained networks rediscover edge detectors on their own, unprompted
- **One filter, one pattern** — 9 weights + 1 bias = 10 numbers, reused at every image position
- **The payoff** — 32 filters cost 320 weights; a dense layer on 28×28 into 100 neurons costs 78,400

*Example (italic):* The hand-made edge filter here is a teaching prop — real CNNs learn theirs, and they look eerily similar.

**Key point:** Reusing one small learned filter everywhere is why CNNs need far fewer weights than dense layers — not because someone drew clever stencils.

### Visualization (canvas `c4`, 720×300)

Two-bar comparison of weight counts, dense vs convolutional.

- **Title (bold 15px `#1a5276`, top center):** "Weights Needed on a 28×28 Digit Image".
- **Bar 1 (dense):** at (90, 76), 480×44px, fill `rgba(42,120,214,0.35)`, stroke `#2a78d6` 2px; inside label (bold 13px `#1a5276`, left-aligned): "dense layer: 784 pixels × 100 neurons = 78,400 weights".
- **Bar 2 (conv):** at (90, 160), 6×44px (proportionally tiny), fill `rgba(0,131,0,0.4)`, stroke `#008300` 2px; label beside it (bold 13px `#008300`): "conv layer: 32 filters × (9 weights + 1 bias) = 320 weights".
- **Bottom annotations (centered):** bold 14px magenta `#d55181`: "245× fewer weights — because each filter is reused at every position"; then 12px `#6b7280`: "and all of them start random: training, not an engineer, shapes the stencils".

## Regeneration instructions

- **Template/layout:** tutorials topic-page skeleton. `<h1>` (no index number) + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%), both 12px padding, top-aligned.
- **Left column structure:** `.tags` row of colored pill spans (`.tag` — 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) whose `<strong>` prefix is "Key point:".
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions (this page has none).
