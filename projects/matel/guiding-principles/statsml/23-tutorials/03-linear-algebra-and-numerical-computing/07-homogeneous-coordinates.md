# Homogeneous Coordinates

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Homogeneous Coordinates

**Subtitle:** One extra coordinate — a plain 1 — turns "slide the object" into a matrix multiplication, so rotate, scale, and move all chain into a single 4×4

## The Slide That Refuses to Multiply

**Tags:** `core idea` (blue), `transforms` (green), `the misfit` (orange)

- **The sprite** — a game triangle sits at corners (1,1), (3,1), (2,3) on the screen grid
- **Scale** — doubling it is a 2×2 matrix multiply: corners become (2,2), (6,2), (4,6)
- **Rotate** — turning 90° is also a 2×2 multiply: (x,y) → (−y,x), so (3,1) lands at (−1,3)
- **Slide** — moving right 4 and up 1 is an ADD: (x+4, y+1) — no 2×2 matrix can do it
- **The itch** — one move out of three breaks the "everything is a multiply" pattern

*Example (italic):* A game engine wants to scale, rotate, and slide a sprite every frame — two of the moves are multiplies, one stubbornly is not.

**Key point:** Rotation and scaling keep the origin pinned, so 2×2 matrices can express them. Translation moves the origin itself — no plain matrix of any size in 2D can.

### Visualization (canvas `c1`, 720×300)

Three-panel grid diagram: the same gray triangle transformed three ways — scaled (multiply), rotated (multiply), and slid (addition, the misfit).

- **Title (bold 15px, `#1a5276`, top center):** "Scale and Rotate Multiply — the Slide Does Not".
- **Data:** original triangle `[[1,1],[3,1],[2,3]]`; scaled `[[2,2],[6,2],[4,6]]`; rotated 90° `[[-1,1],[-1,3],[-3,2]]`; slid `[[5,2],[7,2],[6,4]]`.
- **Panels:** three plot areas 200 wide × 170 tall, baselines at y=240; panel origins x=30, x=270, x=505; each with 1px `#e5e9ef` gridlines and 2px `#999` axes, tick labels 11px `#444`.
- **Panel 1 (scale, x range 0–7, y range 0–7):** original triangle outline `#6b7280` 2px, fill `rgba(107,114,128,0.15)`; scaled triangle blue `#2a78d6` 2px, fill `rgba(42,120,214,0.25)`; caption bold 12px blue below: "scale ×2 = matrix multiply".
- **Panel 2 (rotate, x range −4–4, y range 0–4.5):** original triangle in gray as panel 1; rotated triangle green `#008300` 2px, fill `rgba(0,131,0,0.2)`; caption bold 12px green: "rotate 90° = matrix multiply".
- **Panel 3 (slide, x range 0–8, y range 0–5):** original triangle in gray; slid triangle orange `#d95926` 2px, fill `rgba(217,89,38,0.25)`; dashed orange arrow (dash 4/3) from centroid (2, 1.7) to centroid (6, 2.7); caption bold 12px magenta `#d55181`: "slide (+4,+1) = addition — the odd one out".

## Add a 1, Get a Multiplication

**Tags:** `worked example` (blue), `the trick` (green)

- **The trick** — tack a 1 on: the point (3, 2) is written (3, 2, 1), and matrices grow to 3×3
- **Translation matrix** — rows (1,0,4), (0,1,1), (0,0,1) encode "slide right 4, up 1"
- **The multiply** — row 1 gives 1·3 + 0·2 + 4·1 = 7; row 2 gives 0·3 + 1·2 + 1·1 = 3
- **Read it back** — the result (7, 3, 1) drops its trailing 1 to give the moved point (7, 3)
- **In 3D** — points become (x, y, z, 1) and the same trick makes translation a 4×4 matrix

*Example (italic):* The sprite corner (3, 2) slides to (7, 3) purely by matrix multiplication — the extra 1 smuggles the +4 and +1 into the arithmetic.

**Key point:** The constant 1 lets the matrix's last column act like an addition. Translation becomes multiplication, at the price of one bookkeeping coordinate.

### Visualization (canvas `c2`, 720×300)

Split view: the full 3×3 multiplication written out digit by digit (left), and the before/after point on the screen grid (right).

- **Title (bold 15px, `#1a5276`, top center):** "The 3×3 Trick: (3, 2) Slides to (7, 3) by Multiplication Alone".
- **Left panel (x 30–340):** monospace 14px `#2c3e50` matrix layout — 3×3 matrix rows `[1 0 4]`, `[0 1 1]`, `[0 0 1]` between blue `#2a78d6` 2px bracket strokes centered near x=90, rows at y=90/115/140; the last column digits (4, 1, 1) drawn in orange `#d95926` bold; column vector (3, 2, 1) with brackets near x=195; "=" at x=240; result vector (7, 3, 1) with green `#008300` bold digits near x=285.
- **Arithmetic lines (12px, from y=185):** green bold "row 1: 1·3 + 0·2 + 4·1 = 7"; green bold "row 2: 0·3 + 1·2 + 1·1 = 3"; violet `#4a3aa7` bold 12px annotation at y=235: "last column (4, 1) carries the slide".
- **Right panel (grid origin x=390, width 300, baseline y=230, height 160):** x range 0–8, y range 0–4, gridlines `#e5e9ef`, axes 2px `#999`, integer tick labels 11px `#444`; blue `#2a78d6` 6px dot at (3,2) labeled bold 12px "(3, 2) before" below; green `#008300` 6px dot at (7,3) labeled bold 12px "(7, 3) after" above; dashed green arrow (dash 4/3, 2px) from (3,2) to (7,3).
- **Caption (12px `#444`, bottom of right panel):** "drop the trailing 1 to read the answer".

## One Matrix for the Whole Pipeline

**Tags:** `where it's used` (blue), `chaining` (green), `4×4` (orange)

- **Chaining** — rotate 90° then slide (4,1): the two 3×3 matrices multiply into ONE matrix M
- **Combined** — M has rows (0,−1,4), (1,0,1), (0,0,1); apply it once and both steps happen
- **Check** — M times (3, 2, 1): row 1 gives 0·3 − 1·2 + 4·1 = 2; row 2 gives 1·3 + 0·2 + 1·1 = 4
- **Same answer** — step by step: (3,2) rotates to (−2,3), then slides to (2,4) — it matches
- **The payoff** — a GPU folds a 3D scene's whole move into one 4×4 and reuses it on every point

*Example (italic):* Instead of rotating and then sliding each of a model's 100,000 vertices, the engine multiplies the two matrices once and applies the single result.

**Key point:** Once translation is a multiplication, any sequence of moves collapses into one matrix — that is exactly why graphics hardware is built around 4×4 multiplies.

### Visualization (canvas `c3`, 720×300)

Flow diagram of the two-step path versus the one-matrix shortcut (top), with the combined matrix and a mini grid tracing the point (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "Two Steps, One Matrix: Rotate Then Slide Collapses into M".
- **Flow band (y 50–105):** three rounded boxes (1.5px `#1a5276` border, fill `rgba(26,82,118,0.06)`, bold 13px labels) — "(3, 2)" centered at x=90, "(−2, 3)" at x=330, "(2, 4)" at x=570; solid 2px arrows between boxes labeled bold 12px blue `#2a78d6` "× R (rotate 90°)" and bold 12px green `#008300` "× T (slide 4, 1)".
- **Shortcut arrow:** violet `#4a3aa7` 2px curved arrow beneath the boxes from the "(3, 2)" box to the "(2, 4)" box, dipping to y=145, labeled bold 13px violet "× M = T·R (one matrix, same answer)".
- **Bottom-left (monospace 13px, from y=190):** M rows `[0 −1 4]`, `[1  0 1]`, `[0  0 1]` in violet between bracket strokes near x=80; check line 12px `#444` at y=265: "M·(3,2,1): 0·3 − 1·2 + 4 = 2, 3 + 0 + 1 = 4 — matches".
- **Bottom-right mini grid (origin x=430, width 260, baseline y=265, height 100):** x range −3–4, y range 0–5; dots 5px at (3,2) blue, (−2,3) green, (2,4) violet with 11px coordinate labels; dashed `#999` arrows (3,2)→(−2,3)→(2,4).
- **Caption (bold 12px `#c98500`, bottom center):** "in 3D the same trick is a 4×4 — one M for millions of points".

## Points Move, Directions Don't

**Tags:** `common mistake` (red), `w = 0 vs w = 1` (orange)

- **Two kinds** — a position gets w = 1, like (3, 2, 1); a direction gets w = 0, like (1, 0, 0)
- **Slide a point** — the translation matrix sends (3, 2, 1) to (7, 3, 1): the point moves
- **Slide a direction** — the same matrix sends (1, 0, 0) to (1, 0, 0): the arrow doesn't budge
- **Why that's right** — "facing east" means the same thing before and after you walk 4 blocks
- **The w** — it is a bookkeeping switch, not a hidden third spatial axis of the screen

*Example (italic):* A sprite at (3, 2) facing east slides to (7, 3) still facing east — the position translated, but its facing arrow (1, 0, 0) did not.

**Common mistake:** Giving a direction w = 1 (or a point w = 0). Directions then get dragged along by every translation, and velocity and lighting math silently break.

### Visualization (canvas `c4`, 720×300)

One grid showing the point sliding from (3,2) to (7,3) while its east-facing direction arrow stays identical at both positions.

- **Title (bold 15px, `#1a5276`, top center):** "w = 1 Moves, w = 0 Doesn't: Points vs Directions".
- **Grid (origin x=60, width 600, baseline y=235, height 165):** x range 0–9, y range 0–5, gridlines 1px `#e5e9ef`, axes 2px `#999`, integer tick labels 11px `#444`.
- **Points:** blue `#2a78d6` 7px dot at (3,2) labeled bold 12px "point (3, 2, 1)" below; green `#008300` 7px dot at (7,3) labeled bold 12px "(7, 3, 1) after slide" above; dashed blue arrow (dash 4/3, 2px) from (3,2) to (7,3) labeled bold 12px blue "+ (4, 1)" at its midpoint.
- **Direction arrows:** solid orange `#d95926` 3px arrows of length 1.2 grid units pointing east (+x) from both (3,2) and (7,3), with arrowheads; label bold 12px orange "direction (1, 0, 0)" at the first, "still (1, 0, 0)" at the second.
- **Annotation (bold 13px magenta `#d55181`, upper left of plot):** "the slide terms multiply w — and w = 0 switches them off".
- **Caption (12px `#444`, bottom center):** "row 1 on the direction: 1·1 + 0·0 + 4·0 = 1 — the +4 never lands".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
