# Gradient Descent in Many Dimensions

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Gradient Descent in Many Dimensions

**Subtitle:** "Feel the slope, step downhill, repeat" works everywhere — but in many dimensions the landscape is ruled by ravines and saddle points, not by the deep traps people picture

## A Hiker in the Fog

**Tags:** `core idea` (blue), `gradient` (green), `learning rate` (orange)

- **The fog** — a hiker on a foggy hillside can only feel the slope directly under their feet
- **The rule** — feel the steepest downhill direction, step a fixed fraction of the slope, repeat
- **The start** — on a smooth round valley our hiker begins at map position (2.40, 1.80)
- **The step** — stepping 20% of the slope each time shrinks the position by ×0.6 per step
- **Five steps** — the path runs (2.40, 1.80) → (1.44, 1.08) → ... → (0.19, 0.14), nearly the floor
- **The translation** — each map coordinate is a model parameter and the altitude is the model's error

*Example (italic):* In machine learning the "map" has one axis per parameter — training a small neural net is this same foggy hike in a million dimensions.

**Key point:** Gradient descent is just "feel the slope, step downhill, repeat". Everything interesting about it comes from what the landscape looks like — and high-dimensional landscapes look strange.

### Visualization (canvas `c1`, 720×300)

Contour map of a round valley with the hiker's five-step descent path, plus a step list on the left.

- **Title (bold 15px, `#1a5276`, top center):** "Five Steps Downhill on a Round Valley (contour view)".
- **Data (path, map coords):** `[[2.40,1.80],[1.44,1.08],[0.86,0.65],[0.52,0.39],[0.31,0.23],[0.19,0.14]]`; each point is 0.6× the previous.
- **Contours:** ellipses centered at px (330, 163), for r in `[0.6, 1.2, 1.8, 2.4, 3.0]` draw rx = r×78, ry = r×43, stroke `#c7d3de` 1.25px (light contour lines).
- **Mapping:** px = 330 + x×78, py = 163 + y×43; path points land at (517,240), (442,209), (397,191), (371,180), (354,173), (345,169).
- **Path:** orange `#d95926` 2.5px polyline with 4.5px dots at each point; small orange arrowheads on the first two segments; steps cross the contour lines at right angles.
- **Valley floor:** green `#008300` 5px dot at (330,163) with green bold 12px label "valley floor (0, 0)" just below.
- **Step list (left column, x=20):** heading bold 12px `#1a5276` "the hiker's log:"; six 12px `#444` lines from y=70, 24px apart: "start (2.40, 1.80)", "step 1 (1.44, 1.08)", "step 2 (0.86, 0.65)", "step 3 (0.52, 0.39)", "step 4 (0.31, 0.23)", "step 5 (0.19, 0.14)".
- **Annotation (bold 12px blue `#2a78d6`, near path start):** "each step: keep 60% of the position".
- **Caption (12px `#444`, bottom right):** "step size 20% of the slope — illustrative".

## The Ravine That Makes You Zigzag

**Tags:** `worked example` (blue), `ravines` (orange), `momentum` (green)

- **The ravine** — a valley 16× steeper across than along: sideways slope 16 times the forward slope
- **The zigzag** — the steepest direction points at the wall, so the side offset bounces 1.00 → −0.76 → 0.58
- **The crawl** — meanwhile progress along the floor inches −3.00 → −2.67 → −2.38, about 11% per step
- **The dilemma** — a bigger step launches off the walls and diverges; a smaller step slows the crawl further
- **The fix** — momentum averages recent steps, so opposite sideways bounces cancel and the crawl adds up

*Example (italic):* After ten steps the hiker has spent most of the effort bouncing wall to wall and still has about a third of the valley floor left to cover.

**Key point:** Ravines, not distance, are what make gradient descent slow — one step size must serve the steep sideways direction and the shallow forward direction at the same time.

### Visualization (canvas `c2`, 720×300)

Elongated contour ellipses of a ravine with the ten-step zigzag path bouncing between the walls while crawling toward the floor.

- **Title (bold 15px, `#1a5276`, top center):** "A 16× Ravine: Bouncing Sideways, Crawling Forward".
- **Data (path):** xs `[-3.00, -2.67, -2.38, -2.12, -1.88, -1.68, -1.49, -1.33, -1.18, -1.05]`; ys `[1.00, -0.76, 0.58, -0.44, 0.33, -0.25, 0.19, -0.15, 0.11, -0.09]` (loss = x²/2 + 8y², step size 0.11).
- **Mapping:** px = 600 + x×130, py = 150 − y×85; path px `[210, 253, 291, 324, 356, 382, 406, 427, 447, 464]`, py `[65, 215, 101, 187, 122, 171, 134, 163, 141, 158]`.
- **Contours:** ellipses centered at (600, 150) with (rx, ry) pairs `(130, 32), (260, 64), (390, 96), (520, 128)`, stroke `#c7d3de` 1.25px, clipped at canvas edges.
- **Path:** orange `#d95926` 2.5px polyline with 4px dots; bold 11px `#444` labels "start" at the first point and "step 10" at the last.
- **Valley floor:** green `#008300` 5px dot at (600, 150) labeled "valley floor" in green bold 12px.
- **Annotations:** magenta `#d55181` bold 13px near the first bounces: "bounces wall to wall"; blue `#2a78d6` bold 12px below the path midline: "crawls along the floor ~11% per step".
- **Caption (12px `#444`, bottom left):** "loss = x²/2 + 8y², step size 0.11 — illustrative".

## The Mountain Pass That Feels Like the Bottom

**Tags:** `saddle point` (orange), `core idea` (blue), `escape` (green)

- **The pass** — at a mountain pass the ground is flat: downhill ahead and behind, uphill to both sides
- **The trap** — the hiker feels zero slope underfoot and declares "bottom", but it is not a minimum
- **Two slices** — walking one axis the height reads 6, 3, 2, 3, 6 (a valley); the other reads −2, 1, 2, 1, −2 (a hill)
- **Zero everywhere** — at the center both slices have slope 0, so plain gradient descent stalls there
- **The escape** — any tiny nudge off the exact pass finds the downhill side and the descent resumes

*Example (italic):* Both slices pass through the same point at height 2 with slope 0 — one direction says valley floor, the perpendicular direction says hilltop.

**Key point:** A saddle point is flat without being a bottom. Zero slope only means "no direction is downhill right here", not "you have arrived".

### Visualization (canvas `c3`, 720×300)

Dual-panel slice chart: the same saddle point cut along two perpendicular directions — a valley slice (left) and a hill slice (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One Flat Point, Two Directions: Valley Slice vs Hill Slice".
- **Data:** positions `[-2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2]`; valley slice heights (2 + x²) `[6, 4.25, 3, 2.25, 2, 2.25, 3, 4.25, 6]`; hill slice heights (2 − y²) `[-2, -0.25, 1, 1.75, 2, 1.75, 1, -0.25, -2]`.
- **Shared y scale:** −2 to 6 maps to py 250 (bottom) up to 50 (top), 25px per unit.
- **Left panel (valley slice):** axis origin x=70, width 250; blue `#2a78d6` 3px curve with 4px dots; ink `#1a5276` 6px dot at the center point (height 2); dashed `#bdc3c7` horizontal tangent 60px wide through it labeled bold 12px `#444` "slope = 0"; green `#008300` bold 13px annotation "looks like a bottom"; caption 12px `#444` "slice along the valley direction".
- **Right panel (hill slice):** axis origin x=410, width 250; magenta `#d55181` 3px curve with 4px dots; same ink center dot at height 2 with the same dashed tangent and "slope = 0" label; magenta bold 13px annotation "actually a hilltop"; caption "slice along the perpendicular direction".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Takeaway (bold 13px `#1a5276`, bottom center):** "flat both ways, minimum in neither — a saddle point".

## Why Millions of Dimensions Change the Rules

**Tags:** `common mistake` (red), `high dimensions` (blue), `rule of thumb` (green)

- **The coin flip** — at a flat point, treat each direction as curving up or down like an independent coin flip
- **The odds** — all directions curving up in d dimensions is roughly (1/2)^d: 50% at d=1, 25% at d=2
- **The collapse** — the chance falls to 3.1% at d=5, 0.098% at d=10, and 0.0001% at d=20
- **The verdict** — with a million parameters, essentially every flat point is a saddle, not a minimum
- **The surprise** — the local minima that do exist in big networks tend to be nearly as good as the best

*Example (italic):* At d = 20 the coin-flip odds that a flat point is a true minimum are about 1 in a million — and real networks have millions of dimensions, not 20.

**Common mistake:** Picturing a high-dimensional loss surface as a 2D landscape full of deep traps. The real hazards are saddles and ravines that slow the descent, not bad local minima that end it.

### Visualization (canvas `c4`, 720×300)

Bar chart of the coin-flip chance that a flat point is a true minimum as the number of dimensions grows, with the bars visibly collapsing to nothing.

- **Title (bold 15px, `#1a5276`, top center):** "Chance a Flat Point Is a True Minimum (coin-flip heuristic)".
- **Data:** dimensions `[1, 2, 3, 5, 10, 20]`; chance (%) `[50, 25, 12.5, 3.1, 0.098, 0.0001]`, i.e. (1/2)^d.
- **Layout:** axis origin x=70, baseline y=240, chart height 180 (50% = 180px, so heights `[180, 90, 45, 11, 2, 2]` px with a 2px minimum so tiny bars stay visible); six bars 60px wide, 40px gaps, starting at x=90.
- **Bars:** first three fill `rgba(42,120,214,0.5)` (blue), last three fill `rgba(213,81,129,0.5)` (magenta) to mark the collapse.
- **Value labels:** bold 12px above each bar in the bar's color: "50%", "25%", "12.5%", "3.1%", "0.098%", "0.0001%".
- **X labels:** 12px `#444` below baseline: "d = 1", "d = 2", "d = 3", "d = 5", "d = 10", "d = 20".
- **Annotation (bold 13px magenta `#d55181`, upper right):** "at d = 20: about 1 flat point in a million is a minimum".
- **Caption (12px `#444`, bottom center):** "each direction curves up or down with equal chance — illustrative heuristic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
