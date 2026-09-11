# OLS as Projection

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** OLS as Projection

**Subtitle:** OLS doesn't hunt for a line — it drops the outcome vector straight down onto the plane of all possible predictions, and the fitted values are the shadow

## Three Days of Sales as One Arrow

**Tags:** `core idea` (blue), `projection` (green), `fitted values` (orange)

- **The stand** — a lemonade stand logs three days: 1, 2, 3 hours open and $30, $50, $40 in sales
- **One arrow** — stack the sales into a single arrow y = (30, 50, 40) in a space with one axis per day
- **The plane** — every candidate prediction b0 + b1·hours is another arrow; together they tile a flat plane
- **The shadow** — no plane point hits y exactly, so OLS takes the point directly beneath it, like a noon shadow
- **Fitted values** — that shadow is ŷ = (35, 40, 45); the leftover straight-up piece is the residual e

*Example (italic):* Sales (30, 50, 40) can't be produced by any line through hours 1, 2, 3 — the closest plane point is (35, 40, 45).

**Key point:** The fitted values are not a formula trick — they are literally the closest point to y on the plane of everything a line could predict.

### Visualization (canvas `c1`, 720×300)

Schematic 3D projection diagram: a shaded predictor plane, the vector y rising off it, its shadow ŷ on the plane, and a vertical dashed residual joining them at a right angle.

- **Title (bold 15px, `#1a5276`, top center):** "Three Days of Sales: y and Its Shadow on the Predictor Plane".
- **Plane:** parallelogram with corners (120, 240), (450, 240), (560, 155), (230, 155); fill `rgba(42,120,214,0.10)`, 1.5px `#2a78d6` border; italic 12px `#2a78d6` label "plane of all predictions b0 + b1·hours" at (140, 232).
- **Base point:** O = (300, 205), 4px ink `#1a5276` dot on the plane (drawn origin of both arrows).
- **y arrow:** 3px blue `#2a78d6` line with arrowhead from O to (480, 55); bold 13px blue label "y = actual (30, 50, 40)" near the tip.
- **ŷ arrow:** 3px green `#008300` line with arrowhead from O to H = (480, 190); bold 13px green label "ŷ = fitted (35, 40, 45)" below H.
- **Residual:** dashed (4/3) 2.5px magenta `#d55181` vertical line with arrowhead from H up to (480, 55); bold 13px magenta label "e = residual (−5, 10, −5)" right of its midpoint.
- **Right-angle marker:** 10px ink `#1a5276` square at H between the plane and the residual.
- **Annotation (bold 12px ink, two lines, at (60, 62)):** "the shadow is the closest" / "point on the plane".
- **Caption (12px `#6b7280`, bottom left):** "axes = day 1, day 2, day 3 (schematic)".

## Casting the Shadow by Hand

**Tags:** `worked example` (blue), `orthogonality` (green)

- **Slope by hand** — mean hours = 2, mean sales = 40; slope = Σ(x−2)(y−40) / Σ(x−2)² = 10/2 = 5
- **The line** — intercept = 40 − 5×2 = 30, so the fitted line is sales = 30 + 5 × hours
- **Fitted** — plugging in 1, 2, 3 hours gives ŷ = (35, 40, 45) and residuals e = (−5, +10, −5)
- **Right angle, part 1** — residuals sum to zero: −5 + 10 − 5 = 0, so e ⊥ the intercept column
- **Right angle, part 2** — hours·e = 1(−5) + 2(10) + 3(−5) = 0, so e ⊥ the hours column too

*Example (italic):* Day 2 sold $50 but the line says $40 — that +10 miss is exactly cancelled by the two −5 misses on days 1 and 3.

**Key point:** "Residuals sum to zero" and "residuals are uncorrelated with x" are not bonus facts — they ARE the shadow falling straight down at 90°.

### Visualization (canvas `c2`, 720×300)

Dual panel split by a dashed divider at x=360: the familiar scatter-plus-line picture (left) and the same numbers as actual-vs-fitted bars per day (right).

- **Title (bold 15px, `#1a5276`, top center):** "Fitting by Hand: sales = 30 + 5 × hours".
- **Data:** hours `[1, 2, 3]`; actual sales `[30, 50, 40]`; fitted `[35, 40, 45]`; residuals `[-5, 10, -5]`.
- **Left panel (scatter):** axis origin x=55, width 280, baseline y=245, chart height 185; x scale 0–3.5 hours, y scale 25–55; green `#008300` 3px fitted line drawn from (hours 0.8, $34) to (hours 3.2, $46); blue `#2a78d6` 6px dots at the three points; dashed (4/3) 2px magenta `#d55181` vertical segments from each dot to the line, each with a bold 12px magenta label "−5", "+10", "−5"; axis labels "hours" and "sales ($)" 12px `#444`; x ticks 1, 2, 3; y ticks 30, 40, 50.
- **Right panel (bars):** axis origin x=400, width 280, baseline y=245, chart height 185, y scale 0–55; per day a pair of bars 30px wide with 6px gap — actual fill `rgba(42,120,214,0.5)`, fitted fill `rgba(0,131,0,0.35)`; value labels 11px `#444` above each bar; bold 12px magenta residual label "−5", "+10", "−5" centered above each pair; day labels "day 1", "day 2", "day 3" 12px `#444` below baseline; 11px legend top-right (blue square "actual", green square "fitted").
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Caption (12px `#6b7280`, under right panel):** "residuals sum to 0 and cancel against hours".

## Pythagoras Gives You R²

**Tags:** `where it's used` (blue), `R squared` (green), `anova identity` (orange)

- **Three lengths** — squared distances: total Σ(y−40)² = 200, explained Σ(ŷ−40)² = 50, residual Σe² = 150
- **Right triangle** — because e is perpendicular to the plane, Pythagoras applies: 50 + 150 = 200
- **R² for free** — R² = explained / total = 50/200 = 0.25, the squared cosine of the angle at ȳ
- **Never off** — explained + residual = total holds exactly for any data, purely from the geometry
- **Best possible** — no line beats RSS = 150; a shorter residual would mean a closer plane point

*Example (italic):* The stand's line explains 25% of the sales variance — the arrow y sits at a steep 60° angle to the plane.

**Key point:** The ANOVA identity TSS = ESS + RSS is Pythagoras on the projection triangle — that is why R² with an intercept can never leave [0, 1].

### Visualization (canvas `c3`, 720×300)

A right triangle drawn to scale from the three squared lengths, next to a stacked bar splitting the total 200 into explained and residual parts.

- **Title (bold 15px, `#1a5276`, top center):** "Pythagoras on the Projection Triangle: 200 = 50 + 150".
- **Triangle (side lengths = √value × 12):** vertices B = (150, 250), A = (235, 250), T = (235, 103); base B→A green `#008300` 3px (length 85 ≈ √50×12), vertical A→T magenta `#d55181` 3px (length 147 ≈ √150×12), hypotenuse B→T blue `#2a78d6` 3px (length 170 ≈ √200×12).
- **Side labels:** bold 12px green "ŷ − ȳ, length² = 50 (explained)" below the base; bold 12px magenta "e, length² = 150 (residual)" right of the vertical leg; bold 12px blue "y − ȳ, length² = 200 (total)" left of the hypotenuse.
- **Right-angle marker:** 10px ink `#1a5276` square at A.
- **Angle annotation:** small arc at B, bold 12px ink label "θ = 60°, cos²θ = R²".
- **Stacked bar (right side):** heading bold 12px `#444` "total 200 split by the right angle" at (420, 118); bar from x=420, width 260, y=140, 26px tall — green `rgba(0,131,0,0.4)` segment 65px labeled "explained 50" (11px), magenta `rgba(213,81,129,0.4)` segment 195px labeled "residual 150" (11px); below it bold 13px ink "R² = 50 / 200 = 0.25" at (420, 196).
- **Caption (12px `#6b7280`, bottom left):** "distances measured in day-space, mean ȳ = 40 subtracted first".

## Perpendicular Where, Exactly?

**Tags:** `common mistake` (red), `total least squares` (orange)

- **Two spaces** — the scatter has axes (hours, sales); the projection picture has one axis per day
- **Vertical drops** — in the scatter, OLS residuals −5, +10, −5 are vertical gaps, never tilted ones
- **The imposter** — dropping residuals perpendicular to the line is total least squares, a different fit
- **Where ⊥ lives** — OLS's right angle is between e and the predictor columns in day-space, off-screen
- **Scale trap** — perpendicular-in-the-scatter changes if sales switch to cents; OLS rescales cleanly

*Example (italic):* Rescale sales from dollars to cents and the perpendicular-drop fit tilts to a new line; the vertical-drop fit just rescales.

**Common mistake:** Saying "OLS minimizes perpendicular distance to the line." The right angle is real, but it lives in day-space between e and the predictors — in the scatter plot, OLS gaps are strictly vertical.

### Visualization (canvas `c4`, 720×300)

Dual panel split by a dashed divider at x=360: the same three points and fitted line twice — vertical residual drops (left, OLS) vs perpendicular drops to the line (right, not OLS).

- **Title (bold 15px, `#1a5276`, top center):** "Vertical Drops (OLS) vs Perpendicular Drops (Not OLS)".
- **Shared data (both panels):** points (1, $30), (2, $50), (3, $40); green `#008300` 3px line sales = 30 + 5×hours drawn from (hours 0.8, $34) to (hours 3.2, $46); blue `#2a78d6` 6px dots; x scale 0–3.5, y scale 25–55, baseline y=245, chart height 185; x ticks 1, 2, 3; y ticks 30, 40, 50.
- **Left panel:** axis origin x=55, width 280; dashed (4/3) 2px magenta `#d55181` vertical segments from each dot to the line labeled bold 12px magenta "−5", "+10", "−5"; annotation bold 12px green "OLS: gaps measured straight down"; caption 12px `#444` "what OLS minimizes: 25 + 100 + 25 = 150".
- **Right panel:** axis origin x=400, width 280; from each dot a dashed (4/3) 2px orange `#d95926` segment meeting the green line at 90° (foot of perpendicular computed geometrically in pixel space), with a 6px orange right-angle marker at each foot; annotation bold 12px orange "perpendicular-to-line = total least squares"; second annotation bold 12px red `#e74c3c` "not what OLS does"; caption 12px `#444` "this distance changes if you rescale sales".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
