# Activation Functions

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks, each h2 + two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Activation Functions

**Subtitle:** Without a bend between layers, ten stacked layers collapse into one straight line — the activation function is the bend that lets a network draw curves

## Ten Stacked Layers That Collapse Into One Line

**Tags:** `core idea` (blue), `running example` (green)

- **A bare layer is a line** — multiply by a weight, add a bias: layer 1 says y = 2x + 1
- **Stack another** — layer 2 says z = 3y − 2; feed one into the other
- **Do the algebra** — z = 3(2x + 1) − 2 = 6x + 1: still just one straight line
- **Ten layers, same story** — lines of lines of lines are always one line
- **The fix** — put a bend (an activation function) between every pair of layers

*Example (italic):* Check at x = 1: layer 1 gives 3, layer 2 gives 3×3 − 2 = 7 — exactly what 6x + 1 predicts.

**Key point:** Without activations, depth is fake — a 10-layer network computes nothing a 1-layer network couldn't.

### Visualization (canvas `c1`, 720×300)

Line plot showing two composed linear layers collapsing into a single line, with a legend column on the right.

- **Title (bold 15px, ink `#1a5276`, centered over the plot):** "Layer After Layer of Lines Is Still a Line"
- **Plot area:** padding top 52 / bottom 52 / left 62 / right 200; x from −2 to 2, y from −12 to 14; light gray `#ccc` axes drawn through the origin; 12px `#888` "x" label at the right end of the x-axis.
- **Lines:** layer 1 `y = 2x + 1` as a dashed blue `#2a78d6` line (width 2, dash 6/4); composed result `z = 6x + 1` as a solid magenta `#d55181` line (width 3.5).
- **Check point:** orange `#d95926` dot (r=6) at (1, 7), bold 13px label "x=1 → 7".
- **Legend (right column, x = width−190):** dashed blue swatch with 13px "layer 1: y = 2x + 1"; solid magenta swatch with "after layer 2:" / "z = 3y − 2 = 6x + 1".
- **Annotations (right column):** bold 13px orange, three lines: "two layers collapsed" / "into one line —" / "so would ten"; then 12px gray: "slope 6, intercept 1:" / "one layer could do it".

## The Two Famous Bends: ReLU and Sigmoid

**Tags:** `core idea` (blue), `rule of thumb` (blue)

- **ReLU** — "negatives become 0, positives pass through": ReLU(−2) = 0, ReLU(3) = 3
- **One kink** — ReLU is two half-lines meeting at 0; that single corner is the bend
- **Sigmoid** — squashes any number into 0-1: sigmoid(−2) = 0.12, sigmoid(0) = 0.5, sigmoid(2) = 0.88
- **Where each lives** — ReLU between hidden layers; sigmoid at the output for a probability-like score
- **Both are cheap** — one comparison or one exponential per neuron, applied after the sum

*Example (italic):* ReLU is an if-statement: max(0, x). You can compute a whole layer of it in your head.

**Key point:** The activation runs after each neuron's weighted sum — it is the only non-line step in the whole network.

### Visualization (canvas `c2`, 720×300)

Two side-by-side function plots: ReLU (left) and sigmoid (right), each with verifiable labeled points.

- **Title (bold 15px, ink, top center):** "The Two Bends, With Points You Can Verify"
- **Left panel (ReLU):** plot area at (60,60) 280×180; x from −3 to 3, y from −0.6 to 3.2; light gray axes through origin; ReLU curve in blue `#2a78d6` width 3 (flat at 0 for x≤0, then slope-1 line to (3,3)). Orange points (r=5) with bold 12px labels: "ReLU(−2) = 0" at (−2,0) and "ReLU(1.5) = 1.5" at (1.5,1.5). Caption bold 14px blue below: "ReLU: max(0, x)". Annotation bold 12px magenta `#d55181` near the origin: "the kink at 0 is the bend".
- **Right panel (sigmoid):** plot area at (410,60) 280×180; x from −4 to 4, y from −0.15 to 1.15; gray axes; dashed `#bbb` asymptote line at y=1 labeled "1" (11px `#888`). Sigmoid curve `1/(1+e^{-x})` sampled over 100 steps in aqua `#199e70` width 3. Orange points with bold 12px labels: "−2 → 0.12" (right-aligned), "0 → 0.5", "2 → 0.88". Caption bold 14px aqua below: "sigmoid: squash into 0–1".

## Two ReLUs Make a Ramp, Three Make a Bump

**Tags:** `worked example` (green), `core idea` (blue)

- **Combine two** — r(x) = ReLU(x) − ReLU(x − 1): a ramp that rises then flattens
- **Check it** — r(0) = 0, r(0.5) = 0.5, r(1) = 1, r(2) = 2 − 1 = 1: flat after 1
- **Add a third** — t(x) = ReLU(x) − 2·ReLU(x − 1) + ReLU(x − 2): a triangle bump
- **Check the peak** — t(1) = 1 − 0 + 0 = 1; t(2) = 2 − 2 + 0 = 0: up then back down
- **Scale this up** — thousands of shifted kinks approximate any curve you like

*Example (italic):* t(3) = 3 − 2×2 + 1 = 0 — the bump stays down forever after x = 2. Try it on paper.

**Key point:** Each ReLU contributes one corner; the network's "curve" is really many small corners placed where the data needs them.

### Visualization (canvas `c3`, 720×300)

Function plot of the ramp and triangle-bump built from ReLUs, with check points and a right-side legend.

- **Title (bold 15px, ink, centered over the plot):** "Curves Assembled From Corners"
- **Plot area:** padding top 52 / bottom 52 / left 62 / right 210; x from −1 to 3.5, y from −0.25 to 1.35; light gray axes; 12px gray x ticks at −1, 0, 1, 2, 3.
- **Ramp curve:** `r(x) = ReLU(x) − ReLU(x−1)` sampled over 180 steps, solid blue `#2a78d6` width 3.
- **Bump curve:** `t(x) = ReLU(x) − 2·ReLU(x−1) + ReLU(x−2)` sampled over 180 steps, dashed magenta `#d55181` width 3 (dash 7/4).
- **Check points** (r=5 dots with bold 12px labels in matching colors): "r(0.5) = 0.5" at (0.5, 0.5) blue, right-aligned; "r(2) = 1, flat" at (2, 1) blue; "t(1) = 1, the peak" at (1, 1) magenta, right-aligned; "t(2) = 0" at (2, 0) magenta, label offset below.
- **Legend (right column, x = width−200):** solid blue swatch, 12px "ramp: ReLU(x)" / "− ReLU(x−1)"; dashed magenta swatch, "bump: ReLU(x)" / "− 2·ReLU(x−1)" / "+ ReLU(x−2)".
- **Annotation (right column, bold 13px orange, four lines):** "3 corners already" / "make a bump —" / "thousands make" / "any curve".

## Why the Bend Matters to a Data Scientist

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Real patterns are curvy** — sales vs temperature rises, peaks, falls; no line can follow that
- **A line-only net fits the average** — it slices a hump with a flat line and misses both ends
- **With bends it follows** — a few kinked units trace the rise and the fall
- **Common mistake** — activation is not just "on/off"; it reshapes values, it doesn't only gate them
- **Debug clue** — a deep model performing exactly like linear regression often lost its activations

*Example (italic):* An ice-cream stand's sales peak at 7 cones near 25°C; a straight line predicts 4.2 cones at every temperature.

**Key point:** You pay for depth to get curves — the activation function is the part that actually delivers them.

### Visualization (canvas `c4`, 720×300)

Scatter of hump-shaped sales data with a flat line fit vs a kinked (piecewise linear) fit, plus a right-side legend.

- **Title (bold 15px, ink, centered over the plot):** "Ice-Cream Sales vs Temperature: Line Fit vs Bent Fit"
- **Plot area:** padding top 52 / bottom 56 / left 62 / right 190; x from 0 to 50 (°C), y from 0 to 8; gray `#999` L-shaped axes; 12px ticks "0°, 10°, 20°, 30°, 40°, 50°"; axis titles 12px gray: "temperature (°C)" (bottom center), "cones sold" (rotated, left).
- **Data (orange `#d95926` dots, r=5):** temps `[5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45]`, sales `[1, 2.5, 4, 5.5, 6.5, 7, 6.5, 5.5, 4, 2.5, 1]`.
- **Line-only fit:** dashed violet `#4a3aa7` horizontal line at y=4.2 (width 2.5, dash 7/5) from x=2 to x=48.
- **Bent fit:** solid green `#008300` piecewise line, width 3, through (5,1) → (17,5.5) → (25,7) → (33,5.5) → (45,1).
- **Annotation:** bold 13px magenta "peak: 7 at 25°C" above the (25,7) point.
- **Legend (right column, x = width−180):** orange dot + "daily sales data"; dashed violet swatch + "no activations:" / "flat line at 4.2"; solid green swatch + "with ReLU bends:" / "follows the hump".
- **Annotation (right column):** bold 13px orange "the bends are what" / "buy the curve"; italic 11px `#888` "illustrative data".

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference style). h1 + `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (`width:100%`, border-collapse collapse) with one row: `td.text-col` (50%) and `td.viz-col` (50%), both `vertical-align: top`, padding 12px.
- **Text column structure:** `.tags` row of pill spans first (`.tag` — inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.tag.blue` bg `rgba(26,82,118,0.12)` color `#1a5276`; `.tag.green` bg `rgba(39,174,96,0.15)` color `#27ae60`; `.tag.red` bg `rgba(231,76,60,0.12)` color `#e74c3c`; `.tag.orange` bg `rgba(230,126,34,0.15)` color `#e67e22`), then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`, one italic `.example` paragraph (`#555`, 0.9rem), and one `.key-point` callout (bg `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>Key point:</strong>` lead-in.
- **Page CSS:** global reset; body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases styled `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** each canvas declares `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data is hardcoded literal arrays (no `Math.random()`); invented data carries an "illustrative data" label. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none — it is a leaf tutorial page).
