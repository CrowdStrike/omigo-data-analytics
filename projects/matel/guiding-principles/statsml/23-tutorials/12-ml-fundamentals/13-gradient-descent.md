# Gradient Descent

**Page type:** detail page (tutorial card-sections: one h2 + two-column table per section, text left 50%, canvas right 50%)
**HTML title tag:** Gradient Descent

**Subtitle:** Walk downhill on the error landscape one small step at a time until you reach a low point

## The Error Landscape of One Knob

**Tags:** `core idea` (blue), `optimization` (green)

- **The model** — a pizza shop predicts delivery time as b + 4×km, and must tune the base time b
- **The data** — four past deliveries at 1, 2, 4, 5 km took 18, 22, 30, 34 minutes
- **One loss per b** — every candidate b gets a squared-error score; here it works out to (b − 14)²
- **The landscape** — plotting loss against b draws a valley whose bottom sits at b = 14
- **Blindfolded hiker** — the algorithm cannot see the valley, only the slope under its feet
- **The rule** — feel the slope, step the opposite way, repeat until the ground is flat

*Example (italic):* Start with a bad guess of b = 2: the ground tilts steeply down toward 14, so step right.

**Key point:** **Gradient descent** is walking downhill on the loss: compute the slope at the current setting, move a small step against it, and repeat. Flat ground means you have arrived.

### Visualization (canvas `c1`, 720×300)

Parabola (loss valley) with a ball at the starting guess and a downhill slope arrow.

- **Title (bold 15px, `#1a5276`, top center):** "Loss for Every Possible Base Time b: a Valley"
- **Curve:** loss(b) = (b − 14)² plotted in ink `#1a5276`, width 3, over b in [0, 28], clipped at loss 200, sampled in 100 segments.
- **Axes:** x from 0 to 28 (ticks every 7), label "base time b (minutes)"; y from 0 to 200 (ticks every 50). Axis lines `#999`, tick labels muted `#6b7280` 12px. Padding: top 48, bottom 46, left 60, right 30.
- **Bottom marker:** dashed green (`#008300`, dash 5/4, width 1.5) vertical line at b=14 from the x-axis to the curve bottom; green 6px dot at (14, 0); bold 13px green label above: "bottom: b = 14, loss = 0".
- **Start ball:** magenta `#d55181` 8px dot at (2, 144); bold 13px label to its right: "start: b = 2, loss = 144".
- **Slope arrow:** orange `#d95926` line (width 3) from (2.8, 130) to (5.5, 78) with filled arrowhead; bold 13px orange two-line annotation near (6.2, 105): "slope points uphill —" / "so step the other way".

## Walking Downhill by Hand: Five Steps to the Bottom

**Tags:** `worked example` (green), `slopes` (blue)

- **The slope** — for loss (b − 14)² the slope at any b is 2×(b − 14); at b = 2 it is −24
- **The update** — new b = b − 0.25 × slope, so b = 2 becomes 2 + 6 = 8
- **Step 2** — at b = 8 the slope is −12, so b becomes 8 + 3 = 11
- **Steps 3-5** — b goes 11 → 12.5 → 13.25 → 13.625, each step half the last
- **Loss collapses** — 144 → 36 → 9 → 2.25 → 0.56 → 0.14, never touching zero
- **Steeper = bigger step** — far from the bottom the slope is large, so progress is fast early

*Example (italic):* Every number above is two multiplications on paper: slope = 2×(b − 14), step = −0.25 × slope.

**Key point:** **The step shrinks by itself:** the gap to the bottom halves every step (12, 6, 3, 1.5, ...), because gentler slopes near the bottom automatically mean smaller moves.

### Visualization (canvas `c2`, 720×300)

Same loss valley with the six visited points and curved hop arrows between consecutive steps.

- **Title (bold 15px, `#1a5276`, top center):** "Five Hand-Computed Steps (step = −0.25 × slope)"
- **Curve and axes:** identical valley to c1 (loss (b − 14)², b 0–28, y 0–200, ink `#1a5276` width 3, x label "base time b (minutes)"). Padding: top 48, bottom 46, left 60, right 30.
- **Visited points:** b values `[2, 8, 11, 12.5, 13.25, 13.625]` plotted at (b, loss(b)) as 6px dots — magenta `#d55181` for all but the last, which is green `#008300`.
- **Hop arrows:** orange `#d95926` quadratic-curve arcs (width 2) between consecutive points, each with a filled orange arrowhead at the destination.
- **Point labels (bold 12px):** magenta "b=2", "8", "11" above the first three points; green "13.6 — nearly there" above the final point.
- **Annotation (bold 13px orange, near b≈16.5):** "big hops on steep ground," / "small hops near the bottom".

## Why Not Just Try Every Value of b?

**Tags:** `where it's used` (blue), `optimization` (green)

- **One knob is easy** — you could test b = 0, 1, 2, ... 30 and pick the best by brute force
- **Real models** — a large neural network has millions of knobs; grids of guesses explode
- **Slopes are cheap** — calculus gives the downhill direction for all knobs in one pass
- **Same walk** — millions of knobs still means: feel the slope, step against it, repeat
- **This IS training** — "the model is learning" means gradient descent is lowering the loss
- **Fast then slow** — most of the loss drop happens in the first few steps, then it flattens

*Example (italic):* Trying just 10 values per knob for 1,000,000 knobs needs 10^1,000,000 guesses; slopes need one.

**Key point:** **Why it matters:** nearly every modern model — linear regression, boosted trees' cousins, every neural network — can be fit by some flavor of this one downhill walk.

### Visualization (canvas `c3`, 720×300)

Line chart of loss vs step number showing fast early progress then a flat tail.

- **Title (bold 15px, `#1a5276`, top center):** "Loss After Each Step: Most of the Work Happens Early"
- **Data:** losses by step 0–6: `[144, 36, 9, 2.25, 0.56, 0.14, 0.04]`.
- **Axes:** x from 0 to 6 (integer ticks), label "step number"; y from 0 to 150 (ticks every 50). Axis lines `#999`, tick labels muted `#6b7280` 12px. Padding: top 50, bottom 50, left 65, right 40.
- **Series:** blue `#2a78d6` connected line, width 3, with 5px-radius blue dots at each point; 12px text labels "144", "36", "9", "2.25" near the first four points.
- **Annotations (bold 13px):** green `#008300` near step 2.2, y≈100: "75% of the loss gone in one step"; orange `#d95926` near step 3.6, y≈28: "then a long flat tail — each step halves the remaining gap".

## The Confusion: It Finds a Low Point, Not the Lowest

**Tags:** `common mistake` (red), `caution` (orange)

- **Nice valleys** — our pizza loss has one bottom, so any start rolls to the same answer
- **Bumpy landscapes** — complex models have many dips; the walk stops in whichever it enters
- **Local minimum** — a dip that is lowest nearby but not lowest overall; flat ground fools the hiker
- **Start matters** — two different starting guesses can settle into two different dips
- **Practical fix** — random restarts, momentum, and noise in the steps help escape shallow dips
- **Usually fine** — for big models many dips are near-equally good, so this rarely ruins training

*Example (italic):* A hiker descending in fog stops in the first hollow they find — the deepest gorge may be one ridge over.

**Key point:** **The confusion:** "the loss stopped falling" means the ground is flat here, not that no better setting exists. Gradient descent guarantees downhill, never the global bottom.

### Visualization (canvas `c4`, 720×300)

Bumpy loss landscape with two valleys: a shallow local minimum (where the walk gets stuck) and a deeper global minimum one ridge over.

- **Title (bold 15px, `#1a5276`, top center):** "A Bumpy Landscape: the Walk Stops in the First Dip"
- **Curve:** deterministic bumpy shape in ink `#1a5276`, width 3, sampled in 200 segments over t in [0,1]: f(t) = 0.75 − 0.35·exp(−((t − 0.28)/0.12)²) − 0.62·exp(−((t − 0.72)/0.17)²), giving a left shallow valley (local minimum, loss ≈ 0.40 at t = 0.28) and a right deep valley (global minimum, loss ≈ 0.13 at t = 0.72); low loss is drawn low on the canvas. Axis lines `#999` (left and bottom only, no ticks). Padding: top 48, bottom 46, left 45, right 30.
- **Stuck ball:** magenta `#d55181` 8px dot resting in the left (local) dip, found by scanning the left half of the curve for its minimum; bold 13px two-line label below: "stuck here: flat ground," / "walk stops (local minimum)".
- **Global minimum marker:** green `#008300` 7px dot at the right-half minimum; bold 13px two-line green label above: "true lowest point" / "one ridge away".
- **Start point:** orange `#d95926` 6px dot near t=0.06 on the curve, bold 12px label "start"; an orange quadratic-curve arrow (width 2) from the start down into the local dip, with a filled orange arrowhead.
- **X-axis label (muted 12px, bottom center):** "model setting (illustrative complex model)"

## Regeneration instructions

- **Layout:** tutorial detail page. h1, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (full width, border-collapse) with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pill row, `<ul>` bullets (each starting with `<b>` term in `#1a5276`), italic `.example` paragraph, and `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic `#555` 0.9rem. Bullets 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. blue: bg rgba(26,82,118,0.12) / `#1a5276`; green: bg rgba(39,174,96,0.15) / `#27ae60`; red: bg rgba(231,76,60,0.12) / `#e74c3c`; orange: bg rgba(230,126,34,0.15) / `#e67e22`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** intrinsic 720×300 attributes; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `loss(b) = (b − 14)²` and a valley scaler/drawer mapping b in [0, 28] and loss in [0, 200] used by c1 and c2. All data hardcoded and deterministic (no Math.random). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
