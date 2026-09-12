# Regularization

**Page type:** detail page (tutorial card-sections: one h2 + two-column table per section, text left 50%, canvas right 50%)
**HTML title tag:** Regularization

**Subtitle:** A penalty for overly complicated models that nudges them toward simpler, more trustworthy answers

## Two House-Price Models: the Wiggle and the Line

**Tags:** `core idea` (blue), `complexity penalty` (orange)

- **The job** — predict house price from listing data; eight sold houses are the training set
- **The wiggle** — a model using size PLUS junk (house number, street-name length) nails all 8
- **The line** — a size-only model misses each sale by a few thousand dollars
- **On new houses** — the wiggle's junk-powered swerves point the wrong way; the line holds up
- **The nudge** — regularization charges the model rent for every unit of complexity it uses
- **New objective** — minimize fit error + penalty, not fit error alone

*Example (italic):* Illustrative listing data: a 1,400 sq ft house at #7 Elm sells for $250k — the "#7" should not move the prediction.

**Key point:** **Regularization** adds a complexity charge to the loss, so a slightly-worse-fitting simple model can beat a perfectly-fitting complicated one — on purpose.

### Visualization (canvas `c1`, 720×300)

Scatter of eight sold houses with two fitted models: a wiggly curve through every point and a straight line.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Sold Houses: Perfect Wiggle vs Honest Line (illustrative)"
- **Data points (ink `#1a5276`, 5px dots):** size (100s of sq ft) `[8, 10, 12, 14, 16, 18, 20, 22]`, price ($k, illustrative) `[165, 195, 230, 250, 290, 310, 350, 370]`.
- **Axes:** x labeled in sq ft (800 to 2200, ticks every 200), label "size (sq ft)"; y from $150k to $400k (ticks every $50k, formatted "$150k" etc.). Axis lines `#999`, tick labels muted `#6b7280` 12px. Padding: top 48, bottom 48, left 65, right 160.
- **Line model:** green `#008300`, width 3: price = 50 + 14.5 × size, drawn from size 7 to 23.
- **Wiggle model:** magenta `#d55181`, width 2.5: passes exactly through every data point via quadratic curves whose midpoints alternate ±32 above/below the segment midpoint (overshooting between points).
- **Annotations (bold 12px, left-aligned):** magenta near top-left: "wiggle: zero training error," / "junk features do the swerving"; green mid-right: "line: a few $k off each sale," / "steady on new houses".
- **Legend (right margin, 12px):** magenta line "5-feature model"; green line "size-only model".

## Charging Rent for Complexity, by Hand

**Tags:** `worked example` (green), `complexity penalty` (blue)

- **The rule** — total score = fit error + 0.1 × (sum of squared weights); smaller total wins
- **Model A** — five weights [9, 6, −5, 4, −3]: squares sum to 167, penalty 16.7
- **Model A total** — fit error 4 + penalty 16.7 = 20.7
- **Model B** — two weights [8, 1, 0, 0, 0]: squares sum to 65, penalty 6.5
- **Model B total** — fit error 9 + penalty 6.5 = 15.5 — the worse fitter wins
- **Without the penalty** — A's fit of 4 beats B's 9, and the junk-stuffed model ships

*Example (italic):* The whole comparison is five squarings and two additions per model — redo it on paper in a minute.

**Key point:** **The penalty flips the winner:** A fits the past better, B scores better once complexity costs something — and B is the one you want predicting the future.

### Visualization (canvas `c2`, 720×300)

Two stacked bars (fit error + penalty) for models A and B with totals annotated.

- **Title (bold 15px, `#1a5276`, top center):** "Total Score = Fit Error + 0.1 × (Sum of Squared Weights)"
- **Data:** Model A (5 features): fit 4, penalty 16.7, total 20.7. Model B (2 features): fit 9, penalty 6.5, total 15.5.
- **Bars:** 110px wide, positioned at 28% and 72% of plot width. Bottom segment = fit error in blue `#2a78d6` (50% alpha fill, 2px blue stroke); top segment = penalty in orange `#d95926` (50% alpha fill, 2px orange stroke). Inside labels bold 12px: "fit 4" / "fit 9" in blue, "penalty 16.7" / "penalty 6.5" in dark brown `#8a3d12`. Above each bar bold 14px ink: "total 20.7" / "total 15.5". Below the axis 12px: "Model A (5 features)" / "Model B (2 features)".
- **Axes:** y from 0 to 24 (ticks every 6). Axis lines `#999`, tick labels muted `#6b7280` 12px. Padding: top 54, bottom 60, left 70, right 220.
- **Legend (right margin, 12px):** blue swatch "fit error on the 8 houses"; orange swatch "complexity penalty".
- **Annotations (right margin):** bold 13px green `#008300`: "B wins 15.5 vs 20.7 —" / "the penalty flips the winner"; 12px magenta `#d55181`: "(on fit alone, A's 4 beats B's 9)".

## L1 vs L2: Shrink Everything, or Fire the Junk

**Tags:** `L1 and L2` (blue), `simpler models` (green)

- **L2 (ridge)** — penalty on squared weights: shrinks every weight toward zero, none reach it
- **L1 (lasso)** — penalty on absolute weights: pushes weak weights to EXACTLY zero
- **Free feature selection** — an L1 zero means "this feature is dropped from the model"
- **Here** — L1 zeroes house number, lucky number and street-name length; size survives
- **Why the difference** — squaring makes tiny weights nearly free, so L2 keeps them around
- **In practice** — L2 is the default; L1 when you suspect most features are junk

*Example (italic):* After L1, the price model reads "size and bedrooms only" — the junk columns fired themselves.

**Key point:** **Both penalties shrink; only L1 deletes.** If you need a short, explainable feature list, L1 gives it to you as a side effect of training.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart of the same model's five weights under no penalty, L2 and L1, drawn around a zero baseline (bars go up for positive, down for negative weights).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Model's Weights Under No Penalty, L2, L1"
- **Features (x groups):** `["size", "bedrooms", "house no.", "lucky no.", "street len"]`
- **Weight data (three bars per feature, 22px wide, 55% alpha fills):**
  - no penalty, mute gray `#6b7280`: `[9, 6, -5, 4, -3]`
  - L2 (ridge), blue `#2a78d6`: `[7, 3, -2, 1.5, -1]`
  - L1 (lasso), green `#008300`: `[7.5, 2, 0, 0, 0]` (zero weights drawn as 1px slivers)
- **Axes:** horizontal zero line `#999` at plot mid-height; y symmetric −10 to +10, muted 12px labels "0", "+10", "−10" on the left. Padding: top 54, bottom 56, left 60, right 165.
- **Annotations:** bold 13px green centered around 62% of plot width, below the baseline: "L1 sets the three junk weights to exactly 0 — features deleted"; bold 12px blue near the top at 63% width: "L2: smaller, never zero".
- **Legend (right margin, 12px swatches):** gray "no penalty"; blue "L2 (ridge)"; green "L1 (lasso)"; muted note "y: weight value".

## The Dial λ: How Hard to Push Toward Simple

**Tags:** `common mistake` (red), `tuning` (orange)

- **λ is the rent level** — total = fit error + λ × penalty; λ sets how much complexity costs
- **λ = 0** — complexity is free: back to the wiggle, training error 4 but new-house error 18
- **λ huge** — everything shrinks to zero: the model predicts one average price for all houses
- **The sweet spot** — here around λ = 1, where new-house error bottoms out at 9.5
- **Pick by validation** — try several λ values and keep the one with lowest held-out error
- **The classic mistake** — tuning λ on training error, which always votes for λ = 0

*Example (italic):* Asking the training score whether it wants a penalty is asking a student whether exams should count.

**Key point:** **Regularization is a dial, not a switch:** too little brings the overfit back, too much underfits — the validation curve, not the training curve, chooses λ.

### Visualization (canvas `c4`, 720×300)

Two-line chart of training vs validation error across λ values, with the validation curve forming a U-shape and a highlighted sweet spot.

- **Title (bold 15px, `#1a5276`, top center):** "Error vs λ: Training Always Votes 0, Validation Picks 1 (illustrative)"
- **Data (x categories):** λ = `["0", "0.01", "0.1", "1", "10", "100"]`; training error (blue `#2a78d6`) `[4, 5, 7, 9, 14, 22]`; validation error (violet `#4a3aa7`) `[18, 13, 10, 9.5, 15, 23]`.
- **Axes:** x six evenly spaced λ labels, axis label "penalty strength λ"; y from 0 to 25 (ticks every 5). Axis lines `#999`, tick labels muted `#6b7280` 12px. Padding: top 52, bottom 54, left 60, right 170.
- **Series:** width-3 lines with 4.5px dots at every point in each series' color.
- **Sweet spot:** green `#008300` 8px dot at (λ=1, 9.5) with bold 13px green label above: "sweet spot: λ = 1, new-house error 9.5".
- **Annotations (bold 12px):** magenta `#d55181` near λ=0: "λ = 0: the wiggle returns"; orange `#d95926` right-aligned near λ=100: "λ huge: predicts one" / "average price for all".
- **Legend (right margin, 12px swatches):** blue "training error"; violet "validation error".

## Regeneration instructions

- **Layout:** tutorial detail page. h1, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (full width, border-collapse) with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pill row, `<ul>` bullets (each starting with `<b>` term in `#1a5276`), italic `.example` paragraph, and `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic `#555` 0.9rem. Bullets 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. blue: bg rgba(26,82,118,0.12) / `#1a5276`; green: bg rgba(39,174,96,0.15) / `#27ae60`; red: bg rgba(231,76,60,0.12) / `#e74c3c`; orange: bg rgba(230,126,34,0.15) / `#e67e22`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** intrinsic 720×300 attributes; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded and deterministic; invented data labeled "(illustrative)" in chart titles. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
