# Weighted Least Squares

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Weighted Least Squares

**Subtitle:** When some observations are more trustworthy than others, give each one a weight — the line then listens to the precise points and shrugs off the noisy ones

## Eight Neighborhood Averages, Not Eight Equal Votes

**Tags:** `core idea` (blue), `unequal precision` (orange), `weighted fit` (green)

- **The courier firm** — plots average delivery time against distance for eight neighborhoods, A to H
- **Unequal evidence** — A's average rests on 400 deliveries; H's rests on just 4
- **OLS** — ordinary least squares treats all eight dots as equally trustworthy votes
- **WLS** — weighted least squares lets each dot vote in proportion to a weight you assign
- **The definition** — WLS minimizes the sum of weight × (error)², so heavy points pull harder

*Example (italic):* The four small-sample averages drag the equal-weight slope down to 2.71 min/km; the weighted fit stays at 3.75.

**Key point:** When observations differ in reliability, don't treat them as equals — WLS minimizes weighted squared error so precise points steer the line.

### Visualization (canvas `c1`, 720×300)

Single-panel scatter of the eight neighborhood averages with dot area proportional to sample size, plus the OLS and WLS fitted lines.

- **Title (bold 15px, `#1a5276`, top center):** "Avg Delivery Time vs Distance: Dot Size = Deliveries Behind the Average".
- **Data:** neighborhoods A–H at km `[1, 2, 3, 4, 5, 6, 7, 8]`, avg minutes `[14.2, 17.8, 22.3, 25.9, 22, 45, 27, 31]`, deliveries `[400, 350, 300, 250, 12, 8, 5, 4]`, dot radii `[13, 12, 11, 10.5, 4, 3.5, 3.2, 3]`.
- **Axes:** origin x=60, plot width 600, baseline y=250, plot height 200; y scale 0–50 min with gridlines `#e5e9ef` and 12px `#444` labels at 0/10/20/30/40/50; x maps km 1–8 evenly with 12px labels "1 km" … "8 km"; letters A–H 12px `#6b7280` beside each dot.
- **Dots:** fill `rgba(42,120,214,0.45)`, 2px stroke blue `#2a78d6`.
- **OLS line:** orange `#d95926`, 3px dashed (6/4), y = 13.45 + 2.71x drawn from km 0.5 to 8.5, labeled bold 12px "OLS: equal votes".
- **WLS line:** green `#008300`, 3px solid, y = 10.58 + 3.75x over the same range, labeled bold 12px "WLS: weighted votes".
- **Annotation (bold 13px magenta `#d55181`, near the small dots):** "4 noisy little dots bend the dashed line".

## Turning Delivery Counts into Weights

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The rule** — an average of n deliveries has variance σ²/n, so its natural weight is n itself
- **The weights** — A–H get 400, 350, 300, 250, 12, 8, 5, 4; the weights total 1,329
- **The shares** — the four big samples hold 30.1 + 26.3 + 22.6 + 18.8 = 97.8% of the say
- **100 to 1** — neighborhood A (400 deliveries) outvotes H (4 deliveries) by exactly 100×
- **The fit** — the same slope formula, with every sum term multiplied by w, gives 10.6 + 3.75 × km

*Example (italic):* OLS says time = 13.4 + 2.71 × km; redoing the identical sums with each term multiplied by n gives 10.6 + 3.75 × km.

**Key point:** Inverse-variance weighting: set each weight to 1/variance. For a group average of n values, that is simply proportional to n.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of each neighborhood's share of the total vote (weight ÷ 1,329), with the four precise bars in blue and the four noisy bars in magenta.

- **Title (bold 15px, `#1a5276`, top center):** "Share of the Vote: Weight = Deliveries Behind Each Average".
- **Data:** shares % `[30.1, 26.3, 22.6, 18.8, 0.9, 0.6, 0.4, 0.3]` for A–H; delivery counts `[400, 350, 300, 250, 12, 8, 5, 4]`.
- **Axes:** origin x=60, plot width 600, baseline y=245, plot height 185, y scale 0–35% with gridlines `#e5e9ef` and 12px `#444` labels at 0/10/20/30.
- **Bars:** eight bars, 46px wide, evenly spaced; A–D fill `rgba(42,120,214,0.55)` with 1px stroke blue `#2a78d6`; E–H fill `rgba(213,81,129,0.55)` with 1px stroke magenta `#d55181`.
- **Labels:** letter A–H 12px `#444` below each bar with "n=400" … "n=4" in 11px `#6b7280` beneath; share value bold 12px above each bar ("30.1%" … "0.3%").
- **Annotation (bold 13px blue `#2a78d6`, over bars A–D):** "four precise averages hold 97.8% of the vote".
- **Caption (12px `#444`, bottom right):** "weight ÷ total weight (1,329)".

## Why Equal Weights Bend the Line

**Tags:** `where it's used` (blue), `unequal noise` (orange), `failure mode` (red)

- **Error bars** — ±2 SE per average: about ±0.6 min for A but ±6.0 min for H (per-delivery σ = 6)
- **The funnel** — small-sample averages scatter far from any line; big-sample ones hug it
- **OLS damage** — the equal-weight line predicts 35.1 min at 8 km; the weighted line says 40.6
- **The check** — fitting only the four precise points gives 10.2 + 3.96 × km, close to the WLS fit
- **The name** — statisticians call unequal noise "heteroscedasticity"; WLS is the textbook fix

*Example (italic):* Promising an 8-km customer a 35-minute delivery off the OLS line, when the answer is near 41, misses by 5.5 minutes.

**Key point:** Group averages, survey strata, and repeated measurements almost never share one noise level — equal weighting lets the noisiest points bend the line.

### Visualization (canvas `c3`, 720×300)

The same scatter drawn with ±2 SE error bars on every point; the WLS line threads through the tight bars while the OLS line is dragged toward the wide ones.

- **Title (bold 15px, `#1a5276`, top center):** "Same Averages with ±2 SE Error Bars (σ = 6 per delivery)".
- **Data:** km `[1, 2, 3, 4, 5, 6, 7, 8]`, minutes `[14.2, 17.8, 22.3, 25.9, 22, 45, 27, 31]`, half-widths ±2 SE `[0.6, 0.6, 0.7, 0.8, 3.5, 4.2, 5.4, 6.0]`.
- **Axes:** same frame as c1 — origin x=60, width 600, baseline y=250, height 200, y scale 0–50, gridlines `#e5e9ef`, 12px labels, x labels "1 km" … "8 km".
- **Error bars:** vertical 2px lines with 8px end caps; blue `#2a78d6` for A–D, magenta `#d55181` for E–H; 5px dots at each mean, matching color.
- **WLS line:** green `#008300` 3px solid, y = 10.58 + 3.75x from km 0.5 to 8.5.
- **OLS line:** orange `#d95926` 3px dashed (6/4), y = 13.45 + 2.71x over the same range.
- **Annotation (bold 13px green, right side):** "at 8 km: OLS 35.1 vs WLS 40.6 min".
- **Caption (12px `#444`, bottom left):** "±2 SE = ±2σ/√n; illustrative data".

## Weights Mean Trust, Not Importance

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Trust, not value** — a weight says "how precise is this point", not "how much money it brings in"
- **Wrong weighting** — upweighting downtown because it earns more revenue just biases the fit
- **Only ratios matter** — weights n and 2n give the identical line: 10.6 + 3.75 × km both times
- **Scaling is free** — multiply every weight by any constant and nothing about the line changes
- **Equal is OLS** — if every point gets the same weight, whatever it is, WLS is exactly OLS

*Example (italic):* Doubling every weight from (400, …, 4) to (800, …, 8) leaves the slope at 3.75 and the intercept at 10.6, untouched.

**Common mistake:** Choosing weights by business importance instead of statistical precision. Weights encode how noisy each point is — priorities belong in the question, not in the fit.

### Visualization (canvas `c4`, 720×300)

Bar chart comparing the fitted slope under four weighting schemes, showing that rescaling all weights changes nothing while equal weights give a different answer.

- **Title (bold 15px, `#1a5276`, top center):** "Fitted Slope (min per km) Under Four Weighting Schemes".
- **Data:** schemes `["equal weights (OLS)", "w = n", "w = 2n", "w = n ÷ 100"]` with slopes `[2.71, 3.75, 3.75, 3.75]`.
- **Axes:** origin x=70, plot width 580, baseline y=240, plot height 175, y scale 0–4.5 with gridlines `#e5e9ef` and 12px `#444` labels at 0/1/2/3/4.
- **Bars:** four bars, 90px wide; first bar fill `rgba(217,89,38,0.55)` with 1px stroke orange `#d95926`; the other three fill `rgba(0,131,0,0.4)` with 1px stroke green `#008300`; slope value bold 13px above each bar; scheme label 12px `#444` below each bar.
- **Reference line:** dashed (4/3) violet `#4a3aa7` horizontal line at slope 3.96 labeled 12px bold "precise-points-only fit: 3.96".
- **Annotation (bold 13px green, over the three green bars):** "same ratios → the exact same line".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
