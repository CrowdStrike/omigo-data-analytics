# Linear Discriminant Analysis

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Linear Discriminant Analysis

**Subtitle:** LDA finds the one tilted direction along which two classes pull apart the most — it projects data to separate classes, not to preserve variance

## One Packing Shed, Two Rulers, One Tilted Line

**Tags:** `core idea` (blue), `one direction` (green), `classification` (orange)

- **The shed** — a packing line must sort 16 fruits into lemons and oranges using weight and diameter
- **Weight alone** — lemons run 100–135 g and oranges 125–160 g, so the 125–135 g middle mixes both
- **Diameter alone** — lemon and orange diameters average 65.4 mm vs 64.4 mm, nearly identical
- **The tilt** — the combined score weight − 2 × diameter splits the 16 fruits with zero mixing
- **The name** — finding that single best separating direction is linear discriminant analysis (LDA)

*Example (italic):* A 130 g fruit alone is ambiguous, but 130 g at only 60 mm wide scores 130 − 120 = +10 — dense for its size, so it goes to the orange bin.

**Key point:** LDA looks for one weighted combination of the features along which the labeled classes pull apart the most — a projection built for separation, not a full model of the data.

### Visualization (canvas `c1`, 720×300)

2D scatter of all 16 fruits (weight on x, diameter on y) with the tilted LDA decision line cutting the two clouds apart.

- **Title (bold 15px, `#1a5276`, top center):** "16 Fruits by Weight and Diameter: One Tilted Line Sorts Them".
- **Data (weight g, diameter mm):** lemons `[[100,58],[110,62],[120,66],[130,70],[105,61],[125,69],[115,64],[135,73]]`; oranges `[[125,57],[135,61],[145,65],[155,69],[130,60],[150,68],[140,63],[160,72]]`.
- **Axes:** origin x=70, baseline y=250, plot width 600, plot height 200; x maps weight 90–170 g with ticks 90/110/130/150/170; y maps diameter 54–76 mm with ticks 55/60/65/70/75; 12px `#444` tick labels; axis titles "weight (g)" and "diameter (mm)" 12px `#6b7280`; 2px `#1a5276` axis lines.
- **Points:** lemons yellow `#c98500` 5px filled dots; oranges orange `#d95926` 5px filled dots; legend swatches + 12px labels "lemon" / "orange" at top right of plot.
- **Class means:** bold "×" markers in ink `#1a5276` at lemon mean (117.5, 65.4) and orange mean (142.5, 64.4).
- **Decision line:** dashed violet `#4a3aa7` 2px line for score = 0, i.e. weight = 2 × diameter, drawn from data point (110, 55) to (150, 75); bold 12px violet label "weight − 2×diameter = 0" along the line.
- **Annotations (bold 12px):** magenta `#d55181` "lemon side: score < 0" left of the line; green `#008300` "orange side: score > 0" right of the line.
- **Caption (12px `#444`, bottom right):** "illustrative packing-shed data".

## Scoring Every Fruit by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The score** — each fruit gets s = weight − 2 × diameter: one multiplication, one subtraction
- **A lemon** — the (120 g, 66 mm) lemon scores 120 − 132 = −12; every lemon lands below zero
- **An orange** — the (145 g, 65 mm) orange scores 145 − 130 = +15; every orange lands above zero
- **The gap** — lemon scores span −17 to −10 and orange scores span +10 to +17: a 20-point gap
- **The rule** — score below 0 goes to the lemon bin, above 0 to the orange bin; one number sorts all

*Example (italic):* The heaviest lemon (135 g, 73 mm) scores −11 and the lightest orange (125 g, 57 mm) scores +11 — neighbors on the weight ruler, opposites on the score ruler.

**Key point:** LDA compresses many measurements into one discriminant score per row, and the whole decision becomes a single threshold on that score.

### Visualization (canvas `c2`, 720×300)

Three horizontal ruler strips showing the same 16 fruits projected onto weight alone, diameter alone, and the LDA score.

- **Title (bold 15px, `#1a5276`, top center):** "Three Rulers for the Same 16 Fruits".
- **Strips:** three 2px `#999` horizontal lines from x=70, width 580, at y=95 (weight), y=170 (diameter), y=245 (score); each strip has a bold 12px `#444` heading above its left end; lemon dots yellow `#c98500` 5px drawn 6px above the line, orange dots orange `#d95926` 5px drawn 6px below the line.
- **Strip 1 (weight, range 95–165 g):** heading "weight only (g) — mean gap 25"; lemons `[100, 110, 120, 130, 105, 125, 115, 135]`, oranges `[125, 135, 145, 155, 130, 150, 140, 160]`; shaded rect `rgba(213,81,129,0.15)` over 125–135 spanning 26px above/below the line; magenta `#d55181` bold 12px annotation "overlap zone: 6 of 16 fruits"; end tick labels "95" and "165" 11px `#6b7280`.
- **Strip 2 (diameter, range 55–75 mm):** heading "diameter only (mm)"; lemons `[58, 62, 66, 70, 61, 69, 64, 73]`, oranges `[57, 61, 65, 69, 60, 68, 63, 72]`; magenta `#d55181` bold 12px annotation "means 65.4 vs 64.4 — useless alone"; end tick labels "55" and "75".
- **Strip 3 (score, range −20 to +20):** heading "score = weight − 2 × diameter"; lemons `[-16, -14, -12, -10, -17, -13, -13, -11]`, oranges `[11, 13, 15, 17, 10, 14, 14, 16]`; dashed ink `#1a5276` vertical tick at 0 labeled "threshold 0" (bold 12px); green `#008300` bold 13px annotation "gap of 20 points, zero mixing"; end tick labels "−20" and "+20".

## Not the Same Line as PCA

**Tags:** `where it's used` (blue), `LDA vs PCA` (orange), `dimensionality reduction` (green)

- **PCA's goal** — PCA points along the biggest spread of all fruits pooled together, ignoring labels
- **The size axis** — here that long axis is overall size, roughly 0.9 × weight + 0.4 × diameter
- **The mix-up** — projected onto that size axis, 6 of 16 fruits land in the shared zone 135–151
- **LDA's goal** — LDA reads the labels and points where class means pull apart against the spread
- **Direction count** — with C classes LDA yields at most C − 1 directions; two classes give one line

*Example (italic):* Same 16 fruits, two projections: PCA's best axis leaves 6 fruits mixed, LDA's axis leaves none.

**Key point:** PCA preserves variance and never looks at class labels; LDA gives up variance to maximize class separation. Different objective, and usually a different line.

### Visualization (canvas `c3`, 720×300)

Dual panel split by a dashed divider at x=360: the scatter with both candidate axes (left) and the two resulting 1D projections (right).

- **Title (bold 15px, `#1a5276`, top center):** "Same Fruits, Two Projections: Keep Variance vs Separate Classes".
- **Left panel (scatter):** same 16 points as `c1` (lemons yellow `#c98500`, oranges orange `#d95926`, 4px dots); origin x=50, plot width 290, baseline y=245, plot height 175; x maps weight 90–170, y maps diameter 54–76; no tick labels, just 1px `#e5e9ef` frame; overall mean at (130, 64.9) marked with a 4px ink dot.
- **Axes drawn through the mean (pixel space):** PCA axis aqua `#199e70` 3px double-headed arrow at ~45° up-to-the-right, ±105 px, bold 12px aqua label "PCA: long axis (size)"; LDA axis violet `#4a3aa7` 3px double-headed arrow at ~77° down-to-the-right (direction of weights (1, −2)), ±85 px, bold 12px violet label "LDA: separating axis".
- **Right panel (two strips):** 2px `#999` lines from x=400, width 280, at y=110 and y=210; lemon dots 6px above the line, orange dots 6px below, colors as left.
- **Strip 1 (PCA scores, range 110–175):** heading bold 12px `#444` "projected on PCA axis (0.9w + 0.4d)"; lemons `[113.2, 123.8, 134.4, 145.0, 118.9, 140.1, 129.1, 150.7]`, oranges `[135.3, 145.9, 156.5, 167.1, 141.0, 162.2, 151.2, 172.8]`; shaded rect `rgba(213,81,129,0.15)` over 135.3–150.7; magenta `#d55181` bold 12px label "6 of 16 mixed".
- **Strip 2 (LDA scores, range −20 to +20):** heading "projected on LDA axis (w − 2d)"; lemons `[-16, -14, -12, -10, -17, -13, -13, -11]`, oranges `[11, 13, 15, 17, 10, 14, 14, 16]`; green `#008300` bold 13px label "0 mixed".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Caption (12px `#444`, bottom of left panel):** "PCA axis: long axis of the pooled cloud (illustrative)".

## Chasing the Gap and Ignoring the Spread

**Tags:** `common mistake` (red), `Fisher ratio` (green)

- **Tempting shortcut** — pick the axis with the biggest gap between the class means and stop there
- **Similar gaps** — weight's mean gap is 25 g (117.5 vs 142.5); the LDA score's mean gap is 27
- **Different spreads** — within a class, weights spread about ±11.5 g but scores only about ±2.2
- **Fisher's ratio** — LDA maximizes gap ÷ spread: roughly 2.2 on weight vs 12.3 on the score
- **Fine print** — LDA assumes the classes share a similar spread; wildly unequal spreads mislead it

*Example (italic):* On weight the ±11.5 g class bands nearly touch (129 vs 131); on the score the bands sit more than 20 points apart.

**Common mistake:** Ranking directions by the mean gap alone. A big gap with a big spread still mixes classes — LDA's objective is the ratio of between-class gap to within-class spread.

### Visualization (canvas `c4`, 720×300)

Two ruler rows comparing mean gap and ±1 sd class bands for the weight axis (top) and the LDA score (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "Two Similar Gaps, Very Different Spreads".
- **Rulers:** two 2px `#999` horizontal lines from x=70, width 560, at y=110 and y=215; bold 12px `#444` heading above each left end.
- **Row 1 (weight, range 95–165 g):** heading "ruler 1: weight — gap 25, spread ±11.5"; lemon band 106–129 filled `rgba(201,133,0,0.25)` (18px tall, centered on the line) with a yellow `#c98500` 6px mean dot at 117.5; orange band 131–154 filled `rgba(217,89,38,0.25)` with an orange `#d95926` 6px mean dot at 142.5; ink 2px bracket between the two mean dots above the line, bold 12px label "gap 25"; magenta `#d55181` bold 13px label at the right end "separation = 25 ÷ 11.5 ≈ 2.2".
- **Row 2 (score, range −20 to +20):** heading "ruler 2: LDA score — gap 27, spread ±2.2"; lemon band −15.45 to −11.05 filled `rgba(201,133,0,0.25)` with mean dot at −13.25; orange band 11.55 to 15.95 filled `rgba(217,89,38,0.25)` with mean dot at 13.75; ink bracket between means labeled "gap 27" (bold 12px); green `#008300` bold 13px label at the right end "separation = 27 ÷ 2.2 ≈ 12.3".
- **Takeaway (bold 13px green `#008300`, centered at y=285):** "similar gaps — the spread decides; LDA maximizes gap ÷ spread".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data arrays are hardcoded literals (no `Math.random()`); every number in the text bullets matches the chart data above.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
