# Linear Regression as ML

**Page type:** detail page (tutorial card-sections: one `<h2>` per section, two-column `table.layout` — text 50% / viz 50%; section 2 holds both canvases side by side in a `.viz-pair` flex row.)
**HTML title tag:** Linear Regression as ML

**Subtitle:** The same straight line from statistics class — now trained on past deliveries and judged only on orders it has never seen

## Predicting a Delivery Before It Happens

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — a pizza shop logs every delivery: distance, order size, minutes taken
- **The pattern** — farther trips and bigger orders take longer, in a straight-line way
- **The line** — minutes = 10 + 4×km + 2×items, fitted to the shop's past orders
- **Train** — fitting that line to old orders is what ML calls training
- **Predict** — plug a new order's km and items into the line before the driver leaves

*Example (italic):* A 3 km, 2-item order gets a promise of 10 + 12 + 4 = 26 minutes — before anything is cooked.

**Linear regression as ML:** fit a line on the past (training), read it out on the future (prediction). Same line-fitting as statistics — the job changed, not the math.

### Visualization (canvas `c1`, 720×300)

Scatter plot of the six training deliveries with the learned line.

- **Title (bold 15px, `#1a5276`, top center):** "Six Past Deliveries and the Line Learned From Them".
- **Axes:** L-shaped gray axes; padding top 46 / bottom 52 / left 62 / right 30. X: 0–7 km with 12px mute labels "0 km" … "7 km"; Y: 0–50 with labels 0, 10, 20, 30, 40, 50. Axis captions (12px mute): "distance to the customer" bottom center; rotated "delivery minutes" on the left.
- **Training points (green `#008300`, radius-6 dots), data `[km, items, actual minutes]`:** `[1,1,17], [2,2,21], [3,1,25], [4,3,33], [5,2,33], [6,4,43]`; each dot annotated above with its item count in 12px mute ("1 item", "2 items", …).
- **Learned line:** blue `#2a78d6`, width 3, drawn at items=2 (minutes = 14 + 4×km) from km 0.4 to 6.6.
- **Line labels (near y=46 level, left-aligned):** bold blue 13px "learned line: minutes = 10 + 4×km + 2×items"; mute 12px "(line drawn for 2-item orders)".

## Training on Six Orders, Testing on Two

**Tags:** `worked example` (green), `arithmetic` (blue)

- **Training set** — six past orders the line is allowed to see while being fitted
- **The fit** — the line misses each of the six by about 1 minute (train error = 1.0)
- **Held-out set** — two orders kept in a drawer; the line never sees them
- **Order A** — 3 km, 2 items: predict 10 + 12 + 4 = 26 min; it took 28 (off by 2)
- **Order B** — 5 km, 4 items: predict 10 + 20 + 8 = 38 min; it took 35 (off by 3)
- **The score** — held-out error = (2 + 3) / 2 = 2.5 minutes off, on average

*Example (italic):* The promise printed on order A's receipt is 26 minutes; reality said 28.

**Hand-checkable:** the number that matters is 2.5 minutes — the average miss on the two orders the line never trained on.

### Visualization (canvas `c2a`, 310×300)

Grouped bar chart: predicted vs actual for the six training orders.

- **Title (bold 15px, `#1a5276`):** "Training Orders: Fit".
- **Subtitle annotation (bold ink 12px, centered under the title):** "each miss ≈ 1 min — train error 1.0".
- **Data:** predicted `[16, 22, 24, 32, 34, 42]` (blue `#2a78d6`), actual `[17, 21, 25, 33, 33, 43]` (green `#008300`); six paired bars labeled "#1"–"#6" below (12px mute); y scale 0–50 with tick labels every 10; padding top 56 / bottom 52 / left 46 / right 14.
- **Legend (top left):** blue swatch "predicted", green swatch "actual" (12px).
- **Caption (12px mute, bottom center):** "the six orders the line trained on".

### Visualization (canvas `c2b`, 310×300)

Grouped bar chart: predicted vs actual for the two held-out orders.

- **Title (bold 15px, `#1a5276`):** "Held-Out Orders: Test".
- **Subtitle annotation (bold magenta 12px, centered under the title):** "held-out error = (2 + 3) / 2 = 2.5 min".
- **Data:** two-line x labels "Order A" / "3 km, 2 items" and "Order B" / "5 km, 4 items"; predicted `[26, 38]` (blue `#2a78d6`, 42px bars), actual `[28, 35]` (orange `#d95926`); bold 13px value labels above each bar; magenta (`#d55181`) bold "off by +2" / "off by −3" above each pair; y scale 0–50; padding top 78 / bottom 62 / left 46 / right 14.
- **Legend (top left, under the annotation):** blue swatch "predicted", orange swatch "actual".
- **Caption (12px mute, bottom center):** "the two orders the line never saw".

## Why the Held-Out Orders Are the Whole Point

**Tags:** `where it's used` (blue), `caution` (orange)

- **Training error flatters** — the line was bent to fit those six orders; 1.0 min is a best case
- **Held-out error is honest** — 2.5 min is what tomorrow's customers will actually feel
- **Overfitting** — a wiggly curve can hit all six training points and still miss new orders badly
- **The habit** — every ML project splits its data: fit on one part, score on the other
- **Same loop everywhere** — house prices, delivery ETAs, demand forecasts all train this way

*Example (italic):* A curve that memorized all six training orders promised order B 45 minutes; it took 35.

**Key point:** report the error on data the model never saw — training error only says how well the line bent, not how well it will predict.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: training vs held-out error for a straight line and a wiggly (memorizing) curve.

- **Title (bold 15px, `#1a5276`, top center):** "Two Models, Two Report Cards (held-out is the honest one)".
- **Data:** groups "straight line" and "wiggly curve (memorizes)"; training error `[1.0, 0.0]` (blue `#2a78d6`, 92px bars), held-out error `[2.5, 8.0]` — green `#008300` for the straight line, magenta `#d55181` for the wiggly curve. Bold 13px value labels above bars; y scale 0–10 with ticks every 2; padding top 52 / bottom 56 / left 64 / right 30.
- **Legend (top left):** blue swatch "training error (min)", green swatch "held-out error (min)".
- **Annotation (bold magenta 13px, at ~72% width, ~28% height):** "perfect in training, worst where it counts".
- **Caption (12px mute, bottom center):** "wiggly-curve numbers illustrative — average minutes off per order".

## Statistics Class vs the ML Framing

**Tags:** `common confusion` (red), `core idea` (blue)

- **Same math** — least squares finds the same 10, 4, and 2 under either name
- **Statistics asks** — is the 4-min-per-km effect real, and how uncertain is it?
- **ML asks** — how many minutes off will the next prediction be?
- **Different report card** — p-values and intervals vs error on held-out orders
- **Both useful** — explain the kitchen with one, promise delivery times with the other

*Example (italic):* The +4 min per km answers the manager's "why slow?"; the 2.5-min held-out error answers the customer's "when?".

**The confusion:** linear regression is not "old stats" vs "new ML" — one tool, two questions. The ML question is only answered on held-out data.

### Visualization (canvas `c4`, 720×300)

Split panel: statistics view (coefficients) on the left, ML view (one held-out number) on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Fitted Line, Two Questions".
- **Divider:** vertical dashed gray line (`#bdc3c7`, dash 4/3) at x=370.
- **Left panel (header bold blue 13px at (190,52)):** "statistics asks: what does each part mean?" — three horizontal blue bars (0.75 alpha, 20px tall, scale max 10 over 170px, starting x=118) with mute right-aligned labels and bold value texts: "base time" 10 → "10 min"; "per km" 4 → "+4 min"; "per item" 2 → "+2 min". Caption (12px mute, centered): "each coefficient, read as a story about deliveries".
- **Right panel (header bold green 13px at (545,52)):** "ML asks: how wrong on the next order?" — a 230×120 green-outlined box (2px) containing bold green 44px "2.5 min" and 12px "average miss on held-out orders". Below (bold ink 12px, two lines): "one tool, two report cards —" / "the ML one is earned on unseen data".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). h1 (no index number) + `.subtitle`, then four `.card-section` blocks each with an `<h2>` and a `table.layout`. Every section uses two columns (`td.text-col` 50% / `td.viz-col` 50%). One section places canvases `c2a`/`c2b` (310×300 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Left column per section:** `.tags` pill row, `<ul>` of one-line bullets opening with `<b>bold term</b>` (bold in `#1a5276`), one italic `.example` line, one `.key-point` callout.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic sizes 720×300 (c1, c3, c4) and 310×300 (c2a, c2b), scaled with `window.devicePixelRatio` via a shared `setup(id)` helper that reads the width/height attributes (backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates). Shared training data array `TRAIN = [[1,1,17],[2,2,21],[3,1,25],[4,3,33],[5,2,33],[6,4,43]]` ([km, items, actual minutes]); line predictions 16, 22, 24, 32, 34, 42. Data hardcoded, no `Math.random()`; invented numbers labeled "illustrative".
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions.
