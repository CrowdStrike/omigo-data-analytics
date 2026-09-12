# R-Squared for Prediction

**Page type:** detail page (tutorial layout: `.card-section` blocks, each with a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** R-Squared for Prediction

**Subtitle:** What "explains 80% of the variance" really means — and why a high R-squared can still predict badly

## The Laziest Predictor: Guess the Average Price

**Tags:** `core idea` (blue), `baseline mean` (blue)

- **The setup** — you predict house prices for 5 houses that sold for $200k to $400k
- **The lazy rival** — a "model" that ignores the house and always guesses the average: $300k
- **Its misses** — $100k, $50k, $0, $50k, $100k off; squared and summed: 25,000 (in $k²)
- **The bar to beat** — that 25,000 is the total squared error around the mean, the baseline score
- **R² in words** — what fraction of the lazy rival's squared error does your model erase?

*Example:* A model that only matches "always guess $300k" has learned nothing about houses — R² = 0.

**Key point:** R² is a comparison, not an absolute grade — your model is scored against the guess-the-average baseline.

### Visualization (canvas `c1`, 720×300)

Dot plot of five sale prices against a dashed mean line, with vertical error segments to the mean.

- **Title (bold 15px, `#1a5276`, top center):** "Five Sale Prices vs the \"Always $300k\" Guess"
- **Padding:** top 52, bottom 56, left 70, right 30. Gray `#999` L-frame axes. Y range 150–450 ($k).
- **Mean line:** dashed orange `#d95926` (dash 6/4, width 2) horizontal at $300k, labeled (bold 12px orange, left): "baseline: guess $300k every time".
- **Data:** prices `[200, 250, 300, 350, 400]` at 5 evenly spaced x positions; blue `#2a78d6` dots radius 7; bold 12px `#444` price labels "$200k"…"$400k" (above dot if price ≥ 300, below otherwise); x labels "house 1"…"house 5" (12px `#222`).
- **Error segments:** magenta `#d55181` vertical lines (width 2) from mean to each dot; midpoint labels 12px magenta for nonzero misses: "off $100k", "off $50k", (none for house 3), "off $50k", "off $100k".
- **Y-axis title (rotated 12px `#444`):** "sale price, $k".
- **Caption (bold 13px magenta, bottom center):** "squared misses total 25,000 — the bar any model must beat"

## From 25,000 Down to 4,500: R² = 0.82

**Tags:** `worked example` (green), `variance explained` (blue)

- **Your model** — uses size and location, and misses each of the 5 prices by $30k
- **Its score** — squared errors: 900 each, five times; total = 4,500 (in $k²)
- **The formula** — R² = 1 − (model error / baseline error) = 1 − 4,500/25,000
- **The answer** — R² = 0.82: the model erased 82% of the baseline's squared error
- **Plain reading** — "explains 82% of the variance" = "82% less squared error than guessing the mean"

*Example:* House 1 sold for $200k; the mean-guesser was $100k off, your model $30k off.

**Key point:** "Variance explained" is just the share of the mean-guesser's squared error your model removed — nothing more mystical.

### Visualization (canvas `c2`, 720×300)

Two-bar comparison of total squared error with a computation panel on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Total Squared Error: Baseline vs Your Model"
- **Padding:** top 56, bottom 60, left 90, right 240. Gray `#999` L-frame axes. Y max 27,000.
- **Bars (120px wide, at 28% and 72% of plot width):** "guess-the-mean baseline" = 25,000 in orange `#d95926`; "your model" = 4,500 in green `#008300`. Bold 15px value labels ("25,000", "4,500") above bars in the bar's color; bold 12px `#222` labels below; 12px `#666` sub-captions: "5 misses of $100k, $50k, $0, $50k, $100k" and "5 misses of $30k each (900 × 5)".
- **Y-axis title (rotated 12px `#444`):** "total squared error, $k²".
- **Right computation panel (x = w−220):** bold 13px `#1a5276` "R² = 1 − 4,500 / 25,000"; bold 16px green "R² = 0.82"; 12px gray `#6b7280` "82% of the baseline's" / "squared error erased".

## Same $30k Misses, New Neighborhood, Negative R²

**Tags:** `common mistake` (red), `fit vs predict` (orange)

- **New data** — a tight neighborhood: 5 houses priced $280k–$320k, mean still $300k
- **Small baseline** — the mean-guesser is barely wrong here: squared total only 1,000
- **Same accuracy** — your model still misses by $30k per house: squared total 4,500
- **The shock** — R² = 1 − 4,500/1,000 = −3.5: worse than guessing the average
- **The lesson** — identical dollar accuracy scored 0.82 on spread-out data, −3.5 on tight data

*Example:* The buyer feels the same $30k miss in both neighborhoods — only R² changed its mind.

**Key point:** R² moves when the spread of the data moves, even if your accuracy in dollars never changes — always report an error in real units next to it.

### Visualization (canvas `c3`, 720×300)

Two side-by-side dot-and-whisker panels split by a vertical dashed divider (`#bdc3c7`, dash 4/3) at x=360 from y=40 to h−15. Shared y range 150–450 ($k), panel plot area from y=62 to y=250.

- **Title (bold 15px, `#1a5276`, top center):** "Identical $30k Accuracy — Opposite Verdicts"
- **Each panel:** gray `#999` baseline; dashed orange `#d95926` mean line at $300k (dash 5/4, width 1.5); five blue `#2a78d6` dots (radius 6) with magenta `#d55181` vertical whiskers spanning ±$30k around each price; panel title bold 13px `#1a5276` at y=52; verdict bold 14px at y=272.
  - Left panel (x=50, width 280): prices `[200, 250, 300, 350, 400]`; title "wide: $200k–$400k, baseline 25,000"; verdict "R² = 0.82" in green `#008300`.
  - Right panel (x=400, width 280): prices `[280, 290, 300, 310, 320]`; title "tight: $280k–$320k, baseline 1,000"; verdict "R² = −3.5" in red `#e74c3c`.
- **Caption (12px gray `#6b7280`, bottom center y=292):** "blue dot = sale price · pink whisker = the model's ±$30k miss · dashed = $300k mean"

## A High R² Measured on the Wrong Houses

**Tags:** `fit vs predict` (orange), `rule of thumb` (green)

- **Fit R²** — measured on the houses the model trained on; it only ever rises as you add features
- **Predict R²** — measured on held-out houses the model never saw; this one can fall
- **The trap** — memorizing quirks of the training houses lifts fit R² while predictions rot
- **Not accuracy** — R² = 0.82 does not mean "82% of predictions are right"; it is an error ratio
- **The habit** — quote R² from held-out data, alongside a dollar error like MAE or RMSE

*Example:* Adding "seller's lucky number" as a feature nudges fit R² up — and helps no future prediction.

**Common mistake:** Celebrating a training-set R². The only R² that speaks about prediction is the one computed on data the model never touched.

### Visualization (canvas `c4`, 720×300)

Two-line chart: fit R² vs predict R² as feature count grows (illustrative).

- **Title (bold 15px, `#1a5276`):** "R² on Training Houses vs Held-Out Houses (illustrative)"
- **Padding:** top 52, bottom 56, left 66, right 180. Gray `#999` L-frame axes. X = features 1–10, y = R² 0–1.0.
- **Series (width 3 lines, 4px dots):**
  - Fit (violet `#4a3aa7`): `[0.55, 0.65, 0.72, 0.78, 0.82, 0.85, 0.88, 0.91, 0.93, 0.95]`
  - Predict (green `#008300`): `[0.50, 0.60, 0.68, 0.72, 0.74, 0.73, 0.70, 0.65, 0.58, 0.50]`
- **X ticks:** 1…10 (12px `#222`). Axis titles 12px `#444`: "number of features in the model" (bottom), rotated "R²" (left).
- **Legend (right side, x = w−168):** violet swatch "fit: training houses"; green swatch "predict: held-out".
- **Annotation (bold 12px red `#e74c3c`, right panel):** "fit keeps climbing;" / "prediction turns down"

## Regeneration instructions

- **Layout:** tutorial detail page — h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pill row, a `<ul>` of bold-term bullets (`li b` in `#1a5276`), an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) in a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Shared data:** 5 houses in $k — wide neighborhood prices 200, 250, 300, 350, 400 (mean 300, baseline SS = 25,000); model misses each by $30k (model SS = 4,500, R² = 0.82); tight neighborhood 280–320 (baseline SS = 1,000, R² = −3.5).
- This page has no card links; in regenerated HTML any links would use `.html` extensions.
