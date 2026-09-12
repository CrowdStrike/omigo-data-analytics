# PCA: Squashing Dimensions

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table layout text left 50% / canvas right 50%)
**HTML title tag:** PCA: Squashing Dimensions

**Subtitle:** Find the direction your data varies the most, keep positions along it, and throw the rest away — fewer columns, most of the story

## Height and Weight Are Mostly One Story

Tags: `core idea` (blue), `variance` (green), `running example` (orange)

- **The data** — height and weight for 30 customers, plotted as a cloud (illustrative)
- **The shape** — the cloud is a tilted cigar: tall people tend to be heavier
- **The insight** — most of the spread runs along one diagonal direction: overall "body size"
- **PC1** — that long axis; a single number per person that captures most of both columns
- **PC2** — the short axis across it: "heavy for one's height", small and often droppable

*Example (italic):* Two columns collapse into one "size" score, and the cloud barely notices the loss.

**Key point:** **PCA:** rotate to the directions of largest spread (principal components), keep the first few, drop the rest. It compresses columns, losing as little variation as possible.

### Visualization (canvas `c1`, 720×300)

Scatter plot of 30 customers with PC1/PC2 arrows overlaid.

- **Title (bold 15px `#1a5276`, center):** "30 Customers: the Cloud Is a Tilted Cigar (illustrative)".
- **Shared point data `PTS` (height dev cm, weight dev kg), cigar along (0.8, 0.6):** `[-14,-12], [-12,-7], [-11,-10], [-9,-5], [-8,-8], [-7,-3], [-6,-6], [-5,-10], [-4,-1], [-3,-4], [-2,-3], [-1,1], [0,-2], [0,2], [1,0], [2,3], [3,1], [4,4], [5,2], [5,7], [6,5], [7,3], [8,7], [9,5], [10,5], [11,9], [12,7], [13,11], [14,9], [-10,-4]`. Dots 5px, `rgba(42,120,214,0.65)`. Center (360, 165), scale 8 px/unit on both axes (equal, so PCs render perpendicular).
- **Axes through the mean:** light gridline-colored (`#e5e9ef`) horizontal and vertical lines; labels (12px `#6b7280`): "height vs avg →" and "weight vs avg ↑".
- **PC1 arrow:** green `#008300`, width 3.5, double-length line through the center along data direction (0.8, 0.6), half-length 160px, filled arrowhead; label (bold 13px): 'PC1 — "size": 90% of spread'.
- **PC2 arrow:** orange `#d95926`, along the perpendicular (−0.6, 0.8), half-length 70px; label "PC2: 10%".
- **Annotation (bold 13px green, bottom left area):** "keep PC1, drop PC2 — one column, most of the story".

## Projecting Two Customers by Hand

Tags: `worked example` (green), `projection` (blue)

- **Center first** — work in differences from the average height and average weight
- **The direction** — say PCA finds PC1 = (0.8, 0.6): 0.8 parts height, 0.6 parts weight
- **Customer P** — +10 cm and +5 kg above average: score = 0.8×10 + 0.6×5 = 11
- **Customer Q** — −5 cm and −10 kg: score = 0.8×(−5) + 0.6×(−10) = −10
- **That's projection** — each score is where the person's shadow lands on the PC1 line
- **Bookkeeping** — here PC1 keeps 90% of the total spread, PC2 the remaining 10%

*Example (italic):* A PC1 score is just a weighted sum — two multiplications and one addition per person.

**Key point:** **Hand-checkable:** "compressing to 1 column" means replacing (height, weight) by that single weighted sum. Nothing more mysterious happens inside PCA.

### Visualization (canvas `c2`, 720×300)

Projection diagram: the same 30-point cloud with perpendicular drops onto the PC1 line; customers P and Q highlighted.

- **Title (bold 15px `#1a5276`, center):** "Projection: Each Point's Shadow on the PC1 Line".
- **PC1 line:** green `#008300`, width 2.5, through the center (360, 165) along unit direction (0.8, 0.6) from t = −18 to +18 data units; label "PC1 line" (bold 12px green) near its upper end.
- **All 30 points (`PTS`, same array as c1):** faint blue dots 4px `rgba(42,120,214,0.35)`, each with a thin gray connector `rgba(107,114,128,0.4)` to its projection point — a 3px green dot `rgba(0,131,0,0.5)` on the PC1 line (projection t = 0.8x + 0.6y).
- **Highlighted customers:** P = (10, 5) in violet `#4a3aa7`, 7px point and 5px projected dot, width-2 connector, label (bold 13px): "P: score 0.8×10 + 0.6×5 = 11". Q = (−5, −10) in magenta `#d55181`, same styling, label "Q: score = −10".
- **Annotation (bold 13px green, bottom):** "the green dots are the compressed dataset: one number per person".

## From 50 Survey Questions to 3 Scores

Tags: `where it's used` (blue), `compression` (green)

- **The problem** — a 50-question customer survey: too many columns to model or plot
- **Questions overlap** — "I like discounts" and "I hunt for coupons" move together
- **PCA's answer** — here PC1 keeps 42% of the spread, PC2 18%, PC3 11% (illustrative)
- **Three scores** — 71% of the variation in 50 answers survives in just 3 columns
- **Downstream wins** — models train faster, distances mean more, plots become possible
- **Without it** — 50 correlated columns feed the curse of dimensionality and unstable fits

*Example (italic):* The elbow in the chart is the standard "how many components?" reading: keep the bars before the flat tail.

**Key point:** **Where you meet it:** image pixels, survey batteries, sensor bundles, gene panels — any table whose columns are many and correlated is a PCA customer.

### Visualization (canvas `c3`, 720×300)

Scree plot: per-component bars plus a cumulative line.

- **Title (bold 15px `#1a5276`, center):** "50-Question Survey: Spread Kept per Component (illustrative)".
- **Bars (PC1–PC8, width 40):** values `[42, 18, 11, 6, 4, 3, 2, 2]` (percent). First three bars blue `#2a78d6` (kept), remaining five `rgba(107,114,128,0.35)` (dropped). Value labels 12px `#222` above bars; x labels "PC1"…"PC8". Padding: top 55, bottom 58, left 65, right 180.
- **Y-axis:** 0–100% with gridlines (`#e5e9ef`) and labels at 0/25/50/75/100% (12px `#666`).
- **Cumulative line (orange `#d95926`, width 3, 4px dots):** `[42, 60, 71, 77, 81, 84, 86, 88]`.
- **Annotation (bold 13px orange, near the PC3 cumulative point):** "3 scores keep 71% of 50 questions".
- **Legend (right, x = w−168):** blue swatch "kept components"; `rgba(107,114,128,0.45)` swatch "dropped (flat tail)"; orange swatch "cumulative %" (12px `#222`).

## The Unit Trap: Grams Hijack the Direction

Tags: `common mistake` (red), `scaling` (orange)

- **PCA counts variance** — whichever column has the biggest numbers wins the direction
- **Weight in kg** — spreads of cm and kg are comparable; PC1 is the sensible diagonal
- **Weight in grams** — the same data ×1000: weight variance explodes a million-fold
- **PC1 flips** — it now points almost straight along weight; height is ignored
- **The fix** — standardize each column (mean 0, spread 1) unless columns share one unit

*Example (italic):* Nothing about the customers changed — only the unit did, and the "main direction" swung around.

**Key point:** **Second trap:** PC1 is the direction of most variance, not most usefulness — a target you care about can hide in a small component PCA would happily throw away.

### Visualization (canvas `c4`, 720×300)

Two-panel scatter comparison: PC1 direction under standardized units vs weight in grams, split by a dashed vertical divider at x=360.

- **Title (bold 15px `#1a5276`, center):** "Same Customers, Different Units: PC1 Swings to the Loud Column".
- **Divider:** vertical dashed `#bdc3c7` line (dash 4/3) from y=40 to h−15.
- **Both panels:** the same `PTS` points as blue 4px dots `rgba(42,120,214,0.55)`, 8 px/unit x, 4.5 px/unit y; PC1 shown as a width-3.5 double-headed-length arrow (half-length 100px) with filled arrowhead.
- **Left panel (center 185, 160):** y-stretch 1, PC1 along (0.8, 0.6) in green `#008300`; caption (bold 12px, y=268): "weight in kg: PC1 = size (height + weight)".
- **Right panel (center 540, 160):** y-stretch 1.8 (variance exploded vertically), PC1 along (0.02, 1) — nearly vertical — in magenta `#d55181`; caption: "weight in grams: PC1 ≈ weight alone".
- **Annotations (bold 13px):** magenta at (540, 60): "height is ignored"; green at bottom center (y=292): "scale columns before PCA — units decide the winner otherwise".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, bottom border 2px solid `#2980b9`), `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: `.text-col` (50%) text, `.viz-col` (50%) canvas 720×300.
- **Text cell structure:** `.tags` row of pill spans first, then `<ul>` of one-line bullets each opening with `<b>` term (bold terms colored `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. blue = `rgba(26,82,118,0.12)`/`#1a5276`; green = `rgba(39,174,96,0.15)`/`#27ae60`; red = `rgba(231,76,60,0.12)`/`#e74c3c`; orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 logical; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via shared `setup(id)` helper; the 30-point `PTS` array is shared by canvases c1, c2, and c4. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
