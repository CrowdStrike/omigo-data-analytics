# Gradient Boosting

**Page type:** detail page (tutorial card-sections: one h2 per section, two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Gradient Boosting

**Subtitle:** Predict house prices with one tree, then train the next tree on the leftover errors, then the next on what is still left — each tree is a small correction added to the total

## Predict the Price, Then Fit the Miss

Tags: `core idea` (blue), `running example` (green)

- **Four houses** — true prices $200k, $240k, $300k, $360k
- **First guess** — predict the average, $275k, for every house
- **Measure the miss** — house A is over-priced by 75, house D under-priced by 85
- **Tree 2's target** — it is trained on the misses, not on the prices
- **Add, don't replace** — prediction = first guess + correction 1 + correction 2

*Example (italic):* For house A the answer builds up as 275 − 55 − 25 = 195 — three small pieces, one price.

**Key point:** Gradient boosting is a running total: each new tree predicts the error left by everything before it, and its output is added on top.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: staged predictions per house closing in on the true price.

- **Title (bold 15px, `#1a5276`, top center):** "Each Added Tree Pulls the Prediction Toward the True Price".
- **Data:** houses A, B, C, D; true prices `[200, 240, 300, 360]`; stage 0 predictions `[275, 275, 275, 275]`; after tree 1 `[220, 220, 330, 330]`; after tree 2 `[195, 245, 305, 355]`.
- **Layout:** 4 groups at x = 95, 250, 405, 560 (group width 110), 3 bars per group (30px wide, 5px gap), baseline y=240, chart height 170, y scale 0–400. Bars at 0.7 alpha, colors per stage: gray `#6b7280` (guess), aqua `#199e70` (+ tree 1), green `#008300` (+ tree 2).
- **Y-axis:** gray `#444` labels "$0k", "$100k", "$200k", "$300k", "$400k" at ticks 0/100/200/300/400; thin `#999` baseline from x=55 to x=695.
- **True-price lines:** per group, dashed horizontal line (dash 5/3, width 2) in `#c0392b` across the group at the true price, with bold red label "true 200" / "true 240" / "true 300" / "true 360" above; bold dark label "house A"…"house D" below the baseline.
- **Legend (bottom row, 12px swatches at 0.7 alpha):** "guess: mean 275" (gray), "+ tree 1" (aqua), "+ tree 2" (green); to the right, bold green `#008300` 13px text "all within $5k".

## Four Houses, Two Small Trees, by Hand

Tags: `worked example` (green), `core idea` (blue)

- **Stage 0** — predict 275 everywhere; misses: A −75, B −35, C +25, D +85
- **Tree 1** — one split on size: small houses {A,B} get −55, large {C,D} get +55
- **After tree 1** — predictions 220, 220, 330, 330; misses shrink to ±20 and ±30
- **Tree 2** — one split on location: {A,C} get −25, {B,D} get +25
- **After tree 2** — predictions 195, 245, 305, 355; every miss is now just $5k

*Example (italic):* The total absolute miss falls 220 → 100 → 20 ($k) — each tree eats most of what is left.

**Key point:** A tree's leaf value is the average miss of the houses in that leaf — tree 1's "small houses" leaf averages −75 and −35 into −55.

### Visualization (canvas `c2`, 720×300)

Three small residual bar panels (one per stage) sharing a zero line, showing misses shrinking.

- **Title (bold 15px, `#1a5276`, top center):** "The Misses Each Tree Was Trained On — Shrinking Every Stage".
- **Stages (label, residuals A–D, panel x origin, total):**
  - "stage 0: guess 275" — `[-75, -35, 25, 85]`, x=70, total 220
  - "after tree 1" — `[-20, 20, -30, 30]`, x=290, total 100
  - "after tree 2" — `[5, -5, -5, 5]`, x=510, total 20
- **Bars:** width 34, gap 10, zero line at y=155, scale 1.05 px per unit; per-house colors blue `#2a78d6` (A), violet `#4a3aa7` (B), aqua `#199e70` (C), orange `#d95926` (D), 0.7 alpha. Positive bars go up, negative down; signed value labels (e.g. "+85", "-75") in bold dark on the outward side, house letters A–D in gray on the inner side. Thin `#999` zero line per panel.
- **Panel captions:** bold ink stage label centered under each panel, plus gray "total miss $220k" / "$100k" / "$20k".
- **Arrows:** two green `#008300` right-pointing arrows along the zero line between panels (from x 255→275 and 475→495).
- **Annotation (bold green 13px, centered near top):** "220 → 100 → 20: each tree fits what is left, so the leftovers keep shrinking".

## The XGBoost / LightGBM Family in Practice

Tags: `where it's used` (blue), `watch out` (orange)

- **Same recipe** — XGBoost, LightGBM, CatBoost are this idea plus speed and regularization engineering
- **Learning rate** — real setups add only a fraction (say 0.1) of each correction: many tiny steps
- **Tabular default** — for spreadsheet-style data it is usually the first serious model to try
- **Hundreds of trees** — small steps mean 100–1000 trees is normal, not a red flag
- **Stop by validation** — training error always falls; add trees until held-out error stops falling

*Example (italic):* In the chart, held-out error bottoms out near 200 trees at about $27k — more trees only polish the training set.

**Key point:** The learning rate trades speed for safety — smaller steps need more trees but are far less likely to overshoot into noise.

### Visualization (canvas `c3`, 720×300)

Two-line chart: training vs held-out error as trees are added.

- **Title (bold 15px, `#1a5276`, top center):** "Average Price Error vs Number of Trees (learning rate 0.1)".
- **Data:** x points (number of trees) `[0, 25, 50, 100, 200, 400]`, evenly spaced; training error `[90, 40, 25, 14, 7, 3]`; held-out error `[90, 45, 33, 28, 27, 31]`. Y scale 0–100.
- **Axes:** padding top 52 / bottom 52 / left 62 / right 170; `#999` L-shaped axes; y labels "$0k", "$25k", "$50k", "$75k", "$100k" with light gridlines `#e5e9ef`; x tick labels are the tree counts; x-axis title "number of trees" bottom center.
- **Series:** training in blue `#2a78d6`, held-out in orange `#d95926`, both line width 3 with 4px dots.
- **Vertical marker:** dashed green `#008300` vertical line (dash 5/4, width 2) at the 200-trees point.
- **Annotations:** bold green 13px "held-out floor: ~$27k at 200 trees — stop adding here" near the top; bold `#c0392b` 12px "train → $3k: memorized" near the last training point.
- **Legend (right side):** blue swatch "training error", orange swatch "held-out error", and gray 11px note "illustrative numbers".

## The Mix-Up: It Is Not a Random Forest

Tags: `common mistake` (red), `rule of thumb` (blue)

- **Forest** — every tree predicts the full price on its own; the answers are averaged
- **Boosting** — every tree after the first predicts leftover error; the answers are added
- **Alone test** — one forest tree is a rough but usable model; tree 2 alone outputs −25, useless by itself
- **Tree size** — forest trees grow deep and independent; boosted trees stay small and ordered
- **Failure mode** — forests rarely overfit with more trees; boosting eventually will

*Example (italic):* Both are "300 trees on tabular data" — but one averages 300 prices, the other sums 300 corrections.

**Common mistake:** Reading a boosted tree's leaf as a price — it is a correction to a running total, meaningless outside the sequence.

### Visualization (canvas `c4`, 720×300)

Two-panel box-and-arrow diagram comparing forest averaging vs boosting summing, split by a dashed vertical divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "House A: a Forest Averages Prices, Boosting Sums Corrections".
- **Left panel (heading bold blue `#2a78d6` 13px "RANDOM FOREST" at x=180):** three blue-outlined boxes stacked at x=40 (120×44 each) labeled "tree 1 / says 210", "tree 2 / says 190", "tree 3 / says 205"; gray arrows converge into a green `#008300` box "average / = 202" at (225,126). Caption in gray 12px, two lines centered at x=180: "every tree answers the full question:" / ""what is the price?" — then vote".
- **Right panel (heading bold orange `#d95926` 13px "GRADIENT BOOSTING" at x=540):** boxes in a row — gray "guess / 275" (390,80), violet `#4a3aa7` "tree 1 / −55" (510,80), violet "tree 2 / −25" (630,80) — with bold orange "+" signs between them; gray arrows down into a green box "running total / 275 − 55 − 25 = 195" at (475,165). Caption in gray, two lines centered at x=540: "trees 1 and 2 answer a different question:" / ""how wrong is the total so far?"". Bold `#c0392b` 12px right-aligned annotation near the tree-2 box: "−25 alone is not a price".
- **Box style:** near-transparent fill `rgba(0,0,0,0.02)`, 2px colored stroke, bold colored 12px label with dark 12px sub-line; arrows are 2px lines with filled triangular heads.

## Regeneration instructions

- **Template/layout:** tutorials topic page (simplest-form concept tutorial). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` (full width, border-collapse) with one row: `td.text-col` 50% (tags, bullets, `.example`, `.key-point`) and `td.viz-col` 50% (one canvas 720×300).
- **Text column structure:** `.tags` row of pill spans (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem, weight 600, radius 10px); `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`; italic `.example` paragraph (`#555`, 0.9rem); `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>` lead-in.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; all data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue/ink `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; chart series use the P palette above plus `#c0392b` for alarm-red annotations.
- **Links:** none on this page (no cross-page links, no nav); in regenerated HTML any card links elsewhere use `.html` extensions.
