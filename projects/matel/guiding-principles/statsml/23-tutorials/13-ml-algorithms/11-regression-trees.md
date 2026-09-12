# Regression Trees

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Regression Trees

**Subtitle:** A regression tree predicts a number by splitting rows into groups and answering with each group's mean — splits are chosen to shrink variance, and these little trees are the atoms inside boosting

## One Ice-Cream Cart, Twelve Days

**Tags:** `core idea` (blue), `leaf = mean` (green), `step predictor` (orange)

- **The cart** — an ice-cream cart logs 12 days of temperature and sales, from 58°F/$125 up to 91°F/$310
- **One guess** — with no tree at all, the best single prediction is the overall mean: $210 every day
- **One split** — asking "is temp < 74.5°F?" sorts the days into a cool group and a hot group
- **Leaf = mean** — cool days now predict their own mean, $140; hot days predict theirs, $280
- **A step, not a line** — the tree's prediction is a flat shelf per leaf, jumping at the split

*Example (italic):* On a 70°F day the tree answers $140; on an 85°F day it answers $280 — every day lands in exactly one leaf.

**Key point:** A regression tree predicts a number by routing each row down yes/no questions to a leaf, then answering with that leaf's mean.

### Visualization (canvas `c1`, 720×300)

Scatter of the 12 days (temperature vs sales) with the single overall-mean line and the one-split step prediction drawn on top.

- **Title (bold 15px, `#1a5276`, top center):** "12 Days at the Cart: One Mean vs One Split".
- **Data:** temps `[58, 61, 64, 67, 70, 73, 76, 79, 82, 85, 88, 91]` (°F); sales `[125, 130, 135, 140, 150, 160, 250, 265, 270, 285, 300, 310]` ($).
- **Axes:** origin x=60, plot width 620, baseline y=250, plot height 195; x maps 55–94°F, y maps $100–$330; axis lines 2px ink `#1a5276`; x tick labels 60/70/80/90 and y tick labels $100/$200/$300, 12px `#444`.
- **Mean line:** dashed `#6b7280` (dash 5/4) horizontal at y-value 210 across the plot, labeled bold 12px `#6b7280` "one guess: mean $210" above its left end.
- **Points:** blue `#2a78d6` filled dots, 5px radius.
- **Split line:** orange `#d95926` dashed (dash 4/3) vertical at x-value 74.5 from plot top to baseline, bold 12px orange label "split: temp < 74.5°F" near the top.
- **Step prediction:** green `#008300` 3px horizontal segment at y-value 140 from x=55 to x=74.5 and at y-value 280 from x=74.5 to x=94, joined by a 1px dashed green vertical at 74.5; bold 13px green annotations "leaf mean $140" under the left shelf and "leaf mean $280" above the right shelf.
- **Caption (12px `#444`, bottom left):** "each leaf answers with its group's mean — illustrative data".

## Scoring Every Cut by Leftover Variance

**Tags:** `worked example` (blue), `variance reduction` (green)

- **Start** — around the single mean $210, the total squared error (SSE) of the 12 days is 62,200
- **Try all cuts** — a split can only fall between sorted neighbors, so 12 days give 11 candidates
- **Score each** — for a cut, add the left group's SSE around its mean to the right group's SSE
- **Weak cut** — cutting at 65.5°F leaves 36,600 because cool and hot days still mix on one side
- **Best cut** — cutting at 74.5°F leaves 850 + 2,550 = 3,400, a 95% drop in squared error
- **Greedy** — the tree keeps 74.5°F, then reruns the same search inside each leaf separately

*Example (italic):* Check the cool leaf by hand: residuals −15, −10, −5, 0, 10, 20 square and sum to exactly 850.

**Key point:** "Splitting to reduce variance" just means: pick the cut whose two group means leave the smallest total squared error behind.

### Visualization (canvas `c2`, 720×300)

Bar chart scoring all 11 candidate thresholds by the total SSE that remains after splitting there, with the no-split SSE as a dashed reference line.

- **Title (bold 15px, `#1a5276`, top center):** "Every Candidate Cut, Scored by Remaining Squared Error".
- **Data:** thresholds `[59.5, 62.5, 65.5, 68.5, 71.5, 74.5, 77.5, 80.5, 83.5, 86.5, 89.5]` (°F); remaining SSE `[54318, 45865, 36600, 26163, 15263, 3400, 12691, 22592, 30989, 40540, 51291]`.
- **Axes:** origin x=70, plot width 590, baseline y=245, plot height 180; y maps 0–65,000 with y tick labels "0", "30k", "60k" (12px `#444`); threshold labels 11px `#444` under each bar (e.g. "59.5", "74.5").
- **Bars:** 11 bars, ~38px wide with gaps; fill `rgba(42,120,214,0.45)` except the 74.5 bar in solid green `#008300`.
- **Reference line:** dashed `#6b7280` (dash 5/4) horizontal at 62,200, labeled bold 12px `#6b7280` "no split: 62,200" above its right end.
- **Annotations:** bold 13px green "best: 3,400" above the 74.5 bar with a short green arrow; bold 12px magenta `#d55181` "still 36,600 — groups mixed" above the 65.5 bar.
- **Caption (12px `#444`, bottom left):** "SSE after cut = left-group SSE + right-group SSE".

## The Atom Inside Boosting

**Tags:** `where it's used` (blue), `boosting` (orange), `residuals` (green)

- **Small trees** — gradient boosting stacks hundreds of shallow regression trees, often 1–3 splits each
- **Fit the leftovers** — tree 1 predicts $140/$280; tree 2 trains on the residuals tree 1 left behind
- **Round 2** — the best cut on those residuals is 86.5°F: predict −5 below it, +25 above it
- **Chipping away** — squared error falls 62,200 → 3,400 → 1,900 across the first two trees
- **Even for classes** — boosted classifiers still grow regression trees inside, fitted to gradients

*Example (italic):* The two hottest days (88°F, 91°F) were underpredicted by 20 and 30, so tree 2 adds +25 exactly there.

**Key point:** Boosting is regression trees stacked on residuals — each new tree predicts whatever the sum so far still gets wrong.

### Visualization (canvas `c3`, 720×300)

Two panels split by a dashed divider at x=390: tree 2 fitting the residuals of tree 1 (left), and the squared error shrinking round by round (right).

- **Title (bold 15px, `#1a5276`, top center):** "Tree 2 Fits What Tree 1 Missed".
- **Left panel (residual fit):** axis origin x=55, plot width 300, vertical center (residual 0) at y=155, plot half-height 85; x maps 55–94°F; y maps −40 to +40 with tick labels "−40", "0", "+40" (12px `#444`).
- **Left data:** temps `[58, 61, 64, 67, 70, 73, 76, 79, 82, 85, 88, 91]`; residuals after tree 1 `[-15, -10, -5, 0, 10, 20, -30, -15, -10, 5, 20, 30]`; blue `#2a78d6` 5px dots; solid 1px `#999` zero line.
- **Left step:** green `#008300` 3px shelves at −5 from x=55 to x=86.5 and at +25 from x=86.5 to x=94; orange `#d95926` dashed vertical at 86.5; bold 12px green labels "−5" and "+25" beside the shelves; caption 12px `#444` "tree 2, trained on residuals".
- **Right panel (error by round):** three bars at x=430/510/590, each 55px wide, baseline y=245, plot height 175, heights proportional to value with a 4px minimum; values `[62200, 3400, 1900]` with x labels "mean only", "+ tree 1", "+ tree 2" (11px `#444`).
- **Right styling:** first bar fill `rgba(107,114,128,0.4)`, second `rgba(42,120,214,0.5)`, third solid green `#008300`; bold 12px value labels "62,200", "3,400", "1,900" above each bar; bold 13px green annotation "each round chips away" near the top right.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=390 from y=38 to h-12.

## Steps, Not Lines

**Tags:** `common mistake` (red), `extrapolation` (orange)

- **No slope** — inside a leaf the prediction is perfectly flat; a tree never draws a tilted line
- **Finer stairs** — two more splits give four leaves: $132.5, $155, $261.7, and $298.3
- **Flat outside** — at 100°F the tree still answers $298.3, its hottest leaf's mean, forever
- **Memorizing** — keep splitting until every day is its own leaf and training SSE hits 0 — only there
- **Not classification** — a regression tree's leaf holds a mean, not a class vote or probability

*Example (italic):* A heat wave hits 100°F and real sales keep climbing, but the tree stays pinned at $298.3.

**Common mistake:** Expecting a tree to extrapolate a trend. Outside the training range every regression tree goes flat at its edge leaf's mean — a straight-line fit keeps rising, the staircase does not.

### Visualization (canvas `c4`, 720×300)

Scatter with the depth-2 staircase (4 leaves) and a straight linear fit for contrast, plus a shaded zone past the hottest training day where the tree stays flat.

- **Title (bold 15px, `#1a5276`, top center):** "Four Leaves Make a Staircase — and It Goes Flat Off the Edge".
- **Axes:** origin x=60, plot width 620, baseline y=250, plot height 195; x maps 55–102°F, y maps $60–$400; x tick labels 60/70/80/90/100, y tick labels $100/$250/$400 (12px `#444`).
- **Data:** same 12 points — temps `[58, 61, 64, 67, 70, 73, 76, 79, 82, 85, 88, 91]`, sales `[125, 130, 135, 140, 150, 160, 250, 265, 270, 285, 300, 310]`; blue `#2a78d6` 5px dots.
- **Staircase:** green `#008300` 3px shelves at $132.5 (x 55–68.5), $155 (x 68.5–74.5), $261.7 (x 74.5–83.5), $298.3 (x 83.5–102), joined by 1px dashed green verticals at the three cut points; bold 12px green leaf labels "$132.5", "$155", "$261.7", "$298.3" above each shelf.
- **Linear fit:** dashed `#6b7280` (dash 6/4) line from data point (55, 80) to (102, 393), labeled 12px `#6b7280` "linear fit, ≈ $6.6/°F (for contrast)".
- **Extrapolation zone:** rectangle from x-value 91 to 102, plot top to baseline, fill `rgba(217,89,38,0.08)`; orange `#d95926` dashed vertical at 91 labeled 11px orange "hottest training day".
- **Annotation:** bold 13px magenta `#d55181` "flat at $298.3 forever past the data" inside the shaded zone, with a short magenta arrow to the green shelf.
- **Caption (12px `#444`, bottom left):** "cuts at 68.5, 74.5, 83.5°F; leaf value = leaf mean".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
