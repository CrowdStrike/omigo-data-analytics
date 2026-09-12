# Decision Trees

**Page type:** detail page (tutorial card-sections: h2 + two-column table.layout, text left 50% / canvas right 50%)
**HTML title tag:** Decision Trees

**Subtitle:** Deciding a loan by playing 20 questions — "income over $50k? tenure over 2 years?" — and writing the question list down as a flowchart

## A Loan Decided by Twenty Questions

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — a bank has 20 past loans on file: 12 were repaid, 8 defaulted
- **The game** — judge a new application with yes/no questions, like 20 questions
- **First question** — "income > $50k?" splits the file into a 12-loan and an 8-loan pile
- **Second question** — inside the high-income pile, "tenure > 2y?" splits again
- **The leaves** — stop when a pile is nearly all one outcome; that outcome is the answer

*Example (italic):* A new applicant earns $62k with 4 years on the job — two questions land them in the 8-for-8 repaid leaf.

**Key point callout:** **Decision tree:** the question list written down as a flowchart — every prediction is a walk from the top question to a leaf.

### Visualization (canvas `c1`, 720×300)

Flowchart tree diagram of the loan decision tree, boxes connected by labeled edges.

- **Title (bold 15px, `#1a5276`, top center):** "The Tree the 20 Loans Grow"
- **Boxes** (each 2 lines: bold 13px `#1a5276` heading + 12px `#2c3e50` counts, 2px colored border on a tinted fill):
  - Root at (270,36), 190×40: "income > $50k?" / "20 loans: 12 repaid, 8 default" — border `#1a5276`, fill `#f4f8fb`
  - Left child at (75,132), 180×40: "DENY" / "8 loans: 2 repaid, 6 default" — border orange `#d95926`, fill `#fdf2ec`
  - Right child at (420,132), 190×40: "tenure > 2y?" / "12 loans: 10 repaid, 2 default" — border `#1a5276`, fill `#f4f8fb`
  - Left grandchild at (310,226), 180×40: "STILL MIXED" / "4 loans: 2 repaid, 2 default" — border yellow `#c98500`, fill `#fdf9ee`
  - Right grandchild at (525,226), 180×40: "APPROVE" / "8 loans: 8 repaid, 0 default" — border green `#008300`, fill `#f0f8f0`
- **Edges:** gray `#9aa4ad` width 1.5 lines drawn behind the boxes; edge labels bold 12px at midpoints: "no" in orange `#d95926` (root→DENY, tenure→STILL MIXED), "yes" in green `#008300` (root→tenure node, tenure→APPROVE).
- **Annotation (bold 13px green, left side, two lines):** "each answer moves the applicant" / "down to a purer pile"

## Which Question Goes First? Score the Purity

**Tags:** `worked example` (green), `arithmetic` (blue)

- **The contest** — every candidate question is tried; the purest resulting piles win
- **The score** — Gini = 1 − (share repaid)² − (share default)²; 0 is pure, 0.5 is a coin flip
- **"Owns a pet?"** — both piles come out 6 repaid / 4 default: Gini 0.48 each, nothing learned
- **"Income > $50k?"** — piles of 10R/2D and 2R/6D: Gini 0.28 and 0.38
- **Weighted score** — (12/20)(0.28) + (8/20)(0.38) = 0.32, well under the 0.48 start
- **Repeat** — the winner splits the file, then each pile runs the same contest again

*Example (italic):* Every number here fits a pocket calculator: 1 − (10/12)² − (2/12)² = 0.28.

**Key point callout:** **No understanding needed:** the tree never "knows" what income means — it just measures which question sorts repaid from defaulted best.

### Visualization (canvas `c2`, 720×300)

Bar chart comparing weighted Gini after each candidate first question.

- **Title (bold 15px, `#1a5276`, top center):** "The Split Contest: Weighted Gini After Each Question (lower = purer)"
- **Bars (110px wide, outlined width 2 in bar color with 0.35-alpha fill of the same color):**
  - "no split yet" — 0.48, gray `#6b7280`, note "12R / 8D"
  - "\"owns a pet?\"" — 0.48, violet `#4a3aa7`, note "6R/4D and 6R/4D"
  - "\"age > 30?\"" — 0.45, yellow `#c98500`, note "illustrative"
  - "\"income > $50k?\"" — 0.32, green `#008300`, note "10R/2D and 2R/6D"
- **Value labels:** bold 13px in bar color above each bar (0.48, 0.48, 0.45, 0.32); bar labels 12px `#2c3e50` below the baseline; notes 11px gray below the labels.
- **Axes/scale:** baseline y=240, chart height 175px, y max 0.55; L-shaped `#999` axes starting at x=80; rotated y-axis label "weighted Gini" (gray 12px).
- **Reference line:** dashed `#bbb` (dash 5/4) horizontal at 0.48, labeled 12px gray "starting impurity 0.48".
- **Annotation (bold 13px green, upper right):** "income wins: 0.32, the biggest purity gain"

## Why Data Scientists Reach for Trees

**Tags:** `where it's used` (blue), `best practice` (green)

- **Readable** — the model IS the flowchart; a loan officer can audit every rule
- **No prep** — no scaling or centering needed; dollars and years mix fine in one tree
- **Rectangles** — each rule is a straight cut, so the rules can be drawn on a plot
- **Nonlinear for free** — "low income OR short tenure" patterns need no equations
- **Building block** — bagging, random forests, and boosting are all stacks of trees

*Example (italic):* Plot income against tenure and the tree's two rules appear as fences around an approve zone.

**Key point callout:** **The trade:** one tree gives total transparency for modest accuracy — combine many and trees become the strongest models on tabular data.

### Visualization (canvas `c3`, 720×300)

Scatter plot of the 20 loans (income vs tenure) with the tree's two rules drawn as rectangular "fences" and tinted decision regions.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Two Rules Drawn as Fences (20 loans)"
- **Axes:** x = income, $20k–$100k, tick labels "$20k", "$40k", "$60k", "$80k", "$100k", axis label "income"; y = tenure 0–8 years, rotated label "tenure (years)". L-shaped `#999` axes; padding top 42, bottom 48, left 62, right 30. Gray 12px labels.
- **Region tints:** left of income=$50k `rgba(217,89,38,0.08)` (deny); right of $50k above tenure=2y `rgba(0,131,0,0.08)` (approve); right of $50k below 2y `rgba(201,133,0,0.10)` (mixed).
- **Fences:** ink `#1a5276` width 2.5 — vertical line at income=$50k (full height), horizontal line at tenure=2y (from x=$50k to right edge). Fence labels bold 12px ink: "income = $50k" beside the vertical line, "tenure = 2y" above the horizontal line.
- **Repaid points (green `#008300` filled circles radius 6), [income $k, tenure y]:** `[35,6], [48,4], [55,3], [60,5], [66,4], [72,6], [78,3.5], [85,5.5], [92,7], [64,2.5], [58,1], [75,1.5]`
- **Defaulted points (orange `#d95926` X marks, width 2.5, arm 5px):** `[25,1], [32,3], [38,0.5], [42,5], [30,2], [46,1.5], [68,0.5], [88,1]`
- **Region labels (bold 13px):** "DENY (2R / 6D)" in orange near top-left region; "APPROVE (8R / 0D)" in green near top-right; "mixed (2R / 2D)" in yellow below the tenure fence.
- **Legend (12px, bottom-left inside plot):** green "● repaid", orange "✕ defaulted".

## The Trap: a Tree That Never Stops Asking

**Tags:** `common mistake` (red), `overfitting` (orange)

- **Growing deep** — keep splitting and every leaf ends pure, even leaves holding 1 loan
- **Perfect on the past** — a fully grown tree scores 100% on the 20 training loans
- **Worse on the future** — test accuracy peaks near depth 4, then slides as depth grows
- **Memorizing** — deep leaves store one-off quirks, not patterns that repeat
- **The fix** — cap the depth, require a minimum leaf size, or prune after growing

*Example (italic):* At depth 8 the tree is a lookup table of the 20 old loans wearing a flowchart costume.

**Key point callout:** **The signature:** train accuracy climbing while test accuracy falls means memorization — and depth is the dial that causes it.

### Visualization (canvas `c4`, 720×300)

Dual line chart: train vs test accuracy by tree depth, with an overfitting zone shaded.

- **Title (bold 15px, `#1a5276`, top center):** "Accuracy vs Tree Depth (illustrative)"
- **X axis:** depths 1–8, 12px `#2c3e50` tick labels, gray axis label "tree depth"; **Y axis:** accuracy % scaled 60–105, gray ticks at 60/70/80/90/100, rotated label "accuracy, %". L-shaped `#999` axes; padding top 46, bottom 50, left 62, right 165.
- **Series (width-3 lines with 4px dots):**
  - train accuracy, solid blue `#2a78d6`: `[70, 80, 85, 90, 95, 98, 100, 100]`
  - test accuracy, dashed orange `#d95926` (dash 6/4): `[68, 76, 81, 82, 80, 77, 74, 72]`
- **Overfit zone:** area right of depth 4 shaded `rgba(213,81,129,0.08)` for the full plot height.
- **Annotation (bold 13px magenta `#d55181`, centered over the shaded zone, two lines):** "past depth 4 the tree memorizes:" / "train climbs, test falls"
- **Legend (right margin, 12px):** blue swatch "train accuracy", orange swatch "test accuracy".

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (per `tutorials/CLAUDE.md`, modeled on `most-powerful-signals/07-social-graph-connections.html`): h1 + `.subtitle`, then four `.card-section` blocks, each `<h2>` + `table.layout` with `.text-col` (50%) holding text and `.viz-col` (50%) holding one canvas.
- **Left column structure per section:** `.tags` pill row, then `<ul>` of one-line bullets each opening with `<b>bold term</b> —`, one italic `.example` paragraph, one `.key-point` callout with a `<strong>` lead.
- **Tag pill classes:** `.tag.blue` bg `rgba(26,82,118,0.12)` text `#1a5276`; `.tag.green` bg `rgba(39,174,96,0.15)` text `#27ae60`; `.tag.red` bg `rgba(231,76,60,0.12)` text `#e74c3c`; `.tag.orange` bg `rgba(230,126,34,0.15)` text `#e67e22`. Pills 0.72rem, weight 600, padding 2px 10px, radius 10px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px bottom border `#2980b9`; section h2 1.3rem `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 0.95rem; bullets 0.92rem with `li b` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` bg `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas JS:** shared palette object `P` (blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`); shared `setup(id)` helper sized 720×300 that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded (no `Math.random()`); invented curves labeled "illustrative". Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette reference:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card/grid links use `.html` extensions (this page has no links).
