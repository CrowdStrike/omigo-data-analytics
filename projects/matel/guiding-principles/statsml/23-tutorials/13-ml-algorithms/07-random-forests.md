# Random Forests

**Page type:** detail page (tutorial card-sections: h2 + two-column table.layout, text left 50% / canvas right 50%)
**HTML title tag:** Random Forests

**Subtitle:** Bagging plus a twist — each split may only look at a random handful of features, so the trees stop copying each other and the vote actually cancels errors

## Bagging Plus One Twist: Hide Some Features

**Tags:** `core idea` (blue), `running example` (green)

- **Start from bagging** — 100 trees, each grown on a bootstrap resample of the same loans
- **The problem** — income is the strongest signal, so nearly every bagged tree opens with it
- **Copycat trees** — trees asking the same questions err on the same applicants; votes stop cancelling
- **The twist** — at every split, the tree may only pick from a random handful of features
- **The effect** — trees are forced onto tenure, debt, savings… and stop copying each other

*Example (italic):* Deal each loan officer only 3 of the 9 folders per question — some never get the income folder first.

**Key point callout:** **Random forest = bagging + random feature subsets at each split.** The extra randomness is the decorrelator.

### Visualization (canvas `c1`, 720×300)

Two-panel diagram of mini-trees (root box + two leaf dots each), split by a dashed vertical divider at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px, `#1a5276`, top center):** "First 5 of the 100 Trees: What Question Does Each Open With?"
- **Mini-tree glyph:** 104×26 white root box with a 2px colored border and bold 12px colored root-question label; two gray `#9aa4ad` edges down to two `#cfd8de` leaf dots (radius 5).
- **Left panel (bagging):** header bold 13px violet `#4a3aa7` "bagging: all 9 features offered at every split"; five mini-trees arranged 2-2-1 (positions (120,70), (260,70), (120,150), (260,150), (190,222)), ALL labeled "income?" in violet.
  Bottom caption bold 12px violet: "five copies of one opinion".
- **Right panel (forest):** header bold 13px green `#008300` "forest: 3 random features offered per split"; five mini-trees at (470,70), (610,70), (470,150), (610,150), (540,222) with varied roots and colors: "income?" blue `#2a78d6`, "tenure?" green `#008300`, "debt ratio?" orange `#d95926`, "savings?" aqua `#199e70`, "missed pay?" magenta `#d55181`.
  Bottom caption bold 12px green: "same data, different questions — the trees stop copying".

## The 3-of-9 Draw, By Hand

**Tags:** `worked example` (green), `arithmetic` (blue)

- **Nine features** — income, debt ratio, tenure, savings, age, missed payments, job type, dependents, zip
- **The draw** — at each split, sample 3 of the 9 (the √9 rule of thumb for classification)
- **Root chance** — income lands in the draw 3/9 = 1/3 of the time
- **Across 100 trees** — about 33 may open with income; 67 must open with something else
- **Every split redraws** — income hidden at the root can still show up deeper down

*Example (italic):* In this forest 33 roots used income, 18 debt ratio, 14 tenure — bagging had 100 income roots.

**Key point callout:** **One-line arithmetic:** the 3/9 draw alone predicts the whole spread of root questions you see in the chart.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: root-question feature counts across 100 trees, bagging vs forest.

- **Title (bold 15px, `#1a5276`, top center):** "Root Question Across 100 Trees (counts)"
- **Categories (x):** `income, debt, tenure, savings, age, missed, job, deps, zip` (12px `#2c3e50` labels); x axis label "feature used at the root split"; rotated y label "trees out of 100" (gray 12px).
- **Data:**
  - bagging (violet): `[100, 0, 0, 0, 0, 0, 0, 0, 0]`
  - random forest (green): `[33, 18, 14, 11, 8, 7, 5, 3, 1]`
- **Bars:** 24px wide, paired per feature; bagging fill `rgba(74,58,167,0.45)` stroke violet `#4a3aa7`; forest fill `rgba(0,131,0,0.40)` stroke green `#008300`; bold 12px count labels in the bar color above each bar (bagging label only where count > 0).
- **Axes/scale:** L-shaped `#999`, y max 105 with gray ticks 0/25/50/75/100. Padding: top 54, bottom 64, left 62, right 25.
- **Annotation (bold 13px green):** "forest: only 33 of 100 roots use income — 3/9 predicted it"
- **Legend (12px squares):** violet "bagging", green "random forest".

## The Default Strong Baseline for Tables

**Tags:** `where it's used` (blue), `best practice` (green)

- **First model to try** — on rows-and-columns data, a forest is hard to beat without real effort
- **Almost no tuning** — tree count and the feature-draw size are the only common knobs
- **Free validation** — each tree skipped ~1/3 of the loans; score it on those (out-of-bag error)
- **Feature ranking** — how much each feature improves splits gives a usable importance list
- **Robust** — no scaling needed; tolerates outliers and odd distributions like single trees do

*Example (italic):* On the loan task: single tree 74%, bagging 81%, random forest 85% accuracy (illustrative).

**Key point callout:** **Rule of thumb:** reach for a random forest first — anything fancier must beat this baseline to earn its complexity.

### Visualization (canvas `c3`, 720×300)

Three-bar accuracy ladder: single tree vs bagging vs random forest.

- **Title (bold 15px, `#1a5276`, top center):** "Test Accuracy on the Loan Task (illustrative)"
- **Bars (140px wide, 0.4-alpha fill with 2px stroke in the bar color):**
  - "single tree" — 74%, yellow `#c98500`
  - "bagging (100 trees)" — 81%, violet `#4a3aa7`
  - "random forest (100 trees)" — 85%, green `#008300`
  - Value labels bold 15px in bar color above bars ("74%", "81%", "85%"); model names 12px `#2c3e50` below the baseline.
- **Axes/scale:** y scaled 60–92 with gray tick labels 60%/70%/80%/90% and light `#eee` horizontal gridlines; L-shaped `#999` axes. Padding: top 56, bottom 62, left 80, right 60.
- **Step annotations (bold 12px):** violet "+7: voting cancels variance" above the bagging bar; green "+4: decorrelation, the last jump" above the forest bar.
- **Caption (bottom center, gray 12px):** "same 9 features, no tuning beyond defaults"

## More Trees Is Not More Randomness

**Tags:** `common mistake` (red), `trade-off` (orange)

- **The mix-up** — adding trees is not what separates a forest from plain bagging
- **Both flatten** — past ~100 trees either method plateaus; extra trees never overfit, just cost time
- **The gap** — the forest's lower floor comes from the feature draw, not the tree count
- **The knob** — a smaller draw decorrelates more but weakens each tree; 3-of-9 balances the two
- **Lost readability** — 100 varied trees vote well but can't be read as one flowchart

*Example (italic):* Bagging with 1,000 trees still sits above a 100-tree forest — count can't buy decorrelation.

**Key point callout:** **The vote improves only as much as the trees disagree** — the feature draw is what makes them disagree.

### Visualization (canvas `c4`, 720×300)

Dual line chart: test error vs number of trees, bagging vs forest, with the gap shaded.

- **Title (bold 15px, `#1a5276`, top center):** "Test Error vs Number of Trees (illustrative)"
- **X axis:** tree counts `[1, 5, 10, 25, 50, 100, 200]` evenly spaced, 12px `#2c3e50` labels, gray axis label "number of trees"; **Y axis:** 10–30% scale, gray tick labels 10%/15%/20%/25%/30%, rotated label "test error, %". L-shaped `#999` axes; padding top 50, bottom 52, left 62, right 180.
- **Series (width-3 lines with 4px dots):**
  - bagging, dashed violet `#4a3aa7` (dash 6/4): `[26, 22, 21, 20, 19.5, 19, 19]`
  - random forest, solid green `#008300`: `[26, 20, 17.5, 16, 15.5, 15, 15]`
- **Gap shading:** region between the two curves filled `rgba(0,131,0,0.10)`.
- **Annotation (bold 13px green, two lines mid-right):** "the gap is decorrelation, not tree count —" / "both curves flatten past ~100 trees"
- **Legend (right margin, 12px):** violet swatch "bagging", green swatch "random forest".

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (per `tutorials/CLAUDE.md`, modeled on `most-powerful-signals/07-social-graph-connections.html`): h1 + `.subtitle`, then four `.card-section` blocks, each `<h2>` + `table.layout` with `.text-col` (50%) holding text and `.viz-col` (50%) holding one canvas.
- **Left column structure per section:** `.tags` pill row, then `<ul>` of one-line bullets each opening with `<b>bold term</b> —`, one italic `.example` paragraph, one `.key-point` callout with a `<strong>` lead.
- **Tag pill classes:** `.tag.blue` bg `rgba(26,82,118,0.12)` text `#1a5276`; `.tag.green` bg `rgba(39,174,96,0.15)` text `#27ae60`; `.tag.red` bg `rgba(231,76,60,0.12)` text `#e74c3c`; `.tag.orange` bg `rgba(230,126,34,0.15)` text `#e67e22`. Pills 0.72rem, weight 600, padding 2px 10px, radius 10px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px bottom border `#2980b9`; section h2 1.3rem `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 0.95rem; bullets 0.92rem with `li b` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` bg `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas JS:** shared palette object `P` (blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`); shared `setup(id)` helper sized 720×300 that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded (no `Math.random()`); invented curves labeled "illustrative". Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette reference:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card/grid links use `.html` extensions (this page has no links).
