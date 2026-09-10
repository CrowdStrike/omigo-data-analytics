# Decision Trees

**Page type:** detail page (two-column obj-table layout: text left 42%, canvas right 58%, one row per assumption)
**HTML title tag:** Decision Trees - ML Assumptions

**Subtitle:** Greedy, axis-aligned splits with no built-in statistical validation. Fits noise as eagerly as signal.

## Section 0: What It Does

Recursively partitions the feature space using axis-aligned splits that maximize information gain (or minimize Gini impurity). Creates interpretable if-then rules by choosing the best feature and threshold at each node.

- **Best For:** Medical triage rules, loan approval logic, customer segmentation with mixed feature types
- **Data:** Both classification and regression tasks, handles mixed numeric/categorical features, no feature scaling needed.

### Visualization (canvas `c0`, 720×300)

Tree diagram on the left plus the corresponding rectangular feature-space partition on the right.

- **Tree:** root box "Age < 50?" at (0.35w, 15); "Yes"/"No" labeled gray edges (`#999`, width 1.5, edge labels 10px `#555`) to internal boxes "Income < 40k?" (0.15w) and "Chol > 240?" (0.55w); leaves at y=135 drawn as circles (radius ~23): "Low risk" (fill `#eafaf1`, stroke `#27ae60`), "Med risk" (fill `#fef9e7`, stroke `#e67e22`), "High risk" (fill `#fdedec`, stroke `#e74c3c`), "Med risk" (fill `#fef9e7`, stroke `#e67e22`). Internal nodes: fill `#f0f6fa`, stroke `#1a5276` width 2, 130×34; node text 11px `#1a5276` (leaves bold).
- **Right partition panel:** rectangle at (0.72w, 25), size 0.25w × 160, stroke `#ccc`; axis labels 10px `#555`: "Age" below, rotated "Cholesterol" on the left. Split lines in `#1a5276` width 2: vertical at 50% width (Age=50), horizontal in left half at 50% height (Income), horizontal in right half at 40% height (Chol). Region fills and bold 9px labels: bottom-left `rgba(39,174,96,0.2)` "Low" (`#27ae60`); top-left `rgba(230,126,34,0.2)` "Med" (`#e67e22`); top-right `rgba(231,76,60,0.2)` "High" (`#e74c3c`); bottom-right `rgba(230,126,34,0.2)` "Med" (`#e67e22`).
- **Bottom annotation (10px `#555`, centered):** "Each split partitions space into rectangular regions → leaf predictions".

## Section 1: Axis-Aligned Splits Only

Decision trees can only split **perpendicular to feature axes** — horizontal or vertical cuts. A diagonal decision boundary (e.g., "risk = age + BMI > 50") requires many staircase-like splits to approximate. Each additional split fragments the data, reducing sample size per leaf and increasing variance.

- **Breaks:** True boundary is diagonal (age + cholesterol > 300). The staircase shown uses 11 axis-aligned cuts and still misclassifies 4 of the 70 plotted points (94.3% accuracy), fragmenting the space into thin rectangles.
- **Verify:** Visualize 2D feature interactions — if boundary looks diagonal, tree will struggle
- **Fix:** Create interaction features (age+BMI), use oblique trees, or SVM/logistic regression

### Visualization (canvas `c1`, 720×300)

Scatter with a diagonal true boundary and the tree's staircase approximation.

- **Axes:** L-shaped gray (`#bbb`) axes, padding 55; x-axis "Age", y-axis rotated "Cholesterol"; both mapped 0–100 in data units.
- **Points:** 70 points from the shared seeded Park-Miller LCG `lcg(20250401)` (never `Math.random()`), drawn as `x = 5 + rnd()*90`, `y = 5 + rnd()*90` (x and y consume consecutive draws); class 1 (red `rgba(231,76,60,0.65)`) when x+y > 95, else class 0 (blue `rgba(41,128,185,0.6)`); radius 4. This seed yields 40 red / 30 blue.
- **True boundary:** solid green `#27ae60` width 2.5 diagonal from (0, 95) to (95, 0).
- **Staircase approximation:** dashed red `#e74c3c` (dash 5/3, width 2) stepping through the vertex list (0,95)→(0,80)→(15,80)→(15,65)→(30,65)→(30,50)→(45,50)→(45,35)→(60,35)→(60,20)→(75,20)→(75,5)→(95,5). The vertex list is stored in a `stair` array that drives both the drawing and the counts.
- **Computed labels (not hardcoded):** the interior cut count is derived from `stair` (5 vertical cuts at x = 15/30/45/60/75 plus 6 horizontal cuts at y = 5/20/35/50/65/80 = **11 cuts**); staircase predictions are evaluated against every plotted point, giving **4 misclassified of 70 = 94.3% accuracy**.
- **Legend (top right):** green line "True boundary (diagonal)"; dashed red line "Tree approx (11 cuts)"; then two 12px `#555` lines "70 points: 40 red / 30 blue" and "staircase misses 4 of 70 (94.3% acc)". All four numbers are printed from the computed variables.
- **Caption (bold orange `#e67e22`, centered near bottom):** "Each step = one split = smaller n per leaf".

## Section 2: No Statistical Validation at Splits

A decision tree will split on **any information gain > 0**, even if the split is based on n=5 observations and is pure random noise. There is no built-in hypothesis test, no minimum confidence level. The tree treats "100% positive with n=3" as a perfect leaf — indistinguishable from a 100%-positive leaf with n=2897.

- **Breaks:** A leaf with 3 positives is called "100% pure", but against a 50% base rate that happens by chance with probability 0.5³ = 0.125 — roughly one node in eight. The tree shows no uncertainty.
- **Verify:** Post-hoc binomial test on leaf purities vs. base rate, cross-validation
- **Fix:** CHAID (chi-squared at splits), min_samples_leaf ≥ 20, pruning, or validate ranges statistically

### Visualization (canvas `c2`, 720×300)

Tree diagram with a tiny pure leaf highlighted, plus a comparison box.

- **Counts are derived so the arithmetic closes:** `nRoot = 5000`, `nLeft = 2100`, `nRight = nRoot − nLeft = 2900`, `nTiny = 3`, `nBig = nRight − nTiny = 2897`. Every "n = …" label prints one of these variables, so children always sum to their parent.
- **Nodes (120×40 boxes, title bold 13px `#1a5276` + n count 13px `#555`):** root "chol < 245?" / "n = 5000" (fill `#f0f4f8`, stroke `#2980b9`) at 0.4w; children "age < 55?" / "n = 2100" at 0.2w and "chol < 291?" / "n = 2900" at 0.6w (same style); leaf "71% pos" / "n = 2897" (fill `#eafaf1`, stroke `#27ae60`) at 0.45w; leaf "100% pos !!!" / "n = 3" (fill `#fdedec`, stroke `#e74c3c`) at 0.75w. Gray edges (`#888`) labeled "Yes"/"No".
- **The 71% is computed:** `bigPos = 2057` positives out of `nBig = 2897` → `Math.round(100 × 2057/2897)` = **71%**.
- **Highlight:** thick red (width 3) rectangle around the n=3 leaf; bold red "← Random chance!" to its right, then `#555` lines "3 samples = no statistical" / "power whatsoever".
- **Comparison box (bottom, fill `#f8f9fa`, stroke `#ddd`, spanning most of the width):** bold `#1a5276` "Both leaves are \"100% positive\" — vs a 50% base rate:"; red bullet "• n = 3 → P(all pos by chance) = 0.5^3 = 0.125 — random noise"; green bullet "• n = 2897 → P(all pos by chance) ≈ 10^-872 — real signal". Both probabilities are computed in JS (`Math.pow(0.5, nTiny)` and `nBig × log10(0.5)`), replacing the previously asserted "binomial p=0.12" / "p < 1e-100".

## Section 3: Greedy Split Selection (Locally Optimal)

Trees pick the **best single split at each node** without lookahead. A feature that is globally informative but only becomes useful after another split is never discovered. The greedy algorithm can't see that splitting on feature B first would unlock a perfect split on feature A — it picks whatever reduces impurity most right now.

- **Breaks:** XOR-like pattern — disease occurs when (high age + low BP) OR (low age + high BP). In the plotted sample both single-feature stumps score exactly 50.0% (the base rate), so the true cut yields zero information gain while the best noise cut still gains 0.070 bits — the greedy tree splits on noise.
- **Verify:** Compare single-tree performance with ensembles (Random Forest) — large gap suggests greedy limitation
- **Fix:** Random Forest (samples feature subsets), interaction features, or exhaustive search (CART with lookahead)

### Visualization (canvas `c3`, 720×300)

XOR scatter on the left half, computed step-by-step diagnostics on the right.

- **Left scatter (plot spans left half, padding 55):** L-shaped gray axes; x-axis "Age", y-axis rotated "Blood Pressure", data range 0–100.
- **Points:** 60 points from the shared seeded Park-Miller LCG `lcg(20251005)` (never `Math.random()`), drawn as `x = 10 + rnd()*80`, `y = 10 + rnd()*80`, then a third draw per point for the noise flip. Base class 1 when (x>50 AND y<50) OR (x<50 AND y>50). Class 1 red `rgba(231,76,60,0.7)`, class 0 blue `rgba(41,128,185,0.65)`, radius 4.5.
- **Label noise — design rate vs realized rate:** the flip fires when `rnd() < 0.1`, a *design* rate of 10%. With this seed the realized count is **6 of 60 = 10.0%**, counted in a `flips` variable and printed as "Label noise: designed 10%, realized 6/60 = 10.0%". The realized figure is measured at render time, never asserted.
- **Class balance:** the seed yields exactly **30 red / 30 blue**, printed as "Plotted: 60 pts, 30 red / 30 blue" (30 + 30 = 60 closes).
- **Quadrant composition (measured, seed 20251005):** (x<50, y<50) 14 pts / 1 pos; (x>50, y<50) 14 pts / 13 pos; (x<50, y>50) 16 pts / 14 pos; (x>50, y>50) 16 pts / 2 pos — 14+14+16+16 = 60.
- **XOR grid lines:** dashed green `#27ae60` (dash 4/3, width 1.5) vertical at x=50 and horizontal at y=50.
- **Quadrant labels (bold 13px):** "+" in red at (75, 25) and (25, 75); "−" in blue at (25, 25) and (75, 75) (data coordinates).
- **Right text column (starting at w/2+30) — every statistic computed from the plotted points:** bold `#1a5276` "Why greedy fails on XOR:"; `#555` "Step 1: split on Age alone" / "  → 15/30 = 50% pos left, 15/30 = 50% pos right" / "  → stump accuracy 50.0% (= base rate)"; "Step 2: split on BP alone" / "  → 14/28 = 50% pos low, 16/32 = 50% pos high" / "  → stump accuracy 50.0% (= base rate)"; bold red "IG at the true cut = 0.000 bits"; `#555` "Best noise cut still gains 0.070 bits →" / "greedy tree splits on noise instead."; bold green "Age×BP interaction: 90.0% accuracy".
- **How those are derived:** side positive rates come from filtering the point array; stump accuracy is the majority-class hit rate on each side; the 0.000-bit figure is entropy-based information gain at the x=50 / y=50 cuts (both sides exactly 50% positive → gain is identically 0); the "best noise cut" value scans every midpoint threshold on both features and takes the maximum gain (0.0701 → prints 0.070); the interaction accuracy is the noise-free XOR rule scored against the noisy labels, i.e. 54/60 = 90.0%, which is exactly 60 − 6 flips.

## Section 4: No Built-in Class Imbalance Handling

With a 95%/5% class split, the tree's impurity metrics are **dominated by the majority class**. Gini impurity of a 95/5 node is already 0.095 — very "pure" by default. The tree has little incentive to split further for the minority class, producing leaves that simply predict "majority" everywhere.

- **Breaks:** Fraud detection with 2% fraud rate. Tree achieves 98% accuracy by predicting "not fraud" everywhere. Minority class leaves are never reached or have too few samples to be statistically meaningful.
- **Verify:** Check recall on minority class, inspect leaf class distributions
- **Fix:** Class weights, SMOTE, stratified splits, or use information gain ratio with adjusted priors

### Visualization (canvas `c4`, 720×300)

Stacked class-distribution bar, Gini computation, and an accuracy-vs-recall comparison.

- **Single source for the shares:** `pMin = 0.05`, `pMaj = 1 − pMin`, `gini = 2 × pMaj × pMin`. Bar widths, percent labels, the Gini line and the accuracy figure all print from these, so the bar geometry and the text cannot disagree.
- **Title (bold `#1a5276`, centered):** "Class Distribution: 95% Negative / 5% Positive".
- **Distribution bar (y=40, height 30, spanning pad→w−30):** first `pMaj` of the width in `rgba(41,128,185,0.6)` with `#2980b9` label "Negative: 95%" centred on that segment; final `pMin` in `rgba(231,76,60,0.7)` with red label "5%" centred on that segment; gray outline.
- **Gini text:** `#555` "Gini before any split = 2 × 0.95 × 0.05 = 0.095" (computed: `gini.toFixed(3)` = 0.095); bold orange `#e67e22` "Already \"almost pure\" — tree has little incentive to split further!".
- **"What the tree learns:"** bold `#1a5276` centered heading, then two 200×55 boxes side by side:
  - Left (fill `#eafaf1`, stroke `#27ae60`): bold "Predict \"Not Fraud\"" + green "Accuracy: 95% ✓" (printed from `pMaj`, so it always equals the majority share on the bar).
  - Right (fill `#fdedec`, stroke `#e74c3c`): bold "Minority Recall" + bold red "0% — catches nothing!" (exact by construction: a constant-majority predictor catches zero positives).
- **Bottom (13px `#555`, centered):** "Even when tree does split, minority leaves have n=2–5 samples — statistically meaningless"; red downward arrow; bold red final line "High accuracy ≠ useful model".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width `border-collapse` table, one `<tr>` per section; left `<td>` (42%) holds `.obj-title` + `.obj-desc` paragraph + `.obj-detail` lines, right `<td>` (58%, centered) holds the canvas. Even rows have background `#fafcfe`.
- **Detail-line markup:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses `<span class="tag tag-check">` (pill: background `#eafaf1`, color `#1e8449`); "Fix:" uses `<span class="tag tag-fix">` (pill: background `#fef9e7`, color `#b7950b`). Tag pills: inline-block, 0.75em, padding 2px 8px, radius 4px. Also defined: `.good` `#27ae60`, `.warn` `#e67e22`, `.tag-break` (background `#fdeaea`, color `#c0392b`).
- **Page style:** global reset (`* { margin:0; padding:0; box-sizing:border-box }`); body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border and 8px bottom padding; `.subtitle` `#666` 0.95rem, 32px bottom margin; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#555`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; `canvas { display:block; margin:0 auto; }`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; charts drawn with vanilla canvas 2D in IIFEs.
- **Randomness rule:** the page contains no `Math.random()`. Immediately after `setupCanvas`, the script defines the canonical seeded Park-Miller LCG:

```js
// Seeded Park-Miller LCG — deterministic, never Math.random()
function lcg(seed) {
    var s = seed;
    return function () { s = (s * 16807) % 2147483647; return s / 2147483647; };
}
```

  Each generated chart takes its own generator with a fixed integer seed: `c1` uses `lcg(20250401)` (70 points), `c3` uses `lcg(20251005)` (60 points). Figures are identical on every load, so every printed statistic is verifiable.
- **Computed-label rule:** no statistic describing generated data is hardcoded. `c1` prints its cut count, class split and staircase error from the point array and the `stair` vertex list; `c3` prints its per-side positive rates, stump accuracies, information gains, interaction accuracy, class split and realized flip count from the point array; `c2` derives all node counts and both chance probabilities from `nRoot`/`nLeft`/`nTiny`; `c4` derives its bar widths, percentages and Gini from `pMin`.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#555`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
