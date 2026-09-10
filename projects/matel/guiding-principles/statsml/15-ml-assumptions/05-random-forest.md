# Random Forest

**Page type:** detail page (two-column obj-table layout: text left 42%, canvas right 58%, one row per assumption)
**HTML title tag:** Random Forest - ML Assumptions

**Subtitle:** Averages many trees to reduce variance — but cannot extrapolate, cannot explain, and fails silently when trees lack diversity.

## Section 0: What It Does

Builds many independent decision trees on random subsets of data and features, then averages predictions (regression) or takes majority vote (classification). Reduces variance through ensemble diversity without increasing bias.

- **Best For:** Feature importance ranking, strong baseline with minimal tuning, anomaly detection via isolation scoring
- **Data:** Tabular data (classification or regression). Handles missing values, no scaling needed, works with mixed types. Moderate to large n preferred.

### Visualization (canvas `c0`, 720×300)

Three mini tree glyphs voting, aggregated by majority vote into a final prediction.

- **Trees:** three small stick-figure trees at x = 60, 150, 240, labeled "Tree 1", "Tree 2", "Tree 3" (13px `#444` above each). Each: root dot (radius 6, `#1a5276`) at y=35, two branches to internal dots (radius 5, `rgba(26,82,118,0.55)`) at y=65, four leaf dots (radius 4) at y=90; branch strokes `#1a5276` width 2.
- **Votes:** arrow from each tree down to a 28×20 colored vote box at y=130 with white bold letter: "A" (blue `#2980b9`), "B" (green `#27ae60`), "A" (blue `#2980b9`).
- **Aggregation:** orange (`#e67e22`, width 2.5) rightward arrow at y=140 with bold orange two-line label "Majority" / "Vote" beneath.
- **Final prediction:** 55×30 box at (315, 125) filled `#2980b9`, stroked `#1a5276`, white bold 14px "A"; below it bold `#1a5276` "2 vs 1" and `#444` "Final: A".

## Section 1: Cannot Extrapolate Beyond Training Range

A Random Forest prediction is a **vote among trees, each of which only assigns observations to existing leaves**. It cannot produce a value higher or lower than what exists in training. For any input beyond the training range, it returns the prediction from the nearest leaf — effectively clamping at the boundary.

- **Breaks:** Training max cholesterol = 603 (low-risk elderly patients). Deployment sees cholesterol = 750 (data entry or extreme case). RF assigns the 580–603 leaf prediction → "low risk". True risk is very high.
- **Verify:** Compare training feature ranges vs. deployment ranges; flag OOD inputs
- **Fix:** Range guards on input, linear model for extrapolation regions, or domain-based rules

### Visualization (canvas `c1`, 720×300)

Line chart of true risk vs the RF prediction, which goes flat past the training range.

- **Axes:** L-shaped gray (`#bbb`) axes, padding 60; x-axis "Cholesterol" mapping 100–850; y-axis rotated "P(disease)" mapping 0–1; x ticks at 200, 400, 603, 750.
- **Training zone:** band from x=100 to x=603 filled `rgba(41,128,185,0.06)` with blue `#2980b9` label "Training range" centered above it.
- **True risk curve:** dashed green `#27ae60` (dash 5/4, width 2.5): risk = min(0.95, 0.08 + ((x−100)/700)^1.6 × 0.87) over 100–850.
- **RF prediction:** solid red `#e74c3c` width 3 — identical to the true curve up to x=603, then flat (clamped) at the x=603 value out to 850.
- **Boundary marker:** dashed orange `#e67e22` (dash 3/3, width 1.5) vertical line at x=603.
- **Danger zone:** band from x=603 to x=850 filled `rgba(231,76,60,0.06)` with bold red two-line label "RF: \"low risk\"" / "Truth: high risk".
- **Legend (bottom left):** dashed green line "True risk"; solid red line "RF prediction".

## Section 2: Independent Errors (Tree Diversity Required)

Averaging only reduces variance if trees make **different mistakes**. When one feature dominates (e.g., always ranks #1 in importance), most trees split on it first — all trees become correlated, and the ensemble degenerates toward a single tree. The "wisdom of crowds" requires independent opinions.

- **Breaks:** One feature (age) has 5× more information gain than others. Even with random subsets (mtry), age appears in most subsets. 90% of trees split on age first → ensemble is effectively one tree × 500.
- **Verify:** Tree correlation (RMSE per tree plotted), feature importance concentration
- **Fix:** Extremely randomized trees (Extra-Trees), larger mtry, feature rotation

### Visualization (canvas `c2`, 720×300)

Side-by-side comparison of diverse vs correlated tree boundaries, split by a light gray vertical divider at 0.5w.

- **Left panel — "Diverse Trees (good)"** (bold `#1a5276` title, centered at 0.25w): five step-function decision boundaries stacked at baseY = 60 + i·42, each in a different color (`#2980b9`, `#27ae60`, `#8e44ad`, `#e67e22`, `#16a085`), width 2, with split points staggered (splitX = lx − 60 + i·25) and step heights 15 + 3i. Green caption at bottom: "Different splits → errors cancel".
- **Right panel — "Correlated Trees (bad)"** (bold `#e74c3c` title, centered at 0.75w): five nearly identical step boundaries, all red with increasing alpha (`rgba(231,76,60,0.3 + 0.12i)`), split points nearly the same (rx − 5 + (i−2)·4), step height 16. Red caption at bottom: "Same splits → same errors persist".
- **Middle annotation (13px `#555`, centered on the divider):** "One dominant feature →" / "all trees split the same".

## Section 3: Feature Dilution (Irrelevant Features)

Each tree considers **√p random features at each split**. If you have 100 features and only 3 matter, each split's random subset of 10 has only a 27% chance of containing even one relevant feature. Many trees grow entirely on noise, contributing random votes that dilute the ensemble's signal.

- **Breaks:** 200 features, 4 relevant. √200 ≈ 14 features per split. Probability of missing all 4 relevant ones: (196/200 × 195/199 × ... ) ≈ 75% of splits are noise-only. Most trees are pure noise.
- **Verify:** Compare permutation importance — if only 2–3 features matter among hundreds, dilution is occurring
- **Fix:** Feature selection first, increase mtry, or use boosting which focuses on hard examples

### Visualization (canvas `c3`, 720×300)

Feature-slot grid showing 4 relevant among many, plus a probability calculation and tree-quality bar.

- **Title (bold `#1a5276`, centered):** "200 features, only 4 are relevant — mtry = 14".
- **Grid:** 3 rows × 20 columns of feature cells (28×20 minus 2px gaps) starting at (50, 45); cells at indices 5, 23, 41, 52 are relevant — fill `rgba(39,174,96,0.75)` with `#27ae60` border width 2; all others fill `rgba(200,200,200,0.3)` with `#ccc` border width 0.5. To the right: "... (200 total)" in `#555`.
- **Grid legend:** green swatch "Relevant (4)"; gray swatch "Irrelevant (196)".
- **Probability text (left, from y=170):** bold `#1a5276` "At each split (mtry=14 random draw):"; `#555` "P(at least 1 relevant in subset) ≈ 25%"; bold red "→ 75% of splits are noise-only".
- **Right bar (from 0.55w):** bold centered title "Tree quality distribution:"; green block (60px wide, fill `rgba(39,174,96,0.65)`, stroke `#27ae60`) labeled "Signal" / "~25%"; adjoining wide block (180px, fill `rgba(231,76,60,0.15)`, stroke `#e74c3c`) labeled "Noise (dilutes votes)" / "~75%".

## Section 4: Sufficient Data for Bootstrap Diversity

Each tree trains on a **bootstrap sample (~63% unique observations)**. With small n, bootstrap samples heavily overlap — trees see nearly identical data and produce nearly identical splits. The ensemble has no diversity, no variance reduction, and the same overfitting as a single tree.

- **Breaks:** n=50 observations, 500 trees. Each bootstrap has ~32 unique observations, heavily overlapping. All trees learn the same patterns and same noise. OOB error ≈ single tree error.
- **Verify:** OOB vs. CV error — if OOB ≈ single tree CV error, ensemble isn't helping
- **Fix:** Get more data, use smaller subsample_size, or switch to regularized models

### Visualization (canvas `c4`, 720×300)

Line chart of test error vs number of trees for large n vs small n.

- **Axes:** L-shaped gray (`#bbb`) axes, padding 55; x-axis "Number of Trees" mapping 0–500; y-axis rotated "Test Error" mapping 0.1–0.6; x ticks at 1, 50, 100, 200, 500; y ticks at 0.2, 0.3, 0.4, 0.5.
- **n=5000 curve (green `#27ae60`, width 2.5):** points (trees, error) = (1, 0.45), (10, 0.30), (50, 0.22), (100, 0.19), (200, 0.17), (500, 0.16).
- **n=50 curve (red `#e74c3c`, width 2.5):** points (1, 0.48), (10, 0.42), (50, 0.40), (100, 0.39), (200, 0.39), (500, 0.38).
- **Legend (top right):** green line "n=5000 (diverse bootstrap)"; red line "n=50 (overlapping bootstrap)".
- **Annotation (bold orange `#e67e22`, centered near bottom):** "More trees can't help when all trees see the same data".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width `border-collapse` table, one `<tr>` per section; left `<td>` (42%) holds `.obj-title` + `.obj-desc` paragraph + `.obj-detail` lines, right `<td>` (58%, centered) holds the canvas. Even rows have background `#fafcfe`.
- **Detail-line markup:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses `<span class="tag tag-check">` (pill: background `#eafaf1`, color `#1e8449`); "Fix:" uses `<span class="tag tag-fix">` (pill: background `#fef9e7`, color `#b7950b`). Tag pills: inline-block, 0.75em, padding 2px 8px, radius 4px. Also defined: `.good` `#27ae60`, `.warn` `#e67e22`, `.tag-break` (background `#fdeaea`, color `#c0392b`).
- **Page style:** global reset (`* { margin:0; padding:0; box-sizing:border-box }`); body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border and 8px bottom padding; `.subtitle` `#666` 0.95rem, 32px bottom margin; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#555`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; `canvas { display:block; margin:0 auto; }`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; charts drawn with vanilla canvas 2D in IIFEs.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, teal `#16a085`, gray text `#555`/`#333`/`#444`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
