# Select Features on Full Dataset, Then Split

**Page type:** detail page (two `.card-section` blocks — The Anti-Pattern / The Design Pattern — each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Select Features on Full Dataset, Then Split

**Subtitle:** Feature selection used test data to decide which features matter — +5-10% inflated

## The Anti-Pattern

Running feature selection on the full dataset before splitting means the test set influenced which features were chosen. The selection process "peeked" at test data, inflating performance estimates by 5-10%.

- Univariate feature selection (chi-square, ANOVA)
- Correlation screening against the target
- PCA on the full dataset before splitting
- Mutual information computed on all rows

**Key point (callout):** Feature selection is a model decision. Any model decision using test data = data leakage.

### Visualization (canvas `c1`, 720×300)

Flow diagram: feature selection performed on the full dataset before the train/test split, with a leak line from the test box back to selection.

- **Full dataset box:** at x=160, y=30, 400×70; fill `rgba(26,82,118,0.08)`, stroke `#1a5276` width 2; centered bold 13px label "FULL DATASET (train + test)" in `#1a5276`.
- **Feature selection strip inside the box:** inset 20px horizontally, y=65, height 25; fill `rgba(231,76,60,0.12)`, stroke `#e74c3c` width 1.5; centered 11px red label "Feature selection uses ALL data".
- **Down arrow:** dark `#2c3e50` vertical arrow (width 2) from below the box down 35px at page center, with a filled triangular arrowhead.
- **Split boxes:** at y=150 (boxY+boxH+50), height 50: TRAIN box 220 wide left of center (fill `rgba(26,82,118,0.12)`, stroke `#1a5276` width 1.5, bold 12px centered "TRAIN" in `#1a5276`) and TEST box 160 wide right of center with a 20px gap (fill `rgba(230,126,34,0.12)`, stroke `#e67e22` width 1.5, bold 12px centered "TEST" in `#e67e22`).
- **Leak line:** red dashed line (`#e74c3c`, width 1.5, dash 4/3) from the top center of the TEST box up to the feature-selection strip (y=90).
- **Leak label:** bold 13px red centered text "Test info leaked into selection!" 80px below the split boxes.
- **Bottom note:** 12px red centered text "+5-10% inflated performance estimate" at 20px above the bottom.

## The Design Pattern

Feature selection must happen inside the cross-validation loop. Each fold independently selects features using only training data. The final feature set is the intersection or consensus across all folds.

- Wrap feature selection inside each CV fold
- Each fold: select features → train → evaluate (on fold's test)
- Final feature set = intersection of features selected across folds
- Guarantees test data never influences feature choice

**Key point (callout):** If feature selection sees any test data, the entire evaluation is compromised — not just the selection step.

### Visualization (canvas `c2`, 720×300)

5-fold cross-validation strip diagram with the test fold sliding across folds.

- **Title (bold 12px, `#1a5276`, top center, y=20):** "5-Fold Cross-Validation with Embedded Feature Selection".
- **Fold bars:** 5 horizontal bars from x=80 to x=w−80, each 28px tall, starting at y=35 with 12px gaps. For fold i (0–4), the test portion covers the [i×0.2, i×0.2+0.2] fraction of the bar.
  - Train portions: fill `rgba(26,82,118,0.2)`.
  - Test portion: fill `rgba(230,126,34,0.35)`, with an `#e67e22` border (width 1.5).
  - Whole bar border: `#aaa`, width 1.
  - Left label (10px gray `#666`, right-aligned): "Fold 1" … "Fold 5".
  - Right label (10px `#1a5276`, left-aligned): "select → train → eval" for every fold.
- **Checkmark line (bold 14px green `#27ae60`, centered, 10px below the last bar):** "✓ Each fold independently selects features — no leakage".
- **Legend (bottom, 11px):** swatch `rgba(26,82,118,0.2)` labeled "Train (feature selection here)"; swatch `rgba(230,126,34,0.35)` at x offset +220 labeled "Test (never seen during selection)"; label text `#2c3e50`.

## Regeneration instructions

- **Layout:** two `.card-section` divs, each with an `<h2>` ("The Anti-Pattern", "The Design Pattern", 1.3rem `#1a5276`, bottom border `2px solid #2980b9`) followed by a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (45%) holding a paragraph, `<ul>` bullets, and a `.key-point` callout; right `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 per chart, CSS `width: 100%` with `1px solid #e0e0e0` border and 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)` family.
- Any card links in regenerated HTML use `.html` extensions.
