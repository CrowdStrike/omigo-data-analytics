# Pitfall: Rare Category Explosion

**Page type:** detail page (card-section layout: one h2 section per block, two-column table text left 45% / canvas right 55%)
**HTML title tag:** Rare Category Explosion

**Subtitle:** One-hot encoding high-cardinality features creates sparse columns.

## Section 1: The Problem

**Tags:** `the trap` (red pill), `one-hot` (blue pill)

- **Column explosion** — one-hot encoding a 2000-level feature creates 2000 sparse binary columns
- **High cardinality** — user_id, product_sku, or zip_code carry thousands of distinct values
- **Thin support** — most categories appear in under 10 samples, so columns are nearly all zeros
- **Spurious patterns** — the model learns rare-category associations that are pure coincidence
- **Regularization limit** — no penalty rescues thousands of columns with almost no evidence each

*Example:* One-hot encoding 2000 product SKUs gives train AUC 0.92 but test AUC 0.68, since most test SKUs appeared under 5 times in training.

**Impact:** Training scores look great because the model memorizes rare categories, then test performance collapses on categories it barely saw.

### Visualization (canvas `c1`, 720×300)

Diagram: one categorical column exploding into thousands of sparse binary columns.

- **Title (bold 14px, `#1a5276`, top center):** "Feature Explosion: 1 Categorical Column → 2000 Binary Columns".
- **Left box:** white 140×60 rectangle at (80, 120), stroke `#27ae60` width 3; green text: bold 12px "product_sku", 11px "(1 column)" and "2000 categories".
- **Arrow:** orange `#e67e22` line width 3 from (220, 150) to (280, 150) with filled triangular head; bold 11px orange labels "ONE-HOT" / "ENCODE" above.
- **Right block:** 30 thin columns (12px wide, 60px tall, 2px gaps) starting at x=320, y=120; first 5 filled `rgba(231,76,60,0.4)`, the rest `rgba(231,76,60,0.15)`, all stroked `#e74c3c` width 1. Labels above in red: bold 12px "2000 BINARY COLUMNS" and 10px "(mostly zeros — sparse!)".
- **Stats block (11px `#444`, left-aligned at x=80):** "Density: 0.05% (1 one per column, 1999 zeros)", "Avg samples per category: 2.5", "Categories with < 10 samples: 87%".
- **Bottom line (bold 11px red `#e74c3c`):** "Result: Model memorizes noise, fails to generalize".

## Section 2: Why It Happens

**Tags:** `root cause` (orange pill), `sparsity` (blue pill)

- **Tiny support** — 2000 categories over 5000 samples leaves 2.5 observations per category
- **No statistical power** — a handful of samples cannot support any per-category conclusion
- **Fingerprint effect** — rare levels identify individual rows, so fitting them is memorization
- **Pure-leaf splits** — trees split on rare categories into pure leaves that encode noise
- **Unfounded weights** — linear models assign coefficients backed by almost no evidence

*Example:* Zip 94301 appears 4 times in training, all positive, so the model learns a strong weight — in test it is only 50% positive.

**Root Cause:** A weight learned from 3 chance positives transfers nowhere, so predictions on new or unseen rare categories regress to the mean.

### Visualization (canvas `c2`, 720×300)

Three-bar chart: AUC on train / validation / test showing the overfitting gap.

- **Title (bold 14px, `#1a5276`, top center):** "Performance Degradation with High-Cardinality One-Hot Encoding".
- **Bars:** 80px wide, baseline y=240, height = score × 160; fill at 0.7 alpha with solid stroke width 2.
  - Train: score 0.92, centered x=200, color `#27ae60`, bold 16px value "92" above, 12px label "Train" below, 10px green annotation "Memorizes" / "rare patterns" under the label.
  - Validation: score 0.75, centered x=360, color `#e67e22`, value "75", label "Validation".
  - Test: score 0.68, centered x=520, color `#e74c3c`, value "68", label "Test", 10px red annotation "New rare" / "categories".
- **Callout (bold 12px red `#e74c3c`, centered near top):** "24-point gap: overfitting signal".

## Section 3: The Correct Approach

**Tags:** `the fix` (green pill), `encoding` (blue pill)

- **Cardinality check** — once distinct values reach the dozens (~50), reconsider one-hot
- **Frequency audit** — flag categories with fewer than a few dozen samples as thin support
- **Target encoding** — replace each level with its mean target, cross-validated to avoid leakage
- **Frequency encoding** — replace each level with its count or proportion in the data
- **OTHER bucket** — group rare levels together so every remaining level has real support
- **Embeddings** — for thousands of levels, learn entity embeddings instead of columns

*Example:* Replacing one-hot product_sku with target and frequency encodings drops dimensionality from 2000 to 2 and lifts test AUC from 0.68 to 0.84.

**Fix:** Use target or frequency encoding for high-cardinality features, bucket thin categories into "OTHER", and learn embeddings for thousands of levels.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: train vs test AUC for four encoding strategies.

- **Title (bold 14px, `#1a5276`, top center):** "Encoding Strategy Comparison (Test AUC)".
- **Strategies (centered at x = 150, 320, 490, 660; baseline y=230; height = score × 140; bars 35px wide, train bar at x−45, test bar at x+10):**
  - "One-Hot (2000 cols)" — train 0.92, test 0.68 (two-line label).
  - "Target Encoding" — train 0.88, test 0.84.
  - "Frequency + Grouping" — train 0.86, test 0.83.
  - "Embeddings (dim=10)" — train 0.89, test 0.85.
- **Colors:** train bars fill `rgba(52,152,219,0.5)`, stroke `#3498db`; test bars green `#27ae60` if test > 0.80 else red `#e74c3c` (0.7 alpha fill, solid stroke). Bold 10px score values ("92", "68", etc.) above each bar in the bar's color; 10px `#444` two-line strategy labels below.
- **Legend (top left):** blue swatch "Train", green swatch "Test".
- **Caption (bold 11px green `#27ae60`, bottom center):** "Target/Frequency encoding: better generalization, lower dimensionality".

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: left `td.text-col` (45%) holding `.tags` pills, `<ul>` bullets, `.example` italic paragraph, and `.key-point` callout; right `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; `li b` in `#1a5276`; bullets 0.92rem.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border with 4px radius; scale via a shared `setup(id)` helper using `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, train blue `#3498db`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#666`/`#444`/`#333`. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
