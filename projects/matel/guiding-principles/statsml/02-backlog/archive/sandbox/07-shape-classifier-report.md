# Shape Classifier — Results Report

**Page type:** detail page (two-column `.two-col` table layout: text/tables left 40%, canvas right 60%, one row per section; blue note callout at top)
**HTML title tag:** Shape Classifier — Results Report

Top callout (`.note`, blue background `#d6eaf8`, border `#85c1e9`):

11-class generative model (independent sigmoid heads). Same 64×64 grayscale + SE band input. Discriminative companion achieves 93.3% top-1 with 95.9% agreement.

Shared class color palette used across all charts: bell `#2980b9`, right_skew `#27ae60`, left_skew `#1abc9c`, heavy_tail `#8e44ad`, bimodal `#e67e22`, multimodal `#d35400`, u_shaped `#c0392b`, spike `#f39c12`, descending `#16a085`, ascending `#2c3e50`, zero_inflated `#e74c3c`. Class order: bell, right_skew, left_skew, heavy_tail, bimodal, multimodal, u_shaped, spike, descending, ascending, zero_inflated. Short names: bell, r_skew, l_skew, h_tail, bimod, multi, u_shape, spike, desc, asc, zero_infl.

## 1. Summary

- **11 classes** — bell, right_skew, left_skew, heavy_tail, bimodal, multimodal, u_shaped, spike, descending, ascending, zero_inflated
- **890K parameters**
- **96.6% mean recall**, 0.966 mean F1
- Input: 64×64 grayscale, 23 bins, SE bands
- Multi-label: each head fires independently

("11 classes", "890K parameters", "96.6% mean recall" are `.metric` spans, bold `#1a5276`.)

### Visualization (canvas `chart-f1`, 420×280)

Horizontal bar chart, "Per-Class F1 Score" (bold 11px `#1a5276` title, top center).

- **Data (F1 per class, in class order):** bell 0.929, right_skew 0.926, left_skew 0.945, heavy_tail 0.959, bimodal 0.974, multimodal 0.982, u_shaped 0.994, spike 0.980, descending 0.968, ascending 0.972, zero_inflated 0.996.
- **Layout:** bars start at x=70, height 20, gap 3, top margin 24, width = F1/1.0 × (W−70−50); bar color = class color at 75% alpha.
- **Labels:** short class name right-aligned `#333` 9px left of bar; F1 value (3 decimals) bold 9px right of bar, colored `#27ae60` when ≥0.96 else `#e67e22`.
- **Reference line:** vertical dashed gray `#aaa` (dash 3/3) at F1=0.95, labeled "0.95" in 8px `#999` above it.

## 2. Recall & False Positives

- **Perfect:** u_shaped (100%), ascending (99.9%), multimodal (99.9%)
- **Strong:** zero_inflated (99.2%), heavy_tail (98.6%), spike (98.4%)
- **Weakest:** bell (88.4%) — cross-activates with right_skew/heavy_tail by design

Small gray paragraph (0.82em `#666`): Highest FP rates are between structurally related shapes (bell↔heavy_tail, spike↔heavy_tail, zero_inflated↔multimodal) — expected and informative for routing.

### Visualization (canvas `chart-recall-fp`, 420×280)

Grouped vertical bar chart, "Recall vs Max FP" (bold 11px `#1a5276` title, top center). Two bars per class, 11 groups.

- **Recall data (%, class order):** 88.4, 96.5, 98.3, 98.6, 97.2, 99.9, 100.0, 98.4, 96.2, 99.9, 99.2.
- **Max FP data (%, class order):** 46.5, 34.8, 6.5, 46.9, 9.4, 15.1, 2.5, 44.7, 20.2, 0.9, 40.4.
- **Layout:** margins left 50, top 28, bottom 40, right 20; group width = plotW/11; bar width = 38% of group; recall bar green `#27ae60` at 70% alpha, FP bar red `#e74c3c` at 70% alpha.
- **Axes:** horizontal `#eee` gridlines at 0/25/50/75/100% with right-aligned `#999` 8px percentage labels; x labels are short class names in 7px `#333` rotated 36° (π/5) below each group.
- **Legend (top right):** green swatch "Recall", red swatch "Max FP" (9px `#333`).

## 3. Real Data Validation

Selected features from Ames Housing (n=2930) and Adult Census (n=48842). No retraining — model sees real histograms for the first time.

`.data` table (header row dark blue `#1a5276` with white text; green check badges `.badge-green`, background `#d4efdf`, text `#1e8449`):

| Feature | Top-1 | Check |
|---------|-------|-------|
| SalePrice | right_skew (100%) | ✓ |
| Lot Area | heavy_tail (97%) | ✓ |
| Year Built | ascending (100%) | ✓ |
| 2nd Flr SF | zero_inflated (100%) | ✓ |
| Garage Area | bell (100%) | ✓ |
| Capital_Gain | zero_inflated (100%) | ✓ |
| Hours/week | spike (100%) | ✓ |
| Education_Num | multimodal (91%) | ✓ |

### Visualization (canvas `chart-samples`, 420×280)

Small-multiples gallery, "Sample Shapes (synthetic examples)" (bold 11px `#1a5276` title, top center). 3 columns × 2 rows of 120×80 mini bar panels (background `#f7f9fb`, border `#ddd`; start x=30, y=30; gaps 18 horizontal, 30 vertical), each drawing 23 bars from a generator function at 75% alpha in the class color, with class-name label centered below in 9px class color:

- **bell** (`#2980b9`): Gaussian, exp(−0.5·((i−11)/4)²)
- **right_skew** (`#27ae60`): 0 for i<3 (and i=3), else exponential decay exp(−0.3·(i−4))
- **bimodal** (`#e67e22`): sum of two Gaussians centered at 6 and 17 (sd 2)
- **spike** (`#f39c12`): 1 at i=10, else 0.05
- **u_shaped** (`#c0392b`): exp(−0.3·(i−1)) + exp(−0.3·(22−i))
- **zero_inflated** (`#e74c3c`): 1 at i=0, else 0.08·exp(−0.3·i)

## 4. Key Observations

Three callouts in the left column:

Blue `.note`: **Biggest win:** zero_inflated vs spike distinction. The old 3-class model conflated both as "spike." Now the pipeline knows: zero_inflated → split off zeros first; spike → model the concentration directly.

Yellow `.note note-warn` (background `#fef9e7`, border `#f9e79f`): **Known FP:** bell↔heavy_tail (47% cross-activation). By design — heavy_tail IS bell-shaped in center. Pipeline rule: if both fire, prefer heavy_tail.

Blue `.note`: **Discrete ordinals:** Features like Overall Qual activate spike+multimodal. The type classifier (int_cat) should gate these away from shape analysis.

### Visualization (canvas `chart-comparison`, 420×280)

Canvas-drawn comparison table, "3-Class PoC → 11-Class: Key Upgrades" (bold 11px `#1a5276` title, top center).

- **Columns (bold 9px `#1a5276` header):** "Feature" (x=15), "3-class" (x=100), "→" (x=165), "11-class" (x=185), "Insight gained" (x=300); rows 38px tall starting at y=32+14, even rows get `#f7f9fb` background stripe.
- **Rows (feature / old pill / new pill / gain):**
  - SalePrice | mountain | right_skew | Skew direction
  - Lot Area | mountain | heavy_tail | Outlier behavior
  - Year Built | valley | ascending | Monotone trend
  - 2nd Flr SF | spike | zero_inflated | Zero mechanism
  - Capital_Gain | spike | zero_inflated | Zero mechanism
  - Garage Area | mountain | bell | Symmetry confirmed
- **Styling:** old class in a gray pill (fill `#ddd`, text `#666`, 60×16); arrow "→" in `#1a5276`; new class in a pill filled with the class color at 15% alpha and bold 9px text in the class color (80×16); gain text in green `#27ae60` 9px.

## Regeneration instructions

- **Layout:** report page of four numbered h2 sections (`id`s s1-s4), each a `.two-col` table with one `<tr>`: left `<td>` (40%) holds bullets/tables/callouts, right `<td>` (60%) holds one canvas. Cells are white cards (background `#fff`, border `1px solid #ddd`, radius 8px, padding 16px, border-spacing 12px) on a page background of `#f8f9fa`.
- **Page CSS:** body system sans-serif, margin 40px, text `#2c3e50`, background `#f8f9fa`. h1 `#1a5276` with bottom border `3px solid #1a5276`; h2 `#1a5276` with bottom border `2px solid #2980b9`. `.note` blue callout (background `#d6eaf8`, border `#85c1e9`, radius 8px, 0.88em); `.note-warn` variant yellow (background `#fef9e7`, border `#f9e79f`). `.metric` bold `#1a5276`. `ul` 0.9em, line-height 1.8. `table.data` 0.82em, header background `#1a5276` white text, row bottom borders `#eee`, hover `#f0f8ff`. `.badge` rounded pill 0.75em bold; `.badge-green` `#d4efdf`/`#1e8449`; `.badge-orange` `#fdebd0`/`#d35400` (defined but unused).
- **Canvas:** each `<canvas>` declared with `height="280"` and CSS `width:100%`; a shared `setup(id, w, h)` helper sets 420×280 logical size with `window.devicePixelRatio` scaling (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange, plus the 11-class color map listed at the top of this spec.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
