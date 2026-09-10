# Neural Networks

**Page type:** detail page (two-column obj-table layout: text left 42%, canvas right 58%, one row per section)
**HTML title tag:** Neural Networks - ML Assumptions

**Subtitle:** Universal approximators that memorize freely on small data — requiring massive samples, careful scaling, and explicit regularization.

## What It Does

- Stacks layers of neurons with non-linear activations to learn arbitrary input→output mappings. Universal approximator — given enough neurons, can represent any function.
- **Best For:** Image classification, NLP (translation, summarization, sentiment), complex non-linear relationships where interpretability is secondary
- **Data:** Large datasets (1000s+ samples), numeric features, images/text/tabular. Requires feature scaling.

### Visualization (canvas `c0`, 720×300)

Network diagram: three fully-connected layers of circular nodes.

- **Layers:** 3 input nodes at x=20% width (color `#2980b9`), 4 hidden nodes at x=50% (color `#1a5276`), 2 output nodes at x=80% (color `#27ae60`); node radius 18, vertical spacing 55px, each layer vertically centered; white node borders (width 2.5).
- **Connections:** every node in one layer connected to every node in the next by `rgba(26,82,118,0.6)` lines (width 1.5), drawn edge-to-edge between node rims.
- **Layer labels (bold `#444`, bottom):** "Input (3)", "Hidden (4)", "Output (2)".
- **Top label (gray `#555`, above hidden layer):** "ReLU / σ activations".

## Large Sample Size (n >> parameters)

- Neural networks have **orders of magnitude more parameters than traditional models**. A modest MLP with [50→64→32→1] has ~3,500 parameters. Without enough data to constrain them, the network memorizes training examples by their unique feature combinations — achieving perfect training accuracy while predicting randomly on new data.
- **Breaks:** 800 patient records, 3000 parameters. Training accuracy: 98%, test accuracy: 54%. The network has enough capacity to store each patient as a lookup table — no generalization occurs.
- **Verify:** Train/val gap > 15% = memorization; learning curves; effective parameters vs. n
- **Fix:** More data, dropout, weight decay, smaller architecture, or switch to gradient boosting for tabular < 10K

### Visualization (canvas `c1`, 720×300)

Learning-curve line chart: training accuracy climbs to 98% while test accuracy stays near 54%.

- **Axes:** L-shaped gray `#bbb` axes; labels gray `#555`: x "Training Epochs" (0–50), rotated y "Accuracy" (scale 0.3–1.05).
- **Training curve (blue `#2980b9`, width 2.5):** `acc = 0.5 + 0.48·(1 − e^(−i/6))` over epochs 0–50, saturating near 0.98.
- **Test curve (red `#e74c3c`, width 2.5):** `acc = 0.5 + 0.06·(1 − e^(−i/3)) − 0.01·max(0,(i−10)/40)` — flat near 0.54–0.56.
- **Gap annotation:** bold orange `#e67e22` text "44% gap = memorization" near epoch 35; dashed orange vertical line (dash 3/3) at epoch 45 spanning from accuracy 0.97 down to 0.54.
- **Legend (top right):** blue "Train: 98%", red "Test: 54%".
- **Architecture inset (top left):** `#f8f9fa` box (115×55, `#ddd` border) with gray lines "[50→64→32→1]" and "n=800, params=3000", plus bold red line "3.75× overparameterized".

## Feature Scaling (Gradient Stability)

- Neural networks learn via gradient descent. Unscaled features cause **vanishing or exploding gradients** — large inputs saturate activation functions (gradient ≈ 0) while small inputs produce tiny updates. The network effectively ignores features with the wrong scale, or fails to converge entirely.
- **Breaks:** Input "income" (0–200K) saturates sigmoid from epoch 1. Gradient for that neuron ≈ 0. Meanwhile "normalized_score" (0–1) trains normally. Income is effectively dead weight despite being predictive.
- **Verify:** Check gradient magnitudes per layer; dead neurons (always 0 or saturated)
- **Fix:** StandardScaler to N(0,1), batch normalization, or LayerNorm within the network

### Visualization (canvas `c2`, 720×300)

Sigmoid curve with shaded active vs saturated zones and markers for scaled vs unscaled inputs.

- **Title (bold `#1a5276`, top center):** "Sigmoid activation: input scale determines gradient".
- **Axes:** L-shaped gray `#bbb` axes; labels gray `#555`: x "Weighted Input (w×x)" spanning −8 to +8 (tick labels −6, −3, 0, 3, 6), rotated y "σ(x) output" (0–1).
- **Curve:** sigmoid `y = 1/(1+e^(−x))` in `#1a5276`, width 2.5.
- **Active zone:** green band `rgba(39,174,96,0.2)` over x ∈ [−3, 3], labeled bold green "Active zone" with subtext "gradient ≈ 0.25".
- **Saturated zones:** red bands `rgba(231,76,60,0.15)` over x ∈ [−8,−3] and [3,8], each labeled bold red "Saturated" with subtext "gradient ≈ 0".
- **Markers:** green dot (radius 6) at (0.5, σ=0.62) labeled green "Scaled input (learns)"; red dot at (7, σ=0.999) labeled red "Unscaled input (dead)".

## Appropriate Architecture (No Theory for Choice)

- There is **no principled way to choose** the number of layers, neurons per layer, or activation functions. Too small → underfits (can't represent the true function). Too large → overfits (memorizes noise). Unlike linear models where the structure matches the assumption, neural architecture is a hyperparameter with no analytical solution.
- **Breaks:** Single hidden layer with 4 neurons for a complex 50-feature interaction. Network cannot represent the true function — caps at ~60% accuracy regardless of training time. Adding layers without adding data just shifts the problem to overfitting.
- **Verify:** Training loss plateaus high = underfitting; val diverges = overfitting; neither = right size
- **Fix:** Start small and grow, cross-validate architecture, use established architectures for problem type

### Visualization (canvas `c3`, 720×300)

Classic complexity-vs-error chart with underfit / sweet spot / overfit zones.

- **Title (bold `#1a5276`, top center):** "Model complexity vs. performance".
- **Axes:** L-shaped gray `#bbb` axes; labels gray `#555`: x "Network Size (neurons)" (0.5–10), rotated y "Error".
- **Training error (blue `#2980b9`, width 2.5):** monotone decreasing `e = 0.6·e^(−x/2) + 0.02`.
- **Validation error (red `#e74c3c`, width 2.5):** U-shaped `e = 0.5·e^(−x/2.5) + 0.1 + 0.03·max(0, x−4)`.
- **Zones (shaded vertical bands):** underfit x ∈ [0.5, 2.5] `rgba(230,126,34,0.18)`; sweet spot x ∈ [2.5, 5] `rgba(39,174,96,0.15)`; overfit x ∈ [5, 10] `rgba(231,76,60,0.15)`. Bold labels below the bands: "Underfit" (`#e67e22`), "Sweet spot" (`#27ae60`), "Overfit" (`#e74c3c`).
- **Legend (top right):** blue line sample "Training error", red line sample "Validation error".

## No Built-in Class Imbalance Handling

- Neural networks optimize a **loss function averaged over all samples**. With 99/1 class imbalance, predicting "majority" everywhere achieves 99% accuracy and near-minimum loss. The gradient signal from 1% minority examples is drowned by the 99% majority — the network never learns to recognize the rare class.
- **Breaks:** Fraud detection: 0.5% fraud rate. Network predicts "not fraud" for everything. Loss = 0.005 (excellent!). Recall on fraud = 0%. The model is optimally useless.
- **Verify:** Confusion matrix (not just accuracy), per-class recall, ROC-AUC
- **Fix:** Class-weighted loss, focal loss, SMOTE/oversampling, threshold tuning on calibrated probabilities

### Visualization (canvas `c4`, 720×300)

Gradient-signal bar (99% majority sliver diagram) above a 2×2 confusion matrix showing 0% recall.

- **Title (bold `#1a5276`, top center):** "Gradient signal by class (99/1 imbalance)".
- **Gradient bar:** left-aligned label (gray `#555`): "Gradient signal per batch (batch=64):". Horizontal bar (height 50): 99% of width filled blue `rgba(41,128,185,0.55)` with `#2980b9` border, centered blue caption "Majority class: 63 samples → gradient pushes \"predict negative\""; remaining ~1% sliver filled red `rgba(231,76,60,0.7)`; red arrow pointing to the sliver with labels "1 sample" / "(minority)".
- **Confusion matrix (centered, cells 110×50):** heading bold gray "What the network converges to:". Column headers "Pred Neg", "Pred Pos". Cells: TN — green tint `rgba(39,174,96,0.35)`, bold green "9900 TN"; FP — `#f8f8f8`, gray "0 FP"; FN — red tint `rgba(231,76,60,0.55)`, bold red "100 FN !!!"; TP — `#f8f8f8`, gray "0 TP".
- **Summary (bottom center, bold red `#e74c3c`):** "Accuracy: 99% — Recall: 0%".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (42%) holds `.obj-title` + `.obj-desc` paragraph + `.obj-detail` lines, right `<td>` (58%, centered) holds the canvas. Even rows have background `#fafcfe`.
- **Detail-line labels:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses `<span class="tag tag-check">` (background `#eafaf1`, text `#1e8449`); "Fix:" uses `<span class="tag tag-fix">` (background `#fef9e7`, text `#b7950b`). `.tag` is inline-block, 0.75em, padding 2px 8px, radius 4px.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; subtitle `#666` 0.95rem; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#555`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart (all 720×300); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; `canvas { display:block; margin:0 auto; }`.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#555`/`#333`/`#444`.
