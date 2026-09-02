# CNN Shape Classification

**Page type:** detail page (TOC box, then one h2 + two-column obj-table row per section: text left 45%, canvas right 55%, closing philosophy callout)
**HTML title tag:** CNN Shape Classification

**Subtitle:** MNIST for distributions — classify histogram shapes visually using a trained CNN.

## Table of Contents

**Table of Contents** (a `.toc` box with an ordered list of in-page anchor links)

1. The Core Idea (#idea)
2. The 11 Shape Classes (#classes)
3. Training Data (#training)
4. Multi-Rendering Pipeline (#pipeline)
5. Integration with Pipeline (#integration)
6. Limitations (#limitations)

## 1. The Core Idea

**Raw Data → Histogram Image → CNN → Shape Probabilities**

- Render feature values as a 64×64 grayscale histogram image
- Feed to a CNN (2 conv layers + 2 FC layers) → softmax over 11 classes
- Output: soft probabilities ("70% bimodal, 25% heavy_tail, 5% other")

**Why this works:**

- **Noise tolerance:** Rendering to 64px is a low-pass filter — tiny dips can't survive
- **No thresholds:** Model learns what matters from examples, not manual rules
- **Unlimited training data:** Generate from known distributions — perfect ground truth
- **Multi-candidate:** Softmax gives probabilities, not a hard label

### Visualization (canvas `c1`, 720×240)

Horizontal flowchart of five outlined rounded boxes (120×50, 15px gaps, centered, radius 6; fill = step color at alpha 0.15, stroke = step color width 2, bold 17px colored multi-line labels) joined by gray arrows (`#999`):

- "Raw Data" `#7f8c8d` → "64-bin / Histogram" `#f39c12` → "64×64 / Image" `#e67e22` → "CNN" `#2980b9` → "Shape / Probs" `#27ae60`.

## 2. The 11 Shape Classes

**Visual Categories — Not Mathematical Families**

- **bell:** Single symmetric hill (normal-like)
- **right_skew:** Peak left, long tail right
- **left_skew:** Peak right, long tail left
- **bimodal:** Two distinct hills with valley
- **multimodal:** 3+ hills
- **uniform:** Flat, no dominant peak
- **descending:** Monotone decrease left to right
- **ascending:** Monotone increase left to right
- **u_shaped:** High at edges, low in middle
- **spike:** One extreme concentration
- **heavy_tail:** Bell-like but fat tails

These are what a human sees at a glance — shape identity, not parametric family.

### Visualization (canvas `c2`, 720×340)

Gallery of 11 mini histograms in a 4-column grid (cell width w/4, cell height 80; bars fill `rgba(26,82,118,0.4)`, height scaled to per-shape max over 45px; shape name in 17px `#1a5276` below each, left-aligned):

- "bell": `[1,3,7,14,22,30,35,30,22,14,7,3,1]`
- "right_skew": `[5,35,30,20,12,8,5,3,2,1,1,0,0]`
- "bimodal": `[2,8,20,28,18,6,3,5,18,28,20,8,2]`
- "uniform": `[12,11,13,12,11,14,12,13,11,12,13,12,11]`
- "spike": `[1,1,2,2,3,40,3,2,1,1,1,0,0]`
- "descending": `[40,30,22,16,12,8,6,4,3,2,1,1,0]`
- "heavy_tail": `[2,4,6,10,18,30,35,30,18,10,6,4,2]`
- "u_shaped": `[25,18,10,5,3,2,2,3,5,10,18,25,30]`
- "ascending": `[0,1,1,2,3,4,6,8,12,16,22,30,40]`
- "left_skew": `[0,0,1,1,2,3,5,8,12,20,30,35,5]`
- "multimodal": `[3,15,20,8,3,2,4,18,15,4,3,16,20]`

## 3. Training Data

**Synthetic Generation — Infinite, Perfectly Labeled**

- 11 classes × 10,000 samples × 3 renderings = **330,000 training images**
- Label is your generation choice — no human labeling needed, no ambiguity
- Varied sample sizes (n=200 to n=5000) so model handles both noisy and clean
- **Boundary examples:** Deliberately generated near class borders (e.g., Student-t with df=3→20 spans bell↔heavy_tail)

**Key:** The model sees all 3 rendering styles during training with the same label, forcing it to learn shape rather than rendering artifacts.

### Visualization (canvas `c3`, 720×240)

Three bell histograms at increasing sample size / decreasing noise.

- **Title (bold 17px `#1a5276`, top left):** "Training: Same Shape at Different Sample Sizes".
- **Panels (200px wide at x=40, 270, 500; y=50, 130px tall):** 20 bins each generated as a Gaussian curve `exp(-0.5*((b-10)/4)^2)*30` plus seeded random noise scaled by multiplier 8, 3, 1 respectively (seeded PRNG, mulberry32 seeds 100/150/200), clamped at 0; bars fill `rgba(26,82,118,0.4)`.
- **Labels (centered, 17px `#666`):** "n=200 (noisy)", "n=1000 (clear)", "n=5000 (clean)"; below each in bold 17px `#27ae60`: "→ all labeled \"bell\"".

## 4. Multi-Rendering Pipeline

**Same Data → 3 Renderings → Compare Outputs**

- **Histogram A (Sturges):** Coarse (12 bins). Sees major modes.
- **Histogram B (√n):** Fine (31+ bins). Resolves close peaks.
- **KDE Density:** Smooth curve. Immune to bin-size effects.

**Agreement = confidence.** If all 3 say "right_skew" → high confidence. If histograms say "multimodal" but KDE says "bell" → rounding artifact detected (comb teeth from integer data).

**Example:** SalePrice — all 3 renderings: right_skew 65-78% → high confidence. Year Built — histograms: multimodal, KDE: bimodal → artifact detected, trust KDE.

### Visualization (canvas `c4`, 720×280)

Three agreement rows plus a verdict line.

- **Title (bold 17px `#1a5276`, top left):** "3 Renderings → Compare → Confidence".
- **Rows (full-width minus 80px, 48px tall, 60px spacing from y=50; agree rows use `#27ae60`: fill at alpha 0.1, rounded stroke width 1.5; label left in 17px `#333`, result bold 17px centered in row color, "✓ agree" right-aligned in green 17px):**
  - "Sturges (12 bins)" — "right_skew 65%" — ✓ agree.
  - "√n (54 bins)" — "right_skew 72%" — ✓ agree.
  - "KDE (smooth)" — "right_skew 78%" — ✓ agree.
- **Verdict (bold 17px `#27ae60`, bottom center):** "All 3 agree → HIGH CONFIDENCE: right_skew".

## 5. Integration with Pipeline

**Shape → Routing → Test Selection**

**Shape determines pipeline routing:**

- **bell / heavy_tail:** Direct to testing. t-test candidates viable.
- **right_skew / left_skew:** Test on transformed data or use non-parametric.
- **bimodal / multimodal:** Split first, then test each component. t-test inappropriate on combined.
- **spike:** Separate spike mass, test remainder.
- **uniform:** No central tendency — range-based analysis only.

**CNN + Multi-Resolution Persistence:** If CNN says bimodal (0.72) and persistence=0.85 → high confidence. If CNN says bimodal (0.55) but persistence=0.40 → marginal, report uncertain.

### Visualization (canvas `c5`, 720×280)

Routing table rendered as five colored rows.

- **Title (bold 17px `#1a5276`, top left):** "Shape → Pipeline Route".
- **Rows (full-width minus 80px, 35px tall, 42px spacing from y=48; fill = row color at alpha 0.1, rounded stroke width 1.5; shape bold 17px in row color at x=60, action 17px `#333` at x=280 prefixed "→  "):**
  - "bell / heavy_tail" → "Direct to testing (t-test viable)" — `#27ae60`.
  - "right_skew / left_skew" → "Transform or non-parametric" — `#e67e22`.
  - "bimodal / multimodal" → "Split first, then test each" — `#8e44ad`.
  - "spike" → "Separate spike, test remainder" — `#e74c3c`.
  - "uniform" → "Range-based only (no mean test)" — `#7f8c8d`.

## 6. Limitations

**Hard Cases and Boundaries**

- **bell vs heavy_tail:** Difference is only in tails — few pixels. Hardest distinction.
- **Unequal mixtures (80/20):** Minor component may not create a visible second peak.
- **Very small n (<100-150):** Histogram is so noisy that shape is genuinely ambiguous. CNN should output low confidence.

**What CNN does NOT replace:**

- Goodness-of-fit tests (KS, Anderson-Darling)
- Parameter estimation
- Statistical significance testing

It replaces: the first-pass "what does this look like?" question a human answers by glancing at the plot.

### Visualization (canvas `c6`, 720×240)

Horizontal difficulty bar chart.

- **Title (bold 17px `#1a5276`, top left):** "Hard Cases for the CNN".
- **Bars (from x=60, 35px tall, 60px spacing from y=50; width = difficulty × 400px; fill `rgba(231,76,60,0.2)`, stroke `#e74c3c` width 1; case name bold 17px `#333` and reason 17px `#666` inside the bar; percentage bold 17px `#e74c3c` right of the bar):**
  - "bell vs heavy_tail" — "Difference only in tails (few pixels)" — 0.9 → "90% hard".
  - "Unequal mixture (80/20)" — "Minor peak may not be visible" — 0.7 → "70% hard".
  - "Very small n (<150)" — "Histogram too noisy to classify" — 0.85 → "85% hard".

## Callout (philosophy box)

**v3-full results:** 11 classes. Discriminative: 93.1% top-1, 99.3% top-2. Generative: 92.6% top-1, 99.4% top-2. Independent per-class scores. FP rates: multimodal 0%, u_shaped 0.1%.

## Regeneration instructions

- **Layout:** single page: h1, `.subtitle`, a `.toc` box (background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, radius 4px, bold "Table of Contents" heading + `<ol>` of anchor links in `#2980b9`), then one h2 (with `id` anchor) per section, each followed by its own single-row `.obj-table`: left `<td>` (45%) holds `.obj-title` + bullets/paragraphs, right `<td>` (55%, centered) holds the canvas. Ends with a `.philosophy` callout.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvases:** intrinsic sizes as given per chart (c1 720×240, c2 720×340, c3 720×240, c4 720×280, c5 720×280, c6 720×240); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; c3 uses a seeded mulberry32 PRNG for reproducible noise. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gold `#f39c12`, purple `#8e44ad`, gray `#7f8c8d`, bar fill `rgba(26,82,118,0.4)`.
