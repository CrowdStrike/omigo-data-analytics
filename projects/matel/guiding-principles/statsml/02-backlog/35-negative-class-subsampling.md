# Negative Class Subsampling

**Page type:** detail page (backlog-style layout: intro callout, numbered h2 sections, 2-col text/viz tables plus full-width list sections)
**HTML title tag:** Negative Class Subsampling

**Subtitle:** The sampling ratio is a hyperparameter with no principled science behind its selection

**Intro callout (blue accent):** When the positive class is extremely rare, models require subsampling of the negative class. The ratio everyone picks (1:1, 1:5, 1:10) is chosen by gut feel — nobody asks what properties of the data determine the right ratio.

## 1. The Problem

**Rare positive class → must subsample negatives**

- Many ML models (logistic regression, neural nets, trees) need both pos and neg examples
- Fraud: 0.1% positive. 1M transactions → 1000 fraud, 999,000 legit.
- Training on raw ratio: model learns "predict negative always" (99.9% accurate, useless)
- Standard fix: subsample negatives to some ratio (1:1, 1:3, 1:5, 1:10...)

**Key-point callout (red accent):** The ratio is a hyperparameter chosen by gut feel, not science.

### Visualization (canvas `c1`, 720×300)

Stacked proportion bars: the raw imbalance vs subsampled ratios.

- **Title (bold 16px, `#1a5276`, top center):** "Class Imbalance: 0.1% Positive (Fraud Detection)".
- **Full bar** at y=50, 40px tall, from x=60 to x=w−60: fill `rgba(41,128,185,0.4)` with `#2980b9` 1.5px border; a tiny positive sliver at the left edge (max(3px, 0.1% of the bar width)) in `rgba(231,76,60,0.8)`. Labels (15px `#333`) under the bar: left "Pos: 0.1%", right "Neg: 99.9%".
- **Four subsampled rows** starting at y=120, each 30px tall with 8px gaps, ratio label right-aligned to the left of the bar (15px `#333`): "1:1", "1:5", "1:10", "1:100". Each row splits the bar width by pos fraction 1/(1+neg): positive segment `rgba(231,76,60,0.7)`, negative segment `rgba(41,128,185,0.4)`, `#999` 0.5px border around the row.
- **Data-loss annotations** (15px red `#e74c3c`, left-aligned right of each bar), computed as (1+neg)/1000 of the data kept: "0.2% data kept", "0.6% data kept", "1.1% data kept", "10.1% data kept".

## 2. Current State of Practice

**What people do (no principled basis)**

- **1:1 ratio:** Most common default. Maximizes pos signal, discards 99%+ of negative data. Loses negative class structure.
- **1:5 or 1:10:** "Feels like a good compromise." No theoretical justification beyond "more data is better."
- **Grid search:** Try [1:1, 1:3, 1:5, 1:10], pick by validation AUC. Expensive, no insight into WHY one ratio works.
- **SMOTE/oversampling pos:** Synthesizes fake positives instead of discarding negatives. Creates data that doesn't exist.
- **Class weights:** Weight loss function by inverse frequency. Mathematically equivalent to subsampling but keeps all data.

**Key-point callout (red accent):** None of these approaches ask: **what properties of the negative class distribution determine the optimal ratio?**

### Visualization (canvas `c2`, 720×300)

Row list of current methods with verdict tags.

- **Title (bold 16px, `#1a5276`, top center):** "How Ratios Are Actually Chosen (Spoiler: Guessing)".
- **Five rows** starting at y=45, each 32px tall (38px pitch), spanning x=60 to w−60; each row filled with its color at 12% alpha and stroked in its color (1.5px); method name left-aligned (15px `#333`), verdict right-aligned (bold 15px in the row color):
  - '"Just use 1:1"' — verdict "?", `#e74c3c`
  - '"Try 1:5, worked last time"' — verdict "?", `#e67e22`
  - "Grid search [1:1..1:20]" — verdict "$$$", `#f39c12`
  - "SMOTE (synthesize positives)" — verdict "fake data", `#8e44ad`
  - "Class weights in loss" — verdict "implicit", `#2980b9`
- **Caption (bold 15px `#c0392b`, bottom center):** "None ask: what properties of the data determine the right ratio?"

## 3. Why This Is Hard

**The optimal ratio depends on things nobody measures**

- **Decision boundary complexity:** Simple boundary → fewer negatives needed near it. Complex boundary → need dense negative coverage everywhere.
- **Negative class structure:** If negatives are uniform, random subsampling works. If negatives have clusters, subsampling may eliminate entire clusters.
- **Overlap region density:** The only negatives that matter for the decision boundary are those NEAR positives. Far-away negatives are wasted computation.
- **Feature space dimensionality:** Higher dimensions → need exponentially more negatives to cover the space. Curse of dimensionality hits subsampled data harder.
- **Model capacity:** A high-capacity model can memorize all positives regardless of ratio. A low-capacity model needs the ratio to control what it learns.

### Visualization (canvas `c3`, 720×300)

Scatter plot: positive cluster with a boundary zone vs trivially far negatives.

- **Title (bold 16px, `#1a5276`, top center):** "Only Negatives Near the Boundary Matter".
- **Positives:** 30 points in `rgba(231,76,60,0.7)` (4px radius), tight cluster centered at (250, 130), ±40px x-jitter, ±30px y-jitter; seeded LCG random (seed 42, multiplier 16807 mod 2147483647).
- **Negatives:** 150 points in `rgba(41,128,185,0.4)` (3px radius), spread uniformly over x in [60, 660], y in [40, 240].
- **Boundary zone:** dashed (5/3) orange `#e67e22` width-2 ellipse centered (250, 130), radii 70×55; orange 14px two-line label at (325, 90/105): "boundary zone" / "(informative negatives)".
- **Gray label (14px `#555`) at (500, 50/65):** "trivial negatives" / "(wasted computation)".
- **Legend (bottom left):** red dot + "Positive", blue dot + "Negative" (14px `#333`).

## 4. Directions to Explore

- **Profile-driven subsampling:** Use feature profiling (shape, separation, overlap) to determine which negatives are "near the boundary" vs "trivially far away." Keep all near-boundary negatives, subsample the rest aggressively. The ratio becomes feature-dependent, not a global constant.
- **Negative class stratified subsampling:** Profile the negative class distribution structure (clusters, modes, density regions). Subsample proportional to structure — maintain representation from each mode. A bimodal negative class needs samples from both peaks regardless of ratio.
- **Information-theoretic ratio selection:** The optimal ratio is the one that maximizes information gain per training example. At some point, additional negatives add zero information (they're already "correctly trivial"). Detect the saturation point — that's your ratio.
- **Adaptive ratio via learning curve:** Train with 1:1, 1:2, 1:4, 1:8... Plot validation metric vs ratio. The curve has diminishing returns — the elbow is the optimal ratio. But this is just grid search with a plot. Need a faster predictor.

## 5. Connection to Our Pipeline

**What we already know that's relevant**

- **Bucket purity:** Buckets that are 99.9% negative are "trivially far." Subsample these aggressively.
- **Overlap buckets:** Buckets with mixed pos/neg are the decision boundary. Keep ALL negatives here.
- **Shape of negative class:** If negative is multimodal, each mode needs representation.
- **Effective n per bucket:** After subsampling, must still have n≥30 per class per bucket for profiling to work.

**Key-point callout (red accent):** **The link:** Our feature profiling tells us the shape and separation structure of each feature for pos vs neg. This is exactly the information needed to determine which negatives are "informative" (near the boundary) vs "trivial" (far from any positive). A principled subsampling strategy should use the separation profile, not a magic number.

### Visualization (canvas `c4`, 720×300)

Stacked bar chart of 10 buckets with per-bucket subsampling actions.

- **Title (bold 16px, `#1a5276`, top center):** "Profile-Driven: Subsample by Bucket Purity".
- **Ten bucket bars** (B1–B10), starting at x=80, bar width = (w−140)/10, max height 140px, baseline y=200. Each bar splits into a positive top portion (`rgba(231,76,60,0.7)`, height = posPct × 140) and a negative portion whose fill alpha encodes the action: aggressive `rgba(41,128,185,0.15)`, moderate `rgba(41,128,185,0.3)`, keep `rgba(41,128,185,0.5)`; `#999` 0.5px outline around the full bar.
- **Bucket data (label, posPct, action):** B1 0.0 aggressive; B2 0.01 aggressive; B3 0.05 moderate; B4 0.15 keep; B5 0.35 keep; B6 0.60 keep all; B7 0.20 keep; B8 0.03 moderate; B9 0.01 aggressive; B10 0.0 aggressive.
- **Labels below each bar:** bucket name (13px `#333`), then the action tag (12px) colored red `#e74c3c` for aggressive, orange `#e67e22` for moderate, green `#27ae60` for keep/keep all; action text rendered as "1:50" (aggressive), "1:5" (moderate), "keep", "keep all".
- **Caption (15px `#333`, bottom center):** "Pure-neg buckets: subsample 1:50. Mixed buckets: keep all negatives. Ratio = f(purity)."

## 6. Open Questions

- Does the optimal ratio depend more on the model or the data?
- Can we predict the optimal ratio from the feature profile without training?
- Should the ratio be per-feature (in a bagging ensemble) or global?
- How does subsampling interact with feature profiling — do we profile before or after subsampling?
- Is "ratio" even the right framing? Maybe it should be "keep all negatives within distance d of any positive" — where d is the bandwidth.

## Regeneration instructions

- **Layout:** backlog detail page. h1 with bottom border `2px solid #2980b9`, `.subtitle` paragraph, `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem). One `.lang-section` per numbered section, each with an h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`). Sections 1, 2, 3, 5 use `table.layout` (full width) with one `<tr>`: left `td.text-col` (45%) text, right `td.viz-col` (55%) canvas. Sections 4 and 6 are full-width bullet lists with no canvas.
- **Callout styles:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` — italic, `#555`, 0.9rem.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem with 20px left margin. Canvases styled `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes logical size from the attributes, and calls `ctx.scale` so drawing stays in logical coordinates. Chart fonts use `-apple-system` stack.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; secondary `#2980b9`/`rgba(41,128,185,…)` blue fills, `#f39c12` yellow-orange, `#8e44ad` purple, `#c0392b` dark red; gray text `#333`/`#555`/`#999`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
