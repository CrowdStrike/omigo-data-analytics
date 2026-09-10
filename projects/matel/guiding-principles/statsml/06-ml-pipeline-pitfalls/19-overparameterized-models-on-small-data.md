# Pitfall: Overparameterized Models on Small Data

**Page type:** detail page (three `.card-section` blocks, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Overparameterized Models on Small Data

**Subtitle:** More parameters than data points causes memorization.

## The Problem

Tags: `the trap` (red), `memorization` (blue)

- **Capacity mismatch** — thousands of parameters on hundreds of samples enables pure memorization
- **Neural network** — a 50,000-parameter net on 500 samples has 100 parameters per data point
- **Boosting** — 10,000 trees on 2,000 samples give enough capacity to fit every noise quirk
- **Random forest** — 500 unbounded trees on 2,000 rows memorize: perfect train, chance validation
- **Polynomial** — a degree-20 fit (21 coefficients) on 100 samples chases noise between points

*Example:* On 500 patients with 50 features, a 10,000-parameter network scores 99% train but 52% test, while logistic regression scores 82% train and 78% test.

**Impact:** Training accuracy approaches 100% through memorization, while validation and test performance stay near random because memorized patterns do not transfer.

### Visualization (canvas `c1`, 720×300)

Horizontal bar-length comparison of parameter count vs sample count.

- **Title (bold 14px `#1a5276`, top center):** "Overparameterization: 10,000 Parameters vs 500 Samples".
- **Parameters bar:** at x=100, y=80, 600×50px, fill `rgba(231,76,60,0.4)`, stroke `#e74c3c` width 3; inside-left bold label in `#e74c3c`: "Parameters: 10,000".
- **Samples bar:** at x=100, y=150 (20px below the first), 30×50px wide (30 = 500/10000 × 600), fill `rgba(39,174,96,0.4)`, stroke `#27ae60` width 3; label in `#27ae60`: "Samples: 500".
- **Ratio annotation (centered, `#e74c3c`):** bold 14px "Ratio: 0.05 samples per parameter", then 11px "(classic heuristic: ~10 samples per parameter as an illustrative starting point)".
- **Bottom warning (centered, `#e74c3c`):** bold 12px at y=240 "Result: Model has enough capacity to memorize every training sample", then 11px at y=258 "Ratio is ~200× below the classic starting point — data cannot constrain the fit".

## Why It Happens

Tags: `root cause` (orange), `capacity` (blue)

- **Raw capacity** — a model with P parameters can fit up to roughly P training examples exactly
- **Classic heuristic** — ~10 samples per parameter is a common starting point, not a hard law
- **Extreme ratio** — P=10,000 on N=500 gives 0.05 samples per parameter, about 200× below that
- **Interpolation** — too few points force interpolation of noise, not a smooth decision boundary
- **Zero-error fits** — infinitely many configurations hit zero train error; most don't generalize
- **Modern caveat** — dropout, early stopping, and double descent let P >> N generalize anyway

*Example:* A degree-19 polynomial fitted to 20 samples passes through every point with zero training MSE, yet oscillates wildly and predicts terribly on new data.

**Root Cause:** With 10k parameters and only 500 points, infinitely many configurations reach zero training error, and the data is too sparse to make the optimizer pick one that generalizes.

### Visualization (canvas `c2`, 720×300)

Line chart of learning curves showing train accuracy climbing to 99% while validation plateaus near 52%.

- **Title (bold 14px `#1a5276`, top center):** "Learning Curves: Overparameterized Model Memorizes".
- **Axes:** origin at (80, 250), plot 600 wide × 180 high; axis lines `#444` width 2; x-label "Training Epochs" centered below, rotated y-label "Accuracy (%)".
- **Train series (blue `#2980b9`, width 3):** 51 points over 50 epochs, trainAcc = 50 + 49·(1 − e^(−i/8)) — asymptotes to 99%; y mapped as oy − (acc/100)·plotH.
- **Validation series (red `#e74c3c`, width 3):** valAcc = 50 + 4·(1 − e^(−i/5)) − 2·(i/epochs) — peaks ~53%, settles ~52%.
- **Legend (top right, 11px):** blue swatch "Train (→ 99%)"; red swatch "Validation (plateaus ~52%)".
- **Annotation (bold 11px `#e74c3c`, centered above plot):** "MASSIVE GAP = MEMORIZATION".

### Visualization (canvas `c3`, 720×300)

Grouped train/test bar chart across four models of increasing complexity, on n=500 samples.

- **Title (bold 14px `#1a5276`, top center):** "Model Complexity vs Performance (n=500 samples)".
- **Data (baseline y=230, max bar height 140, bars 30px wide, train bar left of center, test bar right):**
  - "Logistic Regression" (50 params), x=140: train 82, test 78
  - "Shallow Tree (depth=4)" (200 params), x=280: train 86, test 80
  - "Random Forest (depth=8)" (2000 params), x=420: train 94, test 68
  - "Deep NN (3 layers)" (10000 params), x=560: train 99, test 52
- **Colors:** train bars fill `rgba(52,152,219,0.5)` stroke `#3498db`; test bars colored by score — green `#27ae60` if >75, orange `#e67e22` if >65, else red `#e74c3c` — at 0.7 alpha fill with solid stroke. Bold value labels above each bar in the bar's color; two-line model name (9px `#444`) and "(N params)" (8px `#666`) below the baseline.
- **Legend (top left, 10px):** blue square "Train", green square "Test".
- **Winner annotation:** dashed green (`#27ae60`, dash 4/4, width 3) rectangle around the Shallow Tree bars with bold green two-line label above: "BEST" / "GENERALIZATION".
- **Takeaway (bold 11px `#27ae60`, bottom center):** "Simple models win on small data".

## Regeneration instructions

- **Layout:** three `.card-section` divs, each with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, td padding 12px, vertical-align top): left `td.text-col` 45% holds `.tags` pills + `<ul>` bullets + `.example` + `.key-point`; right `td.viz-col` 55% holds one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Text blocks:** `<ul>` 0.92rem with `<b>` lead words in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem, with a `<strong>` lead ("Impact:", "Root Cause:", "Fix:").
- **Canvas:** each 720×300 intrinsic, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled via a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#444`/`#666`.
