# Pitfall: Threshold Selection on Test Set

**Page type:** detail page (three `.card-section` blocks, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Threshold Selection on Test Set

**Subtitle:** Choosing classification threshold that maximizes test set performance leads to overly optimistic evaluation

## The Problem

Tags: `the trap` (red), `threshold` (blue)

- **The habit** — sweep many thresholds on the test set and keep whichever scores best
- **Hidden training** — picking the best threshold fits that parameter to the test data
- **Broken holdout** — once the test set guides a choice, its metrics stop being unbiased
- **Noise exploitation** — the winning threshold exploits quirks of that one test sample
- **Optimistic reporting** — production performance lands below the reported test metrics

*Example:* A fraud model reports 92% F1 on test after trying 50 thresholds, but production F1 is 87%.

**Impact:** Test metrics become optimistically biased, widening the gap between reported and production performance.

### Visualization (canvas `c1`, 720×300)

Two-curve line chart of F1 vs threshold: a smooth true curve and a noisy test-set curve whose spurious peak gets chosen.

- **Title (bold 14px `#1a5276`, top center):** "F1 Score vs Threshold — Test Set Appears Better Than It Is".
- **Axes:** plot area left=60, right=680, top=60, bottom=260; gray `#999` L-shaped axes; x-label "Threshold →" centered below, rotated y-label "F1 Score" (11px `#666`).
- **True curve (green `#27ae60`, width 2.5, dashed 6/4):** 101 points; threshold = 0.1 + t·0.8, f1 = 0.3 + 0.5·exp(−(threshold − 0.45)² / 0.04) — a smooth Gaussian bump peaking near threshold 0.45. Legend text (11px, top right): "True performance (unknown)".
- **Test curve (red `#e74c3c`, width 2.5, solid):** same base f1 plus noise = 0.05·sin(20t), plus an extra +0.08 bump for t in (0.65, 0.75) — the spurious peak. Legend text: "Test set (tuned on it)".
- **Chosen point:** filled red 6px-radius dot at t=0.7 (x = left + 0.7·width), y at f1=0.75, with bold red two-line annotation above: "← Chosen threshold" / "(optimistic)".

## Why It Happens

Tags: `root cause` (orange), `threshold` (blue)

- **Feels harmless** — a threshold looks like post-processing, not a learned parameter
- **No caution trigger** — tuning it skips the discipline applied to model hyperparameters
- **Bad examples** — many tutorials grid-search thresholds directly on the test set
- **Missing validation set** — plain train/test splits leave no data for threshold tuning
- **Small data pressure** — holding out a validation set feels wasteful, so teams skip it

*Example:* A team with 10,000 samples splits 8,000/2,000 and tunes the threshold on the 2,000-row test set.

**Root Cause:** Any tuning based on test set performance invalidates the test set as an unbiased estimator — the threshold is a hyperparameter like any other.

### Visualization (canvas `c2`, 720×300)

Two horizontal 4-box flow diagrams comparing the wrong and correct workflows.

- **Title (bold 14px `#1a5276`, top center):** "Workflow Comparison".
- **Wrong row (y≈90):** red 12px heading "WRONG: Tune on Test"; four white 100×50 boxes stroked `#e74c3c` width 2 at x = 80/240/400/560, connected by red arrows; labels (11px `#2c3e50`, two lines each): "Train Model", "Try 50 Thresholds", "Pick Best on Test", "Report (Biased)".
- **Correct row (y≈210):** green 12px heading "CORRECT: Use Validation Set"; same four-box structure stroked `#27ae60` with green connecting arrows; labels: "Train Model", "Tune on Validation", "Commit Threshold", "Test Once (Unbiased)".

## The Correct Approach

Tags: `the fix` (green), `validation split` (blue)

- **Tune before test** — pick the threshold on data that plays no part in final evaluation
- **Train** — fit the model on the training set only
- **Tune** — sweep thresholds on the validation set or inner CV folds; commit to the winner
- **Evaluate once** — score the committed threshold on the test set exactly once
- **Report honestly** — present those test metrics as the unbiased production estimate

*Example:* With 10k samples, a 70/15/15 split tunes the threshold on 1,500 validation rows and evaluates once on 1,500 test rows.

**Fix:** Treat the threshold as a hyperparameter inside cross-validation; if you tuned it on the test set, report your numbers as upper bounds.

### Visualization (canvas `c3`, 720×300)

Stacked horizontal split bars contrasting a 2-way split with the correct 3-way split.

- **Title (bold 14px `#1a5276`, top center):** "Proper Data Split — Threshold Tuning Needs Its Own Set".
- **2-way bar (label 12px `#666`: "2-way split (WRONG for threshold tuning):"):** total width 600 starting at x=60, y=80, height 60. Segments: TRAIN 80% — fill `rgba(26,82,118,0.7)`, stroke `#1a5276`, bold white centered label "TRAIN (80%)"; TEST 20% — fill `rgba(231,76,60,0.7)`, stroke `#e74c3c`, white label "TEST (20%)". Below the test segment, red 11px caption: "Tuned on this = biased".
- **3-way bar (130px lower; label: "3-way split (CORRECT):"):** segments TRAIN 70% — fill `rgba(26,82,118,0.7)`, stroke `#1a5276`, label "TRAIN (70%)"; VAL 15% — fill `rgba(230,126,34,0.7)`, stroke `#e67e22`, label "VAL (15%)"; TEST 15% — fill `rgba(39,174,96,0.7)`, stroke `#27ae60`, label "TEST (15%)". Captions below (11px): orange "Tune threshold here" under VAL, green "Evaluate once here" under TEST.

## Regeneration instructions

- **Layout:** three `.card-section` divs, each with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, td padding 12px, vertical-align top): left `td.text-col` 45% holds `.tags` pills + `<ul>` bullets + `.example` + `.key-point`; right `td.viz-col` 55% holds one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Text blocks:** `<ul>` 0.92rem with `<b>` lead words in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem, with a `<strong>` lead ("Impact:", "Root Cause:", "Fix:").
- **Canvas:** each 720×300 intrinsic, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled via a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#444`/`#666`.
