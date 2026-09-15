# Pitfall: Temporal Leakage (Shuffling Time Series)

**Page type:** detail page (sectioned card layout: per section an h2, then a two-column table — text left ~45% with tag pills/bullets/example/key-point, canvas right ~55%)
**HTML title tag:** Temporal Leakage (Shuffling Time Series)

**Subtitle:** Random train/test split on time-ordered data → model sees the future

## The Problem

Tags: `the trap` (red), `time series` (blue)

- **Shuffled time** — a random split on time-ordered data scatters future rows into training
- **Predicting the past** — the model is scored on a past it has effectively already seen
- **Future in training** — 2024 stock rows land in train while 2023 rows land in test
- **Cross-day leakage** — training on Tuesday to predict Monday imports not-yet-known outcomes
- **The symptom** — accuracy looks strong on a random split but drops under a temporal split
- **Production collapse** — the inflated score evaporates once the model faces a real future

*Example:* A fraud detection model scoring 95% on a random split drops to 72% under a temporal split — the 23-point gap was temporal leakage.

**Impact:** Offline metrics overstate real performance, so models are deployed on accuracy they can never reproduce in production.

### Visualization (canvas `c1`, 720×300)

Two-row timeline diagram contrasting a random shuffled split with a temporal split.

- **Title (bold 14px, `#1a5276`, top center):** "Random Shuffled Split vs Temporal Split".
- **Top row — WRONG:** left-aligned bold red (`#e74c3c`) label "WRONG: Random Shuffle Split" at y=55. Ten dots (radius 8, alpha 0.7) on a horizontal line at y=80, x = 100…550 step 50, each with a small year label (`#333`, 8px) below. Sequence (x, year, color): (100, 2023, red test), (150, 2024, green train), (200, 2023, green train), (250, 2024, red test), (300, 2023, green train), (350, 2024, green train), (400, 2023, red test), (450, 2024, green train), (500, 2024, red test), (550, 2023, green train) — train dots `#27ae60`, test dots `#e74c3c`.
- **Wrong annotations (red, 10px, centered):** "2024 data in TRAIN, 2023 data in TEST" and "Model learns from the future! Accuracy: 95% (inflated)".
- **Bottom row — RIGHT:** bold green label "RIGHT: Temporal Split" at y=155. Gray (`#666`) timeline from x=60 to x=660 at y=180. Green translucent region (alpha 0.2) from x=60 width 280 holding 6 green dots at x = 100…350 all labeled "2023", with bold green labels "TRAIN" / "(past)". Dashed blue (`#1a5276`, dash 5/3, width 3) vertical divider at x=380 labeled "t₁" above. Blue translucent region from x=380 width 280 holding 5 blue (`#2980b9`) dots at x = 420, 470, 520, 570, 620 all labeled "2024", with bold blue labels "TEST" / "(future)".
- **Right annotations (green, 10px, centered):** "Train on past, test on future. No leakage." and "Honest accuracy: 72% (realistic)".
- **Bottom message (bold red 11px, centered, y=275):** "23-point accuracy gap reveals temporal leakage. Always split time-ordered data chronologically."

## Why It Happens

Tags: `root cause` (orange), `shuffling` (blue)

- **Silent i.i.d. assumption** — standard tools assume independent rows; time series violates that
- **No warning** — nothing in the pipeline flags that the rows carry a time dimension
- **Library defaults** — KFold and train_test_split shuffle rows, destroying chronological order
- **Learned habits** — tutorials teach random splits, and teams copy them into forecasting work
- **Auto-correlation** — nearby time points are similar, so test rows have near-twins in train

*Example:* On daily stock returns, random 5-fold CV scores 68% while training on 2020-2023 and testing on 2024 scores 52%.

**Root Cause:** Adjacent time points are correlated, so shuffling gives the model access to future patterns it cannot have in production.

### Visualization (canvas `c2`, 720×300)

Before/after shuffle diagram showing chronological order destroyed and future points landing in train.

- **Title (bold 14px, `#1a5276`, centered):** "Random Shuffle Breaks Temporal Order".
- **Original order (top):** bold label "Original order:" (`#333`, left at x=30). Gray (`#666`) timeline at y=75 from x=70 to x=630 with arrowhead and "time →" label. Ten blue (`#1a5276`, alpha 0.7) circles radius 12 at x = 80 + (i-1)·55, numbered 1-10 in white bold 10px.
- **Middle:** bold red (`#e74c3c`) centered text "↓  Random Shuffle  ↓" at y=110.
- **After shuffle (y=145):** bold label "After shuffle:". Ten circles in shuffled order 7, 2, 10, 4, 8, 1, 9, 3, 5, 6 — first 7 assigned train (green `#27ae60`), last 3 test (red `#e74c3c`), on light green/light red region backgrounds (alpha 0.1). Group labels: "TRAIN (70%)" in green, "TEST (30%)" in red.
- **Highlights:** dashed red circles (radius 17, dash 4/3, width 3) around point 8 (5th slot, in train) and point 3 (8th slot, in test). Bold red annotations above them: "Point 8 (future)" / "in TRAIN!" and "Point 3 (past)" / "in TEST!".
- **Bottom text (centered):** bold red 11px "Future data (points 7,8,9,10) trains the model to \"predict\" past points (3,5,6)" at y=225; plain `#333` 11px "Adjacent time points are auto-correlated. Model exploits future→past correlation." at y=248; bold orange (`#e67e22`) 11px "Random CV accuracy: 68%  |  Temporal split accuracy: 52%  |  Gap = leakage" at y=278.

## The Correct Approach

Tags: `the fix` (green), `walk-forward` (blue)

- **Mirror deployment** — always train on data that existed before the period being predicted
- **Temporal split** — train on the past interval and test on the future, never mixing rows
- **Walk-forward validation** — TimeSeriesSplit folds test on the period right after training
- **Gap period** — a buffer between train and test stops correlated adjacent days from leaking
- **Leakage check** — a much higher random-split score than temporal-split score signals leakage

*Example:* Walk-forward validation with a 6-month training window and 1-month test window averages an honest 54%.

**Fix:** Replace KFold with TimeSeriesSplit so each fold trains on months 1 to N and tests on month N+1, always predicting the future from the past.

### Visualization (canvas `c3`, 720×300)

Walk-forward (TimeSeriesSplit) fold diagram with expanding training windows.

- **Title (bold 14px, `#1a5276`, centered):** "Correct: Walk-Forward (TimeSeriesSplit)".
- **Time axis:** gray (`#666`) horizontal line at y=45 from x=60 to x=660 with arrowhead and "time →" label; month labels Jan…Dec (`#333`, 9px) at x = 60 + i·50, y=58.
- **Folds:** 5 horizontal fold rows (height 30, gap 10) starting at y=72, each labeled "Fold 1"…"Fold 5" in bold blue at the left. Each fold shows a green train bar (`#27ae60` fill alpha 0.4, 2px stroke) spanning months 0..(5+fold-1) and a blue test bar (`#1a5276` fill alpha 0.4, 2px stroke) covering the single next month: Fold 1 trains Jan-Jun tests Jul; Fold 2 trains Jan-Jul tests Aug; Fold 3 trains Jan-Aug tests Sep; Fold 4 trains Jan-Sep tests Oct; Fold 5 trains Jan-Oct tests Nov. Fold 1 bars carry white internal labels "TRAIN (past)" and "TEST".
- **Legend (y=280):** green swatch "Train (past)", blue swatch "Test (future)" (`#333` 10px text).
- **Expanding window annotation:** dashed orange (`#e67e22`, dash 4/3, width 2) diagonal line along the growing train edges, with bold orange 9px labels "expanding" / "window" to its right.
- **Bottom annotation (bold green 11px, centered, y=260):** "Each fold only predicts the future from the past. Honest performance estimate: 54%"

## Regeneration instructions

- **Template/layout:** ml-pipeline-pitfalls detail page. h1 + `.subtitle`, then three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"). Each section: `h2` with 2px `#2980b9` bottom border, then a `table.layout` (border-collapse, full width) with one row — `td.text-col` (45%) and `td.viz-col` (55%), both top-aligned, 12px padding.
- **Text column structure:** `.tags` div of pill spans, then `ul` of bullets with `<b>` lead-ins (bold `#1a5276`), then italic `.example` paragraph, then `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 8px 12px padding, 0.9rem) whose `<strong>` label is Impact/Root Cause/Fix.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius; ul 0.92rem. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** each canvas declares intrinsic width=720 height=300 and is drawn via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions.
