# Pitfall: Feature Selection on Full Dataset

**Page type:** detail page (card-section layout: h2 per section, two-column table with text left 45% / canvas right 55%)
**HTML title tag:** Feature Selection on Full Dataset

**Subtitle:** Selecting features using test data → optimistic performance

## The Problem

**Tags:** `the trap` (red), `selection` (blue)

- **The setup** — feature importance is computed on all rows before the train/test split
- **Hidden leak** — the test rows helped decide which features survived the cut
- **Broken independence** — the test set no longer gives an independent check on the model
- **Lucky features** — the "top" list includes features only coincidentally good on this fold
- **False rigor** — the split looks proper, but the damage happened before it

*Example:* Selecting the top 20 of 500 features on full data scores 88% on the split; selection inside CV scores 79%.

**Impact:** Reported accuracy is optimistically biased — 88% reported vs 79% honest — so the model cannot keep that promise on genuinely unseen data.

### Visualization (canvas `c1`, 720×300)

Side-by-side wrong-vs-correct pipeline diagram comparing feature selection outside vs inside cross-validation.

- **Top label (bold 13px, `#1a5276`, centered):** "Feature Selection Leak: Outside vs Inside Cross-Validation".
- **Left panel (WRONG):** 340×260 box at (15, 30) with 3px red border `#e74c3c`; title inside top in bold red: "WRONG: Feature Selection Outside CV".
  - Inner "Full Dataset" box (1.5px `#1a5276` border) labeled "Full Dataset" top-left.
  - Orange band (fill `#e67e22` at 15% alpha, 1px `#e67e22` border) spanning the dataset width, bold orange label "Feature Selection (ALL data)".
  - Orange arrow down into a dashed (`4,3`) `#1a5276` "CV Loop" box (label "CV Loop" top-left).
  - Inside the CV loop: 5 fold rectangles 40×55, labels below: "Train", "Train", "Train", "Train", "Test"; train folds fill `rgba(26,82,118,0.2)` stroke `#1a5276`, test fold fill `rgba(231,76,60,0.2)` stroke `#e74c3c`.
  - Red 9px annotation centered: "Test fold already seen by selector!".
  - Bottom result (bold 13px red, centered): "Reported: 88% (inflated)".
- **Right panel (CORRECT):** 340×260 box at (370, 30) with 3px green border `#27ae60`; title in bold green: "CORRECT: Feature Selection Inside CV".
  - Inner "Full Dataset" box (1.5px `#1a5276`) labeled "Full Dataset".
  - Dashed (`4,3`) `#1a5276` "CV Loop" box (label "CV Loop") that now contains the folds AND the selection step.
  - 5 fold rectangles 40×45: train folds fill `rgba(26,82,118,0.2)` stroke `#1a5276`, test fold fill `rgba(39,174,96,0.15)` stroke `#27ae60`; labels "Train"×4, "Test".
  - Green band (fill `#27ae60` at 15% alpha, 1px green border), 198×22, bold green label "Feature Selection (train folds only)"; green arrow from the train folds down into it.
  - Blue arrow down; 9px blue label centered: "Evaluate on held-out fold (no leakage)".
  - Green 9px annotation centered: "Test fold never seen by selector".
  - Bottom result (bold 13px green, centered): "Reported: 79% (honest)".

## Why It Happens

**Tags:** `root cause` (orange), `leakage` (blue)

- **Feels like preprocessing** — selection is run before the split as if it were cleaning
- **Uses the target** — any step that consults labels is model fitting, not preprocessing
- **Full-data statistics** — chi-squared or mutual-information scores include test labels
- **Overfitted criterion** — the choice fits this train+test sample, not the population
- **Chance correlations** — with 500+ candidates, some correlate with test labels by luck

*Example:* Of 1000 pure-noise features, ~50 pass p<0.05 on full data and inflate accuracy; inside CV none pass consistently.

**Root Cause:** Feature selection uses the target to decide which features to keep, so letting test labels into that decision leaks test information through the selection criterion.

### Visualization (canvas `c2`, 720×300)

Flow diagram showing test labels leaking into the selection criterion.

- **Title (bold 13px, `#1a5276`, centered):** "Selection Sees Test Labels: Biased Feature Choice".
- **Full dataset box:** 640×100 at (40, 40), 2px `#1a5276` border, bold label top-left: "Full Dataset (with labels visible)".
  - Train portion (70% of width): fill `rgba(26,82,118,0.15)`, label "Train rows"; 12 small label chips alternating "y=1" (every 3rd, green `#27ae60`, chip fill `rgba(39,174,96,0.4)`) and "y=0" (blue `#1a5276`, chip fill `rgba(26,82,118,0.3)`).
  - Test portion (30% of width): fill `rgba(231,76,60,0.15)` with 2.5px red `#e74c3c` border, label in red: "Test rows (LEAKED!)"; 5 bold red label chips alternating "y=1"/"y=0", chip fill `rgba(231,76,60,0.3)`.
- **Orange arrow** (2px `#e67e22`) down from the dataset to a selection box: 280×30 centered, fill `rgba(230,126,34,0.15)`, 2px orange border, bold orange label: "Selection Algorithm (scans ALL labels incl. test)".
- **Red arrow** (2px `#e74c3c`) down to a result box: 360×28 centered, fill `rgba(231,76,60,0.1)`, 2px red border, bold red label: "Selected Features: coincidentally good on test fold (BIASED)".
- **Warning annotation** (bold 10px red, right-aligned, two lines): "Test labels used in" / "selection criterion!".
- **Dashed red line** (1.5px, dash 4/3) from the test portion down to the selection box.

## The Correct Approach

**Tags:** `the fix` (green), `pipeline` (blue)

- **Selection is modeling** — treat the selector as part of the model, not preprocessing
- **Inside CV** — re-run selection on each training fold so held-out rows never influence it
- **Pipeline wrapper** — chain selector and estimator so they travel through CV together
- **Fold variation** — features may differ per fold; that is correct and reveals instability
- **Weak-signal honesty** — wildly varying selections mean weak signal; accept that finding
- **Final model** — select on the full training set only, keep the test set untouched

*Example:* SelectKBest(k=20) inside a 5-fold pipeline averages an honest 79%; selection on full data reports an inflated 88%.

**Fix:** cross_val_score(Pipeline([selector, model]), X, y, cv=5) fits the selector on each train fold only, so no test data ever touches it.

### Visualization (canvas `c3`, 720×300)

Pipeline-inside-CV diagram: three fold lanes, each running the same selector→model→evaluate chain.

- **Title (bold 13px, `#1a5276`, centered):** "Correct: Feature Selection Inside CV Pipeline".
- **Outer wrapper:** dashed (6,3) 2px `#1a5276` rectangle covering most of the canvas, labeled top-left in bold: "cross_val_score(pipeline, X, y, cv=5)".
- **Three fold lanes** labeled "Fold 1", "Fold 2", "Fold 3" (bold 10px `#1a5276`), each lane a left-to-right chain of boxes connected by arrows:
  - "Train Data" / "(4 folds)" box — 130 wide, fill `rgba(26,82,118,0.15)`, 1px `#1a5276` border.
  - Green arrow → "SelectKBest" / "(k=20, train only)" box — 110 wide, fill `rgba(39,174,96,0.15)`, 1.5px `#27ae60` border, bold green label.
  - Blue arrow → "Model" / "LogisticRegression" box — 100 wide, fill `rgba(26,82,118,0.15)`, 1.5px `#1a5276` border.
  - Green arrow → "Evaluate" / "(test fold)" box — 90 wide, fill `rgba(39,174,96,0.1)`, 1.5px `#27ae60` border, followed by a green checkmark "✓".
- **Bottom label (bold 11px green, centered):** "Pipeline([SelectKBest → LogisticRegression])  —  No test data touches selector".

## Regeneration instructions

- **Layout:** `.card-section` per section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (border-collapse, full width) with one `<tr>`: `td.text-col` (45%) holding `.tags` pills + `<ul>` bullets + `.example` italic paragraph + `.key-point` callout; `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `li b` colored `#1a5276`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- In regenerated HTML, any card links use `.html` extensions.
