# Pitfall: Imputation Leakage

**Page type:** detail page (sectioned card layout: per section an h2, then a two-column table — text left ~45% with tag pills/bullets/example/key-point, canvas right ~55%)
**HTML title tag:** Imputation Leakage

**Subtitle:** Filling missing values using information from the full dataset

## The Problem

Tags: `the trap` (red), `imputation` (blue)

- **Full-dataset fit** — fill values computed on all rows let test data flow into training features
- **Global statistics** — a column mean or median over train plus test rows injects test values
- **Neighbor-based fills** — KNN fit on all data can pick test rows as neighbors for training gaps
- **Informative missingness** — whether a value is missing can itself correlate with the target
- **Learned leak** — fitting that missingness pattern on all rows leaks test-set structure too

*Example:* With 30% of income values missing, the full-dataset median comes out at $52k while the train-only median is $48k.

**Impact:** Every imputed training cell carries test-set information, so validation scores overstate real-world performance.

### Visualization (canvas `c1`, 720×300)

Income column with missing cells forking into a wrong (all-data median) and a right (train-only median) path.

- **Title (bold 14px, `#1a5276`, centered):** "Imputation Leakage: Where Does the Median Come From?"
- **Income column (x=100, cells 70×26, from y=42):** header "Income Column" (blue 11px). Six cells, values top to bottom: $46k, $50k, ?, $54k, ?, $58k with row labels train, train, train, test, test, test. Known train cells light green `#eafaf1`, known test cells light blue `#ebf5fb`, missing cells light orange `#fdebd0` with bold red "?" text; known values in blue 12px; row labels green `#27ae60` for train, orange `#e67e22` for test. Green bracket with rotated "TRAIN" label spans the first three rows; orange bracket with rotated "TEST" label spans the last three.
- **WRONG box (200×70 at x=360, y=40, fill `#fdedec`, red 2px border), reached via a dashed red curved connector:** bold red "WRONG: Median from ALL data"; blue 11px "median($46k, $50k, $54k, $58k)"; bold red 13px "Fill ? with $52k". Bold red "✘" beside the box.
- **RIGHT box (200×70 at x=360, y=170, fill `#eafaf1`, green 2px border), reached via a solid green curved connector:** bold green "RIGHT: Median from TRAIN only"; blue "median($46k, $50k)"; bold green "Fill ? with $48k". Bold green "✔" beside the box.
- **Callout box (130×50 at x=580, y=120, fill `#fef9e7`, orange 2px border):** bold orange "Leaked difference:" over bold 14px "$52k vs $48k".
- **Bottom annotation (blue 11px, centered, y=280):** "Test data ($54k, $58k) inflates the median → model sees artificially high imputed values during training"

## Why It Happens

Tags: `root cause` (orange), `preprocessing` (blue)

- **Feels harmless** — imputation looks like routine cleanup, so it runs before the split
- **Fitted estimator** — an imputer learns its parameters from data, exactly like a model
- **Global fit** — SimpleImputer.fit on the full dataset computes fill values from every row
- **KNN neighbors** — a full-data KNN imputer can copy test values verbatim into training cells
- **Model-based imputers** — MICE fits regressions on all rows, so test rows shape every fill
- **Strategy leakage** — choosing the strategy from full-data missingness patterns also leaks

*Example:* A default-risk model trained on income imputed at $52k instead of the train-only $48k learns a boundary shifted $4k toward the test distribution.

**Root Cause:** Imputation is a form of model fitting — when the observed data includes test rows, the imputed values carry test information into training features.

### Visualization (canvas `c2`, 720×300)

Full-dataset diagram feeding both train and test rows into a contaminated median computation.

- **Title (bold 14px, `#1a5276`, centered):** "How Imputation Parameters Leak Test Info".
- **Full Dataset box (160×200 at x=40, y=50, fill `#f8f9fa`, blue 2px border), titled "Full Dataset":**
  - TRAIN section (upper, fill `rgba(39,174,96,0.15)`, green border) labeled "TRAIN", containing bold red "? ? ?" rows interleaved with green values "$45k $48k $50k".
  - TEST section (lower, fill `rgba(230,126,34,0.15)`, orange border) labeled "TEST", containing bold red "? ?" and orange values "$54k $58k $60k".
- **Compute Median box (160×50 at x=320, y=110, fill `#fdedec`, red 2px border):** bold blue "Compute Median" over red 11px "Uses ALL rows!". A green arrow from the TRAIN section and an orange arrow from the TEST section both point into it.
- **CONTAMINATED box (170×60 at x=530, y=90, fill `#fef9e7`, red 2px border), reached by a red arrow:** bold red "CONTAMINATED"; blue 12px "Median = f(train + test)"; bold red 12px "$52k (not $48k!)". Bold red "⚠" beside it.
- **Bottom annotation (blue 11px, centered):** "Test rows ($54k, $58k, $60k) inflate the median — every imputed train value is biased upward"

## The Correct Approach

Tags: `the fix` (green), `pipeline` (blue)

- **Fit on train only** — the imputer learns its fill values from the training split alone
- **Transform both** — the same train-derived statistics then fill train and test data
- **Use a Pipeline** — sklearn refits the imputer on each training fold inside cross-validation
- **Missingness indicator** — a binary is_missing column keeps the signal without leaked fills
- **Train-only neighbors** — restrict KNN imputation candidates to training rows only
- **Leak check** — compare global-fit vs train-only fill values to size the avoided leak

*Example:* After the fix both splits are filled with the train-only median of $48k, and the is_missing_income flag improves recall by 2%.

**Fix:** Call imputer.fit(X_train) once and transform both splits, adding a missingness-indicator column to keep the signal of why the value is absent.

### Visualization (canvas `c3`, 720×300)

Two-track pipeline (imputation + missingness encoding) inside a dashed Pipeline wrapper, converging on a result box.

- **Title (bold 14px, `#1a5276`, centered):** "Correct: Train-Only Imputation + Missingness Features".
- **Wrapper:** dashed blue (`#1a5276`, dash 6/3, 2px) rectangle around the whole content, labeled "sklearn.pipeline.Pipeline" (bold blue 11px, top-left).
- **Track 1 (bold green 12px label "Track 1: Imputation", y=70):** three green step boxes (140×40, fill `#eafaf1`, green 1.5px border, blue 11px labels with 9px sub-captions), connected by green arrows: "Split Data" (sub: "train / test"), "Fit on Train" (sub: "imputer.fit(X_train)"), "Transform Both" (sub: "train stats → both sets"). Bold green "✔" at row end.
- **Track 2 (bold green label "Track 2: Missingness Encoding", y=150):** three blue step boxes (fill `#ebf5fb`, blue border), connected by blue arrows: "Identify NaN" (sub: "detect missing cells"), "Add is_missing=1/0" (sub: "binary indicator column"), "Predictive Signal" (sub: "missingness = information"). Bold green "✔" at row end.
- **Result box (320×40 at x=200, y=230, fill `#eafaf1`, green 2px border), fed by thin green lines from both tracks:** bold green 12px "No leakage + missingness signal preserved" over blue 10px "+2% recall from is_missing_income feature".

## Regeneration instructions

- **Template/layout:** ml-pipeline-pitfalls detail page. h1 + `.subtitle`, then three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"). Each section: `h2` with 2px `#2980b9` bottom border, then a `table.layout` (border-collapse, full width) with one row — `td.text-col` (45%) and `td.viz-col` (55%), both top-aligned, 12px padding.
- **Text column structure:** `.tags` div of pill spans, then `ul` of bullets with `<b>` lead-ins (bold `#1a5276`), then italic `.example` paragraph, then `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 8px 12px padding, 0.9rem) whose `<strong>` label is Impact/Root Cause/Fix.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius; ul 0.92rem. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, bar fill `rgba(26,82,118,0.35)`; light tints `#eafaf1` (green), `#ebf5fb` (blue), `#fdedec` (red), `#fdebd0`/`#fef9e7` (orange).
- **Canvas:** each canvas declares intrinsic width=720 height=300 and is drawn via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions.
