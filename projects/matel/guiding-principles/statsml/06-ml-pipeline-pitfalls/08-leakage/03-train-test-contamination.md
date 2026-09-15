# Pitfall: Train/Test Contamination

**Page type:** detail page (card-sections with h2 headings; each section is a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Train/Test Contamination

**Subtitle:** Information from test set leaks into training process

## The Problem

Tags: `the trap` (red pill), `contamination` (blue pill)

- **The boundary** — the test set must simulate unseen data, but information sneaks across it
- **Shared entities** — the same patient appears in train and test, so the model memorizes them
- **Shared statistics** — feature means computed on the full dataset let test rows shape training
- **Premature SMOTE** — oversampling before the split puts synthetic near-copies of train in test
- **Graded on seen data** — the evaluation scores the model on material it already trained on

*Example:* A patient with 5 hospital visits lands 3 in train and 2 in test, and the model learns "patient_id_12345 → positive".

**Impact:** Reported accuracy is inflated — 92% on the contaminated split versus 74% on truly unseen entities.

### Visualization (canvas `c1`, 720×300)

Side-by-side panels: wrong random split vs right entity-level split.

- **Title (bold 14px `#1a5276`, top center):** "Entity-Level Split: Wrong vs Right".
- **Left panel (320×220 white box, red `#e74c3c` 3px border, at x=30, y=50):** bold red heading "WRONG: Random Split".
  - "Patient #12345:" (`#333`) with a green box (fill `#27ae60` at 0.6 alpha, green stroke, 100×25) containing white text "Visit 1 (train)" / "Visit 3 (train)", next to a red box (fill `#e74c3c` at 0.6 alpha, red stroke) containing "Visit 2 (test)" / "Visit 5 (test)".
  - Bold red warning centered below: "Same patient in both!".
  - "Patient #67890:" with a green box "Visit 1 (train)" and a red box "Visit 4 (test)".
  - Red caption at panel bottom: "Model memorizes patients," / "not patterns".
- **Right panel (320×220 white box, green `#27ae60` 3px border, at x=390):** bold green heading "RIGHT: Entity-Level Split".
  - "Patient #12345:" / "(ALL in train)" with one wide green box (250×35, fill 0.6 alpha) containing white text "Visit 1, Visit 2, Visit 3," / "Visit 4, Visit 5 (all train)".
  - "Patient #67890:" / "(ALL in test)" with one wide blue `#2980b9` box (fill 0.6 alpha) containing "Visit 1, Visit 2, Visit 3," / "Visit 4 (all test)".
  - Green caption at panel bottom: "No leakage: each patient" / "appears in only ONE split".
- **Bottom message (bold red, centered):** "Split by entity (patient/user/household), not by row. All records from one entity in same set."

## Why It Happens

Tags: `root cause` (orange pill), `row splits` (blue pill)

- **Tooling defaults** — sklearn's train_test_split splits rows, not entities such as patients
- **Scattered entities** — random row splits spread one entity's records across both sides
- **Entity memorization** — entity-specific patterns transfer to that entity's test rows only
- **SMOTE before split** — synthetic minority samples leak into test as near-copies of train
- **Full-data features** — means and counts over all rows carry test information into training

*Example:* With 1000 patients and 5 visits each, a random 80/20 row split yields 92% accuracy while an entity-level split yields 74%.

**Root Cause:** Row-level splitting lets the model answer a patient's test visits by recalling that patient's train visits — memorization, not learning.

### Visualization (canvas `c2`, 720×300)

Flow diagram from a dataset table through a random split into contaminated train/test boxes, plus a SMOTE note.

- **Title (bold 14px `#1a5276`, top center):** "How Entity Contamination Occurs".
- **Dataset table (160×130 at x=30, y=45, blue `#1a5276` 2px border):** header row tinted `rgba(26,82,118,0.15)` with bold blue column labels "patient_id" and "visit"; five rows separated by `#ccc` rules: Patient A visits 1, 2, 3 in orange `#e67e22`; Patient B visits 1, 2 in blue `#1a5276`.
- **Split arrow:** gray `#666` arrow from the table to the split boxes, with a bold red label "train_test_split()" above.
- **Train box (190×100 at x=270, y=50, green `#27ae60` border, fill at 0.1 alpha):** bold green heading "TRAIN SET"; rows "Patient A - Visit 1", "Patient A - Visit 3" (orange) and "Patient B - Visit 1", "Patient B - Visit 2" (blue).
- **Test box (190×100 at x=490, red `#e74c3c` border, fill at 0.1 alpha):** bold red heading "TEST SET"; row "Patient A - Visit 2" (orange).
- **Contamination link:** dashed red line (dash 5/3, width 2) connecting Patient A's train row to Patient A's test row, with a bold red label "SAME ENTITY!" above the midpoint.
- **Explanation (`#333`, centered):** "Patient A appears in BOTH splits. Model memorizes \"Patient A → outcome\"" / "instead of learning generalizable patterns."
- **SMOTE note:** bold orange label "SMOTE before split:" followed by `#333` text "Full Data → SMOTE (creates synthetic) → Split → Synthetic copies in test set!".
- **Bottom messages (bold red, centered):** "✖ Test points are near-copies of training data — not independent evaluation" and "Result: Inflated accuracy (92%) that collapses on truly unseen entities (74%)".

## The Correct Approach

Tags: `the fix` (green pill), `group split` (blue pill)

- **Match the unit** — split at the level the model must generalize to, e.g. new patients
- **Group-aware splitters** — use GroupKFold or GroupShuffleSplit keyed on the entity ID
- **Entity atomicity** — all of one entity's records go to the same side of the split
- **Oversample after split** — apply SMOTE only after splitting, and only to the train partition
- **Train-only statistics** — fit feature statistics on train entities, then reuse them on test
- **Overlap check** — programmatically verify no entity ID appears in both train and test

*Example:* GroupKFold with groups=patient_id sends 800 patients with all their visits to train and 200 to test, and the resulting 74% accuracy holds in production.

**Fix:** Replace train_test_split(X, y) with GroupShuffleSplit(groups=patient_id) to guarantee zero entity overlap.

### Visualization (canvas `c3`, 720×300)

Two regions (train and test) with whole-entity blocks and a no-crossing divider.

- **Title (bold 14px `#1a5276`, top center):** "Correct: Entity-Level Split (GroupKFold)".
- **Train region (350×195 at x=20, y=42, green `#27ae60` 3px border, fill at 0.08 alpha):** bold green heading "TRAIN (800 patients)"; four entity blocks (35px high, 8px gap, fill at 0.3 alpha with 2px matching stroke): Patient A (orange `#e67e22`), Patient B (blue `#1a5276`), Patient C (green `#27ae60`), Patient D (purple `#8e44ad`), each labeled "Patient X  (all visits: v1, v2, v3, v4, v5)" in `#333`.
- **Test region (310×195 at x=390, blue `#2980b9` 3px border, fill at 0.08 alpha):** bold blue heading "TEST (200 patients)"; two entity blocks: Patient E (red `#e74c3c`), Patient F (blue `#2980b9`), each labeled "Patient X  (all visits: v1, v2, v3, v4)".
- **Divider:** vertical dashed green line (dash 8/4, width 3) at x=377 between the regions, with a bold green "✓" below it.
- **Bottom annotations (centered):** bold green "No entity crosses the boundary. GroupShuffleSplit(groups=patient_id) guarantees zero overlap." and `#333` "Honest accuracy: 74% — reproducible in production on unseen entities".

## Regeneration instructions

- **Layout:** h1 with 2px `#2980b9` bottom border, `.subtitle` paragraph, then three `.card-section` divs. Each section has an `h2` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with one row: `td.text-col` (45%, top-aligned) holding `.tags` pills + `<ul>` bullets + `.example` italic line + `.key-point` callout; `td.viz-col` (55%) holding the canvas.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.blue` `rgba(26,82,118,0.12)`/`#1a5276`, `.green` `rgba(39,174,96,0.15)`/`#27ae60`, `.red` `rgba(231,76,60,0.12)`/`#e74c3c`, `.orange` `rgba(230,126,34,0.15)`/`#e67e22`.
- **Bullets:** `<li><b>Label</b> — sentence</li>`; `li b` colored `#1a5276`; list 0.92rem. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem, with bold lead word.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`. Canvases have `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** all three canvases declare intrinsic 720×300; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray text `#666`/`#333`.
