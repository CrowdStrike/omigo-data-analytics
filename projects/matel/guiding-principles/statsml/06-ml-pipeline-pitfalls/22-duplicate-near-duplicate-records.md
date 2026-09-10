# Pitfall: Duplicate / Near-Duplicate Records

**Page type:** detail page (three `.card-section` blocks, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Duplicate / Near-Duplicate Records

**Subtitle:** Same or nearly identical record appearing in both train and test sets causes memorization leakage and inflated performance

## The Problem

Tags: `the trap` (red), `duplicates` (blue)

- **Leaked copies** — training rows that reappear in the test set turn evaluation into recall
- **Memorization** — the model recalls the training copy and scores those test rows near perfectly
- **Inflated metrics** — accuracy blends memorized rows with weaker novel rows, overstating skill
- **Near-duplicates too** — rows differing only in a timestamp leak just as much and hide better
- **False generalization** — the test set no longer measures performance on unseen data

*Example:* A text classifier scores 99% on the 15% duplicated test rows but 76% on novel ones, so the reported 80% masks 76% production performance.

**Impact:** Test performance vastly overstates production performance because the model memorized duplicates rather than learning patterns.

### Visualization (canvas `c1`, 720×300)

Two record-list panels (train and test) with dashed lines connecting duplicated records across the split.

- **Title (bold 14px `#1a5276`, top center):** "Duplicate Records Across Train/Test Split".
- **Panels:** two 200×220 rectangles stroked `#2980b9` width 2 — TRAIN SET at x=100, TEST SET at x=450 (blue 12px headings above each).
- **Train records (each a 160×20 row, fill at 0.3 alpha of its color with solid stroke, 11px centered label):** Record A (green `#27ae60`), Record B (red `#e74c3c`), Record C (green), Record D (red), Record E (green), Record F (red) — at y = 80, 110, 140, 170, 200, 230.
- **Test records (same rows):** Record G (green), Record B (red, duplicate — bold label, stroke width 3), Record H (green), Record D (red, duplicate), Record I (green), Record F (red, duplicate).
- **Connections:** dashed red lines (`#e74c3c`, dash 4/4, width 2) from the train panel edge to the test panel edge linking B→B, D→D, F→F.
- **Bottom annotation (bold 12px `#e74c3c`, centered):** "DUPLICATES = MEMORIZATION".

## Why It Happens

Tags: `root cause` (orange), `pipelines` (blue)

- **Gradual buildup** — no single mistake; ordinary pipeline behaviors add copies over time
- **ETL bugs** — join fan-out and retried batch loads quietly create duplicate rows
- **Collection retries** — clients retrying a successful request log the same event twice
- **Split before dedup** — random splitting sends each copy independently to train or test
- **Invisible near-dupes** — copies differing in ID or timestamp pass exact-match checks

*Example:* A fraud model reports 90% test accuracy but 87% in production; retry logic left exact training matches for 20% of test rows.

**Root Cause:** Random row-wise splitting assumes independent records, so duplicated rows put the same information on both sides of the boundary.

### Visualization (canvas `c2`, 720×300)

Three-bar accuracy breakdown: duplicated test rows vs novel rows vs the blended average.

- **Title (bold 14px `#1a5276`, top center):** "Test Accuracy Breakdown — Duplicates vs Novel Records".
- **Bars (120px wide, baseline y=240, max height 160, fill at 0.35 alpha with solid stroke width 2, bold 16px value label above in the bar color, two-line 11px `#444` label below):**
  - "Duplicates (15% of test)", x=160: 99%, red `#e74c3c`
  - "Novel Records (85% of test)", x=360: 76%, green `#27ae60`
  - "Weighted Average", x=560: 80%, blue `#2980b9`
- **Baseline:** thin gray `#999` line from x=60 to x=680 at y=240.
- **Caption (12px `#e74c3c`, bottom center):** "Model memorized duplicates but struggles with novel data".

## The Correct Approach

Tags: `the fix` (green), `dedup first` (blue)

- **Global dedup first** — deduplicate by content, not just IDs, before any splitting
- **Entity-level splits** — assign each user or document wholly to train or test
- **Temporal guardbands** — split time series by time and leave a gap between windows
- **Detection** — hash rows across the split; use MinHash for text, pHash for images
- **Pipeline monitoring** — flag any unique ID that appears in more than one split
- **Suspicion check** — treat too-good test scores as a cue to inspect for memorization

*Example:* An e-commerce recommender's test accuracy fell from 91% to 78% after switching from a random split to a user-level split.

**Fix:** Always deduplicate by content before splitting, and add automated checks that fail the pipeline when cross-set duplicates appear.

### Visualization (canvas `c3`, 720×300)

Flow diagram of the correct workflow: deduplicate globally before the train/test split.

- **Title (bold 14px `#1a5276`, top center):** "Correct Workflow — Deduplicate Before Splitting".
- **Boxes (white fill, colored stroke width 3, 12px two-line centered labels in `#2c3e50`):**
  - "Raw Data (with dupes)" — 120×50 at (100, 80), stroke `#e74c3c`
  - "Deduplicate Globally" — 140×50 at (280, 80), stroke `#e67e22`
  - "Clean Data (unique)" — 120×50 at (480, 80), stroke `#27ae60`
  - "Train/Test Split" — 140×50 at (360, 180), stroke `#2980b9`
- **Arrows:** gray `#666` width 2 connecting Raw Data → Deduplicate → Clean Data, then an elbow path from Clean Data down/left into Train/Test Split.
- **Final split boxes (y=250, 100×40, bold 12px labels):** "TRAIN" — fill `rgba(26,82,118,0.3)`, stroke `#1a5276`, at x=260; "TEST" — fill `rgba(39,174,96,0.3)`, stroke `#27ae60`, at x=380; blue `#2980b9` connector lines from the split box to both.

## Regeneration instructions

- **Layout:** three `.card-section` divs, each with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, td padding 12px, vertical-align top): left `td.text-col` 45% holds `.tags` pills + `<ul>` bullets + `.example` + `.key-point`; right `td.viz-col` 55% holds one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Text blocks:** `<ul>` 0.92rem with `<b>` lead words in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem, with a `<strong>` lead ("Impact:", "Root Cause:", "Fix:").
- **Canvas:** each 720×300 intrinsic, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled via a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#444`/`#666`.
