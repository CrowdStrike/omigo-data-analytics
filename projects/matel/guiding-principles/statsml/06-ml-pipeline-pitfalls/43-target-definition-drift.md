# Pitfall: Target Definition Drift

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Target Definition Drift

**Subtitle:** What "positive" means changes over time, invalidating the label and model objective.

## The Problem

Tags: `the trap` (red), `labels` (blue)

- **Silent redefinition** — business rules quietly change what counts as a positive outcome
- **Old vs new labels** — training data uses the old definition; production judges by the new one
- **Refund carve-outs** — "purchase" now excludes orders refunded within 7 days; old labels don't
- **Expanding fraud class** — new attack vectors join the positive class, absent from old negatives
- **Blurred concept** — one training year spanning three "churn" definitions teaches a mixed target
- **Documentation lag** — code and spec disagree, so no one knows which definition is authoritative

*Example:* "High-value customer" shifts from >$500 to >$650 to >$800 across the year, and the mixed-year model keeps flagging $600 customers as high-value.

**Impact:** Offline metrics look fine against the old definition while business metrics degrade against the new one.

### Visualization (canvas `c1`, 720×300)

Timeline of three target-definition periods with a "blurred boundary" cloud.

- **Title (bold 14px, `#1a5276`, top center):** "Target Definition Changes Over Training Period".
- **Timeline axis:** horizontal `#999` line (width 2) at y=250 from x=80 to x=660, with 10px `#666` period labels and tick marks at each period midpoint.
- **Three period boxes (y 60–240, side by side, fill at 20% alpha of the stroke color, stroke width 2):**
  - "Jan-Apr" (0–33% of width), color `#3498db`, labels "Definition v1" and bold ">$500".
  - "May-Aug" (33–66%), color `#9b59b6`, labels "Definition v2" and bold ">$650".
  - "Sep-Dec" (66–100%), color `#e74c3c`, labels "Definition v3" and bold ">$800".
- **Change markers:** vertical dashed red lines (`#e74c3c`, dash 4/4, width 2) at 33% and 66% of the width, each topped by a small filled red triangle and 10px `#e74c3c` caption "Definition changed".
- **Confusion cloud:** white-filled ellipse (180×45 radii) centered mid-canvas, stroke `#e67e22` width 3, containing centered `#e67e22` text: bold 13px "MODEL LEARNS BLURRED BOUNDARY", then 11px lines "Training data mixes 3 definitions" and "Prediction threshold is incoherent".

## Why It Happens

Tags: `root cause` (orange), `definitions` (blue)

- **Static pipeline** — the business evolves, but the model is only ever fed "more data"
- **Silent evolution** — label criteria change and stakeholders assume the model adapts on its own
- **Uninformed ML team** — product adopts new KPIs and thresholds without telling model owners
- **Buried in ETL** — a SQL WHERE clause quietly changes what counts as "converted", unannounced
- **Competing definitions** — marketing, product, and finance each define "active user" differently

*Example:* Marketing redefines "engaged user" from 3 logins/week to 5 actions/week, and engagement scores diverge 30% from the business metric before anyone notices.

**Root Cause:** The label lives implicitly in code rather than as a versioned contract coupled to the model lifecycle.

### Visualization (canvas `c2`, 720×300)

Diverging-lines chart: business rules evolve upward while the ML model stays flat.

- **Title (bold 14px, `#1a5276`, top center):** "Growing Gap: Business Rules vs. ML Model".
- **Timeline axis:** horizontal `#999` line at mid-height from x=80 to x=640, with 10px `#666` tick labels "Q1" through "Q6" evenly spaced.
- **Business Rules line:** red bezier curve (`#e74c3c`, width 3) rising from midY−30 at the left to midY−125 at the right; bold 12px `#e74c3c` label "Business Rules (evolving)" near its right end.
- **ML Model line:** nearly flat blue line (`#1a5276`, width 3) from midY+30 to midY+35; bold 12px `#1a5276` label "ML Model (static)" below its right end.
- **Gap arrows:** at 25%, 50%, 75%, and 100% of the width, vertical dashed orange double-headed arrows (`#e67e22`, dash 4/3, width 2, filled heads) spanning the two lines; labels on the axis in `#e67e22`: bold 11px "GAP" (25% and 50%), bold 12px "GROWING GAP" (75%), and bold 13px `#e74c3c` "CRITICAL DRIFT" (100%).
- **Bottom annotation (centered, 11px `#666`):** "No one triggers retraining — model predicts a concept that no longer exists".

## The Correct Approach

Tags: `the fix` (green), `versioning` (blue)

- **Prevent, don't recover** — bookkeeping at change time is cheap; fixing drift later is costly
- **Version definitions** — publish target_def_v1, v2, v3 with effective dates and changelogs
- **Retrain from scratch** — never train incrementally on mixed labels; the concept itself changed
- **Backfill labels** — relabel history under the new definition, or drop pre-change data entirely
- **Document every change** — record effective date, rationale, and impact on label distribution
- **Fail loudly** — the pipeline should reject any training set that mixes definition versions

*Example:* The training pipeline reads target_def_v3.yaml with effective_date 2025-06-01 and rejects earlier data unless it is relabeled under v3 rules.

**Fix:** Treat the label definition as a first-class versioned artifact and gate every retraining on it.

### Visualization (canvas `c3`, 720×300)

Three-column version flow: definition → relabel → retrained model per version.

- **Title (bold 14px, `#1a5276`, top center):** "Correct Flow: Version → Relabel → Retrain".
- **Definition boxes (140×50 at y=55; x=60, 270, 480; fill at 15% alpha of stroke color, stroke width 2):** "Definition v1" / "target_def_v1.yaml" in `#3498db`; "Definition v2" / "target_def_v2.yaml" in `#9b59b6`; "Definition v3" / "target_def_v3.yaml" in `#27ae60`.
- **Relabel boxes (110×35 at y=135, one per column):** fill `rgba(230,126,34,0.15)`, stroke `#e67e22` width 1.5, bold 11px `#e67e22` text "Relabel Data".
- **Model boxes (130×45 at y=200, one per column):** fill `rgba(26,82,118,0.15)`, stroke `#1a5276` width 2, bold 12px `#1a5276` "Model v1"/"Model v2"/"Model v3" plus 10px "(retrained from scratch)".
- **Vertical arrows:** gray (`#666`, width 1.5, filled heads) from each definition box to its relabel box and from each relabel box to its model box.
- **Horizontal change arrows:** dashed red arrows (`#e74c3c`, dash 5/3, width 2, filled heads) between consecutive definition boxes, each labeled "definition change" in 9px `#e74c3c`.
- **Bottom annotations (centered):** bold 11px `#27ae60` "Each version is self-contained: definition + relabeled data + fresh model"; 11px `#e74c3c` "Never mix labels from different definition versions in one training set".

## Regeneration instructions

- **Layout:** three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"), each an h2 with `2px solid #2980b9` bottom border, followed by a full-width `table.layout` with one row: left `td.text-col` (45%) holding tag pills, a bullet list, optional `.example` italic line and a `.key-point` callout; right `td.viz-col` (55%) holding one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 0.95rem; ul 0.92rem with `li b` in `#1a5276`; `.metric strong` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, border-radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem, with bold lead-in word. `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (`canvas.width = 720*dpr`, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; version accent colors `#3498db` (v1) and `#9b59b6` (v2); gray axes/arrows `#999`/`#666`. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
