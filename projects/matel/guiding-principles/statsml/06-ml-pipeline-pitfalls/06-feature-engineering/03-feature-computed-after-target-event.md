# Pitfall: Feature Computed After Target Event

**Page type:** detail page (card-section layout: one `.card-section` per h2 with a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** Feature Computed After Target Event

**Subtitle:** When a feature is computed using data that was recorded or updated after the target event, creating severe temporal leakage.

## The Problem

**Tags:** `the trap` (red), `temporal leakage` (blue)

- **The trap** — a feature is built from data logged or updated after the prediction cutoff
- **Missing in production** — the value exists in the historical extract but not at scoring time
- **Causal violation** — Jan 15 churn prediction uses support tickets filed Jan 16-31
- **Backfilled data** — an A/B variant assigned Jan 20 is backfilled to Jan 1 in the logs
- **Derived fields** — "days until next purchase" is computed from future events by definition

*Example:* A readmission model's "total_cost_of_episode" includes costs billed 60 days after discharge, so production sees $0 where training saw thousands.

**Impact:** A near-perfect 98% offline AUC collapses to a random 50% in production, because the future-data feature simply does not exist at prediction time.

### Visualization (canvas `c1`, 720×300)

Timeline diagram with a prediction-time cutoff, a leakage zone, and a feature-computation region after the target event.

- **Title (bold 14px `#1a5276`, top center):** "Feature After Target: Temporal Leakage".
- **Timeline:** gray `#999` 2px horizontal line at y=100 from x=80 to x=640.
- **Prediction time marker:** vertical dashed (5/5) blue `#1a5276` 3px line at 40% of the timeline, from y=60 to y=240; bold 12px blue label "PREDICTION TIME" above it and 10px "(e.g., discharge date)" below the label.
- **Valid segment:** green `#27ae60` 4px line just above the timeline from the left edge to the cutoff, with right-aligned 11px green labels "Valid features" / "(past data only)".
- **Leakage segment:** red `#e74c3c` 4px line from the cutoff to the right edge, with left-aligned 11px red labels "LEAKAGE" / "(future data)".
- **Target event:** orange `#e67e22` filled 8px-radius dot 100px right of the cutoff; bold 11px label "Target Event" and 10px "(e.g., readmitted)" below the timeline.
- **Feature computation region:** rect filled `rgba(231,76,60,0.2)` with 2px `#e74c3c` stroke starting 10px right of the target dot, 60px tall below the timeline; bold 11px red "Feature uses data from here" and 10px lines "(e.g., \"total_cost_of_episode\"" / "includes costs billed 60d later)".
- **Bottom consequence (left-aligned at x=80):** 12px `#444` "Training: Feature = $5000 (includes future costs) → Model learns cost predicts readmission" (y=235); bold 12px `#e74c3c` "Production: Feature = $0 (no future costs available) → Model fails" (y=255); 11px `#27ae60` "Fix: Only use costs billed BEFORE prediction time" (y=280).

## Why It Happens

**Tags:** `root cause` (orange), `flat tables` (blue)

- **Late extracts** — the modeler's table is built long after the events it describes
- **No column timestamps** — nothing records when each field first came into existence
- **No temporal audit** — nobody compares feature generation times against the target event time
- **Outcome by-products** — billing codes and similar fields are created by the outcome itself
- **Flat tables** — a tabular extract collapses ordering, so every column looks simultaneous
- **Backfilled columns** — retroactively computed fields quietly absorb future information

**Root Cause:** Flat tabular formats destroy temporal order — nothing shows that feature_C was generated 5 days after the target event.

### Visualization (canvas `c2`, 720×300)

A flat table that looks fine, paired with a timeline underneath revealing that feature_C was generated after the target.

- **Title (bold 14px `#1a5276`, top center):** "Flat Table Hides Temporal Order".
- **Flat table:** at (60,42), column width 110, row height 28; header cells filled `rgba(26,82,118,0.1)` with bold 11px `#1a5276` labels "user_id", "feature_A", "feature_B", "feature_C", "target" — except the feature_C column, highlighted `rgba(231,76,60,0.15)` with red `#e74c3c` header text. One data row (11px `#444`): "U001", "3.2", "7.1", "9.8" (cell background `rgba(231,76,60,0.08)`), "1". Bold 11px red annotation under the feature_C column: "Looks fine in flat table!".
- **Timeline below:** gray `#999` 2px line at y=170 from x=80 to x=640 with filled 7px-radius event dots and bold 11px labels above / 10px day labels below:
  - "feature_A" Day 1 (x=140, green `#27ae60`)
  - "feature_B" Day 5 (x=260, green `#27ae60`)
  - "TARGET" Day 10 (x=400, orange `#e67e22`)
  - "feature_C" Day 15 (x=540, red `#e74c3c`)
- **Dashed connector:** red 1.5px dashed (4/3) line from the feature_C timeline dot up toward the feature_C table column.
- **Legend (centered):** bold 12px `#e74c3c` "feature_C generated AFTER target — temporal leakage!" (y=220); 11px `#444` "Timeline reveals what the flat table hides" (y=240).

## The Correct Approach

**Tags:** `the fix` (green), `temporal gate` (blue)

- **Timestamp everything** — record when each feature becomes available, not just its value
- **Hard cutoff gate** — a feature enters the model only if it provably precedes prediction time
- **Strict cutoff rule** — exclude any feature whose timestamp is at or after the target time
- **Point-in-time snapshots** — reconstruct data exactly as it existed at the prediction moment
- **Automated assertion** — test that max(feature_generation_time) < prediction_time
- **Detection question** — ask "would I have this value at scoring time?" for every feature

**Fix:** Every feature must pass a temporal gate — only data generated strictly before the prediction cutoff is allowed into the model.

### Visualization (canvas `c3`, 720×300)

Timeline with a prediction cutoff dividing a valid zone (features accepted) from an excluded zone, plus a validation box.

- **Title (bold 14px `#1a5276`, top center):** "Temporal Gate: Only Past Data Feeds Model".
- **Timeline:** gray `#999` 2px line at y=100 from x=60 to x=660.
- **Prediction cutoff:** vertical dashed (6/4) blue `#1a5276` 3px line at 55% of the timeline, from y=45 to y=170; bold 12px blue label "PREDICTION CUTOFF" above.
- **Valid features (left of cutoff):** green `#27ae60` 7px-radius dots at x=140 ("Feature A") and x=280 ("Feature B"), each with bold 11px label above and a 16px "✓" below.
- **Excluded feature (right of cutoff):** red `#e74c3c` dot 120px right of the cutoff ("Feature C") with bold 16px "✗" and 10px "(EXCLUDED)" below.
- **Zone shading:** left zone `rgba(39,174,96,0.1)` band (70px tall around the timeline) labeled 10px green "VALID ZONE"; right zone `rgba(231,76,60,0.08)` labeled 10px red "EXCLUDED ZONE".
- **Validation box:** fill `rgba(39,174,96,0.08)` with 2px `#27ae60` stroke at (100,190) 520×50; bold 12px green "✓  Validation: For each feature f: assert f.timestamp < cutoff"; 11px `#444` "Only past data feeds model".
- **Bottom (centered):** bold 11px `#1a5276` "Enforce in tests: max(feature_generation_time) < prediction_time" (y=270); 11px `#444` "Use point-in-time snapshots to guarantee temporal correctness" (y=288).

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) containing one `<tr>`: left `td.text-col` (45%) with `.tags` pills, a `<ul>` of labeled bullets, optional `.example` italic paragraph, and a `.key-point` callout; right `td.viz-col` (55%) with one canvas.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. Bullets 0.92rem with `<b>` labels in `#1a5276`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 each, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#444`/`#666`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
