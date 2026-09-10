# Pitfall: Backfill Bias (Data Filled Retroactively)

**Page type:** detail page (card-section layout: one `.card-section` per h2 with a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** Backfill Bias (Data Filled Retroactively)

**Subtitle:** When historical data is corrected or enriched with information that was not available at the time, creating unrealistic training data.

## The Problem

**Tags:** `the trap` (red), `backfill` (blue)

- **The trap** — a backfill rewrites history with information learned only in the future
- **Skewed training** — the model trains on corrected records production will never see
- **Corrections** — missing 2022 geocodes are filled in 2024, hiding the 30% failure rate
- **Schema changes** — "customer_segment" is backfilled to 2020 using 2024 segmentation logic
- **Enrichments** — vendor data added retroactively differs from real-time coverage and accuracy

*Example:* A 2023 bug-fix restores a credit model's NULL 2020 incomes from a recovered database, but production still sees 20% NULL incomes.

**Impact:** A model trained on clean backfilled data hits 90% AUC, then drops to 75% against production's 20% missing values — a structural training-serving skew.

### Visualization (canvas `c1`, 720×300)

Timeline with a backfill arrow, plus side-by-side training vs production comparison panels.

- **Title (bold 14px `#1a5276`, top center):** "Backfill Bias: Training on Retroactively Cleaned Data".
- **Timeline:** gray `#999` 2px line at y=80 from x=80 to x=640, tick marks with 10px `#444` labels "2020", "2021", "2022", "2023 (backfill)" (3 equal segments).
- **Original data band:** orange `#e67e22` rect (fill at 0.3 alpha, 2px stroke) spanning the full timeline width, 25px tall above the line; bold 11px orange label "Original data: 20% missing values".
- **Backfill arrow:** red `#e74c3c` 2px elbow line from the 2023 end back to the timeline midpoint with a filled red arrowhead pointing down into the band; bold 10px red label "2023: Backfill fixes data".
- **Left panel — TRAINING DATA (backfilled):** stroked `#1a5276` 2px rect at (50,120) 280×110; bold 12px blue title; 11px `#444` "2020-2022 records:"; bold 11px `#27ae60` lines "• Missing values: 2%", "• Data quality: HIGH", "• Model AUC: 90%".
- **Right panel — PRODUCTION DATA (reality):** stroked `#e74c3c` 3px rect at (390,120) 280×110; bold 12px red title; 11px `#444` "New records:"; bold 11px `#e74c3c` lines "• Missing values: 20%", "• Data quality: ORIGINAL", "• Model AUC: 75%".
- **Bottom impact (bold 11px `#e74c3c`, centered, y=265):** "Training-serving skew: Model never sees the data quality it will face in production".

## Why It Happens

**Tags:** `root cause` (orange), `mutable history` (blue)

- **Current-truth databases** — tables store the best present values, not what was known when
- **Retroactive corrections** — revisions update historical records long after they were written
- **Final vs preliminary** — training uses revised values instead of the as-published ones
- **Bug fixes** — repairs inject information that did not exist when the record was created
- **Schema migrations** — new columns are backfilled with current logic, not the historical logic

**Root Cause:** The table looks clean but has been silently improved over time — the current snapshot no longer represents the data as first collected.

### Visualization (canvas `c2`, 720×300)

Before/after table pair showing the same records pre- and post-backfill, with a warning that production still sees the messy version.

- **Title (bold 14px `#1a5276`, top center):** "Same Data, Two Versions: Before and After Backfill".
- **Left table — "Original 2020 Data" (bold 12px `#e67e22` caption):** at x=30, y=48, columns "user_id", "income", "segment" (75px wide, 24px rows), header cells filled `rgba(230,126,34,0.15)` with bold 10px `#1a5276` text. Rows: U01/45000/NULL, U02/NULL/NULL, U03/62000/NULL — NULL cells get `rgba(231,76,60,0.08)` background and bold 10px `#e74c3c` text; other values 10px `#444`.
- **Right table — "After 2023 Backfill" (bold 12px `#27ae60` caption):** at x=410, same columns, header cells filled `rgba(39,174,96,0.12)`. Rows (10px `#27ae60`): U01/45000/Premium, U02/51000/Standard, U03/62000/Premium.
- **Arrow between tables:** red `#e74c3c` 2px horizontal arrow with filled arrowhead, bold 10px label "Retrospective fix" above it.
- **Warning (centered):** bold 12px `#e74c3c` "Production in 2024 still sees the messy version!" (y=210); 11px `#444` "Model trained on clean data; deployed against dirty data" (y=230).
- **Emphasis box:** fill `rgba(231,76,60,0.06)` with 1.5px `#e74c3c` stroke at (100,245) 520×40; 11px red lines "New records arrive with NULLs and missing segments — just like 2020 originally was" and "But model was trained expecting the backfilled clean version".

## The Correct Approach

**Tags:** `the fix` (green), `point-in-time` (blue)

- **Honest reconstruction** — train on data as it looked on the prediction date, warts and all
- **Point-in-time queries** — retrieve records exactly as they stood at the past moment
- **Snapshots** — keep daily partitions or slowly changing dimensions to recover old states
- **As-of API** — expose features through get_features(entity_id, as_of_date)
- **No silent corrections** — train on fixed data only if production gets the same fixes
- **Match production NULLs** — the model must meet the same missing values in training

**Fix:** Training data must reflect the same data quality and completeness that the model will encounter in production.

### Visualization (canvas `c3`, 720×300)

Snapshot timeline with a point-in-time training snapshot at T2, a crossed-out "latest" path, an as-of API box, and a success box.

- **Title (bold 14px `#1a5276`, top center):** "Point-in-Time Access: Train on Data As It Existed".
- **Timeline:** gray `#999` 2px line at y=75 from x=60 to x=660 with blue `#1a5276` 6px-radius snapshot dots labeled bold 11px "T1" (x=160), "T2" (x=340), "T3" (x=520), "NOW" (x=620).
- **Training pointer:** green `#27ae60` 2px arrow pointing up at the T2 dot.
- **Snapshot box:** fill `rgba(39,174,96,0.1)` with 2px `#27ae60` stroke, 240×45 centered under T2; bold 11px green "Snapshot at T2"; 10px `#444` "(includes NULLs, messy data)" and "NOT the latest corrected version".
- **Crossed-out latest:** red `#e74c3c` dashed (4/3) 1.5px line from the snapshot box toward NOW, ending in a bold red 3px "X" mark at the NOW position.
- **API box:** fill `rgba(26,82,118,0.06)` with 1.5px `#1a5276` stroke at (80,185) 560×40; bold 12px blue centered text "get_features(user, as_of=T2)  →  returns data as it existed at T2".
- **Success box:** fill `rgba(39,174,96,0.08)` with 2px `#27ae60` stroke at (130,240) 460×40; bold 12px green "✓  Training sees same data quality as production"; 11px `#444` "No training-serving skew from retrospective corrections".

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) containing one `<tr>`: left `td.text-col` (45%) with `.tags` pills, a `<ul>` of labeled bullets, optional `.example` italic paragraph, and a `.key-point` callout; right `td.viz-col` (55%) with one canvas.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. Bullets 0.92rem with `<b>` labels in `#1a5276`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 each, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#444`/`#666`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
