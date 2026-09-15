# Pitfall: Information Leakage (Comprehensive)

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Information Leakage (Comprehensive)

**Subtitle:** Umbrella category for any information crossing a boundary it shouldn't—temporal, train-test, or entity.

## The Problem

Tags: `the trap` (red), `leakage` (blue)

- **The leak** — training uses information that will not exist at prediction time
- **Temporal** — a feature reads data from after the prediction time, revealing the answer
- **Train-test** — normalization or imputation fit on the full dataset leaks the test distribution
- **Entity** — user features built from other users in the same batch leak cross-user signals
- **Target encoding** — encoding categories with full-dataset target means embeds the label itself
- **Shortcut learning** — the model memorizes patterns that violate causality and cannot generalize

*Example:* A loan-default feature computed from account status 90 days after approval scores AUC 0.97 in training but 0.53 in production.

**Impact:** Artificially strong offline metrics collapse to near-random in production once the leaked signal disappears.

### Visualization (canvas `c1`, 720×300)

Three-column diagram of leakage boundaries plus an impact summary box.

- **Title (bold 14px `#1a5276`, top center):** "Three Boundaries Where Leakage Occurs".
- **Column 1 — TEMPORAL (bold 12px red header):** two white boxes with `#999` 2px borders side by side; left box labeled "Past data" / "(available)" in green 10px, right box "Future data" / "(leaked)" in red 10px; a solid red 3px vertical barrier between them with rotated bold 9px red label "Prediction time"; caption below in 9px red: "Feature uses" / "future values".
- **Column 2 — TRAIN-TEST (bold 12px red header):** left box filled `rgba(39,174,96,0.3)` with `#27ae60` border labeled "Train set" (green 10px); right box filled `rgba(231,76,60,0.3)` with `#e74c3c` border labeled "Test set" (red 10px); red 2.5px arrow pointing from test toward train; caption: "Preprocessing" / "on full data".
- **Column 3 — ENTITY (bold 12px red header):** two white circles (radius 25): "User A" with green border/text, "User B" with red border/text; red 2.5px arrow from A to B; caption: "Feature uses" / "other users".
- **Bottom summary box (80, 185, width−160 × 80, white with orange `#e67e22` 2px border):** header "IMPACT: Inflated Offline Metrics, Production Collapse" in bold 12px orange; body 10px `#2c3e50`: "Training AUC = 0.97 (model sees leaked information)", "Production AUC = 0.53 (information not available at inference time)", "Model learned shortcuts that violate causality or isolation assumptions".

## Why It Happens

Tags: `root cause` (orange), `boundaries` (blue)

- **No enforced boundary** — offline, all data coexists, so any stage can cross the prediction line
- **Temporal leakage** — features are computed from data that did not exist at prediction time
- **Split leakage** — grouped data split by row puts the same patient in both train and test
- **Deployment leakage** — batch-computed features exist offline but are missing at serve time
- **Label leakage** — fields like "cancelled_reason" encode the target directly into a feature
- **Population leakage** — selection bias makes the training population differ from deployment

*Example:* A hospital readmission model hits AUC 0.98 offline using "discharge_summary_text", a field only populated after the readmission decision.

**Root Cause:** Nothing in an offline pipeline enforces the prediction-time boundary, so every stage can silently cross it — and which boundary was crossed determines the fix.

### Visualization (canvas `c2`, 720×300)

Tree diagram: a central node fanning out to five leakage types with descriptions.

- **Title (bold 14px `#1a5276`, top center):** "Taxonomy of Information Leakage Types".
- **Central node:** rounded rectangle (160×40, radius 8) filled `#1a5276` at center (w/2, 90), white bold 12px label "Information Leakage".
- **Five branch nodes (rounded 100×35, radius 6, white bold 11px labels, at y=180) with 2px connector lines from the central node in the branch color:** Temporal `#e74c3c` at x=80; Split `#e67e22` at x=220; Deployment `#8e44ad` at x=360; Label `#27ae60` at x=500; Population `#2980b9` at x=640.
- **Descriptions (9px `#2c3e50`, two lines under each node):** "Future data used" / "to predict past"; "Same entity in" / "train and test"; "Features absent" / "at serve time"; "Target encoded" / "in features"; "Training != deploy" / "population".
- **Bottom box:** rounded white rectangle (150, 245, width−300 × 40, radius 6) with red 2px border; bold 11px red text: "Common Symptom: Unrealistically good offline metrics".

## The Correct Approach

Tags: `the fix` (green), `availability` (blue)

- **Availability test** — ask of every feature: is it available at prediction time, yes or no
- **Train-only stats** — fit normalization, imputation, and encodings on train, then freeze them
- **Entity splits** — split by patient, user, or session so one entity never spans both sides
- **Time splits** — train on the past, validate on the future; never shuffle a time series
- **Feature parity** — ensure the serving environment computes every feature training relied on
- **Feature registry** — record when each feature becomes available relative to prediction time

*Example:* A registry stores available_at per feature (T-0, T+1h, T+1d), and the training pipeline auto-excludes any feature with available_at past the prediction time.

**Fix:** Document each feature's availability relative to prediction time — any feature filled after the prediction moment is leakage.

### Visualization (canvas `c3`, 720×300)

Timeline diagram: features placed before and after the prediction moment, marked safe or leakage.

- **Title (bold 14px `#1a5276`, top center):** "Feature Availability Timeline".
- **Timeline:** horizontal `#2c3e50` 2px line at y=80 from x=60 to x=w−60 with an arrowhead at the right end.
- **Prediction moment:** `#1a5276` 3px dashed vertical divider (dash 6/3) at x=w/2 spanning y 50–110, labeled "PREDICTION MOMENT" in bold 12px `#1a5276` above.
- **Time labels (10px `#666` below the line):** T-7d, T-1d, T-0 (at the divider), T+1h, T+1d.
- **Available features (left, stacked from y=140 at 35px spacing):** rounded pills (110×24, radius 4) filled `rgba(39,174,96,0.15)` with `#27ae60` 2px border, each with a bold green "✓" and 10px name: `age`, `login_count_7d`, `account_tenure`.
- **Unavailable features (right of divider, stacked same rows):** rounded pills (125×24) filled `rgba(231,76,60,0.15)` with `#e74c3c` 2px border, each with a bold red "✗" and name: `discharge_summary`, `outcome_30d`, `final_diagnosis`.
- **Legend (bottom, 10px `#2c3e50`):** green swatch — "Available at prediction time (safe)"; red swatch — "Not yet available (LEAKAGE)".

## Regeneration instructions

- **Layout:** three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"), each with an h2 underlined by `2px solid #2980b9` and a `table.layout` (one `<tr>`): left `<td class="text-col">` (45%) holds `.tags` pills, a `<ul>` of labeled bullets, a `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (55%) holds one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; `li b` in `#1a5276`; `ul` 0.92rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Callouts:** `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, `1px solid #e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); rounded shapes use `ctx.roundRect`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; accents `#8e44ad`, `#2980b9`; text `#2c3e50`/`#666`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
