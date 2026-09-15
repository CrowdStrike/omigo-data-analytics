# Pitfall: Experiment Provenance Ignored (A/B Test Data Mixed Into Training)

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Experiment Provenance Ignored (A/B Test Data Mixed Into Training)

**Subtitle:** Training data pools A/B test variants without tracking provenance, confounding learned patterns.

## The Problem

**Tags:** `the trap` (red), `provenance` (blue)

- **Pooled arms** — control and treatment rows are mixed with no experiment_id or variant tag
- **Confounded labels** — a 20% treatment lift teaches the model "in treatment" predicts conversion
- **Shifted semantics** — one column mixes old-formula control and new-formula treatment scores
- **Lost lineage** — archived logs without metadata leave a later retrain no way to filter by arm
- **Simpson's paradox** — pooled data can show the opposite trend of every within-arm trend
- **Post-launch decay** — predictions degrade once every user experiences a single variant

*Example:* A ranking model trained while half the users saw a 10% discount and converted 25% more sees its false-positive rate hit 18% once the test ends.

**Impact:** The model learns experiment-induced heterogeneity instead of stable production patterns, so predictions degrade the moment all users see one variant.

### Visualization (canvas `c1`, 720×300)

Timeline-plus-flow diagram: overlapping A/B tests on a 6-month timeline feeding a pooled warehouse and a corrupted importance chart.

- **Title (bold 14px `#1a5276`, top center):** "Experiment Provenance: A/B Tests Contaminate Training".
- **Timeline bar:** 600×20 at (60,50), fill `rgba(26,82,118,0.1)`, stroke `#1a5276` width 1.5; month labels 9px `#666` centered under each sixth: Jan, Feb, Mar, Apr, May, Jun.
- **Three test bands** (each drawn as a 35%-alpha band on the timeline plus an outlined 18px-tall label row below, bold 10px text in the band color):
  - "Button Color Test" — months 0–2, color `#27ae60`, label row at y=80.
  - "Checkout Flow Test" — months 1.5–4, color `#e67e22`, label row at y=105.
  - "Pricing Experiment" — months 3–5.5, color `#2980b9`, label row at y=130.
- **Gray arrow** down from the tests to a warehouse box: 320×30 at (200,180), stroke `#1a5276` width 2, bold 12px "Data Warehouse (all experiments pooled)"; second gray arrow down to the importance chart.
- **Model feature importance bars:** header bold 11px `#444` "Model Feature Importance:" at (60,242). Bars at x=240 starting y=232 stepping 16px, width = value × 5, fill at alpha 0.4 with matching stroke, right-aligned 9px monospace labels and bold 9px value labels:
  - `button_color_variant` 14% — `#27ae60`
  - `checkout_flow_v2` 11% — `#e67e22`
  - `discount_applied` 9% — `#2980b9`
  - `user_tenure (stable)` 5% — `#1a5276`
- **Warning bracket:** red `#e74c3c` width 1.5 bracket spanning the first three bars, labeled bold 9px red "experimental" / "manipulations!".
- **Bottom annotation (bold 11px `#e74c3c`, centered):** "Importance reflects A/B test artifacts, not stable user behavior".

## Why It Happens

**Tags:** `root cause` (orange), `metadata` (blue)

- **Two systems** — experiment assignments live in the platform, not the analytics warehouse
- **Pooled ingestion** — the warehouse loads all variants together with no record of origin
- **Metadata stripping** — ETL drops experiment columns as "non-essential" operational noise
- **Overlapping tests** — the same user sits in experiments A and B at the same time
- **Unlabeled history** — old data mixes control and treatment rows with no way to tell them apart

*Example:* Three concurrent checkout experiments train one model on all users, so it learns an average effect matching no production configuration.

**Root Cause:** Experiment metadata is treated as operational logging rather than data lineage — once stripped, which behavior generated which outcome is unrecoverable.

### Visualization (canvas `c2`, 720×300)

Flow diagram: three tagged experiment streams merging through an ETL box that drops the tags, into a warehouse of unidentifiable rows.

- **Title (bold 14px `#1a5276`, top center):** "Experiment Tags Lost During Data Merge".
- **Three stream boxes (left):** 140×40 at x=30, y = 55/115/175; colors `#27ae60` (Exp A), `#e67e22` (Exp B), `#2980b9` (Exp C); fill at alpha 0.15, stroke width 2; bold 11px name, plus 9px monospace tags inside: "exp_id=a|b|c" and "variant=ctrl/treat". Gray arrows from each stream to the ETL box.
- **ETL box:** 130×180 at (250,50), fill `#e74c3c` alpha 0.1, stroke `#e74c3c` width 2; bold 12px red "ETL Pipeline"; 10px monospace red lines "DROP exp_id", "DROP variant", "\"non-essential\""; two bold 16px red "✗" marks.
- **Gray arrow** to the warehouse box: 240×140 at (450,70), fill `rgba(26,82,118,0.08)`, stroke `#1a5276` width 2; header bold 12px "Data Warehouse".
- **Mixed rows:** six 9px monospace rows with faint color-tinted backgrounds (alpha 0.12, cycling `#27ae60`/`#e67e22`/`#2980b9`):
  - `user_42  click  purchase  ???`
  - `user_88  view   no_buy    ???`
  - `user_15  click  purchase  ???`
  - `user_63  view   purchase  ???`
  - `user_27  click  no_buy    ???`
  - `user_91  view   purchase  ???`
- **Column header:** bold 9px red "exp? variant?" at (660,100).
- **Bottom warning (bold 11px `#e74c3c`, centered):** "No way to distinguish which experiment generated which row".

## The Correct Approach

**Tags:** `the fix` (green), `lineage` (blue)

- **Tag every row** — log experiment_id and variant at collection time as permanent lineage
- **Train on stable data** — prefer control-only rows or data collected after the test settled
- **Condition explicitly** — if mixing is unavoidable, add experiment_id and variant as features
- **Audit before pooling** — measure how each experiment shifted features and labels first
- **Document windows** — record which experiments were active during each training window

*Example:* The training query keeps rows where experiment_id is NULL, variant='control', or the experiment concluded with that variant winning.

**Fix:** Filter training to control rows and concluded winners — never train on in-flight treatment variants unless the model explicitly conditions on variant assignment.

### Visualization (canvas `c3`, 720×300)

Pipeline diagram: a tagged data store passing through a SQL filter step into a clean training set and model.

- **Title (bold 14px `#1a5276`, top center):** "Proper Filtering: Control-Only / Post-Settled Training Data".
- **Tagged data store box:** 180×160 at (20,40), fill `rgba(26,82,118,0.08)`, stroke `#1a5276` width 2; header bold 11px "Tagged Data Store". Ten 8px monospace rows with faint color-tinted backgrounds, text colored green `#27ae60` (control/concluded), red `#e74c3c` (treatment), or blue `#1a5276` (NULL):
  - `u42 exp_a ctrl` (green), `u88 exp_a treat` (red), `u15 exp_b ctrl` (green), `u63 exp_b treat` (red), `u27 exp_c ctrl` (green), `u91 NULL  none` (blue), `u54 exp_a concluded` (green), `u77 exp_b treat` (red), `u33 NULL  none` (blue), `u19 exp_c treat` (red).
- **Gray arrow** to the filter box: 160×135 at (260,55), fill `#e67e22` alpha 0.1, stroke `#e67e22` width 2; header bold 11px orange "Filter Step". 9px monospace filter rules (WHERE in `#333`, clauses alternating `#27ae60`/`#333`):
  - `WHERE`
  - `  exp_id IS NULL`
  - `  OR variant='control'`
  - `  OR (concluded=true`
  - `      AND variant=`
  - `      winning_variant)`
  - plus bold 9px red "✗ in-flight treatment".
- **Green arrow** to the clean training set box: 220×120 at (480,50), fill `#27ae60` alpha 0.08, stroke `#27ae60` width 2; header bold 11px green "Clean Training Set". Six 9px monospace rows on faint green backgrounds:
  - `u42 exp_a ctrl       ✓`
  - `u15 exp_b ctrl       ✓`
  - `u27 exp_c ctrl       ✓`
  - `u91 NULL  none       ✓`
  - `u54 exp_a concluded  ✓`
  - `u33 NULL  none       ✓`
- **Green arrow down** to the model box: 160×40 at (510,215), fill `#27ae60` alpha 0.1, stroke `#27ae60` width 2; bold 12px "Model", 10px `#333` "Learns stable behavior"; bold 14px green "✓" beside it.
- **Bottom annotation (bold 11px `#27ae60`, centered):** "Model trains only on production-representative data — no experiment artifacts".

## Regeneration instructions

- **Layout:** three `.card-section` blocks (The Problem / Why It Happens / The Correct Approach), each an h2 with blue bottom border followed by a `table.layout` with one row: left `td.text-col` (45%) holding `.tags` pills, a `ul` of labeled bullets, an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `ul` 0.92rem; `li b` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `strong` in `#1a5276`. `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`; secondary blue `#2980b9` used for the third experiment band.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
