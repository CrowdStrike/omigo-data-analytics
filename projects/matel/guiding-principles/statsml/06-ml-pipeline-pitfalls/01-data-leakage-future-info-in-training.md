# Pitfall: Data Leakage (Future Info in Training)

**Page type:** detail page (card-sections with h2 headings; each section is a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Data Leakage (Future Info in Training)

**Subtitle:** Model uses information not available at prediction time

## The Problem

Tags: `the trap` (red pill), `future info` (blue pill)

- **The trap** — training data contains information that will not exist at prediction time
- **Future-derived features** — "total_purchases_lifetime" leaks the answer into a churn model
- **Impossible task** — the leaked feature encodes the answer, so offline metrics look excellent
- **Production collapse** — with only genuinely available inputs, accuracy drops from 99% to 55%
- **Subtle variants** — full-range aggregates, cross-row features, and unfiltered joins leak too

*Example:* Predicting hospital readmission from the discharge diagnosis code fails — that code is assigned only after the outcome is known.

**Impact:** Offline metrics become meaningless, and the failure only surfaces after deployment, when it is most expensive.

### Visualization (canvas `c1`, 720×300)

Timeline diagram showing future information leaking back into training.

- **Title (bold 14px `#1a5276`, top center):** "Timeline: Future Information Leaking Into Training".
- **Timeline:** horizontal `#333` axis (width 2) from x=60 to x=660 at y=150; gray `#666` labels "Past" (left end) and "Future" (right end) below the axis.
- **Prediction point:** vertical blue `#2980b9` line (width 3) at x=300 spanning ±60px around the axis; bold blue stacked labels above: "PREDICTION" / "POINT" / "(t=0)".
- **Valid features (before t=0):** three 40×20 boxes at x=120, 180, 240 just above the axis, fill green `#27ae60` at 0.6 alpha with solid green stroke, labeled "age", "income", "history" in green; green caption above: "Valid features" / "(available at t=0)" at x≈170.
- **Leakage features (after t=0):** three 60×20 boxes at x=380, 480, 580, fill red `#e74c3c` at 0.6 alpha with red stroke, white labels "purchases", "churn", "lifetime_val".
- **Leakage arrow:** dashed red arrow (dash 4/3, width 3) from x=480 back to the prediction line at y=100, with a filled red arrowhead; red two-line label above it: "LEAKAGE: future info" / "flows back to training".
- **Bottom message (bold red, centered):** "At prediction time, you won't have these future values. Model learns an impossible task."

## Why It Happens

Tags: `root cause` (orange pill), `tooling defaults` (blue pill)

- **Not deliberate** — leakage is the default behavior of standard data tooling, not a rare bug
- **No timestamp discipline** — a "past" aggregate silently includes events after the cutoff
- **Missing audit** — no "available at prediction time?" review, so leaky features pass unchallenged
- **Unfiltered joins** — joins pull in future records because no temporal filter is applied
- **Convenient defaults** — rolling stats without cutoff dates run correctly and look reasonable

*Example:* An "avg_spending_30d" feature averaged over the whole dataset lets a Jan 1 prediction quietly include Feb–Dec spending.

**Root Cause:** The pipeline treats all data as one static snapshot, with no concept of when each row was created.

### Visualization (canvas `c2`, 720×300)

Pipeline flow diagram showing where leakage enters.

- **Title (bold 14px `#1a5276`, top center):** "Where Leakage Enters the Pipeline".
- **Pipeline boxes (150×50, white fill, 2px colored stroke, bold centered labels):** "Raw Data" (`#1a5276`) at x=80, "Feature Engineering" (`#e67e22`) at x=285, "Model Training" (`#1a5276`) at x=520, all at y=80.
- **Flow arrows:** solid blue `#1a5276` arrows (width 2) with filled arrowheads: Raw Data → Feature Engineering, Feature Engineering → Model Training.
- **Future Events box:** 150×50 white box with red `#e74c3c` stroke at (285, 200), bold red label "Future Events".
- **Leak arrow:** dashed red arrow (dash 6/4, width 3) from Future Events up into the Feature Engineering box, with a filled red arrowhead pointing up; bold red label "No temporal filter" to the right of the Future Events box.
- **Bottom message (bold red, centered):** "Future events leak into feature engineering because no timestamp boundary is enforced."

## The Correct Approach

Tags: `the fix` (green pill), `time cutoffs` (blue pill)

- **Structural cure** — make time a parameter of every feature, so touching the future is impossible
- **Temporal audit** — ask "available at prediction time?" for every feature; remove any that fail
- **Cutoff parameter** — each computation takes a cutoff timestamp and reads nothing after it
- **Point-in-time joins** — joined tables reflect exactly what was known at the prediction moment
- **Feature registry** — enforce the temporal constraint in code instead of reviewer memory
- **Cutoff validation** — a distribution jump when the cutoff moves is a strong hint of leakage

*Example:* The correct "avg_spending_30d" for a Jan 1 prediction queries only Dec 1–31 transactions from the feature store.

**Fix:** Build a point-in-time feature store whose queries enforce WHERE event_time &lt; prediction_time, so future data is physically inaccessible.

### Visualization (canvas `c3`, 720×300)

Timeline diagram of the correct point-in-time feature store.

- **Title (bold 14px `#1a5276`, top center):** "Correct Pipeline: Point-in-Time Feature Store".
- **Timeline:** horizontal `#333` axis from x=60 to x=660 at y=150; gray `#666` labels "Past" and "Future" at the ends below.
- **Prediction point:** vertical blue `#1a5276` line (width 3) at x=360 spanning ±50px, bold blue label "Prediction Point" above.
- **Feature query box:** 220×40 white box with green `#27ae60` stroke at (100, 50) containing green text "WHERE event_time < prediction_time".
- **Allowed features (before cutoff):** at x=140, 220, 300 — bold green "✓" marks above small 40×16 boxes filled `rgba(39,174,96,0.3)` with green 1px strokes; green labels below: "feature_1", "feature_2", "feature_3"; bold green legend "ALLOWED (before cutoff)" centered at x=220.
- **Blocked features (after cutoff):** at x=440, 520, 600 — bold red "✗" marks above 40×16 boxes filled `rgba(231,76,60,0.2)` with red 1px strokes; red labels "future_1", "future_2", "future_3"; bold red legend "BLOCKED (after cutoff)" centered at x=520.
- **Bottom message (bold green, centered):** "The query enforces WHERE event_time < prediction_time. Future data is physically inaccessible."

## Regeneration instructions

- **Layout:** h1 with 2px `#2980b9` bottom border, `.subtitle` paragraph, then three `.card-section` divs. Each section has an `h2` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with one row: `td.text-col` (45%, top-aligned) holding `.tags` pills + `<ul>` bullets + `.example` italic line + `.key-point` callout; `td.viz-col` (55%) holding the canvas.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.blue` `rgba(26,82,118,0.12)`/`#1a5276`, `.green` `rgba(39,174,96,0.15)`/`#27ae60`, `.red` `rgba(231,76,60,0.12)`/`#e74c3c`, `.orange` `rgba(230,126,34,0.15)`/`#e67e22`.
- **Bullets:** `<li><b>Label</b> — sentence</li>`; `li b` colored `#1a5276`; list 0.92rem. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem, with bold lead word.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`. Canvases have `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** all three canvases declare intrinsic 720×300; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
