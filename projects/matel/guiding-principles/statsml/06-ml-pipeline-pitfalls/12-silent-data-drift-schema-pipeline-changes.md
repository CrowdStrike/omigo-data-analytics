# Pitfall: Silent Data Drift (Schema/Pipeline Changes)

**Page type:** detail page (card-section layout: h2 per section, two-column table with text left 45% / canvas right 55%)
**HTML title tag:** Silent Data Drift (Schema/Pipeline Changes)

**Subtitle:** Upstream data changes without notification, model degrades silently.

## The Problem

**Tags:** `the trap` (red), `data drift` (blue)

- **Silent break** — upstream data changes format without notice, yet the model keeps running
- **Unit change** — a currency column switches from dollars to cents, shifting every value 100x
- **Encoding change** — one-hot switches to label encoding, so the same numbers mean new things
- **Format change** — dates move from YYYY-MM-DD strings to Unix timestamps
- **Missing values** — the sentinel for missing entries changes from -1 to NULL
- **Upstream scaling** — a new scaling step appears that the training data never went through

*Example:* A loan model trained on income in thousands suddenly receives 65500 after an ETL change, reads it as $65.5M, and approves everyone.

**Impact:** The data stays type-valid but semantically wrong, so predictions degrade silently and days of bad decisions accumulate before anyone notices.

### Visualization (canvas `c1`, 720×300)

Two side-by-side histograms of the income feature before and after a unit change.

- **Title (bold 14px, `#1a5276`, centered):** "Income Feature: Training vs Production (After Pipeline Change)".
- **Left histogram (training):** 10 bars starting at x=100, bar width 25 (2px gap), heights proportional to values `[20, 45, 80, 120, 95, 60, 30, 15, 8, 3]` scaled to max height 160 (divide by 120), fill `#27ae60` at 0.5 alpha, baseline y=250. Green 11px label above: "Training: mean=67.3K, std=22.1K". X tick labels below: "50K" at bar 5, "100K" at bar 9.
- **Right histogram (production):** 10 bars starting at x=450, same width/scale, heights `[3, 8, 15, 30, 60, 95, 120, 80, 45, 20]` (mirror-skewed), fill `#e74c3c` at 0.5 alpha. Red label above: "Production: mean=67300 (!), std=22100". X tick labels: "50M" at bar 5, "100M" at bar 9.
- **Axis:** 1px `#333` horizontal baseline from x=90 to x=700 at y=250.
- **Bottom annotation (bold 12px red, centered):** "Pipeline changed units from thousands to dollars. Model still expects thousands!".

## Why It Happens

**Tags:** `root cause` (orange), `pipelines` (blue)

- **End of the pipeline** — models sit downstream of long chains they neither own nor observe
- **Split ownership** — pipeline and model belong to different teams with no schema contract
- **Type-level blindness** — float stays float, so no type error is ever raised
- **Invisible deploys** — changes ship as "internal refactoring" with no notice to the model team
- **No distribution checks** — nothing compares live data against the training distribution
- **Wrong monitoring target** — accuracy lags by days; input statistics shift immediately

*Example:* Feature engineering swaps log(price) for raw price, and predictions shift by orders of magnitude with no error raised.

**Root Cause:** The model applies coefficients learned at mean=50, std=10 to data now arriving at mean=5000, std=1000 — syntactically valid, semantically nonsense.

### Visualization (canvas `c2`, 720×300)

Line chart of model accuracy over an 18-day timeline showing silent degradation after a pipeline change.

- **Title (bold 14px, `#1a5276`, centered):** "Model Accuracy Over Time: Silent Drift Degradation".
- **Data (day, accuracy %):** (0, 88 "Deploy"), (2, 88), (4, 87), (6, 86), (8, 85 "Pipeline change"), (10, 78), (12, 72), (14, 68), (16, 65), (18, 64 "Finally noticed!").
- **Axes:** x spans days 0–18 mapped from x=100 to x=650; y mapped as `y = 220 − (acc − 60) × 5`; gray `#ccc` 2px timeline at y=240; right-aligned y-axis labels "90%", "80%", "70%", "60%" in 10px `#666`.
- **Series:** 3px `#2980b9` connected line; 5px-radius dots at each point colored by accuracy: green `#27ae60` if ≥85, orange `#e67e22` if ≥75, red `#e74c3c` below.
- **Annotations:** two-line 10px blue labels above the marked points ("Deploy", "Pipeline change", "Finally noticed!"); dashed (4,3) 2px red vertical line at day 8 (the pipeline change).
- **Bottom annotation (bold 11px red, centered):** "10 days of silent degradation. No alerts. No errors. Just wrong predictions.".

## The Correct Approach

**Tags:** `the fix` (green), `monitoring` (blue)

- **Detect, don't prevent** — upstream changes cannot be stopped, only caught immediately
- **Baseline stats** — store min, max, mean, std, and cardinality of every training feature
- **Input validation** — before predicting, check live stats sit within tolerance, e.g. ±20%
- **Schema contracts** — enforce them with validation tools like Great Expectations or Pandera
- **Drift alarms** — alert on a failed KS test or a PSI above roughly 0.2, tuned per feature
- **Fail loudly** — version the pipeline, halt predictions, and page the on-call over garbage

*Example:* Training logged income_mean=67.3; production shows 67300, so the drift detector flags a 1000x shift and halts predictions.

**Fix:** Compare production input stats to the training baseline on every run, and halt predictions with an alert when drift exceeds your tolerance.

### Visualization (canvas `c3`, 720×300)

Drift-detection flow: training baseline stats compared against production stats, leading to a halt verdict.

- **Title (bold 14px, `#1a5276`, centered):** "Drift Detection: Compare Production Stats to Training Baseline".
- **Left box — "Training Baseline":** 200×140 at (50, 70), white fill, 2px green `#27ae60` border, bold green heading; 11px `#333` stat lines: "income_mean: 67.3", "income_std: 22.1", "income_min: 18.5", "income_max: 185.2", "age_mean: 42.6", "age_std: 14.8".
- **Blue arrow** (2px `#2980b9`) between the boxes labeled below in bold blue: "Compare".
- **Middle box — "Production Data":** 200×140 at (300, 70), white fill, 2px red `#e74c3c` border, bold red heading; stat lines with the first four in red: "income_mean: 67300", "income_std: 22100", "income_min: 18500", "income_max: 185200"; last two in `#333`: "age_mean: 42.8", "age_std: 14.9".
- **Blue arrow to verdict box:** 140×80 at (550, 100), white fill, 3px red border; bold red "DRIFT" / "DETECTED", 10px "1000x shift" / "HALT".
- **Bottom line (bold 11px green, centered):** "Fail fast. Alert immediately. Don't return garbage predictions.".

## Regeneration instructions

- **Layout:** `.card-section` per section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (border-collapse, full width) with one `<tr>`: `td.text-col` (45%) holding `.tags` pills + `<ul>` bullets + `.example` italic paragraph + `.key-point` callout; `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `li b` colored `#1a5276`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- In regenerated HTML, any card links use `.html` extensions.
