# Pitfall: Look-Ahead Bias in Feature Engineering

**Page type:** detail page (card-section layout: one h2 section per block, two-column table text left 45% / canvas right 55%)
**HTML title tag:** Look-Ahead Bias in Feature Engineering

**Subtitle:** Feature uses information from the same row's future or outcome.

## Section 1: The Problem

**Tags:** `the trap` (red pill), `future data` (blue pill)

- **The trap** — the feature contains data that would not be available at prediction time
- **Churn** — days_since_last_login over the full dataset includes post-prediction logins
- **Fraud** — a mid-month score's total_transactions_this_month counts transactions not yet made
- **Credit** — a default model's credit_score_6months_later is an outcome measured after prediction
- **Time series** — normalizing by the full-series mean and std leaks future stats into each row

*Example:* A churn model using "days_until_next_purchase" scores 0.97 AUC in validation but 0.58 in production.

**Impact:** Validation metrics look excellent because the model sees the future, then performance collapses at deployment when that information is gone.

### Visualization (canvas `c1`, 720×300)

Timeline diagram splitting valid past data from leaked future data.

- **Title (bold 14px, `#1a5276`, top center):** "Look-Ahead Bias: Feature Uses Future Information".
- **Timeline:** horizontal `#444` line width 2 at y=120 from x=80 to x=680.
- **Prediction marker:** vertical orange `#e67e22` line width 3 at x=280 spanning y=90–150; orange labels above: bold 12px "PREDICTION" / "TIME" and 10px "(Day 30)".
- **Past band:** rectangle from x=80 to x=280, y=100–140, fill `rgba(39,174,96,0.3)`, stroke `#27ae60` width 2; green 11px labels centered inside: "VALID DATA" / "(available at prediction time)".
- **Future band:** rectangle from x=280 to x=680, same height, fill `rgba(231,76,60,0.3)`, stroke `#e74c3c` width 2; red 11px labels: "FUTURE DATA" / "(NOT available — LEAKAGE!)".
- **Example text (left-aligned at x=80):** bold 11px red 'Feature: "total_transactions_this_month"'; 10px lines "Computed using all of Month 1 (Days 1-30), but prediction is on Day 15" and "Days 16-30 are future — feature contains look-ahead bias".
- **Impact block (bold 11px `#444` "Impact:" then 10px bullets):** "• Training: model sees future, AUC = 0.97", "• Production: future unavailable, AUC = 0.58".

## Section 2: Why It Happens

**Tags:** `root cause` (orange pill), `aggregation` (blue pill)

- **Order of operations** — features are engineered on the full dataset before the temporal split
- **Global stats** — mean, std, min, max computed before the split push future data into every row
- **Cumulative counts** — running totals and lifetime counts include events after each row's date
- **Careless joins** — tables joined without an as-of timestamp break point-in-time correctness
- **Target encoding** — encodings built from the target on the full data include the test rows

*Example:* A fraud model learns that $5000 in future disputes signals high risk, but in production it sees only the $50 disputed so far and predicts low risk.

**Root Cause:** Offline you hold the complete timeline, so any "lifetime" aggregate quietly counts events past the prediction date that day-30 production will never see.

### Visualization (canvas `c2`, 720×300)

Line chart of AUC dropping off a cliff from validation to production.

- **Title (bold 14px, `#1a5276`, top center):** "Performance Cliff: Validation Looks Great, Production Fails".
- **Points (baseline y=250, plotted y = 250 − score × 1.7):** Train at x=120 score 97; Validation at x=300 score 95; Production at x=480 score 58. Dots 8px radius with white 2px ring; Train and Validation green `#27ae60`, Production red `#e74c3c`. Bold 16px score above each dot in its color; 12px `#444` label ("Train" / "Validation" / "Production") below the baseline.
- **Segments:** Train→Validation line orange `#e67e22` width 3; Validation→Production line red `#e74c3c` width 4 (the cliff).
- **Annotations:** green 10px "Future data" / "available" near (210, 130); bold red 11px "CLIFF: future data" / "not available!" near (390, 150).
- **Deployment boundary:** vertical dashed orange line (dash 6/4, width 2) at x=380 from y=60 to y=240, labeled in 10px orange "Deployment" / "boundary".

## Section 3: The Correct Approach

**Tags:** `the fix` (green pill), `point-in-time` (blue pill)

- **As-of joins** — for a prediction at time T, aggregate only records timestamped strictly before T
- **Expanding windows** — normalize time series with past-only statistics, never the full series
- **Pipeline discipline** — filter by timestamp before any aggregate so leakage cannot happen
- **Walk-forward validation** — retrain on an expanding window that never contains future data
- **Detection** — sample random prediction dates, hide later data; a big metric drop reveals bias

*Example:* Recomputing the feature as "days_since_last_login_as_of_prediction_date" yields train AUC 0.84, validation 0.83, and production 0.82.

**Fix:** For each row with prediction timestamp T, compute every feature using only data where timestamp < T, in every join and aggregation.

### Visualization (canvas `c3`, 720×300)

Side-by-side boxes comparing incorrect vs point-in-time features, plus a performance grid.

- **Title (bold 14px, `#1a5276`, top center):** "Correct (Point-in-Time) vs Incorrect (Look-Ahead) Feature Engineering".
- **Left box (INCORRECT):** white 280×100 rectangle at (60, 60), stroke `#e74c3c` width 3; bold red 12px heading "INCORRECT"; 10px `#444` lines: "Feature: avg_monthly_spend", "Computed: mean over all months", "(includes future months)", "Result: Train 97, Prod 58".
- **Right box (CORRECT):** white 280×100 rectangle at (380, 60), stroke `#27ae60` width 3; bold green heading "CORRECT (Point-in-Time)"; lines: "Feature: avg_monthly_spend_as_of_T", "Computed: mean over months < T", "(only past data)", "Result: Train 84, Prod 82".
- **Comparison grid (heading bold 11px `#1a5276` "Performance Comparison" at y=190):** columns Train / Val / Prod (10px `#444` headers, 80px column width starting x=200, 30px rows); row "Incorrect" in red with values 97 / 95 / 58; row "Correct" in green with values 84 / 83 / 82. Row labels bold 10px right-aligned in row color; each value cell has a 0.2-alpha background tint in the row color with bold 12px value text.
- **Caption (bold 11px green `#27ae60`, bottom center):** "Point-in-time features: stable performance, production-ready".

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: left `td.text-col` (45%) holding `.tags` pills, `<ul>` bullets, `.example` italic paragraph, and `.key-point` callout; right `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; `li b` in `#1a5276`; bullets 0.92rem.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border with 4px radius; scale via a shared `setup(id)` helper using `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#666`/`#444`/`#333`. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
