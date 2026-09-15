# Pitfall: No Data Quality Monitoring Over Time

**Page type:** detail page (card-sections with h2 headers, two-column layout table per section: text left 45%, canvas right 55%)
**HTML title tag:** No Data Quality Monitoring Over Time

**Subtitle:** Data distributions, completeness, and schema drift go unnoticed for months without regular monitoring

## The Problem

Tags: `the trap` (red), `data drift` (blue)

- **Silent degradation** — production data quality drifts while the model stays frozen in time
- **Distribution drift** — feature distributions shift as users and upstream systems change
- **Missingness spikes** — null rates jump when an upstream API fails or a schema drops a field
- **Cardinality growth** — unbounded categoricals keep accumulating new distinct values
- **New outliers** — production produces outlier patterns never present in the training data
- **Late discovery** — nobody notices until business KPIs collapse, long after drift began

*Example:* A recommender's CTR drops 15% over 3 months as a partner API returns nulls 20% of the time, found only in a revenue review.

**Impact:** Performance decays silently for months, and root cause analysis becomes forensics instead of real-time triage.

### Visualization (canvas `c1`, 720×300)

Line chart of a feature mean drifting away from its training baseline over 12 weeks, with an unmonitored alert threshold.

- **Title (bold 14px, top center, `#1a5276`):** "Feature Distribution Drift — Silent Degradation Over 12 Weeks".
- **Plot area:** left=60, right=680, top=60, bottom=260; gray (`#999`) L-shaped axes; x label "Weeks in Production →" (11px `#666`), rotated y label "Feature Mean".
- **Training baseline:** horizontal dashed green (`#27ae60`, width 2, dash 6/4) line at 60% of plot height, labeled "Training baseline" (11px green, right side).
- **Production series:** red (`#e74c3c`, width 2.5) line declining gradually across the full width: value `0.6 - t*0.35` plus small sine noise `0.02*sin(t*15)`, t from 0 to 1 over 100 points. Labeled "Production reality" (11px red, top right).
- **Alert threshold:** horizontal dashed orange (`#e67e22`, width 1.5, dash 4/4) line 20% of plot height below the baseline, labeled "Alert threshold (2σ)" (10px orange, right side).
- **Crossing marker:** filled orange dot (radius 5) where the drift crosses the threshold at 40% of x; below it bold 10px red two-line annotation: "No alert!" / "Drift continues...".

## Why It Happens

Tags: `root cause` (orange), `ownership` (blue)

- **Static-data mindset** — data is cleaned once and treated as frozen, so quality has no owner
- **Project ends at launch** — in most teams' mental model, deployment is the finish line
- **Deferred investment** — monitoring gets no budget until the first major incident
- **Silent upstream changes** — providers change formats or deprecate fields without notice
- **Legitimate shifts** — A/B tests, seasonality, and product changes move distributions too
- **Invisible by default** — drift only shows up if someone measures it, and nobody is assigned

*Example:* A fraud model trained on a 2% missing rate sees 18% nulls after an upstream field is deprecated, unnoticed for 2 months.

**Root Cause:** Production data is a moving target, but no one owns its quality after deployment, so drift goes unmeasured.

### Visualization (canvas `c2`, 720×300)

Horizontal baseline-vs-current bar pairs for four data quality metrics, each with an alert threshold marker.

- **Title (bold 14px, top center, `#1a5276`):** "Data Quality Metrics to Monitor".
- **Rows (metric label right-aligned at x=90 in 11px `#666`, bars start at x=100, full bar span 500px, rows at y = 70, 130, 190, 250):**

| Metric | Baseline | Current | Threshold | Breached |
|---|---|---|---|---|
| Mean Shift | 0.5 | 0.35 | 0.15 | yes |
| Missing Rate | 0.02 | 0.18 | 0.12 | yes |
| Cardinality | 120 | 450 | 300 | yes |
| Outlier Rate | 0.01 | 0.03 | 0.05 | no |

- **Bar rendering:** each row shows a baseline bar (fill `rgba(39,174,96,0.4)`, 30% of span, 10px tall) above a current bar (70% of span if breached in `rgba(231,76,60,0.6)`, else 40% of span in `rgba(39,174,96,0.6)`); a vertical orange (`#e67e22`, width 2) threshold line at 50% of span crosses both bars.
- **Inline labels (9px):** "Baseline" in green on the top bar, "Current" in red (breached) or green (ok) on the bottom bar, "Alert" in orange next to the threshold line.

## The Correct Approach

Tags: `the fix` (green), `monitoring` (blue)

- **Monitor from day one** — build data quality checks into the deployment pipeline itself
- **Log statistics** — record distribution stats for every feature on every batch
- **Relative thresholds** — alert against training-data baselines, not absolute values
- **Drift tests** — track PSI or KL divergence of each batch against the training baseline
- **Watch everything** — mean shifts, missingness, new categories, outlier rates, cardinality
- **Tune per feature** — a 2σ mean shift or 10-point missingness change is only a starting point

*Example:* A credit model monitoring 50 features daily alerts when income_mean drops 15% in a week, catching an upstream bug before retraining.

**Fix:** Track data quality with the same rigor as model performance — baseline at training time, alert on drift, and review on a regular cadence.

### Visualization (canvas `c3`, 720×300)

Monitoring pipeline flow diagram with conditional alert branch and dashboard preview strip.

- **Title (bold 14px, top center, `#1a5276`):** "Monitoring Pipeline — Automated Drift Detection".
- **Flow boxes (white fill, colored stroke width 2, labels 11px `#2c3e50`, two lines each):**
  - "Incoming / Batch" at (80,100), 100×60, stroke `#1a5276`
  - "Compute / Statistics" at (220,100), 100×60, stroke `#1a5276`
  - "Compare to / Baseline" at (360,100), 100×60, stroke `#e67e22`
  - "Alert / Team" at (500,60), 100×50, stroke `#e74c3c`
  - "Log / Metrics" at (500,140), 100×50, stroke `#27ae60`
- **Arrows:** solid gray (`#666`, width 2) between the first three boxes; a dashed red (`#e74c3c`, dash 4/4) branch up to "Alert Team" labeled "If drift > threshold" (9px red); a solid green (`#27ae60`) branch down to "Log Metrics" labeled "Always" (9px green).
- **Dashboard preview strip (bottom):** thin light-gray (`#ccc`) bordered box 600×50 at (60,220), containing two 10px `#666` lines: "Dashboard: Track PSI, KL divergence, missingness, cardinality over time" and "Alert rules: Mean shift > 2σ | Missing rate Δ > 10% | New categorical values".

## Regeneration instructions

- **Layout:** repeated `.card-section` blocks, one per section. Each has an `<h2>` (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) followed by a `table.layout` (full width, border-collapse) with a single `<tr>`: left `td.text-col` (45%) containing `.tags` pills, a `<ul>` of labeled bullets, a `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) containing one `<canvas width="720" height="300">`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `2px solid #2980b9` bottom border. `.subtitle` `#666` 0.95rem. `ul` 0.92rem with `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; intrinsic size 720×300, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
