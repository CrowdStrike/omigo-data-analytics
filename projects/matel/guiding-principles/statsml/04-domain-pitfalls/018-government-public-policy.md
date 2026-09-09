# Government / Public Policy: Domain-Specific Pitfalls

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Domain Pitfalls: Government / Public Policy

**Subtitle:** Government data is shaped by policy, incentives, and accountability constraints — models must navigate Goodhart's Law, fairness impossibility, and the prediction-prescription gap.

## Callout (philosophy box)

**Core tension:** Government decisions affect millions of people who cannot opt out. The stakes of model errors are asymmetric — a false positive in criminal justice means someone loses their freedom; a false negative in benefits allocation means a family goes hungry. Every modeling choice is implicitly a policy choice.

## Policy Changes Invalidate Models

**New Law → Behavior Changes → Old Model Wrong**

When policy changes, the data-generating process shifts. Models trained on pre-policy data make systematically wrong predictions post-policy.

- **Example:** A model predicts tax revenue based on historical filing patterns. A new tax law changes deductions.
- **Divergence:** Filers respond to the new deductions — the model's predictions diverge from reality.
- **Why it's dangerous:** The model looks fine on historical validation but fails silently in production.
- **Delayed failure:** Nothing breaks at deploy time — the errors begin only after the policy takes effect.
- **Mitigation:** Monitor for distributional drift; build "regime change" flags; retrain rapidly after policy shifts.

### Visualization (canvas `canvas1`, 720×300)

Diverging line chart: model prediction vs reality before/after a policy change.

- **Layout:** margins left 80 / right 30 / top 30 / bottom 40; plot background `#f9f9f9`; L-shaped `#333` axes; rotated y-axis label "Outcome" and x-axis label "Time" (14px `#333`).
- **Policy marker:** vertical dashed red line (`#e74c3c`, dash 6/4, width 2) at 50% of plot width, labeled above in red 17px: "New Policy Enacted".
- **Reality line (solid `#1a5276`, width 2.5):** rises linearly from 70% plot height down to 50% at the policy line (improving), then falls back down — after the policy, y descends from 50% to 100% of the drop (`0.5 + (t-0.5)*0.5` of plot height), i.e. reality worsens.
- **Model prediction line (dashed orange `#e67e22`, dash 8/4, width 2.5):** continues the pre-policy trend across the whole plot (from 70% to 30% of plot height), diverging from reality after the policy.
- **Divergence annotation:** at 80% of plot width, a vertical red connector with arrowhead between the model line and reality line, labeled "Error" in red 13px.
- **Legend (top left inside plot, 14px):** blue swatch "Reality"; dashed orange line sample "Model Prediction".

## Reporting Incentives (Goodhart's Law)

**When a Measure Becomes a Target, It Ceases to Be a Good Measure**

Organizations under measurement pressure optimize the metric, not the underlying outcome. The reported number improves while reality stagnates or worsens.

- **Schools:** Teaching to the test raises scores without improving learning.
- **Hospitals:** Reclassifying patients to reduce "wait time" without faster care.
- **Police:** Downgrading crimes to show lower crime statistics.

**Signal:** If a metric improves dramatically right after being used for accountability, suspect gaming.

### Visualization (canvas `canvas2`, 720×300)

Diverging line chart: reported metric vs actual quality after a metric becomes a KPI.

- **Layout:** margins left 80 / right 30 / top 30 / bottom 40; plot background `#f9f9f9`; `#333` axes; rotated y label "Performance", x label "Time" (14px).
- **KPI marker:** vertical dashed orange line (`#e67e22`, dash 5/3, width 2) at 35% of plot width, labeled above in orange 13px: "Metric becomes KPI".
- **Reported metric line (green `#27ae60`, width 2.5):** gentle improvement before the marker (65% → 60% of plot height), then sharp improvement after (60% down to 15% of plot height by the right edge).
- **Actual quality line (red `#e74c3c`, width 2.5):** identical to the reported line before the marker, then flat/slightly declining after (60% drifting to 68% of plot height).
- **Gap annotation:** at 82% of plot width, a dashed gray `#999` double-arrow vertical connector between the two lines, labeled "Gaming gap" in red 13px.
- **Legend (top left inside plot, 14px):** green swatch "Reported Metric"; red swatch "Actual Quality".

## Ecological Fallacy in Resource Allocation

**Aggregate Statistics ≠ Individual Truth**

A zip code with a high crime rate does not mean most residents are criminals. Using area-level statistics to make individual-level decisions is the ecological fallacy.

- **The math:** A "high-crime" zip code might have a 5% offender rate — 95% of residents are law-abiding.
- **Consequence:** Over-policing, higher insurance, reduced services — punishing the majority for the minority.
- **Fix:** Never use aggregate-level features as proxies for individual risk without explicit justification.
- **Audit:** Where such a feature is unavoidable, pair the justification with a documented fairness audit.

### Visualization (canvas `canvas3`, 720×300)

Two-panel comparison: aggregate zip-code label vs individual-level dots.

- **Left panel header (15px `#1a5276`, centered):** "Zip-Code Level View".
  - Rectangle at (40,35), width = half-canvas minus 80, height 130; fill light red `#fdecea`, border red `#e74c3c` width 2.
  - Centered text inside: "ZIP 90210" (red 17px), "\"High Crime Area\"" (red 14px), "Crime Rate: 5%" and "Label: RISKY" (dark 13px `#333`).
- **Center:** large red "→" (20px) between panels.
- **Right panel header (15px `#1a5276`, centered):** "Individual Level View".
  - Grid of 20 dots (5 columns × 4 rows, 12px radius) starting near x = midX+40, y = 45; 19 dots green `#27ae60` and exactly 1 dot (index 7 — row 2, column 3) red `#e74c3c`.
- **Legend (bottom of right panel, 13px `#333`):** green dot "Law-abiding (95%)"; red dot "Offender (5%)".

## Fairness Impossibility Theorem

**You Cannot Satisfy All Fairness Metrics Simultaneously**

Mathematical impossibility results (Chouldechova 2017, Kleinberg et al. 2016) prove that when base rates differ between groups, you cannot simultaneously achieve:

- **Calibration:** Equal positive predictive value across groups
- **Equal FPR:** Same false positive rate for all groups
- **Equal FNR:** Same false negative rate for all groups

**Implication:** Choosing which fairness metric to satisfy is a *policy decision*, not a technical one. There is no "fair algorithm" — only explicit tradeoffs.

### Visualization (canvas `canvas4`, 720×300)

Three-circle Venn diagram with an impossibility marker in the triple overlap.

- **Circles:** radius 70, centers offset 35px from a hub at (w/2−60, h/2+5) at angles −90°, 30°, 150°; fills at 20% alpha and 2px strokes at full alpha:
  - Top circle blue `#1a5276`, labeled above: "Calibration" (14px blue).
  - Lower-left circle green `#27ae60`, labeled below-left: "Equal FPR" (14px green).
  - Lower-right circle orange `#e67e22`, labeled below-right: "Equal FNR" (14px orange).
- **Center marker:** red `#e74c3c` "✖" (17px) at the hub, with red 12px lines below: "Impossible" / "(when base rates differ)".
- **Right-side annotation (left-aligned at x = w−180):** "You can satisfy" (15px `#1a5276`), "at most 2 of 3" (17px red), "(Chouldechova, 2017)" (13px gray `#666`); then "✓ Any two" (14px green) and "✗ All three" (14px red).

## Prediction ≠ Prescription

**Predicting What Will Happen Does Not Answer What Should We Do**

A model that predicts recidivism answers "will this person reoffend?" — it does not answer "should they be detained?" The second question requires values, not data.

- **The gap:** Even a perfect predictor cannot tell you the acceptable error rate for a decision.
- **What data omits:** It cannot say who bears the cost of mistakes, or what rights override predictions.
- **Example:** A 60% recidivism score — does that justify detention? What about 40%? The threshold is a moral choice.
- **Rule:** Predictions inform decisions; they do not make decisions in place of a human decision-maker.
- **Ownership:** The human must own the threshold that is chosen and the consequences that follow.

### Visualization (canvas `canvas5`, 720×300)

Scatter plot of individuals sorted by risk score, split by a decision threshold with a moral gray zone.

- **Layout:** margins left 80 / right 120 / top 30 / bottom 45; plot background `#f9f9f9`; `#333` axes; rotated y label "Risk Score", x label "Individuals (sorted by predicted risk)" (14px).
- **Threshold:** horizontal dashed red line (`#e74c3c`, dash 8/4, width 2) at 40% of plot height, labeled "Decision Threshold" (red 13px, right-aligned above the line). Zone above tinted `rgba(231,76,60,0.08)` and labeled "DETAIN" (red 12px, top-left); zone below tinted `rgba(39,174,96,0.08)` and labeled "RELEASE" (green 12px, bottom-left).
- **Points (5px radius; red `#e74c3c` if above threshold, green `#27ae60` if below), as (x,y) fractions of plot width/height:** (0.05,0.85), (0.1,0.78), (0.15,0.72), (0.2,0.65), (0.25,0.58), (0.3,0.52), (0.35,0.48), (0.38,0.42), (0.42,0.38), (0.45,0.35), (0.5,0.32), (0.55,0.28), (0.6,0.25), (0.65,0.22), (0.7,0.18), (0.75,0.15), (0.8,0.12), (0.85,0.1), (0.9,0.08), (0.95,0.05). Note: y is measured from the top, so points with y-fraction < 0.4 are the red "DETAIN" points.
- **Gray zone:** semi-transparent gray band `rgba(150,150,150,0.15)` spanning ±12% of plot height around the threshold; on the right side, an orange `#e67e22` bracket spanning the band with orange 13px three-line label: "Moral gray" / "zone: prediction" / "can't decide".
- **Y-axis scale (11px gray `#666`):** "100%" at top, "60%" at the threshold, "0%" at bottom.

## Regeneration instructions

- **Layout:** h1 + `.subtitle` + `.philosophy` callout, then one `h2` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by a `.obj-table` with a single `<tr>`: left `<td>` (45%) holds `.obj-title` (1.05em, weight 600, `#1a5276`), an intro paragraph, a `<ul>` of labeled bullets, and (in some sections) a closing **Signal**/**Implication** paragraph; right `<td>` (55%, centered) holds the canvas.
- **Table style:** full width, border-collapse; cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; even rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; paragraphs `#333` 0.95em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, light red fill `#fdecea`, gray text `#666`/`#999`/`#333`.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart (all 720×300); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
