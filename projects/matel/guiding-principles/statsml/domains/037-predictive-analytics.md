# Predictive Analytics Pitfalls

**Page type:** detail page (obj-table layout: one h2 + one-row table per pitfall, text left 50%, canvas right 50%)
**HTML title tag:** Predictive Analytics Pitfalls - Domain-Specific Statistical Issues

**Subtitle:** Common failure modes in enterprise ML deployment that practitioners encounter repeatedly

## Feature Leakage: #1 Cause of "Too Good to Be True"

**99% Accuracy Means a Feature Already Encodes the Answer**

- **The mechanism:** An input encodes the target directly, or via a proxy absent at prediction time.
- **Classic case:** "account_status=closed" predicts churn because the status IS the outcome.
- **More examples:** Discharge codes predicting admission diagnosis; future timestamps as features.
- **The check:** Ask whether each value exists at the exact moment the prediction must be made.
- **If it doesn't:** It's leakage — the chart's 99.2% collapses to 65.0% in production.

### Visualization (canvas `canvas1`, 500×300)

Bar chart of model accuracy with and without leaky features, plus a suspicion threshold line.

- **Background:** full-canvas fill `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Model Accuracy: Leakage vs Legitimate Signal" at (85, 25). Margins: top 50, right 30, bottom 55, left 60; baseline x-axis `#333` 1px.
- **Bars (y-scale 0–100%; bold 11px `#333` value above each; two-line 9px `#555` labels below):**
  - "With leakage (account_status)" — 99.2% — `#e74c3c`
  - "With leakage (future_flag)" — 97.5% — `#e67e22`
  - "Legitimate (all features)" — 78.5% — `#27ae60`
  - "After removing leaky features" — 71.2% — `#2980b9`
  - "Production (real performance)" — 65.0% — `#9b59b6`
- **Threshold line:** dashed [4,4] horizontal `#e74c3c` 1.5px at 95%, labeled bold 10px `#e74c3c` "> 95% = almost certainly leakage".

## Model Degradation Without Retraining

**8% Accuracy Lost Between January and June, Unnoticed**

- **The setup:** Deployed in January performing well; by June accuracy has dropped 8%.
- **Why unnoticed:** **No monitoring was in place**, so degradation left no visible trace.
- **What moved:** Distributions drifted, behavior changed, a competitor launched, seasonality shifted.
- **What didn't:** The model is frozen in January's reality with no alerting on performance.
- **The cost:** By the time anyone checks, months of degraded predictions have caused damage.
- **The general rule:** Every model has a shelf life; most teams don't know what theirs is.

### Visualization (canvas `canvas2`, 500×300)

Declining performance line over 12 months across shaded acceptable/warning/critical zones.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Model Performance Over Time (No Retraining)" at (100, 25). Margins: top 45, right 30, bottom 55, left 60. L-shaped axes `#333` 1px.
- **Data (monthly accuracy, y-scale 60–90%):** `[85, 84.5, 83, 82, 80, 78, 76, 74, 73, 71, 69, 67]` for Jan–Dec. Line `#2980b9`, 3px.
- **Zones (horizontal bands):** above 80% fill `#27ae6015` labeled 10px `#27ae60` "Acceptable"; 70–80% fill `#f39c1215` labeled `#f39c12` "Warning"; below 70% fill `#e74c3c15` labeled `#e74c3c` "Critical".
- **Annotation (bold 11px `#c0392b`, near the June point):** "← Nobody checked".
- **Axis labels:** month abbreviations Jan–Dec 10px `#888` on x; "60%"–"90%" ticks every 10 on y; 12px `#555` x-title "Months since deployment".

## Business Metric ≠ Model Metric

**AUC 0.85 Earning $340K Beats AUC 0.92 Earning $120K**

- **The divergence:** The model optimizes AUC; the business is paid in revenue.
- **The comparison:** AUC 0.85 catching high-value customers beats AUC 0.92 catching low-value ones.
- **Metric mismatch:** Produces technically excellent but commercially useless models.
- **The celebration:** The team ships AUC 0.88 → 0.91 and revenue does not move at all.
- **Why:** The new model only got better at classifying customers who don't matter.
- **The question to ask:** "If this metric improves by X, how much money does the business make?"

### Visualization (canvas `canvas3`, 500×300)

Paired horizontal bars: AUC comparison (left panel) vs revenue impact (right panel) for two models.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Model Metric vs Business Impact" at (140, 25). Margins: top 50, right 30, bottom 55, left 50.
- **Left panel, heading bold 12px `#333` "AUC (Model Metric)"; bars 40px tall, fill = model color + "88" alpha with 1.5px stroke, bold 13px labels inside:**
  - "Model A: 0.92" — `#e74c3c`
  - "Model B: 0.85" — `#27ae60`
  - Annotation bold 11px `#e74c3c` next to Model A's bar: '← "Winner"'.
- **Right panel (starting at 55% of plot width), heading bold 12px `#333` "Revenue Impact ($)"; scale max $400K:**
  - Model A: "$120K" — `#e74c3c`
  - Model B: "$340K" — `#27ae60`
  - Annotation bold 11px `#27ae60` next to Model B's bar: "← ACTUAL winner".
- **Bottom annotations (`#c0392b`):** bold 12px "Higher AUC ≠ More business value"; 11px "Model B captures fewer customers but higher-value ones".

## Stakeholder Cherry-Picking

**"+40% Conversion" Is One Segment of n=200; Overall Is +2%**

- **The move:** Results are mixed overall, so the best single segment goes to leadership.
- **The headline:** "Model improves conversion by 40%!" — one segment, one week, n=200.
- **The reality:** The overall effect across all segments is +2%.
- **What it is:** **Post-hoc subgroup analysis** with no multiple testing correction.
- **Why it happens:** More segments examined means more chance one looks extreme by luck.
- **The question to ask:** "How many segments did you look at before finding this one?"

### Visualization (canvas `canvas4`, 500×300)

Bar chart of per-segment lift around a zero line, with the cherry-picked segment highlighted.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Segment Performance: Cherry-Picking the Winner" at (90, 25). Margins: top 50, right 30, bottom 55, left 60; baseline x-axis plus a zero line at 60% of plot height (`#333` 1px).
- **Data (lift scale ±50% mapped to half plot height; bold 10px signed value labels; 9px `#555` segment label and "n=" caption below):**
  - Age 18-25 — +40% — n=200 — `#e74c3c`
  - Age 25-35 — +8% — n=800 — `#f39c12`
  - Age 35-45 — −3% — n=1200 — `#95a5a6`
  - Age 45-55 — +2% — n=900 — `#95a5a6`
  - Age 55+ — −5% — n=600 — `#95a5a6`
  - OVERALL — +2% — n=3700 — `#2980b9`
- **Highlight:** the first bar gets a 3px `#e74c3c` outline and two bold 10px `#e74c3c` caption lines above: "PRESENTED" / "TO LEADERSHIP".
- **Bottom annotation (bold 11px `#c0392b`):** '"Model improves conversion 40%!" — n=200, one segment'.

## Confidence Miscalibration

**"80% Chance of Churn" Where Only 55% Actually Churn**

- **The promise:** Say "80% chance" for 1,000 customers and ~800 should churn if calibrated.
- **The reality:** Miscalibrated, maybe only 500 do — the chart shows 80% landing at 55%.
- **The root cause:** **Probability outputs are not actual probabilities** without explicit calibration.
- **Which models:** Tree ensembles and neural nets rank correctly but miss true frequencies.
- **Where it bites:** Thresholds like "contact everyone above 70%" fail when 70% isn't 70%.
- **Operational gap:** Calibration is rarely checked and almost never maintained after deployment.

### Visualization (canvas `canvas5`, 500×300)

Calibration plot: overconfident model curve vs perfect-calibration diagonal, with the gap annotated at 80%.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Calibration Plot: Predicted vs Actual Probability" at (85, 25). Margins: top 50, right 30, bottom 55, left 60. L-shaped axes `#333` 1px.
- **Perfect calibration:** dashed [5,5] `#27ae60` 1.5px diagonal from bottom-left to top-right.
- **Model curve (line `#e74c3c` 2.5px with radius-4 dots):** predicted `[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]` vs actual `[0.15, 0.22, 0.28, 0.32, 0.38, 0.42, 0.48, 0.55, 0.62]`.
- **Gap annotation at predicted = 0.8:** vertical `#333` 1px segment from the diagonal (0.80) to the curve (0.55), labeled bold 11px `#333`: "Model says 80%" / "Reality: 55%".
- **Legend (top left):** `#27ae60` swatch "Perfect calibration"; `#e74c3c` swatch "Typical model (overconfident)" (11px `#333`).
- **Axis labels (12px `#555`):** x "Predicted Probability"; y (rotated) "Actual Frequency".

## Deployment Gap (Lab → Production)

**85% in the Notebook, 67% in Production — an 18-Point Fall**

- **The starting point:** Works in a Jupyter notebook on clean historical data.
- **Format drift:** Production sends a different data format than training ever saw.
- **Missing features:** That column simply does not exist in the real-time feed.
- **Encoding mismatch:** Training used one-hot; production sends plain integers.
- **Latency wall:** The model takes 2s while the SLA requires 100ms.
- **Edge cases:** Nulls, negative values, and Unicode never appeared in training.
- **The claim:** **"Works in lab" ≠ "works in prod."**
- **Why it matters:** That gap is not engineering overhead — it's where model value is destroyed.

### Visualization (canvas `canvas6`, 500×300)

Waterfall chart from notebook performance down to production performance.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Performance: Lab vs Production Reality" at (120, 25). Margins: top 50, right 20, bottom 40, left 40; scale max 90%, bars 70% of column width.
- **Stages (full bars show a bold 12px value; drop segments are fill color + "88" alpha with bold 11px `#c0392b` signed labels; two-line 9px `#555` names below):**
  - "Notebook (clean data)" — 85% full bar — `#27ae60`
  - "Missing features" — −5% drop — `#e74c3c`
  - "Encoding mismatch" — −3% drop — `#e74c3c`
  - "Latency constraints" — −4% drop — `#e74c3c`
  - "Edge cases (nulls, etc)" — −6% drop — `#e74c3c`
  - "Production (actual)" — 67% full bar — `#2980b9`
- **Top annotation (bold 13px `#c0392b`):** "18% gap: where value is destroyed".

## Interpretability Theater

**A Colorful SHAP Plot Nobody in the Room Can Read**

- **The ritual:** SHAP values get computed, plotted, and shown to stakeholders who nod.
- **The misuse:** "Feature importance" justifies decisions it does not actually explain.
- **Checkbox culture:** The explainability box is ticked without real understanding.
- **The core fact:** **SHAP shows marginal contribution, not causation.**
- **What "important" isn't:** It does not mean changing the feature would change the outcome.
- **The wrong leap:** "Age is important" becomes "target older people" — SHAP never said that.
- **The verdict:** Interpretability without statistical literacy is theater.

### Visualization (canvas `canvas7`, 500×300)

Split panel: SHAP-style bar plot on the left, mistaken stakeholder conclusions on the right.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "SHAP Values: What Stakeholders Think vs Reality" at (85, 25). Margins: top 50, right 20, bottom 40, left 40.
- **Left panel (45% of plot width), heading bold 11px `#333` "What's shown:"; centered zero axis; bars 25px tall (positive right in `#e74c3c88`, negative left in `#2980b988`), feature name 11px left, signed SHAP value 11px at bar end (scale max 0.2):**
  - Age +0.15, Income +0.12, Tenure −0.08, Usage +0.06, Location −0.04.
- **Right panel (from 55%), heading bold 11px `#333` "What stakeholders conclude:"; lines at 22px spacing — wrong conclusions bold 11px `#e74c3c` prefixed "✗ ", correct notes 11px `#555`:**
  - ✗ "Target older customers"
  - ✗ "Higher income = more churn"
  - ✗ "Tenure prevents churn"
  - (blank line)
  - What SHAP actually says:
  - "Age is correlated with
  - prediction, but NOT causal"
- **Bottom annotation (bold 12px `#c0392b`):** "SHAP ≠ Causation. Feature importance ≠ actionable insight."

## Premature Productionization

**A Demo on n=500 Does Not Survive n=50K**

- **The demo:** A POC with n=500 and 3 features shows impressive results on stage.
- **The demand:** The stakeholder wants it in production immediately.
- **Why it breaks:** The POC was a **lucky subset with unusually high signal-to-noise**.
- **The curated set:** Clean data, complete features, recent customers only.
- **The real set:** Messy, incomplete, with populations the POC never saw.
- **What gets skipped:** "Just deploy it" pressure cuts validation, stability, and edge cases.
- **The pattern:** Most ML project failures trace back to premature productionization.

### Visualization (canvas `canvas8`, 500×300)

Two accuracy distributions: narrow high POC curve vs wide lower production curve.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "POC Performance vs Production Reality" at (120, 25). Margins: top 50, right 30, bottom 50, left 60. L-shaped axes `#333` 1px; x-domain 40–100%.
- **Distributions (Gaussian curves 2.5px, each with a dashed [3,3] vertical mean marker and bold 11px label in its color):**
  - "POC (n=500)": mean 82, std 4, `#27ae60`.
  - "Production (n=50K)": mean 65, std 8, `#e74c3c`.
- **X-axis:** 11px `#888` tick labels "50%"–"90%" every 10; 12px `#555` title "Model Accuracy".
- **Explanation box (fill `#fff3cd`, right side; 10px `#333`, four lines):** "POC: curated data, clean" / "features, recent customers" / "Production: messy, incomplete," / "diverse populations".

## Regeneration instructions

- **Layout:** standard domains detail page. h1, `.subtitle` paragraph, then per pitfall an `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border, padding-bottom 8px) followed by a one-row `.obj-table`: left `<td>` (40%) holds a `.obj-title` punchline plus a `<ul>` of labeled `<li>` bullets (`<strong>Label:</strong> phrase`, each fitting one line), right `<td>` (60%, centered) holds the canvas. Even table rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `ul { margin: 8px 0 8px 20px; font-size: 0.9em; color: #333; }` and `li { margin: 4px 0; }`; `strong` `#1a5276`; `.philosophy` callout style available (background `#f0f4f8`, left border 4px `#2980b9`) but unused. No nav bar, no back/home links.
- **Canvases:** each declared `<canvas id="canvasN" width="500" height="300">`; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) at 500×300px, and calls `ctx.scale` so drawing stays in logical coordinates, and sets base font 17px system sans-serif. Each chart is an IIFE painting on a `#f0f4f8` background.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark red `#c0392b`, secondary blue `#2980b9`, purple `#9b59b6`, amber `#f39c12`, neutral gray `#95a5a6`, text grays `#555`/`#333`/`#888`, warning fill `#fff3cd`.
