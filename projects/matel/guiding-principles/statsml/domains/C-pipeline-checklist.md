# Pipeline Stage Checklist

**Page type:** other (single-page long doc: h2 per pipeline stage, each with one red gate-check box of checkbox questions, followed by one canvas; closing philosophy callout)
**HTML title tag:** Pipeline Stage Checklist

**Subtitle:** For each pipeline stage: the questions you MUST answer before proceeding. Derived from real domain failures. If any answer is "no" or "don't know" — STOP.

## Stage 1: Data Collection & Ingestion

**GATE CHECK — BEFORE USING THIS DATA**

- ☐ **Do I know WHO is NOT in this dataset? (What population is excluded?)**
  - Fail: If no → survivorship bias. You're analyzing survivors, not the population.
  - Domains: Healthcare: only tested patients. Courts: only litigated cases. E-commerce: only converted users.
- ☐ **For every timestamp: do I know the timezone, granularity (sec/ms/μs), and whether it's UTC?**
  - Fail: If no → potential 1-8 hour offsets or 1000× scale errors from ms/s confusion.
  - Domains: cloud provider multi-region. Cross-service joins. Unix epoch ambiguity.
- ☐ **If multiple sources feed the same table: are semantics identical across all sources?**
  - Fail: If no → same column name, different meaning. "Amount" in dollars vs cents vs local currency.
  - Domains: Multi-hospital lab data. Multi-region billing. Cross-platform analytics.
- ☐ **Are there duplicates from retries, fanout joins, or ETL re-runs?**
  - Fail: If yes and undetected → inflated counts, wrong averages, bogus significance.
  - Domains: Event pipelines. Payment retries. 1-to-many joins.
- ☐ **Do I know the data's latency? How old is the "freshest" record?**
  - Fail: If unknown → you may be analyzing stale data from hours/days ago without realizing.
  - Domains: SEC filings (60-day lag). Feature store staleness. Billing lag (30+ days).

### Visualization (canvas `pc1`, 720×180 as drawn; HTML attribute 720×280)

Flow diagram: three sources with different units merging into one table.

- **Title (bold 17px `#1a5276`):** "Data Collection: Semantic Mismatch Across Sources".
- **Source boxes** (120×40 filled rects with white 17px labels, value captions below in 14px of the box color):
  - Source A — green `#27ae60` at (30, 40) — "amount = $150.00"
  - Source B — orange `#e67e22` at (30, 110) — "amount = 15000¢"
  - Source C — red `#e74c3c` at (200, 75) — "amount = ¥22500"
- **Arrows:** 2px arrows in each source's color converging on the merged table.
- **Merged table:** solid `#1a5276` rect (420, 55, 160×70) with white text: bold "Merged Table" / "150, 15000, 22500" / "Same column name!" (14px).
- **Warning (bold 17px `#e74c3c`, right):** "100x error".

## Stage 2: Profiling & Type Detection

**GATE CHECK — BEFORE PROFILING**

- ☐ **For columns with missing values: is the missingness informative (MNAR) or random (MCAR)?**
  - Fail: If MNAR and you impute with mean → you destroy the signal that absence carries.
  - Domains: Healthcare: "lab not ordered" = healthy. Cybersecurity: "no log" = deleted. Survey: "no response" = unhappy.
- ☐ **Do sub-populations in my data have different reference ranges for the same feature?**
  - Fail: If yes and you profile globally → universal thresholds misclassify sub-populations.
  - Domains: Healthcare: hemoglobin by age/sex/ethnicity. Education: test scores by developmental stage.
- ☐ **Has the column's MEANING changed over time? (Same name, different semantics after a migration.)**
  - Fail: If yes and undetected → all analysis on post-change data is wrong, no error thrown.
  - Domains: Revenue gross→net. Status codes renumbered. Units changed (USD→cents).
- ☐ **Are there point masses (spike at zero, cap at max) that need separate treatment?**
  - Fail: If ignored → spike dominates all statistics. Mean is meaningless. Histogram is useless.
  - Domains: Capital gain (95% zero). 2nd floor SF. Insurance claims. Sensor saturation.
- ☐ **What's the meta-distribution Gini? If >0.7: is adaptive bucketing planned?**
  - Fail: If high Gini with equal-width bins → 80% of data in one bin, rest empty. Useless.
  - Domains: Income. Hospital charges. File sizes. Any power-law feature.

### Visualization (canvas `pc2`, 720×180 as drawn; HTML attribute 720×280)

Side-by-side histograms: misleading global profile vs correct per-subpopulation profiles.

- **Title (bold 17px `#1a5276`):** "Profiling: Global vs Sub-Population Distributions".
- **Left (label 14px `#e74c3c`: "Global Profile (MISLEADING)"):** 11 bell-curve bars, heights `[15, 30, 55, 70, 80, 85, 80, 70, 55, 30, 15]` × 0.9, fill `rgba(231,76,60,0.4)`, stroke `#e74c3c`, bar width 22, spacing 25, baseline y=160; dashed red horizontal line at y=90 (dash 5/3) labeled in 13px red: "universal threshold".
- **Right (label 14px `#27ae60`: "Per-Subpopulation (CORRECT)"):** two overlapping histograms with heights × 0.7 — Pop A (lower range): `[25, 55, 80, 60, 30, 10, 0, 0, 0, 0]`, fill `rgba(39,174,96,0.5)`, stroke `#27ae60`; Pop B (higher range): `[0, 0, 0, 0, 10, 30, 60, 80, 55, 25]`, fill `rgba(26,82,118,0.5)`, stroke `#1a5276`. Labels below (13px): "Pop A" (green), "Pop B" (blue).

## Stage 3: Shape Detection

**GATE CHECK — BEFORE CLASSIFYING SHAPE**

- ☐ **Is n > 100 for this feature? (Below this: shape classification is unreliable.)**
  - Fail: If no → report "insufficient for shape" and use non-parametric methods. Don't guess.
  - Domains: Sports: 17 games/season. Rare diseases: 50 patients. New product: 30 purchases.
- ☐ **Have I run at multiple bin resolutions? Does the shape persist across scales?**
  - Fail: If shape only visible at one resolution → it's an artifact, not real structure.
  - Domains: Apparent bimodality that vanishes at different bin width. Noise peaks. Rounding artifacts.
- ☐ **Am I mixing sub-populations that should be profiled separately?**
  - Fail: If yes → artificial multimodality. "Bimodal" is actually two populations with different means.
  - Domains: Male+female hemoglobin. Summer+winter energy. Pre+post migration data.
- ☐ **Is this feature stationary over the time range of my data?**
  - Fail: If no → the "shape" you detected is an average across different regimes. Not real.
  - Domains: Revenue pre/post COVID. User behavior pre/post feature launch. Seasonal features.

### Visualization (canvas `pc3`, 720×180 as drawn; HTML attribute 720×280)

Three histograms of the same data at increasing bin counts, separated by large question marks.

- **Title (bold 17px `#1a5276`):** "Shape Detection: Same Data at 3 Resolutions".
- **5 bins** (label 13px `#1a5276`): heights `[25, 60, 90, 55, 20]`, fill `rgba(26,82,118,0.6)`, stroke `#1a5276`, bar width 36 spacing 40 from x=20, baseline y=160; caption below: "Unimodal?".
- **10 bins** (label 13px `#e67e22`): heights `[15, 40, 55, 35, 15, 10, 30, 60, 45, 12]` × 1.3, fill `rgba(230,126,34,0.6)`, stroke `#e67e22`, bar width 19 spacing 22 from x=250; caption: "Bimodal?".
- **20 bins** (label 13px `#e74c3c`): heights `[8, 20, 30, 45, 38, 50, 30, 18, 12, 8, 5, 10, 25, 40, 55, 48, 35, 20, 10, 5]` × 1.5, fill `rgba(231,76,60,0.5)`, stroke `#e74c3c`, bar width 9 spacing 11 from x=500; caption: "Noise?".
- **Separators:** bold 30px `#1a5276` "?" at (235, 110) and (480, 110).

## Stage 4: Statistical Testing

**GATE CHECK — BEFORE CLAIMING SIGNIFICANCE**

- ☐ **How many tests am I running total? Have I adjusted for multiple comparisons?**
  - Fail: If 100 features × 10 buckets = 1000 tests at p<0.05 → expect 50 false positives. Must correct.
  - Domains: Genomics (20K genes). Feature selection. Multi-bucket enrichment testing.
- ☐ **Is the enrichment PRACTICALLY significant, not just statistically significant?**
  - Fail: At large n, trivial effects (1.006×) become "significant." But useless for classification.
  - Domains: Large datasets where everything is "significant." Clinical trials with negligible effects.
- ☐ **Is the CI lower bound still above the decision threshold?**
  - Fail: If CI includes base rate → point estimate is meaningless. Can't trust the bucket.
  - Domains: Small n buckets with wide CIs. "85% purity" with CI [55%, 97%].
- ☐ **Am I comparing to the correct BASE RATE, not to 50%?**
  - Fail: If base is 5%: a bucket at 10% is 2× enrichment (strong!). Comparing to 50%: "only 10%" = dismissed.
  - Domains: Fraud (0.1% base). Rare disease (1% base). High-value customer (5% base).
- ☐ **Is my test set TRULY independent of all training decisions?**
  - Fail: If test set influenced: feature selection, threshold tuning, or hyperparameters → it's NOT independent.
  - Domains: Test set evaluated 50× (overfit). Features selected on full data. Threshold tuned on test.

### Visualization (canvas `pc4`, 720×180 as drawn; HTML attribute 720×280)

Dot-grid of 1000 tests with 50 highlighted false positives.

- **Title (bold 17px `#1a5276`):** "Multiple Testing: 1000 Tests at p<0.05 = 50 False Positives".
- **Grid:** 40 columns × 25 rows of 2.5px-radius dots starting at (30, 35), spacing 16.5 × 5.5. 50 deterministically spread dots (indices `i*20 + (i%7)*3` for i = 0..49) are red `#e74c3c`; the rest are light gray `#ccd1d9`.
- **Legend (right):** gray dot — "True null" (14px `#333`); red dot — "False positive" (14px red).
- **Note (13px `#1a5276`, right, four lines):** "50 red dots =" / "expected false" / "discoveries at" / bold "alpha = 0.05".

## Stage 5: Feature Engineering

**GATE CHECK — BEFORE USING THIS FEATURE**

- ☐ **Would this feature ACTUALLY be available at prediction time in production?**
  - Fail: If no → leakage. Feature computed from future events. "Collection calls made" predicts default because it FOLLOWS default.
  - Domains: Any derived feature using future data. Treatment-as-feature. Post-event aggregates.
- ☐ **Does this feature encode a protected attribute through a proxy?**
  - Fail: If yes (zip code ↔ race, name ↔ gender) → model discriminates despite removing the protected attribute.
  - Domains: Hiring models. Credit scoring. Insurance pricing. Predictive policing.
- ☐ **Is this feature computed the SAME WAY in training (batch) and serving (real-time)?**
  - Fail: If different (batch SQL vs streaming compute) → training-serving skew. Subtle numerical differences degrade predictions.
  - Domains: Feature stores. Real-time vs batch aggregation. Rolling windows with different boundaries.
- ☐ **If this feature is based on my own system's output: have I debiased it?**
  - Fail: If not → feedback loop. CTR reflects position bias, not item quality. View count reflects ranking, not demand.
  - Domains: Click-through rate. View count. Recommendation-driven engagement.

### Visualization (canvas `pc5`, 720×180 as drawn; HTML attribute 720×280)

Timeline with past/future zones and a leaking feature arrow crossing the prediction boundary.

- **Title (bold 17px `#1a5276`):** "Feature Engineering: Temporal Leakage".
- **Timeline:** `#1a5276` 3px horizontal line at y=100 from x=40 to x=680; tick marks with 13px labels: "t-3", "t-2", "t-1", "Prediction", "t+1", "t+2", "t+3" at x = 80, 170, 260, 360, 460, 550, 640.
- **Boundary:** dashed red `#e74c3c` 3px vertical line (dash 6/4) at x=360 from y=40 to y=160.
- **Zones:** left `rgba(39,174,96,0.15)` rect (40, 40, 320×120) labeled in 14px green: "Valid features (past)"; right `rgba(231,76,60,0.1)` rect (360, 40, 320×120) labeled in 14px red: "FUTURE (unavailable)".
- **Leak arrow:** red 2.5px arrow from (500, 75) leftward to (300, 75), with a bold 28px red "X" where it crosses the boundary.
- **Caption (13px red, bottom right):** "LEAKAGE: feature uses future data".

## Stage 6: Model Training & Evaluation

**GATE CHECK — BEFORE TRUSTING RESULTS**

- ☐ **Is train/test split at the ENTITY level, not the ROW level?**
  - Fail: If same patient/user in both → model memorizes entities, not patterns. Accuracy inflated 10-20%.
  - Domains: Patient visits. User sessions. Multi-row entities. Time-series forecasting.
- ☐ **For time-ordered data: is the split temporal (train on past, test on future)?**
  - Fail: If random split → model uses future info. Accuracy inflated by 10-30%.
  - Domains: Any financial data. User behavior. Sensor data. Event streams.
- ☐ **Is my model complexity appropriate for my data size?**
  - Fail: 1M parameters on 500 rows = memorization. Neural net on n=200: logistic regression will win.
  - Domains: Small medical datasets. Rare event prediction. Low-resource NLP.
- ☐ **Have I evaluated on the RARE class specifically, not just overall accuracy?**
  - Fail: 95% accuracy on 95/5 data = predicting majority always. Recall on minority = 0%.
  - Domains: Fraud. Disease. Cybersecurity attacks. Any imbalanced problem.
- ☐ **How many times have I evaluated on this test set? (>10 = burned)**
  - Fail: Each evaluation leaks test info into decisions. After 50 rounds: effectively training on test set.
  - Domains: Kaggle leaderboard overfitting. Research paper p-hacking. Iterative model development.

### Visualization (canvas `pc6`, 720×180 as drawn; HTML attribute 720×280)

Side-by-side split diagrams: row-level split with entity leak vs entity-level split.

- **Title (bold 17px `#1a5276`):** "Training/Evaluation: Entity Leakage in Splits".
- **Left ("WRONG: Row-level split", bold 14px `#e74c3c`):** two red-bordered boxes (fill `rgba(231,76,60,0.1)`, 140×110) labeled "TRAIN" and "TEST" (12px `#1a5276`); each holds four 10px patient circles with white 10px IDs — TRAIN: P1 (`#e74c3c`), P2 (`#27ae60`), P1 (`#e74c3c`), P3 (`#e67e22`); TEST: P1, P4 (`#1a5276`), P2, P1. Caption (bold 12px red): "P1 in BOTH = leak!".
- **Right ("CORRECT: Entity-level split", bold 14px `#27ae60`):** two green-bordered boxes (fill `rgba(39,174,96,0.1)`) — TRAIN: P1, P1, P2, P2; TEST: P3, P3, P4, P4 (same color coding per patient). Caption (bold 12px green): "No entity overlap!".

## Stage 7: Deployment & Monitoring

**GATE CHECK — BEFORE AND AFTER DEPLOYMENT**

- ☐ **Do I have data validation checks that alert on distribution shifts BEFORE predictions are served?**
  - Fail: If no → silent schema change (dollars→cents) produces wrong predictions for weeks before anyone notices.
  - Domains: Upstream migrations. Unit changes. New category values. NULL rate changes.
- ☐ **Am I monitoring CORRECTNESS, not just availability/latency?**
  - Fail: System serves wrong answers fast and confidently. Dashboard says "healthy" while predictions are garbage.
  - Domains: Stale caches. Feature store failures. Model serving outdated version.
- ☐ **Is there a mechanism to detect when the model's assumptions have been violated in production?**
  - Fail: If no → model degrades silently over months. Blamed on "model staleness" when actual cause is upstream data drift.
  - Domains: Concept drift. Population shift. Seasonal patterns not in training. Policy changes.
- ☐ **For adversarial domains: when was the model last retrained? (>30 days in security = stale)**
  - Fail: Adversaries adapt in days. A 6-month-old model in cybersecurity/fraud/spam is already significantly evaded.
  - Domains: Spam filters. Fraud detection. Content moderation. Any domain with active adversaries.
- ☐ **Do I have a kill switch to fall back to a simple/previous model if the new one misbehaves?**
  - Fail: If no → a bad model in production causes damage until the next full deployment cycle (hours/days).
  - Domains: Canary deployment. Feature flags. Shadow mode. A/B test with abort criteria.

### Visualization (canvas `pc7`, 720×180 as drawn; HTML attribute 720×280)

Drift-monitoring line chart with an alert threshold and alert box.

- **Title (bold 17px `#1a5276`):** "Deployment: Distribution Shift Detection Dashboard".
- **Axes:** `#1a5276` 2px — x from (50, 140) to (550, 140) labeled "Time"; y from (50, 35) to (50, 140) labeled (12px, two lines): "Distribution" / "Distance".
- **Stable segment:** green `#27ae60` 2.5px line from (60, 120) with y-values `[120, 118, 122, 119, 121, 120, 117, 123, 120, 118, 121, 119]` every 25px; label (12px green): "Stable".
- **Drift segment:** red `#e74c3c` 2.5px line rising from (360, 118) with y-values `[110, 95, 78, 60, 48, 42, 38]` every 25px; caption below (12px red): "Schema change ($ to cents)".
- **Threshold:** dashed orange `#e67e22` 2px horizontal (dash 5/4) at y=80, labeled (13px orange): "Alert Threshold".
- **Alert point:** red 8px circle at (435, 78) with white bold "!"; red arrow to an alert box — `rgba(231,76,60,0.1)` fill, `#e74c3c` 2px border (570, 35, 140×70), containing bold 14px red "ALERT" and 12px lines: "Drift detected!" / "Block predictions" / "until investigated".

## Closing callout (philosophy box)

**How to use this document:** Print it. Before each pipeline stage, read the gate checks. If you can't answer YES to all questions: STOP and investigate. Each "no" represents a domain failure that has burned real teams with real consequences. The questions are simple. The consequences of skipping them are not.

## Regeneration instructions

- **Layout:** single long page: h1, `.subtitle`, then 7 `<h2>` stage sections. Each section has one `.gate` box followed by one standalone `<canvas>` (`width="720" height="280"` attributes, inline style `display:block; margin:10px auto;`). A `.philosophy` callout ends the page. No nav, no cross-links.
- **Gate box structure:** `.gate` — white background, border `2px solid #e74c3c`, radius 8px, padding 16px 20px. Inside, repeated triplets: `.stage` (the gate heading — 0.8em, `#e74c3c`, uppercase, letter-spacing 1px, weight 700; appears once at top), `.question` (0.95em, `#1a5276`, weight 700, 20px left padding with a `□` checkbox glyph in `#e74c3c` via `::before`), `.fail` (0.85em, `#c0392b`, indented 20px), `.domain-ref` (0.8em, `#999`, indented 20px).
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with 2px solid `#2980b9` bottom border, margin 35px 0 12px; subtitle `#666` 1.05em; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em.
- **Canvas:** a shared `setupCanvas(id, w, h)` helper resizes each canvas to 720×180 CSS pixels (overriding the 280px height attribute) and scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); default font 17px system sans-serif; a shared `drawArrow(ctx, x1, y1, x2, y2, color)` helper draws 2px arrows with filled heads.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, fail text `#c0392b`, gray text `#999`/`#666`/`#333`, light gray dots `#ccd1d9`.
