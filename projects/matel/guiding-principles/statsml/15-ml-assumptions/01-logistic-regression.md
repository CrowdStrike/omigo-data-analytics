# Logistic Regression

**Page type:** detail page (single obj-table, one row per assumption: text left 42%, canvas right 58%)
**HTML title tag:** Logistic Regression - ML Assumptions

**Subtitle:** Each feature must contribute linearly to the log-odds. Violations are silent — the model simply ignores signal it cannot represent.

## What It Does

Models the probability of a binary outcome by applying a sigmoid function to a linear combination of features. Maps any real-valued input to the [0,1] probability range via log(p/(1-p)) = β₀ + β₁x₁ + ... + βₖxₖ.

**Best For:** Credit scoring, medical diagnosis (disease present/absent), customer churn prediction

**Data:** Binary target variable, numeric or encoded categorical features, moderate sample sizes (EPV ≥ 10 in minority class)

### Visualization (canvas `c0`, 720×300)

Sigmoid fit over noisy binary data.

- **Title (bold 14px `#1a5276`, top center):** "Logistic Regression: Sigmoid Fit on Noisy Binary Data".
- **Axes:** `#bbb` L-shaped axes, left pad 70; x label "Feature value (x)" bottom center, y label "P(y = 1)" rotated left; x range -5 to 5 (ticks at -4, -2, 0, 2, 4), y range 0–1 (tick labels "0", "1", plus "0.5").
- **Threshold line:** horizontal dashed gray `#888` (dash 4/3) at P=0.5, labeled "0.5" left of axis.
- **Sigmoid curve:** `#1a5276`, width 3: p = 1/(1+e^(-1.2t)) for t in [-5, 5].
- **Class 0 dots (red `rgba(231,76,60,0.8)`, radius 5, at y≈0.03):** x = -4.2, -3.8, -3.5, -3.1, -2.8, -2.4, -2.0, -1.6, -1.3, -0.9, -0.5, -0.2, 0.3, 0.8, 1.5.
- **Class 1 dots (green `rgba(39,174,96,0.8)`, radius 5, at y≈0.97):** x = 4.5, 4.0, 3.6, 3.2, 2.8, 2.3, 1.9, 1.4, 1.0, 0.6, 0.1, -0.4, -0.8, -1.2.
- **Uncertain zone:** band from x=-1.5 to x=1.5 filled `rgba(230,126,34,0.12)`, labeled "Uncertain zone" in 13px `#e67e22` at top center.
- **Legend (top right):** red dot "y = 0", green dot "y = 1"; below, bold 13px `#1a5276` formula "σ(z) = 1/(1+e⁻ᶻ)".

## Linear Log-Odds

Each feature must have a **linear relationship with the log-odds** of the outcome. The model fits: log(p/(1-p)) = β₀ + β₁x₁ + ... If the true relationship is U-shaped, threshold-based, or locally concentrated, the linear coefficient averages across all regions — diluting or hiding the real signal entirely.

**Breaks:** A feature with strong predictive power in a specific range (e.g., cholesterol 240-320) gets assigned a near-zero coefficient because outside that range the relationship is flat or reversed.

**Verify:** Box-Tidwell test, plot log-odds vs. feature

**Fix:** Binned features, splines, GAM, or range-based binary encoding

### Visualization (canvas `c1`, 720×300)

Scatter of a non-linear (range-concentrated) relationship with a flat logistic-regression fit that misses it.

- **Axes:** `#bbb`, left pad 55; x label "Cholesterol" (range 150–430, ticks at 150, 200, 240, 320, 430), y label "P(disease)" rotated left.
- **Data:** 45 points at x = 150 + i·6.2 with piecewise probability: x<200 → p≈0.15 ± sine noise 0.05; 200≤x<240 → p rising 0.2→0.55 ± 0.05; 240≤x<320 → p≈0.72 ± 0.08; x≥320 → p≈0.38 declining ± 0.05 (all clamped to [0.05, 0.95]). Dot radius 4.5; fill green `rgba(39,174,96,0.7)` where p>0.5, else red `rgba(231,76,60,0.65)`.
- **Highlight zone:** green box over x=240–320, fill `rgba(39,174,96,0.08)` with dashed `#27ae60` outline (dash 3/3, width 1.5); bold 13px green label above: "76% positive (hidden from LR)".
- **LR fit line:** dashed red `#e74c3c` (dash 8/5, width 3), nearly flat from (150, p=0.37) to (430, p=0.47); 13px red label "LR fit: β=0.02 — "not significant"".

## No Multicollinearity

When features are highly correlated, the model cannot separate their individual effects. Coefficients become **unstable** — small changes in data cause them to flip sign, explode in magnitude, or swap importance rankings. The model still fits, but the coefficients are uninterpretable and unreliable.

**Breaks:** Two correlated features (e.g., systolic + diastolic BP) produce coefficients like +12.3 and -11.8 — individually meaningless, collectively fragile. Drop one observation and both flip.

**Verify:** VIF < 5 for each predictor, correlation matrix

**Fix:** Drop one of correlated pair, PCA, ridge regularization (L2)

### Visualization (canvas `c2`, 720×300)

Correlated scatter cloud with a bootstrap-coefficient instability table.

- **Axes:** `#bbb`, left pad 55; x label "Systolic BP" (range 100–180, ticks at 110, 130, 150, 170), y label "Diastolic BP" (range 60–110, ticks at 70, 80, 90, 100).
- **Data cloud:** 60 blue `rgba(26,82,118,0.55)` dots (radius 4): sys = 110 + i·1.1 + 8·sin(1.7i), dia = 65 + 0.55·(sys−110) + 4·sin(2.3i) — a tight positive-correlation band.
- **Correlation line:** solid `#1a5276` width 2 from (105, 63) to (175, 99).
- **Annotation block (top right):** bold 13px `#1a5276` heading "Coefficients across 5 bootstrap samples:", then five rows (13px; sample label gray `#555`, β_sys red `#e74c3c`, β_dia blue `#2980b9`):
  - Sample 1: β_sys=+8.2, β_dia=-7.5
  - Sample 2: β_sys=-3.1, β_dia=+4.0
  - Sample 3: β_sys=+12.3, β_dia=-11.8
  - Sample 4: β_sys=+0.4, β_dia=+0.6
  - Sample 5: β_sys=-6.7, β_dia=+7.9
- **Takeaway (bold 13px red):** "r = 0.92 → signs flip randomly".

## Independence of Observations

Each row must be an **independent draw** from the population. If observations are clustered (patients within hospitals), repeated (same patient measured multiple times), or temporally correlated (time series), the model underestimates standard errors — making insignificant effects appear significant.

**Breaks:** 100 patients × 5 visits each = model thinks n=500 with tight standard errors. Real effective sample size ≈ 100. P-values are falsely small, confidence intervals too narrow.

**Verify:** Study design review, Durbin-Watson for temporal autocorrelation

**Fix:** Mixed-effects logistic regression, GEE, cluster-robust standard errors

### Visualization (canvas `c3`, 720×300)

Cluster diagram: four hospital clusters of correlated points, with a model-vs-reality annotation.

- **Clusters (dashed-outline ellipses 70×45, each with 12 dots of its border color placed around the center):**
  - Hospital A — center (160, 100), fill `rgba(231,76,60,0.55)`, border `#e74c3c`
  - Hospital B — center (320, 150), fill `rgba(41,128,185,0.3)`, border `#2980b9`
  - Hospital C — center (490, 110), fill `rgba(39,174,96,0.55)`, border `#27ae60`
  - Hospital D — center (240, 220), fill `rgba(142,68,173,0.3)`, border `#8e44ad`
  - Each labeled below its ellipse in bold 13px of its border color.
- **Annotation block (top right):** bold blue `#1a5276` "Model thinks:" with gray `#555` lines "n = 48 independent obs" and "SE = 0.03, p < 0.001"; bold red `#e74c3c` "Reality:" with gray lines "4 clusters × 12 patients", "Effective n ≈ 4–12", "True SE = 0.14, p = 0.22"; bold red conclusion "→ False significance!".
- **Bottom label (13px `#555`, centered):** "Within-cluster correlation inflates apparent sample size".

## Sufficient Sample Size (Events per Variable)

Logistic regression needs **10–20 events per predictor** in the minority class. With 5 features, you need at least 50–100 positive cases. Below this, the model overfits — coefficients are estimated from too few events and don't generalize. Maximum likelihood estimation may not converge or produces infinite coefficients.

**Breaks:** 8 features with only 30 positive cases (EPV = 3.75). Model fits perfectly on training data but predicts randomly on new data. Separation problems produce β → ∞.

**Verify:** EPV ≥ 10 minimum, check for complete/quasi-complete separation

**Fix:** Reduce features, Firth's penalized likelihood, collect more events

### Visualization (canvas `c4`, 720×300)

Train-vs-test AUC curves over EPV with danger/safe zones.

- **Axes:** `#bbb`, left pad 55; x label "Events per Variable (EPV)" (range 0–50, ticks at 5, 10, 15, 20, 30, 40, 50), y label "Model Performance (AUC)" (range 0.45–0.95, ticks 0.5–0.9 by 0.1).
- **Training AUC line (red `#e74c3c`, width 2.5):** (2, 0.95), (5, 0.92), (8, 0.88), (10, 0.85), (15, 0.82), (20, 0.80), (30, 0.78), (40, 0.77), (50, 0.76).
- **Test AUC line (blue `#2980b9`, width 2.5):** (2, 0.52), (5, 0.58), (8, 0.65), (10, 0.70), (15, 0.74), (20, 0.76), (30, 0.77), (40, 0.77), (50, 0.76).
- **Zones:** EPV<10 shaded `rgba(231,76,60,0.06)` with dashed red boundary at EPV=10; EPV≥10 shaded `rgba(39,174,96,0.06)`. Bold 13px zone labels: red "DANGER: EPV < 10", green "SAFE: EPV ≥ 10".
- **Legend (top right):** red line "Training AUC (overfit)"; blue line "Test AUC (reality)".
- **Gap annotation (bold 13px `#e67e22`, near EPV=5):** "← gap = overfitting".

## No Extreme Outliers / High-Leverage Points

A single extreme observation can **dominate the entire coefficient** for that feature. Unlike linear regression which has well-known diagnostics, logistic regression outliers are harder to spot — the model silently warps around the extreme point, distorting predictions for the entire population.

**Breaks:** One patient with cholesterol = 900 (data entry error) pulls the sigmoid far right. The model now predicts everyone below 400 as low-risk, even though the true threshold is 240.

**Verify:** Cook's distance, DFBETAS, leverage (hat) values

**Fix:** Winsorize, robust regression, or remove after confirming data error

### Visualization (canvas `c5`, 720×300)

True vs outlier-distorted sigmoid on a wide cholesterol axis.

- **Axes:** `#bbb`, left pad 55; x label "Cholesterol" (range 100–950, ticks at 150, 240, 350, 500, 700, 900), y label "P(disease)" (range 0–1).
- **Normal data:** 35 blue `rgba(26,82,118,0.6)` dots (radius 4) at x = 160 + i·5.5 ± sine jitter, p following sigmoid 1/(1+e^(−(x−240)/30)) ± 0.08 noise, clamped [0.05, 0.95].
- **Outlier:** red `#e74c3c` filled dot (radius 8) with a red ring (radius 13) at (900, 0.85); bold 13px red labels "Outlier: 900" above and "(data entry error)" below.
- **True sigmoid (green `#27ae60`, width 2.5, solid):** 1/(1+e^(−(x−240)/30)) for x 100–400.
- **Distorted sigmoid (red `#e74c3c`, width 2.5, dashed 6/4):** 1/(1+e^(−(x−420)/120)) for x 100–950.
- **Legend (top left):** green solid line "True sigmoid (without outlier)"; red dashed line "Distorted sigmoid (with outlier)".
- **Impact annotation (bold 13px `#e67e22`, near x=350, p=0.25):** "Threshold shifts 240 → 420".

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, then a single `.obj-table` (full width, border-collapse) with one `<tr>` per section (no thead): left `<td>` (42%) holds `.obj-title` div, `.obj-desc` paragraph, and `.obj-detail` lines; right `<td>` (58%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Text markup:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses pill `<span class="tag tag-check">` (background `#eafaf1`, text `#1e8449`); "Fix:" uses pill `<span class="tag tag-fix">` (background `#fef9e7`, text `#b7950b`); tags are 0.75em, padding 2px 8px, radius 4px. Also defined: `.good` green `#27ae60`, `.warn` orange `#e67e22`, `.tag-break` background `#fdeaea` text `#c0392b`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#555`; `strong` `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width="720" height="300"` per chart; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, mid blue `#2980b9`, purple `#8e44ad`, point fill `rgba(26,82,118,0.55)`, gray text `#555`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
