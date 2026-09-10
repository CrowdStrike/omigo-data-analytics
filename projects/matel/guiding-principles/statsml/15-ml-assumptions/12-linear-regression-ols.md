# Linear Regression (OLS)

**Page type:** detail page (two-column obj-table layout: text left 42%, canvas right 58%, one row per assumption)
**HTML title tag:** Linear Regression - ML Assumptions

**Subtitle:** Fits a line minimizing squared residuals — the most assumption-heavy algorithm in common use

## Section 1: What It Does

Fits coefficients that minimize sum of squared residuals (OLS). Each coefficient represents the expected change in Y for a one-unit change in X, holding others constant. Closed-form solution via normal equations.

- **Best For:** Pricing models and economic forecasting, dose-response and controlled experiments, causal inference with interpretable coefficients
- **Data:** Continuous target variable, numeric features, linear relationships. The interpretability gold standard.

### Visualization (canvas `c0`, 720×300)

Scatter plot with best-fit line and dashed residual segments.

- **Axes:** L-shaped gray (`#bbb`) axes, left padding 40px, bottom margin 30px; axis labels "x" (bottom center) and "y" (rotated, left) in `#444` 13px.
- **Data:** 20 seeded pseudo-random points (seed 31) generated along the line y = 0.15 + 0.7x (normalized 0–1 coordinates) with uniform noise ±0.09, clamped to [0.02, 0.98]. Dots 4px radius, fill `rgba(26,82,118,0.75)`.
- **Best-fit line:** solid `#1a5276`, width 2.5, from (0, 0.15) to (1, 0.85) in normalized coordinates.
- **Residuals:** vertical dashed segments (dash 3/3, width 1.2, `rgba(231,76,60,0.7)`) from each point to the fitted line.
- **Labels:** bold `#1a5276` equation "y = β₀ + β₁x" at upper left (x≈0.02, y≈0.92 normalized); red `#e74c3c` label "residuals" at top right.

## Section 2: Assumes Linearity

The relationship between each feature and the outcome must be linear. A quadratic, threshold, or saturating relationship is invisible — OLS draws a straight line through a curve, systematically over- and under-predicting in different regions.

- **Breaks:** House price vs. sqft is linear below 3000 but flat above (luxury pricing is location-driven). OLS draws one line through both regimes, underestimating small homes and overestimating large ones.
- **Verify:** Plot residuals vs. each predictor — any curve or pattern means linearity fails for that feature.
- **Fix:** Polynomial terms, splines, GAMs, or piecewise regression for known breakpoints.

### Visualization (canvas `c1`, 720×340)

Scatter with a straight OLS line drawn through a piecewise-flat true curve.

- **Title (bold 14px `#1a5276`, top center):** "OLS Line vs True Curve — Systematic Misfit".
- **Axes:** gray `#bbb` L axes with left padding 70px; x-axis label "Square Footage" (bottom center), y-axis label "Price ($K)" (rotated left), both `#444` 13px.
- **True relationship:** y = 0.1 + 0.9t for t < 0.6, flat at 0.64 for t ≥ 0.6 (normalized).
- **Scatter:** 50 seeded points (seed 42) at t in [0.02, 0.97], y = trueY(t) + uniform noise ±0.06; dots 4px, `rgba(26,82,118,0.6)`.
- **OLS line:** solid red `#e74c3c`, width 3, from (0, 0.15) to (1, 0.7) normalized; bold red label "OLS line" near (0.82, 0.76).
- **True curve:** dashed green `#27ae60` (dash 5/4, width 2.5) tracing trueY; green label "True relationship" near (0.62, 0.72).
- **Annotations:** light-red shaded region `rgba(231,76,60,0.1)` covering t > 0.63 of the plot; red label "Overestimates" centered at t≈0.82 near the top.
- **Caption (bottom center, `#1a5276`):** "Linearity breaks at regime change — residuals show clear pattern".

## Section 3: Assumes Homoscedasticity

Variance of residuals must be constant across all predicted values. "Fan-shaped" residual plots mean standard errors are wrong — typically too small — producing false significance. Your p-values and confidence intervals are lies.

- **Breaks:** Predicting income: low earners vary ±$5K, high earners vary ±$100K. OLS reports one SE for the coefficient, averaging the two regimes. CI for high earners is 10× too narrow.
- **Verify:** Plot residuals vs. fitted values — if the spread increases/decreases, homoscedasticity fails.
- **Fix:** Weighted Least Squares (WLS), robust standard errors (HC3), or log-transform the response.

### Visualization (canvas `c2`, 720×340)

Fan-shaped residual plot (residuals vs. fitted values).

- **Title (bold 14px `#1a5276`, top center):** "Residual Plot — Fan Shape = Heteroscedasticity".
- **Axes:** gray `#bbb` L axes, left padding 70px; x-axis label "Fitted Values (ŷ)", rotated y-axis label "Residuals"; dashed gray `#888` horizontal zero line (dash 4/3) at plot mid-height, labeled "0" at left.
- **Data:** 70 seeded points (seed 77) at t in [0.05, 0.95]; residual = uniform(±0.5) × spread where spread = 0.9t (variance grows with fitted value); dots 3.5px, `rgba(26,82,118,0.65)`.
- **Fan boundaries:** two dashed red `#e74c3c` lines (dash 5/3, width 2) from the zero line at t=0.05 spreading to ±0.42 of plot height at t=0.95.
- **Annotations:** bold red "Variance increases →" above the upper fan line at ~70% width; green `#27ae60` "Tight here ✓" at lower left; red "Wide here ✗" at lower right.
- **Caption (bottom center, `#1a5276`):** "SE is averaged → too small for high predictions, too large for low".

## Section 4: Assumes Normal Residuals

For p-values and confidence intervals to be valid, residuals must be normally distributed. With skewed or heavy-tailed residuals, t-tests on coefficients are unreliable — you can't trust which features are "significant."

- **Breaks:** Right-skewed outcomes (income, insurance claims) produce right-skewed residuals. The t-distribution assumption fails, CIs are asymmetric but reported as symmetric.
- **Verify:** Q-Q plot of residuals vs. normal theoretical quantiles. Deviations in the tails indicate non-normality.
- **Fix:** Log-transform response, use GLM with appropriate family, or bootstrap confidence intervals.

### Visualization (canvas `c3`, 720×340)

Two side-by-side Q-Q plot panels: good (normal) vs. bad (heavy-tailed).

- **Title (bold 14px `#1a5276`, top center):** "Q-Q Plot: Residuals vs Normal Distribution".
- **Panels:** two 280×240 panels with light gray `#ddd` borders, left panel at x=60, right at x=420 (left + 280 + 80), both starting at y=52.
- **Left panel:** bold green `#27ae60` heading "Normal Residuals ✓"; gray `#888` 45-degree reference line; 30 points (seed 123) hugging the line with ±6px jitter, dots 3.5px `rgba(39,174,96,0.7)`.
- **Right panel:** bold red `#e74c3c` heading "Heavy-Tailed Residuals ✗"; same gray reference line; 30 points with S-shaped departure — deviation −30×(0.15−t) for t<0.15 and +30×(t−0.85) for t>0.85, amplified 3×, plus small jitter; dots `rgba(231,76,60,0.7)`.
- **Tail annotations (red, 13px):** "Left tail too heavy" near bottom-right of right panel; "Right tail too heavy" near top-left of right panel.
- **Axis labels:** "Theoretical Quantiles" (`#444`) centered under each panel.
- **Caption (bottom center, `#1a5276`):** "Tail departures → p-values unreliable, CIs too narrow at extremes".

## Section 5: Assumes No Multicollinearity

Features must not be strongly correlated with each other. When they are, the coefficient matrix becomes near-singular — coefficients flip signs, explode in magnitude, and standard errors inflate by 10–100×. The model "works" but the coefficients are meaningless.

- **Breaks:** Include both "height in cm" and "height in inches" → one gets +500, the other gets −497. Technically correct prediction, completely uninterpretable coefficients.
- **Verify:** VIF > 10 for any feature means dangerous collinearity. Condition number > 30 means the design matrix is ill-conditioned.
- **Fix:** Drop redundant features, ridge regression (L2 stabilizes), PCA the correlated block, or LASSO (auto-selects).

### Visualization (canvas `c4`, 720×340)

Horizontal VIF bar chart with coefficient column.

- **Title (bold 14px `#1a5276`, top center):** "Variance Inflation Factor (VIF) — Coefficient Instability".
- **Data (feature / VIF / coefficient):** height_cm 85 / +523; height_in 82 / −497; weight 12 / +3.2; bmi 14 / −8.7; age 1.3 / +1.1; exercise 1.8 / +2.4.
- **Layout:** bars 34px high, 10px gap, starting y=50, bar start x=130, max bar width 350px scaled to VIF/90.
- **Threshold:** vertical dashed red `#e74c3c` line (dash 5/3, width 2) at VIF=10, labeled "VIF=10" above and "(danger)" below.
- **Bar colors:** VIF>10 → fill `rgba(231,76,60,0.55)` stroke `#e74c3c`; otherwise fill `rgba(39,174,96,0.55)` stroke `#27ae60`. Feature names right-aligned before bars, bold, colored red if VIF>10 else green.
- **Value labels:** "VIF=n" in `#333` after each bar; coefficient "β=value" in a right-hand column (red if VIF>10, else `#333`) under a bold "Coef" header.
- **Caption (bottom center, `#1a5276`):** "Correlated features → coefficients explode and flip signs".

## Section 6: No Influential Outliers

A single extreme point can drag the entire regression line toward it. With high leverage (extreme X) and large residual, one observation controls the slope. Cook's distance > 1 means that one point is running your model.

- **Breaks:** CEO salary in a company salary regression: one point at $50M when others are $50K–$200K tilts the entire slope, making every other prediction worse.
- **Verify:** Cook's distance plot — any point > 0.5 warrants investigation. Leverage plot identifies extreme X positions.
- **Fix:** Robust regression (Huber, RANSAC), Winsorize outliers, or fit separate models for different regimes.

### Visualization (canvas `c5`, 720×340)

Scatter with one extreme outlier and two fitted lines (with vs. without the outlier).

- **Title (bold 14px `#1a5276`, top center):** "One Outlier Controls the Entire Fit".
- **Axes:** gray `#bbb` L axes, left padding 70px; x-axis label "Experience (years)", rotated y-axis label "Salary ($K)", `#444` 13px.
- **Cluster:** 35 seeded points (seed 99) at exp in [0.05, 0.55], sal = 0.1 + 0.5×exp ± 0.04 noise (normalized); dots 4px `rgba(26,82,118,0.65)`.
- **Outlier:** one large 8px red `#e74c3c` dot at (0.85, 0.92) labeled bold red "CEO ($5M)" to its right.
- **Line with outlier:** solid red `#e74c3c`, width 2.5, from (0, 0.05) to (0.95, 0.9); red label "OLS with outlier" near (0.6, 0.65).
- **Line without outlier:** dashed green `#27ae60` (dash 5/3, width 2.5) from (0, 0.12) to (0.6, 0.38); green label "OLS without outlier" near (0.35, 0.4).
- **Annotations:** bold orange `#e67e22` "Cook's D = 4.7" above the outlier; `#444` "(threshold: 1.0)" beneath it.
- **Caption (bottom center, `#1a5276`):** "High leverage + large residual = one point runs the model".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (42%) holds `.obj-title` + `.obj-desc` paragraph + `.obj-detail` lines, right `<td>` (58%, centered) holds the canvas. Even rows have background `#fafcfe`.
- **Detail-line markup:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses `<span class="tag tag-check">` (background `#eafaf1`, color `#1e8449`); "Fix:" uses `<span class="tag tag-fix">` (background `#fef9e7`, color `#b7950b`). Tags are 0.82em, weight 600, padding 1px 6px, radius 3px. `.warn` class is orange `#e67e22`.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; `.subtitle` `#666` 0.95rem with 32px bottom margin; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#444`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes per chart (c0 720×300, c1–c5 720×340); a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Scatter data uses a seeded LCG pseudo-random generator (`seed = seed*16807 % 2147483647`) so charts are deterministic.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#444`/`#666`.
