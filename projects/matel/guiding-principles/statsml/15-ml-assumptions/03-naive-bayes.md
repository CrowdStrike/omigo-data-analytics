# Naive Bayes

**Page type:** detail page (two-column obj-table layout: text left 42%, canvas right 58%, one row per assumption)
**HTML title tag:** Naive Bayes - ML Assumptions

**Subtitle:** Multiplies per-feature likelihoods as if each carries unique evidence. When features are redundant or densities are wrong, the posterior is wildly miscalibrated.

## Section 0: What It Does

Applies Bayes' theorem with the "naive" assumption that all features are conditionally independent given the class. Multiplies individual feature likelihoods to get posterior probability: P(class|features) ∝ P(class) × ∏ P(featureᵢ|class).

- **Best For:** Spam filtering, sentiment analysis, document categorization with bag-of-words features
- **Data:** High-dimensional sparse data, categorical or discretized features, works well even with small training sets per class.

### Visualization (canvas `c0`, 720×300)

Bar-chain diagram of the posterior as a product of feature likelihoods.

- **Title (bold 13px `#1a5276`, centered at top):** "P(spam | email) ∝ P(spam) × P(f₁|spam) × P(f₂|spam) × P(f₃|spam) × ..."
- **Bars:** six vertical bars, width 80, max height 160, baseline y=235, gap 18, starting x=45. Data (label, value, stroke color):
  - "P(spam)" 0.40, blue `#1a5276` (fill `rgba(26,82,118,0.6)`)
  - "P(\"free\"|spam)" 0.78, green `#27ae60` (fill `rgba(39,174,96,0.75)`)
  - "P(\"win\"|spam)" 0.65, green
  - "P(\"$$\"|spam)" 0.72, green
  - "P(\"click\"|spam)" 0.68, green
  - "P(\"dear\"|spam)" 0.15, orange `#e67e22` (fill `rgba(230,126,34,0.75)`)
- Each bar has its value (2 decimals) bold on top in its color and its label below in `#555`; bold "×" signs in `#555` between bars at mid-height.
- **Result:** "=" then "0.017" in bold `#1a5276` 14px with "(unnormalized)" in `#555` beneath it, to the right of the last bar.
- **Bottom note (13px `#555`, centered):** "Each feature contributes independently — no interactions modeled".

## Section 1: Conditional Feature Independence

Naive Bayes requires that **all features are conditionally independent given the class**: P(X₁,X₂|Y) = P(X₁|Y) × P(X₂|Y). In practice, this means each feature must contribute unique evidence. If two features carry the same information (e.g., systolic BP and diastolic BP, r=0.78), NB counts that evidence twice — producing posteriors that are drastically overconfident.

- **Breaks:** Including both systolic and diastolic BP. They're 78% redundant. NB multiplies both likelihoods, producing P(disease)=0.99 when the true probability is 0.65. The model becomes overconfident on every prediction.
- **Verify:** Within-class correlation matrix — any |r| > 0.3 is problematic
- **Fix:** Remove one of correlated pair, PCA, or use logistic regression which handles correlations

### Visualization (canvas `c1`, 720×300)

Split panel: correlated-features scatter on the left, overconfident posterior bars on the right.

- **Left scatter (width ~0.45w, padding 50):** L-shaped gray axes; x-axis "Systolic BP" (range 90–200), y-axis rotated "Diastolic BP" (range 50–120). 50 points along the correlation: sys = 100 + i·1.6 + sin jitter (±5), dia = 0.55·sys + 18 + sin jitter (±6); disease points red `rgba(231,76,60,0.65)`, others blue `rgba(41,128,185,0.5)`, radius 3.5.
- **Correlation line:** orange `#e67e22` width 2.5 from (100, 73) to (185, 120) in data coordinates, with bold orange label "r = 0.78".
- **Right panel (starting at 0.52w, width 0.38w):** bold `#1a5276` title "Posterior P(disease | patient)".
  - Bar 1 (y 55–83): track fill `#f0f4f8`, filled 65% in solid green `#27ae60`, green outline; white bold centered text "True: P = 0.65".
  - Bar 2 (y 100–128): track fill `#f0f4f8`, filled 99% in solid red `#e74c3c`, red outline; white bold centered text "NB output: P = 0.99".
  - Orange arrow between the two bars.
- **Formula text (left-aligned):** "NB computes:" in `#555` 13px; monospace line "P(D|sys,dia) ∝ P(sys|D) × P(dia|D)"; red 13px line "But sys ≈ f(dia) — same signal counted twice!".
- **Calibration annotation (bold orange, centered):** "Overconfidence: 0.34 too high".

## Section 2: Correct Density Model (Distribution Fit)

Gaussian Naive Bayes estimates P(feature|class) using a **normal distribution parameterized by mean and variance**. If the actual within-class distribution is bimodal, skewed, or uniform, the Gaussian assigns high probability to regions where no data exists (the valley between modes) and low probability to where data actually lives (the tails of a skewed distribution).

- **Breaks:** Feature "age" is bimodal within the positive class (peak at 28, peak at 65). Gaussian NB fits μ=46, σ=18. It assigns the HIGHEST likelihood to age=46 — exactly where no positive cases exist. Observations at the actual peaks get lower scores.
- **Verify:** Q-Q plots per feature per class, Shapiro-Wilk, visual histograms
- **Fix:** KDE-based density, multinomial NB with binned features, or mixture models per class

### Visualization (canvas `c2`, 720×300)

Histogram of a bimodal true distribution with the Gaussian NB fit overlaid.

- **Axes:** L-shaped gray (`#bbb`) axes, padding 55; x-axis "Age (within positive class)" mapping ages 15–80; y-axis rotated label "P(age | positive)"; x ticks at 20, 30, 40, 50, 60, 70.
- **Histogram (true bimodal, fill `rgba(39,174,96,0.55)` with `#27ae60` outlines):** bins (center, density): (18, 0.15), (22, 0.35), (26, 0.55), (30, 0.45), (34, 0.25), (38, 0.10), (42, 0.05), (46, 0.03), (50, 0.05), (54, 0.10), (58, 0.22), (62, 0.45), (66, 0.50), (70, 0.38), (74, 0.18).
- **Gaussian NB curve:** dashed red `#e74c3c` (dash 6/4, width 2.5): 0.42·exp(−(x−46)²/280).
- **Error zone:** vertical band from age 38 to 54 filled `rgba(231,76,60,0.1)` with bold red three-line annotation centered above it: "NB says HIGH" / "likelihood here" / "(almost no data!)".
- **Peak annotations (bold green):** "actual peak" above the ~27 bin and above the ~64 bin.
- **Legend (top right):** green filled square "True distribution (bimodal)"; dashed red line "Gaussian NB assumes (μ=46)"; legend text in `#333`.

## Section 3: No Zero-Frequency (Smoothing Required)

If a feature value appears in test data that was **never seen in training for a given class**, its likelihood is zero: P(feature=x|class) = 0. Because NB multiplies all likelihoods, a single zero wipes out the entire posterior — regardless of how strong all other features are. One unseen word in a spam classifier makes the entire email "not spam" with certainty.

- **Breaks:** Email contains the word "cryptocurrency" — never seen in training spam. P("cryptocurrency"|spam) = 0 → P(spam|email) = 0, even though all other 50 features scream spam. One missing word overrides everything.
- **Verify:** Check for feature values with zero counts in any class, vocabulary coverage
- **Fix:** Laplace smoothing (add-1), Lidstone smoothing (add-α), or back-off to seen values

### Visualization (canvas `c3`, 720×300)

Horizontal likelihood bars multiplied together, with one zero factor destroying the posterior.

- **Title (bold 13px `#1a5276`, centered):** "NB posterior = product of all feature likelihoods".
- **Left column — six horizontal bars** (height 28, max width 120, starting y=45, vertical gap 38; monospace right-aligned word labels; track fill `#f0f4f8`; value in bold to the right; "×" signs between rows): 
  - `"free"` 0.82 green; `"money"` 0.75 green; `"urgent"` 0.68 green; `"click"` 0.71 green; `"crypto"` **0.00 red `#e74c3c`**; `"offer"` 0.60 green. Green bars fill `rgba(39,174,96,0.55)`, the zero bar `rgba(231,76,60,0.55)`.
- **Zero highlight:** thick red box (width 2.5) around the "crypto" bar; bold red left-aligned label "← NEVER SEEN IN TRAINING".
- **Right column (starting at 0.58w):** bold centered "Result:"; monospace `#555` lines: "P(spam|email) ∝" / "  0.82 × 0.75 × 0.68" / "  × 0.71 × 0.00 × 0.60"; then bold red 16px "= 0.000".
- **Explanation (13px `#555`):** "One zero factor kills the" / "entire posterior — all other" / "evidence is irrelevant."
- **Fix annotation:** bold green "Fix: Laplace smoothing"; monospace `#555` "P(w|c) = (count+1)/(N+V)" / "→ 0.00 becomes ~0.001".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width `border-collapse` table, one `<tr>` per section; left `<td>` (42%) holds `.obj-title` + `.obj-desc` paragraph + `.obj-detail` lines, right `<td>` (58%, centered) holds the canvas. Even rows have background `#fafcfe`.
- **Detail-line markup:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses `<span class="tag tag-check">` (pill: background `#eafaf1`, color `#1e8449`); "Fix:" uses `<span class="tag tag-fix">` (pill: background `#fef9e7`, color `#b7950b`). Tag pills: inline-block, 0.75em, padding 2px 8px, radius 4px. Also defined: `.good` `#27ae60`, `.warn` `#e67e22`, `.tag-break` (background `#fdeaea`, color `#c0392b`).
- **Page style:** global reset (`* { margin:0; padding:0; box-sizing:border-box }`); body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border and 8px bottom padding; `.subtitle` `#666` 0.95rem, 32px bottom margin; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#555`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; `canvas { display:block; margin:0 auto; }`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; charts drawn with vanilla canvas 2D in IIFEs.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#555`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
