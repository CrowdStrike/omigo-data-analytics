# Metric Testing — Non-Normal Distributions

**Page type:** detail page (numbered h2 sections, each holding one or more two-column obj-table rows: text left 40%, canvas right 60%; closing philosophy callout)
**HTML title tag:** Metric Testing — Non-Normal Distributions & Alternatives

**Subtitle:** Most business metrics are NOT normally distributed. Using t-tests on them gives wrong answers. Here's what to use instead.

## 1. The Problem: Most Metrics Are Non-Normal

### Revenue Per Session — Right-Skewed with Zero-Inflation

- 90% of sessions have $0 revenue (browsed, didn't buy)
- 9% have $10-$100 revenue (typical purchases)
- 1% have $500-$10,000 revenue (high-value purchases)
- Distribution: massive spike at zero + right-skewed tail. NOT a bell curve.
- **t-test assumes normality.** On this data: wrong confidence intervals, wrong p-values, wrong conclusions.

**Also:** High-price purchases don't happen in a single session. A $5000 TV purchase took 5 sessions of research. Attributing to "last session" biases the metric toward impulse/low-price items. The metric itself is biased toward what's easy to measure, not what matters.

#### Visualization (canvas `c1`, 720×300)

Histogram of revenue per session with a zero spike and long right tail.

- **Title (bold 17px `#1a5276`, centered):** "Revenue/Session: 90% Zero + Right-Skewed Tail (NOT Normal)".
- **Bins (20 bars, plot margins left 50 / right 30 / top 40 / bottom 35, scale max 92):** heights `[90, 0, 3, 4, 5, 6, 5, 4, 3, 2, 2, 1, 1, 1, 0, 0, 0, 0, 0, 1]`; first bar filled purple `rgba(142,68,173,0.5)`, all others blue `rgba(41,128,185,0.4)`.
- **Annotations:** purple `#8e44ad` 17px "90% = $0 (spike)" (top left of plot); blue `#2980b9` right-aligned "1% = $500+ (tail drives revenue)".
- **Bottom line (bold 17px red `#e74c3c`, centered):** "t-test assumes bell curve. This is NOT a bell curve. t-test = wrong answer.".

### Conversion Rate — Binomial, Not Normal (Especially at Small n)

- Each session: converts (1) or doesn't (0). Binary outcome.
- At large n (10,000+): CLT makes the AVERAGE approximately normal. t-test is OK.
- At small n (100-500 per variant): binomial intervals needed. t-test CI is wrong by 20-50%.
- At very low conversion rate (0.1%): even at n=10,000, only 10 conversions. Normal approximation fails completely.
- **Trap:** A/B test with 200 sessions/variant and 3% conversion = 6 conversions per variant. "Variant B has 50% lift!" = 9 vs 6 conversions. p=0.30. Not significant. But the dashboard shows "+50%" in green.

#### Visualization (canvas `c2`, 720×300)

Text diagram of a small-n conversion test.

- **Title (bold 17px `#1a5276`, centered):** "Conversion: Binary (0 or 1). At Small n: NOT Normal".
- **Lines (17px `#333` left-aligned at x=50):** "A/B test: n=200/variant, 3% conversion rate" (y=55); "Control: 6 conversions / 200 = 3.0%" (y=80); "Variant:  9 conversions / 200 = 4.5%" (y=105).
- **Green bold 17px (y=135):** "Dashboard: \"+50% lift!\" (GREEN!)".
- **Red bold 17px:** "Reality: p=0.30, CI includes zero. Not significant." (y=160); "9 vs 6 conversions = NOISE, not signal." (y=182).

### Session Duration — Zero-Inflated + Heavy Tail

- Bounces: 0-3 seconds (40% of sessions). Not a "short session" — user never engaged.
- Normal sessions: 30 seconds to 5 minutes (50%)
- Deep engagement: 10-60+ minutes (10%) — or: user left tab open while at lunch
- Mean session = 3 minutes. Median = 45 seconds. p99 = 47 minutes.
- **The mean is a lie.** Nobody has a "3-minute session." You either bounce (0s) or engage (2-5min).

**A/B test on session duration:** If variant B causes fewer bounces → mean goes UP even if engaged users spend less time. The metric conflates "got them in the door" with "kept them engaged." Two different things measured as one number.

#### Visualization (canvas `c3`, 720×300)

Histogram of session duration with bounce spike, main body, and tail.

- **Title (bold 17px `#1a5276`, centered):** "Session Duration: Zero-Inflated (Bounces) + Heavy Tail (Left Tab Open)".
- **Bins (20 bars, margins left 50 / right 30 / top 40 / bottom 30, scale max 42):** heights `[40, 5, 8, 12, 15, 12, 8, 5, 3, 2, 1, 1, 1, 0, 0, 0, 0, 0, 0, 2]`; bar 0 red `rgba(231,76,60,0.5)`, bars 1–9 blue `rgba(41,128,185,0.4)`, bars 10+ orange `rgba(230,126,34,0.4)`.
- **Bottom labels (17px):** red "Bounces (0-3s)" left; blue "Real sessions" center; orange `#e67e22` "Left tab open?" right.

### Latency / Response Time — Log-Normal with Occasional Spikes

- Most requests: 20-100ms (log-normal core)
- GC pauses: 200-500ms spikes every 30 seconds
- Cache misses: 500-2000ms (occasional)
- Timeouts: 30,000ms (reported as "response time" even though it's a failure)
- **t-test on latency:** The 30s timeouts dominate the mean, inflate variance, make EVERYTHING "not significant" because variance is enormous.

**Real problem:** You need to detect a 5ms p50 regression. But variance from the tail is 10,000×. t-test power: zero. The signal is tiny relative to the noise — but it affects 100% of users while the tail affects 0.1%.

#### Visualization (canvas `c4`, 720×300)

Log-normal latency curve with GC-spike bars and a far-right timeout bar.

- **Title (bold 17px `#1a5276`, centered):** "Latency: Log-Normal Core + GC Spikes + Timeouts".
- **Core curve:** blue `#2980b9` width 2.5, a bell-like curve `exp(-0.5·((i-8)/4)²)` over the left half of the plot (margins left 50 / right 30 / top 45 / bottom 30), peak at ~20% of plot width.
- **GC spikes:** two red `rgba(231,76,60,0.5)` bars at ~55% and ~62% of plot width (heights 50% and 40% of plot height).
- **Timeout spike:** purple `rgba(142,68,173,0.5)` bar at ~92% of plot width (height 25%).
- **Labels (17px, centered):** blue "p50=45ms" under the core; red "GC: 200-500ms" above the spikes; purple `#8e44ad` "Timeout: 30s" above the far-right bar.

## 2. Alternatives to t-test for Non-Normal Metrics

### Mann-Whitney U Test (Rank-Based)

- Doesn't assume normality. Compares RANKS not values.
- Tests: "Is the probability that a random observation from A exceeds a random observation from B > 50%?"
- Works for: any continuous distribution, any skewness, any outlier pattern.
- **When to use:** Revenue, session duration, latency — any metric with heavy tails or zero-inflation.
- **Limitation:** Less powerful than t-test when data IS actually normal. Trades power for robustness.

**Example:** Revenue A/B test. t-test: p=0.23 (one whale in control group inflates variance). Mann-Whitney: p=0.004 (rank-based, whale doesn't dominate). The rank test finds the signal the t-test misses because it's not distracted by extreme values.

#### Visualization (canvas `c5`, 720×300)

Text comparison of t-test vs Mann-Whitney on whale-contaminated revenue.

- **Title (bold 17px `#1a5276`, centered):** "Mann-Whitney: Ignores Outlier Magnitude, Focuses on Rank Order".
- **Lines (left-aligned at x=50):** 17px `#333` "Revenue A/B test with one \"whale\" ($50K purchase in control):" (y=55); red 17px "t-test: p = 0.23 (whale inflates variance → can't detect signal)" (y=85); green `#27ae60` "Mann-Whitney: p = 0.004 (ranks: whale = rank #1 but only 1 rank)" (y=115); gray `#555` "The whale is rank #1 in both tests. But in t-test: its VALUE ($50K) dominates variance." (y=150) and "In Mann-Whitney: it's just \"the biggest\" — same contribution as the #2 rank." (y=172).

### Bootstrap Confidence Intervals

- Resample your data 10,000 times (with replacement). Compute the metric each time. The distribution of resampled metrics = your CI.
- Makes ZERO assumptions about underlying distribution. Works for means, medians, percentiles, ratios — anything.
- Computationally expensive but universally applicable.
- **When to use:** Complex metrics (ratio of ratios, weighted averages, percentile differences), small samples, unknown distributions.

**Example:** "Revenue per converted user" in A/B test. Can't use t-test (non-normal). Can't use Mann-Whitney easily (it's a ratio). Bootstrap: resample users, compute ratio each time, get CI directly. p-value = fraction of bootstrap samples where difference crosses zero.

#### Visualization (canvas `c6`, 720×300)

Bootstrap sampling distribution with CI bound markers.

- **Title (bold 17px `#1a5276`, centered):** "Bootstrap: Resample 10,000× → Distribution of Your Metric → CI".
- **Curve:** green `#27ae60` width 2.5 bell curve `exp(-0.5·((i-25)/8)²)` across the plot (margins left 60 / right 40 / top 45 / bottom 30).
- **CI bounds:** dashed red `#e74c3c` vertical lines (dash 4/3, width 2) at 20% and 80% of plot width, labeled red 17px "2.5%" and "97.5%" below.
- **Labels:** green centered "95% CI from 10K resamples" above the curve; gray `#555` bottom line "Works for ANY distribution. Zero assumptions. Just resample.".

### Permutation Test (Exact, Distribution-Free)

- Shuffle the group labels (A/B) randomly 10,000 times. For each shuffle: compute the test statistic. Your ACTUAL difference: how extreme is it compared to the shuffled distribution?
- Exact Type I error control. No distributional assumptions. Works with any sample size.
- **When to use:** Small samples (n<100 per group), very non-normal data, when you need EXACT p-values not approximations.
- **Limitation:** Computationally intensive. At n=10,000 per group: 10K permutations still fast. At n=1M: use bootstrap instead.

**Example:** A/B test with 50 users per variant (new feature rollout to small segment). Revenue is bimodal (some convert, most don't). t-test: invalid (non-normal, small n). Permutation test: exact p-value from all possible label shuffles. Trustworthy at any n.

#### Visualization (canvas `c7`, 720×300)

Null distribution histogram from label shuffles with the actual result in the tail.

- **Title (bold 17px `#1a5276`, centered):** "Permutation: Shuffle Labels → Null Distribution → Where Does Your Result Fall?".
- **Null histogram:** 30 gray `rgba(150,150,150,0.3)` bars in a bell shape `exp(-0.5·((i-15)/5)²)` (margins left 60 / right 40 / top 45 / bottom 30).
- **Actual result:** one red `rgba(231,76,60,0.6)` bar at bin 26 (height 60% of plot), labeled bold red 17px "YOUR result" above.
- **Labels:** gray `#999` 17px "Shuffled null distribution" over the histogram; gray `#555` bottom line "p-value = fraction of shuffled results MORE extreme than yours".

### Delta Method / Ratio Metrics

- Many metrics are RATIOS: revenue/session, conversions/visitors, clicks/impressions.
- The ratio of two random variables has a complex distribution — even if numerator and denominator are both normal, the ratio is NOT.
- **Delta method:** Approximates the variance of a ratio using Taylor expansion. Gives approximate CI for ratio metrics.
- **When to use:** CTR, conversion rate, ARPU, any "X per Y" metric in A/B tests where both X and Y vary.

**Example:** Revenue per session A/B test. Control: $50,000 revenue / 10,000 sessions = $5.00. Treatment: $48,000 / 9,000 sessions = $5.33. Is +$0.33 significant? Can't just t-test the per-session values (zero-inflated). Delta method: accounts for variance in both revenue AND session count.

#### Visualization (canvas `c8`, 720×300)

Text walkthrough of the ratio-metric CI.

- **Title (bold 17px `#1a5276`, centered):** "Delta Method: Proper CI for Ratio Metrics (X/Y)".
- **Lines (17px `#333` left-aligned at x=50):** "Control: $50,000 / 10,000 sessions = $5.00/session" (y=55); "Treatment: $48,000 / 9,000 sessions = $5.33/session" (y=80); "Naive: \"$5.33 > $5.00, significant!\" — but variance in BOTH numerator AND denominator matters." (y=110).
- **Green bold 17px (y=145):** "Delta method accounts for: Var(revenue) + Var(sessions) + Cov(revenue, sessions)".
- **Gray `#555` 17px (y=175):** "Result: CI = [$4.85, $5.81] — includes $5.00 → NOT significant despite +$0.33 point estimate.".

### Quantile Regression / Percentile Tests

- Instead of testing "did the MEAN change?" → test "did the MEDIAN change?" or "did p90 change?"
- Median is robust to outliers (one whale doesn't move it). p90/p99 captures tail experience.
- Tests exist for quantile differences (Koenker's quantile regression, bootstrap on quantiles).
- **When to use:** Latency (care about p99), revenue (median more meaningful than mean), any bimodal metric where mean is meaningless.

**Example:** Latency A/B test. Mean latency: variant A = 150ms, variant B = 145ms. "Not significant" (p=0.4) because variance from timeouts. p50 test: A = 45ms, B = 42ms (p=0.001). p99 test: A = 2100ms, B = 1800ms (p=0.03). The MEAN test missed signal that QUANTILE tests found — because you asked the right question ("did typical experience improve?") not the wrong one ("did the average shift?").

#### Visualization (canvas `c9`, 720×300)

Three result rows comparing mean vs quantile tests.

- **Title (bold 17px `#1a5276`, centered):** "Quantile Tests Find Signal the Mean Test Misses".
- **Rows (boxes 40px tall spanning x=40 to w−40, 50px pitch from y=50; fill 10%-alpha and 1.5px stroke of the verdict color):** "Mean test" A=150ms B=145ms, "p=0.40 ✗" red `#e74c3c`; "p50 test" A=45ms B=42ms, "p=0.001 ✓" green `#27ae60`; "p99 test" A=2100ms B=1800ms, "p=0.03 ✓" green. Test names bold 17px `#333`, values 17px `#333`, p-values bold right-aligned in the verdict color.
- **Bottom line (bold 17px red, centered):** "Mean missed it. Percentiles found it. Ask the RIGHT question.".

## 3. Decision Guide: Which Test for Which Metric?

### Quick Reference

- **Conversion rate (binary):** Proportion test (z-test for proportions) at large n. Fisher's exact at small n. NOT t-test.
- **Revenue (zero-inflated, skewed):** Mann-Whitney OR bootstrap. Split: test "did conversion change?" (proportion test) + "did average order value change for converters?" (t-test on converters only, which IS more normal).
- **Session duration (zero-inflated):** Exclude bounces → test engaged sessions separately. Or: Mann-Whitney on full data. Or: two metrics (bounce rate + engaged duration).
- **Latency (log-normal + spikes):** Test on log(latency) with t-test (log-normal → normal after log). Or: test percentiles directly (bootstrap on p50, p99).
- **Count metrics (page views, clicks):** Poisson or negative binomial regression. NOT t-test (counts aren't continuous).
- **Any ratio metric (X per Y):** Delta method or bootstrap. Never just "divide then t-test."
- **Don't know the distribution:** Bootstrap or permutation test. Always valid. No assumptions needed.

**Rule of thumb:** If your metric's histogram looks like a bell → t-test is fine. If it looks like ANYTHING ELSE (spike at zero, heavy tail, bimodal, discrete) → use an alternative. When in doubt: bootstrap.

#### Visualization (canvas `c10`, 720×240)

Metric-shape-to-test lookup table rendered as colored rows.

- **Title (bold 17px `#1a5276`, centered):** "Decision Guide: Metric Shape → Correct Test".
- **Rows (23px-tall tinted bands spanning x=30 to w−30, 27px pitch from y=42; metric bold 17px in the row color at x=50, test 17px `#333` at x=280 prefixed "→ "):**
  - "Conversion (binary)" → "Proportion z-test / Fisher exact" — blue `#2980b9`
  - "Revenue (zero-inflated)" → "Mann-Whitney OR bootstrap" — green `#27ae60`
  - "Duration (zero + tail)" → "Exclude bounces + separate test" — orange `#e67e22`
  - "Latency (log-normal)" → "Test on log(x) OR quantile test" — purple `#8e44ad`
  - "Counts (discrete)" → "Poisson / neg-binomial regression" — blue `#2980b9`
  - "Any ratio (X/Y)" → "Delta method or bootstrap" — orange `#e67e22`
  - "Unknown distribution" → "Bootstrap or permutation (always safe)" — green `#27ae60`
- **Bottom line (bold 17px red, centered):** "Default t-test is WRONG for 6 of 7 common metric types.".

## Callout (philosophy box)

**The meta-problem:** Most A/B testing platforms default to t-test. Most business metrics violate t-test assumptions. Result: thousands of A/B tests worldwide reaching wrong conclusions every day — shipping non-improvements and missing real improvements — because the default statistical test doesn't match the data's distribution. Checking the SHAPE of your metric before choosing a test is not optional — it's the difference between science and theater.

## Regeneration instructions

- **Layout:** h1 + `.subtitle` paragraph, then three numbered `<h2>` sections ("1.", "2.", "3.", 1.3em `#1a5276` with a 2px `#2980b9` bottom border). Section 1 holds four `.obj-table` blocks (one per metric type), section 2 holds five, section 3 holds one. Each `.obj-table` is a full-width single-row table: left `<td>` (40%) with `.obj-title` (1.05em, weight 600, `#1a5276`), a `<ul>` (0.9em), and optionally a closing `<p>` (0.95em); right `<td>` (60%, centered) with the canvas (explicit `width`/`height` attributes). Cell borders `1px solid #e0e0e0`, padding 20px 24px, `vertical-align: middle`; even rows `#fafcfe`. Page ends with a `.philosophy` callout — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** sizes 720×300 (c1–c9) and 720×240 (c10); shared `setup(id)` helper reads width/height attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Charts mix histograms/curves with text-diagram layouts; all text uses 17px `-apple-system` (bold 17px for titles and verdicts).
- **Palette:** primary blue `#1a5276`, chart blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, grays `#555`/`#333`/`#999`.
