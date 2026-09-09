# T-Test Precondition Verification

**Page type:** detail page (TOC box, then two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** T-Test Precondition: Multi-Candidate Verification

**Subtitle:** Treat precondition checking as a research problem, not a binary gate.

## Table of Contents

**Table of Contents** (ordered list of in-page anchor links):

1. The Problem (#problem)
2. Why Single Thresholds Fail (#single-thresholds)
3. Four Candidate Interpretations (#four-candidates)
4. What Gets Checked (#preconditions)
5. The Bimodal Trap (#bimodal-trap)
6. Validate Across Time Windows (#validation)
7. Trust Rating → Decision (#trust-rating)
8. Confidence Degradation (#confidence)
9. Heavy Tails: Same Skew, Different Impact (#heavy-tails)
10. Sample Size Is Not a Fixed Bar (#sample-size)
11. Real-World Example: Medical Claims Data (#example)

## The Problem

"Is this data suitable for a t-test?" is not a yes/no question. Single-threshold checks (Shapiro-Wilk p > 0.05, skewness < 2) are arbitrary boundaries that reject good data and pass bad data depending on sample size and shape.

**The real question:** "Will the t-test give a reliable answer with THIS data?" — and that has a continuous answer.

## The Multi-Candidate Approach

(h2 heading; the following nine sections are rows of one obj-table under it.)

## 1. Why Single Thresholds Fail

- Shapiro-Wilk: too sensitive at large n (rejects everything), too lenient at small n
- Skewness < 2: data at 1.99 passes, 2.01 fails — why?
- n > 30 (CLT): oversimplified — depends on HOW non-normal the data is
- Visual QQ-plot: subjective, not automatable

The t-test needs the **sampling distribution of the mean** to be approximately normal. This requires either near-normal data OR enough n for CLT to dominate.

### Visualization (canvas `c1`, 720×280)

Histogram with a threshold line showing the arbitrariness of a single cutoff.

- **Bars:** 16 bins `[2,4,8,14,22,30,35,32,24,16,10,6,4,3,2,1]`, scale max 36, fill `rgba(26,82,118,0.35)`, plot area x from 50 with bar width (w−100)/16, baseline at h−60.
- **Density overlay:** Gaussian-smoothed line over bin heights (sigma 1.3 bins, winsorized at 2× bar height) in `#1a5276` width 2, with 95% SE band fill `rgba(26,82,118,0.13)` (SE = 1.96·smoothed/√effN, effN=120).
- **Threshold line:** vertical dashed red `#e74c3c` (dash 5/3, width 2) at bin 7, labeled above in bold 14px red: "threshold = 0.5".
- **Verdict labels (17px):** left in green `#27ae60`: "skew = 0.49 → PASS"; right of center in red `#e74c3c`: "skew = 0.51 → FAIL".
- **Caption (bottom center, `#555`):** "Same data. Different day. Different verdict."

## 2. Four Candidate Interpretations

Instead of one threshold, run four philosophies in parallel:

- **Strict:** data must actually look normal (|skew| < 0.5, kurtosis < 4)
- **Moderate:** some deviation is fine (|skew| < 1.0, kurtosis < 6)
- **CLT-Reliant:** large n compensates (|skew| < 2.0, but needs n ≥ 80)
- **Shape-Adaptive:** thresholds scale with √n — more data = more tolerance

Each produces a verdict with a confidence score (how comfortably it passes, not just pass/fail).

### Visualization (canvas `c2`, 720×320)

Horizontal acceptance-region bars on a shared |skewness| axis (0 to 3.0, axis from x=180 to w−40, tick labels 0.0–3.0 every 0.5 along a gray `#aaa` baseline at h−20).

- **Candidate rows** (each a 28px-tall translucent band from skew 0 to its threshold at 20% alpha, a solid 2px threshold tick, right-aligned bold 14px name label, and "|skew|<T" label above the tick):
  - Strict, threshold 0.5, color `#c0392b`, y=60
  - Moderate, threshold 1.0, color `#e67e22`, y=130
  - Shape-Adaptive (n=100), threshold 1.1, color `#8e44ad`, y=200
  - CLT-Reliant (n≥80), threshold 2.0, color `#27ae60`, y=270
- **Example data marker:** vertical dashed `#1a5276` line (dash 4/3) at skew=0.82, labeled above in bold 13px: "data: skew=0.82".

## 3. What Gets Checked

Five preconditions, in priority order:

- **Unimodality** — single peak? (Hartigan's dip test). Most critical: bimodal = never t-test.
- **Symmetry** — balanced around center? (skewness, quartile balance)
- **Tail behavior** — extreme outliers? (kurtosis, IQR/range ratio)
- **Sample size** — enough n for observed non-normality?
- **Point masses/gaps** — discrete clusters or zero-inflation?

### Visualization (canvas `c3`, 720×280)

Priority checklist of five full-width rows (36px tall, starting y=25, 44px row pitch, x 40 to w−40):

- Rows: "① Unimodal?" (critical), "② Symmetric?", "③ Light tails?", "④ Enough n?", "⑤ No gaps?".
- Critical row: fill `#fdedec`, border `#e74c3c`, bold text `#c0392b`, with right-aligned red note "← MUST pass all candidates". Non-critical rows: fill `#f0f4f8`, border `#ddd`, text `#1a5276`, 17px.
- **Caption (bottom center, `#555`):** "Priority order: fail unimodality → stop immediately".

## 4. The Bimodal Trap

Bimodal data can have skewness ≈ 0 and kurtosis ≈ 2 — passing every metric except unimodality.

The mean falls in the **valley between peaks** — it represents nobody. Comparing means is meaningless.

**All four candidates reject bimodal data.** No amount of sample size fixes this. CLT makes the mean's sampling distribution normal, but the mean itself is the wrong statistic.

### Visualization (canvas `c4`, 720×280)

Bimodal histogram with the mean marked in the valley.

- **Bars:** 13 bins `[0,12,28,35,20,4,2,4,20,35,28,12,0]`, scale max 36, fill `rgba(142,68,173,0.35)`, x from 50, baseline h−60.
- **Density overlay:** smoothed line `#5b2c6f` width 2, band `rgba(142,68,173,0.15)`, sigma 1.3, effN 150.
- **Mean line:** vertical dashed red `#e74c3c` width 3 (dash 5/3) at the center of bin 6; bold 15px label "MEAN" above, 17px "(nobody lives here)" just above the baseline.
- **Caption (bottom center, `#555`):** "skew≈0, kurt≈2 — passes every check EXCEPT unimodality".

## 5. Validate Across Time Windows

A candidate that passes could be fitting noise in this particular sample. To trust it:

- Run candidates on primary sample (e.g., Jan–Mar)
- Re-run on validation sample (e.g., Apr–Jun)
- Passes both → **stable interpretation**
- Passes one, fails other → **unstable** (discard)

Only stable interpretations earn trust.

### Visualization (canvas `c5`, 720×280)

Text table drawn on canvas with bold 14px `#1a5276` headers "Candidate", "Primary", "Validation", "Stable?" (y=35), and four rows (46px pitch from y=60):

| Candidate (color) | Primary | Validation | Stable? |
|---|---|---|---|
| Strict (`#c0392b`) | FAIL | FAIL | ✓ yes |
| Moderate (`#e67e22`) | PASS | PASS | ✓ yes |
| CLT (`#27ae60`) | PASS | PASS | ✓ yes |
| Adaptive (`#8e44ad`) | PASS | PASS | ✓ yes |

PASS rendered in green `#27ae60`, FAIL in red `#e74c3c`; stable "✓ yes" green / "✗ no" red.

- **Caption (bottom center, bold 13px `#1a5276`):** "3/4 stable → HIGH TRUST → apply t-test".

## 6. Trust Rating → Decision

- **High trust** (3–4 stable candidates): apply t-test with full confidence
- **Low trust** (1–2 stable): apply t-test AND non-parametric, report both
- **No trust** (0 stable): don't apply t-test — use Mann-Whitney or permutation test

Trust flows through: precondition confidence → test confidence → evidence weight → classification confidence.

### Visualization (canvas `c6`, 720×280)

Three outcome boxes (190×180 at y=40, 30px gaps, centered), each with tinted fill, 2px colored border, bold 18px label, 17px `#555` description, bold 14px action:

- HIGH / "3-4 stable" / "Apply t-test" — color `#27ae60`, background `#d4efdf`
- LOW / "1-2 stable" / "T-test + MW" — color `#e67e22`, background `#fef9e7`
- NONE / "0 stable" / "Non-parametric only" — color `#e74c3c`, background `#fdedec`

**Caption (bottom center, `#555`):** "Trust flows: precondition → test → evidence weight → classification".

## 7. Confidence Degradation

As data deviates from normality, candidates drop off at different rates:

- Strict drops at |skew| = 0.5
- Moderate drops at |skew| = 1.0
- Shape-Adaptive at ~1.1 (for n=100)
- CLT-Reliant at |skew| = 2.0

The progressive dropout IS the information — it tells you how far from "safe" this data is.

### Visualization (canvas `c7`, 720×320)

Line chart of confidence vs |skewness| with light `#eee` gridlines, black `#333` L-shaped axes, padding top 35 / bottom 45 / left 60 / right 25.

- **Axes:** x = |Skewness| 0→3.0 (ticks every 0.5, axis label "|Skewness| (n=100)"); y = Confidence 0→1 (ticks every 0.2, rotated label "Confidence").
- **Curves** (width 2.5), each following conf = 0 for skew ≥ threshold, else max(0, 1 − (skew/threshold)^1.5 × 0.9):
  - Strict `#c0392b` (th 0.5), Moderate `#e67e22` (th 1.0), Adaptive `#8e44ad` (th 1.1), CLT `#27ae60` (th 2.0).
- **Legend:** top-right, small color swatch + name per curve.

## 8. Heavy Tails: Same Skew, Different Impact

- **Extreme outliers** (3 points at 10× range): kurtosis = 12, all candidates fail. A few points dominate variance.
- **Mildly heavy tails** (t-distribution-like): kurtosis = 5.8 but symmetric. Moderate/CLT/Adaptive pass.

**Key insight:** kurtosis + asymmetry is dangerous. Kurtosis alone (symmetric heavy tails) is usually fine with enough n.

### Visualization (canvas `c8`, 720×280)

Side-by-side pair of histograms split by a light `#ddd` vertical divider at w/2.

- **Left:** 16 bins `[0,2,8,20,35,40,35,20,8,2,0,0,0,0,0,1]`, max 42, bars `rgba(231,76,60,0.35)`, density line `#922b21`, band `rgba(231,76,60,0.13)`, sigma 1.2, effN 60. Title (bold 13px `#e74c3c`): "Extreme outliers"; bottom label (`#555`): "kurt=12 → ALL FAIL".
- **Right:** 16 bins `[1,3,7,14,22,30,34,30,22,14,7,3,1,0,0,0]`, max 36, bars `rgba(142,68,173,0.35)`, density line `#5b2c6f`, band `rgba(142,68,173,0.13)`, sigma 1.2, effN 100. Title (bold 13px `#8e44ad`): "Mildly heavy tails"; bottom label (`#555`): "kurt=5.8, symmetric → 3/4 PASS".

## 9. Sample Size Is Not a Fixed Bar

Required n depends on how non-normal the data is:

- skew=0.3, kurt=3.2 → n ≥ 22 is enough
- skew=1.0, kurt=4.0 → n ≥ 40
- skew=2.0, kurt=8.0 → n ≥ 105
- If you need n > 500 → just use non-parametric

The shape-adaptive candidate embeds this logic: threshold = 0.5 + 0.06√n.

### Visualization (canvas `c9`, 720×280)

Curve of required n vs |skewness| with a shaded feasible region. Padding top 30 / bottom 45 / left 60 / right 25; black axes.

- **Axes:** x = |Skewness| 0→3.0 (ticks every 0.5, label "|Skewness|"); y = Required n 0→200 (ticks every 50, rotated label "Required n", values capped at 200).
- **Curve:** `#1a5276` width 2.5, reqN(skew) = 20 + skew²·15 + max(0, skew−1)·20.
- **Region fill:** area below the curve filled `rgba(39,174,96,0.1)`.
- **Labels (17px):** "t-test OK" in green `#27ae60` inside the lower-left region; "not enough n" in red `#e74c3c` in the upper-right region.

## 11. Real-World Example: Medical Claims Data

A hospital wants to know if patients with heart disease have different cholesterol than those without. Here's how the multi-candidate approach plays out on three features from the same dataset.

## Feature A: Cholesterol (n=150)

**Shape:** Right-skewed (skew=0.82, kurtosis=3.4, unimodal). A few patients have very high cholesterol pulling the right tail.

- **Strict:** FAIL — skew 0.82 > 0.5 threshold
- **Moderate:** PASS (confidence 0.85) — skew 0.82 < 1.0
- **CLT-Reliant:** PASS (confidence 0.95) — n=150 is plenty
- **Shape-Adaptive:** PASS (confidence 0.88) — threshold is 1.22 for n=150

**Validation (Apr–Jun):** All three passing candidates remain stable.

**Verdict:** HIGH TRUST. Apply t-test. The mild right-skew is well-handled by n=150.

### Visualization (canvas `cA`, 720×280)

Right-skewed histogram with mean line and verdict tags.

- **Bars:** 18 bins `[2,4,8,14,22,30,38,35,28,20,14,9,6,4,3,2,1,1]`, max 40, fill `rgba(39,174,96,0.35)`, x from 50, baseline h−60.
- **Density overlay:** line `#1e8449`, band `rgba(39,174,96,0.15)`, sigma 1.3, effN 150.
- **Mean line:** vertical dashed red `#e74c3c` (dash 4/3, width 2) at bin position 6.5, labeled above in 17px red: "mean=245".
- **Verdict tags** (bold 13px, top-right column at x=w−200, y 30/52/74/96): "Strict: FAIL" red; "Moderate: PASS", "CLT: PASS", "Adaptive: PASS" green.
- **Captions (bottom center):** 17px `#555` "3/4 stable across validation"; bold 13px `#1a5276` "Cholesterol: skew=0.82, n=150 → HIGH TRUST".

## Feature B: Blood Pressure (n=45)

**Shape:** Moderately skewed (skew=1.1, kurtosis=4.2, unimodal). Small sample from a specialized ward.

- **Strict:** FAIL — skew 1.1 > 0.5
- **Moderate:** PASS (confidence 0.52) — barely, skew 1.1 close to threshold 1.0... wait, this is above. Actually fails too.
- **CLT-Reliant:** FAIL — n=45 < 80 minimum
- **Shape-Adaptive:** PASS (confidence 0.61) — threshold is 0.90 for n=45... also fails.

**Validation (different ward):** Skew jumps to 1.4. Everything fails.

**Verdict:** NO TRUST. Use Mann-Whitney U instead. Too few samples for this much skew.

### Visualization (canvas `cB`, 720×280)

Skewed histogram, all-fail verdict tags.

- **Bars:** 18 bins `[1,3,6,12,18,22,18,12,8,5,4,3,2,2,1,1,1,0]`, max 24, fill `rgba(231,76,60,0.35)`.
- **Density overlay:** line `#922b21`, band `rgba(231,76,60,0.13)`, sigma 1.3, effN 45.
- **Mean line:** vertical dashed red at bin position 5.5, labeled "mean=138".
- **Verdict tags** (all red, bold 13px): "Strict: FAIL", "Moderate: FAIL", "CLT: FAIL (n<80)", "Adaptive: FAIL".
- **Captions (bottom center):** 17px `#555` "Use Mann-Whitney U instead"; bold 13px `#1a5276` "Blood Pressure: skew=1.1, n=45 → NO TRUST".

## Feature C: Fasting Glucose (n=200)

**Shape:** Bimodal (skew=0.15, kurtosis=1.9). Patients fall into two groups: normal glucose (~90 mg/dL) and pre-diabetic (~140 mg/dL). Classic subpopulation split.

- **All candidates:** FAIL — dip test p=0.001

Despite perfect skewness and low kurtosis (looks "normal" by those metrics alone), every candidate correctly rejects. The mean of 115 mg/dL represents **neither** the normal nor the pre-diabetic group.

**Verdict:** NO TRUST. Split into subpopulations first, then test each separately. Or use permutation test on the combined data.

### Visualization (canvas `cC`, 720×280)

Bimodal histogram, mean in the valley.

- **Bars:** 16 bins `[2,8,18,28,22,10,4,2,2,4,10,22,28,18,8,2]`, max 30, fill `rgba(142,68,173,0.35)`.
- **Density overlay:** line `#5b2c6f`, band `rgba(142,68,173,0.13)`, sigma 1.3, effN 200.
- **Mean line:** vertical dashed red width 3 at bin position 7.5, labeled above in bold 13px: "mean=115 (in the valley!)".
- **Peak labels** (17px `#8e44ad`): "Normal ~90" near bin 3, "Pre-diabetic ~140" near bin 12.
- **Verdict block** (top-right at x=w−200): bold 13px red "ALL: FAIL (bimodal)"; 17px `#555` lines "dip test p=0.001", "skew=0.15 (looks fine!)", "kurt=1.9 (looks fine!)".
- **Captions (bottom center):** 17px `#555` "Skewness/kurtosis alone would have missed this"; bold 13px `#1a5276` "Fasting Glucose: bimodal, n=200 → NO TRUST (split subpopulations)".

## Callout (philosophy box)

**The Principle:** Multiple interpretations of a feature's shape are legitimate. Run them all. Validate them all. Trust only what holds up across independent evidence. The confidence in the precondition flows through to the confidence in the final answer.

## Regeneration instructions

- **Layout:** h1, subtitle, `.toc` box (background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, radius 4px, `<ol>` of `#2980b9` anchor links), "The Problem" h2 with two paragraphs, "The Multi-Candidate Approach" h2, then one `.obj-table` with rows for sections 1–9 (each `<tr>` has an `id` anchor). Then h2 "11. Real-World Example: Medical Claims Data" with an intro paragraph and a second `.obj-table` with rows for Features A/B/C. Finally the `.philosophy` callout.
- **obj-table:** full width, border-collapse; each `<td>` border `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; left cell 45% with `.obj-title` (1.05em, weight 600, `#1a5276`) + bullets (`ul` 0.9em `#333`) / paragraphs; right cell 55%, centered, holds the canvas. Even rows background `#fafcfe`. `strong` renders `#1a5276`.
- **Page style:** body -apple-system/system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; subtitle `#666` 1.05em. `.philosophy`: background `#f0f4f8`, left border `4px solid #2980b9`, padding 16px 20px, 1em. No nav bar, no back/home links.
- **Canvases:** intrinsic `width`/`height` attributes as given per chart (all 720 wide); scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper. Density overlays use the shared Gaussian-smoothed-bins approach (sigma in bin units, kernel radius 3σ, winsorize at 2× bar height, SE band = 1.96·smoothed/√effN with effN clamped to [30, 200]).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; extras `#c0392b`, `#8e44ad`, `#5b2c6f`, `#922b21`, `#1e8449`, gray text `#555`/`#333`.
