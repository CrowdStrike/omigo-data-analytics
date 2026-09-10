# ANOVA (F-test)

**Page type:** detail page (two-column obj-table layout: text left 45%, two stacked canvases right 55%, plus two philosophy callouts below)
**HTML title tag:** ANOVA (F-test) — Statistical Tests Reference

**Subtitle:** Tests whether means differ across 3+ groups simultaneously

## Overview

**Tests whether means differ across 3+ groups simultaneously**

- **What it measures:** Are the group means all equal, or does at least one differ? Compares between-group variance to within-group variance.
- **Assumes normality:** Residuals within each group must be normally distributed. Skewed groups → F-statistic is unreliable.
- **Assumes equal variance (homoscedasticity):** All groups must have similar spread. One tight group + one dispersed group → inflated Type I error. Levene's test checks this.
- **Assumes independence:** Observations are independent within and between groups. Repeated measures need repeated-measures ANOVA.
- **Sensitive to unequal group sizes:** Unbalanced designs (n₁=200, n₂=15, n₃=8) combined with unequal variance → results are unreliable in either direction (liberal or conservative).
- **Tells you SOMETHING differs, not WHAT:** Significant F-test only says "not all means are equal." Need post-hoc tests (Tukey, Bonferroni) to find which pairs differ — and these have their own multiple-comparison problems.

**Failure example (red paragraph, `#e74c3c`, 0.88em):** Comparing drug effect across 4 age groups. Group 1 (age 20-30, n=500): tight bell. Group 4 (age 70+, n=23): right-skewed (some patients respond extremely). ANOVA reports p=0.04 — but Welch's ANOVA gives p=0.18. The "significant" result was driven entirely by the variance inequality, not a real mean difference.

**Alternatives (green paragraph, `#27ae60`, 0.88em):** Non-normal → Kruskal-Wallis. Unequal variance → Welch's ANOVA. Only 2 groups → just use t-test/Welch. Post-hoc → Games-Howell (doesn't assume equal variance).

**Real-world callout 1 (`.real-world`, orange-left-border), domain line "📈 SaaS pricing: Conversion rate across plan tiers":**

A product team compares trial-to-paid conversion across 4 pricing tiers introduced at different times. Tier 1 (legacy, n=3200): stable 22% conversion, tight variance. Tier 4 (launched 2 weeks ago, n=45): volatile 31% conversion, huge variance from small sample. ANOVA says tiers differ significantly (p=0.02). But the F-test is dominated by Tier 4's inflated variance — Welch's ANOVA gives p=0.14. The team almost restructured pricing based on 45 data points with high variance masquerading as a real signal.

**Real-world callout 2 (`.real-world`), domain line "🎓 Education research: Teaching method effectiveness":**

A university compares exam scores across 5 teaching methods. Method A (traditional lecture, n=400): normally distributed, σ=8. Method E (self-paced online, n=28): bimodal — students either thrive (score 85+) or disengage (score 40-50), σ=22. ANOVA reports p=0.03 and the department nearly mandates Method A. Kruskal-Wallis: p=0.41. The "significant" difference was an artifact of Method E's bimodal distribution violating both normality and homoscedasticity simultaneously. The real story: Method E works brilliantly for motivated students and fails for others — a question ANOVA can't even ask.

### Visualization (canvas `c9`, 960×460)

Four vertical violin-style distributions (one per age group) showing the equal-variance assumption violated.

- **Title (17px `#555`, top center):** "ANOVA: equal-variance assumption violated across age groups".
- **Groups (left to right, equal-width columns starting at x=60):**
  - "Age 20-30", n=500, mean 120, σ=15, color `rgba(39,174,96,0.5)` (green), symmetric bell
  - "Age 30-50", n=300, mean 125, σ=20, color `rgba(41,128,185,0.5)` (blue), symmetric bell
  - "Age 50-70", n=80, mean 130, σ=28, color `rgba(230,126,34,0.5)` (orange), symmetric bell
  - "Age 70+", n=23, mean 135, σ=45, color `rgba(231,76,60,0.5)` (red), right-skewed (heavier lower tail: density exp(-0.5·d²)·0.7 for d>0 vs exp(-2·d²) otherwise)
- **Rendering:** each group drawn as 12 stacked horizontal density bars (centered, width proportional to density × 80% of column width) from y=80 over the chart height, with a smooth outline curve traced on both sides in the group color at full alpha (width 2.5). Group label in bold 15px `#1a5276`, "n=NNN" in 17px `#888` beneath.
- **Mean line:** dark blue `#1a5276` horizontal line (width 2) across each group at mid-height.
- **Spread bracket:** ±1SD vertical bracket (scaled σ/50 × 40% of chart height) at each group's right edge in the group color; "σ=NN" label in 17px `#555` below each group.
- **Bottom annotations (centered):** bold 15px red `#e74c3c`: "ANOVA says p=0.04 — but variance ratio is 9:1 (σ=45 vs σ=15)"; then 17px green `#27ae60`: "Welch's ANOVA (no equal-variance assumption) says p=0.18 → not significant".

### Visualization (canvas `c9r`, 960×340, 20px top margin)

Mean-and-variance band plot: conversion rate per pricing tier with confidence bands, exposing the high-variance small group.

- **Title (bold 16px, `#1a5276`, top center):** "📈 SaaS Pricing: ANOVA Fooled by High-Variance Small Group (Tier 4)".
- **Axes:** plot area x from 120 to w-80, y from 45 to h-80. Y axis "Conversion Rate" (rotated 13px `#666`) from 0% to 50% with gridlines (`#f0f0f0`) and right-aligned percent labels every 10%; light `#ddd` axis lines.
- **Tiers (evenly spaced columns; each drawn as a ±1σ filled band 40px wide around a 5px-radius mean dot, band fill in tier color at 0.5 alpha with 0.9-alpha stroke):**
  - "Tier 1 (Legacy)" — n=3200, rate 22%, σ=3%, green `rgba(39,174,96,0.5)`
  - "Tier 2" — n=800, rate 24%, σ=5%, blue `rgba(41,128,185,0.5)`
  - "Tier 3" — n=200, rate 20%, σ=8%, orange `rgba(230,126,34,0.5)`
  - "Tier 4 (New)" — n=45, rate 31%, σ=18%, red `rgba(231,76,60,0.5)`
- **Labels:** tier name (13px `#333`) and "n=NNNN, σ=NN%" (12px `#888`) below the axis; the rate percentage in bold 13px tier color above each band.
- **Tier 4 annotation:** dashed red (`#e74c3c`, dash 3/3) pointer line to two lines of 13px red text: "σ = 18% (6× others)" / "Only 45 observations!".
- **Bottom verdict (centered):** 14px red `#e74c3c`: 'ANOVA: p=0.02 — "Tier 4 is significantly better!" (driven by variance, not signal)'; 14px green `#27ae60`: "Welch's ANOVA: p=0.14 — not significant. Don't restructure pricing on n=45."; 13px gray `#666`: "Variance ratio: Tier 4 σ² / Tier 1 σ² = 36:1 — homoscedasticity assumption obliterated".

## When to Use ANOVA vs Alternatives

**Callout (`.philosophy` box):**

**Use ANOVA when:** You have 3+ groups, approximately normal data within each group, similar variances across groups, and independent observations.

**Use Welch's ANOVA when:** Variances are unequal across groups (more robust, almost always preferable).

**Use Kruskal-Wallis when:** Data is skewed, has outliers, or violates normality assumptions.

**Use t-test/Welch's t-test when:** Only comparing 2 groups (ANOVA is overkill).

**Post-hoc testing:** If ANOVA is significant, use Games-Howell (no equal variance assumption) or Tukey HSD (assumes equal variance) to find which specific pairs differ. Always correct for multiple comparisons.

**Callout (`.philosophy` box):**

**The Rule:** ANOVA has a strict contract: normality + equal variance + independence. Violate it, and the p-value means nothing. When in doubt, use Welch's ANOVA (more robust) or Kruskal-Wallis (non-parametric). Never trust ANOVA with small sample sizes in any group or wildly different variances.

## Regeneration instructions

- **Layout:** h1 + subtitle, `h2` "Overview" above a standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (45%) holds `.obj-title` heading, labeled bullet list, red failure paragraph, green alternatives paragraph, and both `.real-world` callouts; right `<td>` (55%, centered, vertical-align middle) holds canvas `c9` stacked above canvas `c9r` (`margin-top:20px`). Then `h2` "When to Use ANOVA vs Alternatives" followed by two `.philosophy` callout boxes (the second has no separate h2). No nav bar, no back/home links.
- **Page CSS:** body -apple-system/'Segoe UI' sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6, font-size 0.95em; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `border-bottom: 2px solid #2980b9`; subtitle `#666` 1.05em; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px; even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `code` on `#e8f0f8`.
- **Callout styles:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 16px 20px, 1em, paragraphs separated by `<br><br>`. `.real-world` — background `#fef9e7`, left border `4px solid #e67e22`, padding 10px 14px, 0.88em, `.domain` weight 600 `#7d6608`, `strong` `#e67e22`.
- **Canvas:** intrinsic sizes `c9` 960×460 and `c9r` 960×340; `display:block; margin:0 auto`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`, gray text `#666`/`#555`/`#888`.
