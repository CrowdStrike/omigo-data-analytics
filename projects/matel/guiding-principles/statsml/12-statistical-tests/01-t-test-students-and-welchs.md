# t-test (Student's and Welch's)

**Page type:** detail page (card-sections: Overview two-column layout table with text 45% / canvas 55%, full-width Real-World Examples canvas + callout boxes, Quick Decision table)
**HTML title tag:** t-test — Statistical Tests Reference

**Subtitle:** Tests whether two groups have significantly different means

Note: the source HTML has a top back link "← Statistical Tests Reference" to `../12-statistical-tests.html`; per project convention (no back/home links) it is not part of the regenerated page.

## Overview

**What it measures**

Whether two groups have significantly different means.

**Key assumptions**

- Data in each group is approximately normal (CLT helps at large n)
- Student's: equal variances; Welch's: allows unequal variances
- Observations are independent
- Continuous measurement scale

**What breaks when violated**

- Skew + small n: the mean is no longer the best location estimate; p-values become unreliable
- Heavy tails inflate variance estimates, reducing power (true differences go undetected)
- Outliers in small samples can flip the conclusion entirely

**Failure box (monospace, red):**

Income feature, n=45, skew=1.8:
t-test says p=0.12 (not significant)
Mann-Whitney says p=0.003 (highly significant)
The skewed tail pulled the mean toward the outliers, masking real group separation.

**Alt-note box (green):**

**Use instead:** Mann-Whitney U when skew > 1.0 and n < 100. Permutation test when distribution is unknown. Welch's over Student's always (no reason to assume equal variance).

### Visualization (canvas `c1`, 960×460)

Overlaid histograms of two groups with smoothed density lines, mean markers, and verdict captions.

- **Data (18 bins each):** Group A (normal): `[1,3,7,14,24,35,42,38,28,18,10,5,2,1,0,0,0,0]`; Group B (skewed): `[0,0,1,4,12,22,30,28,20,14,10,8,7,5,4,3,2,1]`. Y scale max 44; bars start at x=60 with bin width = (width−120)/18, drawn at 0.5 global alpha — Group A fill `#2980b9`, Group B fill `#e67e22`; bar baseline at h−50, max bar height h−90.
- **Density overlays:** Gaussian-smoothed (sigma 1.3) line for each histogram with a ±1.96·smoothed/√effN confidence band. Group A: line `#1a5276`, band `rgba(41,128,185,0.18)`, effN 200. Group B: line `#7f3b08`, band `rgba(230,126,34,0.18)`, effN 45. Line width 2.5.
- **Mean lines:** vertical dashed (dash 6/3, width 3.5) at each histogram's weighted mean bin: Group A in `#2980b9` labeled "Mean A" (bold 12px, above at y=32), Group B in `#e67e22` labeled "Mean B" (y=22). The two means fall close together.
- **Captions (17px, bottom center):** red `#e74c3c` "Means look close (t-test: p=0.12)"; green `#27ae60` "But bulk of data is clearly separated (Mann-Whitney: p=0.003)".
- **Legend (top right):** blue swatch "Group A (normal)", orange swatch "Group B (skewed)".
- **Top-left note (bold 13px `#1a5276`):** "Skew pulls the mean — t-test misled".

## Real-World Examples

### Visualization (canvas `c1r`, 960×340)

Overlaid histograms for the clinical-trial example: control vs treatment response times with non-responder cluster.

- **Title (bold 16px `#1a5276`, top center):** "🏥 Clinical Trial: Response Time (min) — Non-responders inflate mean"
- **Data (20 bins):** Control: `[0,1,2,4,8,14,22,30,35,38,35,28,20,12,6,3,1,0,0,0]`; Treatment: `[0,0,2,8,18,32,36,28,16,8,4,2,1,1,0,0,0,2,4,3]` (note the bump at bins 17-19 = non-responders). Y scale max 40; bars from x=60, baseline h−55, max height h−100, 0.45 global alpha — control fill `#95a5a6`, treatment fill `#27ae60`.
- **Density overlays** (same smoothing as c1, sigma 1.3): control line `#7f8c8d`, band `rgba(127,140,141,0.15)`, effN 40; treatment line `#1e8449`, band `rgba(39,174,96,0.15)`, effN 38.
- **Non-responder marker:** vertical dashed red line (dash 4/3, width 2) at bin 17; red 14px labels to its right: "5 non-responders" / "pull mean →".
- **Mean lines:** dashed (dash 6/3, width 2.5) at weighted mean bins: control gray `#7f8c8d` labeled "Control mean=45" (14px), treatment green `#27ae60` labeled "Treatment mean=38".
- **Captions (15px, bottom center):** red "t-test: p=0.09 (not significant) — non-responders inflated treatment mean"; green "Mann-Whitney: p=0.001 — drug works for 87% of patients".
- **Legend (top right, 14px):** gray swatch `#95a5a6` "Placebo (n=40)"; green swatch `#27ae60` "Treatment (n=38)".

**Real-world box 1 (🏥 Clinical trial: Drug response time):**

A pharma company tests whether a new analgesic works faster than placebo. Control group (n=40): response times ~normally distributed around 45 min. Treatment group (n=38): most patients respond in 20-30 min, but 5 non-responders cluster at 90+ min. The non-responders inflate the treatment mean to 38 min. Student's t-test: p=0.09 (fails to reject). The drug actually works for 87% of patients — Mann-Whitney catches this (p=0.001) because it compares *ranks*, not means pulled by outliers.

**Real-world box 2 (💰 A/B test: Revenue per user):**

An e-commerce team tests a checkout redesign. Revenue per user is wildly right-skewed (most users spend $0-$50, a few whales spend $2000+). With n=500 per group, the t-test gives p=0.23. But one whale in the control group shifts its mean by $4. After log-transforming revenue (or using Mann-Whitney), the redesign shows a genuine 12% median lift (p=0.008). The team almost killed a winning experiment because they trusted the mean.

## Quick Decision

| Data Situation | If Assumptions Met | If Violated | Universal Fallback |
|---|---|---|---|
| Two groups, continuous outcome | Welch's t-test | Skew > 1 or n < 30: means mislead | Mann-Whitney U / Permutation test |

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, then three `.card-section` blocks each with an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border): "Overview" (a `table.layout` row: `td.text-col` 45% with `.obj-title` headings, bullets, `.failure`, `.alt-note`; `td.viz-col` 55% with canvas `c1`), "Real-World Examples" (full-width canvas `c1r` followed by two `.real-world` boxes), "Quick Decision" (`.decision-table`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `code` on `#e8f0f8`; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Callout boxes:** `.failure` — background `#fdedec`, left border 3px `#e74c3c`, monospace ('SF Mono'/'Fira Code'), color `#922`, 0.85rem. `.alt-note` — background `#eafaf1`, left border 3px `#27ae60`, color `#1a5276`, 0.85rem. `.real-world` — background `#fef9e7`, left border 4px `#e67e22`, 0.88rem; `.domain` heading weight 600 `#7d6608`; `strong` inside `#e67e22`.
- **Decision table:** `.decision-table` — th background `#1a5276` white text; td 1px `#e0e0e0` border; even rows `#fafcfe`; column 3 text `#e74c3c`, column 4 text `#27ae60` weight 500.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`.
- **Canvas:** intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper. Density overlays use a shared `drawDensityLine` helper (Gaussian kernel smoothing, clamped near empty bins, 95% band from effective n). No nav bar, no back/home links.
