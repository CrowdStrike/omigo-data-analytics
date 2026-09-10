# KS test — Statistical Tests Reference

**Page type:** detail page (single-row two-column obj-table: text + real-world boxes left 45%, two stacked canvases right 55%; followed by a Quick Decision Table section)
**HTML title tag:** KS test — Statistical Tests Reference

**Subtitle:** Tests whether two samples come from the same continuous distribution

## Main content (left column)

**What it measures**

Whether two samples come from the same continuous distribution (maximum distance between empirical CDFs).

**Key assumptions**

- Data is continuous (no ties)
- Comparing against a fully specified distribution (parameters not estimated from data)
- Independent observations
- Sensitive to location, scale, and shape differences

**What breaks when violated**

- Ties (discrete/rounded data): test becomes conservative, power drops dramatically
- Parameters estimated from same data: p-values are too large (test is anti-conservative)
- Small samples: poor power for tail differences (max gap tends to be in the middle)

**Failure box (monospace, red):**

Income distributions (rounded to $1000), n1=200, n2=180:
KS test: D=0.08, p=0.42 (not significant)
Anderson-Darling (tie-corrected): p=0.006
60% of values are ties — KS loses almost all power.

**Alt-note box (green):**

**Use instead:** Anderson-Darling for better tail sensitivity. Cramer-von Mises for overall shape. For discrete data: Chi-squared or exact permutation tests. Epps-Singleton for data with ties.

**Real-world box 1 (🏦 Credit scoring: Score distribution drift):**

A bank monitors whether their credit score distribution has shifted between Q1 and Q2. Scores are integers from 300 to 850, creating massive ties (scores like 680, 700, 720 each have thousands of customers). KS test reports D=0.04, p=0.18 — "no drift." But Anderson-Darling with tie correction: p=0.002. The drift was concentrated in the 620-660 "near-prime" range where 15% of customers migrated downward — KS missed it because ties in popular score bands suppressed the CDF gap.

**Real-world box 2 (🏭 Manufacturing: Comparing supplier quality):**

A factory compares defect rates from two suppliers. Measurements are rounded to 0.1mm precision, so with tight tolerances (0.5mm range), there are only ~50 unique values across 3000 parts. KS test: D=0.03, p=0.35 — suppliers look identical. Permutation test on the raw (tied) data: p=0.004. Supplier B has a subtle rightward bias (0.02mm average) that matters when tolerance is ±0.25mm — the KS test's power collapse from 60% tie rate hid a real quality difference.

### Visualization (canvas `c4`, 960×460)

Two empirical CDF curves with the maximum vertical gap (D statistic) marked.

- **Title (bold 13px `#1a5276`, top center):** "KS Test: Maximum Distance Between CDFs"
- **Axes:** plot x=80 to width−40, y=40 to height−50; light `#ccc` L axes; x label "Income ($k)" (17px `#666`, bottom center); rotated y label "CDF"; y ticks 0.00–1.00 every 0.25 (17px `#999`) with faint `#f0f0f0` gridlines.
- **Data (15 evenly spaced points each):** Group 1 CDF (blue `#2980b9`, width 3.5): `[0, 0.02, 0.08, 0.18, 0.32, 0.48, 0.62, 0.74, 0.83, 0.89, 0.93, 0.96, 0.98, 0.99, 1.0]`. Group 2 CDF (orange `#e67e22`, width 3.5): `[0, 0.01, 0.03, 0.08, 0.15, 0.25, 0.38, 0.52, 0.65, 0.76, 0.85, 0.91, 0.95, 0.98, 1.0]`.
- **Max gap marker:** vertical dashed red line (`#e74c3c`, dash 5/3, width 3) at the index of maximum |CDF1−CDF2| (index 6, gap 0.24), with bold 13px red label "D = 0.24" beside it at gap midpoint.
- **Caption (17px red, bottom center):** "With ties (rounded data): D underestimated, power lost"
- **Legend (top right, 17px):** blue swatch "Group 1 CDF"; orange swatch "Group 2 CDF".

### Visualization (canvas `c4r`, 960×300, 20px top margin)

Credit-score drift CDF chart: Q1 vs Q2 CDFs with highlighted drift zone and result captions.

- **Title (bold 16px `#1a5276`, top center):** "🏦 Credit Score Drift: KS Misses Shift in Near-Prime Band (Ties = 65%)"
- **Axes:** plot x=80 to width−60, y=40 to height−50; light `#ddd` L axes; x tick labels at scores `500, 550, 600, 620, 640, 660, 680, 700, 720, 750, 800, 850` (13px `#666`, evenly spaced), x-axis label "Credit Score" (14px).
- **Data (12 points):** Q1 baseline CDF (blue `#2980b9`, width 3): `[0.02, 0.06, 0.14, 0.22, 0.32, 0.44, 0.58, 0.72, 0.84, 0.92, 0.97, 1.0]`. Q2 drifted CDF (orange `#e67e22`, width 3): `[0.02, 0.07, 0.17, 0.28, 0.40, 0.52, 0.62, 0.74, 0.85, 0.93, 0.97, 1.0]`.
- **Drift zone:** shaded rectangle from index 2 to index 5 (scores 600–660), fill `rgba(231,76,60,0.08)`, dashed red outline (dash 3/3, width 1); 13px red label at zone top center: "Drift zone: 600-660".
- **Max gap marker:** vertical dashed red line (dash 4/3, width 2) at the max-gap index (index 4, gap 0.08), bold 14px red label "D=0.08".
- **Legend (top right, 14px `#333`):** blue swatch "Q1 (baseline)"; orange swatch "Q2 (drifted)".
- **Result captions (14px, bottom center):** red `#e74c3c` "KS: D=0.04, p=0.18 (ties suppress gap) — MISSED"; green `#27ae60` "Anderson-Darling (tie-corrected): p=0.002 — caught 15% near-prime migration".

## Quick Decision Table

| Data Situation | If Assumptions Met | If Violated | Universal Fallback |
|---|---|---|---|
| Two distributions same? | KS test (continuous, no ties) | Ties > 20%: power collapses | Anderson-Darling / Permutation |

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, then a single-row `.obj-table` (full width, one `<tr>`): left `<td>` (45%) holds `.obj-title` headings, paragraphs, bullets, `.failure`, `.alt-note`, and both `.real-world` boxes; right `<td>` (55%, centered) holds canvases `c4` and `c4r` stacked (`c4r` with 20px top margin). Below the table, an h2 "Quick Decision Table" with a `.decision-table`.
- **Page CSS:** body -apple-system/'Segoe UI' sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6, 0.95em base; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-table` td 1px `#e0e0e0` border, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `code` on `#e8f0f8`; canvases block-centered.
- **Callout boxes:** `.failure` — background `#fdedec`, left border 3px `#e74c3c`, monospace ('SF Mono'/'Fira Code'), color `#922`, 0.85em. `.alt-note` — background `#eafaf1`, left border 3px `#27ae60`, color `#1a5276`, 0.85em. `.real-world` — background `#fef9e7`, left border 4px `#e67e22`, 0.88em; `.domain` heading weight 600 `#7d6608`; `strong` inside `#e67e22`.
- **Decision table:** `.decision-table` — th background `#1a5276` white text; td 1px `#e0e0e0` border; even rows `#fafcfe`; column 3 text `#e74c3c`, column 4 text `#27ae60` weight 500.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`.
- **Canvas:** intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper. No nav bar, no back/home links.
