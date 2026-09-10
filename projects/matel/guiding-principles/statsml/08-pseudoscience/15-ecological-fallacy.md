# Ecological Fallacy

**Page type:** detail page (single-row obj-table layout: text left ~40%, two stacked canvases right ~60%)
**HTML title tag:** Ecological Fallacy — Pseudoscience in Data Analysis

**Subtitle:** Group Stats Applied to Individuals

## Section: Group-Level Statistics Applied to Individuals (Simpson's Paradox Territory)

- **Country-level correlations:** "Countries with higher chocolate consumption win more Nobel Prizes!" Wealth drives both chocolate consumption and the education that produces Nobel Prizes. The correlation exists at country level but says nothing about individuals eating chocolate.
- **Demographic stereotyping:** "Group X averages $Y, so this individual from Group X earns $Y." Using the group mean to predict individual outcomes throws away all within-group variance, which is typically 10× larger than between-group variance.
- **ML fairness:** "Zip code Z has high default rates → deny credit to everyone in Z." Within that zip code, most people repay their loans. The aggregate hides the individual reality and penalizes individuals for group-level statistics.
- **Medicine:** "Drug works for the average patient." The average patient doesn't exist — the drug may work for 30% (strong responders) and do nothing for 70%. The "average effect" is not what any individual experiences.

**Why it's pseudoscience:** Aggregate data masks heterogeneity — what's true of the group is often false for most individuals in it, so applying group statistics to individual decisions is statistically invalid and often discriminatory.

### Visualization (canvas `c1`, 720×340)

Two overlapping dot clouds representing two groups whose means differ but whose individuals massively overlap.

- **Title (bold 17px, top center, `#1a5276`):** "Group Average ≠ Individual Reality".
- **Group A cloud:** 50 semi-transparent red dots (`rgba(231,76,60,0.3)`, radius 4) scattered deterministically around center (150, 120) with roughly ±130px horizontal and ±70px vertical spread (cosine/sine plus modular jitter pattern).
- **Group B cloud:** 50 semi-transparent green dots (`rgba(39,174,96,0.3)`, radius 4) using the same scatter pattern around center (450, 120), so the two clouds overlap in the middle.
- **Mean labels (bold 17px, centered):** red at (180, 190): "Group A mean: $45K"; green at (500, 190): "Group B mean: $65K".
- **Overlap annotation:** dashed orange line (`#e67e22`, dash 4/3, width 2) from (250, 80) to (380, 80), with orange 17px label above at (315, 75): "Massive individual overlap".
- **Caption (bold gray `#555` 17px, centered at y=225):** "Group means differ. Most INDIVIDUALS in both groups earn $40K-$70K. Mean ≠ members.".

### Visualization (canvas `c2`, 720×300)

Two overlapping filled bell curves with a small between-means gap arrow.

- **Title (bold 16px `#2c3e50`, centered at y=16):** "Between-group difference: 10%. Within-group variance: 80%. The group mean tells you almost nothing about any individual.".
- **Curves:** Gaussian density curves plotted over normalized x ∈ [0, 1] mapped to x = 50…50+(width−100), baseline at y = height−40:
  - Group A: mean 0.4, sigma 0.15 — fill `rgba(26,82,118,0.3)`, stroke `#1a5276` width 2.
  - Group B: mean 0.55, sigma 0.15 — fill `rgba(39,174,96,0.3)`, stroke `#27ae60` width 2.
- **Mean markers:** dashed vertical lines (dash 3/3, width 1) from baseline up to y=35 at each group mean, blue for A and green for B.
- **Gap arrow:** solid red (`#e74c3c`, width 2) horizontal line at y=30 connecting the two mean markers, with an arrowhead at the right end and bold red 17px label "10% gap" centered above.
- **Labels:** bold 18px "Group A" in blue at (50, baseline+15) and "Group B" in green at (130, baseline+15); centered `#2c3e50` 16px caption on the baseline row: "← 80% overlap: any individual could be from either group →".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title`, bullets, and the "Why it's pseudoscience" paragraph; right `<td>` (60%, centered) holds two stacked canvases (`c1` 720×340, `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; paragraphs `#333` 0.95em; `ul` 0.9em `#333` with 6px item spacing; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276` (bar/area fill `rgba(26,82,118,0.3)`), green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#555`, dark slate `#2c3e50`.
- In regenerated HTML, any card links use `.html` extensions.
