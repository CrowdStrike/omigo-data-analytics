# Education Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Education Data Pitfalls

**Subtitle:** Educational data is shaped by measurement effects, socioeconomic context, and the fundamental difficulty of observing learning itself — metrics that seem straightforward often measure something else entirely.

## Teaching to the Test / Goodhart's Law

**Obj-title:** Math Scores Rose 18% While Novel-Format Performance Held Flat

- When test scores become the target, they cease to be a good measure of learning
- Curricula narrow to tested material, sacrificing broader understanding and critical thinking
- Score inflation creates an illusion of improvement while actual competency remains flat
- Students learn test-taking strategies rather than subject mastery
- Year-over-year "gains" reflect familiarity with test format, not knowledge growth

**Example:** A district adopted high-stakes standardized testing with teacher bonuses tied to scores. Over 5 years, math scores rose 18%. When students took a novel assessment measuring the same concepts in unfamiliar formats, performance was unchanged from baseline.

### Visualization (canvas `canvas1`, 720×240)

Two-line chart: rising test scores diverging from flat actual learning.

- **Title (top center, `#1a5276`, 17px):** "Test Scores vs Actual Comprehension".
- **Data:** years `['2019','2020','2021','2022','2023','2024']`; standardized test scores `[62, 67, 72, 76, 79, 82]` (rising, red `#e74c3c`, width 2.5); novel assessment / actual learning `[61, 62, 60, 63, 61, 62]` (flat, blue `#2980b9`, width 2.5).
- **Axes:** margins top 30 / right 20 / bottom 40 / left 55; y-axis 50%–90% with labels every 10% and `#eee` gridlines; x-axis one label per year; axis lines `#333`.
- **Legend (top right, line swatches):** red line "Standardized Test Score"; blue line "Novel Assessment (Actual Learning)".
- **Annotation:** italic red 11px text right-aligned near the right edge (~55px below top margin): "Goodhart divergence".

## Engagement Does Not Equal Learning

**Obj-title:** 40% More Time on Platform, r = 0.12 With Test Performance

- Time-on-platform and click counts are easy to measure but weakly correlated with comprehension
- Students may spend hours on gamified features without meaningful knowledge acquisition
- High engagement can indicate confusion and struggle rather than productive learning
- Platforms optimize for engagement metrics because they drive revenue, not because they drive outcomes

**Example:** An adaptive learning platform reported students spending 40% more time per session after a gamification update. A controlled study found comprehension scores were statistically identical between gamified and non-gamified groups (r = 0.12 between time-on-platform and test performance).

### Visualization (canvas `canvas2`, 720×240)

Scatter plot showing near-zero correlation between time on platform and comprehension.

- **Title (top center, `#1a5276`, 17px):** "Time on Platform vs Comprehension Score (r = 0.12)".
- **Data:** 60 points generated with a deterministic pseudo-random function `pseudoRandom(s) = frac(sin(s) × 10000)`; time = 10 + pseudoRandom(i×3+1) × 90 (10–100 min); score = 30 + pseudoRandom(i×7+5) × 60 + (time − 50) × 0.05, clamped to [25, 95] — a deliberately weak relationship.
- **Points:** 4px-radius circles, fill `rgba(41, 128, 185, 0.5)`.
- **Axes:** margins top 30 / right 20 / bottom 40 / left 60; y-axis 20%–100% with labels every 20%, rotated y label "Comprehension"; x-axis 0–100 min with labels every 25 min ("0 min" … "100 min"), axis caption "Time on Platform (minutes/week)" bottom center.
- **Regression line:** nearly horizontal dashed red `#e74c3c` line (dash 5/5, width 2) from y=55 at the left edge to y=57 at the right edge (in score units).
- **Annotation (top right, bold red 13px):** "r = 0.12 (nearly no relationship)".

## Ceiling and Floor Effects

**Obj-title:** 73% Score Above 95% — The Test Cannot Measure Growth

- When assessments are too easy or too hard, scores pile up at extremes and lose discriminative power
- Top performers all score 95-100%, making it impossible to distinguish growth among advanced students
- Floor effects similarly mask differences among struggling students who all score near zero
- Intervention effects become invisible when instruments cannot detect change in the relevant range
- Aggregate statistics (means, gains) are distorted by truncated distributions

**Example:** A gifted program evaluation used grade-level tests where 73% of participants scored above 95%. The program appeared to have "no effect" because the assessment could not measure growth beyond the ceiling. When an above-grade-level assessment was administered, significant gains appeared.

### Visualization (canvas `canvas3`, 720×240)

Histogram with a massive pile-up at the top score bin (ceiling effect).

- **Title (top center, `#1a5276`, 17px):** "Score Distribution: Ceiling Effect in Advanced Class".
- **Bins and counts:** `40-50: 1`, `50-60: 2`, `60-70: 4`, `70-75: 5`, `75-80: 7`, `80-85: 9`, `85-90: 12`, `90-95: 18`, `95-100: 42` (massive pile-up).
- **Bars:** all `#2980b9` except the last bin (`95-100`) in red `#e74c3c`; bin range labels 10px under each bar; x-axis caption "Score Range".
- **Axes:** margins top 30 / right 20 / bottom 40 / left 55; y-axis 0–42 with labels at quarter steps (0, 11, 21, 32, 42), rotated y label "# Students".
- **Annotation (bold red 12px, three lines near the top-right above the last bar):** "42% pile up here" / "Cannot distinguish" / "top performers!" — with a red arrow (width 1.5) pointing down to the top of the last bar.

## Socioeconomic Confounds

**Obj-title:** School Rank Tracks ZIP-Code Income at r = 0.89

- School "quality" metrics overwhelmingly reflect student demographics rather than instructional effectiveness
- Neighborhood income predicts test scores more strongly than any school-level intervention
- Ratings and rankings reward schools for their intake population, not their value-added contribution
- Causal claims about pedagogy collapse when SES is properly controlled

**Example:** A state ranked schools by average proficiency rates. The correlation between school rank and median household income of the surrounding ZIP code was r = 0.89. Schools serving affluent areas scored highly regardless of instructional quality; schools in low-income areas scored poorly regardless of their growth metrics.

### Visualization (canvas `canvas4`, 720×240)

Scatter plot with strong positive correlation between neighborhood income and school rating.

- **Title (top center, `#1a5276`, 17px):** "School "Quality" Rating vs Median Neighborhood Income".
- **Data:** 45 points via `pseudoRandom(s) = frac(sin(s) × 10000)`; income = 30 + pseudoRandom(i×5+2) × 120 ($30k–$150k); rating = 20 + (income − 30) × 0.55 + noise where noise = (pseudoRandom(i×11+7) − 0.5) × 15, clamped to [15, 95] — a strong relationship.
- **Points:** 5px-radius circles, fill `rgba(41, 128, 185, 0.6)`.
- **Axes:** margins top 30 / right 20 / bottom 40 / left 60; y-axis 0–100 with labels every 25, rotated y label "School Rating"; x-axis $30K–$150K with labels every $30K ("$30K" … "$150K"), axis caption "Median Household Income" bottom center.
- **Regression line:** solid red `#e74c3c`, width 2, from rating 20 at the left edge to rating 86 at the right edge.
- **Annotations (top left):** bold red 13px "r = 0.89"; below it red 11px "Income explains 79% of "school quality"".

## Dropout Equals Missing Data

**Obj-title:** 60% Dropped Out in Two Weeks — Excluded From All Metrics

- Students who disengage or drop out generate the least data, creating systematic survivorship bias
- Platform analytics only reflect those who continue using the system, not those who left
- "Average student outcomes" improve simply because struggling students disappear from the dataset
- Interventions appear successful when they actually just accelerated attrition of difficult cases
- The students most in need of support are precisely those whose data you lack

**Example:** An online course reported 85% completion rates and high satisfaction scores. However, 60% of enrollees dropped out in the first two weeks and were excluded from all outcome metrics. The "successful" 40% were predominantly those who would have succeeded in any format.

### Visualization (canvas `canvas5`, 720×240)

Bar chart of data points generated per student group, inverse to need level.

- **Title (top center, `#1a5276`, 17px):** "Data Points Generated vs Student Need Level".
- **Bars (value = data points per student; bold "Need: X" label in the bar color above each bar; two-line group label below):**
  - "High Achievers" — 450 data points, Need: Low, green `#27ae60`.
  - "Average Students" — 280 data points, Need: Medium, yellow-orange `#f39c12`.
  - "At-Risk Students" — 90 data points, Need: High, red `#e74c3c`.
  - "Dropped Out" — 12 data points, Need: Highest, gray `#7f8c8d`.
- **Axes:** margins top 30 / right 20 / bottom 40 / left 55; y-axis 0–450 with labels at quarter steps (0, 113, 225, 338, 450 rounded), rotated y label "Data Points per Student".
- **Annotation (bold red 12px, top center offset right):** "Inverse relationship: most need = least data".

## Regeneration instructions

- **Layout:** standard detail-page structure: h1, `.subtitle` paragraph, then per pitfall an `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a `.obj-table` with a single `<tr>`: left `<td>` (40%) contains `.obj-title` + `<ul>` bullets + `.example` callout, right `<td>` (60%, centered) contains the canvas. Even table rows have background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; bullets 0.9em `#333`; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Example callout:** `.example` — background `#f0f4f8`, left border `3px solid #2980b9`, padding 10px 14px, 0.9em. (A `.philosophy` class with 4px border also exists in the stylesheet but is unused on this page.)
- **Canvas:** all canvases 720×240, declared with intrinsic `width`/`height` attributes and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), one IIFE per chart. Scatter data uses the deterministic `frac(sin(s) × 10000)` pseudo-random generator so charts reproduce identically.
- **Palette:** primary blue `#1a5276`, chart blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (plus `#f39c12` and `#7f8c8d` used in the dropout chart), text `#333`/`#666`.
