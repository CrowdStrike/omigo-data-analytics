# HR / People Analytics

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** HR / People Analytics - Domain-Specific Data Pitfalls

**Subtitle:** Five critical pitfalls unique to workforce data science, where predictions change behavior, proxies encode bias, and sample sizes vanish under segmentation.

## Callout (philosophy box)

**Core tension:** HR analytics operates on people who know they are being measured, whose managers act on predictions, and whose protected attributes are encoded in seemingly neutral features. The feedback loops are shorter and the ethical stakes higher than in most domains.

## Self-Fulfilling Prophecy in Attrition Models

**Obj-title:** Predict Leave → Treat Differently → They Leave

When a model flags an employee as "flight risk," managers unconsciously (or consciously) withdraw investment: fewer stretch assignments, less mentoring, exclusion from succession planning. The employee notices the change in treatment, disengages, and eventually leaves.

- **The trap:** Model accuracy improves over time because predictions cause the outcome
- **Concrete example:** 72% of "high flight risk" employees who left cited "lack of growth opportunities"
- **The irony:** Growth opportunity is the very thing withdrawn from the employee after the flag
- **Mitigation:** Track whether flagged employees receive fewer opportunities once the flag is visible
- **Holdout test:** Run an A/B test where some flight-risk flags are withheld from managers entirely

### Visualization (canvas `canvas1`, 720×260)

Circular feedback-loop diagram with four stages.

- **Loop:** circle centered at (360,130), radius 85, drawn as four red `#e74c3c` arc segments (width 2.5) with arrowheads, connecting the stages clockwise.
- **Stage labels** (bold 14px, two lines each, positioned outside the circle at top / right / bottom / left):
  - Top: "Model predicts" / ""flight risk"" in `#1a5276`
  - Right: "Manager gives" / "fewer projects" in `#e67e22`
  - Bottom: "Employee" / "disengages" in `#e74c3c`
  - Left: "Employee" / "leaves" in `#e74c3c`
- **Center label (bold 13px, `#e74c3c`, two lines):** "SELF-FULFILLING" / "LOOP".
- **Bottom label (italic 12px, `#27ae60`, centered at (360, 258 area — cy+128)):** "Model "confirmed" ✔".

## Proxy Discrimination via Correlated Features

**Obj-title:** Removed "Gender" but Zip Code Does the Same Job

Removing protected attributes from the model is necessary but insufficient. Proxy variables - zip code, university name, commute distance, part-time status - carry heavy correlations with race, gender, and socioeconomic class.

- **zip_code:** correlates 0.82 with race in most US metro areas
- **university_tier:** correlates 0.75 with family socioeconomic status
- **part_time_flag:** correlates 0.68 with gender (female)
- **commute_distance:** correlates 0.61 with race due to housing patterns
- **Mitigation:** Measure disparate impact on outcomes and use adversarial debiasing during training
- **Feature audit:** Audit feature importance for proxy effects before the model reaches production

### Visualization (canvas `canvas2`, 720×260)

Horizontal correlation bar chart with a danger-threshold line.

- **Title (bold 17px, `#1a5276`, centered at (360,22)):** "Proxy Correlation with Protected Attributes".
- **Bars** (x from 180, max width 380 = correlation 1.0; height 32, gap 18, starting y=40; background track `#f0f0f0`; bar fill at 0.7 alpha; bold 14px `#1a5276` feature name right-aligned left of bar; white bold value inside bar right edge; 13px `#666` proxy label after bar):
  - `zip_code` — 0.82, `#e74c3c`, "→ Race"
  - `university_tier` — 0.75, `#e67e22`, "→ Socioeconomic"
  - `part_time_flag` — 0.68, `#e74c3c`, "→ Gender"
  - `commute_dist` — 0.61, `#e67e22`, "→ Race"
- **Threshold:** vertical dashed 4/4 `#e74c3c` line (width 1.5) at correlation 0.7 spanning all bars, 11px centered label below: "Danger threshold (0.7)".
- **Bottom note (italic 12px, `#666`, centered at (360,248)):** ""Gender" removed from model, but proxies encode it anyway".

## Performance Reviews Are Mostly Noise

**Obj-title:** Same Person, Different Score, Different Manager

Research consistently shows that 50-60% of variance in performance ratings reflects the rater, not the ratee. When you build models on performance scores, you are modeling manager psychology more than employee capability.

- **Example:** One employee rated by 5 managers gets: 3.2, 4.5, 2.8, 4.1, 3.6
- **Manager variance:** SD = 0.67 between managers for same employee
- **Employee variance:** SD = 0.31 between employees with same manager
- **The ratio:** Inter-rater variance is 2.2x the inter-ratee variance
- **Mitigation:** Use multiple raters, calibration sessions, or behavioral anchors to average rater noise
- **Hard rule:** Never treat single-rater performance scores as ground truth for a downstream model

### Visualization (canvas `canvas3`, 720×260)

Two overlapping Gaussian curves over a 1–5 rating axis, plus individual score dots.

- **Title (bold 17px, `#1a5276`, centered at (360,22)):** "Variance Decomposition: Manager vs Employee".
- **X axis:** `#333` line at y=200 from x=80 to x=640; ticks and 12px labels at ratings 1–5 (140px per rating unit).
- **Curves** (Gaussian shape, peak height 120px above axis, both mean 3.64):
  - Between-manager (wide): sigma 0.67, stroke `#e74c3c` width 2.5, fill `rgba(231,76,60,0.15)`
  - Between-employee (narrow): sigma 0.31, stroke `#1a5276` width 2.5, fill `rgba(26,82,118,0.15)`
- **Score dots:** red `#e74c3c` dots (radius 5) just above the axis at ratings `[3.2, 4.5, 2.8, 4.1, 3.6]`, each with its value in bold 13px red above.
- **Legend (13px, swatches at x=100, y=42 and y=60):** red swatch "Between-manager variance (SD=0.67) - SAME employee"; blue swatch "Between-employee variance (SD=0.31) - SAME manager".
- **Callout (bold 14px, `#e74c3c`, centered at (360,240)):** "Manager noise is 2.2x larger than true employee differences".

## Survivorship Bias in "Top Performer" Studies

**Obj-title:** You Only See Winners Who Stayed

Studies of "what makes top performers successful" only examine survivors. People with identical traits who failed or left are invisible. When you conclude "top performers all have trait X," you never checked whether failed employees also had trait X.

- **Example:** "92% of our top sellers have assertiveness scores > 80th percentile"
- **What's missing:** 85% of people who washed out in the first year also scored above the 80th percentile
- **Same trait:** That washout group scored high on the same assertiveness measure as the top sellers
- **The illusion:** Trait X appears predictive because we only look at one side
- **Mitigation:** Track everyone from their hire date, including all employees who later leave
- **Comparison rule:** Compare traits across all outcomes, not just the successes that stayed visible

### Visualization (canvas `canvas4`, 720×260)

Two groups of person-dots separated by a dashed "visibility wall".

- **Title (bold 17px, `#1a5276`, centered at (360,22)):** "Survivorship Bias in Talent Studies".
- **Left group (centered x=180):** header bold 14px `#27ae60` "WHAT WE SEE", sub 12px `#666` "Top performers (n=50)". 12 solid green `#27ae60` circles (radius 14, 4 columns, 36px spacing) each with white bold "X" inside. Below: bold 13px green "92% have Trait X" and 12px ""Trait X predicts success!"".
- **Right group (centered x=540):** header bold 14px `#e74c3c` "WHAT WE DON'T SEE", sub 12px `#666` "Failed/left early (n=200)". 12 red `#e74c3c` circles at 0.4 alpha, same layout, white "X" inside. Below: bold 13px red "85% ALSO have Trait X" and 12px "But they failed anyway!".
- **Divider:** vertical dashed 5/5 `#999` line (width 1.5) at x=360 from y=65 to y=245, with rotated italic 11px `#999` label "VISIBILITY WALL".

## Small n After Segmentation

**Obj-title:** 10,000 Employees Sounds Like a Lot Until You Segment

HR analytics inevitably requires segmentation by department, level, location, tenure band, and demographics. A seemingly large workforce rapidly fragments into cells too small for reliable inference.

- **Start:** 10,000 employees
- **÷ 8 departments:** ~1,250 each
- **÷ 5 levels:** ~250 each
- **÷ 4 locations:** ~62 each
- **÷ 2 tenure bands:** ~31 each
- **Result:** Cells of 15-30 people, where a single departure is a 3-6% attrition "spike"
- **Mitigation:** Use hierarchical/mixed-effects models and report confidence intervals on every segment
- **Reporting floor:** Refuse to report on cells that fall below the agreed minimum n threshold

### Visualization (canvas `canvas5`, 720×260)

Vertical funnel of trapezoids narrowing as segmentation multiplies.

- **Title (bold 17px, `#1a5276`, centered at (360,22)):** "How 10,000 Employees Vanish Under Segmentation".
- **Funnel:** centered at x=360, top y=48, total height 180 split into 5 equal steps. Each stage is a trapezoid from its width to the next stage's width, stroked `#1a5276` (last stage `#e74c3c`) width 1, with bold 13px stage label centered inside (white for the first two, `#1a5276` for middle, `#e74c3c` for last) and bold 14px "n ≈ N" count to the right:
  - "Total Workforce" — width 600, n ≈ 10,000, fill `rgba(26,82,118,0.2)`
  - "÷ 8 Departments" — width 460, n ≈ 1,250, fill `rgba(26,82,118,0.35)`
  - "÷ 5 Levels" — width 320, n ≈ 250, fill `rgba(26,82,118,0.5)`
  - "÷ 4 Locations" — width 200, n ≈ 62, fill `rgba(26,82,118,0.65)`
  - "÷ 2 Tenure Bands" — width 110, n ≈ 31, fill `rgba(231,76,60,0.3)` (red, highlighted)
- **Warning (bold 13px, `#e74c3c`, centered at (360,248)):** "⚠ 1 departure = 3% "spike" — not statistically meaningful".

## Regeneration instructions

- **Layout:** standard detail page. h1 + `.subtitle` + one `.philosophy` callout, then one `<h2>` per pitfall followed by an `.obj-table` (full-width, border-collapse) with a single `<tr>`: left `<td>` (45%) holds `.obj-title`, an intro paragraph, and a labeled-bullet `<ul>`; right `<td>` (55%, centered) holds the canvas. Even rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; paragraphs 0.95em `#333`; bullets 0.9em; cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvases:** each 720×260 intrinsic; shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Chart titles bold 17px -apple-system.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#333`/`#666`/`#999`, fills `rgba(26,82,118,α)` and `rgba(231,76,60,α)`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
