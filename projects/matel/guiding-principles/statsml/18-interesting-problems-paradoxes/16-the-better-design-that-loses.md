# The Better Design That Loses

**Page type:** detail page (h1, subtitle, opening philosophy callout, per-aspect numbered h2 + one-row obj-table: text left 45%, canvas right 55%; section 5 is a summary table; closing philosophy callout)
**HTML title tag:** The Better Design That Loses — Interesting Problems & Paradoxes

**Subtitle:** A redesign that is objectively better for someone learning it from scratch loses its A/B test — because nobody in the test is learning it from scratch.

## Callout (philosophy box, top)

**The paradox:** Fresh trainees are 15% faster on the new layout, yet the A/B test on current users shows it losing. Both numbers are correct — they measure different things.

## 1. The Setup: Strictly Better, Measurably Worse

**Obj-title:** Two True Numbers, One Wrong Conclusion

Math-box:

Fresh trainees, lab: `+15%` — the new design wins on time, errors, and satisfaction
A/B test on current users: `−12%` — "kill it"
`measured effect(t) = design quality − retraining cost(t)` — while the cost exceeds the quality, the better design loses

- **Lab says better:** train fresh recruits on both layouts and the new design wins outright.
- **Test says worse:** existing users lose speed — the test isn't broken and the users aren't lying.
- **Both are correct:** the test measures quality minus a retraining tax that decays over time.

### Visualization (canvas `c1`, 720×360)

Line chart: measured treatment effect over 12 weeks climbing from deeply negative toward the constant design-quality line.

- **Padding:** left 60, right 30, top 30, bottom 50. Light gray `#ccc` L-shape axes; light gray `#ddd` zero line across the plot.
- **Y-axis:** effect from −20% to +20%; labels (15px gray `#666`, right-aligned) "+20%", "+15%", "0%", "−10%", "−20%".
- **X-axis labels (centered):** "Wk 0", "Wk 3", "Wk 6", "Wk 9", "Wk 12" at fractions 0, 0.25, 0.5, 0.75, 1.0.
- **Design-quality line:** horizontal dashed green `#27ae60` line (dash 6/4, width 2) at +15%, labeled bold 15px green above its right half: "Design quality (fresh trainees): +15%".
- **Measured-effect line (blue `#1a5276`, width 3):** `[0, -20], [0.1, -14], [0.2, -9], [0.3, -5], [0.4, -2], [0.5, 0], [0.6, 2], [0.7, 5], [0.8, 8], [0.9, 11], [1.0, 13]`.
- **Test-readout marker:** vertical red `#e74c3c` dashed line (dash 5/4, width 2) at x-fraction 0.15 spanning the plot; bold 15px red two-line label just right of it, mid-height: "Typical test readout:" / "−12% → 'kill it'".
- **Gap annotation (14px gray `#666`, left-aligned near x-fraction 0.55, just below the measured line):** "gap = retraining cost, decaying".
- **X-axis title (15px gray, centered below axis):** "Measured Effect = Design Quality − Retraining Cost(t)".

## 2. The Resolution: Expert-vs-Novice, Not A-vs-B

**Obj-title:** What the Test Actually Compares

Math-box:

Incumbent with trained experts: `100`
Challenger at exposure zero: `85` — what the test sees
Challenger after retraining: `115` — what the decision needs, never observed

- **Expert-A vs novice-B:** every tester is trained on the incumbent and new to the challenger, so the incumbent's edge is muscle memory, not design.
- **The missing bar:** expert-B is the comparison the decision needs, and the test never runs it.

### Visualization (canvas `c2`, 720×320)

Three-bar chart of task throughput index showing the unfair comparison and the missing bar.

- **Baseline:** thin gray `#999` horizontal line from x=80 to x=660 at y=250; y is throughput (index 0 at baseline, 1.6px per index point).
- **Bar 1 (x=110, 140 wide):** "Design A" at height index 100; fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 2; bold 18px blue value "100" above; captions below (15px gray `#666`): "Incumbent" and "(users trained for years)".
- **Bar 2 (x=300, 140 wide):** "Design B today" at height index 85; fill `rgba(231,76,60,0.35)`, stroke `#e74c3c` width 2; bold 18px red value "85" above; captions: "Challenger, exposure 0" and "(everyone is a novice)".
- **Bar 3 (x=490, 140 wide):** "Design B retrained" at height index 115; fill `rgba(39,174,96,0.15)`, stroke `#27ae60` width 2 dashed (dash 6/4); bold 18px green value "115" above; captions: "Challenger, retrained" and "(the test never sees this)".
- **Comparison brackets:** bold 15px red `#e74c3c` centered at (275, 60): "the test compares these two"; small red arrows or a thin red line linking the tops of bars 1 and 2. Bold 15px green `#27ae60` centered at (395, 30): "the decision needs these two"; thin green dashed line linking the tops of bars 1 and 3.
- **Title (bold 17px `#2a2a2a`, centered at (360, 20)):** none (brackets serve as annotation; keep top clear).
- **Takeaway (bold 15px `#1a5276`, centered at (360, h−10)):** "The incumbent's edge is your training, not its design".

## 3. The Second Twist: The Pooled Line Moves While Nobody Changes Their Mind

**Obj-title:** Composition Drift

Math-box:

New users: `+6%`, constant for six months
Tenured users: `−4%`, constant for six months
Pooled effect: `−3% → +3.5%` — only the mix changed

- **Aggregate motion, zero individual change:** new users keep joining, so the pooled line climbs while every cohort holds still.
- **The false story:** read backwards it becomes "users are warming up to it" — a temporal cousin of Simpson's paradox.

### Visualization (canvas `c3`, 720×340)

Line chart over 6 months: two perfectly flat cohort lines with a rising pooled line between them.

- **Padding:** left 60, right 30, top 30, bottom 60. Light gray `#ccc` L-shape axes; light gray `#ddd` zero line.
- **Y-axis:** effect from −6% to +8%; labels (15px gray, right-aligned) "+8%", "+6%", "0%", "−4%", "−6%".
- **X-axis labels (centered):** "Month 0" to "Month 6" at fractions 0, 1/6 … 1.0 (label every other: "Month 0", "Month 2", "Month 4", "Month 6").
- **New-user line (green `#27ae60`, width 2.5):** flat at +6% across the plot; 14px green label above its left end: "New users: +6% (constant)".
- **Tenured line (red `#e74c3c`, width 2.5):** flat at −4% across the plot; 14px red label below its left end: "Tenured users: −4% (constant)".
- **Pooled line (blue `#1a5276`, dashed 6/4, width 3):** `[0, -3], [0.17, -2.2], [0.33, -1.2], [0.5, 0], [0.67, 1.2], [0.83, 2.4], [1.0, 3.5]`.
- **Annotation (bold 15px blue `#1a5276`, centered near x-fraction 0.6 above the pooled line):** "Pooled climbs — only the MIX changed".
- **X-axis title (15px gray, centered below axis):** "Composition drift: a temporal cousin of Simpson's paradox".

## 4. The Century-Old Analogue: QWERTY and the Price of Switching

**Obj-title:** The Switching Dip

Math-box:

Familiar layout: `60 wpm`
Day one on the new layout: `~25 wpm`
Break-even: `~week 7` — most switchers quit well before it

- **Everyone crosses alone:** performance craters immediately, the payoff sits weeks away, and most quit inside the dip.
- **Lock-in without a villain:** whether alternative layouts are truly faster is debated — the dip alone keeps the incumbent in place.

### Visualization (canvas `c4`, 720×340)

Line chart: typing speed over 10 weeks — flat incumbent line vs a switcher's dip-and-recover curve.

- **Padding:** left 60, right 30, top 30, bottom 50. Light gray `#ccc` L-shape axes.
- **Y-axis:** speed 0–80 wpm; labels (15px gray, right-aligned) "80", "60", "40", "20" at those values.
- **X-axis labels (centered):** "Wk 0", "Wk 2", "Wk 4", "Wk 6", "Wk 8", "Wk 10" at fractions 0, 0.2, 0.4, 0.6, 0.8, 1.0.
- **Incumbent line:** horizontal dashed gray `#999` line (dash 4/4, width 1.5) at 60 wpm; 14px gray label above its left end: "Familiar layout: 60 wpm".
- **Switcher line (blue `#1a5276`, width 3):** `[0, 60], [0.1, 60], [0.12, 25], [0.2, 30], [0.3, 38], [0.4, 45], [0.5, 51], [0.6, 56], [0.7, 60], [0.8, 64], [0.9, 66], [1.0, 68]`.
- **Dip shading:** area between the switcher line and the 60-wpm line, from x-fraction 0.12 to 0.7 (where the curve is below 60), filled `rgba(231,76,60,0.10)`.
- **Dip label (bold 15px red `#e74c3c`, centered around x-fraction 0.38, mid-dip):** two lines: "Weeks of being worse —" / "most switchers quit in here".
- **Payoff label (bold 15px green `#27ae60`, right-aligned near the end of the curve, above it):** "The payoff sits past the quit point".
- **X-axis title (15px gray, centered below axis):** "Switching cost: the dip everyone must cross alone".

## 5. Every Reading of the Same Redesign

Summary table (`.summary-table`): one redesign, five contradictory-looking readings, all correct.

| Who you measure | What the number contains | Reading |
|---|---|---|
| Fresh trainees (lab) | Design quality alone | +15% — "clearly better" |
| Tenured users, week 1 | Design quality − full retraining tax | −20% — "disaster" |
| Tenured users, month 6 | Design quality − residual tax | +10% — "they adjusted" |
| Pooled, week 2 | Mostly-tenured mix, deep in transition | −12% — "kill it" |
| Pooled, month 6 | Mix shifted toward new users, most retrained | +8% — "ship it" |

## Callout (philosophy box, bottom)

**The general lesson:** Wherever users hold trained skill — keyboards, editors, internal tools, org processes — short experiments measure the switching cost more than the design. The number answers "how expensive is the transition?" while everyone reads it as "which is better?"

## Regeneration instructions

- **Layout:** detail page. h1, `.subtitle`, opening `.philosophy` callout, then per aspect: `<h2>N. Title</h2>` (h2 1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table` — left `<td>` (45%) holds `.obj-title` (1.05em weight 600 `#1a5276`), a `.math-box` of concrete numbers, and bold-labeled one-sentence bullets; right `<td>` (55%, centered) holds the canvas. Section 5 is a `.summary-table`; page closes with a `.philosophy` callout.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `strong` `#1a5276`; ul 0.95em `#333`. No nav bar, no back/home links.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Math box:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.95em; `code` background `#eef2f7`, padding 2px 6px, radius 3px.
- **Summary table:** `.summary-table` — 0.9em, th background `#f0f4f8` `#1a5276` padding 10px 14px left-aligned, td padding 10px 14px, borders `1px solid #e0e0e0`.
- **Canvas:** intrinsic sizes as given per chart; a shared `setup(id)` helper sizes the backing store to displayed CSS width × `window.devicePixelRatio`, scales the context, and all charts redraw on window resize (debounced).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#999`, accent `#2980b9`.
- **Links:** none on this page; grid cards linking here use the `.html` extension in regenerated HTML.
