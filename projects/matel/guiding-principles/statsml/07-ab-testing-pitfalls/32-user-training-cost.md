# User Training Cost

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** User Training Cost — A/B Testing Pitfalls

**Subtitle:** Design Flaw — Tenured users aren't judging your new design. They're paying the cost of relearning it.

## Section 1: The Mechanism: Muscle Memory Is Real Capital

**Math box:**
What breaks on day 1: `click paths`, `spatial memory`, `keyboard flow`
What it costs: `+task time`, `+misclicks`, `−satisfaction`

- **Skill is capital**: built over hundreds of sessions, wiped overnight.
- **The dip is real**: objective task metrics regress, not just opinions.
- **Exposure clock**: relearning advances per use, not per calendar day.
- **Daily vs weekly**: power users retrain in days; light users, months.

**Correct approach**: judge the design where metrics settle, not in the dip.

**The tell**: task time spikes at launch, then improves with each exposure.

### Visualization (canvas `c1`, 720×340)

Line chart: average task time across the first 50 exposures to a redesign, spiking above the old-UI baseline and settling below it.

- **Padding:** left 60, right 30, top 30, bottom 50. Axes drawn as light gray `#ccc` L-shape (left + bottom).
- **Y-axis labels (16px gray `#666`, right-aligned):** "32s" near top, "26s" at 35% plot height, "20s" at 60%, "17s" at 75%. Y scale maps task time 14–34s to plot height (34 at top edge, 14 at bottom edge).
- **X-axis labels (centered):** "Exposure 0", "10", "20", "30", "40", "50" at fractions 0, 0.2, 0.4, 0.6, 0.8, 1.0 of plot width.
- **Old-UI baseline:** horizontal dashed gray line (`#999`, dash 4/4, width 1.5) at task time 20s, with right-aligned 15px gray label "Old UI: 20s" just above its left end.
- **Task-time line (blue `#1a5276`, width 3):** points as (x-fraction, seconds): `[0, 32], [0.06, 30], [0.12, 28], [0.2, 25.5], [0.3, 23], [0.4, 21], [0.5, 19.5], [0.6, 18.3], [0.7, 17.6], [0.8, 17.2], [0.9, 17], [1.0, 17]`.
- **Relearning zone label:** bold 16px red `#e74c3c` at x-fraction ~0.12, near the top: "Real efficiency loss (relearning)".
- **Settled label:** bold 16px green `#27ae60` at x-fraction ~0.72, just below the settled line: "Settles BELOW old UI → design is better".
- **Crossing marker:** small solid green dot (radius 5) where the line crosses the 20s baseline (x-fraction ~0.47).
- **X-axis title (16px gray, centered below axis):** "Task Time by Exposure Count (not calendar time)".

## Section 2: Many Clocks: Every Segment Relearns at Its Own Speed

**Math box:**
Burn-in drivers: `usage frequency` > `tenure` > `age`
Age is confounded with both — check exposure counts first

- **Heavy users**: retrain in days — first to flip positive.
- **Light users**: still lost at week 12 — the last clock.
- **Pooled illusion**: flat average while every cohort moves.
- **Shifting blend**: the mix changes weekly; stability is fake.

**Correct approach**: declare equilibrium per cohort, never on the pooled curve.

**The tell**: stable pooled effect, cohort curves converging from opposite sides.

### Visualization (canvas `c2`, 720×340)

Line chart: treatment effect over 12 weeks for three usage cohorts converging at different times, with a deceptively flat pooled line.

- **Padding:** left 60, right 30, top 30, bottom 60. Light gray `#ccc` L-shape axes.
- **Y-axis:** effect from −8% to +6%; labels (16px gray, right-aligned) "+6%", "+3%", "0%", "−4%", "−8%"; solid light gray `#ddd` horizontal zero line (width 1) across the plot at 0%.
- **X-axis labels (centered):** "Wk 0", "Wk 3", "Wk 6", "Wk 9", "Wk 12" at fractions 0, 0.25, 0.5, 0.75, 1.0.
- **Heavy-user line (green `#27ae60`, width 2.5):** `[0, -6], [0.08, -3.5], [0.17, 0], [0.25, 2.5], [0.33, 3.8], [0.5, 4], [0.75, 4], [1.0, 4]`.
- **Medium-user line (orange `#e67e22`, width 2.5):** `[0, -6], [0.17, -4.5], [0.33, -1.5], [0.42, 0], [0.5, 1.5], [0.67, 3.5], [0.83, 4], [1.0, 4]`.
- **Light-user line (red `#e74c3c`, width 2.5):** `[0, -6], [0.25, -5.5], [0.5, -4], [0.75, -1], [0.83, 0], [1.0, 2]`.
- **Pooled line (gray `#666`, dashed 6/4, width 2.5):** `[0, -6], [0.17, -3.5], [0.33, -1.8], [0.5, -0.8], [0.67, 0.2], [0.83, 1], [1.0, 3.2]`.
- **Legend (top-left inside plot, 14px):** color swatch lines for "Heavy users" (green), "Medium" (orange), "Light users" (red), "Pooled" (gray dashed).
- **Annotation (bold 15px gray `#666`, centered near the pooled line around x-fraction 0.55):** "Pooled looks 'settled' — every cohort is still moving".
- **X-axis title (16px gray, centered below axis):** "Treatment Effect by Cohort — each on its own clock".

## Section 3: The Clean Read: New Users Carry No Training

**Math box:**
New users read: `design` — Tenured users read: `design − retraining(t)`

- **New users**: zero retraining cost — the clean read.
- **Tenured users**: design plus retraining tax; can lose for weeks.
- **Sign flip**: new up + tenured down = retraining, not bad design.
- **Slope test**: shrinking deficit → parity; stable → real dislike.

**Correct approach**: decide on new-user equilibrium plus the tenured slope.

**The tell**: same metric, opposite signs across tenure segments.

### Visualization (canvas `c3`, 720×300)

Three-bar chart: treatment effect for new users, tenured users, and pooled, around a zero baseline.

- **Baseline:** thin gray `#999` horizontal zero line from x=90 to x=650 at y=170; 14px gray "0%" label to its left.
- **Scale:** 12px of bar height per 1% of effect.
- **New-users bar:** at x=130, 120px wide, +6% (72px above baseline); fill `rgba(39,174,96,0.35)`, stroke `#27ae60` width 2; bold 18px green value label "+6%" above; captions below the chart area in 15px gray `#666`: "New users" and "(design only)".
- **Tenured bar:** at x=310, 120px wide, −4% (48px below baseline); fill `rgba(231,76,60,0.35)`, stroke `#e74c3c` width 2; bold 18px red label "−4%" below the bar; captions: "Tenured users" and "(design + retraining)".
- **Pooled bar:** at x=490, 120px wide, −1% (12px below baseline); fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 2; bold 18px blue label "−1%" below; captions: "Pooled" and "(\"kill it\"?)".
- **Title (bold 17px `#2a2a2a`, centered at (360, 26)):** "Same Test, Three Readings".
- **Takeaway (bold 16px red `#e74c3c`, centered at (360, h−12)):** "The pooled −1% hides a +6% design and a temporary retraining tax".

## Section 4: When Equilibrium Is Untestable: Big Redesigns in the Wild

**Math box:**
Snapchat 2018: `1M-signature petition`, `DAU decline`, partial revert
Instagram 2018: `horizontal feed`, pulled the same day

- **Transition > test**: relearning outlasts any feasible window.
- **Bundling**: dozens of changes at once — nothing attributable.
- **Staged rollout**: ship cohort by cohort, watch each clock.
- **Opt-in preview**: users self-pace the transition.
- **Long-term holdback**: the only true equilibrium measurement.

**Correct approach**: ship staged; measure against a months-long holdback.

**The tell**: every big-redesign test "fails," so the org bans redesigns.

### Visualization (canvas `c4`, 720×300)

Timeline chart: treatment effect over 24 weeks with the feasible test window shaded and equilibrium arriving far outside it.

- **Padding:** left 60, right 30, top 30, bottom 50. Light gray `#ccc` L-shape axes.
- **Y-axis:** effect from −8% to +4%; labels (15px gray, right-aligned) "+4%", "0%", "−4%", "−8%"; light gray `#ddd` zero line across the plot.
- **X-axis labels (centered):** "Wk 0", "Wk 6", "Wk 12", "Wk 18", "Wk 24" at fractions 0, 0.25, 0.5, 0.75, 1.0.
- **Test-window shading:** rectangle from x-fraction 0 to 0.25 over the full plot height, fill `rgba(231,76,60,0.08)`; bold 15px red `#e74c3c` label centered in it near the top: "Feasible test window" and a second line "effect negative throughout".
- **Effect line (blue `#1a5276`, width 3):** `[0, -7], [0.125, -5.5], [0.25, -4], [0.375, -2.5], [0.5, -1], [0.583, 0], [0.667, 1], [0.75, 1.8], [0.875, 2.3], [1.0, 2.5]`.
- **Equilibrium marker:** vertical green `#27ae60` dashed line (dash 5/4, width 2) at x-fraction 0.583 spanning the plot; bold 15px green label just right of it near the top: "Crosses zero at week 14 — no test runs this long".
- **Holdback annotation (bold 15px `#1a5276`, bottom-right of plot area, right-aligned):** "Measure this via a long-term holdback, not a test".
- **X-axis title (15px gray, centered below axis):** "Big redesign: the transition outlives the test".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + math-box + bullets, right `<td>` (60%, centered) holds the canvas.
- **Density elements:** each section opens with a `.math-box` (bare example list, `code` pills, `<br>`-separated lines); bullets are one-line labeled items with a bold lead label (`strong`, house blue); **Correct approach** and **The tell** are bold-labeled single lines.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; ul 0.95em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `.math-box` background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 12px 16px, 0.95em, `code` pills background `#eef2f7` at 1em. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; a shared `setup(id)` helper sizes the backing store to displayed CSS width × `window.devicePixelRatio`, scales the context, and all charts redraw on window resize (debounced).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- **Links:** none on this page; if this spec is linked from a grid, regenerated HTML card links use `.html` extensions.
