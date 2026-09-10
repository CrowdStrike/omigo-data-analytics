# Bragging Metrics

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + one-row table per pattern, even rows shaded)
**HTML title tag:** Bragging Metrics

**Subtitle:** A vanity metric is uninformative. A bragging metric is worse: it is usually <em>true</em>, and it was chosen because it is the most flattering true statement available. The defect is not in the arithmetic — it is in the selection of numerator, denominator, window, segment and framing. Every headline on this page is derived from one shared 12-month series, so the contradictions are all facts about the same data.

## The Shared Scenario (Illustrative Example)

**Obj-title:** One construction, declared once, never re-entered

Every number on this page comes from one construction, declared once and never re-entered:

- **The series:** monthly first-time activations of Feature X, months M1–M12: `2, 3, 5, 6, 9, 14, 20, 24, 22, 17, 12, 8`.
- **The total:** the twelve months sum to **142** activations; that is also the cumulative curve's last point.
- **The cohort:** the Feature X pilot contains **284** accounts, so the 142 activators are exactly **50.00%** of the pilot.
- **The split:** 142 adopters and 142 non-adopters, each split into heavy and light prior-usage tiers.
- **The wider platform:** 4,000 total accounts, 1,600 active in the last 90 days, 800 active weekly, 160 onboarding-trained admins.
- **The rule:** no headline below invents a number — each one selects a slice of the above and states it correctly.

### Visualization (canvas `bg0`, 720×340)

Bar chart of the shared series with every bar value printed.

- **Title (bold 16px `#1a5276`, top center):** "The Shared Series — Every Headline Comes From These 12 Numbers".
- **Layout:** margins left 55, right 30, top 52, bottom 58. Twelve bars, width 0.62 of a slot, y scale 0–28, gridlines `#e0e0e0` every 7 with 11px `#999` left labels.
- **Bars:** fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 1.5; each bar's value printed above it in bold 11px `#1a5276` from the array.
- **X labels:** "M1"…"M12" in 11px `#555`.
- **Summary strip (12px `#555`, left-aligned below the axis):** computed as `"total = " + TOTAL + "   ·   pilot = " + PILOT + "   ·   peak = " + PEAK + " at M" + (idx+1) + "   ·   current = " + LAST` (renders total = 142, pilot = 284, peak = 24 at M8, current = 8).
- **Bottom annotation (bold 14px `#c0392b`, centered):** "Nothing below is fabricated — only selected."

### Reconciliation table (must appear on the page)

| Headline (all true) | Slice selected | Arithmetic |
|---|---|---|
| "88.75% adoption" | numerator 142, denominator 160 | 142 / 160 |
| "3.55% adoption" | numerator 142, denominator 4,000 | 142 / 4,000 |
| "3× growth in one quarter" | M1 → M4 monthly level | 6 / 2, absolute +4 |
| "142 activations and climbing" | cumulative to M12 | running total of the series |
| "up 1,100%" | M1 → M8 monthly level | (24 − 2) / 2 |
| "down 66.7%" | M8 → M12 monthly level | (8 − 24) / 24 |
| "record 24 activations" | single peak month M8 | max of the series |
| "adopters retain 1.51× better" | 119/142 vs 79/142 | subset comparison |
| "fastest-growing in its class" | 1 of 2 after four filters | rank within narrowed class |

## Denominator Shopping

**Obj-title:** Same numerator, five true percentages, one chosen

- **The move:** Fix the numerator at the flattering count, then hunt for the smallest denominator that can still be named honestly.
- **Illustrative Example:** 142 accounts activated Feature X — that single numerator supports 3.55%, 8.88%, 17.75%, 50.00% and 88.75%.
- **Arithmetic:** 142/4,000 = 3.55%; 142/1,600 = 8.88%; 142/800 = 17.75%; 142/284 = 50.00%; 142/160 = 88.75%.
- **Why it is not a lie:** Each denominator names a real population, and each ratio is computed correctly from it.
- **The post-hoc tell:** "Engaged users" is defined <em>after</em> the numerator is known, so the qualifier is fitted to the answer.
- **The 25× spread:** The largest and smallest framings of the identical event differ by a factor of 88.75 / 3.55 = 25.0.
- **The audit question:** Was the denominator's definition written down before the numerator was measured, or after?

**Fix:** Require every rate to ship its numerator and denominator as literal counts, and require the denominator definition to be dated earlier than the reporting period it governs.

### Visualization (canvas `bg1`, 720×340)

Horizontal bars, one per candidate denominator, longest at the bottom.

- **Title (bold 16px `#1a5276`, top center):** "One Numerator (142), Five True Denominators".
- **Layout:** margins left 250, right 170, top 46, bottom 46. Five rows, bar height 30, vertical spacing derived from the plot height.
- **Rows (label, denominator):** "All platform accounts" 4000; "Active in last 90 days" 1600; "Active weekly" 800; "Feature X pilot segment" 284; "Onboarding-trained admins" 160.
- **Bar length:** proportional to `142 / d` on a 0–100% scale spanning the plot width; fill `rgba(26,82,118,0.35)` for the first three rows, `#e67e22` for row 4, `#e74c3c` for row 5 (the two most flattering framings); stroke `#1a5276` width 1.
- **Row labels:** right-aligned 13px `#555` at `left − 10`. A gray `#999` baseline runs vertically at x = left.
- **Value labels:** bold 13px `#1a5276` immediately right of each bar, text computed at render time as `"142/" + d + " = " + (142/d*100).toFixed(2) + "%"`.
- **Bottom annotation (bold 14px `#c0392b`, centered):** computed spread — `"Same event, widest framing / narrowest framing = " + ratio.toFixed(1) + "×"` (renders 25.0×).

## Ratio Versus Absolute

**Obj-title:** Percentage framing is selected exactly when the absolute is embarrassing

- **The asymmetry:** A relative change is unbounded above when the base is small, while an absolute change is bounded by reality.
- **Illustrative Example:** M1 → M4 rose 2 → 6 activations, and M7 → M8 rose 20 → 24 activations.
- **Identical absolutes:** Both moves are exactly +4 activations — the same amount of real-world adoption in each case.
- **Divergent ratios:** The first is 6/2 = 3.00× (+200%); the second is 24/20 = 1.20× (+20%).
- **The selection rule:** Report the ratio when the base is tiny, and the absolute when the base is large — whichever is bigger.
- **The reverse tell:** A report that switches units between adjacent paragraphs is choosing per paragraph, not per question.
- **The honest form:** State both — "+4 activations, from a base of 2" carries the size and the significance together.

**Fix:** Mandate paired reporting — absolute delta and base alongside every percentage — and reject any relative change whose base is below a pre-registered minimum.

### Visualization (canvas `bg2`, 720×340)

Two bar pairs, before/after, with computed absolute and ratio labels.

- **Title (bold 16px `#1a5276`, top center):** "Both Moves Are +4 Activations".
- **Layout:** margins left 60, right 40, top 50, bottom 60. Shared y scale 0 to 28, gridlines `#e0e0e0` every 7 with 11px `#999` left labels.
- **Group 1 (centered at 28% of plot width):** bars for M1 = 2 and M4 = 6, width 62, gap 12, fill `rgba(26,82,118,0.35)` with `#1a5276` stroke.
- **Group 2 (centered at 72%):** bars for M7 = 20 and M8 = 24, same widths and colors.
- **Bar value labels:** bold 13px `#1a5276` above each bar, printed from the array values.
- **Group captions (13px `#555`, below baseline):** "M1 → M4" and "M7 → M8".
- **Delta brackets:** for each group, an orange `#e67e22` bracket spanning the two bar tops, labeled in bold 13px `#e67e22` with text computed as `"+" + (b − a) + " activations"`.
- **Ratio callouts:** bold 15px under each group caption — group 1 in `#e74c3c`, group 2 in `#27ae60` — computed as `(b/a).toFixed(2) + "× (+" + ((b/a−1)*100).toFixed(0) + "%)"`.
- **Bottom annotation (bold 14px `#c0392b`, centered):** "Identical absolute change. The ratio is 2.5× larger only because the base was 10× smaller."

## The Cumulative Curve That Cannot Fall

**Obj-title:** A running total is monotone, so it can never report a decline

- **The structural fact:** A cumulative count of a non-negative quantity is non-decreasing by construction, so it has no vocabulary for a downturn.
- **Illustrative Example:** Activations peaked at 24 in M8 and fell to 8 by M12, while the cumulative curve rose from 83 to 142.
- **Arithmetic, same array:** Rate M9 → M12 is (8 − 22) / 22 = −63.6%; cumulative M8 → M12 is (142 − 83) / 83 = +71.1%.
- **Forced by arithmetic:** The bars and the line are drawn from one array — the cumulative is the running sum, so the divergence cannot be an artifact of drafting.
- **The slope is the signal:** The cumulative curve does encode the decline, as a flattening slope, which no chart axis label announces.
- **The brag phrasing:** "Total activations ever" and "X to date" are the give-away prefixes for a monotone metric.
- **The audit question:** Could this chart look worse next month than this month? If no, it cannot report bad news.

**Fix:** Pair every cumulative figure with its per-period rate on the same chart, and set alerts on the rate rather than the total.

### Visualization (canvas `bg3`, 720×340)

Bars for the monthly rate with the cumulative line overlaid on a second scale, both from the same array.

- **Title (bold 16px `#1a5276`, top center):** "Same Array: Monthly Rate Collapsing, Cumulative Still Rising".
- **Layout:** margins left 55, right 62, top 48, bottom 58. Twelve slots across the plot width.
- **Bars (monthly rate):** fill `rgba(26,82,118,0.35)`, stroke `#1a5276`, width = 0.6 of a slot, scaled to a left axis 0–28; left axis labels 11px `#999` every 7.
- **Line (cumulative):** `#e67e22`, width 3, drawn from the running sum of the same array, scaled to a right axis 0–150; right axis labels 11px `#e67e22` every 50. The cumulative array is computed in JS, never typed as a literal.
- **X labels:** "M1"…"M12" in 11px `#555`.
- **Peak marker:** dashed (4/3) `#e74c3c` vertical line at the argmax month, labeled in bold 12px `#e74c3c` above the plot as `"rate peaks: M" + (idx+1)`.
- **Two computed callouts (bold 13px):** in `#e74c3c`, `"rate M9→M12: " + pctRate.toFixed(1) + "%"`; in `#27ae60`, `"cumulative M8→M12: +" + pctCum.toFixed(1) + "%"`.
- **Legend (top right, 12px):** blue swatch "monthly activations", orange swatch "cumulative".
- **Bottom annotation (bold 14px `#c0392b`, centered):** "The line is the running sum of the bars. It rises while they fall — by construction."

## Window Shopping

**Obj-title:** The start date is a free parameter, so the headline is a choice

- **The move:** Hold the series fixed and search the start and end points for the pair that maximises the reported change.
- **Illustrative Example:** The same twelve months support "+1,100%" and "−66.7%" with no number altered.
- **Best window:** M1 → M8 monthly level, 2 → 24, giving (24 − 2) / 2 = +1,100%.
- **Worst window:** M8 → M12 monthly level, 24 → 8, giving (8 − 24) / 24 = −66.7%.
- **Aggregation is a second knob:** Q1 → Q4 totals give 10 → 37 = +270%; H1 → H2 totals give 39 → 103 = +164.1%.
- **The spread:** Eight defensible windows span 1,166.7 percentage points of reported change on identical data.
- **The tell:** A window whose boundaries match no calendar, fiscal or release cadence was fitted after the fact.
- **The audit question:** Was the window fixed before the data arrived, and does the report show the alternatives?

**Fix:** Pre-register the reporting window with the metric definition, and publish the full series alongside any windowed change so a reader can pick their own endpoints.

### Visualization (canvas `bg4`, 720×340)

Series line on the left, ranked table of every candidate window's computed change on the right.

- **Title (bold 16px `#1a5276`, top center):** computed as `wins.length + " Honest Windows, One Series"` (renders "8 Honest Windows, One Series").
- **Left panel (x 46 to 356):** the monthly series as a `#1a5276` line, width 2.5, with 3px dots; y scaled 0–28; x labels "M1", "M4", "M8", "M12" in 10px `#555`.
- **Window markers:** the best window drawn as a `#27ae60` bracket above the line spanning M1→M8; the worst as an `#e74c3c` bracket below spanning M8→M12.
- **Right panel (x 376 to 702):** eight rows, 13px, one per candidate window; label in `#555` on the left, computed change right-aligned at x = 700.
- **Rows (window, endpoints):** M1→M8 level (2, 24); M1→M12 level (2, 8); Q1→Q4 total (10, 37); H1→H2 total (39, 103); M6→M12 level (14, 8); Q3→Q4 total (66, 37); M10→M12 level (17, 8); M8→M12 level (24, 8). Each row prints its label followed by the endpoint pair as `label + "  (" + a + " → " + b + ")"`, with a 1px `#e0e0e0` rule under it. Every endpoint is derived in JS from the shared series — quarters and halves are summed, not typed.
- **Value color:** `#27ae60` when the computed change is positive, `#e74c3c` when negative; the max and min rows drawn bold.
- **Rows render as:** `(chg >= 0 ? "+" : "") + chg.toFixed(1) + "%"` — expected output +1100.0%, +300.0%, +270.0%, +164.1%, −42.9%, −43.9%, −52.9%, −66.7%.
- **Bottom annotation (bold 14px `#c0392b`, centered):** computed spread — `"Best minus worst = " + (max − min).toFixed(1) + " percentage points, same data"` (renders 1166.7).

## Best-of-k Reporting

**Obj-title:** Reporting the winner of k looks like a finding even when nothing happened

- **The move:** Compute a metric across many segments or many candidate metrics, then report only the best one.
- **The multiplicity math:** If each of k independent readings is positive with probability p under a pure null, at least one is positive with probability 1 − (1 − p)^k.
- **Illustrative Example:** 12 segments, each with p = 0.05 of a spuriously positive reading — 1 − 0.95^12 = 0.4596, so **46.0%**.
- **A looser threshold:** At p = 0.10 and k = 12, 1 − 0.90^12 = 0.7176, so **71.8%** — a "win" is the modal outcome under the null.
- **Expected count:** k · p = 12 × 0.05 = 0.6 spurious positives per sweep, so finding one is unremarkable.
- **Non-degenerate by check:** 46.0% is neither ~0 nor ~100, so the effect is real selection, not a rounding artifact.
- **The correction:** A per-test threshold of 0.05 / 12 = 0.00417 restores a 5% family-wide error rate.
- **The tell:** A named segment with no pre-stated hypothesis, especially a segment whose definition mentions three attributes.

**Fix:** Pre-register which segments and which metrics will be reported, count the comparisons actually made, and correct the threshold or state the search space in the headline.

### Visualization (canvas `bg5`, 720×340)

Two curves of 1 − (1 − p)^k against k, with computed markers at k = 12.

- **Title (bold 16px `#1a5276`, top center):** "P(at least one segment looks good) Under a Pure Null".
- **Layout:** margins left 62, right 155, top 48, bottom 52. x = k from 1 to 12, y = 0 to 1.
- **Axes:** gray `#999`; gridlines `#e0e0e0` every 0.25 with 11px `#999` labels "0%", "25%", "50%", "75%", "100%"; x labels 1…12 in 11px `#555`; x caption "segments tested (k) →" in 12px `#555` centered below.
- **Curve A (p = 0.05):** `#2980b9`, width 3, evaluated in JS as `1 − Math.pow(0.95, k)`.
- **Curve B (p = 0.10):** `#e74c3c`, width 3, evaluated as `1 − Math.pow(0.90, k)`.
- **Markers at k = 12:** filled 5px dots on both curves, with labels to the right in bold 13px matching each curve's color, text computed as `(v*100).toFixed(1) + "%  (p = " + p.toFixed(2) + ")"` (renders 46.0% and 71.8%).
- **Reference line:** dashed (5/4) `#27ae60` horizontal at y = 0.05, labeled "nominal 5%" in 12px `#27ae60` right of the plot.
- **Expected-count notes (12px `#555`, upper left of plot, two lines):** `"E[false positives] = k·p = " + (12*0.05).toFixed(1)` (renders 0.6) and `"per-test threshold after correction = " + (0.05/12).toFixed(5)` (renders 0.00417).
- **Bottom annotation (bold 14px `#c0392b`, centered):** computed — "With 12 segments at p = 0.05, a 'best segment' appears 46% of the time with no real effect."

## Peak Reported as Level

**Obj-title:** A single record day quoted where a rate belongs

- **The move:** Report the maximum of the series in a sentence whose grammar implies a sustained level.
- **Illustrative Example:** "Activations hit 24 in a month" is true of exactly one month out of twelve — M8.
- **Current level:** M12 stands at 8 activations, so the peak is 24 / 8 = 3.00× the current month.
- **Versus the recent trend:** The trailing three-month mean is (17 + 12 + 8) / 3 = 12.33, so the peak sits 94.6% above it.
- **Share of the whole:** The record month is 24 / 142 = 16.90% of the year's activations, not a typical month.
- **Why records drift upward:** The maximum of a growing-then-shrinking series is set once and quoted forever, so the gap to reality widens with time.
- **The tense tell:** "Reached", "hit", "as high as" and "record" mark a peak; "runs at" and "averages" mark a level.
- **The audit question:** How many periods matched or beat this figure, and what was the most recent one?

**Fix:** Quote peaks only with their date and the current value beside them, and set dashboards to display a trailing mean as the default with the record as an annotation.

### Visualization (canvas `bg6`, 720×340)

Monthly bars with the peak highlighted and two computed reference lines.

- **Title (bold 16px `#1a5276`, top center):** "One Record Month, Eleven Others".
- **Layout:** margins left 55, right 195, top 48, bottom 52. Twelve bars, width 0.62 of a slot, y scale 0–28, gridlines every 7 with 11px `#999` labels.
- **Bars:** fill `rgba(26,82,118,0.35)` with `#1a5276` stroke, except the argmax bar filled `#e74c3c` — the highlight index is computed from the array, not hardcoded.
- **Peak label:** bold 13px `#e74c3c` above the peak bar, text computed as `"record: " + peak + " (M" + (idx+1) + ")"`.
- **Trailing-mean line:** dashed (5/4) `#e67e22` horizontal at the mean of the last three months, labeled to the right in bold 12px `#e67e22` as `"trailing 3-mo mean " + mean.toFixed(2)`.
- **Current line:** dashed (3/3) `#2980b9` horizontal at the last value, labeled to the right in bold 12px `#2980b9` as `"current " + last`.
- **X labels:** "M1"…"M12" in 11px `#555`.
- **Bottom annotation (bold 14px `#c0392b`, centered):** computed — `"The record is " + (peak/last).toFixed(2) + "× the current month and " + ((peak/mean − 1)*100).toFixed(1) + "% above the recent trend."` (renders 3.00× and 94.6%).

## Survivor-Subset Framing

**Obj-title:** The subset selected itself, and it selected on the outcome

- **The move:** Compare a self-selected subset against everyone else and attribute the gap to the thing that defined the subset.
- **Illustrative Example:** 142 adopters retain at 83.80%, 142 non-adopters at 55.63% — a genuine 1.51× ratio.
- **Arithmetic:** adopters 119/142 = 83.80%; non-adopters 79/142 = 55.63%; ratio 0.8380 / 0.5563 = 1.5063.
- **The confound, stated:** Retention is 90% for heavy prior users and 50% for light ones, <em>identically in both groups</em>.
- **Within-tier counts:** adopters 108/120 heavy and 11/22 light; non-adopters 18/20 heavy and 61/122 light.
- **The mix is the whole gap:** Adopters are 84.5% heavy prior users, non-adopters 14.1% — so adoption's within-tier effect is exactly zero.
- **The honest restatement:** "Accounts that were already heavy users both adopted more and retained better" — a statement about selection.
- **The audit question:** Is the subgroup gap still there after conditioning on whatever predicted membership in the subgroup?

**Fix:** Report subset comparisons stratified on the strongest pre-membership predictor, and prefer an intent-to-treat or randomised-exposure comparison over an adopter-versus-rest cut.

### Visualization (canvas `bg7`, 720×340)

Grouped bars: identical within-tier retention on the left, the misleading aggregate on the right.

- **Title (bold 16px `#1a5276`, top center):** "Identical Within Tier — Yet Adopters 'Retain 1.51× Better'".
- **Layout:** margins left 58, right 34, top 50, bottom 62. y scale 0–100%, gridlines `#e0e0e0` every 25 with 11px `#999` labels.
- **Three groups across the plot:** "Heavy prior users", "Light prior users", "All (aggregate)".
- **Bars per group:** adopters in `#27ae60`, non-adopters in `rgba(231,76,60,0.7)`; width 54, gap 10.
- **Heights, all computed from the 2×2 counts:** heavy 108/120 and 18/20 (both 90.0%); light 11/22 and 61/122 (both 50.0%); aggregate 119/142 and 79/142 (83.80% and 55.63%).
- **Bar labels:** bold 12px `#333` above each bar, printed as `(r*100).toFixed(1) + "%"`; beneath each bar in 10px `#999` the count pair as `num + "/" + den`.
- **Legend (top left of plot, 12px):** green swatch "adopters", `rgba(231,76,60,0.7)` swatch "non-adopters".
- **Equality bracket:** thin `#2980b9` brackets over the first two groups, each labeled in bold 12px `#2980b9` "identical".
- **Mix note (12px `#555`, two lines above the aggregate group):** computed as `"heavy share: adopters " + (120/142*100).toFixed(1) + "%"` and `"vs non-adopters " + (20/142*100).toFixed(1) + "%"` (renders 84.5% and 14.1%).
- **Bottom annotation (bold 14px `#c0392b`, centered):** "Every within-tier rate is equal. The aggregate gap is entirely the mix of who adopted."

## Superlative With an Unstated Reference Class

**Obj-title:** Narrow the class until the claim is true, then drop the class

- **The move:** Add qualifiers to the comparison set one at a time and stop at the first filter that makes the superlative true.
- **Illustrative Example:** Team A's H1 → H2 growth is (103 − 39) / 39 = +164.1%, ranked against eleven other vendors.
- **The ladder:** 9th of 12 overall → 6th of 8 in region R1 → 4th of 6 among mid-market → 2nd of 3 launched in 2024 → **1st of 2** self-serve.
- **Every rung is true:** Each rank is computed by sorting the surviving vendors on the same growth figure, so nothing is misstated.
- **The vanishing class:** The winning claim rests on a comparison set of two, and the headline omits all four filters.
- **The tell:** A superlative with three or more stacked qualifiers, or one whose class size is never printed.
- **The honest restatement:** "1st of 2 self-serve mid-market vendors launched in 2024 in region R1" — accurate and no longer impressive.
- **The audit question:** How many entities are in the reference class, and was the class defined before the ranking was run?

**Fix:** Require every superlative to print its reference-class definition and its class size in the same sentence, and reject filters added after the ranking was computed.

### Visualization (canvas `bg8`, 720×340)

Narrowing funnel: five rows shrinking left-to-right, each showing class size and computed rank.

- **Title (bold 16px `#1a5276`, top center):** "Rank Improves Only Because the Class Shrinks".
- **Layout:** margins left 250, right 130, top 58, bottom 46. Five rows, height 34, evenly spaced. A gray `#999` baseline runs vertically at x = left, and Team A's growth is printed above the plot in 12px `#555` as `"Team A growth = " + TA.toFixed(1) + "% (H1 " + H0 + " → H2 " + H1 + ")"` (renders 164.1%, 39 → 103).
- **Row labels (right-aligned 13px `#555`):** "All vendors", "+ region R1", "+ mid-market", "+ launched 2024", "+ self-serve".
- **Bars:** width proportional to the surviving class size out of 12; fill `rgba(26,82,118,0.35)` for rows 1–3, `#e67e22` row 4, `#e74c3c` row 5.
- **Vendor data (name, growth %, region, segment, launch year, motion):** Vendor B 640 R2 ent 2022 assisted; Vendor C 520 R1 ent 2022 assisted; Vendor D 470 R1 mid 2022 self; Vendor E 410 R2 mid 2024 self; Vendor F 380 R1 ent 2024 self; Vendor G 350 R1 mid 2022 assisted; Vendor H 310 R2 mid 2022 self; Vendor J 295 R1 mid 2024 assisted; Team A (growth computed from the shared series as H2/H1 − 1, renders 164.1) R1 mid 2024 self; Vendor K 150 R1 mid 2024 self; Vendor L 120 R2 ent 2024 assisted; Vendor M 90 R1 mid 2022 self.
- **Rank labels:** bold 13px right of each bar, computed by filtering then sorting the vendor array descending on growth, printed as `"rank " + r + " of " + n`. Color `#e74c3c` for the final row, `#555` otherwise.
- **Class-size marks:** the surviving count printed inside each bar as `n + " vendors"` in bold 12px when the bar exceeds 70px — white on the two colored rows, `#1a5276` on the pale rows.
- **Bottom annotation (bold 14px `#c0392b`, centered):** computed — `"“Fastest-growing” is rank " + r + " of a class of " + n + ", and the class is not in the headline."` (renders rank 1 of a class of 2).

## The Detection Questions

**Obj-title:** Five knobs produce every brag, and each has one disarming question

- **Numerator/denominator:** Which population is the denominator, and was its definition dated before the measurement?
- **Units:** Are the absolute delta and the base printed next to every percentage?
- **Window:** Were the endpoints fixed in advance, and is the full series shown so a reader can re-pick them?
- **Aggregation:** Is this a level, a period total, or a running total — and can the chart ever move downward?
- **Selection:** How many segments and metrics were examined before this one was chosen for the headline?
- **Reference class:** How large is the comparison set, and how many qualifiers narrow it?
- **The general test:** Ask for the same fact in three other framings; a decision-grade metric survives all four, a brag survives one.
- **The disclosure fix:** A brag becomes a finding the moment the search space is published alongside the winner.

### Honest restatements (must appear on the page)

| The brag | The honest restatement |
|---|---|
| "88.75% adoption" | "142 of 4,000 accounts (3.55%); 88.75% of the 160 trained admins" |
| "3× growth" | "+4 activations, from a base of 2" |
| "142 activations and climbing" | "142 to date; the monthly rate fell 63.6% since M9" |
| "Up 1,100% this year" | "M1 → M8 rose 1,100%; M8 → M12 fell 66.7%" |
| "Record 24 activations" | "24 in M8, the single best month; M12 stands at 8" |
| "Adopters retain 1.51× better" | "Retention is 90% heavy / 50% light in both groups; adopters were 84.5% heavy" |
| "Best segment beat control" | "1 of 12 segments; a win appears 46% of the time under the null" |
| "Fastest-growing in its class" | "1st of 2 self-serve mid-market vendors launched in 2024 in region R1" |

### Visualization (canvas `bg9`, 720×340)

Diagram: one underlying series feeding five framing knobs, each emitting a headline.

- **Title (bold 16px `#1a5276`, top center):** "One Series, Five Knobs, Any Headline You Like".
- **Source box:** `#1a5276` filled rounded rect 190×54 at x = 24, vertically centered on the knob stack, white bold 13px two-line text "Shared series" / computed as `"12 months, total " + total` (renders 142).
- **Knob rows:** five rounded rects 150×36 stacked in the middle column at x = 260, top 58, spacing 10, outlined 2px, 12px `#333` centered text: "Denominator" `#2980b9`, "Units" `#27ae60`, "Window" `#e67e22`, "Selection" `#e74c3c`, "Reference class" `#8e44ad`.
- **Connectors:** `#999` width 1.5 lines from the source box's right edge (x = 214) to each knob's left edge, and from each knob's right edge (x = 410) to x = 430.
- **Headline column (text begins x = 436):** one line per knob in bold 12px matching the knob color, each printed from a computed value: `142/160 → "88.75% adoption"`, `6/2 → "3× growth"`, `pct(2,24) → "+1100% this year"`, "best of 12 segments", "fastest-growing in class".
- **Truth strip (12px `#555`, centered, above the annotation):** computed as `"Every headline above is arithmetically true of the same " + total + " activations."`
- **Bottom annotation (bold 14px `#c0392b`, centered):** "Nothing here is a lie. The defect is that only one framing was shown."

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** same detail-page style as `metrics/10-sign-inverting-metrics.html`. Single page: `h1` (no index number), `.subtitle` paragraph, then one unnumbered `h2` + one-row `.obj-table` per section above. The Shared Scenario section and The Detection Questions section additionally carry a `.recon` table.
- **Table layout:** `.obj-table` full width, collapsed borders `#e0e0e0`, cell padding 20px 24px; first cell 50% width (text), second cell 50% centered (canvas). Even rows shaded `#fafcfe`. No `<thead>` in `.obj-table`.
- **Reconciliation tables:** `.recon` full width, collapsed borders `#e0e0e0`, cells 8px 12px, 0.86em; first row bold `#1a5276` on `#f0f4f8` written as `<tr><td>` cells (no `<thead>`); monospace-ish arithmetic column allowed.
- **Text cell structure:** `.obj-title` (1.05em, 600 weight, `#1a5276`), then a `ul` of labeled bullets, then a `<p><strong>Fix:</strong> …</p>` where present.
- **Canvas:** every canvas 720×340 with `width: 100%`, drawn via the shared `setup(id)` helper that reads the `width`/`height` attributes, caps display at the logical width via `style.maxWidth`, sizes the backing store to rendered width × `window.devicePixelRatio`, and calls `ctx.scale` so all drawing stays in logical coordinates. All draw functions push into a `__charts` array, run on load and re-run on debounced (150ms) window resize. Keep all drawing within x ≤ 702.
- **Shared data, declared once at the top of the script, before any chart:**
  ```js
  var SERIES = [2, 3, 5, 6, 9, 14, 20, 24, 22, 17, 12, 8];   // monthly activations
  var CUM = []; (function(){ var t = 0; for (var i = 0; i < SERIES.length; i++) { t += SERIES[i]; CUM.push(t); } })();
  var TOTAL = CUM[CUM.length - 1];          // 142
  var PILOT = 284, WEEKLY = 800, ACTIVE90 = 1600, ALL_ACCTS = 4000, TRAINED = 160;
  var RET = { adHeavy:[108,120], adLight:[11,22], naHeavy:[18,20], naLight:[61,122] };
  function sum(a, i, j) { var t = 0; for (; i < j; i++) t += a[i]; return t; }
  function pct(a, b) { return (b - a) / a * 100; }
  ```
  Quarter and half totals are computed with `sum`, never typed as literals. `TOTAL`, `CUM`, every percentage, every rank and every ratio are derived — no statistic printed on a canvas is a hardcoded string.
- **No `Math.random()` anywhere.** Every series on this page is a literal array whose shape carries the lesson, so no PRNG is required; if one were ever added it must be the seeded Park-Miller LCG, not `Math.random()`.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; `h1` 1.8em `#1a5276`; `h2` 1.3em `#1a5276` with 2px `#2980b9` bottom border and 6px bottom padding; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`. No nav bar, no badges, no back/home/cross-reference links.
- **Palette:** primary blue `#1a5276`, accent `#2980b9`, green `#27ae60`, red `#e74c3c` / `#c0392b`, orange `#e67e22`, purple `#8e44ad` (reference-class knob only), bar fill `rgba(26,82,118,0.35)`, gridlines `#e0e0e0`, gray text `#555`/`#999`.
- **Labeling:** the page is a construction throughout — the Shared Scenario heading and each example bullet carry "Illustrative Example" where a figure could be mistaken for a measurement. No real firms, no real reported statistics; vendors are "Vendor B"…"Vendor M" and the subject is "Team A".
