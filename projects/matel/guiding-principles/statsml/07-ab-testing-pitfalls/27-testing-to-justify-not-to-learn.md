# Testing to Justify, Not to Learn

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Testing to Justify, Not to Learn — A/B Testing Pitfalls

**Subtitle:** Mindset — The hypothesis is backwards. The test exists to produce evidence for a pre-decided launch.

## Section 1: The Justification Mindset

- The hypothesis is inverted: not "will this improve X?" but "how do I show this improves X?" The conclusion precedes the experiment.
- **Post-hoc metric selection:** Run test, scan all metrics, find one that's significant, report that one. The others never appear in the slide deck.
- **Window manipulation:** Test runs 2 weeks, no significance. Extend to 3. Still nothing. Cut to the first 5 days where a spike existed. Report that window.
- **Segment mining:** Overall result flat. Slice by platform/geo/user-age until one segment shows lift. "Significant improvement for iOS users in APAC." Ship to everyone.
- These are all forms of p-hacking, but institutionally normalized because the org rewards launches, not correct inference.

**Differs from #18 (post-hoc segments):** This isn't one technique — it's the MINDSET. The entire test exists to justify, not to learn. Metric shopping, window gaming, and segment mining are all deployed together.

**Correct approach:** Pre-register primary metric, success criteria, and decision rule BEFORE the test starts. If you won't kill the feature on a negative result, don't waste experiment capacity — just ship it with monitoring.

### Visualization (canvas `c1`, 720×400)

Two horizontal flowchart pipelines stacked vertically, red vs green. Background `#f8f9fa`.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Two Pipelines: Justification vs Learning".
- **Justification pipeline (top, y=75):** left-aligned bold 13px `#e74c3c` label "JUSTIFICATION PIPELINE" above. Six rounded boxes (95×50, radius 6) evenly spaced across the width, stroke `#e74c3c`, fill `rgba(231,76,60,0.05)` — except box 4 ("Find one green") highlighted with fill `rgba(231,76,60,0.12)` and thicker 2.5px stroke. Box texts (12px `#333`, two lines): "Commit to / feature", "Run test / (formality)", "Scan all / metrics", "Find one / green", "\"Data-driven\" / slide", "Ship". Red arrows (line + solid triangle head) connect the boxes.
- **Outcome annotation (italic 12px `#e74c3c`, centered):** "Outcome predetermined. Test is theater."
- **Divider:** dashed gray `#ddd` horizontal line across, with 11px `#999` centered "vs" on it.
- **Learning pipeline (bottom):** left-aligned bold 13px `#27ae60` label "LEARNING PIPELINE". Five rounded boxes (105×50), stroke `#27ae60`, fill `rgba(39,174,96,0.08)`, green arrows. Box texts: "Hypothesis", "Pre-register / metric + criteria", "Run test", "Analyze / result", "Decision".
- **Fork from Decision:** two branch arrows below the last box — green to bold 12px `#27ae60` "Ship", red to bold 12px `#e74c3c` "Kill".
- **Bottom label (italic 12px `#27ae60`, centered, y=H−12):** "Both outcomes are real possibilities. That makes the test meaningful."

## Section 2: P-Value Shopping

- Run a test measuring 12 metrics. 11 show no significance. One crosses p < 0.05 by chance alone (expected: 0.6 false positives at alpha=0.05 with 12 metrics).
- The significant metric gets highlighted. The report says "statistically significant improvement in [metric]." No mention of the other 11.
- With 20 metrics, probability of at least one false positive: 1 - (0.95)^20 = 64%. You'll almost always find "evidence."
- The statistical framework assumes you chose the metric BEFORE seeing data. Choosing AFTER invalidates the p-value entirely.
- **The tell:** If the "primary metric" in the report wasn't defined in the test plan, it was selected post-hoc.

### Visualization (canvas `c2`, 720×400)

Horizontal bar chart of 12 metric p-values with a vertical alpha line; one significant bar highlighted. Background `#f8f9fa`.

- **Title (bold 14px `#1a5276`, centered, y=22):** "P-Value Shopping: 12 Metrics Tested, 1 Reported".
- **Data (metric name, p-value):** CTR 0.72, Revenue 0.34, Sessions 0.61, Bounce 0.19, Time on page 0.55, Sign-ups 0.83, Retention D7 0.41, Page views 0.038, Cart adds 0.27, Latency 0.68, NPS 0.91, Share rate 0.47.
- **Layout:** bars start at x=160, chart width = W−200; bar height 24, gap 4, starting y=45; bar width proportional to p on a 0–1.0 scale.
- **Alpha line:** vertical dashed red (`#e74c3c`, dash 6/3, width 2) at p=0.05 with bold 11px red label "α = 0.05" centered below the chart.
- **Bars:** non-significant — fill `rgba(26,82,118,0.2)`, stroke `rgba(26,82,118,0.4)` width 1, name 12px `#555` right-aligned, p-value 10px `#999` right of bar ("p=0.72" format). The significant bar (Page views, p=0.038) — fill `rgba(39,174,96,0.7)`, stroke `#27ae60` width 2, full-width row highlight `rgba(39,174,96,0.08)`, name bold 12px `#27ae60`, and bold 14px `#27ae60` "★ REPORTED" right of the bar.
- **Annotation (italic 12px `#999`, right-aligned below chart):** "← buried (never shown in report)".
- **Bottom annotations (centered):** 12px `#555`: "With 12 metrics at α=0.05: P(at least 1 false positive) = 1 - 0.95¹² = 46%"; bold 12px `#e74c3c`: "The \"significant\" result is expected by chance. The p-value is meaningless post-selection."

## Section 3: Illustrative Example: The Redesign That Was Already Announced

- A product team at a large consumer tech company announced a major redesign to the press before any experiment had run, so shipping it was already a done deal. The A/B test that followed existed only to produce a supportive number for the launch story.
- When the first results came back flat, the team kept slicing the data by country, device, and user tenure until one group finally showed a positive number. That single slice became the headline of the results deck, and the flat overall result never appeared in it.
- The fix is to write down the success metric and the kill criteria before the test starts, and to skip the test entirely if a bad result would not stop the launch (this is called pre-registration).

### Visualization (canvas `c3`, 720×300)

Horizontal flowchart of five boxes with a dashed bracket looping back from the test to the decision. White background.

- **Title (bold 16px `#2a2a2a`, centered, y=26):** "The Decision Came First — the Test Came After".
- **Boxes (116×62 at y=80, evenly spaced):** two-line bold 14px labels, fill `rgba(26,82,118,0.08)` (box 4 fill `rgba(230,126,34,0.15)`), stroke color per box: "Decision: / \"we ship it\"" `#e74c3c`; "Public / announcement" `#e74c3c`; "Feature / built" `#1a5276`; "A/B test / (formality)" `#e67e22`; "Launch / (guaranteed)" `#e74c3c`. Gray (`#999`) arrows with triangle heads connect the boxes.
- **Bracket:** dashed red (`#e74c3c`, dash 5/3, width 2) U-shaped bracket from under the "A/B test" box back to under the "Decision" box, with bold 14px `#e74c3c` centered text: "outcome was locked in back here — the test result cannot change anything".
- **Bottom line (15px `#333`, centered, y=H−15):** "A test that cannot say \"no\" is not evidence — set the metric and the kill rule before committing".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section (three rows in one table); left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `ul` 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
