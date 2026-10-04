# Framing Effect — Viz

**Page type:** detail page — card-section template
**HTML title tag:** Framing Effect — Cognitive Biases
**Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the converted `05-clustering-illusion.html` and `01-confirmation-bias.html` in this folder.
**Source note wording:** the sibling `.txt.md` says figures are "computed at render time"; the html `.src` notes say "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a literal array, an axis range, or a target invalidates that prose — re-read the computed falls, shares, red counts and ratios and update the text to match. The prose quotes: section 1 — the January and December values 90.0 and 88.2, the 1.8-point fall, the cropped axis 88.1–90.2, the 86% cropped share and the under-2% zero-based share; section 2 — eight branches, the targets 100 and 103, the red counts 3 and 6, and the 100.5 average; section 3 — the eight readings 36.8 to 38.9 easing to 37.9, the 2.1-degree swing, the 0–40 axis and its 5% share, the 36.5–39.2 crop and its 78% share, and the bars 100.5 and 102.0 at 1.5% apart drawn 4.0× taller.

**Determinism:** no `Math.random()`. All three sections use literal arrays, so no PRNG is needed on this page and the `lcg()` helper is absent. Every panel share, red count, height ratio and average is computed in the draw function from the plotted data and printed from that variable.

---

## 1. One Year of Figures, Two Scales, Two Verdicts

**Tag colors:** `core idea` magenta, `same figures` blue, `opposite reading` violet
**Hue family:** magenta versus blue — misleading view versus honest view

### canvas `c1` — 720×330

Two line panels side by side plotting one identical twelve-value series, differing only in the y-axis range. Left panel cropped to the data, right panel zero to a hundred.

- **Data (literal array):** `S = [90.0, 89.8, 90.1, 89.6, 89.5, 89.7, 89.2, 89.0, 89.1, 88.7, 88.4, 88.2]`.
- **Computed in the draw function:** `fall = S[0] − S[11] = 1.8`; `rel = 100 × fall / S[0] = 2.0%`; `dMin = 88.2`, `dMax = 90.1`; cropped axis `lo = dMin − 0.1 = 88.1`, `hi = dMax + 0.1 = 90.2`; `cropShare = 100 × fall / (hi − lo) = 86%`; `fullShare = 100 × fall / 100 = 2%`. Every printed figure comes from these variables.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Depot's On-Time Rate, Drawn Twice"
- **Panels:** one `panel()` helper called twice, so neither rendering can diverge from the other. Plot box 252 wide × 158 tall, `y = 62 … 220`, 1px `P.grid` border. Left at `x = 56`, right at `x = 404`.
- **Left panel (the misleading one):** header bold 13px `P.magenta` at y=46, "SCALE 88.1 – 90.2" with both numbers printed from `lo`/`hi`. Gridlines at `lo`, `(lo+hi)/2`, `hi`, labelled 12px `P.mute` through the shared `tick()` helper. Line 2.5px `P.magenta`, points radius 3 filled `rgba(213,81,129,0.65)`. A dashed (4/3) 1.5px `P.magenta` arrow with a triangular tip at each end sits at the right edge, spanning exactly `Y(S[0])` to `Y(S[11])`.
- **Right panel (the honest one):** header bold 13px `P.blue` "SCALE 0 – 100". Gridlines at 0, 50, 100. Line 2.5px `P.blue`, points radius 3 filled `rgba(42,120,214,0.60)`. The same arrow over the same two values in `P.blue`.
- **Panel shares,** one under each panel at y=254: bold 19px in the panel's hue printing `share.toFixed(0) + '%'` from `cropShare` / `fullShare`, then 12px `P.mute` "of the panel height" placed by `measureText`. At y=276, bold 12px in the panel's hue: left "reads as: the depot is failing", right "reads as: nothing happened".
- **Centre line (bold 13px `P.ink`, centered, y=304):** "The fall is " + `fall.toFixed(1)` + " in every hundred parcels — " + `rel.toFixed(1)` + "% of where it began", both computed.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "Same twelve numbers. The scale, not the data, decided which one you believe."

---

## 2. Where Somebody Put the Target

**Tag colors:** `red and green` red, `chosen target` green, `alarm first` orange
**Hue family:** hard red versus green — the one section where `#e74c3c` is licensed as a chart colour, because red/green framing is that section's subject

### canvas `c2` — 720×340

The same eight bars drawn twice, one row above the other. The bars are identical in both rows — same list, same axis, same heights, every one labelled. Only the dashed target line moves, and with it the colour. Red and green appear here because red/green framing is the section's subject.

- **Data (literal array, percent of last month's sales):** `CH = [104, 97, 101, 99, 106, 95, 102, 100]`.
- **Rows:** two, distinguished only by target — `{target: 100, y: 130}` then `{target: 103, y: 268}`. Two rows, not three: three rows of twelve bars asked the reader to hold too much at once, and the third row (a lowered line turning everything green) only restated the second.
- **Computed in the draw function:** the red count per row is tallied as `CH[i] < target`, giving 3 of 8 at target 100 and 6 of 8 at target 103; `avg = mean(CH) = 100.5`, printed to one decimal. Nothing about the bars is derived from the target.
- **Title (bold 15px `P.ink`, centered, y=22):** "Eight Branches, One Set of Sales, Two Targets"
- **Bars:** eight slots across `x = 150 … 690`, bar width `min(34, slot − 10)`. Both rows share one axis, 90 to 108, and every bar runs from that row's baseline up to its value — so a bar's height is a function of its value alone and cannot shift when the target moves. Under the target: fill `rgba(231,76,60,0.45)`, stroke `#e74c3c` — the only place on the page hard red is used as a chart colour, licensed because the alarm colour is this section's subject. At or over: fill `rgba(0,131,0,0.40)`, stroke `P.green`.
- **Value labels:** 12px `P.mute` above every bar in **both** rows. The earlier version labelled only the first row, which left the reader taking "the figures are identical" on trust — the claim the chart exists to prove.
- **Target line:** 1.5px dashed (5/4) `P.text` across the row at that target's height, labelled to its left in bold 13px `P.text` as "target 100" / "target 103", printed from the target value.
- **Row annotations,** right-aligned left of each row: bold 12px `#e74c3c` `miss + ' of 8 red'` above the target label, and 12px `P.mute` beneath it reading "a couple of branches to look at" (top row) / "the whole region is failing" (bottom row).
- **Foot note (y = 314):** 12px `P.mute` left-aligned "same eight branches, same eight bars — only the target moved"; right-aligned bold 12px `P.orange` "typical branch: 100.5", printed from `avg`.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "Red tells you where the target was put, not how a branch did."

---

## 3. When Cutting the Scale Is Honest and When It Is Not

**Tag colors:** `the boundary` green, `legitimate crop` aqua, `where it turns` red
**Hue family:** aqua and green for the legitimate crop against magenta and red for the misleading one

### canvas `c3` — 720×340

Two blocks. On the left, one temperature series drawn on a full scale and on a cropped scale, where cropping is the only way to see the illness. On the right, two bar values drawn off a cut base and off zero, where cropping invents a ratio.

- **Left data (literal, degrees C):** `T = [36.8, 37.1, 37.6, 38.2, 38.6, 38.9, 38.4, 37.9]`.
- **Computed:** `swing = max − min = 2.1`; full panel share `= 100 × swing / 40 = 5%`; cropped axis 36.5–39.2, share `= 100 × swing / 2.7 = 78%`.
- **Right data (literal):** `bars = [100.5, 102.0]`, cut base 100.
- **Computed:** drawn height ratio off the cut base `= (102.0 − 100) / (100.5 − 100) = 4.0`; true excess `= 100 × (102.0 / 100.5 − 1) = 1.5%`.
- **Title (bold 15px `P.ink`, centered, y=22):** "Cropping That Reveals, Cropping That Invents"
- **Left block header (bold 13px `P.aqua`, x=56, y=48):** "A LINE — CROP TO THE BAND THAT MATTERS"
- **Left panels:** one `tempPanel()` helper called twice, 118 wide × 150 tall, `y = 66 … 216`, at `x = 56` and `x = 216`, 1px `P.grid` border. First on a 0–40 axis with gridlines at 0, 20, 40; second on a 36.5–39.2 axis with gridlines at 36.5, 37.85, 39.2, all labelled 12px `P.mute` through the shared `tick()` helper. Line 2.5px `P.mute` on the full panel (the view that hides the story) and 2.5px `P.aqua` on the cropped panel.
- **Left labels:** bold 12px in the panel's hue at y=238 printing `share.toFixed(0) + '% of the panel'` from `fullShare` / `cropShare`; 12px at y=255 "the fever is invisible" / "the fever is the story". A 12px `P.mute` line at y=274 reads `tLo.toFixed(1)` + " to " + `tHi.toFixed(1)` + " degrees in both panels", printed from the array ends.
- **Right block header (bold 13px `P.magenta`, x=404, y=48):** "A BAR — THE EYE MEASURES HEIGHT"
- **Right panels:** one `barPanel()` helper called twice, 118 wide × 150 tall, `y = 66 … 216`, at `x = 404` and `x = 556`. First with its base cut at 100 (axis 100–102.4), two bars width 32 in `rgba(213,81,129,0.50)` stroked `P.magenta`, each labelled its own value bold 12px `P.magenta`. Second with base 0 (axis 0–110), the same two values in `rgba(107,114,128,0.35)` stroked `P.mute`. Both bars run from the panel floor, so the height the reader measures is exactly what the axis produces.
- **Right labels:** bold 12px at y=238 — left `ratio.toFixed(1) + '× taller'` from the computation, right "the same two bars"; 12px at y=255 "base cut at 100" / "base at zero". A 12px `P.mute` line at y=274 reads `BARS[0].toFixed(1)` + " and " + `BARS[1].toFixed(1)` + " in both panels".
- **Verdict strip** at y=300, bold 12px: under the left block in `P.green` "honest — the reader is asked how the value moved"; under the right block in `#e74c3c` "misleading — it is " + `excess.toFixed(1)` + "% bigger, drawn " + `ratio.toFixed(1)` + "× bigger", both figures computed.
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Ask what the reader's eye is measuring — the movement, or the amount."

---

## Page-specific constraints

- **Canvas heights:** intrinsic `width="720"` plus per-chart height 330, 340, 340.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. The canvas is capped at 720px, so a wide cell leaves slack — centering puts the chart in the middle of the right half.
- **Hard red is restricted.** `#e74c3c` appears only in section 2's bars and section 3's verdict strip. It is licensed in section 2 because red/green framing is that section's subject; it is not a general chart colour on this page.
- **Text stands alone; the chart adds clarity** — the text carries the argument and names every quantity it turns on; the canvas adds precision, intermediate values and per-point labels. No bullet points at a position on the canvas. See `ui-templates/README.md`.
- **Every section carries a `.src`** — all three are constructed examples. No paragraph blocks, no data tables, no philosophy box.
- **Bullet counts follow the content:** 7, 8, 8. No padding, no line that restates another.
- **Section titles name the content**, never a role. No index number appears anywhere on the page.
- **Shared helpers:** `mean(a)`, and `tick(v)` which returns `String(parseFloat(v.toFixed(2)))`. Axis labels go through `tick()` because two panels here have derived midpoints — a gridline drawn at 89.15 or 37.85 must not be labelled "89.2" or "37.9", or the label contradicts the line beside it.
- **The lead chart must show one dataset drawn two ways with the impression flipping.** The page is about display choice, so the first canvas has to put both renderings on screen at once; describing the effect in prose does not make the case. Both panels in sections 1 and 3 are drawn by a single helper called twice, so a change to one rendering cannot silently fail to reach the other.
- **Scope, and what was cut:**
  - The page is deliberately three sections. An earlier five-section version added a shaded clinic map (moving the colour ramp's midpoint) and a self-scoring drill (practice score climbing because missed answers were looked up). Both were dropped as harder to read than the point they carried — the map required holding two ramps and sixteen values at once, and the drill was closer to leakage than to framing.
  - The remaining three are the y-axis pair (the core demonstration), the target repaint (the same trick applied to colour rather than scale), and the honest-versus-misleading boundary (which stops the page reading as "never crop an axis").
  - **Section 2 was rebuilt once for legibility** and must not drift back. The first version drew twelve bars in three rows, plotted each bar *from the threshold line to its value* so bar lengths changed between rows, and labelled only the first row. Nothing on screen then held still, so the claim "these are the same figures" was unverifiable by looking — the reader had to take it on trust. The rebuild fixes all three: eight bars, two rows, one shared axis with every bar running from the baseline, and labels on both rows.
  - An older version of this page asserted its figures rather than computing them: a hardcoded "+5.2%" with an interval of [−2.1%, +12.5%], an "n=200 / 625 required" progress bar with no data behind it, and a curve pair labelled "8% self-deception" whose plotted ends actually differed by 7.8. All of that is gone; every figure on every canvas now derives from the plotted points.
  - The old page's framing was ML-pipeline vocabulary — test set, holdout, overfitting, confidence intervals. All of it is gone; the ideas are carried by a depot, a branch board, a patient's temperature and a pair of bars.
