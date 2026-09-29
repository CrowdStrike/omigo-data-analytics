# Framing Effect: How a Number Is Drawn Decides What It Means

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Framing Effect — Cognitive Biases

**Subtitle:** One list of figures, drawn two ways, read as a crisis and as a quiet year. Nobody changed a number.

---

## Section 1 — One Year of Figures, Two Scales, Two Verdicts

**Tags:** `core idea` (magenta), `same figures` (blue), `opposite reading` (violet)

**Bullets:**
- **The measure** — the share of parcels a depot got there on time, one figure for each month
- **What happened** — the year opened a little higher than it closed, drifting down in between
- **The panel on the left** — its scale starts just under the worst month and stops just over the best
- **What that does** — the line dives almost the whole height of the panel and reads as collapse
- **The panel on the right** — the same months on a scale running from nothing at all up to perfect
- **What that does** — the line barely leaves the flat, so the year reads as nothing happened
- **Nothing was faked** — both panels plot every point correctly from one identical list

**Key point:** The reader is not measuring parcels, they are measuring how far the line moved down the page. Whoever picks the top and bottom of the scale picks how far that is, and so picks the conclusion.

**Source note (`.src`):** Illustrative Example — twelve constructed monthly figures; the fall and both panel shares are computed in the draw function from the plotted points.

### Visualization — canvas `c1`, 720×330

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

## Section 2 — Where Somebody Put the Target

**Tags:** `red and green` (red), `chosen target` (green), `alarm first` (orange)

**Bullets:**
- **The board** — eight branches, each bar its sales as a percent of last month
- **The rule** — a branch under the target is painted red, one at or over it green
- **Top row, target at 100** — hold last month's sales and you pass, so three go red
- **Bottom row, target at 103** — the same eight bars, and now six of them go red
- **The bars never changed** — both rows are drawn from one list, same heights, same labels
- **What the room does** — reads the red count as how the region did, and panics at the second row
- **What red actually marks** — which side of somebody's chosen number a branch fell on
- **The figure neither row shows** — the typical branch beat last month, a modest but real result

**Key point:** A target is a choice, not a measurement, and colour hides that it was ever made. Red arrives as a verdict already reached, so the reader argues about branches instead of asking who set the number and why.

**Source note (`.src`):** Illustrative Example — eight constructed branch figures; each row's red count and the average are computed in the draw function from the plotted bars.

### Visualization — canvas `c2`, 720×340

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

## Section 3 — When Cutting the Scale Is Honest and When It Is Not

**Tags:** `the boundary` (green), `legitimate crop` (aqua), `where it turns` (red)

**Bullets:**
- **A cropped scale is not automatically a lie** — sometimes the narrow band is the whole story
- **A patient's temperature** — a run of readings taken across one illness, rising then easing off
- **Drawn on a wide scale** — the fever is a faint wobble near the top and looks like nothing at all
- **Drawn on a scale cropped to the fever** — the illness fills the panel, the picture a doctor needs
- **Why cropping is right here** — a couple of degrees separates resting at home from a hospital bed
- **Where it turns** — a bar chart, because a reader takes bar height as how much there is
- **Cut the base off a bar chart** — 100.5 beside 102.0 draws the second bar four times as tall
- **The working rule** — crop a line when the band is the story, never crop a bar the eye measures

**Key point:** The test is not whether the scale starts at zero, it is what the reader's eye is being invited to measure. A line asks how the value moved, so cropping to the band it moved in is honest. A bar asks how much there is, so a cut base makes the eye read a ratio that does not exist.

**Source note (`.src`):** Illustrative Example — eight constructed temperature readings and two constructed bar values; every panel share and height ratio is computed in the draw function.

### Visualization — canvas `c3`, 720×340

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

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the converted `05-clustering-illusion.html` and `01-confirmation-bias.html` in this folder. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. The canvas is capped at 720px, so a wide cell leaves slack — centering puts the chart in the middle of the right half.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note. Every section on this page is a constructed example, so every section carries a `.src`. No paragraph blocks, no data tables, no philosophy box.
- **Bullet form:** each is ONE line that does not wrap at 50% column width (≤105 characters including the bold label). Bullet counts follow the content: 7, 8, 8. No padding, no line that restates another.
- **Numbers live in the charts, not the prose** — at most a couple of figures in bullets, and only where the figure is the argument. Bullets state the idea in plain words ("a slight slip", "most of the board goes red"); the exact values, shares, counts and ratios are computed and printed on the canvas. No bullet opens with a count, a size or a percentage, and no decimal percentages or precise averages appear in prose.
- **Section titles name the content**, never a role. No index number appears anywhere on the page.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue` `.green` `.red` `.orange` `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`.
- **Colour, per section:** 1 magenta versus blue (misleading view versus honest view), 2 hard red versus green — the one section where `#e74c3c` is licensed as a chart colour, because red/green framing is that section's subject, 3 aqua and green for the legitimate crop against magenta and red for the misleading one.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart height (330, 340, 340). `setup(id)` caches the logical size in `dataset` on the first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart header bold 12–13px; body and axis labels 12px floor; the big callout figure bold 19px; caption bold 13px. No table is drawn on any canvas.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` appears only in section 2's bars and section 3's verdict strip.
- **Determinism:** no `Math.random()`. All three sections use literal arrays, so no PRNG is needed on this page and the `lcg()` helper is absent. Every panel share, red count, height ratio and average is computed in the draw function from the plotted data and printed from that variable.
- **Shared helpers:** `mean(a)`, and `tick(v)` which returns `String(parseFloat(v.toFixed(2)))`. Axis labels go through `tick()` because two panels here have derived midpoints — a gridline drawn at 89.15 or 37.85 must not be labelled "89.2" or "37.9", or the label contradicts the line beside it.
- **The lead chart must show one dataset drawn two ways with the impression flipping.** The page is about display choice, so the first canvas has to put both renderings on screen at once; describing the effect in prose does not make the case. Both panels in sections 1 and 3 are drawn by a single helper called twice, so a change to one rendering cannot silently fail to reach the other.
- **Scope, and what was cut:**
  - The page is deliberately three sections. An earlier five-section version added a shaded clinic map (moving the colour ramp's midpoint) and a self-scoring drill (practice score climbing because missed answers were looked up). Both were dropped as harder to read than the point they carried — the map required holding two ramps and sixteen values at once, and the drill was closer to leakage than to framing.
  - The remaining three are the y-axis pair (the core demonstration), the target repaint (the same trick applied to colour rather than scale), and the honest-versus-misleading boundary (which stops the page reading as "never crop an axis").
  - **Section 2 was rebuilt once for legibility** and must not drift back. The first version drew twelve bars in three rows, plotted each bar *from the threshold line to its value* so bar lengths changed between rows, and labelled only the first row. Nothing on screen then held still, so the claim "these are the same figures" was unverifiable by looking — the reader had to take it on trust. The rebuild fixes all three: eight bars, two rows, one shared axis with every bar running from the baseline, and labels on both rows.
  - An older version of this page asserted its figures rather than computing them: a hardcoded "+5.2%" with an interval of [−2.1%, +12.5%], an "n=200 / 625 required" progress bar with no data behind it, and a curve pair labelled "8% self-deception" whose plotted ends actually differed by 7.8. All of that is gone; every figure on every canvas now derives from the plotted points.
  - The old page's framing was ML-pipeline vocabulary — test set, holdout, overfitting, confidence intervals. All of it is gone; the ideas are carried by a depot, a branch board, a patient's temperature and a pair of bars.
