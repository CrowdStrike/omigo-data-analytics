# Display Framing: How a Number Is Drawn Decides What It Means

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Display Framing — Cognitive Biases

**Subtitle:** One list of figures, drawn two ways, read as a crisis and as a quiet year. Nobody changed a number.

---

## Section 1 — The Same Twelve Months, Two Scales, Two Verdicts

**Tags:** `core idea` (magenta), `same figures` (blue), `opposite reading` (violet)

**Bullets:**
- **The measure** — the share of parcels a depot got there on time, one figure for each month
- **What happened** — the year opened at 90.0 in every hundred and closed at 88.2 in every hundred
- **The honest size** — a fall of 1.8 in every hundred parcels, one part in fifty of where it began
- **The panel on the left** — its scale runs from just under the worst month to just over the best
- **What that does** — the line falls through 86 percent of the panel height and reads as collapse
- **The panel on the right** — the same twelve figures on a scale running from zero to a hundred
- **What that does** — the line covers 2 percent of the panel height and reads as a flat year
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

## Section 2 — Where Somebody Put the Red Line

**Tags:** `red and green` (red), `chosen cut-off` (green), `alarm first` (orange)

**Bullets:**
- **The board** — twelve branches, each bar showing how its sales moved against last month
- **The rule** — a bar below the line is painted red, a bar at or above it is painted green
- **Line at zero** — four go red, and the room starts asking what went wrong in four places
- **Line nudged up to plus 1.5** — eight go red, and the same year looks like a broad failure
- **Line nudged down to minus 1.5** — one goes red, and the same year looks like a clean run
- **The numbers never moved** — all three rows carry the identical twelve figures, to the digit
- **What red does to a reader** — it hands over alarm before anyone has judged a single size
- **Who put the line there** — a person, once, usually with nothing written down about why
- **The figure no colouring shows** — the average branch was up 0.8, a middling but real year

**Key point:** Colour is not a summary of the data, it is a summary of somebody's cut-off. Red arrives as a verdict already reached, so the reader spends their attention defending or attacking a threshold they never saw chosen.

**Source note (`.src`):** Illustrative Example — twelve constructed branch figures; each row's red count is tallied in the draw function against that row's line.

### Visualization — canvas `c2`, 720×340

The same twelve bars drawn in three stacked rows. Only the threshold changes between rows; the red/green count under each row is tallied from the array. Red and green appear here because red/green framing is the section's subject.

- **Data (literal array):** `CH = [3.2, 1.4, −0.6, 2.1, 0.3, −1.8, 1.9, 0.8, −0.2, 2.6, 1.1, −1.3]`.
- **Thresholds:** `TH = [0, 1.5, −1.5]` in that visual order — the ordinary rule first, then the two nudges.
- **Computed in the draw function:** the red count per row is tallied as `CH[i] < t`, giving 4, 8 and 1; `avg = mean(CH) = 0.79`, printed to one decimal as +0.8. Bar heights come straight from the array.
- **Title (bold 15px `P.ink`, centered, y=22):** "Twelve Branches, One Set of Figures, Three Paint Jobs"
- **Rows:** three bands with baselines at `y = 72, 162, 252`. Twelve bar slots across `x = 156 … 690`; bar width `min(30, slot − 8)`; one unit of movement is 11px.
- **Bars:** drawn from the row's threshold line to the value, so a bar's own length shifts with the threshold as well as its colour. Below the line: fill `rgba(231,76,60,0.45)`, stroke `#e74c3c` — the only place on the page hard red is used, licensed because the alarm colour is this section's subject. At or above: fill `rgba(0,131,0,0.40)`, stroke `P.green`.
- **Threshold line:** 1.5px dashed (5/4) `P.text` across the band, labelled to its left in bold 12px `P.text` as "line at 0", "line at +1.5", "line at −1.5", printed from the threshold value.
- **Row tally,** left of each band in bold 12px: `#e74c3c` `red + ' red'` above the line label and `P.green` `(12 − red) + ' green'` below it, both from the tally.
- **Value labels:** 12px `P.mute` at each bar's tip in row one only, so the reader can confirm the figures are one list; rows two and three carry no labels, which is the point.
- **Foot note (y = 312):** 12px `P.mute` left-aligned "the same twelve figures in every row — only the line moved"; right-aligned bold 12px `P.orange` "average branch: +0.8", printed from `avg`.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "Red is a statement about the line, not about the branch."

---

## Section 3 — Moving the Colour Ramp's Middle Repaints the Map

**Tags:** `shaded map` (aqua), `where the ramp turns` (orange), `four minutes total` (yellow)

**Bullets:**
- **The map** — sixteen districts, each shaded by the average wait at its walk-in clinic
- **The waits** — from 17.2 minutes to 21.2 minutes, a spread of four minutes end to end
- **The shading** — cool blue-green at the quick end, warm orange at the slow end
- **Middle set at 18.5 minutes** — eleven of the sixteen come out warm and the map looks sick
- **Middle set at 20 minutes** — five come out warm and the very same map looks mostly healthy
- **The true middle** — 19.2 minutes, halfway between the quickest district and the slowest
- **What the eye reads** — warm as failing, cool as fine, without ever reading a minute figure
- **The tell** — nothing on the map says the whole ramp covers only four minutes of waiting

**Key point:** A shaded map hands the reader a verdict per district without ever handing over a scale. Two maps that disagree about which half of a city is failing can be drawn from one identical set of waits.

**Source note (`.src`):** Illustrative Example — sixteen constructed district waits; both warm counts and the range are computed in the draw function from the shaded values.

### Visualization — canvas `c3`, 720×330

The same sixteen district waits drawn as two four-by-four shaded maps that differ only in where the colour ramp turns from cool to warm.

- **Data (literal array, minutes):** `W = [17.2, 18.0, 19.6, 20.4, 18.6, 21.0, 17.8, 19.1, 20.8, 19.4, 18.3, 17.5, 19.9, 18.9, 20.1, 21.2]` laid out row-major into 4×4.
- **Computed in the draw function:** `lo = 17.2`, `hi = 21.2`, `span = 4.0`, `trueMid = (lo + hi) / 2 = 19.2`; the warm count per map is tallied as `W[i] > m` for `m = 18.5` (gives 11) and `m = 20.0` (gives 5).
- **Shading:** a diverging ramp about whichever midpoint that map uses. Above the midpoint `rgba(217,89,38,α)` (orange, warm), below it `rgba(25,158,112,α)` (aqua, cool), with `α = 0.15 + 0.60 × min(1, |v − m| / (span / 2))` — so a district's own shade shifts between the two maps, not just its side of the ramp.
- **Title (bold 15px `P.ink`, centered, y=22):** "Sixteen Clinics, One Set of Waits, Two Colour Ramps"
- **Maps:** two 4×4 grids, cell 42px, left grid at `x = 76`, right at `x = 404`, both starting `y = 66`. Each cell stroked 1px `#dfe4ea` and carrying its wait in bold 12px `P.text`, so the figure stays readable even where the colour disagrees with the other map.
- **Map headers (bold 13px, centered above each grid, y=52):** left in `P.orange`, right in `P.aqua`, each reading "RAMP TURNS AT " + `mid.toFixed(1)` + " MIN" from that map's midpoint variable.
- **Warm tallies (bold 12px, centered under each grid):** `n + ' of 16 painted warm'` in the map's hue, `n` from the tally.
- **Verdict strips (12px `P.mute`, centered, one line lower):** left "reads as: most of the city is slow"; right "reads as: a few slow pockets".
- **Range bar** at `y = 294`, `x = 200 … 520`: a 10px gradient strip, aqua at 0 through white at 0.5 to orange at 1, stroked `#dfe4ea`. `lo.toFixed(1)` and `hi.toFixed(1)` printed 12px `P.mute` at its ends; bold 12px `P.yellow` above it reads `span.toFixed(1)` + " minutes covers the whole ramp — true middle " + `trueMid.toFixed(1)`.
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "The ramp's middle, not the waiting time, decides which districts look bad."

---

## Section 4 — Scoring Yourself on the Questions You Already Looked Up

**Tags:** `self-scoring` (violet), `tuned against` (yellow), `familiarity` (blue)

**Bullets:**
- **The setup** — Alice drills for a driving theory test on the same fifty practice questions
- **Round one** — she gets 62 in a hundred, on the practice fifty and on unseen questions alike
- **What she does after each round** — looks up the answers she got wrong, which is sensible
- **Round eight on the practice fifty** — 98 in a hundred, so she books the test feeling ready
- **Round eight on unseen questions** — 64 in a hundred, barely above where she began
- **The gap** — 34 points, and every one of them is memory of these fifty questions
- **Why the score had to climb** — by round eight she had looked up 49 of the fifty answers
- **What the 98 measures** — how familiar those fifty are, which is not what the test asks
- **The unseen score is the flat one** — her actual driving knowledge never moved all month

**Key point:** A score taken on the material you have already adjusted yourself against measures familiarity, not ability. It has to rise, whatever happens to the skill underneath it, and it rises fastest right before it is most trusted.

**Source note (`.src`):** Illustrative Example — a seeded practice-and-lookup routine at a fixed underlying ability; both lines and the gap are computed in the draw function.

### Visualization — canvas `c4`, 720×320

Two lines over eight rounds of drilling: the practice-set score climbing to nearly full marks, and the score on unseen questions staying put. The shaded band between them is the part of the score that is memory.

- **Construction, seeded Park–Miller LCG, seed 77.** Fixed underlying ability 0.62 throughout — nothing about Alice improves. Practice pool 50 questions with a `known` flag per question, initially all false. Each of 8 rounds, in this exact order:
  1. For each of the 50 practice questions: if `known`, it is correct; otherwise correct when `rng() < 0.62`, and if wrong the question is recorded as missed. Practice score = `round(100 × right / 50)`.
  2. Every missed question is marked `known` — she looks the answer up.
  3. 400 unseen questions, each correct when `rng() < 0.62`. Unseen score = `round(100 × right / 400)`.
- **Resulting series (regenerate, do not hardcode):** practice `[62, 80, 82, 88, 96, 96, 98, 98]`; unseen `[62, 65, 65, 60, 61, 64, 64, 64]`; `known` after each round `[19, 29, 38, 44, 46, 48, 49, 50]`; final gap `98 − 64 = 34`. The 400-question unseen set keeps that line steady enough that the climb is unmistakably the practice set's.
- **Title (bold 15px `P.ink`, centered, y=22):** "Eight Rounds on the Same Fifty Questions"
- **Plot:** `x = 62 … 556`, `y = 54 … 232`. Y range 50–100, gridlines every 10 in `P.grid` labelled 12px `P.mute`, x ticks at each round 1–8, 12px `P.mute` axis title "round".
- **Gap band:** the area between the two lines filled `rgba(74,58,167,0.10)` — visually, the part of the practice score that is memory.
- **Practice line:** 2.5px `P.violet`, points radius 3.5 filled `rgba(74,58,167,0.70)`. Label bold 12px `P.violet` "practice set" above the round-4 point.
- **Unseen line:** 2.5px `P.yellow`, points radius 3.5 filled `rgba(201,133,0,0.70)`, drawn first so the practice line sits on top. Label bold 12px `P.yellow` "unseen questions" below the round-4 point.
- **Gap bracket** at round 8: a 2px `P.violet` bracket spanning `Y(prac[7])` to `Y(unseen[7])`, labelled bold 12px `P.violet` with `gap + ' points'` — differenced from the two plotted end points, never typed.
- **Right panel** at `x = 584`: bold 13px `P.ink` "BY ROUND EIGHT"; bold 19px `P.violet` `prac[7]` over 12px `P.mute` "on the fifty / she has drilled"; bold 19px `P.yellow` `unseen[7]` over 12px `P.mute` "on questions / she has not seen"; then bold 12px `P.blue` `knownBefore + ' of 50 answers'` over 12px `P.mute` "already looked up", where `knownBefore` is the count standing at the start of round 8 (49).
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "The score she trusted was the one she had been quietly correcting all month."

---

## Section 5 — When Cutting the Scale Is Honest and When It Is Not

**Tags:** `the boundary` (green), `legitimate crop` (aqua), `where it turns` (red)

**Bullets:**
- **A cropped scale is not automatically a lie** — sometimes the narrow band is the whole story
- **A patient's temperature** — eight readings across one illness, from 36.8 up to 38.9 degrees
- **On a zero-to-forty scale** — the illness covers 5 percent of the panel and looks like nothing
- **On a 36.5-to-39.2 scale** — it covers 78 percent, which is the picture a doctor needs
- **Why cropping is right here** — two degrees separates resting at home from a hospital bed
- **Same with pond acidity** — 7.4 down to 6.8 changes what can live there, and barely shows
- **Where it turns** — a bar chart, because a reader takes bar height as how much there is
- **Two bars off a base cut at 100** — 100.5 against 102.0 draws the second four times as tall
- **What it actually is** — one and a half percent bigger, so the drawing overstates it wildly
- **The working rule** — crop a line when the band is the story, never crop a bar the eye measures

**Key point:** The test is not whether the scale starts at zero, it is what the reader's eye is being invited to measure. A line asks how the value moved, so cropping to the band it moved in is honest. A bar asks how much there is, so a cut base makes the eye read a ratio that does not exist.

**Source note (`.src`):** Illustrative Example — eight constructed temperature readings and two constructed bar values; every panel share and height ratio is computed in the draw function.

### Visualization — canvas `c5`, 720×340

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
- **Bullet form:** each is ONE line that does not wrap at 50% column width (≤95 characters including the bold label). Bullet counts follow the content: 8, 9, 8, 9, 10. No padding, no line that restates another.
- **Section titles name the content**, never a role. No index number appears anywhere on the page.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue` `.green` `.red` `.orange` `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06c00`.
- **Colour, per section:** 1 magenta versus blue (misleading view versus honest view), 2 hard red versus green — the one section where `#e74c3c` is licensed, because red/green framing is that section's subject, 3 an aqua-to-orange diverging ramp with a yellow range bar, 4 violet versus yellow, 5 aqua and green for the legitimate crop against magenta and red for the misleading one. No two consecutive sections share a dominant hue.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart height (330, 340, 330, 320, 340). `setup(id)` caches the logical size in `dataset` on the first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart header bold 12–13px; body and axis labels 12px floor; the big callout figure bold 19px; caption bold 13px. No table is drawn on any canvas.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` appears only in section 2's bars and section 5's verdict strip.
- **Determinism:** no `Math.random()`. Sections 1, 2, 3 and 5 use literal arrays; section 4 uses the seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`) with seed 77. Every panel share, red count, warm count, gap, height ratio and average is computed in the draw function from the plotted data and printed from that variable.
- **Shared helpers:** `mean(a)`, and `tick(v)` which returns `String(parseFloat(v.toFixed(2)))`. Axis labels go through `tick()` because two panels here have derived midpoints — a gridline drawn at 89.15 or 37.85 must not be labelled "89.2" or "37.9", or the label contradicts the line beside it.
- **The lead chart must show one dataset drawn two ways with the impression flipping.** The page is about display choice, so the first canvas has to put both renderings on screen at once; describing the effect in prose does not make the case. Both panels in sections 1 and 5 are drawn by a single helper called twice, so a change to one rendering cannot silently fail to reach the other.
- **Corrections and changes from the old page:**
  - The old page had two sections and two canvases and asserted its figures. It now has five sections, five canvases, and every figure is computed at render time.
  - The old dashboard canvas hardcoded "+5.2%", a 95% interval of [−2.1%, +12.5%], "n=200 / 625 required" and a 32% progress bar with none of them derived from anything and no data behind them. That construction is dropped; the red/green point is now made by repainting one real array under three thresholds, and the interval vocabulary is gone in line with the layman rule.
  - The old second canvas plotted `78 + 11·(1 − exp(−i/18))` against `81 + 0.3·sin(0.3i)` and labelled the difference "8% self-deception" while the curves actually reach 88.8 and 81.0 at iteration 42, a gap of 7.8 — and the legend's "Test set accuracy (89%)" was never attained by the drawn curve. The self-assessment idea survives as section 4, where the climb is produced by an explicit lookup mechanism rather than a chosen formula, and the gap is differenced from the plotted points.
  - The old page's framing was ML-pipeline vocabulary — test set, holdout, overfitting, confidence intervals. All of it is gone; the same ideas are carried by a depot, a branch board, a clinic map and a driving-theory drill.
