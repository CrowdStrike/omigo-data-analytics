# Texas Sharpshooter — Viz

**Page type:** detail page — card-section (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Texas Sharpshooter — Cognitive Biases
**Template:** card-section layout from `statistical-paradoxes/03-berksons-paradox.html`
**Source note wording:** the sibling `.txt.md` says figures are found/tallied "at render time"; the html `.src` notes say "in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed or the construction invalidates that prose — re-read the computed values and update the text to match. The prose quotes, in order: chart 1's ring area **4.9%**, deserved **2** holes, painted **10**, declared **1**, and the **5.1×** ratio rendered as "five times"; chart 2's trio `2 → 47`, `3 → 79`, `14 → 88` with its **+41** climb, the opposite trio's **−40** fall, the shared country at `3 → 79`, the median **65**, the **8**-soda heavy cut, **13 of 25** against **13 of 25**, and **9,277** / **1,979** / **1,329** trios; chart 3's **24** images, **120** verses, **30** events, the **2**-image match rule, **8** fitting the typical event against **112** not, **103** of 120 fitting some event, and the **3**-to-**18** range.

**Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`). Seeds: 321 for the barn wall, 146 for the 50 countries, 1847 for the verses and events. Hole counts, healthy-half counts, trio counts and verse match counts are computed in the draw function and printed from those variables.

---

## 1. What the Fallacy Is

**Tag colors:** `definition` violet, `four steps` blue, `the barn wall` magenta
**Hue family:** magenta painted ring against aqua honest ring on grey holes

### canvas `c1` — 720×340

The barn wall with 40 seeded holes, the after-the-fact ring drawn around the densest patch, and the honest ring — named before firing — sitting at the middle of the wall with almost nothing in it.

- **Geometry:** wall rectangle at `WX=40, WY=54, WW=400, WH=230`. Holes confined to an 18px inset so none straddles the edge. Ring radius `R=38`, which covers 4.93% of the wall, so a ring placed without looking deserves `40 × 0.0493 = 1.97` holes.
- **Data:** seeded Park–Miller LCG, seed 321; 40 holes at `x = WX+18+rng()·(WW−36)`, `y = WY+18+rng()·(WH−36)`.
- **Painted ring:** found by scanning candidate centres on a 2px lattice for the one enclosing the most holes, then re-centring on the mean of the enclosed holes if that keeps the same count. Lands at (94, 108) holding **10** holes.
- **Declared ring:** fixed at the wall centre (240, 169) — the spot a shooter would have to name in advance. Holds **1** hole. Both counts computed, never typed.
- **Title (bold 15px `P.ink`, centered, y=22):** "Forty Shots at a Blank Barn Wall"
- **Wall:** fill `rgba(107,114,128,0.05)`, stroke `#d7dbe2` 1.5px, with four faint plank lines in `P.grid` so it reads as a wall rather than a plot box.
- **Holes:** radius 3.6. Holes inside the painted ring `rgba(213,81,129,0.70)` stroked `P.magenta`; all others `rgba(107,114,128,0.45)` stroked `P.mute` — the unchosen shots are deliberately dull.
- **Painted ring:** 2.5px solid `P.magenta` circle, fill `rgba(213,81,129,0.08)`. Label bold 12px `P.magenta` above it: "painted after firing", and beneath the ring "10 holes" in bold 13px.
- **Declared ring:** 2px dashed (6/4) `P.aqua` circle at the wall centre, no fill. Label bold 12px `P.aqua`: "named before firing", with "1 hole" beneath.
- **Side panel** at `WX+WW+28`: bold 13px `P.ink` "A RING THIS SIZE COVERS", then bold 19px `P.mute` "4.9%" + 12px "of the wall"; bold 13px `P.ink` "SO IT DESERVES", bold 19px `P.mute` "2.0" + 12px "holes"; then bold 19px `P.magenta` "10" + 12px `P.mute` "where it was painted" and bold 19px `P.aqua` "1" + 12px `P.mute` "where it was named". Closing bold 12px `P.magenta` line: "5.1× its fair share" computed as painted ÷ deserved.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "Same forty holes. The marksmanship is entirely in where the ring went."

---

## 2. Cherry-Picking: Three Countries Out of Fifty

**Tag colors:** `cherry-picking` magenta, `soda and health` orange, `the 47 dropped` green
**Hue family:** grey countries with a magenta shown-trio against an aqua opposite-trio

### canvas `c2` — 720×370

The three countries in the claim, drawn alone at the top as the reader would see them. Then all 50 below, with those same three ringed, the opposite three ringed in another colour, and the even 13-against-13 split printed beside.

- **Data:** seeded Park–Miller LCG, seed 146, drawn as two blocks so the axes are independent — 50 whole-number soda counts `1 + floor(rng()·14)`, then 50 whole-number wellness scores `45 + floor(rng()·46)`. Every value is an integer; no decimals appear anywhere on the chart.
- **The layman check:** the median wellness over all 50 is **65**; "healthier half" means scoring 65 or above. Heavy drinkers (8+ sodas) number **25**, of whom **13** are in the healthier half. Light drinkers (1–7) also number **25**, of whom **13** are in the healthier half. Both counts tallied, never typed.
- **The trio shown:** searched over trios whose soda counts span 8 or more and step strictly upward in wellness, keeping the largest total climb. Lands on `2 → 47`, `3 → 79`, `14 → 88` — a climb of **+41** wellness points.
- **The trio that says the opposite:** the same search for the largest fall. Lands on `1 → 86`, `3 → 79`, `13 → 46` — a fall of **−40** points. The `3 → 79` country belongs to both trios: the same country is evidence for and against the soda, and the chart rings it in both colours with a bold 12px `P.ink` note "in both stories".
- **Points:** radius 4.5. The **45** countries in neither trio `rgba(107,114,128,0.32)` stroked `P.mute`. The three shown `rgba(213,81,129,0.75)` stroked `P.magenta` 2px at radius 6.5, joined by a 2.5px `P.magenta` line. The opposite three ringed 6.5px in `P.aqua` 2px with no fill, joined by a 2px dashed (5/4) `P.aqua` line. The shared country gets both rings, drawn magenta then aqua at radius 8.
- **Trio census:** **9,277** of the 19,600 possible trios span 8+ sodas with three distinct soda counts; of those, **1,979** climb step by step and **1,329** fall step by step. All three counts tallied in the draw function.
- **Title (bold 15px `P.ink`, centered, y=22):** "The Three Countries in the Advert"
- **Top strip — the claim as published** (y 40–130): just the three shown countries as large magenta dots on their own small axis, joined by a 2.5px `P.magenta` line, each labelled beneath in 12px `P.text` as "2 sodas / 47" style. Bold 12px `P.magenta` to the right: "more soda, higher wellness — three for three". This strip is what the reader is given.
- **Divider:** a 1px `P.grid` horizontal rule at y=142 with 12px `P.mute` centered label above the lower half: "the file those three came from".
- **Main plot:** `x` from 56 to `w−188`, `y` from 176 to `h−60`. X-axis 0–15 sodas with ticks every 3, labelled "sodas a week"; y-axis 40–95 with ticks every 10, labelled "wellness score" rotated. `P.grid` gridlines.
- **Points:** radius 4.5, drawn from the tallies described above.
- **Median guide:** a 1px dashed `#c8cdd6` horizontal at wellness 65, labelled 12px `P.mute` at its left: "healthier half above this line".
- **Side panel** at `w−178`: bold 13px `P.ink` "THE EVEN SPLIT", then bold 19px `P.magenta` "13 of 25" + 12px `P.mute` "heavy drinkers healthy", then bold 19px `P.aqua` "13 of 25" + 12px `P.mute` "light drinkers healthy", then bold 12px `P.ink` "no difference at all". Below that, bold 13px `P.ink` "TRIOS ON OFFER", bold 19px `P.magenta` "1,979" + 12px `P.mute` "climb with soda", bold 19px `P.aqua` "1,329" + 12px `P.mute` "fall with soda". All five figures printed from the tallies.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "Three countries were shown. Forty-seven were not."

---

## 3. Prophecies: Circle the Verse That Fits

**Tag colors:** `the same move, words` violet, `read backwards` blue, `120 verses` magenta
**Hue family:** magenta lit tiles on a grey grid with a violet footer

### canvas `c3` — 720×340

A grid of 120 verse tiles for one chosen event: the 8 that can be read as foretelling it lit up, the 112 that cannot left grey — the ones a reading never mentions.

- **Construction:** seeded Park–Miller LCG, seed 1847, over a vocabulary of **24** images, in this order: a tower, a river, a red sky, a black bird, a broken wall, fire, a great wind, a silver coin, a locked gate, thunder, a white horse, a dry well, a falling star, a crowd, a bell, a bridge, a mountain, a shadow, rain, a king, a ship, a serpent, a lamp, winter. Each of **120** verses names 3 distinct images; each of **30** events is described by 4. A verse counts as foretelling an event when they share **2 or more** images.
- **Computed counts:** the median match count over the 30 events is **8**; the chart draws the first event hitting that median, which is event **8** — images *a king, a great wind, a red sky, a serpent*. For it, **8** verses match and **112** do not. Across events the count runs from **3** at fewest to **18** at most. **103** of the 120 verses match at least one of the 30 events, leaving **17** that fit nothing. All figures tallied, none typed.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Event, One Hundred and Twenty Verses"
- **Event line:** bold 13px `P.ink` at y=46: "what happened", then the event's four images as 12px `P.text` words in small rounded `rgba(107,114,128,0.10)` chips across the line.
- **Verse grid:** 120 tiles in a 15 × 8 grid, tile 30×20 with 4px gaps, starting at `x=56, y=76`. Matching tiles fill `rgba(213,81,129,0.65)` stroked `P.magenta`; non-matching fill `rgba(107,114,128,0.10)` stroked `#dfe3e9`. Each matching tile carries a small bold 11px white check-free dot; no text inside tiles.
- **Bracket and labels:** a 1.5px `P.magenta` brace to the right of the grid pointing at the lit tiles with bold 13px `P.magenta` "8 verses fit — these get quoted"; below it bold 13px `P.mute` "112 verses do not — these never appear".
- **Footer strip** (12px, below the grid): bold 12px `P.ink` "across all 30 events:" then `P.mute` "as few as 3 verses fit, as many as 18"; on the next line bold 12px `P.violet` "103 of 120 verses fit at least one event".
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "The verse did not predict the event. The reader picked it afterwards."

---

## Page-specific constraints

- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`, capped at 720px so a wide cell leaves slack. Canvas CSS also carries `border: 1px solid #e0e0e0`, radius 4px. Per-chart heights 340 / 370 / 340.
- **Bullet counts: 10 / 7 / 7.** Seven is enough for a worked example — resist adding a bullet for every computed figure the chart already shows.
- **Text stands alone; the chart adds clarity** — the text carries the argument and names every quantity it turns on; the canvas adds precision, intermediate values and per-point labels. No bullet points at a position on the canvas. See `ui-templates/README.md`.
- **Countable whole numbers only.** No correlation coefficients, no fitted slopes, no conversion rates anywhere on the page. A claim is settled by counting things a reader can point at — one group's healthy count against the other's, the lit tiles against the grey — because "r = 0.00" and "4.0 points per serving" are unreadable to a non-statistician and cannot be checked by eye. The prose names the counts; the chart's side panel carries the ring-area percentage so the deserved hole count is derivable rather than asserted.
- **Name the move in plain words.** The page uses "cherry-picking" for showing the few cases that fit, and "painted afterwards" for the ring. It does not use multiple comparisons, family-wise error, p-hacking, subgroup analysis or selection bias anywhere in the visible text. This governs vocabulary only — it is not a licence to withhold a figure.
- **The numbered `Step 1 … Step 4` labels are sequence markers, not data, and stay.**
- **Section titles name the content.** No role labels ("The Trap", "Pipeline Defense") and no phrasing that would fit another page.
- **Colour variety across sections is required.** Section 1 magenta painted ring against aqua honest ring on grey holes; section 2 grey countries with a magenta shown-trio against an aqua opposite-trio; section 3 magenta lit tiles on a grey grid with a violet footer. Blue-fill-plus-orange-highlight must not be every chart.
- **No simulation anywhere on this page.** Every figure is a direct tally over the plotted points, so a full redraw is a few milliseconds and nothing needs memoising. If a future edit wants a probability figure, derive it in closed form or leave it off — do not add a repeat-loop to the browser for a number no chart displays.
- **Scope kept distinct from `05-clustering-illusion`.** That page argues that random points clump. This page takes the clumping as given and is about the *selection step* — the ring, the trio, the verse chosen after the fact. The disease-cluster-near-a-factory example belongs to that page and is deliberately absent here. No chart is shared: no dot grid, no coin strip, no shuffle, no street map.
- **Nothing is presented as measured.** The soda-and-health scatter is seeded with independent axes and labelled Illustrative Example; the factory and prophecy examples are stated as shapes of argument, with no company, place or text named.
- **No false-positive expectations.** Do not add figures of the form "200 subgroups × 0.05 = 10 significant results by chance" — that is an expected count, not the chance of finding one, and it reads as a discovery count. The selection argument is carried by counted objects instead: 10 holes against 1, and 1,979 rising trios against 1,329 falling out of the 9,277 that span 8+ sodas.
- **Layman vocabulary only.** No genome-wide thresholds, correction names, holdout or cross-validation terms. The same ideas appear as "name the group first" and "make it repeat on fresh visitors".
