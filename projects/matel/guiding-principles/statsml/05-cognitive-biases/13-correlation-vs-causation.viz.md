# Correlation vs Causation — Viz

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Correlation vs Causation — Cognitive Biases
**Template:** card-section layout from `statistical-paradoxes/03-berksons-paradox.html` — five `.card-section` blocks, each an `<h2>` plus a `table.layout` with one `<tr>`: `td.text-col` 50% / `td.viz-col` 50%.
**Source note wording:** the sibling `.txt.md` says figures are "worked out at render time"; the html `.src` notes say "worked out in the drawing code".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Design intent:** the who-causes-whom picture leads every section; the scatter is demoted to supporting evidence beside it. Plain language throughout — no fitted slopes, percentages or statistical vocabulary in the visible text. The recurring visual motif is a dashed arrow struck through with a red cross: *the arrow everyone assumes is the one that is not there.*

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed, a planted quantity (`TRUE_V`, `TRUE_F`) or a construction invalidates that prose — re-read the computed values and update the text to match. The prose currently quotes: the 13-day gap between the 14 heaviest and 14 lightest pill takers (`c1` people, mean days not sick 349.3 against 336.3); the shelf plan's +120 promised against +12 delivered (`c3`, `f.slope = 4.02` × 3 slots × 10 products against `TRUE_F = 0.4` × 30); and the cafe/dog figures (`c4`, cafes 6.0–48.3, dogs 429–2482, fitted slope 54.18 → the canvas prints "about 54").

**Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`). Seeds: `buildPeople()` (shared by `c1`, `c2`, `c5`) 1234; `c3` 4; `c4` 42; the dropped-rows panel in `c2` seeds 6 but plots fixed offsets, not fitted data.

---

## 1. The Vitamin Chart That Looks Like Proof

**Tag colors:** `the setup` violet, `one chart` blue, `a hidden third thing` magenta
**Hue family:** violet habits with a blue chart and a green outcome

### canvas `c1` — 720×340

Left: the innocent-looking scatter. Right: the boxes-and-arrows picture that explains it.

- **Construction (shared by `c1`, `c2`, `c5` via `buildPeople()`):** seeded Park–Miller LCG, seed 1234; 28 people. A hidden habit `q` in 0–1 (jogging, sleep, vegetables) is drawn first, then `p = clamp(2 + 10q + (rng()·2−1)·5.0, 0, 14)` pills a week. With `pbar = mean(p)`, `d = min(365, 330 + 26q + TRUE_V·(p − pbar) + (rng()·2−1)·4.0)` days not sick. `TRUE_V = 0.5` is the only planted quantity — what one pill a week is really worth.
- **Why these constants:** the hidden habit correlates with pills at about 0.81 — a genuine confounder, not a near-duplicate of the treatment. An earlier draft used a confound at 0.98, which is collinear enough that *no* method could separate the two, teaching a different and wrong lesson.
- **Title (bold 15px `P.ink`, centered, y=22):** "What Was Charted, and What Was Actually Going On"
- **Left scatter** at `PX=52`, `PY=58`, `PW=0.34w`, `PH=150`, via the shared `pillScatter()` helper with `shade=true`. X axis 0–15 pills, Y axis 324–364 days. Dots radius 4, fill `rgba(74,58,167, 0.15 + 0.6q)` stroked `P.violet`, so the hidden habit is visible rather than asserted. Best-fit line 2.5px `P.blue`, computed from the plotted dots.
- **Left labels:** bold 12px `P.blue` "more pills, fewer sick days" above the frame; 12px `P.mute` axis captions "vitamin pills a week" and rotated "days not sick"; two 12px `P.violet` lines beneath — "darker dots = the joggers and" / "salad eaters, sitting up and right".
- **Right picture** centered at `0.71w`: a violet `GOOD HABITS / jogging, sleep, veg` box at y=76, with `VITAMIN PILLS` (blue) and `DAYS NOT SICK` (green) boxes at y=196 either side. Two 2.2px `P.violet` arrows fan down from habits to both. Between the two lower boxes, a dashed `P.mute` horizontal arrow struck through with a red `cross()` — the assumed link that does not exist.
- **Right captions:** bold 12px `P.violet` "one cause, two effects" at y=130; bold 12px `#e74c3c` "the arrow everyone assumes" and 12px `P.mute` "is the one that is not there" below the boxes.
- **Caption (bold 13px `P.violet`, centered, `h−14`):** "The pills marked the healthy people. They did not make them healthy."

---

## 2. The Six Worlds That Draw the Same Chart

**Tag colors:** `the whole list` orange, `six pictures` yellow, `one shape` red
**Hue family:** one hue per explanation panel (green, blue, violet, magenta, yellow, aqua) with an orange caption, since which world you are in *is* the content

### canvas `c2` — 720×420

One shared scatter at the top, then six small explanation pictures in a three-by-two grid. The point is structural: the same chart sits above all six, so the chart cannot be what distinguishes them.

- **Shared chart:** `pillScatter()` at `SW=168`, `SH=104`, centered, `SY=36`, `shade=false`, dots radius 3, line `P.ink`. A 12px `P.mute` caption reads "pills against sick days — this is all you are shown".
- **Grid:** origin `GY = SY + SH + 34`, cell width `w/3`, cell height 118. Per panel a bold 12px numbered title in the panel's hue, the picture at `cy+46`, and an 11px `P.mute` note at `cy+90`.
- **Panel 1 — "it really works"** (`P.green`), note "handing out pills pays off": `pills` box → `health` box, one forward arrow.
- **Panel 2 — "the cause runs backwards"** (`P.blue`), note "the well buy the vitamins": same two boxes, arrow reversed.
- **Panel 3 — "a third thing causes both"** (`P.violet`), note "jogging did both jobs": a `habits` box above, `pills` and `health` below, two arrows fanning down.
- **Panel 4 — "somebody dropped rows"** (`P.magenta`), note "the misfits left the data": four filled magenta dots rising, three hollow `P.mute` dots each struck through with a red cross.
- **Panel 5 — "it is pure luck"** (`P.yellow`), note "too few people to mean a thing": six scattered dots with a 2px line pushed through them.
- **Panel 6 — "both simply grew"** (`P.aqua`), note "only the calendar links them": two rising lines side by side with an 11px "over the years →".
- **Title (bold 15px `P.ink`, centered, y=22):** "One Chart Up Here. Six Different Worlds Down There."
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "Only the first one rewards buying pills. The chart looks identical in all six."
- **Deliberately not computed.** Unlike an earlier draft, these six panels are *diagrams*, not six seeded scatters fitted to matching slopes. The claim is about causal structure, not about numbers, so no figures are printed and none need verifying.

---

## 3. Backwards Cause: Shelf Space and Best Sellers

**Tag colors:** `backwards cause` aqua, `shelf space` orange, `acting on it` blue
**Hue family:** aqua chart against an orange real-cause picture

### canvas `c3` — 720×340

Left: the steep-looking scatter. Right: the assumed picture struck out, the real picture below it, then the payoff.

- **Construction:** seeded LCG, seed 4; 30 products. `last = 4 + 34·rng()` (last year's weekly units) is drawn first, then `f = clamp(round(last/3.6 + (rng()·2−1)·1.0), 1, 12)` shelf slots. With `fbar = mean(f)`, this year's `u = max(1, last + TRUE_F·(f − fbar) + (rng()·2−1)·2.2)`. `TRUE_F = 0.4` units a week is what a slot is really worth.
- **Computed and verified:** the fitted line makes a slot look worth about +4 units a week, roughly ten times the truth. Three extra slots to each of the ten slowest products means 30 slots moved; **the chart promises +120 units a week and it delivers +12**, taken off the best sellers. Both figures print from `f.slope × 3 × 10` and `TRUE_F × 3 × 10`, never typed in.
- **Title (bold 15px `P.ink`, centered, y=22):** "Which Came First: The Shelf Space or the Sales?"
- **Left scatter:** `PX=50`, `PY=56`, `PW=0.32w`, `PH=150`. X axis 0–12 slots, Y axis 0–44 units. Dots radius 4 `rgba(25,158,112,0.50)` stroked `P.aqua`; best-fit line 2.5px `P.aqua`. Bold 12px `P.aqua` "wider shelf, more sales" above; 12px `P.mute` captions "shelf space →" and rotated "units sold".
- **Right, upper picture** centered at `0.70w`: bold 12px `P.mute` header "WHAT THE SHOP ASSUMED", then `shelf space` → `sales` in mute boxes with a dashed arrow struck through by a red `cross()`.
- **Right, lower picture:** bold 12px `P.orange` header "WHAT ACTUALLY HAPPENED", then `shelf space` ← `last year's sales` in orange boxes with a solid 2.2px `P.orange` arrow running right to left.
- **Payoff block:** 12px `P.mute` "give the slow products more space:", then bold 13px `P.orange` "the chart promises +120 units a week" and bold 13px `P.aqua` "it delivers +12, taken off the best sellers".
- **Caption (bold 13px `P.aqua`, centered, `h−12`):** "Shelf space did not make a product popular. Being popular won it the space."

---

## 4. Both Simply Grew: Cafes and Dogs

**Tag colors:** `both just grow` yellow, `a growing town` magenta, `nothing in common` violet
**Hue family:** yellow cafes and magenta dogs under a violet shared cause

### canvas `c4` — 720×340

Left: the two counts climbing together. Right: the town as the shared cause, with the cafe→dog link struck out.

- **Construction:** seeded LCG, seed 42; 24 years. `cafes[i] = 9 + 1.7i + (rng()·2−1)·3.0` and `dogs[i] = 420 + 95i + (rng()·2−1)·180`. Neither array references the other — every apparent link is the shared growth.
- **Computed and verified:** cafes run 6–48, dogs 429–2482, and the fitted line gives **about 54 more dogs per new cafe** — printed from the fit, not typed in. The two series track each other at about 0.98, which is why the chart looks like a law of nature.
- **Title (bold 15px `P.ink`, centered, y=22):** "Cafes and Dogs in One Growing Town, 24 Years"
- **Left plot:** `PX=46`, `PY=54`, `PW=0.40w`, `PH=158`. Each series is independently min–max scaled into the frame so both fit — the shape is the message, not the units. Dogs 2.4px `P.magenta`, cafes 2.4px `P.yellow`. Inline bold 12px legends "cafes" and "dogs registered" inside the frame; 12px `P.mute` "24 years →" below; bold 12px `P.violet` "about 54 more dogs per new cafe" under that.
- **Right picture** centered at `0.74w`: a violet `THE TOWN FILLED UP / more people, every year` box up top, with `CAFES` (yellow) and `DOGS` (magenta) boxes below, two 2.2px `P.violet` arrows fanning down. Between the lower boxes, a dashed `P.mute` arrow struck through with a red `cross()`, and a 12px `P.mute` note "no dog has ever been to a cafe".
- **Caption (bold 13px `P.yellow`, centered, `h−12`):** "Closing a cafe has never cost anybody a dog."
- **Replaced the old section 4.** This slot previously held twelve monthly series, all 66 pairings tallied, and a taught technique (compare month-to-month changes instead of levels) resting on a bespoke closeness measure. That was the most technical thing on the page and the measure was an invented stand-in for a named statistic. Cafes and dogs makes the same point with two lines and no machinery.

---

## 5. When a Link Is Useful Anyway: Signs and Levers

**Tag colors:** `a sign is safe` green, `guessing` blue, `changing is not` red
**Hue family:** green safe half against a red unsafe half

### canvas `c5` — 720×340

Two halves split by a vertical `P.grid` rule: the safe act on the left, the unsafe one on the right. No scatter — both sides are pictures, because the distinction is about what you *do*, not about any number.

- **Left half** centered at `w/4`: bold 13px `P.green` header "READING A SIGN IS SAFE", then a vertical chain `dark sky` → `rain` → `people buy umbrellas` in mute / blue / green boxes with solid arrows. Below, bold 12px `P.green` "the shop watches the sky and stocks up" and two 12px `P.mute` lines — "it never has to know why —" / "nothing was interfered with".
- **Corrected claim.** An earlier draft asserted "clouds do not make anyone want an umbrella, they simply arrive first," which is false — clouds cause rain and rain causes umbrella buying. The chain is now drawn as it actually runs, and the real point is made instead: the shop never needs to know the mechanism, because it changes nothing.
- **Right half** centered at `3w/4`: bold 13px `#e74c3c` header "PULLING A LEVER IS NOT". A red `hand out the pills` box up top, an arrow down to `pills go up` (blue), and `jogging unchanged` (violet) beside it. Between them the dashed arrow the plan depended on, struck through with a red `cross()`. A `health barely moves` mute box at the bottom, reached by a violet arrow from the unchanged habit.
- **Right captions:** bold 12px `#e74c3c` "a pill does not make anybody jog", then 12px `P.mute` "so the habit that did the work never changed".
- **Title (bold 15px `P.ink`, centered, y=22):** "A Sign You Can Read, and a Lever You Cannot Pull"
- **Caption (bold 13px `P.green`, centered, `h−12`):** "Guessing needs no cause. Changing something does."

---

## Page-specific constraints

- **Text stands alone; the chart adds clarity** — the text carries the argument and names every quantity it turns on; the canvas adds precision, intermediate values and per-point labels. No bullet points at a position on the canvas. See `ui-templates/README.md`.
- **Register — the governing constraint.** This is one of the easiest concepts on the site and the page must read that way. Everyday examples and plain words, and no statistical vocabulary in anything the reader sees: no fitted-slope or correlation values, no percentages, no "typical miss", no invented metrics. Plain counts in everyday units (days, units a week, dogs per cafe) are not vocabulary and belong in the prose. The word "correlation" appears only in the page title and in the phrase being quoted and dismissed. "Slope" survives only as the everyday verb in "slope up".
- **Visual grammar — pictures first.** Every section leads with a boxes-and-arrows picture of who causes whom; any scatter is supporting evidence beside it, never the main event. The recurring motif is a dashed arrow struck through with a red cross, meaning *this is the link everyone assumes and it is not there*. It appears in `c1`, `c3`, `c4` and `c5`, which is what ties the page together.
- **Canvas heights are page-specific:** `c1` 340, `c2` 420, `c3` 340, `c4` 340, `c5` 340.
- **Shared drawing helpers (bespoke to this page):** `box(ctx, cx, cy, bw, bh, lines, hue, fill, fs)` draws a rounded label box centered on a point, taking one or two text lines; `arrow(ctx, x1, y1, x2, y2, hue, dash, lw)` draws a line with a filled arrowhead, dashed for a claimed-but-false link; `cross(ctx, cx, cy, r)` strikes a red `#e74c3c` X through a false arrow; `pillScatter(ctx, x0, y0, bw, bh, pp, shade, hue, dotR)` draws the vitamin scatter at any size and returns the fit.
- **Extra tag-pill classes beyond the house four:** `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06c00`. Red crosses use `#e74c3c` directly, outside the shared palette.
- **Bullet form on this page:** ONE line each, ≤95 characters including the bold label.
- **The two planted quantities** are `TRUE_V = 0.5` days per weekly pill and `TRUE_F = 0.4` units per shelf slot. Every promised-against-delivered figure derives from them; every printed line value is fitted at render time from the dots that chart plots.
- **Figures verified by running the page's own code:** the shelf plan promises +120 units a week and delivers +12; the cafe chart gives about 54 more dogs per new cafe; the hidden habit correlates with pills at about 0.81 and the two town counts at about 0.98.
- **Keep the confounder at about 0.81, not 0.98.** An intermediate draft built the hidden confounder collinear enough with the treatment that no method could separate them — a stronger and different lesson than intended.
- **Do not reinstate the computed six-scatter version of section 2.** Five arrow diagrams with no data were once replaced by six computed scatters fitted to matching slopes; those were found too technical and replaced by the present picture-led diagrams. Panels must name their mechanism in the label position, never "Panel one… Panel two…".
- **Do not reinstate the old section 4.** Twelve monthly series, 66 tallied pairings and a bespoke closeness measure standing in for a named statistic. Cut for cafes and dogs.
- **All five alternatives must be named in plain words,** each with its own picture. The old page taught only the shared third cause and called it "the most common trap".
- **No unsourced population claims.** The old takeaway asserted "most 'A causes B' headlines are driven by an unmeasured confound C" — removed.
- **Non-causal links are not useless.** Section 5 exists to make that correction: guessing needs no cause, only changing does.
- **Figures are digits, not words.** An intermediate draft spelled numbers out ("three tenths of a star", "a twentieth"), which is unscannable and uncheckable against the chart. Most were cut entirely.
- **Rendering not verified.** Per project instructions no browser or screenshot check was run. The arrow diagrams are hand-placed canvas layouts, so coordinate collisions are the likeliest defect if anything looks wrong.
- **File was renamed.** This page was `13-causal-reasoning.*` and is now `13-correlation-vs-causation.*`; the rename happened outside this document's history.
- **History note.** The page previously carried "Correlation ≠ Causation", "Base-Rate Neglect in Co-occurrence" and "Crediting the Wrong Active Ingredient" as three list sections; the latter two became separate pages.
