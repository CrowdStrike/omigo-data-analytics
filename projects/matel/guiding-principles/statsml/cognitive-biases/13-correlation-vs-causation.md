# Correlation vs Causation: Five Other Things It Could Be

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Correlation vs Causation — Cognitive Biases

**Subtitle:** Everybody knows that two things moving together proves nothing. The useful part is knowing what else it could be — because the chart itself will never tell you.

**Design intent:** the who-causes-whom picture leads every section; the scatter is demoted to supporting evidence beside it. Plain language throughout — no fitted slopes, percentages or statistical vocabulary in the visible text. The recurring visual motif is a dashed arrow struck through with a red cross: *the arrow everyone assumes is the one that is not there.*

---

## Section 1 — The Vitamin Chart That Looks Like Proof

**Tags:** `the setup` (violet), `one chart` (blue), `a hidden third thing` (magenta)

**Bullets:**
- **What was counted** — 28 people, their vitamin pills a week, their days not sick
- **What the chart shows** — the people taking more pills were sick on fewer days
- **The obvious conclusion** — vitamins work, so hand them to everybody else
- **The question nobody asks** — what kind of person buys vitamins in the first place
- **The answer** — the same person who jogs, sleeps properly and eats their vegetables
- **So who earned the healthy days** — the jogging and the sleeping, not the pills
- **Why the chart still rises** — those habits bought the health and bought the pills too
- **What the pills were** — a marker of the kind of person, not the reason they were well

**Key point:** The chart is an honest picture of the data. "Vitamins keep you well" is a story about a world nobody watched — and the very same picture gets drawn by six completely different worlds.

**Source note (`.src`):** Illustrative Example — 28 made-up people where a hidden habit lifts both numbers; the dots and the line beside them are worked out in the drawing code.

### Visualization — canvas `c1`, 720×340

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

## Section 2 — The Six Worlds That Draw the Same Chart

**Tags:** `the whole list` (orange), `six pictures` (yellow), `one shape` (red)

**Bullets:**
- **One shape, six reasons** — the dots slope up in all six, and only one is worth acting on
- **It really works** — the pills do it, so handing them out delivers what was promised
- **The cause runs backwards** — people who are never ill are the ones who bother with pills
- **A third thing causes both** — the jogging buys the health and buys the vitamins too
- **Somebody dropped rows** — the people who did not fit the story quietly left the data
- **It is pure luck** — with a handful of people, a line through them means nothing
- **Both simply grew** — pill sales and lifespans both drifted up over the decades
- **What you cannot do** — pick between these six by staring harder at the dots

**Key point:** "Correlation is not causation" is the easy half that everyone can recite. The hard half is this list — and telling the six apart needs to know where the numbers came from, which is exactly what a chart leaves out.

**Source note (`.src`):** Illustrative Example — the six explanations drawn as pictures of who causes whom; the sample chart above them is the same 28 made-up people.

### Visualization — canvas `c2`, 720×420

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

## Section 3 — Backwards Cause: Shelf Space and Best Sellers

**Tags:** `backwards cause` (aqua), `shelf space` (orange), `acting on it` (blue)

**Bullets:**
- **What a shop sees** — the products with the most shelf space are the ones selling most
- **The tempting move** — give the slow products more space and watch them take off
- **What actually happened** — last year's best sellers were handed the space to begin with
- **So the arrow points the other way** — selling well won the space, not the reverse
- **Why the plan disappoints** — space helps a little, nowhere near what the chart suggested
- **The part people forget** — that space came off the best sellers, which now sell less
- **Net effect on the shop** — roughly nothing, after weeks of shifting shelves around
- **The tell** — ask what was decided first, and the whole picture flips

**Key point:** When the result quietly chose the cause, the cause looks powerful. Acting on it takes space away from the products that earned it and hands it to the ones that did not.

**Source note (`.src`):** Illustrative Example — 30 made-up products where last year's sales set this year's shelf space; the dots and both outcomes are worked out in the drawing code.

### Visualization — canvas `c3`, 720×340

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

## Section 4 — Both Simply Grew: Cafes and Dogs

**Tags:** `both just grow` (yellow), `a growing town` (magenta), `nothing in common` (violet)

**Bullets:**
- **Two counts in one town** — cafes on the high street, and dogs registered, over 24 years
- **What the chart shows** — they rise together so neatly it looks like a law of nature
- **The number you could quote** — around 50 extra dogs for every cafe that opened
- **What connects them** — nothing whatsoever, and no dog has ever been in a cafe
- **What is really happening** — the town filled up, and both counts rose with the people
- **Why it feels convincing** — anything growing keeps step with anything else growing
- **How common this is** — any two rising counts you pick will do the same thing
- **The giveaway** — the only thing the two counts share is the calendar

**Key point:** Two things that grow over the same years will always look linked, whatever they are. Growth does all the work, and closing a cafe has never cost anybody a dog.

**Source note (`.src`):** Illustrative Example — 24 made-up years where each count grows on its own with its own wobble, neither built from the other.

### Visualization — canvas `c4`, 720×340

Left: the two counts climbing together. Right: the town as the shared cause, with the cafe→dog link struck out.

- **Construction:** seeded LCG, seed 42; 24 years. `cafes[i] = 9 + 1.7i + (rng()·2−1)·3.0` and `dogs[i] = 420 + 95i + (rng()·2−1)·180`. Neither array references the other — every apparent link is the shared growth.
- **Computed and verified:** cafes run 6–48, dogs 429–2482, and the fitted line gives **about 54 more dogs per new cafe** — printed from the fit, not typed in. The two series track each other at about 0.98, which is why the chart looks like a law of nature.
- **Title (bold 15px `P.ink`, centered, y=22):** "Cafes and Dogs in One Growing Town, 24 Years"
- **Left plot:** `PX=46`, `PY=54`, `PW=0.40w`, `PH=158`. Each series is independently min–max scaled into the frame so both fit — the shape is the message, not the units. Dogs 2.4px `P.magenta`, cafes 2.4px `P.yellow`. Inline bold 12px legends "cafes" and "dogs registered" inside the frame; 12px `P.mute` "24 years →" below; bold 12px `P.violet` "about 54 more dogs per new cafe" under that.
- **Right picture** centered at `0.74w`: a violet `THE TOWN FILLED UP / more people, every year` box up top, with `CAFES` (yellow) and `DOGS` (magenta) boxes below, two 2.2px `P.violet` arrows fanning down. Between the lower boxes, a dashed `P.mute` arrow struck through with a red `cross()`, and a 12px `P.mute` note "no dog has ever been to a cafe".
- **Caption (bold 13px `P.yellow`, centered, `h−12`):** "Closing a cafe has never cost anybody a dog."
- **Replaced the old section 4.** This slot previously held twelve monthly series, all 66 pairings tallied, and a taught technique (compare month-to-month changes instead of levels) resting on a bespoke closeness measure. That was the most technical thing on the page and the measure was an invented stand-in for a named statistic. Cafes and dogs makes the same point with two lines and no machinery.

---

## Section 5 — When a Link Is Useful Anyway: Signs and Levers

**Tags:** `a sign is safe` (green), `guessing` (blue), `changing is not` (red)

**Bullets:**
- **A shop and a dark sky** — put umbrellas by the door when it clouds over, and they sell
- **The shop needs no theory** — it only needs the sky to darken before the rain arrives
- **Why the sign keeps working** — nothing was interfered with, so the pattern holds
- **Same with the vitamins** — guess who is rarely ill from who takes pills, and you do well
- **That guess stays honest** — you are spotting a type of person, not treating anybody
- **Now hand out the pills** — and the guess collapses, because you changed the world
- **What went wrong** — the pills marked the joggers; a pill does not make anyone jog
- **The line worth remembering** — an unexplained sign is usable, an unexplained lever is not

**Key point:** Using a link to guess something you did not measure is safe and needs no story about cause. Using it to decide what to change is a different act — it assumes a world that may never have existed.

**Source note (`.src`):** Illustrative Example — the umbrella shop as the everyday case, and the same 28 made-up people for the pills.

### Visualization — canvas `c5`, 720×340

Two halves split by a vertical `P.grid` rule: the safe act on the left, the unsafe one on the right. No scatter — both sides are pictures, because the distinction is about what you *do*, not about any number.

- **Left half** centered at `w/4`: bold 13px `P.green` header "READING A SIGN IS SAFE", then a vertical chain `dark sky` → `rain` → `people buy umbrellas` in mute / blue / green boxes with solid arrows. Below, bold 12px `P.green` "the shop watches the sky and stocks up" and two 12px `P.mute` lines — "it never has to know why —" / "nothing was interfered with".
- **Corrected claim.** An earlier draft asserted "clouds do not make anyone want an umbrella, they simply arrive first," which is false — clouds cause rain and rain causes umbrella buying. The chain is now drawn as it actually runs, and the real point is made instead: the shop never needs to know the mechanism, because it changes nothing.
- **Right half** centered at `3w/4`: bold 13px `#e74c3c` header "PULLING A LEVER IS NOT". A red `hand out the pills` box up top, an arrow down to `pills go up` (blue), and `jogging unchanged` (violet) beside it. Between them the dashed arrow the plan depended on, struck through with a red `cross()`. A `health barely moves` mute box at the bottom, reached by a violet arrow from the unchanged habit.
- **Right captions:** bold 12px `#e74c3c` "a pill does not make anybody jog", then 12px `P.mute` "so the habit that did the work never changed".
- **Title (bold 15px `P.ink`, centered, y=22):** "A Sign You Can Read, and a Lever You Cannot Pull"
- **Caption (bold 13px `P.green`, centered, `h−12`):** "Guessing needs no cause. Changing something does."

---

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`. Five `.card-section` blocks, each an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) plus a `table.layout` with one `<tr>`: `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note. No paragraph blocks, no data tables.
- **Bullet form:** ONE line each, ≤95 characters including the bold label, so nothing wraps at 50% column width.
- **Register — the governing constraint.** This is one of the easiest concepts on the site and the page must read that way. Everyday examples, plain words, no statistical vocabulary in anything the reader sees: no fitted slopes, no correlation values, no percentages, no "typical miss", no invented metrics. The word "correlation" appears only in the page title and in the phrase being quoted and dismissed. "Slope" survives only as the everyday verb in "the dots slope up".
- **Visual grammar — pictures first.** Every section leads with a boxes-and-arrows picture of who causes whom; any scatter is supporting evidence beside it, never the main event. The recurring motif is a dashed arrow struck through with a red cross, meaning *this is the link everyone assumes and it is not there*. It appears in `c1`, `c3`, `c4` and `c5`, which is what ties the page together.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse; td vertical-align top, padding 12px; `.viz-col` `text-align: center`. `ul` 0.92rem, margin `8px 0 8px 20px`; `li` 4px bottom margin; `li b` `#1a5276`. `.key-point` `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes: `.blue` `.green` `.red` `.orange` plus `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06c00`.
- **Hue family per section:** 1 violet habits with a blue chart and a green outcome; 2 one hue per explanation panel (green, blue, violet, magenta, yellow, aqua) with an orange caption, since which world you are in *is* the content; 3 aqua chart against an orange real-cause picture; 4 yellow cafes and magenta dogs under a violet shared cause; 5 green safe half against a red unsafe half.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"`; heights `c1` 340, `c2` 420, `c3` 340, `c4` 340, `c5` 340. `setup(id)` caches the logical size in `dataset` on first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on a 150ms debounced resize.
- **Shared drawing helpers:** `box(ctx, cx, cy, bw, bh, lines, hue, fill, fs)` draws a rounded label box centered on a point, taking one or two text lines; `arrow(ctx, x1, y1, x2, y2, hue, dash, lw)` draws a line with a filled arrowhead, dashed for a claimed-but-false link; `cross(ctx, cx, cy, r)` strikes a red `#e74c3c` X through a false arrow; `pillScatter(ctx, x0, y0, bw, bh, pp, shade, hue, dotR)` draws the vitamin scatter at any size and returns the fit.
- **Canvas font sizes:** chart title bold 15px; in-picture header bold 12–13px; box labels bold 11–12px; panel notes 11px; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Red crosses use `#e74c3c` directly.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`). Seeds: `buildPeople()` (shared by `c1`, `c2`, `c5`) 1234; `c3` 4; `c4` 42; the dropped-rows panel in `c2` seeds 6 but plots fixed offsets, not fitted data.
- **The two planted quantities** are `TRUE_V = 0.5` days per weekly pill and `TRUE_F = 0.4` units per shelf slot. Every promised-against-delivered figure derives from them; every printed line value is fitted at render time from the dots that chart plots.
- **Figures verified by running the page's own code:** the shelf plan promises +120 units a week and delivers +12; the cafe chart gives about 54 more dogs per new cafe; the hidden habit correlates with pills at about 0.81 and the two town counts at about 0.98.
- **Rendering not verified.** Per project instructions no browser or screenshot check was run. The arrow diagrams are hand-placed canvas layouts, so coordinate collisions are the likeliest defect if anything looks wrong.
- **File was renamed.** This page was `13-causal-reasoning.*` and is now `13-correlation-vs-causation.*`; the rename happened outside this document's history.
- **History of this page.** It previously carried "Correlation ≠ Causation", "Base-Rate Neglect in Co-occurrence" and "Crediting the Wrong Active Ingredient" as three list sections; the latter two became separate pages. Since then, in order: five arrow diagrams with no data at all were replaced by six computed scatters; those were then found too technical and replaced by the present picture-led treatment. Corrections carried through every revision:
  - The old page taught only ONE alternative to "A causes B" — the shared third cause — and called it "the most common trap". All five alternatives are now named in plain words, each with its own picture.
  - The old takeaway asserted "most 'A causes B' headlines are driven by an unmeasured confound C", an unsourced claim about a population nobody counted. It is gone.
  - The old page implied any non-causal link is useless. Section 5 corrects that: guessing needs no cause, only changing does.
  - An intermediate draft labelled panels only as "Panel one… Panel two…", leaving the reader to reverse-engineer each mechanism from a paraphrase. The mechanisms are now named in the label position.
  - An intermediate draft built the hidden confounder at a correlation of about 0.98 with the treatment — collinear enough that no method could separate them, a stronger and different lesson than intended. It now sits at about 0.81.
  - An intermediate draft spelled every number as a word ("three tenths of a star", "a twentieth"), which is unscannable and cannot be checked against the chart. Figures that remain are digits; most were cut entirely.
