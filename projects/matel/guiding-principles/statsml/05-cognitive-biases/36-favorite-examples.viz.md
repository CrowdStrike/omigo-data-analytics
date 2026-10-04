# Favorite Examples — Viz

**Page type:** detail page, card-section (template `06-sectioned-cards-callout`)
**HTML title tag:** Favorite Examples — Cognitive Biases
**Template:** card-section layout from `05-cognitive-biases/25-mere-exposure-effect.html` — one `.card-section` per section, `<h2>` plus `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
**Source note wording:** the sibling `.txt.md` says figures are "tallied / computed at render time"; the html `.src` notes say "tallied in the draw function" and "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing the door lists or the working/broken array invalidates that prose — the coverage counts, the untouched count, both scores and the cracked count must be re-read from the new draw and the text updated to match.

**Determinism:** no `Math.random()` and no generator. Both check orders and the working/broken assignment are fixed lists; the lesson is the coverage count and the score gap, and a draw would only make them irreproducible.

---

## 1. Twenty Checks, Three Features Covered

**Tag colors:** `core idea` violet, `frozen slice` blue, `no new coverage` magenta
**Hue family:** magenta against aqua with a violet caption

### canvas `c1` — 720×340

**A worn path, not a chart.** Ten doors along a corridor. Three of them have a track beaten into the floor by twenty trips; the other seven are still shut, with the dust undisturbed.

- **Data:** no PRNG. 10 doors, 20 trips. Frozen habit: doors 2, 5 and 7 in rotation. The comparison habit walks doors 1 through 10, twice round. Both trip lists are fixed arrays in the code.
- **Computed:** **20** trips either way; frozen reaches **3 of 10** doors and leaves **7** shut; spread reaches **10 of 10** and leaves **0**. Trip counts per door are tallied from the arrays, and the footprint marks drawn per door equal that tally.
- **Title (bold 15px `P.ink`, centered, y=21):** "Twenty Trips Down the Same Corridor"
- **Layout:** two corridors stacked, one per habit. Each is a 10-door row across `PX=52 … w−52`, doors 34px wide and 46px tall standing on a floor line — corridor one floored at y=132, corridor two at y=268.
- **Doors:** a visited door is drawn open — frame stroked in the panel hue, panel swung ajar as a thin parallelogram, interior `rgba(255,255,255,0.9)`. An unvisited door is shut, filled `#f4f5f7` stroked `P.grid`, with three short 1px `P.grid` dust ticks across its base.
- **The worn track:** a 7px `rgba(213,81,129,0.30)` line along the floor of corridor one that spans only the visited doors, thickening where the trips concentrate; in corridor two a 3px `rgba(25,158,112,0.25)` line spanning the whole floor evenly.
- **Footprints:** small 4px marks stacked above each visited door, one per trip to that door, in the panel hue — so the frozen corridor grows three tall stacks and the spread corridor grows ten short ones.
- **Door numbers:** 12px `P.mute` beneath each door. A shut door additionally carries a 12px `P.mute` "—" where its footprint stack would be.
- **Corridor headers** (bold 12px at each corridor's left): `P.magenta` "THE SAME THREE DOORS, TWENTY TIMES" and `P.aqua` "TWENTY TRIPS, EVERY DOOR OPENED".
- **Corridor tallies** (right end of each floor line): bold 19px in the panel hue "3 of 10" / "10 of 10", then 12px `P.mute` "doors opened", computed.
- **Note line** (12px `P.mute`, centered, `h−28`): "both walked 20 times and both feel equally thorough", with the 20 computed.
- **Caption (bold 13px `P.violet`, centered, `h−8`):** "Confidence counts every trip; coverage counts only the distinct doors."

---

## 2. Whatever Gets Checked Every Time Gets Fixed First

**Tag colors:** `the drift` orange, `tuned to the sample` yellow, `over-rated` red
**Hue family:** yellow against aqua with a hard red gap bracket

### canvas `c2` — 720×340

**A stage lamp, not a chart.** Ten objects on a shelf under a lamp that lights only three of them. The lit three are polished because they are the ones anyone can see; three of the seven in the dark are cracked.

- **Data:** no PRNG. 10 features as objects. The 3 under the lamp all work. Of the 7 in the dark, 4 work and 3 do not. A fixed array in the code holds the working flag per feature.
- **Computed:** lit set **3 of 3**; whole shelf **7 of 10**; the gap **3 features**, all derived from the same array rather than typed.
- **Title (bold 15px `P.ink`, centered, y=21):** "The Lamp Lights Three of the Ten"
- **Layout:** a shelf line across `PX=48 … w−48` at y=214, with 10 objects standing on it at even spacing. A lamp at the top centre-left casts a cone over objects 2 to 4.
- **The lamp:** a small 12px trapezoid housing at y=52, with a cone drawn as a filled path down to the shelf in a `rgba(201,133,0,0.16)` fill, edges 1px `rgba(201,133,0,0.45)`. Everything outside the cone sits on a `rgba(43,62,80,0.06)` wash to read as dim.
- **Objects:** each a 26×34 rounded box on the shelf. A working object is filled `rgba(25,158,112,0.30)` stroked `P.aqua`; a cracked one is filled `rgba(213,81,129,0.45)` stroked `P.magenta` with a 2px `#e74c3c` zigzag crack down its face. Lit objects additionally carry a 2px white highlight stroke down their left edge to read as polished.
- **Labels:** 12px `P.mute` numbers beneath each object. Bold 12px `P.yellow` "checked every time" above the cone; 12px `P.mute` "nobody looks here" at the shelf's right end.
- **Two verdict blocks** below the shelf at y=282: on the left, bold 19px `P.yellow` "3 of 3" over 12px `P.mute` "what the favourites report"; to its right, bold 19px `P.aqua` "7 of 10" over 12px `P.mute` "what is actually on the shelf". Both computed.
- **The cracked count:** bold 12px `#e74c3c` between the blocks, "3 cracked, all of them outside the light", computed from the array.
- **Note line** (12px `P.mute`, centered, `h−28`): "a check that has passed every time for a year is no longer measuring anything".
- **Caption (bold 13px `P.orange`, centered, `h−8`):** "Whatever the lamp lights is the part that gets polished."

---

## Page-specific constraints

- **Hard red `#e74c3c`** is reserved for broken features and the gap bracket only; everything else comes from the shared palette.
- **Every printed figure is computed in its draw function** — both coverage counts and the untouched count in section 1; both scores and the gap in section 2.
- **No decay curve.** An earlier attempt modelled the favourites losing their separating power round by round, at a leak rate of 15% per reuse. That rate is invented, and a fabricated slope printed beside a plotted line teaches a number nobody can check. The one-directional over-rating in section 2 makes the same point out of counting.
- **Keep the numbers round.** 10 features, 3 favourites, 20 checks, 7 of 10. No probability arithmetic anywhere on the page.
- **The favourites must be genuinely good checks.** They earned their place by catching something real. The page fails if it reads as "these people picked bad tests" — the defect is that a good check goes stale when it is never replaced.
- **What separates this from card 35.** Scope Neglect is a slice each person happens to land on, which errs in both directions. Here the slice is deliberately frozen and publicly known, so the error is one-directional and gets worse the longer the set is kept.
- **What separates this from card 25.** Mere-Exposure Effect is about the familiar feeling better made. Here familiarity has a mechanical consequence rather than a felt one: the familiar checks pull the maintenance effort toward themselves.
- **Keep this page at two sections.**
