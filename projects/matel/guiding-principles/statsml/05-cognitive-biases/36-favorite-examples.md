# Favorite Examples: The Same Five Checks, Forever

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** Favorite Examples — Cognitive Biases

**Subtitle:** Everyone keeps a personal set of go-to checks — the queries typed into every search system, the questions asked in every interview, the examples run at every demo. Repeating a fixed set builds confidence without adding coverage, and anything repeated long enough gets fixed first.

---

## Section 1 — Twenty Checks, Three Features Covered

**Tags:** `core idea` (violet), `frozen slice` (blue), `no new coverage` (magenta)

**Bullets:**
- **The habit** — a fixed set of favourite checks, used unchanged every time something is evaluated
- **Why it forms** — the favourites once caught a real problem, so they earned a permanent place
- **What they cover** — 3 of the system's 10 features, the same 3 on every single run
- **Run them 20 times** — 20 checks performed, and still only 3 features ever touched
- **Spread the same 20** — every one of the 10 features gets checked twice over
- **What repetition adds** — confidence, since the answer keeps coming back the same
- **What repetition does not add** — coverage, because the 7 untouched features stay untouched
- **How it feels from inside** — like thorough testing, since the effort is real and the count is high
- **The honest description** — one observation of 3 features, repeated 20 times over

**Key point:** Confidence grows with the number of checks while coverage grows only with the number of *distinct* checks. A frozen favourite set decouples the two, so the person with 20 runs behind them feels far better informed than the person with 10 — while actually knowing about a third as much of the system.

**Source note (`.src`):** Illustrative Example — a 10-feature system checked 20 times, once as a frozen 3-feature set and once spread across all 10; the coverage counts are tallied in the draw function.

### Visualization — canvas `c1`, 720×340

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

## Section 2 — Whatever Gets Checked Every Time Gets Fixed First

**Tags:** `the drift` (orange), `tuned to the sample` (yellow), `over-rated` (red)

**Bullets:**
- **The favourites are public** — everyone knows which queries get typed and which questions get asked
- **So they get attention first** — a failure on a favourite is the one guaranteed to be noticed
- **After a while** — all 3 favourite features work, because they are the ones that got the work
- **Meanwhile** — of the 7 nobody checks, 4 work and 3 do not, since nothing pushed anyone to look
- **What the favourites report** — 3 of 3, a clean pass, and it is a true report of those 3
- **What the system actually is** — 7 of 10, and no favourite check can ever show that
- **The direction is not random** — the frozen set over-rates, because it is the set that got repaired
- **This is the opposite of a harsh verdict** — a stale favourite set is reliably too kind
- **What fixes it** — retiring checks that always pass, since a check that never fails measures nothing

**Key point:** A slice that is chosen once and reused becomes the slice most likely to work, because effort follows attention and attention follows the favourites. That makes the error one-directional: an unchanging set of checks does not merely sample the system badly, it samples the best-maintained part of it and reports that as the whole.

**Source note (`.src`):** Illustrative Example — the same 10-feature system after the 3 favourite features have been repaired; the two scores are computed in the draw function.

### Visualization — canvas `c2`, 720×340

**A stage lamp, not a chart.** Ten objects on a shelf under a lamp that lights only three of them. The lit three are polished because they are the ones anyone can see; two of the seven in the dark are cracked.

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

## Regeneration instructions

- **Template:** the card-section layout from `05-cognitive-biases/25-mere-exposure-effect.html`. One `.card-section` per section, each an `<h2>` plus a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note.
- **Bullet form:** one line at 50% column width, 90–100 characters including the bold label.
- **Page CSS:** identical to `25-mere-exposure-effect.html`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Hue family per section:** 1 magenta against aqua with a violet caption, 2 yellow against aqua with a hard red gap bracket.
- **Canvas:** intrinsic `width="720"`, both heights 340. `setup(id)` caches the logical size in `dataset`, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)`. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12px; body and axis labels 12px floor; big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` for broken features and the gap bracket only.
- **Determinism:** no `Math.random()` and no generator. Both check orders and the working/broken assignment are fixed lists; the lesson is the coverage count and the score gap, and a draw would only make them irreproducible.
- **Every printed figure is computed in its draw function** — both coverage counts and the untouched count in section 1; both scores and the gap in section 2.
- **No decay curve.** An earlier attempt modelled the favourites losing their separating power round by round, at a leak rate of 15% per reuse. That rate is invented, and a fabricated slope printed beside a plotted line teaches a number nobody can check. The one-directional over-rating in section 2 makes the same point out of counting.
- **Keep the numbers round.** 10 features, 3 favourites, 20 checks, 7 of 10. No probability arithmetic anywhere on the page.
- **The favourites must be genuinely good checks.** They earned their place by catching something real. The page fails if it reads as "these people picked bad tests" — the defect is that a good check goes stale when it is never replaced.
- **What separates this from card 35.** Scope Neglect is a slice each person happens to land on, which errs in both directions. Here the slice is deliberately frozen and publicly known, so the error is one-directional and gets worse the longer the set is kept.
- **What separates this from card 25.** Mere-Exposure Effect is about the familiar feeling better made. Here familiarity has a mechanical consequence rather than a felt one: the familiar checks pull the maintenance effort toward themselves.
- **Keep this page at two sections.**
