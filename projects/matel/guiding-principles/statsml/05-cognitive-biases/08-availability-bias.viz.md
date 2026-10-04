# Availability Bias — Viz

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Availability Bias — Cognitive Biases
**Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, as converted in `05-clustering-illusion.html`
**Source note wording:** the sibling `.txt.md` says figures for sections 2 and 3 are "computed at render time"; the html `.src` notes say "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed or the construction invalidates that prose — re-read the computed values and update the text to match.

**Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, for `c2`'s recall patterns and `c4`'s letter-writer placement; literal arrays elsewhere.

---

## 1. What People Name First Happens Least Often

**Tag colors:** `core idea` violet, `two orderings` magenta, `loud beats large` blue
**Hue family:** violet with magenta crossings

### canvas `c1` — 720×340

Two ranked columns of the same eight hazards — left ordered by how many people name it, right by how often it happens — joined by lines that cross.

- **Data (literal array, `[name, named-out-of-50, cases per year in a city of ten million]`):**
  `Shark bite, 34, 2` · `Lightning strike, 29, 60` · `Dog bite, 26, 30000` · `Choking on food, 22, 9000` · `Escalator fall, 18, 900` · `Reaction to a sting, 15, 4000` · `Bathroom slip, 12, 90000` · `Fall on the stairs, 9, 140000`
- **Computed in the draw function:** the two orderings by sort; the number of disagreeing pairs (23 of 28); the rank shift of each hazard (max 7, for the shark and the stairs); and the rate ratio between the first-named and the most-common hazard (70,000×). The most common hazard, 140,000 a year, also beats the other seven summed (133,962) — checked separately, not printed on the chart.
- **Title (bold 15px `P.ink`, centered, y=22):** "Eight Hazards, Ranked Two Ways"
- **Column headers (bold 13px, y=52):** left, right-aligned at x=234, `P.magenta` — "HOW EASILY IT COMES TO MIND"; right, left-aligned at x=428, `P.aqua` — "HOW OFTEN IT ACTUALLY HAPPENS".
- **Rows:** eight rows, first baseline y=76, pitch 25. Left labels 12px right-aligned at x=234 with the naming count in 12px `P.mute` at x=54; right labels 12px left-aligned at x=428 with the yearly rate in 12px `P.mute` right-aligned at `w−30`, thousands-separated.
- **Connectors:** from `(246, leftRowY − 4)` to `(416, rightRowY − 4)`. Line width and colour driven by the computed shift: shift ≥ 5 gets 2.5px `P.magenta`; shift 1–4 gets 1.5px `rgba(74,58,167,0.55)`; shift 0 gets 1.5px `P.aqua`. A 3px dot in the line colour at each end.
- **Column footnotes (12px `P.mute`, y = last row + 20):** "named, out of fifty asked" at the left; "cases a year per ten million people" right-aligned at `w−30`.
- **Bottom band:** bold 13px `P.magenta` at `h−48` "23 of the 28 pairs come out in the opposite order" printed from the pair count; 12px `P.mute` at `h−30` "the first-named hazard is 70,000× rarer than the last-named one", the multiple computed.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "The list memory hands you is sorted by vividness, not by count."

---

## 2. Thirty Crashes and Thirty Swapped Columns

**Tag colors:** `vividness` aqua, `equal counts` blue, `unequal traces` orange
**Hue family:** aqua against yellow

### canvas `c2` — 720×320

Two strips of thirty identical squares, one per event, with the squares that stayed in memory filled in — same length, very different amount of ink.

- **Data:** seeded Park–Miller LCG, seed 42. Thirty draws for the dramatic fault, kept if `rng() < 0.40`; then thirty more for the dull fault, kept if `rng() < 0.08`. Yields 13 kept and 4 kept, both read back by a scan.
- **Title (bold 15px `P.ink`, centered, y=22):** "Two Faults, Thirty Times Each"
- **Strip A** (`y=58`, height 26, 30 cells from x=54 to `w−34`): header bold 12px `P.aqua` above-left "CHECKOUT CRASHED MID-PAYMENT". Remembered cells `rgba(25,158,112,0.55)` stroked `P.aqua`; forgotten cells `rgba(107,114,128,0.12)` stroked `#dcdfe4`.
- **Strip B** (`y=136`, same geometry): header bold 12px `P.yellow` "REPORT CAME OUT WITH COLUMNS SWAPPED". Remembered cells `rgba(201,133,0,0.55)` stroked `P.yellow`; forgotten cells as above.
- **Per-strip counts:** under each strip, 12px `P.mute` "30 events, one square each" at the left; bold 12px in the strip's hue right-aligned at `w−34`, "13 still remembered" / "4 still remembered", printed from the scans.
- **Comparison bars:** header bold 13px `P.ink` at y=208 "SHARE OF EACH FAULT'S EVENTS STILL REMEMBERED". Two rows at y=222 and y=248, height 15, track `rgba(107,114,128,0.12)` from x=210 to `w−96`; bar length ∝ recalled ÷ 30. Row labels 12px `P.mute` right-aligned at x=202 — "recalled: crash", "recalled: report". Bold 12px "13 of 30" / "4 of 30" in the strip hue after each bar end.
- **Multiple callout** (y=288): bold 19px `P.aqua` printing `(13/4).toFixed(1) + '×'` at x=54, with 12px `P.mute` "as many crashes recalled, from the same number of events" beside it.
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "Same thirty events. The one that stung left three times the trace."

---

## 3. One Failure Told Twelve Times

**Tag colors:** `repetition` orange, `retelling` blue, `one event, many mentions` magenta
**Hue family:** orange against yellow

### canvas `c3` — 720×320

A ninety-day timeline with two tracks: one incident carrying twelve mention ticks, and nine incidents carrying one each.

- **Data (literal arrays over days 1–90):**
  Track A — one incident on day 12; mentions on days `12, 13, 14, 15, 16, 18, 21, 24, 27, 33, 40, 47` (12 mentions, spanning 35 days ≈ five weeks).
  Track B — nine incidents on days `5, 19, 26, 38, 44, 57, 63, 71, 84`, each with a single mention on the same day (9 mentions).
- **Computed in the draw function:** `A.events = 1`, `A.mentions = 12`, `B.events = 9`, `B.mentions = 9`, and mentions-per-event 12 versus 1, all from the array lengths.
- **Title (bold 15px `P.ink`, centered, y=22):** "Ninety Days, Two Tracks"
- **Track A** (baseline `y=110`, spanning x=54 to `w−160`): a faint `#d8dce2` day line; the incident as a 9px `P.orange` diamond on it; each mention as a 2.5px `P.orange` tick rising 22px, over a `rgba(217,89,38,0.10)` band covering the mention window. Header bold 12px `P.orange` above-left: "ONE FAILURE, BROUGHT UP AGAIN AND AGAIN".
- **Track B** (baseline `y=212`): nine 9px `P.yellow` diamonds, each with one 2.5px `P.yellow` tick rising 22px. Header bold 12px `P.yellow`: "NINE SEPARATE FAILURES, MENTIONED ONCE EACH".
- **Legend (12px `P.mute`, y=134):** "a diamond is a failure — a tick above it is one mention of it".
- **Day axis:** a `#ccc` line at `y=252`, ticks and 12px `P.mute` labels at days 1, 30, 60, 90, and the 12px `P.mute` title "day" centered at y=290.
- **Right panel** (x = `w−142`): for each track, bold 19px figure in the track hue giving the mention count, 12px `P.mute` "mentions" beside it, then 12px `P.mute` "from 1 failure" / "from 9 failures" beneath. Both counts printed from the arrays.
- **Contrast strip** (y=252 and y=272): bold 12px `P.orange` "12 mentions each" over bold 12px `P.yellow` "1 mention each", both computed as mentions ÷ events.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "The track you remember had one failure on it. The other had nine."

---

## 4. Twenty-Five Letters Against Two Hundred and Sixty People

**Tag colors:** `who leaves a trace` magenta, `the silent group` blue, `what to fix first` green
**Hue family:** magenta on grey with a green verdict

### canvas `c4` — 720×300

Two blocks of squares, one square per person affected, with the handful who wrote in filled dark — the big block has almost no dark squares, the small block is peppered with them.

- **Data:** fault A — 260 affected, 6 letters. Fault B — 100 affected, 25 letters. Which squares are letter-writers is chosen by a seeded shuffle (LCG, seed 42) so the dark squares scatter through the block instead of clumping at the front.
- **Computed in the draw function:** affected and letter counts by scanning the drawn cell arrays; people-per-letter as `affected / letters` → 43 and 4; and the share of affected people who wrote nothing → 98%.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Square Per Person Affected"
- **Block A** (x=54, y=58): 260 cells, 26 per row × 10 rows, cell 10px on an 11px pitch → 286×110. Silent cells `rgba(107,114,128,0.14)` stroked `#dcdfe4`; letter-writers `rgba(213,81,129,0.75)` stroked `P.magenta`.
- **Block B** (x=372, y=58): 100 cells, 10 per row × 10 rows, same cell size → 110×110, same two fills.
- **Block labels:** bold 12px `P.ink` above each block — "FAULT A" / "FAULT B"; beneath each, 12px `P.mute` "260 people affected" / "100 people affected" and bold 12px `P.magenta` "6 wrote in" / "25 wrote in", all printed from the scans.
- **Right panel** (x=508): bold 13px `P.ink` "ONE LETTER ARRIVED / FOR EVERY —", then bold 19px `P.magenta` "43" with 12px `P.mute` "people hit / by fault A", and bold 19px `P.magenta` "4" with 12px `P.mute` "people hit / by fault B".
- **Silent-share note** (12px `P.mute`, y=226): "98% of the people hit by fault A never wrote a word", the percentage computed.
- **Verdict** (bold 12px `P.green`, y=248): "fault A is the larger problem, and the quieter one" — the comparison stated from the counts.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The bigger fault is the quieter one, because almost nobody it hit said so."

---

## 5. When Easy to Recall Is a Fair Guide

**Tag colors:** `the boundary` green, `usually right` aqua, `where it breaks` orange
**Hue family:** green line with orange outliers

### canvas `c5` — 720×330

How often something happens against how many people name it, with the ordinary events sitting on a line and the memorable ones floating far above it.

- **Data (literal arrays, `[label, times per year, named out of 50]`):**
  Ordinary — `a five-minute queue, 90, 32` · `rain on the walk home, 50, 29` · `a password reset, 25, 25` · `a late parcel, 12, 19` · `mislaid keys, 6, 14`
  Memorable — `losing your wallet, 1, 44` · `delayed overnight, 2, 40` · `airline loses your bag, 5, 36` · `motorway breakdown, 3, 31`
- **Computed in the draw function:** a least-squares fit of *named* against `log10(times per year)` **over the five ordinary points only** — slope 15.5, and 2.35 named at one time a year; the largest absolute miss among those five, 0.9 people; and, for the wallet, the yearly count the line implies for 44 names, `10^((44 − b)/m)` = 482 times a year against an actual once.
- **Title (bold 15px `P.ink`, centered, y=22):** "How Often It Happens vs How Many Name It"
- **Axes:** left 64, right 40, top 46, bottom 62. x is `log10` from 0.8 to 130 with 12px `P.mute` ticks at 1, 2, 5, 10, 20, 50, 100; y is 0 to 50 with ticks every 10. Horizontal gridlines `P.grid`. Axis titles 12px `P.mute`: x "times a year", y (rotated) "how many of fifty name it".
- **Fitted line:** solid 2px `P.green` across the ordinary range (6 → 90 times a year), dashed 2px `P.green` (dash 5/4) on both extrapolated stretches, so the reader can see where the line puts a once-a-year event.
- **Ordinary points:** 5px circles `rgba(0,131,0,0.65)` stroked `P.green`, labels 12px `P.green` right-aligned to the left of the point.
- **Memorable points:** 6px diamonds `rgba(217,89,38,0.75)` stroked `P.orange`, labels 12px `P.orange` above-right. From each, a 1px dashed `P.orange` vertical leader drops to the line, so the gap between what happened and what recall implies is drawn to scale.
- **Wallet callout** (three lines from x=430, y=64/82/98): bold 12px `P.orange` "the wallet went missing once all year", then 12px `P.mute` "yet 44 of fifty named it — as many as" / "name something happening 482 times a year". The count and the implied count are both printed from the fit. Below at y=118, 12px `P.mute` "each dashed drop is that same gap".
- **Line-fit note:** 12px `P.green` right-aligned inside the plot at its bottom right — "the five ordinary events sit within 0.9 person of this line", the miss computed.
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Recall tracks frequency until something is memorable for its own reasons."

---

## Page-specific constraints

- **Canvas placement:** `td.viz-col` gets `text-align: center`, the canvas `display: block; width: 100%; margin: 0 auto`, capped at 720px by `style.maxWidth` so a wide cell leaves slack around it.
- **Every section carries a `.src`** — every section here is constructed. No paragraph blocks, no data tables, no `.example` lines.
- **Bullet count follows the content** — eight where the mechanism needs eight, nine in section 5 because the boundary has two halves.
- **Canvas heights, per chart:** `c1` 340, `c2` 320, `c3` 320, `c4` 300, `c5` 330.
- **Hue family per section, and it must vary:** 1 violet with magenta crossings, 2 aqua against yellow, 3 orange against yellow, 4 magenta on grey with a green verdict, 5 green line with orange outliers. **Blue appears only in tag pills.**
- **Every printed figure is computed in the draw function from the plotted data** — the disagreeing-pair count, the 70,000× rate ratio, 13 and 4 recalled, the 3.2× multiple, 12 mentions from 1 failure, 43 and 4 people per letter, the 98% silent share, the 0.9-person miss and the 482-a-year implied wallet count.
- **Scope kept distinct from its siblings:** this page is about *ease of recall driven by vividness and repetition*. It does not cover "you cannot unsee a failure so you fix it regardless of size" or "the latest event overwrites the trend", and it carries no links to any other page.
- **Corrections applied to the earlier version of this page:**
  - The old page opened on a three-stage funnel of 100,000 → 50 → 5 with no stated basis for any of the three magnitudes — asserted numbers dressed as a measurement. The lead chart is now two orderings of one constructed table, and both orderings are sorted in code.
  - Its "1 − (1 − p)ⁿ" section computed the chance that someone you know is affected. That is a network-size argument, not an ease-of-recall argument, and it required probability notation on a layman page. Dropped.
  - Its regeneration notes claimed a "5,619,256-year figure on `c4`" while the section's own text said 305,590 years — the two contradicted each other, and neither figure had a role in the availability mechanism. The whole lottery section is dropped.
  - The old `c3` drew `y = exp(−4.2x)` and labelled the y-axis "how often it happens", which makes a shape claim about the world that nothing supports. Replaced by counted markers on a timeline.
  - The eight-row "Where It Strikes" table and the "Analyst's Defense" checklist were role-labelled filler sections with no computed content. Their one live idea — that letters measure willingness to complain, not headcount — is now section 4, with the arithmetic shown.
  - Nothing in the old page said when ease of recall is *right*. Section 5 adds that boundary and marks it with a fitted line, so "usually a fair guide" is a measured statement rather than a concession.
- **Pair count verified:** **23 of the 28** pairs disagree — computed from the literal `HAZ` table, 5 pairs agreeing (dog bite, choking, escalator and sting rank consistently). An earlier regeneration note said 22; that figure was stale and is gone.
