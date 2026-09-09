# The Fault You Watched Fail Jumps the Queue: Nothing Else on the List Was Ever Counted

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-familiarity-feels-like-quality.html`)
**HTML title tag:** The Fault You Watched Fail Jumps the Queue — Cognitive Biases

**Subtitle:** A fault somebody watched break gets fixed first, and because nothing else on the list was ever counted, queue position ends up set by who happened to be in the room.

---

## Section 1 — Witnessed Faults Eat the Week, and They Are Not the Big Ones

**Tags:** `core idea` (violet), `capacity follows sight` (blue), `same days either way` (magenta)

**Bullets:**
- **The ones somebody watched break** — a checkout warning, a dead menu link, a receipt dated yesterday
- **The ones nobody happened to see** — a half-saved form, a search finding nothing, a mislabelled export
- **The week's fixing capacity** — exactly what the witnessed ones cost, so they fill it on their own
- **Spend it on what was seen** — a few hundred people a month stop being hit, a small slice of the harm
- **Spend the same days by reach** — most of the harm goes instead, and not one extra day is needed
- **What being watched tells you** — that the fault is real, and nothing at all about how far it reaches
- **What it does to you anyway** — it moves that fault to the front of a queue never sorted by reach
- **The worst fault on the list** — the half-saved form, larger than the rest together, and unwatched

**Key point:** Nobody chose the smaller outcome. The faults that happened to break in front of somebody used up the whole week on their own, and the days spent were identical either way.

**Source note (`.src`):** Illustrative Example — six constructed faults with made-up monthly reach and fix costs; the totals, both shares, the ratio and the best affordable set are all computed in the draw function.

### Visualization — canvas `c1`, 720×340

Six reach bars with the days-spent column beside them, so the biggest bar visibly has no days against it, then the harm each ordering actually removes for the same days.

- **Data (literal array, in the draw order shown — six rows need no generator):**

  | fault label | people hit each month | days to fix | watched |
  |---|---|---|---|
  | form comes back half-saved | 1800 | 2 | no |
  | search returns nothing | 1200 | 1 | no |
  | export column mislabelled | 400 | 1 | no |
  | receipt shows wrong date | 250 | 1 | yes |
  | broken link on a menu | 200 | 1 | yes |
  | checkout warning shown | 150 | 2 | yes |

- **Computed in the draw function:** `TOT = 4000` (sum of all six); `BUD = 4` days (sum of the watched rows' fix costs, so the witnessed set is exactly affordable and exactly exhausts the week); `seenHarm = 600` = 15% of `TOT`; `bestHarm` found by exhaustive search over all 64 subsets costing at most `BUD` days, which returns the three unwatched faults, `1200 + 1800 + 400 = 3400` at `2 + 1 + 1 = 4` days = 85% of `TOT`; ratio `bestHarm / seenHarm = 5.7`. Largest single fault share `1800 / 4000 = 45%`, which exceeds the other five combined against the witnessed set. **Both orderings consume four days — that equality is the argument, and it is enforced by deriving `BUD` from the watched rows and capping the search at it, not asserted.**
- **Title (bold 15px `P.ink`, centered, y=22):** "Six Faults, N Days of Fixing" — N interpolated from the derived `BUD`, so the title cannot disagree with the squares drawn below it.
- **Column headers (bold 12px `P.ink`, y=44):** "PEOPLE HIT EACH MONTH" at `BX = 192`; "DAYS SPENT" at `EX = 470`.
- **Rows:** six rows, `y0 = 56`, pitch 25, bar height 16. Labels right-aligned 12px `P.mute` at `LX = 186`. Bars from `BX`, width `210 × p / 1800`. Watched rows fill `rgba(74,58,167,0.50)` stroked 1.5px `P.violet`; unwatched fill `rgba(107,114,128,0.26)` stroked 1px `P.mute`. Count printed bold 12px in the row's hue just past the bar end.
- **Effort column** at `EX`: one 9px square per day (12px pitch) filled `rgba(74,58,167,0.55)` stroked `P.violet`, drawn only for watched rows; unwatched rows get a 12px `P.mute` en dash. Then bold 12px `P.violet` "watched fail" or 12px `P.mute` "nobody saw it" at `NX = 520`.
- **Divider:** 1px `P.grid` line at y=214.5, from x=40 to `w−30`.
- **Lower panel:** header bold 13px `P.ink` at `BX`, y=232, "PEOPLE SPARED BY THE SAME N DAYS" with N from `BUD`. Two bars from `BX`, track width 260 scaled to `TOT` on a `rgba(107,114,128,0.12)` track, height 18, at y=244 and y=270. Row 1 "fix what you saw" fills `rgba(213,81,129,0.50)` stroked `P.magenta`; row 2 "fix by reach instead" fills `rgba(42,120,214,0.45)` stroked `P.blue`. Labels right-aligned 12px `P.mute` at `LX`; past the track each row prints, bold 12px in-hue, its count, its share of `TOT`, and **the days that ordering actually costs in parentheses** — the two must read the same figure, which is how the reader checks the capacity claim rather than taking it on trust.
- **Callout:** bold 19px `P.blue` `ratio.toFixed(1) + '×'` at `x=560, y=296`, with 12px `P.mute` "as many people" beside it. A 12px `P.mute` note "all six faults were live the whole month" at `x=40, y=296`.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Same N days of work. One ordering reaches 15% of the harm, the other 85%." — the day count and both percentages interpolated from the computed values.

---

## Section 2 — Ten Defensible Weeks in a Row

**Tags:** `it compounds` (orange), `ten weeks` (magenta), `queue-jumping` (red)

**Bullets:**
- **The rule in force** — one fix a week, given to whichever fault somebody just watched break
- **Ten quiet weeks** — a different small fault surfaces in view each week and takes the slot
- **The one at the back** — a counted fault, worse than any of them, that nobody ever watched break
- **Every week it loses** — the fresh sighting jumps the queue, because you cannot unsee a fault
- **No single week is wrong** — each choice is defensible on the morning it is made, and made alone
- **What the ten weeks cost** — nearly three times the harm the same ten fixes could have removed
- **Why nothing corrects it** — nothing got unfixed; the big one simply never reached the front
- **Where the cost really lives** — in the weeks the worst fault stayed live, not in any one decision

**Key point:** No single week's decision was wrong on its own. The large fault lost every week to a fresher one, and ten defensible choices in a row cost close to three times the harm the very same ten fixes could have removed.

**Source note (`.src`):** Illustrative Example — ten seeded weekly fault sizes plus one constructed large fault; both cumulative curves, their difference and the ratio are computed in the draw function.

### Visualization — canvas `c2`, 720×320

Two cumulative harm curves over ten weeks, the shaded band between them being the whole cost of ordering by sight.

- **Data:** seeded Park–Miller LCG, seed 42. `small[k] = 10 + round(rng() × 50)` for k = 0…9, giving `[10,36,47,23,29,20,59,36,37,23]`, sum 320. One constructed large fault, `BIG = 420` people a week. All eleven faults are live from week 1.
- **Harm model, computed in the draw function:** each fault contributes `size × (weeks it stayed live)`; a fault fixed at the end of week k was live for weeks 1…k, and anything never fixed is live for all ten. One shared `curve(fixed, never)` routine produces both series, so neither can drift from the other.
  - Visibility order: week k fixes `small[k−1]`, the one just watched; `BIG` is never picked. Cumulative `[740, 1470, 2164, 2811, 3435, 4030, 4605, 5121, 5601, 6044]`.
  - Reach order: week 1 fixes `BIG`, weeks 2–10 fix the nine largest smalls descending, leaving the smallest (10) unfixed. Cumulative `[740, 1060, 1321, 1535, 1712, 1853, 1958, 2034, 2087, 2117]`.
  - Difference at week ten = `6044 − 2117 = 3927`; ratio `6044 / 2117 = 2.85`, which is what "nearly three times" in the bullets refers to. **Both schedules perform exactly ten fixes**, so the capacity is equal by construction and only the order differs. Week 1 is identical under both (740), so the curves separate only from week 2 — a non-degenerate setup rather than a gap opened at the origin.
- **Title (bold 15px `P.ink`, centered, y=22):** "Ten Weeks, One Fix a Week"
- **Plot:** `PX = 62`, right panel width `PR = 180`, `PW = w − PR − PX`, `PTOP = 48`, `PBOT = 228`, y range 0 to 6500. `P.grid` gridlines with 12px `P.mute` labels at 0 / 2,000 / 4,000 / 6,000. X = weeks 1…10, 12px `P.mute` tick labels at `PBOT+16`, axis label "week" at `PBOT+34`.
- **Curves:** visibility order 2.5px `P.orange` with 3.5px dots; reach order 2.5px `P.green` with 3.5px dots. The band between them filled `rgba(217,89,38,0.10)`.
- **Annotations:** bold 12px `P.green` "the 420-a-week fault fixed in week 1" placed inside the band near week 4 so it sits on neither curve; bold 12px `P.orange` "still unfixed in week 10" above the last orange point. Endpoint totals printed bold 12px in-hue beside each final dot.
- **Right panel** at `x = w − PR + 14`: bold 13px `P.ink` "EXTRA ENCOUNTERS", then bold 19px `P.orange` with the computed difference; below, 12px `P.mute` "visibility order — …", "reach order — …" from the two endpoints, and "same 10 fixes either way" from `W`, which is the equal-capacity check stated on the chart; then bold 12px `P.magenta` "the worst fault waited" / "all ten weeks"; then 12px `P.mute` "one person hit once" / "is one encounter" as the unit note. Every figure read out of the model above.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "Ten reasonable weekly choices, N encounters that did not have to happen." — N interpolated from the computed difference.

---

## Section 3 — When Acting on What You Saw Is Right, and What Actually Fixes This

**Tags:** `the boundary` (green), `confirmed beats suspected` (aqua), `count the rest` (orange)

**Bullets:**
- **The honest case for acting** — a fault you watched break is confirmed and needs no hunch to justify
- **The one you only inferred** — bigger if it is there, but nobody counted it and it may not be there
- **The sure thing** — a modest number of people, certain, and the half-day you have covers it outright
- **The maybe** — many times that many if the hunch holds, and nobody at all if it does not
- **Where the two break even** — about a one-in-five chance of being right makes them worth the same
- **Below that line** — chasing the hunch helps fewer people than fixing the thing you actually saw
- **The defect stated plainly** — not that you fixed what you saw, but that nothing else was counted
- **What an hour of counting buys** — it tells you which side of that line you were standing on

**Key point:** Acting on what you witnessed is not the mistake — a confirmed fault carries no risk of being imaginary, and below roughly one in five that certainty helps more people than a bigger maybe. The mistake is that nothing else on the list was ever measured, so you never learn which side of the line you are on. An hour of counting converts the whole problem.

**Source note (`.src`):** Illustrative Example — two constructed faults, one counted and one only suspected; the break-even chance is computed in the draw function as the confirmed reach divided by the suspected reach.

### Visualization — canvas `c3`, 720×330

People helped by the same half-day, plotted against how likely the suspected fault is to be real, with the break-even marked where the two lines meet.

- **Data (constructed):** `CONFIRMED = 240` people a month, certain. `SUSPECTED = 1200` people a month if the hunch is right, and none if it is not.
- **Computed in the draw function:** the confirmed line is flat at `CONFIRMED`; the suspected line is `SUSPECTED × p` for p from 0 to 1; they cross at `pStar = CONFIRMED / SUSPECTED = 0.20`, printed as "1 in " + `round(1 / pStar)`. Neither endpoint is degenerate — the crossing sits well inside the plotted range rather than at 0 or 1, so the chart has two genuine regions.
- **Title (bold 15px `P.ink`, centered, y=22):** "People Helped by the Same Half-Day"
- **Plot:** `PX = 66`, right panel width `PR = 178`, `PW = w − PR − PX`, `PTOP = 52`, `PBOT = 236`, y range 0 to 1300 with `P.grid` gridlines and 12px `P.mute` labels at 0 / 400 / 800 / 1,200. X range 0 to 100% with 12px `P.mute` labels every 20% and axis label "chance the suspected fault is real" at `PBOT+36`.
- **Shading:** the region left of `pStar` filled `rgba(0,131,0,0.07)` with a 12px `P.green` label "what you saw" / "wins here" low in the plot; the region right of it `rgba(25,158,112,0.07)` with a 12px `P.aqua` label "the hunch wins here" on the same baseline. Both boundaries drawn at the computed `pStar`.
- **Lines:** confirmed 2.5px `P.green`, flat, labelled bold 12px `P.green` "the fault you watched fail — 240, certain" just below the line right of the crossing. Suspected 2.5px `P.aqua`, rising from the origin, labelled bold 12px `P.aqua` "the fault you suspect — 1,200 if real" above its right end. Both figures printed from the constants.
- **Crossing:** 1.5px dashed `P.mute` vertical at `pStar`, a 6px `P.ink` dot at the intersection, and bold 19px `P.ink` "1 in 5" to the right of the dashed line with 12px `P.mute` "the two are worth the same here" beneath.
- **Right panel** at `x = w − PR + 14`: bold 13px `P.ink` "WHAT AN HOUR OF" / "COUNTING BUYS", then 12px `P.mute` lines "it replaces the chance", "with a count, and the", "line stops mattering"; then bold 12px `P.orange` "unmeasured is not" / "the same as small".
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Fixing what you saw is right until the hunch clears 1 in 5." — the figure interpolated from the computed `pStar`.

---

## Regeneration instructions

- **Numbers live in the charts, not the prose — at most a couple of figures in bullets, and only where
  the figure is the argument.** This page carries no bare digits in any bullet or key point at all.
  Quantities appear as "a few hundred people", "a small slice of the harm", "most of the harm",
  "nearly three times", "about a one-in-five chance". The charts print the exact tallies, computed at
  render time, for a reader who wants them. Never open a bullet with a count, a size or a percentage —
  use a descriptive opener. No decimal percentages and no precise averages in prose.
  **Do not import a "digits, not words" rule from any sibling page's notes: it would regress this page.**
- **The concept is never named with an established term that means something else.** The two-word name
  this page used to carry — the noun "exposure" paired with "bias" — is already taken twice over: in ML
  it denotes the train/inference mismatch from teacher forcing, and in epidemiology it denotes
  misclassification of who was exposed. This page is about neither, so that pairing must not appear in
  the title, any heading, any bullet or any chart label, and must not be reintroduced on regeneration.
  Name the thing behaviourally instead: the fault somebody watched break jumps the queue.
- **Boundary with the availability page in this folder.** That page is about recall and estimation —
  what people *name* when asked what is risky. This page is about action ordering: what gets *fixed*
  first and which fixing capacity is spent. Keep every framing here on queue position and spent days.
  No link between the two pages — the repo forbids cross-page links; the angle alone keeps them apart.
- **Template:** the card-section layout copied verbatim from `05-cognitive-biases/25-familiarity-feels-like-quality.html`
  — the whole `<style>` block, the `setup()` canvas helper, the `lcg()` seeded PRNG, the `P` palette
  object, the `__charts` array and the debounced resize-redraw tail. Three `.card-section` blocks, each
  holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a
  `table.layout` with one row: `td.text-col` 50% / `td.viz-col` 50%.
- **The 50/50 split is fixed.** Shrink a chart via canvas `max-width` / height, never by narrowing the
  viz column below half.
- **No index number** in the `<h1>`. `<h1>` is "The Fault You Watched Fail Jumps the Queue: Nothing Else
  on the List Was Ever Counted".
- **Text column order:** `.tags` pill row of three → `<ul>` of one-line bullets each opening
  `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note. No paragraph blocks, no data
  tables, no `.example` lines restating a bullet.
- **Bullet form:** each is ONE line aiming not to wrap at 50% column width, roughly 90–105 characters
  including the bold label. Never delete a fact to hit the length — split it into another bullet.
  Eight bullets per section here, following the content rather than a quota.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with
  `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom
  margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells
  vertical-align top padding 12px; `.viz-col` centered. canvas `display:block; width:100%;
  margin:0 auto; border:1px solid #e0e0e0; border-radius:4px`. `ul` 0.92rem margin `8px 0 8px 20px`,
  `li` 4px bottom margin, `li b` `#1a5276`. `.key-point` `#f8f9fa` background,
  `border-left:3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used:
  `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta`
  `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow`
  `rgba(201,133,0,0.15)`/`#a06c00`.
- **No nav, no `.nav` CSS, no back/home links, no cross-page links of any kind.**
- **Hue family per section:** 1 violet/blue with a magenta losing bar; 2 orange against green with a
  magenta note; 3 green/aqua with an orange warning. No two charts share a palette.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`,
  `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`,
  `grid #e5e9ef`.
- **Canvas:** intrinsic `width="720"` plus the per-chart height (340, 320, 330). `setup(id)` caches the
  logical size in `dataset` on the first call (because `canvas.width` overwrites the attribute), sets
  `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to
  `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in
  `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12–13px; labels 12px floor; big callout
  figures bold 19px; caption bold 13px.
- **Determinism:** no `Math.random()`. Where the counts themselves carry the lesson — the six-fault list
  in section 1, the two constructed faults in section 3 — a literal array beats a seeded draw. The ten
  weekly fault sizes in section 2 come from a seeded Park–Miller LCG
  (`s = (s × 16807) % 2147483647`), seed 42, one fresh stream for that chart. Every printed figure —
  totals, shares, the ratio, both cumulative curves, the difference and the break-even chance — is
  derived inside the draw function from the plotted data and printed from that variable.
- **Arithmetic that must close, and is enforced rather than asserted:** in section 1 the week's capacity
  is *derived* as the sum of the watched rows' fix costs and the alternative allocation is found by
  exhaustive search capped at that same figure, so the two orderings provably cost the same four days;
  the two spared-people bars sum from the per-fault reaches in the plotted table, and the two shares add
  to the whole only because the affordable set happens to be the complement of the watched set. In
  section 2 both schedules perform exactly ten fixes through one shared `curve()` routine, so the gap
  between the curves is purely an ordering effect and cannot be an artifact of unequal capacity.
- **The lead chart must show the mismatch, not describe it:** reach bars and the days-spent column sit
  side by side so the largest bar visibly has no days against it, and the two spared-people bars put
  the cost of the ordering on screen as a computed number.
- **History of this page.** It replaces an earlier page that named the concept with a term already taken
  by two other fields, and that argued one claim across three of its four sections with three different
  toy datasets — witnessed faults eat the budget, witnessing carries no information about size, the big
  one never reaches the front. Those are one point, so the size-independence material is folded into
  section 1 as two bullets rather than holding its own section and its own scatter chart. The sharpest
  idea on the old page was buried as the final bullet of its final section — that the defect is not
  fixing what you saw but that nothing else was ever priced — and it is now the spine of the page,
  named in the title, the subtitle and section 3's key point. The old page also recited thirteen
  figures across its bullets, including two averages and several exact counts; all of them now live in
  the charts.
