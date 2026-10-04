# Non-Response Bias — Viz

**Page type:** detail page, card-section (template `06-sectioned-cards-callout`)
**HTML title tag:** Non-Response Bias — Cognitive Biases
**Template:** copied verbatim from `05-cognitive-biases/25-mere-exposure-effect.html` — the entire `<style>` block, the `setup(id)` canvas helper, the `lcg(seed)` PRNG, the `P` palette object, the `__charts` array with its debounced resize tail, the `table.layout` / `td.text-col` / `td.viz-col` 50/50 structure, the `.tags` pills, `.key-point` and `.src` conventions. Only the content differs.
**Source note wording:** the sibling `.txt.md` says figures are "counted / computed at render time"; the html `.src` notes say "counted inside the draw function" (section 1) and "computed in the draw function" (sections 2 and 3).

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** The split is: the sibling `.txt.md`
carries the reader-facing construction — the two private numbers in words, the push arithmetic, the
section-by-section table of who is surveyed on which form, and a derivation table for every quoted
figure — while this file carries its implementation (the constant names, the `panel()` / `form()`
helpers, the seed, the drawing). One construction runs the whole page, so changing any constant
(`ROUGH`, `FINE`, `SPREAD`, `COST`, `RESID`, `THRESH`) or the seed moves every figure at once —
re-read the computed values and update both siblings to match. Section 2's "33.8 points against 1.8"
and section 3's "nearly eight times" are the two figures the prose depends on.

**Determinism:** no `Math.random()` anywhere. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, one fresh stream per chart. Every rate, count, gap, ratio and difference printed on a canvas is computed inside that draw function from the plotted values and printed from that variable.

---

## 1. The Survey Everyone Answered Honestly

**Tag colors:** `core idea` violet, `nobody lied` blue, `two rooms` magenta
**Hue family:** violet against blue with a magenta pooled pair

### canvas `c1` — 720×350

Three pairs of bars — the two groups and then everyone pooled — each pair showing how many quietly held a low opinion against how many put a complaint on the form.

- **Shared construction — implementation of the reader-facing one in the sibling `.txt.md`:** that
  sibling's unnumbered preamble carries the premise, the push arithmetic in words and the derivation
  table for every figure on the page; this block is only the code that produces them. In code the
  module-level constants are `ROUGH`, `FINE`, `SPREAD`, `COST`, `RESID` and `THRESH`; `bell(rng, sd)`
  returns the noise generator `(rng()+rng()+rng()−1.5) × 2 × sd`; `panel(rng, nz, n, base)` builds `n`
  records of `{ felt: base + nz(), pv: 0.7 + 0.6·rng() }`; `form(p, dep, isAnon)` returns
  `round(clamp10(felt + COST × dep × pv × (isAnon ? RESID : 1)))`; `feltScores(p)` returns
  `round(clamp10(felt))`; `cnt()` and `pct()` count and rate everything below `THRESH`. Every chart
  calls `panel()` and `form()`, differing only in `dep` and `isAnon`. Seeded Park–Miller LCG, seed 42,
  one fresh stream per chart. Changing any of those six constants or the seed invalidates the
  sibling's preamble tables as well as its bullets.
- **Data:** two panels of 150 at `ROUGH`. Group one at `dep = 0.95`, group two at `dep = 0.10`,
  both on a named form. Quietly unhappy (private opinion under five): **74 of 150** and **72 of 150**,
  so **146 of 300 — 48.7%**. Complaints reaching the form: **4 of 150 (2.7%)** and **64 of 150
  (42.7%)**, so **68 of 300 — 22.7%**. `4 + 64 = 68` and `74 + 72 = 146`, both checked in the draw.
- **The 70:** group one's quietly unhappy who nonetheless wrote five or more — `74 − 4 = 70`,
  counted respondent by respondent rather than subtracted.
- **Title (bold 15px `P.ink`, centered, y=21):** "Who Felt It Against Who Said It"
- **Legend (bold 12px, y=42):** `P.mute` "quietly held a low opinion" at x=58, `P.violet`
  "put a complaint on the form" at x=282.
- **Bars:** plot box `PX=62`, width `w − 102`, `TOPY=70`, `BASEY=252`, percent axis 0–60 with
  `P.grid` gridlines at 0/15/30/45/60 and 12px `P.mute` labels. Three groups; within each, a pale
  hollow bar (`rgba(107,114,128,0.14)` stroked `P.mute`, dashed) for the felt rate on the left and a
  solid bar on the right for the reported rate — `rgba(74,58,167,0.50)` stroked `P.violet` for group
  one, `rgba(42,120,214,0.45)` stroked `P.blue` for group two, and `rgba(213,81,129,0.50)` stroked
  `P.magenta` for the pooled pair. Each bar carries its rate in bold 15px above and its raw
  "n of N" in 12px `P.mute` below the axis.
- **Group labels (bold 12px `P.ink`, two lines each):** "no alternative / 150 people",
  "three alternatives / 150 people", "the reported average / all 300".
- **The gap callout:** inside group one, a 2px `P.violet` vertical arrow from the top of the felt bar
  down to the top of the reported bar, labelled bold 12px `P.violet` with the computed difference
  ("46.7 points of it never reached the form") and 12px `P.mute` "70 of the 74 wrote a passing score".
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "They saw it. They were not going to say it to your face."

---

## 2. Take the Name Off the Form and Watch What Arrives

**Tag colors:** `the test` aqua, `two worlds` magenta, `falsifiable` green
**Hue family:** aqua against magenta over muted named bars

### canvas `c2` — 720×380

Four bars in two labelled worlds — the named form and the unnamed form for each — with the gap between the two forms drawn and measured inside each world.

- **Data:** two panels of 400 sharing one seeded stream. The silenced world is at `ROUGH` with
  `dep = 0.95`; the nothing-wrong world is at `FINE` with `dep = 0.12`. Each panel is scored twice,
  once with `anonymous = false` and once `true`, with the same private opinions and the same personal
  prices both times.
- **Figures, all counted in the draw:** silenced world **21 of 400 (5.3%)** named against **156 of 400
  (39.0%)** unnamed, a gap of **33.8 points**; nothing-wrong world **22 of 400 (5.5%)** named against
  **29 of 400 (7.3%)** unnamed, a gap of **1.8 points**. The two named forms sit **0.3 points** apart,
  the two unnamed forms **31.8 points** apart.
- **Who moved:** respondents who scored five or more on the named form and under five on the unnamed
  one — **135** in the silenced world, **7** in the nothing-wrong world. Nobody moves the other way,
  so `21 + 135 = 156` and `22 + 7 = 29`; both identities are asserted in the draw and the printed
  arrow label comes from the counted value.
- **Title (bold 15px `P.ink`, centered, y=21):** "The Same People, Named Form and Unnamed Form"
- **Legend (bold 12px, y=42):** `P.mute` "name attached to the answer" at x=58, `P.aqua`
  "no name attached" at x=282.
- **Bars:** plot box `PX=62`, width `w − 210`, `TOPY=74`, `BASEY=278`, percent axis 0–45 with
  `P.grid` gridlines at 0/15/30/45. Two world groups; within each, the named bar in
  `rgba(107,114,128,0.16)` stroked `P.mute` on the left and the unnamed bar on the right —
  `rgba(25,158,112,0.45)` stroked `P.aqua` in the silenced world, `rgba(213,81,129,0.40)` stroked
  `P.magenta` in the nothing-wrong world. Each bar carries its rate in bold 17px above and
  "n of 400" in 12px `P.mute` beneath the axis.
- **World labels (bold 12px `P.ink`, two lines):** "no alternative, rough service / dep 0.95",
  "free to leave, decent service / dep 0.12".
- **Gap arrows:** within each world, a 2px vertical arrow from the top of the named bar to the top of
  the unnamed bar in the world's colour, labelled bold 13px with the computed point gap, and 12px
  `P.mute` beneath with the number who newly spoke up ("135 people who said nothing before").
- **Side panel** at `PX + PW + 26`: bold 13px `P.ink` "WHAT EACH FORM / CAN TELL APART", then bold
  17px `P.mute` "0.3 points" over 12px `P.mute` "between the two named forms", then bold 17px `P.aqua`
  "31.8 points" over "between the two unnamed forms", then bold 12px `P.aqua` "the unnamed form /
  separates the two worlds; / the named form does not."
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "The complaints did not appear. The reason to withhold them disappeared."

---

## 3. The Number Moves When the Exit Door Opens

**Tag colors:** `not personality` orange, `the exit door` yellow, `read it backwards` blue
**Hue family:** orange with a yellow band

### canvas `c3` — 720×350

A rising line of reported complaint rate across six steps of easier exit, drawn against a flat dashed line for the private opinion that never moved.

- **Data:** one panel of 400 at `ROUGH`, held fixed, scored on a named form at
  `dep = 0.95, 0.85, 0.68, 0.48, 0.30, 0.14`. Reported complaints **21, 26, 48, 79, 117, 162** —
  **5.3%, 6.5%, 12.0%, 19.8%, 29.3%, 40.5%**. The private rate is **191 of 400 — 47.8%** at every step,
  because the same private opinions are reused.
- **Read off the plotted points:** the rise end to end is **35.3 points**, the ratio **7.7×**, and the
  distance still left at step six is **47.8 − 40.5 = 7.3 points**. All three printed from variables.
- **Title (bold 15px `P.ink`, centered, y=21):** "Reported Complaints as Leaving Gets Easier"
- **Axes:** plot box `PX=62`, width `w − 102`, `TOPY=66`, `BASEY=252`, percent axis 0–55 with `P.grid`
  gridlines at 0/15/30/45 and 12px `P.mute` labels. Six evenly spaced steps; two-line 12px `P.mute`
  step labels beneath the axis: "no / alternative", "one other / provider", "two other / providers",
  "notice cut / to a month", "switching / made cheap", "free to leave / any time".
- **The flat line:** dashed 2px `P.mute` horizontal at the private rate, labelled bold 12px `P.mute`
  "191 of 400 privately below passing — unchanged all year" at the right.
- **The rising line:** 3px `P.orange` through the six reported rates, dots 5.5px — pale
  `rgba(217,89,38,0.45)` where the reported rate is under half the private rate, solid `P.orange`
  above it. Each rate printed bold 12px `P.orange` above its dot with the raw count in 12px `P.mute`
  below it.
- **Shading:** the band between the rising line and the flat private line filled
  `rgba(201,133,0,0.12)` — the complaints that exist and are not being filed, narrowing as the exit
  door opens.
- **End callout (bold 12px `P.yellow`, right-aligned above the last point):** the computed ratio,
  "7.7× the complaints from the same 400 people", with 12px `P.mute` "and 7.3 points of it still
  hidden" beneath.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "A quiet room can mean a locked door rather than a good service."

---

## Page-specific constraints

- **Structure:** a canvas-less `.card-section` for the sibling's unnumbered preamble — an `<h2>`,
  prose paragraphs and three `table.layout` derivation tables, no text/viz row — then three numbered
  `.card-section` blocks. The 50/50 split is fixed; a chart is shrunk through
  canvas `max-width` / `height`, never by narrowing the viz column.
- **Canvas heights:** 350, 380, 350 — section 2 is taller to carry its side panel.
- **Canvas CSS:** `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px.
- **Register: the prose states mechanisms, the charts carry the arithmetic.** No bullet opens with a
  count or a group size, no decimal percentages appear in prose, and no bullet reconciles subgroup
  counts to a total — that is the chart's job, done at render time. A figure earns a place in a
  bullet only where the figure *is* the argument: section 2 keeps the named-versus-unnamed gap
  (33.8 points against 1.8) because the contrast between those two numbers is the whole test, and
  section 3 keeps "nearly eight times the complaints" because the multiple is the finding. Elsewhere
  the fact is stated in words — "most customers were fine, out of a room where about half quietly
  were not" rather than a pair of rates. Every exact value still appears on the canvas.
- **One construction runs the whole page.** `ROUGH = 4.6`, `FINE = 6.9`, noise spread `1.6`,
  `COST = 2.8`, `RESID = 0.18`, complaint threshold `< 5`. Every chart calls the same `panel()` and
  `form()` helpers, so section 1's split, section 2's named/unnamed gap and section 3's rising line
  are the same model under different `dep` and `anonymous` arguments. Changing one constant moves
  every figure on the page at once, which is the point.
- **Every printed figure is computed in its draw function** from the plotted values and printed from
  that variable — rates, counts, gaps, ratios, differences. The subgroup identities (`4 + 64 = 68`,
  `74 + 72 = 146`, `21 + 135 = 156`, `22 + 7 = 29`) are checked in the draw, and the counts that look
  like subtractions (the 70, the 135) are counted respondent by respondent instead.
- **What the page claims, precisely.** The claim is *not* that respondents fail to notice harm — it
  is that they notice it and do not report it, because reporting it costs them something. Three
  behaviours are kept distinct: genuinely not noticing (a real bias, not this page), noticing and
  accepting anyway (a correct choice when the alternatives are worse, not an error), and noticing
  and staying quiet under a power gap (what this page measures). Silence is never framed as a
  reasoning mistake by the person staying silent; the defect belongs to whoever collects the
  feedback and reads the average as the population.
- **Scope boundaries.** How bad news is weighted against good belongs to
  `26-negativity-dominance`; a steady good level going unremarked belongs to
  `27-absence-blindness`. Neither is discussed here, and neither is linked.
- **Language.** Plain and physical throughout. No survey-methodology or social-science terminology
  in the prose — "they saw it, they just were not going to say it to your face" rather than a named
  effect. Organisations are `Vendor A` / `Team A` style if named at all; no real companies, no
  politics. Every constructed figure sits under an "Illustrative Example" `.src` note.
