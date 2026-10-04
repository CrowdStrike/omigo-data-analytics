# Mere-Exposure Effect — Viz

**Page type:** detail page — card-section
**HTML title tag:** Mere-Exposure Effect — Cognitive Biases
**Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`
**Source note wording:** the sibling `.txt.md` says figures are "computed at render time"; the html `.src` notes for sections 2 and 4 say "computed in the draw function" / "counted in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** The split is: the sibling `.txt.md` carries the reader-facing construction — the unnumbered preamble, the score rule in words, the derivation table of expected scores, and the note that section 2's printed turning point of 17 comes from its sampled line rather than the rule's own 25 — while this file carries its implementation (the constants, the `panel()` helper, the seeds, the drawing). Changing a seed or any constant invalidates both halves — every average, count, share, crossing week and turning point must be re-read from the new draw and the sibling's preamble and bullets updated to match.

**Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, one fresh stream per chart. Every average, count, share, crossing week and turning point is computed inside the draw function and printed from that variable.

---

## 1. Two Rater Groups, One Redesign, Opposite Verdicts

**Tag colors:** `core idea` violet, `two groups` blue, `same design` magenta
**Hue family:** violet against blue

### canvas `c1` — 720×340

Two score distributions side by side over the same 0–10 axis, with the old version's own score drawn as the line both are read against, and the two averages marked underneath.

- **Shared construction — implementation of the reader-facing one in the sibling `.txt.md`:** that
  sibling's unnumbered preamble carries the premise, the rule in words and the derivation table; this
  block is only the code that produces them. A rater's whole-number score is
  `round(clamp(OLD + gain − relearn(visits) × decay × (0.75 + 0.5·rng()), 0, 10))` with constants
  `OLD = 5`, `GOOD = 1.2`, `WORSE = −0.5`, `HABIT = 0.85`, `SPREAD = 1.5`, `REGULAR = 800`, where
  `relearn(visits) = HABIT × log10(1 + visits)`, `decay` is the caller's retraining factor, and the
  noise term is `bell(rng, SPREAD) = (rng()+rng()+rng()−1.5) × 2 × SPREAD`. One `panel()` helper, one
  seeded Park–Miller LCG stream per chart, seed 42 in all five. Panel sizes are small on purpose in
  section 1 (40 a side), so its two averages sit further off the rule's centres than the 300- and
  400-rater panels do — the sibling's preamble states that reconciliation and must be kept in step.
- **Data:** two panels of 40. Daily users at `visits = 800`, newcomers at `visits = 0`, both scoring
  the same genuinely-better redesign. Daily users average **3.3** with **34 of 40** below five;
  newcomers average **6.5** with **3 of 40** below five. All four figures counted in the draw function.
- **Title (bold 15px `P.ink`, centered, y=21):** "Two Rater Groups Score the Same Redesign"
- **Legend (bold 12px, y=42):** `P.violet` "daily users — 34 of 40 rated it below the old" at x=56;
  `P.blue` "newcomers — 3 of 40 did" at x=430. Both counts printed from the tally.
- **Bars:** plot box `PX=56`, width `w−90`, `TOPY=78`, `BASEY=218`. Eleven score slots; each slot
  carries a violet bar (`rgba(74,58,167,0.50)` stroked `P.violet`) left of centre for the daily users
  and a blue bar (`rgba(42,120,214,0.45)` stroked `P.blue`) right of centre for the newcomers, each
  scaled against the tallest bar in either tally. Score labels 0–10 in 12px `P.mute` beneath.
- **The mark to beat:** dashed 1.5px `P.mute` vertical at score 5, labelled 12px `P.mute`
  "the old version scores 5" above the plot.
- **Average strip** at `BASEY + 64`: a 2px `P.grid` rule spanning the same score axis, a violet
  triangle at 3.3 and a blue triangle at 6.5, each with its figure in bold 19px beneath, plus
  12px `P.mute` "average score" at the left. Both positions come from the computed means.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "One design, one form — the groups differ only in habit."

---

## 2. The Score Falls as the Habit Grows

**Tag colors:** `the dial` orange, `where it flips` yellow, `habit only` blue
**Hue family:** orange with a yellow crossing marker

### canvas `c2` — 720×320

Average score plotted against prior visits on a compressed horizontal axis, crossing the old version's score partway along, with the crossing point marked.

- **Data:** groups at `visits = 0, 5, 25, 100, 400, 800, 2000`, 300 raters each, all scoring the same
  genuinely-better redesign. Averages **6.0, 5.5, 4.8, 4.6, 4.0, 3.6, 3.3**. The fall across the row,
  2.7 points, is computed as first minus last.
- **Turning point:** where the drawn line crosses five, interpolated between the two straddling groups
  along the same compressed axis the chart plots on — **17 prior visits**. Printed from that variable,
  not asserted.
- **Title (bold 15px `P.ink`, centered, y=21):** "Score Against How Often the Rater Used the Old Version"
- **Axes:** plot box `PX=62`, width `w−102`, `TOPY=60`, `BASEY=232`, score range 2.5–7. Horizontal axis
  spaced by `log10(1 + visits)` so the whole range fits; tick labels are the raw visit counts in
  12px `P.mute`. Gridlines at 3–7 in `P.grid`.
- **The mark to beat:** dashed 2px `P.mute` horizontal at five, labelled bold 12px `P.mute`
  "the old version scores 5" at the right.
- **The line:** 3px `P.orange` through the seven group averages, each point a 5.5px dot — solid
  `P.orange` where the group still rates it above the old version, `rgba(217,89,38,0.55)` where it
  does not. Each average printed bold 12px `P.orange` above its dot.
- **Crossing marker:** dashed 2px `P.yellow` vertical at the interpolated crossing, with bold 19px
  `P.yellow` "17" and bold 12px "prior visits — the verdict flips here" beside it.
- **Footnote (12px `P.mute`, below the axis label):** "the redesign is identical in every group — the
  score falls 2.7 points across the row", the drop computed from the plotted points.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "The score slides down as habit goes up, and nothing else moved."

---

## 3. Ten Weeks Later, Nobody Changed the Design

**Tag colors:** `it wears off` aqua, `the proof` green, `launch week` violet
**Hue family:** aqua over a green band

### canvas `c3` — 720×340

A weekly line rising from well below the old version's score up past it, with the band below that score shaded gray and the band above it shaded green.

- **Data:** 300 daily users (`visits = 800`) surveyed in each of ten weeks. The habit cost decays as
  `exp(−t / 5)`, so week one carries the full cost and week ten almost none. Weekly averages
  **3.6, 4.1, 4.4, 4.9, 5.1, 5.2, 5.4, 5.5, 5.7, 5.8**.
- **Read off the line:** launch score **3.6**, first week at or above five is **week 5**, week-ten score
  **5.8**, total rise **2.2 points** — each printed from the array rather than typed in.
- **Title (bold 15px `P.ink`, centered, y=21):** "The Same Daily Users, Asked Again Every Week"
- **Axes:** plot box `PX=58`, width `w−98`, `TOPY=58`, `BASEY=224`, score range 3–6.5, weeks 1–10
  evenly spaced. Gridlines at 3–6 in `P.grid`.
- **Shading:** everything below score five filled `rgba(107,114,128,0.09)`, everything above filled
  `rgba(25,158,112,0.09)`, so crossing the line is visible as leaving one band for the other.
- **The mark to beat:** dashed 2px `P.mute` horizontal at five, labelled bold 12px `P.mute`
  "the old version scores 5" at the right.
- **The line:** 3px `P.aqua` through the ten weekly averages; dots solid `P.aqua` in weeks at or above
  five, `rgba(25,158,112,0.40)` below. Bold 12px `P.violet` "launch week: 3.6" beside the first point,
  bold 19px `P.aqua` "5.8" above the last.
- **Recovery marker:** dashed 2px `P.aqua` vertical dropped from the first week at or above five to the
  axis, labelled bold 12px `P.aqua` "week 5: level with the old version again".
- **Footnote (12px `P.mute`):** "nothing was changed after launch — the score rose 2.2 points on its own".
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "A verdict that expires was never about the design."

---

## 4. A Worse Redesign Draws the Same Boos

**Tag colors:** `cuts both ways` magenta, `no signal` blue, `the real problem` red
**Hue family:** blue against magenta

### canvas `c4` — 720×330

Four bars in two labelled groups showing the share who call the redesign worse than the old version, with the within-group gap between the good and bad redesign printed beside them.

- **Data:** four panels of 400. Daily users (`visits = 800`) and newcomers (`visits = 0`), each scoring
  a genuinely-better redesign (`gain = +1.2`) and a genuinely-worse one (`gain = −0.5`). Shares scoring
  it under five: daily users **72%** and **94%**; newcomers **14%** and **53%**. Group averages
  **3.6 / 2.0** and **6.2 / 4.4**.
- **The gaps:** **22 points** inside the daily-user group, **39 points** inside the newcomer group, each
  computed as the difference of the two rounded shares actually printed on the bars.
- **Title (bold 15px `P.ink`, centered, y=21):** "Share Who Call the Redesign Worse Than the Old Version"
- **Legend (bold 12px, y=42):** `P.blue` "redesign is genuinely better" at x=58, `P.magenta`
  "redesign is genuinely worse" at x=262.
- **Bars:** plot box `PX=58`, width `w−222`, `TOPY=62`, `BASEY=244`, percent axis 0–100 with `P.grid`
  gridlines and 12px `P.mute` labels. Two groups; within each, the better-redesign bar in
  `rgba(42,120,214,0.45)` stroked `P.blue` and the worse-redesign bar in `rgba(213,81,129,0.50)`
  stroked `P.magenta`. Each bar carries its share in bold 19px above and "avg N.N" in 12px `P.mute`
  below. Group labels bold 12px `P.ink`: "daily users of the old version", "newcomers, no habit to lose".
- **Side panel** at `PX + PW + 26`: bold 13px `P.ink` "HOW FAR THE TWO / CASES PULL APART", then bold
  19px `P.blue` "22 points" over 12px `P.mute` "for daily users", and bold 19px `P.magenta` "39 points"
  over "for newcomers"; then bold 12px `P.blue` "daily users vote / it down either way".
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The group that always says worse cannot tell you when it is."

---

## 5. Telling Worse Apart from Merely Different

**Tag colors:** `the method` green, `outcomes not opinions` orange, `let time pass` aqua
**Hue family:** green above orange with a magenta window

### canvas `c5` — 720×340

Two stacked panels over one week axis — task seconds above, score out of ten below — with the weeks where the two answers disagree boxed across both.

- **Data:** 250 daily users, ten weeks. Task seconds are
  `42 × (1 − 0.09 + 0.22 × exp(−t / 1.5))` plus seeded noise of spread 2.2, so the redesign is
  genuinely 9% quicker once learned but clumsier at first. Weekly means **47.4, 42.9, 40.5, 39.5,
  38.9, 38.5, 38.3, 38.1, 38.4, 38.4** seconds against the old version's 42.
- **Opinion:** the same users scoring the redesign, habit cost decaying as `exp(−t / 4.5)`. Weekly means
  **3.6, 4.0, 4.6, 4.8, 5.1, 5.4, 5.5, 5.4, 5.9, 5.8**.
- **Crossings, both scanned from the arrays:** the clock beats the old version from **week 3**; opinion
  does not until **week 5**; the disagreement window is **2 weeks** wide, printed as the difference of
  those two week indexes.
- **Title (bold 15px `P.ink`, centered, y=21):** "Stopwatch and Opinion, Measured Every Week"
- **Upper panel** (`AT=56`, `AB=142`, seconds axis 36–50): bold 12px `P.ink` header "SECONDS TO FINISH
  THE TASK — lower is better"; dashed 2px `P.mute` line at 42 labelled "old version took 42 seconds";
  3px `P.green` line through the weekly means with dots solid `P.green` in weeks at or under 42 and
  `rgba(0,131,0,0.35)` above; bold 12px `P.green` "quicker than the old version from week 3" at the
  first qualifying point.
- **Lower panel** (`BT=190`, `BB=274`, score axis 3–6.5): bold 12px `P.ink` header "SCORE THE SAME USERS
  GIVE IT — higher is better"; dashed 2px `P.mute` line at five labelled "old version scored 5"; 3px
  `P.orange` line with dots solid `P.orange` at or above five and `rgba(217,89,38,0.35)` below; bold
  12px `P.orange` "liked better than the old version from week 5"; week numbers 1–10 in 12px `P.mute`.
- **Disagreement window:** a `rgba(213,81,129,0.10)` fill with a dashed 1.5px `P.magenta` outline
  spanning both panels from the clock's crossing week to opinion's, labelled bold 12px `P.magenta`
  "2 weeks when the clock says better and the group says worse" between the panels.
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Ask the stopwatch first and the group later."

---

## Page-specific constraints

- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block;
  width: 100%; margin: 0 auto`. The canvas is capped at 720px, so a wide cell leaves slack.
- **No paragraph blocks, no data tables, no `.math-box`** in the text column.
- **Bullet count follows the content** — eight or nine here because the construction needs both groups
  named and both figures given; never padded to a quota, and no line restates another.
- **Section titles name the content.** No role labels, no phrasing reused from another page.
- **Colour variety across sections is a requirement.** Each section owns a hue family and its pills,
  chart fills and caption sit in it: section 1 violet against blue, section 2 orange with a yellow
  crossing marker, section 3 aqua over a green band, section 4 blue against magenta, section 5 green
  above orange with a magenta window. No section is blue-fill-plus-orange-highlight.
- **Per-chart canvas heights:** 340, 320, 340, 330, 340.
- **One construction runs the whole page.** `OLD = 5`, `GOOD = +1.2`, `WORSE = −0.5`,
  `relearn(visits) = 0.85 × log10(1 + visits)`, noise spread 1.5, daily users at 800 prior visits.
  Every chart calls the same `panel()` helper, so section 1's split, section 2's slide, section 3's
  recovery, section 4's two cases and section 5's opinion line are all the same model under different
  arguments. Changing a constant moves every figure on the page at once, which is the point. The
  reader-facing statement of this rule, with its derivation table, is the unnumbered preamble in the
  sibling `.txt.md` — both halves move together.
- **Corrections applied to the earlier version of this page:** every figure on the old page was
  asserted rather than computed. A line chart of "rated preference" against exposures used the
  hardcoded points `[0,4] … [1.0,8.3]` with a flat green line labelled "objective task performance
  (unchanged)" — no data, no seed, nothing to check. A bar chart printed lifetime session counts
  (10,000 / 1,500 / 200 / 0) with bar widths 440 / 170 / 62 / 3 px, described in its own spec as
  proportional to the square root of the counts; the true square roots are 100 / 38.7 / 14.1 / 0,
  which scale to 440 / 170 / 62 / 0 px, so the widths were roughly right but the zero bar was drawn
  3px wide to stay visible while representing zero. Two of the four canvases were flow diagrams of
  boxes and arrows containing no data at all. All five charts are now generated and self-labelling.
  The old page's "mirror image" section, which paired this bias with novelty-as-quality, has been
  dropped: that belongs on the newer-bigger-better page, and its removal makes room for the
  recovery curve and the worse-redesign comparison, which are what make this page's claim checkable.
