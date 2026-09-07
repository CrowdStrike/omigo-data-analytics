# Familiarity Feels Like Quality: You Are Rating Your Own Retraining Bill

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Familiarity Feels Like Quality — Cognitive Biases

**Subtitle:** Change a tool someone uses every day and they will tell you it got worse. Show the same change to a stranger and they prefer it on the spot.

---

## Section 1 — Two Rooms, One Redesign, Opposite Verdicts

**Tags:** `core idea` (violet), `two rooms` (blue), `same design` (magenta)

**Bullets:**
- **The setup** — one redesign, one score sheet out of ten, and two rooms that never meet
- **Room one** — forty people who used the old version every working day for two years
- **Room two** — forty people meeting either version for the first time this morning
- **The mark to beat** — the old version scores five out of ten from both rooms alike
- **What the daily users said** — an average of 3.3, and 34 of the forty put it below the old
- **What the newcomers said** — an average of 6.5, and only 3 of the forty rated it down
- **What differs between the rooms** — nothing but the hours already spent on the old version
- **What the daily users measured** — the cost of unlearning their own fingers, not the design

**Key point:** The two rooms saw an identical design and split by more than three points. A verdict that moves that far on the rater's history is measuring the rater, not the thing being rated.

**Source note (`.src`):** Illustrative Example — eighty seeded score sheets; both averages and both counts are read back out of the plotted bars.

### Visualization — canvas `c1`, 720×340

Two score distributions side by side over the same 0–10 axis, with the old version's own score drawn as the line both are read against, and the two averages marked underneath.

- **Shared construction (used by every chart on the page):** a rater's whole-number score is
  `round(clamp(OLD + gain − relearn(visits) × decay × (0.75 + 0.5·rng()), 0, 10))` where
  `OLD = 5` is what the old version scores, `gain` is how much genuinely better the redesign is
  (`GOOD = 1.2`, `WORSE = −0.5`), `relearn(visits) = 0.85 × log10(1 + visits)` is what habit costs
  a rater with that many prior visits, `decay` shrinks that cost as they retrain, and the noise term
  is `(rng()+rng()+rng()−1.5) × 2 × 1.5`. Seeded Park–Miller LCG, seed 42, in every chart.
- **Data:** two panels of 40. Daily users at `visits = 800`, newcomers at `visits = 0`, both scoring
  the same genuinely-better redesign. Daily users average **3.3** with **34 of 40** below five;
  newcomers average **6.5** with **3 of 40** below five. All four figures counted in the draw function.
- **Title (bold 15px `P.ink`, centered, y=21):** "Two Rooms Score the Same Redesign"
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
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "One design, one form — the rooms differ only in habit."

---

## Section 2 — The Score Falls as the Habit Grows

**Tags:** `the dial` (orange), `where it flips` (yellow), `habit only` (blue)

**Bullets:**
- **One redesign, seven rooms** — sorted only by how often each rater used the old version
- **Nobody in any room** — is told which version is newer or what anyone else scored
- **First-timers** — have no habit to protect, and they hand it 6.0 out of ten
- **Five prior visits** — 5.5, a mild preference, and the redesign is still the better one
- **A hundred prior visits** — 4.6, and the redesign has quietly become the worse one
- **Two thousand visits** — 3.3, a fall of 2.7 points with nothing changed about the design
- **Where the verdict turns** — around 17 prior visits, past which habit outweighs the gain
- **The dial** — the score is set by hours spent on the old version, not by the new one

**Key point:** Prior exposure alone walks the verdict from clearly better to clearly worse. Anywhere past the turning point, "I think this is worse" and "I have used the old one a lot" produce the same sentence.

**Source note (`.src`):** Illustrative Example — seven seeded panels of 300; every printed score and the turning point are computed in the draw function.

### Visualization — canvas `c2`, 720×320

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

## Section 3 — Ten Weeks Later, Nobody Changed the Design

**Tags:** `it wears off` (aqua), `the proof` (green), `launch week` (violet)

**Bullets:**
- **The launch survey** — the daily users score the new version 3.6, well under the old five
- **Nothing shipped after that** — no fixes, no rollback, not one changed pixel for ten weeks
- **The same people, asked weekly** — the score climbs on its own as their hands relearn it
- **Week five** — level with the old version again, off the identical score sheet
- **Week ten** — 5.8, comfortably above the version they were defending at launch
- **The rise** — 2.2 points, bought entirely by practice rather than by any design work
- **What it proves** — the launch score was a bill for retraining, and the bill got paid off
- **What it prevents** — rolling back in week two on a number that was about to fix itself

**Key point:** A score that recovers while the design sits untouched was never a reading of the design. The recovery is the receipt: what the launch survey priced was the transition, and transitions end.

**Source note (`.src`):** Illustrative Example — ten seeded weekly surveys of the same 300 daily users; the launch score, the recovery week and the rise are all read off the drawn line.

### Visualization — canvas `c3`, 720×340

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

## Section 4 — A Worse Redesign Draws the Same Boos

**Tags:** `cuts both ways` (magenta), `no signal` (blue), `the real problem` (red)

**Bullets:**
- **A second redesign** — genuinely worse than the old version, shown to the same two rooms
- **Daily users, better redesign** — 72 in a hundred call it worse than what they had
- **Daily users, worse redesign** — 94 in a hundred, only 22 points away from the good one
- **Newcomers, better redesign** — 14 in a hundred call it worse
- **Newcomers, worse redesign** — 53 in a hundred, a 39-point gap that separates the two cases
- **Why the boos sound alike** — habit swamps quality, so both land as the same complaint
- **The practical problem** — a room that boos everything cannot say which one to keep
- **Where the signal went** — to the room with nothing to unlearn, whose answers still split

**Key point:** The bias is not that daily users dislike change — it is that they dislike improvement and damage by almost the same amount. Their reaction is honest and nearly useless, because it barely moves when quality does.

**Source note (`.src`):** Illustrative Example — four seeded panels of 400, one per room-and-redesign pair; every share and both gaps are counted in the draw function.

### Visualization — canvas `c4`, 720×330

Four bars in two labelled groups showing the share who call the redesign worse than the old version, with the within-room gap between the good and bad redesign printed beside them.

- **Data:** four panels of 400. Daily users (`visits = 800`) and newcomers (`visits = 0`), each scoring
  a genuinely-better redesign (`gain = +1.2`) and a genuinely-worse one (`gain = −0.5`). Shares scoring
  it under five: daily users **72%** and **94%**; newcomers **14%** and **53%**. Group averages
  **3.6 / 2.0** and **6.2 / 4.4**.
- **The gaps:** **22 points** inside the daily-user room, **39 points** inside the newcomer room, each
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
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The room that always says worse cannot tell you when it is."

---

## Section 5 — Telling Worse Apart from Merely Different

**Tags:** `the method` (green), `outcomes not opinions` (orange), `let time pass` (aqua)

**Bullets:**
- **The question** — is this worse, or is it only different from what my hands expect
- **What opinion cannot settle** — both answers feel identical from the inside, at any exposure
- **The measurement that can** — time the same task on both versions and count the fumbles
- **The old version** — took 42 seconds a task, the mark the new one has to beat
- **Week one on the new one** — slower, because the users are still hunting for everything
- **Week three** — already quicker than the old version on the clock, for the same people
- **Their opinion in week three** — still below the old version, and it stays there in week four
- **The two-week window** — the clock says keep it, the room says bin it, and the clock is right
- **The method** — measure outcomes not opinions, and let a few weeks pass before you ask

**Key point:** Two things break the tie that a survey cannot. Measure what people accomplish rather than what they report, and wait long enough for retraining to finish. A dislike that survives both is about the design.

**Source note (`.src`):** Illustrative Example — ten seeded weeks of task timings and score sheets from the same 250 daily users; both crossing weeks and the width of the window are read off the drawn lines.

### Visualization — canvas `c5`, 720×340

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
  "2 weeks when the clock says better and the room says worse" between the panels.
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Ask the stopwatch first and the room later."

---

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`. One
  `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid
  #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block;
  width: 100%; margin: 0 auto`. The canvas is capped at 720px, so a wide cell leaves slack.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>term</b>` →
  one `.key-point` callout → `.src` note. No paragraph blocks, no data tables, no `.math-box`.
- **Bullet form:** each is ONE line that does not wrap at 50% column width (≤95 characters including
  the bold label), opening with a `<b>bold label</b>` then an em dash and the fact. Bullet count
  follows the content — eight or nine here because the construction needs both rooms named and both
  figures given; never padded to a quota, and no line restates another.
- **Section titles:** name the content. No role labels, no phrasing reused from another page.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276`
  with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px
  bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse,
  cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom
  margin, `li b` in `#1a5276`. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`,
  padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no back/home links, no cross-page links,
  no `.nav` CSS.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes
  used: `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta`
  `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow`
  `rgba(201,133,0,0.15)`/`#a06c00`.
- **Colour variety across sections is a requirement.** Each section owns a hue family and its pills,
  chart fills and caption sit in it: section 1 violet against blue, section 2 orange with a yellow
  crossing marker, section 3 aqua over a green band, section 4 blue against magenta, section 5 green
  above orange with a magenta window. No section is blue-fill-plus-orange-highlight.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus
  the per-chart height (340, 320, 340, 330, 340). `setup(id)` caches the logical size in `dataset` on
  the first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`,
  computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and
  `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on
  debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart header bold 12–13px; body and axis labels
  12px floor; the big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`,
  `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`,
  `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **One construction runs the whole page.** `OLD = 5`, `GOOD = +1.2`, `WORSE = −0.5`,
  `relearn(visits) = 0.85 × log10(1 + visits)`, noise spread 1.5, daily users at 800 prior visits.
  Every chart calls the same `panel()` helper, so section 1's split, section 2's slide, section 3's
  recovery, section 4's two cases and section 5's opinion line are all the same model under different
  arguments. Changing a constant moves every figure on the page at once, which is the point.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`),
  seed 42, one fresh stream per chart. Every average, count, share, crossing week and turning point
  is computed inside the draw function and printed from that variable.
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
