# Deciding in Advance: A Rule Written Before You Care Which Way It Falls

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Deciding in Advance — Cognitive Biases

**Subtitle:** Bias needs a moment where somebody chooses. Fix the choice beforehand and that moment never arrives — for the choices you actually fixed.

---

## Section 1 — A List Written at Home Beats Twenty Decisions in the Aisles

**Tags:** `core idea` (violet), `one decision, not twenty` (blue), `the entry point` (magenta)

**Bullets:**
- **The mechanism** — bias needs a moment where a person chooses, so removing the moment removes it
- **The list** — ten things, priced and written down at home, before any shelf is in view
- **Walking in without one** — twenty aisles, and every aisle asks the question again
- **What the list spends** — 36.36 on every trip, because nothing at all was decided in the shop
- **What ten unlisted trips spent** — between 46.01 and 76.28, buying the same ten things
- **On average** — 60.16, sixty-five percent over the list, and no trip came anywhere near it
- **Where it went** — three to eleven unplanned yeses a trip, each one perfectly defensible alone
- **The list's real work** — not making you thrifty, but cutting twenty chances to be talked round

**Key point:** A rule fixed in advance does not make the decision wiser. It cuts down the number of moments at which a preference can get in — and those moments are the only door bias has.

**Source note (`.src`):** Illustrative Example — one seeded list and ten seeded trips; every total is computed in the draw function.

### Visualization — canvas `c1`, 720×330

Running spend against aisle number: ten wandering unlisted trips and one flat listed trip.

- **Data:** seeded Park–Miller LCG, seed 42. Ten list items priced `round(180 + rng()·420)/100` (1.80–6.00), total **36.36**. Then ten trips over 20 aisles; in aisles 0–9 the listed item is added, and at every aisle an unplanned yes fires when `rng() < 0.32`, priced `round(150 + rng()·400)/100` (1.50–5.50).
- **Computed:** trip ends 66.76, 61.34, 59.90, 59.59, 76.28, 60.25, 57.79, 63.93, 49.77, 46.01 → lowest **46.01**, highest **76.28**, average **60.16**, which is **+65%** over the list. Unplanned yeses per trip run **3 to 11**, average **6.5**. All read off the arrays in the draw function.
- **Title (bold 15px `P.ink`, centered, y=22):** "Twenty Aisles, One Shopping List"
- **Axes:** x = aisle 0…20 across `PX=52 … w−190`; y = running spend 0…80, baseline `h−72`, top `y=46`. Faint `P.grid` horizontal lines and 12px `P.mute` ticks every 20; x ticks every 5 with "aisle" centred beneath.
- **Trip paths:** ten polylines, 1.5px `rgba(42,120,214,0.40)`; the costliest trip redrawn 2.5px `P.magenta` with a filled end dot.
- **List path:** 3px `P.violet`, rising through the first ten aisles then flat, filled end dot, bold 12px `P.violet` label "36.36 — the list" set below and left of the dot.
- **Range bracket** just right of the plot spanning lowest to highest trip end, 2px `P.magenta`, with bold 12px `P.magenta` "76.28" above its top arm and "46.01" below its bottom arm.
- **Side panel** at `w−168`: bold 13px `P.ink` "AVERAGE UNLISTED TRIP", bold 19px `P.magenta` "60.16", then 12px `P.mute` "+65% over the list" and "3–11 unplanned yeses"; then bold 13px `P.ink` "MOMENTS OF CHOICE", bold 19px `P.violet` "1" beside 12px "with the list, / decided at home", and bold 19px `P.magenta` "20" beside 12px "without it, one / per aisle".
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Fewer moments of choice, not better choices — that is the whole of the protection."

---

## Section 2 — The One Line Left Blank Decides the Hire

**Tags:** `what it protects` (aqua), `the blank line` (blue), `where the argument lands` (violet)

**Bullets:**
- **The scorecard** — four lines per candidate, eight candidates, every line marked out of twenty
- **Filled in beforehand** — the winner is whoever tops the total, and nobody negotiates afterwards
- **As scored** — F leads on 53 and B is on 52, so the order is settled before anyone speaks
- **Now leave one line open** — the team task gets marked after the interview, from memory
- **That line runs five to twenty** — fifteen points of room, on totals in the forties and fifties
- **What one open line can do** — four of the seven runners-up can be lifted into first place by it
- **Who stays out of reach** — three cannot: two already near that line's top, one too far back
- **The tell** — nobody argues about the three fixed lines, every argument lands on the open one

**Key point:** A rule protects exactly the choices it actually pins down and nothing else. One line left to be filled in later is enough to reach the answer you wanted, and it will be the only line anyone fights over.

**Source note (`.src`):** Illustrative Example — eight seeded scorecards; each reach and every count is computed in the draw function.

### Visualization — canvas `c2`, 720×340

Eight stacked bars: the three fixed lines solid, then the reach of the one open line drawn hollow past the leader's total.

- **Data:** seeded LCG, seed 42; eight candidates × four lines, each `5 + floor(rng()·16)` (range 5–20). Scores — A 5/13/16/9, B 11/8/20/13, C 13/9/6/18, D 19/12/8/8, E 8/10/6/17, F 13/18/16/6, G 13/7/18/12, H 12/12/8/5.
- **Computed:** totals F 53, B 52, G 50, D 47, C 46, A 43, E 41, H 37. Fixed part = first three lines; ceiling = fixed part + 20. Ceilings F 67, B 59, D 59, G 58, A 54, H 52, C 48, E 44. Against F's 53, the ceilings of **B, G, D and A** clear it and **C, E, H** do not → **4 of 7** runners-up reachable. All computed in the draw function.
- **Title (bold 15px `P.ink`, centered, y=22):** "Eight Scorecards, One Line Still Blank"
- **Rows:** eight rows in descending total, 25px pitch from y=52, bar height 16. x = points 0…70 across `AX=112 … w−96`.
- **Bar segments:** written test `rgba(42,120,214,0.45)`/`P.blue`, trial task `rgba(25,158,112,0.45)`/`P.aqua`, references `rgba(74,58,167,0.40)`/`P.violet` — the three fixed lines. The team task as actually scored: `rgba(107,114,128,0.35)` stroked `P.mute`.
- **Reach:** from the as-scored total out to the ceiling, drawn as a dashed (5/4) 1.5px `P.blue` outline with no fill, so the room the open line has reads as empty space.
- **Leader line:** 2px `P.aqua` vertical at F's total, bold 12px `P.aqua` label "the total to beat — 53" above it.
- **Row labels:** candidate letter 12px `P.mute` left of the axis; its total printed bold 12px at the end of the solid part, in `P.aqua` for the leader and `P.mute` otherwise; then bold 12px `P.blue` "can reach first" past the dashed end for every row whose ceiling clears the leader line, `P.mute` "cannot" otherwise — both decided by comparison, not asserted.
- **Legend** under the rows: three filled swatches naming the fixed lines, then a dashed `P.blue` swatch labelled "room the blank team task line still has".
- **Callout** under the legend, left-aligned at `AX`: bold 19px `P.blue` "4 of 7" with 12px `P.mute` "runners-up the open line can lift into first place".
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "The fixed lines are safe. The one left blank carries the whole decision."

---

## Section 3 — Four Honest Weightings, Four Different Winners

**Tags:** `the limit` (magenta), `the weighting` (violet), `bias moved earlier` (red)

**Bullets:**
- **The same eight people** — the same four marks each, and nothing about any candidate changes
- **Count all four lines equally** — F comes first, on 53
- **Count references double** — B comes first instead, on 72
- **Count the team task triple** — C comes first, on 82
- **Count the written test triple** — D comes first, on 85
- **Every one of those rules** — sounds fair, applies identically to all eight, and is written down
- **Sweep every simple weighting** — five of the eight can win under one, three can never win any
- **Where the bias went** — into the weighting, chosen once, by one person, never looked at again

**Key point:** Deciding in advance does not remove the judgement, it relocates it into the rule. Whoever set the weights picked the winner — earlier, and with far less scrutiny than an argument in the room would have drawn.

**Source note (`.src`):** Illustrative Example — the same eight seeded scorecards; every winner and every share is computed in the draw function.

### Visualization — canvas `c3`, 720×340

Four named weightings with the winner each produces, above the share of all simple weightings that crowns each candidate.

- **Data:** the same eight seeded scorecards as section 2.
- **Named rules (weights on written / trial / references / team):** `1,1,1,1` → F on 53; `1,1,2,1` → B on 72; `1,1,1,3` → C on 82; `3,1,1,1` → D on 85. Winners found by scanning the weighted totals.
- **Sweep:** every weighting with each line's weight in 0…3 and not all zero — **255** rules. **17** end in a tie at the top and are not credited. Crown shares: **F 39%**, **B 29%**, **C 16%**, **D 9%**, **G 1%**, and **A, E, H 0%** — five candidates can win, three never can. Counted in the draw function.
- **Title (bold 15px `P.ink`, centered, y=22):** "Same Eight Marks, Four Different Winners"
- **Top block:** bold 13px `P.ink` header "FOUR RULES ANYONE WOULD CALL FAIR" at y=48. Four rows on a 26px pitch from y=74: 12px `P.mute` rule name right-aligned at x=312, then a filled circle radius 11 in `P.magenta` (the equal-weight rule in `P.violet`) with the winning letter in bold 12px white, then bold 12px in the same hue reading "comes first, on 53 / 72 / 82 / 85".
- **Bottom block:** bold 13px `P.ink` header "SHARE OF ALL SIMPLE WEIGHTINGS THAT CROWN EACH CANDIDATE" at y=196. Eight columns from x=60, baseline `h−52`, top `y=212`, bar width capped at 44. Bars in `rgba(213,81,129,0.45)`/`P.magenta` scaled to the largest share; candidates with a zero share drawn as a 1px dashed `P.mute` outline only, labelled bold 12px `P.mute` "never". Percentages printed bold 12px above each non-zero bar from the tally, letters 12px `P.mute` below the baseline, and 12px `P.mute` "255 weightings, 17 tied" right-aligned on the header line.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The candidates never changed. Only the weighting did, and the weighting chose."

---

## Section 4 — A Screen Set in Advance Drops the Strongest Applicant

**Tags:** `where it fails` (orange), `the screen` (yellow), `nobody re-reads it` (red)

**Bullets:**
- **The pool** — forty applicants, four marked lines each, and one screen applied before all else
- **The screen** — score under thirteen on the trial task and the rest of your card is never read
- **What it drops** — twenty-five of the forty, evenly, exactly as it was written
- **The strongest applicant overall** — totals 65, scored ten on the trial task, gone at the screen
- **The best of the survivors** — totals 61, so four points come off the top of the pool unnoticed
- **Of the ten strongest overall** — five never reach the second stage at all
- **Why it survives** — it was set before any application arrived, so it reads as even-handed
- **What nobody asks** — whether the trial task earned the right to be the one disqualifying line

**Key point:** A rule applied evenly to everybody can still be the wrong rule. Because it was fixed in advance nobody re-reads it, so the mistake stops being arguable and becomes permanent.

**Source note (`.src`):** Illustrative Example — forty seeded scorecards under one constructed screen; every count is computed in the draw function.

### Visualization — canvas `c4`, 720×330

Forty applicants plotted as total against trial-task mark, with the screen drawn as a horizontal line and the casualties below it.

- **Data:** seeded LCG, seed 42; forty candidates × four lines, each `5 + floor(rng()·16)`. Totals run 35 to 65; trial-task marks run 5 to 20.
- **Computed:** the screen "trial task ≥ 13" keeps **15** and cuts **25**. The best total in the pool is **65** with a trial mark of **10**, so it is cut. The best surviving total is **61**, a gap of **4**. Of the ten highest totals, **5** are cut. All scanned in the draw function.
- **Title (bold 15px `P.ink`, centered, y=22):** "Forty Applicants, One Screen Applied First"
- **Axes:** x = total 32…68 across `PX=54 … w−202`; y = trial-task mark 4…21, top `y=48`, baseline `h−58`. Faint `P.grid` verticals every 8. Axis labels 12px `P.mute`: "total across all four lines" under the x axis, "trial-task mark" rotated at the left, y ticks every 5.
- **Screen line:** 2px `P.orange` horizontal at 13 with a bold 12px `P.orange` label "screen: 13" just above its left end; the band below it filled `rgba(107,114,128,0.07)`.
- **Dots:** radius 5. Kept `rgba(217,89,38,0.55)` stroked `P.orange`; cut `rgba(107,114,128,0.30)` stroked `P.mute`. The best total in the pool is ringed 2.5px `#e74c3c` with a bold 12px `#e74c3c` label "strongest overall — cut" to its left; the best survivor is ringed 2.5px `P.orange`, labelled bold 12px "best kept — 61".
- **Side panel** at `w−172`: bold 13px `P.ink` "WHAT THE SCREEN DID", bold 19px `P.orange` "15" + 12px `P.mute` "cards read", bold 19px `P.mute` "25" + 12px "never opened"; then bold 13px `P.ink` "COST AT THE TOP", bold 19px `#e74c3c` "5" beside 12px `P.mute` "of the ten strongest / overall, cut", and bold 12px `P.mute` "best total lost:" over "65 → 61".
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "Applied to everyone alike, and still the wrong line to disqualify on."

---

## Section 5 — When a One-Point Lead Is Not a Finding

**Tags:** `how big a lead counts` (green), `over-trusting the rule` (red), `a second panel` (aqua)

**Bullets:**
- **The output** — F on 53, B on 52, and the rule names F without a flicker of hesitation
- **What that lead is** — one point in fifty-three, less than a single mark on a single line
- **The test** — hand the same eight people to an equally careful panel using the same scorecard
- **How much marks wander** — up to two points a line either way, with no favouritism in it
- **How often F still comes first** — fifty-six times in a hundred
- **How often the others do** — B takes it thirty-three times, G ten, and two more occasionally
- **So the rule decided** — it did not discover; a coin weighted five to four looks just like this
- **What the rule did earn** — the margin is on paper, so anyone can see it was one point
- **When it is a finding** — when the gap survives marks wandering, not merely when it shows

**Key point:** The rule's job was to make the decision auditable, not to make it certain. Reading a one-point win as a fact about the candidates is the procedure being over-trusted — the honest report is that two people are indistinguishable and one of them had to be picked.

**Source note (`.src`):** Illustrative Example — the same eight seeded scorecards, re-marked 8,000 times with seeded wander; every share is computed in the draw function.

### Visualization — canvas `c5`, 720×340

The eight totals on one line with the top margin bracketed, above how often each candidate comes first when the same scorecard is marked again.

- **Data:** the same eight seeded scorecards. Margin between first and second = **53 − 52 = 1**.
- **Re-marking:** 8,000 seeded re-scorings on a separate LCG stream, each line shifted by `(rng()·2 − 1)·2` — continuous, so no ties. Shares of coming first: **F 56%**, **B 33%**, **G 10%**, **D under 1%**, **C under 1%**, and A, E, H never. Stable to within a point across 2,000 / 4,000 / 8,000 / 16,000 trials.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Point Apart, Then Marked Again"
- **Top strip:** one axis, totals 34…56, from `AX=64` to `w−64` at y=78, ticks every 4 in 12px `P.mute` with "total as the card was actually marked" centred beneath. Each candidate a filled circle radius 7 above the axis, letter in bold 12px white — first place `P.green`, second `#e74c3c`, the rest `P.mute`.
- **Margin bracket:** 2.5px `#e74c3c` bracket spanning the top two positions with bold 12px `#e74c3c` "1 point" printed above it, the gap computed from the totals.
- **Bottom block:** bold 13px `P.ink` header "HOW OFTEN EACH COMES FIRST WHEN THE SAME CARD IS MARKED AGAIN" at y=140. Horizontal bars on a 21px pitch from y=164, height 14, track `rgba(107,114,128,0.12)`, lengths scaled to the largest share. Rows ordered by as-scored total, one per candidate that ever came first. The as-scored winner `rgba(0,131,0,0.45)`/`P.green`; the runner-up `rgba(231,76,60,0.40)`/`#e74c3c`; the rest `rgba(107,114,128,0.30)`/`P.mute`. Row label 12px `P.mute` "F  (53)" style; shares under 0.95% print "under 1%" rather than a rounded zero.
- **Note lines** under the bars, 12px `P.mute`: "3 of the eight never came first in 8,000 re-markings" and "same eight people, same four lines, marks allowed to wander 2 points" — the count from the tally.
- **Caption (bold 13px `P.green`, centered, `h−10`):** "A one-point lead is a decision the rule had to make, not a fact it found."

---

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching `cognitive-biases/05-clustering-illusion.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center`, the canvas `display: block; width: 100%; margin: 0 auto`, capped at 720px so a wide cell leaves slack either side.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note. No paragraph blocks, no data tables, no `.example` line restating a bullet.
- **Bullet form:** one line that does not wrap at 50% column width (≤95 characters including the bold label).
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` `#1a5276`. `.key-point` `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06c00`.
- **Hue family per section:** 1 violet with a magenta spread, 2 blue/aqua, 3 magenta/violet, 4 orange with a red alarm, 5 green against red. No section repeats another's dominant pair.
- **Canvas:** intrinsic `width="720"` plus per-chart height (330, 340, 340, 330, 340). `setup(id)` caches the logical size in `dataset` on first call, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12–13px; body and axis labels 12px floor; big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` reserved for the two genuine alarms (the cut top applicant, the one-point margin).
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42. The eight-candidate scorecard is rebuilt by a shared `buildCards(n)` helper so sections 2, 3 and 5 describe one dataset; changing its generation order silently breaks all three.
- **Every printed figure is computed in its draw function** — trip totals and averages from the plotted paths, winners from weighted sums, crown shares from a 255-rule sweep, kept and cut counts from a scan, re-marking shares from a tally.
- **Corrections applied to the previous version of this page:** it claimed automation removes bias because "the pipeline doesn't have a hypothesis, it doesn't want any particular result". That is false as stated — whoever writes the rule, picks the candidate list, sets the threshold and decides what gets measured has already made every biased choice, only earlier and less visibly. Sections 3 and 4 now show a rule encoding the very preference it was meant to block, and section 5 shows the failure mode of over-trusting the output. The old "six biases blocked" bar chart printed asserted "directional" percentages (85%, 70%, 60%, 55%, 90%, 65%) with no data behind them and has been dropped, as have the two data-free box-and-arrow diagrams (the firewall wall, the four-box manual-versus-pipeline flow).
