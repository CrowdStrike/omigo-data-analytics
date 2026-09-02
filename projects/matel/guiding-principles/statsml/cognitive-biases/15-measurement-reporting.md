# Recall Bias: The Remembering Is Part of the Instrument

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Recall Bias — Cognitive Biases

**Subtitle:** Ask people what happened and you measure their memory too. Knowing how it turned out changes what comes back.

---

## Section 1 — Two Groups, One Set of Logs, Two Different Years

**Tags:** `core idea` (violet), `same history` (blue), `different memory` (magenta)

**Bullets:**
- **The survey** — eighty phone owners asked how often they charged overnight last year
- **The two groups** — forty whose battery later went bad, forty whose battery is still fine
- **The logs** — the charging app counted every night, and the two sets of logs are identical
- **The bad-battery group** — remembers 44 nights in every hundred its own log recorded
- **The other group** — remembers 30 in every hundred, out of the very same kind of year
- **What the survey finds** — overnight charging looks about half again as common in the failures
- **What the logs find** — nothing; both groups averaged the same number of nights on charge
- **Why they differ** — one group searched its year hard, the other just answered the question
- **Not hindsight** — nobody misremembers how sure they were, they misremember what happened

**Key point:** The outcome did not change anyone's history — it changed how hard they looked for it. A person with a broken battery goes back over the year hunting for a cause and finds more of what was always there. The survey then reports the difference in searching as a difference in behaviour.

**Source note (`.src`):** Illustrative Example — one seeded set of forty logged years reused for both groups; every printed figure is computed from the plotted values.

### Visualization — canvas `c1`, 720×340

Three stacked strips over one shared count axis: the logged nights (identical for both groups, drawn once), then what each group recalls out of it. The two recall strips sit under the same axis so the leftward slide of the bad-outcome group is visible as a shift of the whole cloud.

- **Data:** seeded Park–Miller LCG, seed 42; forty logged years, each `2 + floor(rng() × 19)` nights per month on charge, range 2–20, mean **10.5**. This one array is the history of *both* groups — the page's whole point is that the histories are identical, so there is only one of them.
- **Recall model:** a second stream, `lcg(7)`. Each logged night survives into memory independently. The ordinary rate is `p0 = 0.30`. The group whose battery failed searches harder — the same extra effort applied to whatever memory left behind, `p1 = 1 − (1 − p0)^1.8 = 0.474`. Both rates are constants in the draw function; every printed figure is tallied from the resulting arrays.
- **Computed:** logged mean 10.5 nights; bad-battery group recalls mean **4.6** = **44%** of its own log; other group recalls mean **3.1** = **30%**; apparent gap **1.5** nights, apparent ratio **1.48×**; logged gap **0.0**, logged ratio **1.00×**. Person-by-person, the bad-outcome recall exceeds the other in **27** of 40 pairs, falls short in 8, ties in 5 — so it is a lean, not a rule.
- **Title (bold 15px `P.ink`, centered, y=22):** "Forty Logged Years, Remembered Twice"
- **Geometry:** one horizontal count axis 0–21 nights across `x = 150 … w−40`. Three rows on a 74px pitch starting `y=64`: "WHAT THE LOGS SAY" (drawn once, spanning both groups), "BATTERY WENT BAD — RECALLED", "BATTERY STILL FINE — RECALLED". Row labels bold 12px `P.ink`, right-aligned at `x=138`.
- **Marks:** each of the forty values is a dot, radius 4.5, jittered vertically within its row by a deterministic offset from the value's stack position so equal counts spread instead of overprinting. Logs row `rgba(107,114,128,0.45)` stroked `P.mute`; bad-battery row `rgba(213,81,129,0.55)` stroked `P.magenta`; fine row `rgba(42,120,214,0.50)` stroked `P.blue`.
- **Mean markers:** a 3px vertical rule at each row's mean in the row's hue, with the mean printed bold 19px to the right of the axis (`10.5`, `4.6`, `3.1`) and a 12px `P.mute` "nights" beside it.
- **The gap band:** a `rgba(213,81,129,0.10)` rectangle spanning the two recall means, across both recall rows, labelled bold 12px `P.magenta` "the whole gap, and it is all memory" — the width is computed from the two means, not drawn to taste.
- **Bottom callout (12px, left at `x=150`, `h−44` and `h−28`):** bold `P.magenta` "survey says 1.48× as much charging" then bold `P.mute` "logs say 1.00× — no difference at all". Both multiples computed.
- **Caption (bold 13px `P.magenta`, centered, `h−9`):** "Same year on both logs. The group with the bad outcome simply looked harder."

---

## Section 2 — A Flat Year That Remembers Itself as a Climb

**Tags:** `plain fading` (aqua), `looks like a trend` (yellow), `nothing changed` (orange)

**Bullets:**
- **The setting** — thirty people, each eating out four times a month, every month, all year
- **The record** — a flat line, a hundred and twenty outings a month from January to December
- **The question** — asked in December to remember the year, one month at a time
- **Last month** — 92 outings in every hundred come back
- **Twelve months back** — 8 in every hundred come back
- **The shape that appears** — a steady climb, eleven times higher at the end than the start
- **What actually changed** — nothing; fading memory has been plotted as if it were behaviour
- **The whole year** — 39 outings in a hundred recalled, so the level is wrong as well as the trend

**Key point:** A question about last year measures memory quality at least as much as it measures events. Plotting recalled counts against time draws the fade curve of memory and labels it a trend, and the curve slopes the way a growth story is expected to slope.

**Source note (`.src`):** Illustrative Example — a constructed flat year with seeded fading; the recalled totals are tallied in the draw function.

### Visualization — canvas `c2`, 720×320

The flat true line and the fading recalled line on one monthly axis, with the share recovered printed under each month.

- **Data:** thirty people × four outings per month × twelve months. The truth is a constant **120** outings a month — deliberately flat, so any slope on the chart belongs entirely to memory.
- **Fade model:** a night from `m` months ago survives recall with chance `0.90 × exp(−m/5)`. Simulated per outing on a seeded stream, `lcg(11)`.
- **Computed recalled counts, January → December:** 10, 12, 19, 21, 31, 34, 42, 55, 60, 73, 90, 110. As a share of the 120 that happened: 8%, 10%, 16%, 18%, 26%, 28%, 35%, 46%, 50%, 61%, 75%, 92%. Year totals: **1,440** happened, **557** recalled = **39%**. December over January = **11×**.
- **Title (bold 15px `P.ink`, centered, y=22):** "A Flat Year, Recalled in December"
- **Axes:** plot box `x = 60 … w−48`, `y = 52 … 236`. Y scale 0–130 outings, four `P.grid` gridlines with 12px `P.mute` tick labels. X: twelve month slots labelled with initials (J F M A M J J A S O N D) 12px `P.mute` at `y = 254`.
- **True line:** flat 2.5px `P.mute` line at 120 with a dashed segment style (dash 6/4), labelled 12px `P.mute` "what actually happened — 120 every month" just above it.
- **Recalled line:** 3px `P.aqua`, with `rgba(25,158,112,0.18)` fill down to the baseline, and a radius-4 `rgba(25,158,112,0.75)` dot on each month.
- **The missing wedge:** the area between the two lines filled `rgba(201,133,0,0.14)`, with bold 12px `P.yellow` "everything in here happened and was forgotten" placed inside it.
- **Endpoint labels (bold 12px):** `P.aqua` "92% recalled" above December's dot, `P.aqua` "8% recalled" above January's — both computed from the tallies.
- **Right-edge callout (bold 13px `P.ink` header, bold 19px figure):** "SHAPE THE SURVEY DRAWS" then `P.yellow` "11×" and 12px `P.mute` "December over January"; below it "OF THE WHOLE YEAR" then `P.aqua` "39%" and 12px `P.mute` "came back at all".
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "The line slopes because memory fades, and the flat truth is the dashed one."

---

## Section 3 — Six Hundred Answers Land on Seven Numbers

**Tags:** `rounding` (orange), `piles and gaps` (yellow), `cut-offs break` (red)

**Bullets:**
- **The question** — six hundred people asked how many times they used a service last year
- **The records** — spread smoothly across the range, no count more popular than any other
- **The answers** — six hundred people pile onto seven numbers and leave the rest nearly bare
- **On a multiple of ten** — seven answers in ten, against fewer than one record in ten
- **The gaps between** — a handful of counts from eleven to fifty-nine that nobody named
- **The average survives** — a fifth of a visit off, because rounding up cancels rounding down
- **What does not survive** — any cut-off near a round number, since a pile sits on the line
- **Cut at twenty** — "more than twenty" puts 61 in a hundred above it, the records put 68
- **One notch lower** — "more than nineteen" puts 76 above it, from exactly the same answers

**Key point:** Rounding leaves the average almost untouched, which is why it goes unnoticed. It destroys anything that depends on where a boundary falls, because a boundary drawn near a round number decides whether a pile of six hundred people's worth of answers sits above it or below it.

**Source note (`.src`):** Illustrative Example — six hundred seeded true counts and their rounded answers; every share is tallied in the draw function.

### Visualization — canvas `c3`, 720×330

Two histograms back to back over one shared count axis 1–60: the smooth record above, the spiked answers below, with the piles unmissable and the empty counts visible between them.

- **Data:** seeded LCG, seed 42; six hundred true counts drawn `1 + floor(rng() × 60)`, so the record is flat by construction — no count is more likely than any other.
- **Rounding model:** for each person one further draw `u`. If `u < 0.62` the answer rounds to the nearest ten when the true count is 13 or more, otherwise to the nearest five. If `0.62 ≤ u < 0.80` it rounds to the nearest five. Otherwise the exact count is given. A zero is bumped to 1.
- **Computed:** answers landing on a multiple of ten about **70%**, against about **8%** in the records; on any multiple of five about **83%** against about **18%**. The tallest answer pile is roughly **90** people (on 20) against a tallest record pile near **18**. Seven or eight counts between 11 and 59 go unnamed. Mean true **30.2**, mean answered **30.4** — the average survives. Share above a "more than twenty" line: records **68%**, answers **61%**; above "more than nineteen": records **70%**, answers **76%** — the flip is the point, and every one of these is tallied from the two arrays at render time rather than fixed here, since the exact pile heights depend on the draw order.
- **Title (bold 15px `P.ink`, centered, y=22):** "Six Hundred True Counts, and What People Said"
- **Geometry:** shared x axis for counts 1–60 across `x = 52 … w−150`. Records drawn as upward bars from a midline at `y=150` (bar tops rising toward `y=56`); answers drawn as downward bars from the same midline (toward `y=252`). Each side scaled to its own maximum, and the two maxima are printed so the reader is not misled by the independent scaling: 12px `P.mute` "tallest bar: 17 people" above, "tallest bar: 81 people" below.
- **Bars:** records `rgba(107,114,128,0.40)` stroked `P.mute`; answers `rgba(217,89,38,0.55)` stroked `P.orange`, except bars sitting on a multiple of ten which take `rgba(201,133,0,0.75)` stroked `P.yellow` so the piles read as one family.
- **Empty markers:** a small 3px `P.red`-free `P.mute` tick on the axis at each count between 11 and 59 that nobody named, with 12px `P.mute` "8 counts nobody named" labelling them.
- **Axis:** 12px `P.mute` labels at 1, 10, 20, 30, 40, 50, 60 on the midline; row headers bold 12px `P.ink` "WHAT THE RECORDS SAY" (above, left) and "WHAT PEOPLE ANSWERED" (below, left).
- **Right panel** at `w−140`: bold 13px `P.ink` "ON A ROUND TEN", then bold 19px `P.yellow` "66%" and 12px `P.mute` "of answers"; bold 19px `P.mute` "9%" and 12px `P.mute` "of records". Below that bold 13px `P.ink` "AVERAGE", then 12px `P.mute` "30.4 said, 30.2 true".
- **Cut-off strip** at `y=286`: a horizontal bar showing the two cut-offs. Bold 12px `P.orange` "cut at 20 → 61% above, truly 68%" and bold 12px `P.orange` "cut at 19 → 76% above, truly 70%", each figure computed from the two arrays.
- **Caption (bold 13px `P.orange`, centered, `h−9`):** "The average shrugged it off. Every threshold near a round number did not."

---

## Section 4 — Why a Lean Does Not Wash Out and Vagueness Does

**Tags:** `does not cancel` (violet), `more data no help` (blue), `false confidence` (red)

**Bullets:**
- **Two kinds of error** — memory that is merely vague, and memory that leans in one group only
- **The vague kind** — some people round up, some round down, and the two halves cancel out
- **Ten people each side** — vagueness alone opens a two-night gap between identical groups
- **Three thousand each side** — that gap shrinks to a tenth of a night, near enough to nothing
- **The one-sided kind** — the group with the bad outcome recalls more, and most of them do
- **Ten people each side** — a gap of nearly two nights, from histories that match exactly
- **Three thousand each side** — the same two nights, no smaller for all the extra data
- **Why more data cannot help** — a bigger sample pins the lean down precisely, it never removes it
- **How it reads** — as a firm, repeatable finding, which is the worst impression it could give

**Key point:** Sloppy memory is survivable — collect enough of it and the errors cancel. A memory that leans one way in one group is not survivable, because there is nothing for it to cancel against. Collecting more of it makes the false gap look better established, never smaller.

**Source note (`.src`):** Illustrative Example — five hundred seeded repeats at each group size, both error types applied to matched histories.

### Visualization — canvas `c4`, 720×320

Two curves of average false gap against group size, on a log-spaced size axis: vagueness collapsing toward zero, the one-sided lean running flat.

- **Construction:** at each group size `n` in {10, 30, 100, 300, 1000, 3000}, five hundred repeats. Each repeat draws two independent true histories of `n` people (same generator, same distribution, so the truth carries no gap) and applies each error type to both sides, then records the absolute difference of the two recalled means. The plotted value is that absolute difference averaged over the five hundred repeats. Seeded stream `lcg(42)`.
- **Vague memory:** each true count shifted by `round((rng()×2 − 1) × 4)`, floored at zero — symmetric, so it has no direction.
- **One-sided memory:** each true night survives with chance `p0 = 0.30` on one side and `p1 = 1 − (1 − p0)^1.8 = 0.474` on the other — the same two rates as the opening chart, so the page's two constructions cannot disagree.
- **Computed gaps (nights):** vague falls roughly 2.2 → 1.3 → 0.7 → 0.4 → 0.2 → 0.1 across n = 10 … 3000; one-sided stays flat at about 1.9 at every size. Both series are averaged over the repeats in the draw function and printed from those values — the point is the shape (one collapses, one does not), not the third digit.
- **Title (bold 15px `P.ink`, centered, y=22):** "False Gap Between Identical Groups, as the Groups Grow"
- **Axes:** plot box `x = 76 … w−188`, `y = 52 … 244`. Y scale 0 to 2.4 nights with `P.grid` gridlines at 0, 0.5, 1.0, 1.5, 2.0 labelled 12px `P.mute`; rotated 12px `P.mute` "false gap (nights)" on the left. X: the six sizes spaced evenly (each is roughly triple the last), labelled 12px `P.mute` with 12px `P.mute` "people in each group" beneath.
- **Curves:** one-sided 3px `P.violet` with radius-4.5 `rgba(74,58,167,0.75)` dots; vague 3px `P.blue` dashed (dash 6/4) with radius-4.5 `rgba(42,120,214,0.60)` dots. Series labels sit at the right end of each curve, bold 12px in the curve's hue: "leans one way — stays" and "merely vague — cancels".
- **Truth line:** a 1.5px `P.mute` line at zero labelled 12px `P.mute` "no real difference exists".
- **Right panel** at `w−178`: bold 13px `P.ink` "AT THREE THOUSAND EACH SIDE", then bold 19px `P.violet` "1.9" + 12px `P.mute` "nights, one-sided" and bold 19px `P.blue` "0.1" + 12px `P.mute` "nights, vague". Below that bold 13px `P.ink` "WHAT THE LEAN SHRANK BY", then bold 19px `#e74c3c` "0%" and 12px `P.mute` "from ten to three thousand" — the shrinkage computed as the ratio of the two endpoint values, printed as whatever it comes out to.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "One curve is a problem more data fixes. The other is a problem more data hardens."

---

## Section 5 — Which Questions Memory Can Actually Answer

**Tags:** `where it is safe` (green), `recent and distinctive` (aqua), `the routine fails` (red)

**Bullets:**
- **What memory is good at** — one recent event, distinctive enough to have left a mark
- **Did the boiler break last month** — nearly everyone knows, so hard searching adds 4 percent
- **Did you move house this year** — big and dated, so hard searching adds 9 percent
- **How many coffees last month** — routine and repeated, so hard searching adds 26 percent
- **How many meals out this year** — hard searching adds 58 percent, and the answer is unusable
- **The pattern** — the false gap follows how much the question leaves behind, nothing else
- **The usable rule** — below nine tenths recalled by an ordinary person, expect a gap over a tenth
- **So ask** — for a recent, distinctive, consequential event, never for a count of the routine
- **When you need a count** — take it from a record, and leave memory for what no record holds

**Key point:** The size of the false gap is set by how much the question leaves unrecalled, because that leftover is exactly what a motivated search has left to find. A question with nothing left over is safe to ask however badly the person wants an explanation.

**Source note (`.src`):** Illustrative Example — four constructed questions scored on one recall model; the false gap for each is computed, not assigned.

### Visualization — canvas `c5`, 720×330

The false gap plotted against ordinary recall as one curve, with the four questions placed on it and the point where the gap drops under a tenth marked.

- **The rule being shown:** if an ordinary person recalls a share `p0` of what happened, then `1 − p0` is what memory left behind, and that leftover is what a harder search can still recover. Searching harder is modelled as the same effort applied to the leftover: `p1 = 1 − (1 − p0)^1.8`. The false gap between two groups with identical histories is then `p1 / p0 − 1`. It depends on nothing but `p0` — which is why the curve is a single line and the four questions are just four places on it.
- **Computed points:** boiler last month `p0 = 96%` → gap **+4%**; moved house this year `p0 = 90%` → **+9%**; coffees last month `p0 = 70%` → **+26%**; meals out this year `p0 = 30%` → **+58%**. All four `p0` values are stated inputs describing the question type; every gap is computed from the formula in the draw function.
- **Thresholds, solved numerically in the draw function:** the gap falls below a tenth once ordinary recall passes **89%**, and below a twentieth once it passes **95%**.
- **Title (bold 15px `P.ink`, centered, y=22):** "How Big the False Gap Gets, by What the Question Leaves Behind"
- **Axes:** plot box `x = 78 … w−206`, `y = 56 … 246`. X: ordinary recall 20%–100%, ticks every 20%, 12px `P.mute`, labelled "what an ordinary person recalls". Y: false gap 0%–70%, gridlines every 20%, rotated 12px `P.mute` "false gap between identical groups".
- **Curve:** 3px `P.green`, computed at one-point steps across the range. The stretch left of the 89% crossing is drawn `P.red` `#e74c3c` at 3px to mark the unsafe region; right of it stays `P.green`.
- **Crossing marker:** a dashed (5/4) 1.5px `P.green` vertical at 89% with bold 12px `P.green` "gap drops under a tenth here" beside it, the 89 printed from the numeric solve.
- **Question markers:** four radius-6 dots on the curve, `rgba(0,131,0,0.75)`/`P.green` for the two safe ones and `rgba(231,76,60,0.75)`/`#e74c3c` for the two that fail, each labelled 12px in its hue with a short question tag ("boiler, last month", "moved house, this year", "coffees, last month", "meals out, this year") and its gap.
- **Right panel** at `w−196`: bold 13px `P.ink` "SAFE TO ASK", then two 12px `P.green` lines "recent" / "distinctive" / "consequential"; bold 13px `P.ink` "NOT SAFE TO ASK", then 12px `#e74c3c` "routine" / "frequent" / "undistinguished". Below both, bold 19px `P.green` "89%" with 12px `P.mute` "the line, in ordinary recall".
- **Caption (bold 13px `P.green`, centered, `h−9`):** "Memory answers 'did this happen' well. It answers 'how many times' badly."

---

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching `05-clustering-illusion.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center`, the canvas `display: block; width: 100%; margin: 0 auto`, capped at 720px so a wide cell leaves slack around it.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note. No paragraph blocks, no data tables, no `.example` line restating a bullet.
- **Bullet form:** one line that does not wrap at 50% column width — every bullet on this page is ≤93 characters including its label.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` `#1a5276`. `.key-point` `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. Tag pills inline-block 0.72rem weight 600 padding 2px 10px radius 10px. No nav, no `.nav` CSS, no back/home links, no cross-page links of any kind.
- **Tag pill classes used:** `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06a00`.
- **Hue family per section:** 1 magenta/violet against grey logs, 2 aqua/yellow, 3 orange/yellow, 4 violet/blue with a red endpoint figure, 5 green with red for the unsafe stretch. No section repeats another's dominant fill.
- **Canvas:** intrinsic `width="720"` plus the per-chart height (340, 320, 330, 320, 330). `setup(id)` caches the logical size in `dataset` on first call, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart header bold 12–13px; body and axis labels 12px floor; one big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` only where the chart is flagging a real failure (section 4's zero shrinkage, section 5's unsafe stretch).
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`). Seed 42 for the histories, seed 7 for section 1's recall draws, seed 11 for section 2's fading. Every count, share, mean and multiple is tallied inside the draw function from the plotted arrays and printed from that variable.
- **One recall model, two charts.** Sections 1, 4 and 5 all use `p1 = 1 − (1 − p0)^1.8` with `p0 = 0.30`, so the opening story, the sample-size curve and the closing rule are the same construction seen three ways and cannot contradict one another.
- **The distinction from hindsight bias is load-bearing.** That page is about a person misremembering their own past confidence. This page is about people misremembering events and exposures, which corrupts a dataset a third party will later analyse. Section 1's last bullet states the difference explicitly; do not blur it in a rewrite.
- **Corrections and changes from the old page.** This page replaces the "Recall Bias" section of the old five-topic `Measurement & Reporting Biases` document, which is now split one topic per page — observer bias, early termination, publication bias and misclassification each live elsewhere and must not appear here.
  - The old section asserted "cases recall 45% medication use; controls recall 20%; pharmacy records show both groups at 25%" with a chart hardcoding those four bars. Those numbers were plausible-looking but not derived from anything, and the arithmetic did not close: if both groups truly sit at 25%, the recalled figures cannot both be errors *of recall* in opposite directions without a stated mechanism. The construction here fixes that — one shared history, one recall rate per group, and the difference emerges rather than being asserted.
  - The old examples were medical (birth defects, cancer, chemical exposure) and would need real research to support. They are replaced with mundane non-medical ones: overnight phone charging, meals out, service visits, a boiler.
  - The old page had no illustration of rounding or heaping and none of the sample-size argument. Both are new and both are computed.
