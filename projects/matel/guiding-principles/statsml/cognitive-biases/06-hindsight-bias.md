# Hindsight Bias: Memory Rewrites Itself to Match the Answer

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Hindsight Bias — Cognitive Biases

**Subtitle:** Learn how it turned out and your memory of what you expected quietly slides toward it. The slide leaves no trace.

---

## Section 1 — What People Said Beforehand, and What They Remember Saying

**Tags:** `core idea` (violet), `memory drifts` (blue), `no one notices` (magenta)

**Bullets:**
- **The setup** — ten calls made in advance, each written down as a percentage on the day
- **What happened next** — six of the ten came true, four did not, and everyone was told which
- **The second question** — weeks later, each person is asked what they had said at the time
- **The drift** — nine of the ten remembered numbers had moved toward the result they now knew
- **How far** — on average the remembered number sits twelve points nearer the known answer
- **The six that came true** — went in at forty-eight on average, remembered as sixty-two
- **The four that did not** — went in at forty-nine, remembered as thirty-nine, same direction
- **Nobody lied** — the remembered number feels like the original, so the shift is invisible

**Key point:** The remembered number is not a memory being reported, it is a number being reconstructed with the answer already in hand. It arrives feeling exactly as reliable as a real memory.

**Source note (`.src`):** Illustrative Example — ten seeded forecasts; every printed figure is computed from the plotted pairs.

### Visualization — canvas `c1`, 720×330

Ten slope lines. Each case is a pair of dots on two vertical scales — "said beforehand" on the left, "remembers saying" on the right — joined by a line, coloured by which way the outcome went. The systematic fan is the picture: lines for outcomes that happened rise, lines for outcomes that did not fall.

- **Data construction:** seeded Park–Miller LCG, seed 42. For each of ten cases, in this exact draw order: `before = 30 + rng()*40`; `happened = rng() < 0.5`; `pull = -0.15 + rng()*0.85`; `recalled = before + pull * ((happened ? 100 : 0) - before)`. The `pull` term is what the bias does — it drags the remembered number a fraction of the way to the known answer, and the one negative draw gives a case that drifts the wrong way, so the chart is not uniform by construction.
- **Resulting pairs** (before → recalled, rounded as drawn): 1 didn't 30→16, 2 happened 41→42, 3 didn't 69→48, 4 happened 40→73, 5 happened 66→68, 6 happened 40→47, 7 didn't 34→24, 8 didn't 64→69, 9 happened 52→80, 10 happened 49→61.
- **Computed in the draw function:** 6 happened / 4 didn't; 9 of 10 lines move toward the known result (case 8 is the one that moves away); mean signed move toward the outcome 12 points; happened group mean 48 → 62; didn't group mean 49 → 39. Every printed number is read from the array, none typed in.
- **Title (bold 15px `P.ink`, centered, y=22):** "Ten Calls, Written Down Then Remembered"
- **Geometry:** two vertical axes at `x = 110` and `x = 340`, running `y = 62` (100%) to `y = 246` (0%), each 1px `P.grid`, with tick labels 12px `P.mute` at 0 / 50 / 100 on the left axis only. Column headers bold 13px `P.ink` centered above each axis: "SAID BEFOREHAND" and "REMEMBERS SAYING".
- **Slope lines:** 2px, `P.violet` at 0.55 alpha where the outcome happened, `P.magenta` at 0.55 alpha where it did not. End dots radius 4.5, filled in the same hue at 0.75 alpha, stroked in the solid hue 1px.
- **Group means:** a 2.5px `P.violet` line and a 2.5px `P.magenta` line drawn between the two group mean positions, each labelled at its right end bold 12px in its own hue: "happened 48→62" and "did not 49→39". These are the only labels attached to lines, and both numbers come from the group averages.
- **Direction band:** `rgba(74,58,167,0.07)` behind the upper half and `rgba(213,81,129,0.07)` behind the lower half of the right axis only (a 68px-wide strip), so the fan's two destinations read at a glance.
- **Right panel** at `x = 480`: bold 13px `P.ink` "MOVED TOWARD THE KNOWN RESULT", then bold 19px `P.violet` "9 of 10" with 12px `P.mute` "cases" beside it; bold 13px `P.ink` "AVERAGE SIZE OF THE MOVE", then bold 19px `P.violet` "12" with 12px `P.mute` "points closer" beside it; then 12px `P.mute` "one case drifted the other way" as the honest footnote.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Same person, same case — the number moved, and the move felt like remembering."

---

## Section 2 — The Seven Endings the Final Whistle Deleted

**Tags:** `why it feels inevitable` (aqua), `paths not taken` (orange), `the survivor` (mute)

**Bullets:**
- **The whistle blows** — one goal settles it, and that ending now feels inevitable
- **Rewind to kickoff** — three moments were still to come, each able to go either way
- **Count the endings** — that leaves eight possible final scores, none of them settled yet
- **Each one's share** — about one in eight, so the ending you got was never the favourite
- **What the result does** — it deletes the other seven, which leave nothing behind to look at
- **Why it feels certain** — the one surviving path is the only evidence still in the room
- **How to undo it** — name two endings that nearly happened before calling the result obvious

**Key point:** Inevitability is a trick of the light. The path that happened is fully visible and the seven that did not are gone, so the survivor looks like the only route there ever was.

**Source note (`.src`):** Illustrative Example — a constructed three-moment match; the branch counts and shares are computed from the tree.

### Visualization — canvas `c2`, 720×320

A binary tree of three decisive moments fanning left to right into eight endings. The single path that happened is drawn solid and dark; the seven that did not are drawn faint and grey — visibly present, so the reader sees what the memory throws away.

- **Data construction:** three moments, each a two-way branch, giving `2³ = 8` leaves. Leaf index bits `[m>>2 & 1, m>>1 & 1, m & 1]` map to: moment 1 = home scores, moment 2 = away scores, moment 3 = home scores again. So leaf `home = bit0 + bit2`, `away = bit1`. The path that happened is drawn from the seeded LCG (seed 42), three draws of `rng() < 0.5`, giving bits `1,0,0` → leaf index 4 → final score 1–0.
- **Computed in the draw function:** 8 endings; one path's share `100/8` printed as "13%"; endings still open after each moment counted from the tree as 8 → 4 → 2 → 1; the number of leaves sharing the final score 1–0 is 2, printed as "25%" under "ENDINGS WITH THAT SCORE". The taken path's leaf, its score, and every leaf label are read off the leaf list, so no printed score is typed in.
- **Title (bold 15px `P.ink`, centered, y=22):** "Three Moments Still to Come, Eight Ways It Can End"
- **Tree geometry:** four columns of nodes at `x = 50, 160, 270, 380`; leaf labels at `x = 394`. Node rows spread evenly over `y = 52 … 248` (`ny(d,k) = TOP + (BOT−TOP)(k+0.5)/2^d`). Edges are straight 1.5px segments. A node at depth `d`, index `k`, is on the taken path iff its `d` bits match `taken` — checked by one `onPath(d,k)` helper so the live path cannot be mis-highlighted.
- **Edges and nodes:** every edge not on the taken path is `rgba(107,114,128,0.30)` with nodes radius 3.5 in `rgba(107,114,128,0.35)`. The 3 edges on the taken path are 3px `P.aqua`, its 4 nodes radius 5 filled `rgba(25,158,112,0.75)` stroked `P.aqua`. Dead branches draw first so the live path sits on top.
- **Leaf labels:** 12px, each printed as its score (e.g. "1-0"), read off the leaf list. Dead leaves `P.mute`; the leaf that happened bold 12px `P.aqua` with "← happened" appended.
- **Moment axis:** 12px `P.mute` labels under the columns at `y = BOT + 20` — "kickoff", "moment 1", "moment 2", "moment 3" — with the count of endings still open printed bold 12px `P.yellow` beneath at `y = BOT + 38`: "8 open", "4 open", "2 open", "1 left", each computed as `LEAVES >> d`.
- **Callout block** at `x = 520`: bold 13px `P.ink` "CHANCE OF THE PATH / THAT HAPPENED", then bold 19px `P.orange` "13%" with 12px `P.mute` "one route in 8" beside it. Below, bold 13px `P.ink` "ENDINGS WITH THAT SCORE", bold 19px `P.yellow` "25%" with 12px `P.mute` "finish 1-0" beside it — so the reader sees the score was likelier than the route that produced it.
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "The seven grey paths were live at kickoff. Only one is left to look at."

---

## Section 3 — Half the File Goes Missing in the Retelling

**Tags:** `how the story tightens` (orange), `signs dropped` (blue), `nothing invented` (mute)

**Bullets:**
- **Before the result** — sixteen signs sat on the table, eight each way, evenly balanced
- **After the result** — the story gets told again, and the retelling is shorter than the file
- **What survives** — seven of the eight signs that agree with the ending make the retelling
- **What drops out** — seven of the eight that pointed elsewhere are left on the floor
- **The new balance** — the retold file runs nearly nine parts in ten toward the ending
- **Nobody edited it** — the fitting details are simply the ones that come to mind first
- **The effect** — the case looks overwhelming, because half the original file is missing

**Key point:** The retelling is not a summary of the evidence, it is the evidence that still fits. A balanced file becomes a lopsided one without a single sign being altered or invented.

**Source note (`.src`):** Illustrative Example — sixteen constructed signs with seeded recall; the shares are tallied from the flags.

### Visualization — canvas `c3`, 720×300

Two stacked rows of sixteen tiles. Top row is the file as it stood before the outcome — eight tiles one way, eight the other, obviously balanced. Bottom row is the retelling, with the dropped tiles reduced to empty grey outlines. The lopsidedness is the picture.

- **Data construction:** eight signs that fit the eventual outcome, eight that point elsewhere. Seeded LCG, seed 42, drawing in this order: first eight draws set the fitting signs' survival, `keep = rng() < 0.85`; next eight set the others', `keep = rng() < 0.20`. Resulting flags: fitting `1 1 1 1 1 1 0 1`, other `0 0 1 0 0 0 0 0`.
- **Computed in the draw function:** before, 8 of 16 fit — printed "50%"; kept 7 fitting and 1 other, 8 tiles retold; the retelling's fitting share `7/8` printed "88%"; 8 of 16 signs dropped. All from the flag arrays.
- **Title (bold 15px `P.ink`, centered, y=22):** "Sixteen Signs, Before the Outcome and in the Retelling"
- **Rows:** two rows of 16 tiles across `x = 130`, width 560, tile height 30. Top row at `y = 62`, bottom at `y = 148`. Row labels right-aligned at `x = 118`, bold 12px `P.ink`: "BEFORE" and "RETOLD". Tiles 0–7 are the fitting signs, 8–15 the ones pointing elsewhere.
- **Tiles:** fitting signs `rgba(217,89,38,0.55)` stroked `P.orange`; other signs `rgba(42,120,214,0.45)` stroked `P.blue`. In the bottom row a dropped tile is drawn as `rgba(107,114,128,0.06)` fill with a 1px dashed `#d8dce2` outline (dash 3/2) — the slot stays, the content is gone.
- **Share bars:** immediately under each row (`y = 96` and `y = 182`) a single 12px-tall bar spanning the same 560px, blue track with the fitting share filled orange from the left, and the fitting percentage printed bold 12px at the split: `50% fits the ending` in `P.orange` to the right of the split on the top bar, `88% fits the ending` in white to the *left* of the split on the bottom bar (the split runs past 65%, so an orange label there would sit on orange).
- **Legend** 12px `P.mute` at `y = 126`: "orange fits the ending — blue points somewhere else".
- **Callout** at `x = 130`: bold 13px `P.ink` "SIGNS LEFT ON THE FLOOR" at `y = 232`, then bold 19px `P.mute` "8" and 12px `P.mute` "of 16 — 7 of them inconvenient" at `y = 258` — the total and the inconvenient count both tallied from the flags.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "Nothing was invented. The half that disagreed simply did not come to mind."

---

## Section 4 — A Review That Grades the Ending Instead of the Call

**Tags:** `the damage` (magenta), `graded on luck` (blue), `careful and punished` (mute)

**Bullets:**
- **The exercise** — sixteen past decisions, graded out of ten by people who know the ending
- **The hidden split** — eight were sound calls on what was known then, eight were reckless
- **Sound and it worked** — averages just under eight out of ten, the top of the board
- **Sound and it failed** — averages under five, level with a reckless call that got lucky
- **The penalty for a bad ending** — three points, whether the call was sound or reckless
- **What three points buys** — exactly the gap between a careful call and a careless one
- **The consequence** — the review rewards luck, and the careful people learn nothing from it

**Key point:** When the ending moves the grade as much as the quality of the reasoning does, the review is scoring dice. A sound call that failed and a reckless call that got lucky come out level.

**Source note (`.src`):** Illustrative Example — sixteen constructed decisions with a seeded grading wobble; every average is computed from the sixteen scores.

### Visualization — canvas `c4`, 720×320

Four groups of graded decisions on **one shared 0–10 scale**, stacked as rows. Each group shows its four grades as dots plus a heavy tick at the group average. Because all four rows share the scale, the two middle rows — a sound call that failed and a reckless call that got lucky — visibly land on the same vertical line.

- **Data construction:** sixteen decisions, `i = 0..15`. `sound = (i % 2 === 0)` gives 8 sound and 8 reckless; `bad = (i % 4 < 2)` gives 8 bad outcomes and 8 good, crossed so each of the four groups holds exactly 4. Score `= (sound ? 7.5 : 4.5) + (bad ? -2.6 : 0.4) + (rng() - 0.5) * 1.2`, clamped to 1..10, seeded LCG seed 42. Quality and outcome are set independently, so any grade gap the ending produces is the reviewers' doing rather than the decision's.
- **Computed in the draw function:** sound + worked out 7.9; sound + went wrong 4.8; reckless + worked out 4.9; reckless + went wrong 1.7. Cost of a bad ending (all good-outcome grades minus all bad-outcome grades) 3.1; value of a sound call (all sound minus all reckless) 3.1. The near-tie at 4.8 against 4.9 is read off those two group averages, not asserted.
- **Title (bold 15px `P.ink`, centered, y=22):** "Sixteen Decisions, Graded by People Who Know How They Ended"
- **Shared scale:** `x = 210` to `x = 510` maps grade 0 to 10. Vertical `P.grid` gridlines every 2 points from `y = 52` to `y = 168`, with 12px `P.mute` tick numbers at `y = 244` and the axis title "grade out of ten" centered at `y = 262`.
- **Rows:** four rows on a 46px pitch from `y = 62`, in order sound/worked out, sound/went wrong, reckless/worked out, reckless/went wrong. Row label right-aligned bold 12px `P.ink` at `x = 196`.
- **Dots:** radius 5 on the row's baseline — `rgba(213,81,129,0.55)` stroked `P.magenta` for the two "went wrong" rows, `rgba(42,120,214,0.50)` stroked `P.blue` for the two "worked out" rows.
- **Group average:** a 3px vertical tick in the row's hue spanning `y ± 15`, with the average printed bold 19px in that hue at `x = 524`.
- **The tie bracket:** a 1.5px dashed `P.mute` line (dash 4/3) joining the two middle rows' average ticks, labelled bold 12px `P.mute` "graded the same" to the right of them. Drawn **only** if the two averages sit within 0.5 of each other, so the annotation cannot outlive the data.
- **Right panel** at `x = 600`: bold 13px `P.ink` "COST OF A / BAD ENDING", bold 19px `P.magenta` "3.1" with 12px `P.mute` "points off"; then bold 13px `P.ink` "VALUE OF A / SOUND CALL", bold 19px `P.blue` "3.1" with 12px `P.mute` "points on".
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The ending shifted the grade as much as the quality of the call did."

---

## Section 5 — What Separates a Real Call from a Rewritten One

**Tags:** `the boundary` (green), `what the note settles` (aqua), `fair criticism` (red)

**Bullets:**
- **The claim** — four people now say they called the outcome, and say it with confidence
- **The check** — pull the note each of them wrote beforehand and read the number on it
- **The verdict** — one note backs the claim; the other three sat close to a coin toss
- **What that does not prove** — the three were not lying, and their calls were not bad calls
- **The record cuts both ways** — two others were sure of something that did not happen
- **Their critics are right** — those two were wrong on information already in front of them
- **The test** — does the objection cite what was knowable then, or only the ending?
- **The one defence** — a dated note, written before, specific enough to be checked later
- **Why nothing else works** — memory cannot audit memory; only the record sits outside it

**Key point:** Hindsight bias does not make every after-the-fact criticism unfair. Some calls really were bad when they were made, and the note proves that too. The line is whether the objection rests on what was knowable at the time or only on how it turned out.

**Source note (`.src`):** Illustrative Example — the same ten seeded forecasts as the opening chart, re-read as claims against notes.

### Visualization — canvas `c5`, 720×340

Four claim rows, each a paired marker: what the person says now against what their dated note says. A vertical bar at 60% is the line for "I called it", and it runs down through a second band below, where the same kind of note works against its own author — two people who were past the bar on something that never happened.

- **Data construction:** the ten seeded forecasts from section 1, rebuilt by the same `forecasts()` function with the identical draw order, so the two charts cannot disagree about what anyone wrote down. A "claim" is a case where the outcome happened and the remembered number is 60 or more. A claim is **backed** when the note also reads 60 or more.
- **Computed in the draw function:** 4 claims — case 4 (now 73, note 40), case 5 (now 68, note 66), case 9 (now 80, note 52), case 10 (now 61, note 49). Backed: 1 of 4, tallied by the same rule that selects the rows. Separately, cases where the outcome did **not** happen but the note reads 60 or more: cases 3 (69) and 8 (64) — 2 of them, also tallied, and the count is spelled out in the band heading via a number-word lookup.
- **Title (bold 15px `P.ink`, centered, y=22):** "Four People Say They Called It — Read the Notes"
- **Scale:** one shared horizontal 0–100 mapping, `x = 146 … 476`, used by both bands. Tick labels 12px `P.mute` at 0 / 50 / 100 drawn once at `y = 240` on a 1px `P.grid` rule at `y = 224`.
- **The bar:** a 2px `P.mute` vertical line at 60 running `y = 52 … 300` — through both bands, so the same standard visibly applies to the claims and to the failures. Labelled bold 12px `P.mute` "the bar for \"I called it\"" at `y = 46`.
- **Band headings:** bold 13px `P.ink` "THEY SAY THEY CALLED IT" at `x = 40, y = 50`; bold 13px `P.ink` "THE SAME NOTES CONVICT TWO PEOPLE" at `x = 40, y = 272`, its number-word taken from `convicted.length`.
- **Legend** at `y = 72`: a hollow `P.magenta` diamond outline with 12px `P.mute` "what they say now", and a filled `rgba(0,131,0,0.75)` dot stroked `P.green` with 12px `P.mute` "what the note says".
- **Claim rows:** four rows on a 30px pitch from `y = 96`. Row label right-aligned 12px `P.mute` at `x = 134` ("case 4" … "case 10"). Per row: a hollow diamond (2px `P.magenta`, 6px half-width) at the remembered number, a filled dot (radius 5.5, `rgba(0,131,0,0.75)` stroked `P.green`) at the note's number, a 1.5px `P.mute` connector between them, and a bold 12px verdict at `x = 488` — `P.green` "note agrees" where backed, `P.magenta` "note says coin toss" where not.
- **Second band** at `y = 292`, under a 1px `P.grid` rule at `y = 254`: the two convicting notes as filled dots in `rgba(231,76,60,0.75)` stroked `#e74c3c` on the same scale, row label 12px `P.mute` "notes", verdict bold 12px `#e74c3c` "sure, and wrong", and 12px `P.mute` "both notes read past the bar, and the outcome never came" at `y = 316`. This is the page's only use of hard red, because it marks a real failure rather than a bias.
- **Stat block** at `x = 520`: bold 13px `P.ink` "CLAIMS THE NOTE BACKS" with bold 19px `P.green` "1 of 4" beneath it; bold 13px `P.ink` "CALLS THE NOTE CONDEMNS" at `y = 254` with bold 19px `#e74c3c` "2" beneath.
- **Caption (bold 13px `P.green`, centered, `h−6`):** "The note is the only witness that was in the room before the answer arrived."

---

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching `05-clustering-illusion.html` in this folder. Five `.card-section` blocks, each an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) plus a `table.layout` with one `<tr>`: `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note. No paragraph blocks, no data tables, no `.example` lines.
- **Bullet form:** each is ONE line that does not wrap at 50% column width (≤95 characters including the bold label) and a complete thought. Counts follow the content: 8, 7, 7, 7, 9.
- **No index number, no nav, no back/home links, no cross-page links of any kind, no `.nav` CSS.**
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, td vertical-align top padding 12px, `.viz-col` centered. canvas `display:block; width:100%; margin:0 auto; border:1px solid #e0e0e0; border-radius:4px`. `ul` 0.92rem margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` `#1a5276`. `.key-point` `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.mute` `rgba(107,114,128,0.14)`/`#5b6472`.
- **Hue family per section, rotated deliberately:** 1 violet/magenta, 2 aqua/yellow/orange, 3 orange/blue, 4 magenta/blue, 5 green/aqua with the page's only hard red `#e74c3c` reserved for the two genuinely bad calls in section 5.
- **Canvas:** intrinsic `width="720"`, heights 330 / 320 / 300 / 320 / 340. Copy `setup(id)`, `lcg(seed)`, the `P` palette, the `__charts` array and the 150ms debounced resize handler verbatim from `05-clustering-illusion.html`. `setup` caches the logical size in `dataset` on first call, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store and `ctx.scale`s back to logical coordinates.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12–13px; labels 12px floor; big callout figure bold 19px; caption bold 13px. Every chart ends with a bold 13px caption stating its takeaway. No tables drawn on canvas.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42 everywhere. Charts `c1` and `c5` rebuild the same ten forecasts with the identical draw order, so the two cannot drift apart.
- **Every printed figure computed in the draw function** from the plotted data — the 9-of-10 count scanned from the pairs, the 12-point average summed from them, the branch counts tallied from the tree, the 88% share from the flags, the four cell averages from the sixteen scores, the 1-of-4 and the 2 from the same rule that selects the rows.
- **The lead chart must show the rewriting happening.** Before-and-after on the same cases, joined, so the fan toward the known answer is a shape on screen rather than a claim in the text. A single before/after pair of bars — the old page's approach — cannot show that the drift is systematic.
- **Section 5 must draw the line, not just defend against the bias.** Hindsight bias is not a blanket excuse; the chart therefore shows the record clearing one claim of foreknowledge *and* condemning two calls that were wrong on information already available. A version that only shows unfounded claims teaches that all after-the-fact criticism is invalid, which is false.
- **Corrections applied to earlier versions of this page:** the old chart `c2` drew a 45% bar beside an 85% bar and labelled the gap "Memory inflates confidence by ~40pp" — all three numbers hardcoded next to hand-drawn bars, computed from nothing. The old `c5` paired "recalled confidence" against "actual prediction accuracy" across eight domains from two hardcoded arrays, and the two quantities are not on the same scale, so the comparison was meaningless as drawn. The old `c1` asserted "0/5 predictions correct" from a literal rank list; the replacement computes its counts. The five old canvases are consolidated into five sections whose figures all derive from two seeded constructions. The pipeline/automation framing (pre-registration, immutable logs, blind holdouts, AUC ranges) is dropped in favour of the analyst-psychology scope and plain language.
