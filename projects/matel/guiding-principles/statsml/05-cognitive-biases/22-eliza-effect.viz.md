# ELIZA Effect — Viz

**Page type:** detail page, card-section (template `06-sectioned-cards-callout`)
**HTML title tag:** ELIZA Effect — Cognitive Biases
**Template:** card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching `05-clustering-illusion.html` and `01-confirmation-bias.html`
**Source note wording:** the sibling `.txt.md` says figures are "tallied / derived / computed at render time"; the html `.src` notes say "in the draw function". Section 3's note reads "computed from those same arrays" in the html, softened to "from those same figures" in the text.

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a literal array or a constant invalidates that prose — counts, shares, middles and multiples must be re-read from the new draw and the text updated to match.

**Determinism:** no `Math.random()` and no generator needed — all five charts run on literal arrays declared once at script scope. `REACH_BOB` / `REACH_AI` are shared by `c1` and `c3`; `TURNS` is shared by `c2` and `c3`. Every tally, share, middle, multiple and formatted tick label is computed inside the draw function from those arrays, so no printed figure can drift from the plotted data. Changing an array changes every label that depends on it.

---

## 1. Warmth That Never Has a Bad Day

**Tag colors:** `core idea` violet, `same reply every time` blue, `reads as caring` magenta
**Hue family:** violet with blue and mute

### canvas `c1` — 720×330

Two strips of twelve squares — Alice's messages to Bob and to the assistant — coloured by what came back, with the warm-reply share counted off the strips.

- **Data (literal arrays, outcome codes `2` warm reply within minutes, `1` short reply, `0` reply next day, `-1` no reply):**
  - `REACH_BOB = [2,2,1,0,2,1,-1,1,2,0,1,2]` → 5 warm, 4 short, 2 next-day, 1 none
  - `REACH_AI  = [2,2,2,2,2,2,2,2,2,2,2,2]` → 12 warm
  - Both length 12. All four tallies and both shares (`5/12 = 42%`, `12/12 = 100%`) are counted in the draw function, never typed.
- **Title (bold 15px `P.ink`, centered, y=22):** "Twelve Times Alice Reached Out, and What Came Back"
- **Strips:** `SX = 170`, `SW = w − 30 − SX`, cell width `SW/12`. Row 1 at `y=54`, row 2 at `y=100`, both height 30, 2px gap between cells.
- **Cell colours:** warm reply `rgba(74,58,167,0.60)` stroked `P.violet`; short reply `rgba(42,120,214,0.35)` stroked `P.blue`; next-day `rgba(107,114,128,0.18)` stroked `#dcdfe4`; no reply left unfilled with a 1px dashed `P.mute` stroke (dash 3/3) and a small `P.mute` diagonal.
- **Row labels (right-aligned at `SX − 12`):** 12px `P.text` "Alice → Bob" and "Alice → assistant", with a 12px `P.mute` subline under each printing that row's warm count, e.g. "5 of 12 came back warm".
- **Legend (12px `P.mute`, y=162 baseline, starting at `SX`):** four swatches 13×11 in the four cell styles, laid out by measured text width, labelled "warm reply, minutes", "short reply", "reply next day", "no reply".
- **Share panel:** header bold 13px `P.ink` at `SX`, y=194, "WARM REPLY, BACK WITHIN MINUTES". Two horizontal bars, `BX = SX`, `BW = w − 210 − BX`, height 20, pitch 32, starting y=206. Bob's bar `rgba(42,120,214,0.40)`/`P.blue`, the assistant's `rgba(74,58,167,0.55)`/`P.violet`, each on an `rgba(107,114,128,0.12)` full-width track. Value printed bold 12px in the bar hue just right of the bar as `n + ' of 12'` then a 12px `P.mute` percentage — both computed (5 of 12, 42%; 12 of 12, 100%).
- **Variety lines (bold 12px, at `SX`, y=282 and y=300):** `P.blue` "4 different things happened on the top row" and `P.violet` "1 thing happened on the bottom row", where each count is the number of distinct outcome codes present in that array.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "One row varies because a person is on the end of it."

---

## 2. An Hour Where Nobody Else Needs Anything

**Tag colors:** `the whole hour` aqua, `airtime` yellow, `nothing owed back` orange
**Hue family:** aqua with yellow

### canvas `c2` — 720×340

The same sixty sentences twice — once split with Bob, once entirely Alice's — with a gauge underneath each showing how many of her four subjects got aired.

- **Data (literal array of turn lengths, even indexes are Alice):** `TURNS = [7,5,4,6,5,3,8,4,6,5,3,4]`. Computed in the draw function: total 60, Alice 33 (55%), Bob 27. `PER_TOPIC = 15`, so topics aired `= floor(33/15) = 2` with Bob and `floor(60/15) = 4` alone. Nothing is hardcoded as a total.
- **Title (bold 15px `P.ink`, centered, y=22):** "The Same Sixty Sentences, Twice"
- **Geometry:** `SX = 40`, `SW = w − 70`, one sentence `= SW/60`.
- **Row 1** — header bold 13px `P.ink` at `SX`, y=46: "AN HOUR WITH BOB". Strip `y=54`, height 30: walk `TURNS`, drawing each turn as a rectangle whose width is its length; Alice's turns `rgba(25,158,112,0.50)` stroked `P.aqua`, Bob's `rgba(201,133,0,0.45)` stroked `P.yellow`, 1px separations.
- **Row 1 gauge** — `y=94`, height 14, full width `SW` on an `rgba(107,114,128,0.10)` track, filled `rgba(25,158,112,0.50)` to `33 × unit`. Dashed 1px `P.mute` dividers at 15, 30 and 45 sentences. Bold 12px `P.aqua` at y=128: computed "2 of 4 things aired"; right-aligned at `SX + SW` on the same line, 12px `P.mute` "Alice: 33 of 60 sentences, 55% of the hour", both figures computed.
- **Row 2** — header bold 13px `P.ink` at `SX`, y=156: "THE SAME HOUR WITH THE ASSISTANT". Strip `y=164`, height 30, one full-width `rgba(25,158,112,0.50)` rectangle stroked `P.aqua`.
- **Row 2 gauge** — `y=204`, same geometry, filled to `60 × unit`. Bold 12px `P.aqua` at y=238: computed "4 of 4 things aired"; right-aligned on the same line, "Alice: 60 of 60 sentences, 100% of the hour".
- **Legend (12px, y=276 baseline at `SX`):** aqua swatch "Alice talking", yellow swatch "Bob talking", then 12px `P.mute` at y=300 "a subject needs about 15 sentences before it is properly aired", with the 15 printed from `PER_TOPIC`.
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "Emptier of somebody else, so more of it was hers."

---

## 3. Four Things It Reliably Provides, and What Is Not on the List

**Tag colors:** `what it provides` orange, `full marks every row` yellow, `the missing row` blue
**Hue family:** orange with yellow

### canvas `c3` — 720×340

Four paired bars, each against its own ceiling, so the assistant's flush right edge in every row is the visible point — with the rows the comparison cannot hold stated plainly underneath.

- **Rows (each `bob / ai / ceiling`, every numerator taken from the section 1 and 2 arrays where it exists):**
  | Row label | Bob | Assistant | Ceiling |
  |---|---|---|---|
  | Hours of the day it will answer | 11 | 24 | 24 |
  | Messages that came back warm | 5 (counted) | 12 (counted) | 12 |
  | Plans it went along with | 2 | 5 | 5 |
  | Sentences of the hour left to her | 33 (computed) | 60 (computed) | 60 |
- **Computed shares:** 46% / 100%, 42% / 100%, 40% / 100%, 55% / 100%. The count of rows where the assistant equals the ceiling is computed (4 of 4) and printed, not asserted.
- **Title (bold 15px `P.ink`, centered, y=22):** "Four Things Each Side Reliably Provides"
- **Geometry:** `BX = 280`, `BW = w − 200 − BX`. Rows start `TOP = 56` on a 40px pitch. Each row draws an `rgba(107,114,128,0.10)` full-width ceiling track 28px tall, then two sub-bars 12px tall with a 4px gap: Bob's `rgba(201,133,0,0.45)` stroked `P.yellow` on top, the assistant's `rgba(217,89,38,0.50)` stroked `P.orange` below.
- **Sub-bar key (bold 12px, y=44):** `P.yellow` "upper bar: Bob" right-aligned at `BX − 12`, `P.orange` "lower bar: the assistant" left-aligned at `BX` — stated once, since the order never varies.
- **Row labels:** 12px `P.text`, right-aligned at `BX − 12`, on two lines.
- **Value labels (bold 12px in the sub-bar's hue, just right of each sub-bar):** computed `n + ' of ' + ceiling`. Where the bar reaches the ceiling the label is followed by bold 12px in the same hue "at the ceiling", positioned from the measured width of the value label.
- **Ceiling rule:** 1.5px `P.orange` vertical line at `BX + BW` running the height of the four rows, so four flush right edges read as one line. Beneath the rows, bold 12px `P.orange` at `BX`, y=230: computed "at the ceiling on 4 of the 4 rows".
- **Footer band:** 1px `P.grid` rule at y=244, then bold 13px `P.ink` at x=40, y=262 "WHAT NONE OF THESE FOUR ROWS MEASURES", then two 12px `P.mute` lines: "whether anyone will remember this conversation tomorrow" and "whether anyone was changed by having it".
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "One side is at the ceiling on every row it was picked for."

---

## 4. The Middle Wait Slides and a Friend Starts Reading as Slow

**Tag colors:** `the reset` magenta, `same replies` violet, `reads as avoidance` red
**Hue family:** magenta with a violet marker

### canvas `c4` — 720×340

Twenty reply waits as dots on one squashed time axis, drawn twice — before and after twelve of them move to something that answers instantly — with the middle marker sliding left and the four dots that changed sides ringed.

- **Data (literal array, minutes):** `WAITS = [15,240,45,1440,90,20,600,120,35,180,300,60,25,720,150,40,480,75,200,55]`. `INSTANT = 0.1` minutes, `MOVED = 12` (the first twelve indexes).
- **Computed in the draw function:** middle of the twenty as they were `= 105` min, printed by a formatter as "1 h 45 m"; middle after twelve become instant `= 0.1` min, printed "6 sec". Of the 8 waits still with people, those faster than the old middle `= 4` (25, 40, 75, 55 min) and those faster than the new middle `= 0`. Every one of these is derived, including the tick labels.
- **Title (bold 15px `P.ink`, centered, y=22):** "Twenty Replies Alice Waited On"
- **Squashed time axis:** `AX = 152`, `AW = w − 44 − AX`; a wait at `v` minutes sits at `AX + (ln v − ln 0.06)/(ln 2200 − ln 0.06) × AW`, so seconds and days fit on one line. Axis rule 1px `#ccc` at `AXY = h − 80`, ticks at 0.1, 1, 10, 60, 240 and 1440 minutes with labels from the shared `dur()` formatter ("6 sec", "1 min", "10 min", "1 h", "4 h", "1 day"), 12px `P.mute`. Axis title 12px `P.mute`, centered: "time from asking to answered".
- **Row 1** — label bold 12px `P.ink`, right-aligned at `AX − 10`, "AS IT WAS" over 12px `P.mute` "all 20 with people" (count computed). Twenty dots at `y=78`, radius 5, `rgba(213,81,129,0.60)` stroked `P.magenta`, vertical offset `((i % 3) − 1) × 7` so overlaps stay visible.
- **Row 2** — label bold 12px `P.ink` "AS IT IS NOW" over 12px `P.mute` "12 moved across". Dots at `y=180`: the twelve moved ones at `INSTANT` in `rgba(107,114,128,0.35)` stroked `P.mute`; the eight kept at their original values in magenta. The four now behind the new middle but ahead of the old one get a 2px `P.violet` ring — the ring condition is `v > medAfter && v < medBefore`, evaluated per dot.
- **Middle markers:** 2.5px vertical `P.magenta` line through row 1 at the old middle, 2.5px `P.violet` line through row 2 at the new middle, each labelled bold 12px in its own hue with the formatted value — "middle wait 1 h 45 m" and "middle wait 6 sec".
- **Shift arrow:** 2px dashed `P.violet` (dash 5/4) horizontal arrow at `y=134` running from the old marker to the new one, with bold 12px `P.violet` above it: "the line moved this far".
- **Crossing note:** bold 12px `P.violet` at `AX`, y=226, computed "4 of the 8 replies still with people used to beat the middle"; beneath it 12px `P.mute` "none of them do now — every timing is unchanged", printed only when the new-middle count is zero.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** computed — "8 waits unchanged to the minute, 4 of them now on the wrong side."

---

## 5. Who the Endless Patience Is Genuinely Worth Most To

**Tag colors:** `real worth` green, `who gains` aqua, `where it turns` orange
**Hue family:** green with mute

### canvas `c5` — 720×330

The same fourteen evenings for two people, empty slots shown as what the assistant fills — the same assistant, four times the fill for the one who started with less.

- **Data (literal arrays, `1` = someone was free that evening):**
  - `EV_ALICE = [1,0,0,1,0,0,1,0,1,0,0,1,0,1]` → 6 free, 8 empty
  - `EV_DANA  = [1,1,0,1,1,1,1,0,1,1,1,1,1,1]` → 12 free, 2 empty
  - Both length 14. Free counts, empty counts and the multiple `8/2 = 4` are all computed in the draw function.
- **Title (bold 15px `P.ink`, centered, y=22):** "Fourteen Evenings, Two People, One Assistant"
- **Strips:** `SX = 150`, `SW = w − 200 − SX`, cell width `SW/14`. Row 1 (Alice) `y=58`, row 2 (Dana) `y=110`, both height 30, 2px separations.
- **Cell colours:** someone free `rgba(0,131,0,0.45)` stroked `P.green`; nobody free `rgba(107,114,128,0.08)` with a 1.5px dashed `P.green` stroke (dash 4/3) — an evening the assistant fills.
- **Row labels (right-aligned at `SX − 12`):** 12px `P.text` "Alice" / "Dana", each over a 12px `P.mute` subline printing that row's computed free count, e.g. "6 evenings with someone free".
- **Right panel** at `SX + SW + 24`: bold 13px `P.ink` "EVENINGS THE" / "ASSISTANT FILLS", then bold 19px `P.green` "8" with 12px `P.mute` "for Alice", and bold 19px `P.mute` "2" with 12px `P.mute` "for Dana"; beneath, bold 12px `P.green` printing the computed multiple as "4× as much" / "use to Alice".
- **Legend (12px, y=200 baseline at `SX`):** green swatch "someone was free", dashed-green swatch "nobody free — the assistant fills it".
- **Boundary band:** 1px `P.grid` rule at y=218, then bold 13px `P.ink` at `SX`, y=242: "WHAT THE FILLED SQUARES DO NOT SETTLE"; two 12px `P.mute` lines at y=262 and y=280: "the green squares were never competing on being available" and "an evening that took planning is not a worse evening".
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Real worth, largest where there was least — and still not the yardstick."

---

## Page-specific constraints

- **No index number** anywhere on the page: not in the `<h1>`, not in the `<h2>`s.
- **Every section carries a `.src` note**, because every section on this page is constructed. No paragraph blocks, no `.math-box`, no `.example` line, no `.good-pattern` box, no data tables.
- **Bullet count follows the content** — eight per section here because the mechanism needs both sides of each comparison stated.
- **Colour variety across sections is a requirement.** Each section owns a hue family and its pills, chart fills and caption sit in it: section 1 violet with blue and mute, section 2 aqua with yellow, section 3 orange with yellow, section 4 magenta with a violet marker, section 5 green with mute. No chart is blue-fill-plus-orange-highlight.
- **Canvas heights** 330 / 340 / 340 / 340 / 330 — per-section, not uniform.
- **Hard red `#e74c3c`** appears only in the `.key-point` left border, never in a chart.
- **Shared arrays across charts:** `REACH_BOB` / `REACH_AI` feed both `c1` and `c3`; `TURNS` feeds both `c2` and `c3`. Declare once at script scope; do not duplicate literals per chart.
- **Bespoke helper:** a shared `dur()` duration formatter turns minutes into "6 sec" / "1 min" / "1 h 45 m" / "1 day". Both middle labels and every axis tick label in `c4` go through it; none are typed.
- **`c4`'s log-ish axis is deliberate** — a wait at `v` minutes maps through `ln v`, so six seconds and one day sit on the same rule. A linear axis hides every short wait at the left edge and the crossing dots become unreadable.
- **Tone constraints for this page:** no real product or company names — "the assistant" throughout, Alice / Bob / Dana for people. No claim about what people in general do, only what happens in the constructed example. No figure is presented as a measurement of real behaviour; where a mechanism would need real research to quantify, it is stated as a mechanism instead. The last section states the genuine worth first and the failure second, so the page does not read as a warning pamphlet.
- **Corrections and changes from the previous version of this page:**
  - The old page framed the subject as analyst review quality — an "agreeable assistant" catching 6 of 40 planted defects against a human's 26 — which is a claim about model reliability rather than about attachment, and invented a defect-catch rate no example can support. Replaced by the availability / patience / agreement / effort comparison the parent subject actually needs.
  - The old section 3 plotted five perceived-harshness readings and printed a fitted slope and a correlation coefficient off them, presenting an invented feeling score as a measurement with a fit. Replaced by the reply-wait chart, where the moving quantity is a middle of twenty timings that a reader can verify by hand and no person's behaviour changes.
  - Old canvases carried banned vocabulary in their labels (correlation coefficient, "10pp" slope units) and 11px caption text below the 12px floor. All in-chart text now sits at the 12px floor or above, and captions are bold 13px.
  - The `.math-box` blocks, the `.example` italic lines, the `.good-pattern` box and the numbered `<h2>`s are gone, per the card-section template.
