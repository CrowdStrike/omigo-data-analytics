# Clustering Illusion — Viz

**Page type:** detail page — card-section template
**HTML title tag:** Clustering Illusion — Cognitive Biases
**Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`
**Source note wording:** the sibling `.txt.md` says figures are "computed at render time"; the html `.src` note in section 2 says "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed or the construction invalidates that prose — re-read the computed values and update the text to match. The prose quotes, by section: §1 the fullest square's 10, the 4.0-per-square average and the 2.5× multiple; §2 the 29 heads, the longest run of 6, and the run chances 31% (k=6) and 83% (k=4); §3 the mean of 4.0 repeats, the shown strip's 4 repeats, and the 2% zero-repeat bin; §4 the 0.4 average, 66 empty streets, worst street of 3, and the worst-street tail shares 56% (3+), 6% (4+), 0.5% (5+).

**Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42. Cell counts, run lengths, repeat counts, and every percentage are computed in the draw function and printed from those variables, so a label cannot drift from the plotted data.

---

## 1. Random Dots Land in Clumps

**Tag colors:** `core idea` blue, `lumps and voids` violet, `nothing caused it` magenta
**Hue family:** violet/blue with a magenta void

### canvas `c1` — 720×330

A hundred seeded dots over a faint five-by-five grid, the crowded square and the empty one called out, with the counts printed from the tally.

- **Data:** seeded Park–Miller LCG, seed 42; 100 points, `x = rng()`, `y = rng()`. Binned into a 5×5 grid: the fullest square holds 10, one square holds 0, average 4.0. All read off the tally in the draw function.
- **Title (bold 15px `P.ink`, centered, y=22):** "A Hundred Dots, Dropped at Random"
- **Plot box:** square, `PY=44`, side `= min(h − PY − 58, 0.62w)`, left-aligned at `PX=44`.
- **Grid:** 5×5 `P.grid` lines, 1px. The fullest square filled `rgba(74,58,167,0.10)` and stroked 2px `P.violet`; the empty square stroked 2px dashed `P.magenta` (dash 4/3).
- **Dots:** radius 4, `rgba(42,120,214,0.55)` stroked `P.blue` 1px. Dots inside the fullest square get `rgba(74,58,167,0.65)` stroked `P.violet` so the knot reads as one group.
- **Callouts:** bold 12px `P.violet` "10 dots here" with a leader line to the fullest square; bold 12px `P.magenta` "none at all" with a leader to the empty one. Both positioned from the tally, both nudged outside the plot box.
- **Side panel** at `PX + side + 26`: bold 13px `P.ink` "IF THEY SPREAD OUT EVENLY", then bold 19px `P.mute` "4.0" + 12px "dots per square"; bold 13px `P.ink` "WHAT CHANCE ACTUALLY DID", then bold 19px `P.violet` "10" + 12px "in the fullest square" and bold 19px `P.magenta` "0" + 12px "in the emptiest". The multiple ("2.5× its share") printed beneath, computed as fullest ÷ average.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "The crowded corner is the ordinary outcome, not the story."

---

## 2. Six Shots in a Row

**Tag colors:** `streaks` blue, `hot hand` violet, `already expected` magenta
**Hue family:** violet strip with magenta/aqua bars

### canvas `c1b` — 720×320

The fifty tosses as a strip with the longest run boxed, above exact chances for each run length.

- **Data:** seeded LCG, seed 42; 50 draws, heads if `rng() < 0.5`. Yields 29 heads and a longest head-run of 6 starting at index 13, found by a scan.
- **Exact chances:** `pRun(50, k)` — DP over the current run length with one absorbing state. 98% for k=3, 83% for 4, 55% for 5, 31% for 6, 17% for 7, 8% for 8.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Fair Coin, Fifty Tosses"
- **Toss strip:** 50 cells across `x = 42 … w−30` at `y=56`, height 22. Heads `rgba(74,58,167,0.50)` stroked `P.violet`; tails `rgba(107,114,128,0.18)` stroked `#dcdfe4`.
- **Run box:** 2.5px `P.magenta` rectangle inset 3px around the longest run, bold 12px `P.magenta` "longest run: 6 heads" centered above it. Both driven by the scan.
- **Bar panel:** header bold 13px `P.ink` "HOW OFTEN FIFTY TOSSES CONTAIN A RUN THAT LONG". Six horizontal bars on a 26px pitch, track `rgba(107,114,128,0.12)`. Shorter runs `rgba(25,158,112,0.45)`/`P.aqua`; the observed length `rgba(213,81,129,0.45)`/`P.magenta` with "← the run we just saw"; longer `rgba(107,114,128,0.30)`/`P.mute`. Percentages printed from the DP.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "The streak is what a fair coin does, not what a hot hand does."

---

## 3. A Shuffle That Feels Broken Is Working

**Tag colors:** `where it bites` aqua, `shuffle` orange, `complaints` blue
**Hue family:** the multi-hue song strip with yellow/aqua bars

### canvas `c2` — 720×320

The seeded playlist as a colored song strip with its repeats bracketed, above the spread of how many repeats a fair draw produces.

- **Construction:** six singers × five songs = 30, Fisher–Yates with the seeded LCG. The strip is the **first** shuffle, containing 4 adjacent same-singer pairs at positions 1, 7, 14, 26 — found by a scan.
- **Spread:** 4,000 seeded shuffles tallied by repeat count; average 4.0 against the exact `29 × (6·5·4)/(30·29) = 4.00`, and 2% with no repeat. Printed from the tally.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Fair Shuffle of Thirty Songs"
- **Song strip:** 30 cells at `y=46`, height 24, each in its singer's colour at 0.5 alpha stroked in the solid hue. Singers cycle `P.blue`, `P.aqua`, `P.violet`, `P.yellow`, `P.magenta`, `P.green` — the strip is the page's one deliberately multi-hue element, since colour *is* the data here.
- **Repeat brackets:** 2.5px `P.aqua` under each adjacent same-singer pair, with bold 12px `P.aqua` "4 back-to-back repeats" beneath, count from the scan.
- **Spread bars:** repeat counts 0–9 as columns. The zero bin `rgba(107,114,128,0.30)`/`P.mute` labelled "expected"; the bin matching the strip `rgba(25,158,112,0.50)`/`P.aqua` labelled "this one"; the rest `rgba(201,133,0,0.40)`/`P.yellow`. Only those two bins get a printed percentage, so the chart does not become a table.
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "Fairness produces the repeats. Removing them is the unfair step."

---

## 4. How Big Must a Lump Be Before It Counts

**Tag colors:** `the boundary` green, `what clears the bar` orange, `common mistake` red
**Hue family:** yellow/orange map with a green verdict

### canvas `c3` — 720×340

A ten-by-ten street map for one seeded month, beside the computed chance that a quiet month produces a lump of each size.

- **Data:** seeded LCG, seed 42; 40 events into 100 cells. The month yields a worst street of 3, 66 streets with nothing, average 0.4. Read off the tally.
- **Spread:** 4,000 seeded months recording the worst street; chance reaches 2+ always, 3+ in 56% of months, 4+ in 6%, 5+ in 0.5%. Stable across 2,000 / 4,000 / 8,000 / 16,000 trials.
- **Title (bold 15px `P.ink`, centered, y=22):** "Forty Break-Ins, One Hundred Streets, One Month"
- **Map:** 10×10 cells at `PX=44`, `PY=48`, cell `= min(24, (0.52w)/10)`. Empty streets `rgba(107,114,128,0.10)` stroked `#e5e9ef`; one event `rgba(201,133,0,0.40)` stroked `P.yellow`; two `rgba(217,89,38,0.45)` stroked `P.orange`; the worst street `rgba(217,89,38,0.70)` stroked 2.5px `P.orange` with its count printed bold 12px white inside.
- **Map legend (12px `P.mute`, under the map):** "each square is one street — colour is how many break-ins"; then bold 12px `P.orange` "worst street: 3" and 12px `P.mute` "66 streets had none", both from the tally.
- **Chance panel** at `0.60w`: bold 13px `P.ink` "IN A MONTH WITH NOTHING WRONG, HOW OFTEN CHANCE GIVES SOME STREET —", then four rows on a 34px pitch. Each row: bold 19px figure, 12px `P.mute` label ("three or more", "four or more", "five or more"), and a plain-language frequency ("about every other month", "about once in 16 months", "about once in 200 months") computed as `1 / p`. Rows above the bar in `P.mute`, the row where chance runs out in `P.green`.
- **Verdict strip** under the chance panel: bold 12px `P.green` "this month's worst street clears nothing" — the comparison stated from the tally, not asserted.
- **Caption (bold 13px `P.green`, centered, `h−9`):** "Compare the lump to chance's biggest lump, not to the average."

---

## Page-specific constraints

- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. The canvas is capped at 720px, so a wide cell leaves slack — centering puts the chart in the middle of the right half.
- **`.src` note only where the figures are constructed.** Section 1 carries none; sections 2–4 do. No paragraph blocks, no data tables.
- **Bullets: count follows the content, never a quota.** Six where six covers it, eight where the mechanism needs eight. Do not pad a section to reach a number, and do not add an `.example` line that restates a bullet — if the bullets already carry the fact, the section ends at the key point.
- **Section titles:** name the content. No role labels, no phrasing reused from other pages.
- **Extra tag-pill classes.** Beyond the four base classes this page adds `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d` — so a section's pills match its chart's dominant hue.
- **Colour variety across sections is a requirement, not a preference.** Each section owns a hue family and its pills, chart fills, and caption all sit in it: section 1 violet/blue with a magenta void, section 2 violet strip with magenta/aqua bars, section 3 the multi-hue song strip with yellow/aqua bars, section 4 yellow/orange map with a green verdict. Do not let blue-fill-plus-orange-highlight become every chart.
- **The lead chart must show a visible clump.** The page is about clustering; if no chart contains a knot of points the reader can see, the page has not made its case. An earlier draft opened on a distribution of a maximum — second-order, and it showed no cluster at all.
- **Corrections applied to earlier versions of this page:** a histogram highlighted a bin 3.05 standard deviations above expected (72 against 51.3, reached by chance only ~1.2% of the time) while captioning it "within random variance" — it demonstrated the opposite of its lesson. A birthday-month example has been dropped. A "busiest of ten support areas" section was replaced by the street map, which shows the lump instead of describing it.
- **Canvas id note:** section 2's canvas is `c1b`, not `c2` — the ids run `c1`, `c1b`, `c2`, `c3`. Preserve them.
