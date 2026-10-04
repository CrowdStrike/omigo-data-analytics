# Overconfidence in Small Samples — Viz

**Page type:** detail page, card-section (template `06-sectioned-cards-callout`)
**HTML title tag:** Overconfidence in Small Samples — Cognitive Biases
**Template:** card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the approved conversion in `05-clustering-illusion.html`

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** The sibling carries the page's construction in human-readable form as an unnumbered preamble — the premise (both versions truly at 0.10), the volumes, the race count, and a table of every figure with its arithmetic. This file carries the same construction as code. Changing a seed, the true rates, the per-day volume or the race count invalidates that prose — the reversal share, peak-day shares, average gaps, schedule shares, the two paths' values and every day count must be re-read from the new draw and **both** the sibling's preamble table and its section bullets updated to match.

**Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42 for every stream.

---

## 1. Two Identical Pages, Thirty Days of Watching

**Tag colors:** `core idea` violet, `the early lead` blue, `it reverses` magenta
**Hue family:** violet/blue with a magenta stop marker

### canvas `c1` — 720×340

Six running-total lines over thirty days, each the gap between two identical versions, with the day-three leader marked at the moment it looked best and again where it finished.

- **Shared simulation (`SIM`, computed once, reused by every chart):** the reader-facing construction — two identical versions at a true rate of 0.10, sixty visitors a side per day, thirty days, two thousand races, and the full table of figures that follow from it — lives in the sibling `.txt.md` preamble. This block is its implementation only. Code-level: seeded Park–Miller LCG, seed 42; `DAYS = 30`, `PER = 60`, `RATE = 0.10`, `TRIALS = 2000`, `PEEK = 3`, `SHOW = 6`. `raceGaps(rng, lift)` walks day by day accumulating conversions for A (at `RATE`) and B (at `RATE + lift`) and records `g[d] = 100 × (cb/n − ca/n)` with `n` the running per-side total.
- **Sign alignment:** each race is flipped (`sgn = g[PEEK−1] >= 0 ? 1 : −1`) so the day-3 leader is the positive line, giving `SIM.lead` alongside the unflipped `SIM.raw`. This makes "leader loses the lead" readable as "the line crosses below zero" instead of as two mirrored cases. The sibling states the same thing as a sign convention.
- **Display runs:** the first six aligned races. Their day-3 gaps are +1.1, +8.9, +4.4, **+10.6**, +6.7, +2.2 points; their day-30 gaps are −0.1, +0.4, +2.3, **−1.2**, +1.8, −0.1. All read from the arrays in the draw function.
- **Marked run:** whichever display run has the largest day-3 lead — run index 3, at +10.6 points, finishing at −1.2. Chosen by a scan, not hardcoded.
- **Computed figures:** spread of the six lines at day 3 is 9.4 points, at day 30 it is 3.5 points; across all 2,000 races the day-3 leader is behind on day 30 in **38%**.
- **Title (bold 15px `P.ink`, centered, y=22):** "Two Identical Pages, Thirty Days of Watching"
- **Plot box:** `PX=52`, `PY=44`, right margin 26, bottom `h−58`. y-axis spans −4 to +14 points, ticks every 3 points; x-axis is day 1 to 30 with labels at 1, 5, 10, 15, 20, 25, 30.
- **Grid:** horizontal `P.grid` 1px lines at each tick; the zero line 1.5px `P.mute` (this is "no difference at all"), labelled 12px `P.mute` "dead level" at the right end.
- **Lines:** the five unmarked races 1.5px `rgba(42,120,214,0.45)`. The marked race 2.5px `P.violet`, drawn last so it sits on top.
- **Stop marker:** at day 3 on the marked line, a filled 5px `P.magenta` dot, a dashed 1.5px `P.magenta` vertical line down to the axis (dash 4/3), and bold 12px `P.magenta` "day 3: called it, +10.6 pts" above the dot. The figure printed from `marked[2]`.
- **End marker:** at day 30 on the marked line, a hollow 5px `P.violet` circle with bold 12px `P.violet` "ended at −1.2" placed left of the point so it stays inside the box.
- **Reversal callout** placed inside the plot box, upper right: bold 13px `P.ink` "DAY-3 LEADER, BY DAY 30", then bold 19px `P.magenta` "38%" and 12px `P.mute` "finish behind", the percentage computed from the 2,000-race tally.
- **Axis labels:** bottom center 12px `P.mute` "day of the race"; the y-axis label is folded into the top-left tick note 12px `P.mute` "gap between the two, in points".
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Both versions are the same. Every wobble you see is the count, not the page."

---

## 2. The Day the Gap Looks Biggest Is Day One

**Tag colors:** `timing` aqua, `widest when thinnest` yellow, `peak early` orange
**Hue family:** orange/yellow with an aqua side panel

### canvas `c2` — 720×320

A column per day showing how often that day holds the widest gap of the whole race, with the two average gaps set beside it.

- **Data:** for each of the 2,000 races, find the day with the largest `|gap|`. Tally by day and divide by 2,000. Result: day 1 **50%**, day 2 19%, day 3 10%, then a fast decay; the first three days together **79%**, the first week **94%**, the last week (days 24–30) **0.5%**.
- **Also computed:** mean widest gap across races **5.4 points**; mean day-30 gap **0.8 points**. Both accumulated in the same pass.
- **Title (bold 15px `P.ink`, centered, y=22):** "The Day the Gap Looks Biggest, Across 2,000 Races"
- **Columns:** 30 bars, `PX=50` to `w−212`, baseline `h−56`, tallest bar 132px tall, scaled to the day-1 share. Day 1 `rgba(217,89,38,0.55)` stroked `P.orange` 1.5px; days 2–7 `rgba(201,133,0,0.45)` stroked `P.yellow`; days 8–30 `rgba(107,114,128,0.28)` stroked `P.mute` — the fade is the point.
- **Bar labels:** bold 12px `P.orange` "50%" above the day-1 bar only, printed from the tally, so the chart stays a chart. 12px `P.mute` day numbers under days 1, 5, 10, 15, 20, 25, 30.
- **Bracket:** a 2px `P.yellow` bracket under days 1–3 with bold 12px `P.yellow` "first three days: 79%" beneath, both ends and the figure from the tally.
- **Baseline:** 1px `#ccc`, with 12px `P.mute` "day of the race" centered under it.
- **Side panel** at `w−196`: bold 13px `P.ink` "AVERAGE GAP", then bold 19px `P.orange` "5.4" + 12px `P.mute` "points at its widest", then bold 19px `P.aqua` "0.8" + 12px `P.mute` "points on day 30", then bold 12px `P.aqua` "about 7× smaller" computed as the rounded ratio.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "The result peaks while the evidence is thinnest, then quietly deflates."

---

## 3. Every Extra Look Is Another Chance to Be Fooled

**Tag colors:** `stopping early` magenta, `checking often` violet, `false alarm` red
**Hue family:** magenta/violet against an aqua baseline

### canvas `c3` — 720×320

Five horizontal bars, one per checking schedule, showing how often a race between two identical versions produces a gap wide enough to call — with the intended one-in-twenty marked.

- **The bar being cleared:** at day `d` each side has `n = d × 60` visitors, so a gap of `2 × 100 × sqrt(2 × 0.10 × 0.90 / n)` points is the width that a genuinely level pair clears about once in twenty single checks. It narrows as the run goes on: **11.0 points on day 1, 6.3 on day 3, 4.1 on day 7, 2.0 on day 30** — computed in the draw function, not typed in.
- **Schedules and computed shares** (a race counts as fooled if the gap clears the bar on *any* checked day): final day only **5%**, midway and the end **8%**, once a week (4 checks) **12%**, every third day (10 checks) **18%**, every single day (30 checks) **27%**.
- **Title (bold 15px `P.ink`, centered, y=22):** "How Often Two Identical Pages Produce a Gap Worth Calling"
- **Bars:** five rows on a 44px pitch starting at `y=64`, bar height 20, track `rgba(107,114,128,0.10)` running `BX=214` to `w−96`, scaled so 30% is full width. Row 1 (one check) `rgba(25,158,112,0.45)` stroked `P.aqua` — the honest baseline. Rows 2–4 `rgba(74,58,167,0.45)` stroked `P.violet`. Row 5 (daily) `rgba(213,81,129,0.50)` stroked `P.magenta` — the one people actually do.
- **Row labels:** 12px `P.mute` right-aligned at `BX−10`, e.g. "final day only", "every single day", each with its check count in parentheses.
- **Row figures:** bold 12px in the row's hue, printed just right of each bar end, from the tally.
- **Intended-rate line:** a dashed 1.5px `P.aqua` vertical line at the one-check share, running the height of the bar block, labelled 12px `P.aqua` "what one check was meant to cost" above the top row.
- **Multiplier callout:** bold 19px `P.magenta` "5.4×" with 12px `P.mute` "as many false calls as checking once" under the last bar, computed as `daily ÷ once`.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "Stopping at the first good-looking day turns one test into thirty."

---

## 4. Two Runs That Open the Same and End Differently

**Tag colors:** `telling them apart` green, `same start` blue, `different finish` aqua
**Hue family:** green versus blue

### canvas `c4` — 720×320

Two running-total lines that begin almost on top of each other and separate over the month, with the day-three overlap boxed.

- **Data:** the same accumulation as chart 1. Stream one has both sides at `0.10`; stream two has B at `0.13`. From each seeded stream, take the first race whose day-3 gap lies in [7, 9] points.
- **The two paths:** no-difference race — day 3 **+8.9**, day 7 +5.0, day 15 +1.6, day 30 **+0.4**. Three-point-better race — day 3 **+7.8**, day 7 +5.2, day 15 +5.0, day 30 **+5.6**. Both read from the arrays.
- **Computed separation:** the two paths are 1.1 points apart on day 3 and 5.2 points apart on day 30 — a gap that grows by 4.1 points purely from waiting.
- **Title (bold 15px `P.ink`, centered, y=22):** "Same Opening, Different Ending"
- **Plot box:** `PX=52`, `PY=46`, right margin 120, bottom `h−54`. y from −1 to +10 points, ticks every 2 points; x day 1 to 30, labels at 1, 3, 7, 15, 30.
- **Grid:** `P.grid` horizontals; zero line 1.5px `P.mute` labelled 12px `P.mute` "dead level".
- **Lines:** the real-difference path 2.5px `P.green` with a 12px `P.green` right-edge label "truly 3 pts better"; the no-difference path 2.5px `P.blue` with a 12px `P.blue` right-edge label "no real difference". Dots radius 4 at days 3, 7, 15, 30 on both.
- **Overlap box:** a dashed 1.5px `P.mute` rectangle around the two day-3 points (dash 4/3), with 12px `P.mute` "1.1 points apart here" above it — the separation computed from the two arrays.
- **End brackets:** bold 12px `P.green` "+5.6" and bold 12px `P.blue` "+0.4" beside their day-30 points, printed from the arrays.
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Nothing in the first three days separates them. The next twenty-seven do."

---

## 5. Deciding in Advance How Long Is Long Enough

**Tag colors:** `the honest answer` green, `two questions first` aqua, `no magic number` red
**Hue family:** green/aqua

### canvas `c4b` — 720×330

Paired horizontal bars showing how many days of watching each size of difference needs, at two tolerances for being wrong.

- **Computation, in the draw function:** for a starting rate `p₁ = 0.10` and a target `p₂ = p₁ + L`, the per-side count is
  `n = ceil( (z_a·sqrt(2·p̄·(1−p̄)) + z_b·sqrt(p₁(1−p₁) + p₂(1−p₂)))² / (p₂−p₁)² )` with `p̄ = (p₁+p₂)/2`,
  `z_b = 0.841621` (catches a real difference four times in five), and `z_a = 1.959964` for a one-in-twenty tolerance or `2.575829` for one in a hundred. Days are `ceil(n / 60)`.
- **Results:** 1.5 points → **112** days (lenient) / **166** (strict); 2 points → **65** / **96**; 3 points → **30** / **44**; 6 points → **9** / **13**. Every figure printed from the formula, never typed.
- **Title (bold 15px `P.ink`, centered, y=22):** "Days of Watching Needed Before a Difference Shows"
- **Bars:** four label groups on a 62px pitch starting at `y=62`, each holding two 18px bars 4px apart. `BX=196` to `w−104`, scaled so the longest bar (166 days) is full width. Lenient bars `rgba(0,131,0,0.45)` stroked `P.green`; strict bars `rgba(25,158,112,0.40)` stroked `P.aqua`.
- **Group labels:** bold 12px `P.ink` right-aligned at `BX−10`, e.g. "a 3-point gain"; a 12px `P.mute` second line "worth acting on" only on the 3-point group.
- **Bar figures:** bold 12px in the bar's hue just right of each end, "112 days", "166 days", from the computation.
- **Legend** top right, 12px: `P.green` swatch "wrong 1 call in 20", `P.aqua` swatch "wrong 1 call in 100".
- **Four-times note:** bold 12px `P.mute` under the 1.5-point group, "halving the difference multiplies the wait by about four", with the multiple printed as `n(1.5) ÷ n(3)` — computed as 3.8.
- **Caption (bold 13px `P.green`, centered, `h−10`):** "The date falls out of two decisions. Make them before day one."

---

## Page-specific constraints

- **Scope of this page:** the *time dimension and the decision*. A running total watched day after day, the pull to stop on the day it looks best, and the reversal that follows. Deliberately NOT "observed rate versus group size" — that belongs to the denominator-neglect page, and a chart of rate against sample size must not appear here.
- **Shared simulation:** one `SIM` object computed once at script top and reused by charts 1, 2 and 3, so no two charts can disagree. 2,000 races × 30 days × 120 draws is about 7 million LCG calls, roughly 0.3s in node — acceptable once, not once per chart. Charts 4 and 5 use their own seeded streams.
- **Every printed figure computed in the draw function:** the reversal share, the peak-day shares, the average gaps, the bar widths, the schedule shares, the two paths' day-3 and day-30 values, the separation, and all the day counts. No label sits next to drawn data without being derived from it.
- **Bullet count follows the content** — seven per section here, eight in the last section.
- **Every section is constructed, so every section carries a `.src` note.** No paragraph blocks, no data tables, no `.example` line.
- **Canvas heights 320–340** (section 5 at 330), all intrinsic `width="720"`.
- **No section repeats another's fill-plus-highlight pairing** — see the hue family lines above.
- **The last section must not prescribe a number.** It states the two decisions that fix the length and demonstrates the arithmetic. Replacing "wait for 100" with "wait for 1,000" would be the same defect in different clothing.
- **Corrections applied to the earlier version of this page:**
  - The old lead chart plotted an observed rate against sample size on a log axis — the wrong chart for this page (it is the denominator-neglect picture) and it hardcoded a fabricated seven-point series with the label "90%!" typed in beside it. Replaced with seeded accumulation paths.
  - The old page asserted interval half-widths of ±40%, ±28%, ±18%, ±13%, ±9%, ±4% for n = 5, 10, 25, 50, 100, 500 around a 50% rate. These were not computed: at n = 5 the honest half-width is about ±44 points, and at n = 500 about ±4.4. Every figure in the section was typed rather than derived, and the section has been dropped in favour of the checking-schedule chart, which computes its bar widths.
  - The old "Where It Strikes" section was an eight-row table of unsourced domain anecdotes, including a clinical-trial claim and a hiring anecdote presented as fact. Removed — the spec forbids `.data-table`, and the claims had no basis.
  - The old chart 3 typed in five O'Brien-Fleming boundary values and an eleven-point path, then labelled them "z=4.05 needed here!" — a hardcoded label beside invented data, in vocabulary the page's reader has no way to parse. Replaced with the checking-schedule chart, whose every figure is tallied.
  - The claim "you need 4x the data to halve the uncertainty" was correct in the old page and is retained, but now demonstrated: 1.5 points needs 112 days against 30 for 3 points, a factor of 3.8 on the underlying counts.
