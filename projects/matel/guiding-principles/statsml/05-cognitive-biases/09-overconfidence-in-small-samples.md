# Overconfidence in Small Samples: Sure Before There Is Anything to Be Sure About

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Overconfidence in Small Samples — Cognitive Biases

**Subtitle:** Watch a result build up day by day and it swings hardest at the start — which is exactly when the room decides it is settled.

---

## Section 1 — Two Identical Pages, Thirty Days of Watching

**Tags:** `core idea` (violet), `the early lead` (blue), `it reverses` (magenta)

**Bullets:**
- **The setup** — two versions of a page, both converting at exactly the same rate
- **The daily check** — sixty visitors a side per day, and the total is read each morning
- **Day three** — one version is ten points ahead, the room is convinced, the meeting ends
- **Day thirty** — that same version finishes a point behind the one it was beating
- **How often** — the day-three front-runner ends up behind in almost four races in ten
- **What changed** — nothing about the versions; only the amount of data under the number
- **Why it convinces** — the gap is widest when it is thinnest, and size reads as proof

**Key point:** The feeling of certainty arrives days before the evidence does. Early in a run the number moves for reasons that have nothing to do with the thing you are measuring, and moving a long way is what it does best.

**Source note (`.src`):** Illustrative Example — six seeded thirty-day races between two versions that are deliberately identical; the reversal share comes from 2,000 such races.

### Visualization — canvas `c1`, 720×340

Six running-total lines over thirty days, each the gap between two identical versions, with the day-three leader marked at the moment it looked best and again where it finished.

- **Shared simulation (`SIM`, computed once, reused by every chart):** seeded Park–Miller LCG, seed 42. `DAYS = 30`, `PER = 60` visitors per side per day, true rate `0.10` for **both** sides. Each race walks day by day, accumulating conversions for A and B, and records `gap[d] = 100 × (B/n − A/n)` where `n` is the running per-side total. 2,000 races.
- **Sign alignment:** each race is flipped so the day-3 leader is the positive line. This makes "leader loses the lead" readable as "the line crosses below zero" instead of as two mirrored cases.
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

## Section 2 — The Day the Gap Looks Biggest Is Day One

**Tags:** `timing` (aqua), `widest when thinnest` (yellow), `peak early` (orange)

**Bullets:**
- **The question** — on which day of a thirty-day race does the gap look biggest
- **The answer** — day one, half the time, when each side has counted sixty visitors
- **The first three days** — they hold the widest gap in about eight races in ten
- **The final week** — it holds the widest gap in one race in two hundred
- **At its widest** — the gap averages just over five points across races
- **At the finish** — the same gap averages under a single point
- **The trap** — the best-looking number arrives on the morning there is least behind it

**Key point:** The most impressive number a run will ever produce almost always shows up in its first days. Anyone who stops when the result looks best is stopping on the strength of the smallest amount of data the run will ever have.

**Source note (`.src`):** Illustrative Example — the day-by-day shares come from the same 2,000 seeded races, both versions identical.

### Visualization — canvas `c2`, 720×320

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

## Section 3 — Every Extra Look Is Another Chance to Be Fooled

**Tags:** `stopping early` (magenta), `checking often` (violet), `false alarm` (red)

**Bullets:**
- **The bar** — a gap wide enough that identical versions clear it once in twenty per check
- **Checking once** — one race in twenty clears it, which is what the bar was set for
- **Checking twice** — one in twelve, since a second look is a second chance at a wobble
- **Checking weekly** — one in eight clears the bar somewhere along the way
- **Checking daily** — better than one race in four, on two versions that are identical
- **Why it grows** — the question quietly becomes "was the gap ever wide", not "is it wide"
- **What people do** — stop at the first crossing, so a lucky wobble ends the race

**Key point:** Watching a running total and stopping the moment it clears a bar is not the same test as checking once at the end. Each extra look is another draw at the same lottery, and the run ends on whichever draw wins.

**Source note (`.src`):** Illustrative Example — the same 2,000 seeded races, both versions identical, replayed against five checking schedules.

### Visualization — canvas `c3`, 720×320

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

## Section 4 — Two Runs That Open the Same and End Differently

**Tags:** `telling them apart` (green), `same start` (blue), `different finish` (aqua)

**Bullets:**
- **Two runs** — both open with a gap near eight points on day three, both look the same
- **One is nothing** — the two versions behind it convert at exactly the same rate
- **One is real** — the version behind it genuinely converts three points better
- **Day three** — the gaps are 8.9 and 7.8 points, and no reading of them separates the two
- **By day thirty** — the first has drained to 0.4 points, the second has settled at 5.6
- **The difference** — a real effect holds its ground while a lucky start walks back to level
- **Why waiting works** — it is the only thing that tells a settled number from a swinging one

**Key point:** Early results do not distinguish a real difference from a lucky one, because both look the same on day three. What separates them is not a cleverer reading of the early number — it is watching whether the gap stays put.

**Source note (`.src`):** Illustrative Example — two seeded races picked as the first from each stream whose day-three gap lands between 7 and 9 points, one with no true difference and one with a true three-point gain.

### Visualization — canvas `c4`, 720×320

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

## Section 5 — Deciding in Advance How Long Is Long Enough

**Tags:** `the honest answer` (green), `two questions first` (aqua), `no magic number` (red)

**Bullets:**
- **No universal number** — "wait for a thousand" is one guess swapped for another
- **The first question** — how big a difference would actually change what you do
- **The second question** — how often you can bear acting on a difference that was never there
- **A three-point gain** — worth having, and thirty days at sixty a side is enough to see it
- **Half that gain** — a point and a half needs about four times as long: 112 days
- **Twice that gain** — six points shows in nine days; a big difference needs little evidence
- **Tightening tolerance** — one wrong call in a hundred rather than twenty adds half again
- **The honest order** — settle the difference and the tolerance first, then read off the day

**Key point:** There is no sample size that is "enough" on its own. Name the smallest difference worth acting on and how often you are willing to be wrong, and the length of the run follows from those two answers — which is why the answer must be fixed before the first day, not chosen on the day the number looks good.

**Source note (`.src`):** Illustrative Example — days needed are computed from a 10% starting rate at sixty visitors a side per day, for a run that catches a real difference four times in five.

### Visualization — canvas `c4b`, 720×330

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

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the approved conversion in `05-clustering-illusion.html`. One `.card-section` per section, each with an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` → `.src` note (every section here is constructed, so every section has one). No paragraph blocks, no data tables, no `.example` line.
- **Bullet form:** each is ONE line ≤95 characters including the bold label, a complete thought, no wrap at 50% column width. Count follows the content — seven here, eight in the last section.
- **Scope of this page:** the *time dimension and the decision*. A running total watched day after day, the pull to stop on the day it looks best, and the reversal that follows. Deliberately NOT "observed rate versus group size" — that belongs to the denominator-neglect page, and a chart of rate against sample size must not appear here.
- **Shared simulation:** one `SIM` object computed once at script top and reused by charts 1, 2 and 3, so no two charts can disagree. 2,000 races × 30 days × 120 draws is about 7 million LCG calls, roughly 0.3s in node — acceptable once, not once per chart. Charts 4 and 5 use their own seeded streams.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42 for every stream.
- **Every printed figure computed in the draw function:** the reversal share, the peak-day shares, the average gaps, the bar widths, the schedule shares, the two paths' day-3 and day-30 values, the separation, and all the day counts. No label sits next to drawn data without being derived from it.
- **Canvas:** intrinsic `width="720"`, heights 320–340. CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. `setup(id)` caches the logical size in `dataset` on first call, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 13px; body and axis labels 12px floor; the one big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **Hue family per section, required:** 1 violet/blue with a magenta stop marker, 2 orange/yellow with an aqua side panel, 3 magenta/violet against an aqua baseline, 4 green versus blue, 5 green/aqua. No section repeats another's fill-plus-highlight pairing.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06a00`.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px, `.viz-col` centered. `ul` 0.92rem margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` `#1a5276`. `.key-point` `#f8f9fa` bg, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **The last section must not prescribe a number.** It states the two decisions that fix the length and demonstrates the arithmetic. Replacing "wait for 100" with "wait for 1,000" would be the same defect in different clothing.
- **Corrections applied to the earlier version of this page:**
  - The old lead chart plotted an observed rate against sample size on a log axis — the wrong chart for this page (it is the denominator-neglect picture) and it hardcoded a fabricated seven-point series with the label "90%!" typed in beside it. Replaced with seeded accumulation paths.
  - The old page asserted interval half-widths of ±40%, ±28%, ±18%, ±13%, ±9%, ±4% for n = 5, 10, 25, 50, 100, 500 around a 50% rate. These were not computed: at n = 5 the honest half-width is about ±44 points, and at n = 500 about ±4.4. Every figure in the section was typed rather than derived, and the section has been dropped in favour of the checking-schedule chart, which computes its bar widths.
  - The old "Where It Strikes" section was an eight-row table of unsourced domain anecdotes, including a clinical-trial claim and a hiring anecdote presented as fact. Removed — the spec forbids `.data-table`, and the claims had no basis.
  - The old chart 3 typed in five O'Brien-Fleming boundary values and an eleven-point path, then labelled them "z=4.05 needed here!" — a hardcoded label beside invented data, in vocabulary the page's reader has no way to parse. Replaced with the checking-schedule chart, whose every figure is tallied.
  - The claim "you need 4x the data to halve the uncertainty" was correct in the old page and is retained, but now demonstrated: 1.5 points needs 112 days against 30 for 3 points, a factor of 3.8 on the underlying counts.
