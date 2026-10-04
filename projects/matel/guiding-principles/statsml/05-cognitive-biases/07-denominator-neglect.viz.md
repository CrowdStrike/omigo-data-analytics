# Denominator Neglect — Viz

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Denominator Neglect — Cognitive Biases
**Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the approved conversion in `05-clustering-illusion.html`
**Source note wording:** the sibling `.txt.md` says figures are "computed at render time"; the html `.src` notes for sections 2 and 5 say "computed in the draw function" / "found by search in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed, a true rate, a size list or a target invalidates that prose — re-read the computed spans, rung chances, league table, repeat tally and group sizes and update the text to match.

**Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42 for section 1 and seed 99 for section 4. Sections 2, 3 and 5 need no randomness at all — their figures are exact and computed by loop or search.

---

## 1. The Same Product, Watched in Groups of Different Sizes

**Tag colors:** `core idea` violet, `how wide it swings` blue, `nothing changed` magenta
**Hue family:** violet with a blue tail

### canvas `c1` — 720×340

A beeswarm: six columns, one per group size, each holding thirty seeded observed rates around one dashed true-rate line. The funnel narrows left to right.

- **Data:** seeded Park–Miller LCG, seed 42. True rate `TRUE = 0.25`. `SIZES = [8, 24, 60, 150, 400, 1000]`, `TRIALS = 30` per size. For each group, count `k` successes from `n` draws of `rng() < TRUE`; the observed rate is `k / n`. Drawn in size order off one continuous stream, so the figure is fixed.
- **Computed spans (scanned from the dots, printed under each column):** n=8 → 0%–50%, span **50pp**; n=24 → 8%–50%, span 42pp; n=60 → 10%–35%, span 25pp; n=150 → 19%–33%, span 13pp; n=400 → 21%–31%, span 10pp; n=1000 → 22%–27%, span **5pp**. Ratio of first span to last ≈ 10×.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Product, Truly Bought by a Quarter of Shoppers"
- **Plot box:** `PX=54`, `PY=52`, `PW = w − 118 − PX`, `PH = h − 88 − PY`. Y axis is the observed rate, 0% at the bottom to 55% at the top; horizontal `P.grid` lines every 10% with 12px `P.mute` labels "0%", "10%" … right-aligned left of the axis.
- **True-rate line:** dashed (5/4) 1.5px `P.mute` running from the axis out to `w − 12` at 25%, with 12px `P.mute` "true rate 25%" right-aligned at that end — it extends past the plot so the label clears the last column's bracket.
- **Columns:** six equal slots. Dots quantized to 0.5pp buckets and fanned symmetrically about a point 8px left of the slot centre (leaving the right shoulder free) at a 4.4px pitch, radius 3.4. Fill `rgba(74,58,167,0.45)` stroked `P.violet` 1px for the two leftmost (small-group) columns; `rgba(42,120,214,0.45)` stroked `P.blue` for the rest — the hue shift marks where the swing stops being wild.
- **Span brackets:** a 2px vertical bracket with 4px end caps at `cx + slot/2 − 16` spanning that column's min to max, in the column's hue. Bold 12px in the column's hue prints the span as "50pp" … "5pp" beside the bracket midpoint.
- **Axis labels:** bold 12px `P.mute` group size ("8", "24", "60", "150", "400", "1000") centered under each slot, then 12px in the column's hue the range that column printed ("0–50%" … "22–27%"), both from the scan. 12px `P.mute` "people watched per group — and the range of numbers it printed" centered under the row.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Same product all the way across. Only the group size changed."

---

## 2. Eight Visits Give You Nine Possible Answers

**Tag colors:** `the ladder` aqua, `one sale jumps it` orange, `false doubling` red
**Hue family:** yellow/orange with an aqua truth rung

### canvas `c2` — 720×330

The nine reachable answers as rungs on a ladder, each with its exact chance, and the two-rung hop that reads as a doubling marked across it.

- **Construction:** `n = 8`, true rate `p = 0.25`. Exact chance of each rung `k` is `C(8,k) · 0.25^k · 0.75^(8−k)`, computed with a product loop for the binomial coefficient — no simulation, so the labels are exact.
- **Computed rungs (chance printed from the loop):** 0 sales → 0%, chance 10%; 1 → 13%, 27%; 2 → 25%, 31%; 3 → 38%, 21%; 4 → 50%, 9%; 5 → 63%, 2%; 6 → 75%, 0%; 7 and 8 round to 0%. Percentages on the rungs are rounded to whole numbers, matching the prose.
- **Derived figures:** the chance of landing on 50% or above is 11%; the chance that a second week of eight at least doubles a non-zero first week is 21%; the chance the two weeks sit 25 points apart or further is 38%. All computed from the same rung chances.
- **Title (bold 15px `P.ink`, centered, y=22):** "Eight Visits: Every Answer the Week Can Print"
- **Step panel across the top:** bold 13px `P.ink` header "WHAT ONE EXTRA SALE MOVES THE HEADLINE" at `x=42, y=46`, then three tiles on a 200px pitch — bold 19px `P.orange` "12.5" with 12px `P.mute` "points" / "at 8 visits"; bold 19px `P.yellow` "2.5" / "at 40 visits"; bold 19px `P.aqua` "0.5" / "at 200 visits". Each figure computed as `100 / n`.
- **Ladder:** `LX=112`, bar track 118 wide, top rung at `y=112`, 22px pitch, bar height 13, nine rungs running 8-of-8 down to 0-of-8. Each rung's bar length is its chance against the tallest rung, on a `rgba(107,114,128,0.10)` track; rungs whose chance rounds below a pixel draw no bar at all. The truth rung (2 sales) `rgba(25,158,112,0.50)` stroked `P.aqua`; the headline rung (4 sales) `rgba(217,89,38,0.50)` stroked `P.orange`; the rest `rgba(201,133,0,0.35)` stroked `P.yellow`.
- **Rung labels:** right-aligned 12px `P.mute` "N of 8" left of the track; bold 12px the printed headline at `LX+LW+12`; 12px `P.mute` "(N% of weeks)" at `LX+LW+54`; and at `LX+LW+168` bold 12px "← the truth about the product" (`P.aqua`) and "← reads as a doubling" (`P.orange`) on those two rungs. Every percentage from the binomial loop.
- **The hop:** a 2.5px `P.orange` bracket at `LX−64` joining the 2-sales rung to the 4-sales rung, with bold 12px `P.orange` "two customers" rotated a quarter turn beside its midpoint.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "The headline cannot inch. Two customers is the smallest doubling money can buy."

---

## 3. A Spotless Record over Three Jobs

**Tag colors:** `perfect scores` green, `two contractors` magenta, `too few tries` mute
**Hue family:** green against magenta

### canvas `c3` — 720×330

Two curves of "chance of a spotless record so far" against the number of jobs, with the three-job gap shaded and the mix of spotless records printed at three job counts.

- **Construction:** the sloppy contractor is clean on any one job with chance 0.75, the careful one 0.95. Chance of a spotless run of `k` jobs is `0.75^k` and `0.95^k`, computed in the loop.
- **Computed values (printed from the loop):** at 3 jobs 42% vs 86%; at 12 jobs 3% vs 54%; at 20 jobs 0% vs 36%. Share of spotless records belonging to the sloppy contractor, from an even mix of the two, `a / (a + b)`: **33% at 3 jobs**, 6% at 12, 1% at 20.
- **Title (bold 15px `P.ink`, centered, y=22):** "Chance of a Spotless Record, Job by Job"
- **Plot box:** `PX=56`, `PY=50`, `PW = w − 212 − PX`, `PH = h − 62 − PY`. X axis 1 to 20 jobs; Y axis 0% to 100% with horizontal `P.grid` lines every 20% and 12px `P.mute` labels.
- **Curves:** the careful contractor 2.5px `P.green` with 3px dots at jobs 1, 3, 12, 20; the sloppy one 2.5px `P.magenta`, same dots. The vertical gap between the curves at 3 jobs filled `rgba(213,81,129,0.12)`, 12px wide.
- **Curve labels (bold 12px):** `P.green` "clean 19 jobs in 20" above its curve at job 8, `P.magenta` "clean 3 jobs in 4" below its curve at job 8.
- **Three-job marker:** a 1.5px dashed (4/3) `P.mute` vertical at 3 jobs, with bold 12px `P.mute` "3 jobs" under the axis; the two curve values printed bold 12px in their hues beside their dots as "86%" and "42%".
- **Mix panel** at `x = w − 198`: bold 13px `P.ink` "OF ALL SPOTLESS RECORDS, / THE SHARE FROM THE / SLOPPY ONE" on three lines, then three rows on a 48px pitch — bold 19px figure, 12px `P.mute` "after N jobs" beside it, and a bold 12px verdict beneath. 3 jobs → 33% in `P.magenta` with "record proves little"; 12 jobs → 6% in `P.green` with "record has earned it"; 20 jobs → 1% in `P.green`, same. Green switches on when the share drops below one in ten.
- **Axis title (12px `P.mute`, centered under the plot):** "jobs completed"
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Perfect over three jobs is common. Perfect over twenty is a reputation."

---

## 4. Ranking Sixteen Shops That Are All Equally Good

**Tag colors:** `league tables` orange, `identical shops` blue, `footfall decides` red
**Hue family:** orange/yellow

### canvas `c4` — 720×340

The sixteen shops as a ranked ladder of dots against the shared true rate, each dot weighted by footfall, with two range strips underneath and the how-often-does-a-quiet-shop-lead figure beside it.

- **Construction:** seeded LCG, seed 99. `TRUE = 0.25`. Footfall `SIZES = [5, 8, 10, 14, 20, 28, 40, 55, 75, 100, 140, 190, 260, 350, 470, 600]`. Each shop draws `k` sales from `n` visits at the true rate, then the sixteen are sorted by `k / n` descending in the draw function.
- **Computed table (from the draw, top to bottom):** 2/5 = 40%, 5/14 = 36%, 6/20 = 30%, 30/100 = 30%, 134/470 = 29%, 15/55 = 27%, 95/350 = 27%, 70/260 = 27%, 20/75 = 27%, 37/140 = 26%, 10/40 = 25%, 142/600 = 24%, 38/190 = 20%, 5/28 = 18%, 1/8 = 13%, 0/10 = 0%.
- **Computed spreads:** the five quietest (5, 8, 10, 14, 20 visits) run 0% to 40% — a **40pp** spread; the seven busiest (100 and up) run 20% to 30% — a **10pp** spread. Both scanned from the sorted array.
- **Repeat figure:** 2,000 seeded months over the same sixteen shops, recording which shop tops the table. The top spot goes to one of the five quietest **80%** of the time, and to one of the seven busiest 4% of the time. Stable at 2,000 / 4,000 / 8,000 months.
- **Title (bold 15px `P.ink`, centered, y=22):** "Sixteen Shops, All Truly Converting a Quarter"
- **Plot box:** `PX=132`, `PY=52`, `PW = w − 208 − PX`, sixteen rows on a 12px pitch. X axis is the printed rate, 0% to 45%, with vertical `P.grid` lines every 10% and 12px `P.mute` labels under the plot.
- **True-rate line:** 1.5px dashed (5/4) `P.mute` vertical at 25% running 10px above the plot, labelled 12px `P.mute` "every shop is truly 25%" right-aligned at the plot's right edge above the rows — above rather than below, so it does not crowd the strips.
- **Rows:** a faint `rgba(107,114,128,0.20)` leader line from the axis to the dot. Dot radius scales with footfall as `3 + 5·√(n/600)`, so the big shops read as heavy and visual weight means trustworthiness. Shops of 20 visits or fewer fill `rgba(217,89,38,0.55)` stroked `P.orange`; the rest `rgba(201,133,0,0.45)` stroked `P.yellow`.
- **Row labels (12px, right-aligned left of the axis):** "K of N" from the array — `P.orange` for the five quietest, `P.mute` for the rest. The top row is tagged bold 12px `P.orange` "top of the table" to the *left* of its dot (it sits near the right edge) and the bottom row "bottom of the table" to the right of its dot.
- **Range strips** under the axis, at `PY + PH + 28` and `+48`: a filled bar from that family's lowest printed rate to its highest — the quiet family `rgba(217,89,38,0.30)` stroked `P.orange`, the busy family `rgba(201,133,0,0.30)` stroked `P.yellow`. Named 12px right-aligned left of the axis, width printed bold 12px past the bar end as "40pp apart" and "10pp apart". Strips rather than brackets because the two families are interleaved down the ranking — which is itself the point.
- **Repeat panel** at `x = w − 196`: bold 13px `P.ink` "RUN THE MONTH AGAIN — / WHO TOPS THE TABLE", then bold 19px `P.orange` "80%" with 12px `P.mute` "one of the five / quietest shops", and bold 19px `P.yellow` "4%" with 12px `P.mute` "one of the seven / busiest shops". Both printed from the repeat tally.
- **Caption (bold 13px `P.orange`, centered, `h−9`):** "The table is not sorted by quality. It is sorted by who counted fewest people."

---

## 5. How Big a Group Depends on How Big a Gap

**Tag colors:** `the boundary` green, `no magic number` blue, `fixed floors fail` red
**Hue family:** green against blue, with the page's single hard-red warning tile

### canvas `c5` — 720×340

Four gap sizes as bars on a log scale of the group needed, at two levels of how-sure-you-want-to-be, with the doubling-at-eight case shown as the cautionary tile.

- **Construction:** for two versions with true rates `pa` and `pb` and equal group size `n`, `pAhead(n, pa, pb)` is the exact chance the better version prints the higher number. Both counts are exact binomial distributions built by a recurrence around the mode (no factorials, so it stays stable to `n` in the thousands) and combined by convolution over one prefix sum. `minGroup` binary-searches the smallest `n` reaching a target, then steps down and up to land on the true smallest.
- **Computed group sizes (exact, printed from the search):**

  | gap from a true quarter | right 9 times in 10 | right 99 times in 100 |
  |---|---|---|
  | doubled, 25% → 50% | 16 | 43 |
  | up ten in a hundred, 25% → 35% | 78 | 236 |
  | up five in a hundred, 25% → 30% | 281 | 881 |
  | up two in a hundred, 25% → 27% | 1,629 | 5,254 |

- **Derived figures:** at eight a side the doubling is right 79% of the time, so it finishes level or behind 21% — about one time in five. The ratio of the two-in-a-hundred gap to the doubling is 1,629 / 16 ≈ 102, matching "a hundred times". The 99-in-100 column is 2.7× to 3.2× the 9-in-10 column, matching "roughly triples".
- **Title (bold 15px `P.ink`, centered, y=22):** "How Many People You Need, by the Gap You Want to Catch"
- **Plot box:** `PX=176`, `PY=70`, `PW = w − 84 − PX`, `PH=138`. X axis is the group needed on a base-10 log scale from 10 to 10,000, with vertical `P.grid` lines and 12px `P.mute` labels "10", "100", "1,000", "10,000".
- **Bars:** four rows on a 34.5px pitch, each with two bars — the 9-in-10 bar `rgba(0,131,0,0.45)` stroked `P.green` at `yc − 14`, the 99-in-100 bar `rgba(42,120,214,0.40)` stroked `P.blue` at `yc + 2`, both 12px tall. Bar length is the log position of the computed size. The size is printed bold 12px past each bar end in its hue, with a thousands separator.
- **Row labels (12px `P.mute`, right-aligned left of the axis, two lines each):** "doubled" / "25% → 50%", "up ten in a hundred" / "25% → 35%", "up five in a hundred" / "25% → 30%", "up two in a hundred" / "25% → 27%".
- **Legend (12px, above the plot at `PY−23`):** a `P.green` swatch "right 9 times in 10" at `x=42` and a `P.blue` swatch "right 99 times in 100" at `x=232`.
- **Warning tile** across the bottom at `PY + PH + 46`, 34 tall, inset 34px each side: a `rgba(231,76,60,0.06)` box stroked `rgba(231,76,60,0.35)`. Inside, bold 12px `#e74c3c` "A FIXED FLOOR ANSWERS THE WRONG QUESTION", then 12px `P.mute` "the same floor is generous for a doubling and hopeless for two in a hundred" — the only hard red on the page, and it is a genuine warning.
- **Caption (bold 13px `P.green`, centered, `h−9`):** "Pick the gap that would change your mind, then ask how many people that takes."

---

## Page-specific constraints

- **Canvas heights, per chart:** 340, 330, 330, 340, 340.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`, capped at 720px, so a wide cell leaves slack and the chart sits centered in the right half.
- **Every section carries a `.src`** — every section's figures are constructed. No paragraph blocks, no data tables, no `.example` lines.
- **Bullets: count follows the content.** Seven where seven covers it, nine where the mechanism needs nine. No padding, and no line that restates a bullet — each section ends at its key point plus the source note.
- **Numbers in prose are spelled in words** ("fifty points", "nine times in ten"). Bare figures appear only on the charts, where they are computed. The page never states a percentage more precisely than the chart rounds it.
- **Section titles name the content.** No role labels, no phrasing reused from another page.
- **Colour: one hue family per section, rotated.** Section 1 violet with a blue tail; section 2 yellow/orange with an aqua truth rung; section 3 green against magenta; section 4 orange/yellow; section 5 green against blue with the page's single hard-red warning tile. Do not let blue-fill-plus-orange-highlight become every chart. Hard red `#e74c3c` only in section 5's warning tile.
- **Shared exact helpers, defined once above the draws:** `binomChance(n, k, p)` for the section 2 rungs (product-loop binomial coefficient); `binomPmf(n, p)` building the distribution by recurrence outward from the mode and normalising, so it survives `n` in the thousands; `pAhead(n, pa, pb)` convolving two such distributions over one prefix sum; `minGroup(pa, pb, target)` binary-searching the smallest group reaching the target and then correcting one step either way.
- **Every printed figure is computed in its draw function** from the plotted data — spans scanned from the dot arrays, rung chances from the binomial loop, spotless chances from the power loop, the league table sorted in place, group sizes from the search. No hardcoded label sits next to drawn data.
- **The lead chart must make the instability visible.** The page is about a percentage that swings; the funnel of dots narrowing left to right is the argument, and a chart that merely described the effect would not carry it.
- **The last section must refuse to name a universal minimum.** Replacing "denominator too small" with "you need thirty" is the same defect in a new costume. The section states the two choices that set the size, shows four computed answers spanning sixteen to five thousand, and marks the fixed-floor idea as the trap.

### Corrections applied to the earlier version of this page

- The old page framed the mechanism in confidence-interval vocabulary — "CI width", "1/√n", "95% CI", "shrinkage", "empirical Bayes", "minimum-n gates" — none of which a reader without statistics training can use. All of it is now stated in everyday words: how wide the number can swing, how far one extra sale moves the headline.
- The old chart `c1` hardcoded the interval bounds "75% (n=20) → [51, 91]" and the width "40 pp" as literal label text beside drawn bars. Neither figure is right by any standard method: the usual interval for 15 of 20 is about 56%–94% (width 38pp) and the small-sample-corrected one about 53%–89% (width 36pp). The label was asserted, not computed. Both bars are gone; section 1 now scans its range off the plotted dots.
- The old chart `c5` sized each bubble as `max(8, 50/√n)`, which makes the least reliable claim the *biggest* bubble — the opposite of the visual convention, and the caption had to apologise for it in words ("Bigger bubble = smaller sample = less reliable"). Section 4 now sizes dots *with* footfall, so the heavy dots are the trustworthy ones, and no explanation is needed.
- The old chart `c2` plotted the *full* interval width — about 62pp at ten people — but annotated that same point "n=10 → ±31pp", a half-width, with nothing marking the switch. The chart's own bullet list then said "n=10: ±30 percentage points". Three ways of stating one quantity, none reconciled. That chart is dropped.
- The old `c4` shrinkage diagram listed "shrunk" values (90→62, 80→68, 20→38 and so on) that were asserted, not derived from any stated pooling rule, and could not be reproduced. Dropped.
- The old "Three Flavors" section bundled base-rate neglect into this page and spent a whole 10,000-cell icon array on a rare-disease test. That belongs to `statistical-paradoxes/02-base-rate-fallacy`; keeping it here duplicated another page and pushed this one off its own subject.
- The old page named a doctor and quoted a "98% success rate" over fifty procedures as though measured. Replaced by two unnamed contractors, labelled as constructed.
- The old page never addressed the boundary. It ended on "minimum-n gates: refuse to compute metrics below a sample floor" — precisely the fixed threshold that replaces one magic number with another. Section 5 now shows why no single floor can be right, with computed sizes from sixteen to over five thousand for the same true rate.
