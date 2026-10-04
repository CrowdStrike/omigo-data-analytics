# Confirmation Bias — Viz

**Page type:** detail page, card-section
**HTML title tag:** Confirmation Bias — Cognitive Biases
**Template:** card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the approved conversion in `05-clustering-illusion.html`
**Source note wording:** the sibling `.txt.md` says figures are "computed at render time"; the html `.src` notes say "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed, a literal array, or a policy definition invalidates that prose — percentages, counts, averages and errors must be re-read from the new draw and the text updated to match.

**Determinism:** no `Math.random()`. Section 1 uses literal arrays; sections 2–4 use the seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42. Every printed count, percent and average is computed inside the draw function from the plotted data, so a label cannot drift from the chart.

---

## 1. Eight Weeks Get Audited, Week Nine Gets Filed

**Tag colors:** `core idea` violet, `one-sided checking` blue, `nobody lied` magenta
**Hue family:** blue bars with a magenta outlier and a violet hours row

### canvas `c1` — 720×330

Nine weekly bars against a target line, with the hours-spent-checking row underneath so the asymmetry is visible as a shape.

- **Data (literal arrays, no randomness):** `W = [970, 1010, 950, 1000, 980, 1020, 960, 1010, 1120]`, target `BASE = 1000`, hours spent reviewing `HRS = [3, 2, 4, 2, 3, 5, 4, 3, 0]`, double-entry amount `DBL = 140`.
- **Computed in the draw function:** each bar's percent versus target as `100 × (W[i] − BASE) / BASE` → −3.0, +1.0, −5.0, 0.0, −2.0, +2.0, −4.0, +1.0, **+12.0**. Weeks 1–8 average **−1.3** percent; week nine gets 0 review hours against 26 hours for the eight before it. Corrected week nine `1120 − 140 = 980` → **−2.0** percent.
- **Title (bold 15px `P.ink`, centered, y=22):** "Nine Weeks of Takings Against Target"
- **Bar panel:** `PX=46`, right reserve 226, plot top 54, baseline at `h − 116`. The percent axis spans ±15 so the target line sits mid-plot. Nine columns, bar width `min(38, slot − 10)`. Weeks 1–8 filled `rgba(42,120,214,0.45)` stroked `P.blue`; week nine filled `rgba(213,81,129,0.55)` stroked 2.5px `P.magenta`.
- **Target line:** 1.5px dashed `P.mute` (dash 5/4) at the zero-percent level, labelled 12px `P.mute` "target" at the right end.
- **Bar labels:** the signed percent above each bar (below it when negative), 12px `P.mute`, week nine in bold `P.magenta`. Week numbers "1".."9" sit under the hours row in 12px `P.mute`, followed by "week".
- **Hours row:** at `baseline + 32`, a slim 11px-high bar per week scaled to the maximum hours, filled `rgba(74,58,167,0.45)` stroked `P.violet`. Week nine has no bar — instead a 1px dashed `P.violet` outline of the full slot with bold 12px `P.violet` "0 h" beside it. Row header bold 12px `P.ink` above the row: "HOURS SPENT CHECKING".
- **Right callout:** bold 13px `P.ink` "WEEK NINE, AFTER A REVIEW"; bold 19px `P.green` "−2.0%" with 12px `P.mute` "once the double entry / is taken back out"; then bold 12px `P.magenta` "reported as +12.0%"; then 12px `P.mute` "weeks 1-8 averaged" with bold 12px `P.blue` "−1.3%". Every figure printed from the computed arrays.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The only week nobody audited is the only week that agreed with them."

---

## 2. Every Day Has an Excuse, Only Some Days Get Searched

**Tag colors:** `the mechanism` aqua, `excuses are everywhere` yellow, `one-sided search` orange
**Hue family:** gold/green strip with an orange search marker

### canvas `c2` — 720×320

The forty days as a strip split by the target line, with the share of excusable days computed on each side and printed as two bars — the point being that the two bars are nearly the same height.

- **Data:** seeded Park–Miller LCG, seed 42. `v[i] = 100 + round((rng()×2 − 1) × 12)` for 40 days, then a second pass over the same stream sets `ex[i] = rng() < 0.5`. The excuse is drawn **after and independently of** the value, so any difference in the two rates is chance alone.
- **Resulting arrays (fixed by the seed):** `v = [88,101,106,94,97,93,111,100,101,94,91,108,110,99,94,94,93,96,90,107,101,108,106,90,101,91,109,99,99,99,94,88,89,112,110,107,102,106,103,96]`; `ex = [1,1,1,0,1,1,0,1,1,1,1,1,0,0,1,0,1,0,1,0,0,1,0,1,1,0,1,0,0,1,1,1,0,1,1,0,0,1,0,1]`.
- **Computed in the draw function:** 21 days below 100, of which 13 are excusable → 62 percent. 19 days at or above, of which 11 are excusable → 58 percent. 24 excusable days in total, 60 percent of forty.
- **Title (bold 15px `P.ink`, centered, y=22):** "Forty Days, Each With or Without an Excuse"
- **Day strip:** 40 cells across `x = 44 … w−32` at `y=46`, height 26, ordered as generated. Cells at or above the target filled `rgba(25,158,112,0.35)` stroked `P.aqua`; cells below filled `rgba(201,133,0,0.35)` stroked `P.yellow`. A cell carrying an excuse gets a 2px `P.orange` line drawn along its bottom edge, so excuse-marking is a separate visual channel from high/low.
- **Legend (12px `P.mute`, under the strip):** "each square is one day — gold is below target, green is at or above"; then bold 12px `P.orange` "the orange underline marks a day with a documented excuse".
- **Rate bars:** header bold 13px `P.ink` "SHARE OF DAYS THAT CARRY AN EXCUSE" at y=152. Two horizontal bars from `x=186` on a 40px pitch, height 20, track `rgba(107,114,128,0.12)`, width `w − 300`. Row "days below target" filled `rgba(201,133,0,0.50)` stroked `P.yellow`; row "days at or above" filled `rgba(25,158,112,0.45)` stroked `P.aqua`. Each prints its computed percent bold 12px in the row colour at the bar end ("62%", "58%") plus a 12px `P.mute` count ("13 of 21", "11 of 19"). The two bars come out nearly the same length — that is the point of the panel.
- **Search marker:** a 2.5px `P.orange` bracket around only the "days below target" bar, with bold 12px `P.orange` "the only row Bob searched" beneath the panel and 12px `P.mute` "24 of 40 days carry an excuse in all" under that.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "Excuses sit on both sides of the line. Only one side was ever searched."

---

## 3. The Same Spreadsheet Reaches Two Opposite Answers

**Tag colors:** `two analysts` magenta, `same rules` violet, `opposite results` red
**Hue family:** magenta/violet lollipops against a green honest baseline

### canvas `c3` — 720×330

Four averages on one axis — the honest one, the two one-sided ones, and the even-handed one — with the target line drawn through them.

- **Data:** the identical seeded arrays `v` and `ex` from section 2, rebuilt from seed 42 inside this draw function.
- **Computed in the draw function:** honest average of all forty = **99.4** (99.42, kept 40). Alice drops days with `v < 100 && ex` → keeps 27, average **102.6** (102.59). Bob drops days with `v >= 100 && ex` → keeps 29, average **97.1** (97.07). Drop every excusable day → keeps 16, average **100.5** (100.50). Alice-minus-Bob gap = **5.5**. Even-handed rule sits **1.1** off the honest figure; Alice sits 3.2 off.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Spreadsheet, Four Reported Averages"
- **Axis:** horizontal, 96 to 104, baseline at `h − 82`, ticks every 1 with 12px `P.mute` labels and a 12px `P.mute` "reported average" underneath. Target 100 drawn as a 1.5px dashed `P.mute` vertical rule up the full plot, labelled 12px `P.mute` "target 100" above it.
- **Rows:** four lollipop rows on a 44px pitch starting `y=64`, each a 2px stem from the target rule to its value with a radius-7 filled dot at the value. Row order and colour: "all forty days, nothing dropped" `P.green`; "Alice — drops excusable days below" `P.magenta`; "Bob — drops excusable days above" `P.violet`; "drops every excusable day" `P.blue`.
- **Row labels:** 12px `P.mute`, right-aligned at `x = 246`, with the kept-day count on a second line ("kept 40 of 40", "kept 27 of 40", "kept 29 of 40", "kept 16 of 40"), all read from the filtered arrays.
- **Value labels:** the average printed bold 13px in the row's colour just past its dot, one decimal — 99.4, 102.6, 97.1, 100.5.
- **Gap bracket:** a 2.5px `P.magenta` bracket spanning Alice's row to Bob's row with bold 12px `P.magenta` "5.5 points / apart" beside it, the figure computed as the difference of the two averages.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Same data, same rules — the answer followed whoever was reading."

---

## 4. When Checking the Odd Result First Is the Right Call

**Tag colors:** `the boundary` green, `legitimate suspicion` aqua, `where it turns` red
**Hue family:** orange/blue garbles with a green verdict

### canvas `c4` — 720×340

The forty readings as a dot row with the true and garbled positions shown, above the average each checking policy produces against the true average.

- **Data:** seeded LCG, seed 42, 40 boxes, `truth[i] = round((20 + (rng()×2 − 1) × 1.5), 1 dp)`. Then a seeded Fisher–Yates over indices 0..39 on the same stream picks ten positions; the first six get `+5.0`, the last four get `−5.0`. `obs[i] = truth[i] + err[i]`.
- **Resulting arrays (fixed by the seed):** heavy garbles at indices 8, 18, 21, 22, 26, 39; light garbles at indices 0, 29, 33, 35. Observed values run 13.5 to 26.1 with a median of 19.9.
- **Computed in the draw function:** true average **19.92**. Re-weigh nothing → **20.17**, off by **+0.25**. Policy one, the twelve furthest from the median, catches **10 of 10** garbles (its twelfth pick sits 1.4 kg off the median) plus two clean boxes, and lands on **19.92**, off by **0.00**. Policy two, the twelve heaviest, catches **6 of 10** — every heavy garble, no light one — and lands on **19.42**, off by **−0.50**.
- **Title (bold 15px `P.ink`, centered, y=22):** "Forty Boxes, Ten Bad Readings, Twelve Re-Weighs"
- **Reading row:** dots along `x = 50 … w−34` in index order, radius 5, inside a 62px band from `y=42`. Height is the reading scaled 13 to 27 kg, so the two garble groups sit visibly clear of the pack. Clean readings `rgba(107,114,128,0.35)` stroked `P.mute`; heavy garbles `rgba(217,89,38,0.60)` stroked `P.orange`; light garbles `rgba(42,120,214,0.55)` stroked `P.blue`. The median drawn as a 1px dashed `P.grid` horizontal rule.
- **Row legend (12px `P.mute`, under the band):** "each dot is one box — height is the reading"; then bold 12px `P.orange` "6 read too heavy", bold 12px `P.blue` "4 read too light", and 12px `P.mute` "typical reading 19.9 kg" — all three counted from the error array and the sort.
- **Policy panel:** header bold 13px `P.ink` "AVERAGE EACH CHECKING POLICY REPORTS". Three rows on a 46px pitch. Each row: 12px `P.mute` name and catch-rate clause right-aligned, then the reported average bold 19px in the row's colour. "re-weigh nothing" in `P.mute` with "0 of 10 caught" → 20.17; "the twelve furthest from typical" in `P.green` with "10 of 10 caught" → 19.92; "the twelve heaviest" in `P.magenta` with "6 of 10 caught, all heavy" → 19.42.
- **Truth marker:** a 2px `P.green` vertical rule down the value column with bold 12px `P.green` "true average 19.92 kg" above it, so each policy's error reads as horizontal distance. Beside it, each row prints its own signed error — "+0.25 kg off", "spot on" (printed whenever the gap is under 0.005), "−0.50 kg off".
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Check what is implausible, not what is inconvenient."

---

## Page-specific constraints

- **Canvas placement:** `td.viz-col` is `text-align: center`; the canvas is `display: block; width: 100%; margin: 0 auto`, capped at 720px, so a wide cell leaves slack and the chart sits centred in the right half.
- **Every section carries a `.src`.** Every section here has constructed figures. No paragraph blocks, no `.example` lines, no data tables, no philosophy box.
- **Bullet counts follow the content** — seven, eight, eight, ten here. No padding, no line that restates another.
- **Section titles name the content.** No role labels ("The Trap", "Where It Strikes", "In the ML Pipeline", "Pipeline Defense") and no phrasing that would fit another page.
- **Per-chart canvas height 320–340**, varying by section: 330, 320, 330, 340.
- **Colour variety across sections is required.** Section 1 blue bars with a magenta outlier and violet hours row; section 2 gold/green strip with orange search marker; section 3 magenta/violet lollipops against a green honest baseline; section 4 orange/blue garbles with a green verdict. No section repeats another's dominant hue.
- **Every chart ends with a bold 13px caption stating its takeaway.**
- **Sections 2 and 3 must rebuild the same arrays.** Both call `lcg(42)` and generate values then excuses in that order, so the 21/19 split, the 62/58 percent rates and all four averages in section 3 refer to one dataset. Changing the generation order in one place silently breaks the other.
- **The last section states a real boundary, not a blanket condemnation.** Unequal scrutiny is legitimate when a result genuinely is likelier to be an error — that is what policy one exploits, and it beats checking nothing. The failure is when the filter tracks the desired direction rather than the implausible one. Do not rewrite this section to claim all asymmetric checking is bias.
- **Corrections and changes from the earlier version of this page:**
  - The old page was built entirely on ML-experiment vocabulary — AUC scores, random seeds, cross-validation folds, holdouts, data leakage, pre-registration, "seed shopping". All of it is gone; the subject is now a shop's weekly takings, a hand-entered sales spreadsheet, and a warehouse scale.
  - The old section "Where It Strikes" was a `.data-table` of eight domains beside a bar chart whose four pairs of hours (1.2 vs 6.5, 0.8 vs 5.8, 1.5 vs 7.2, 0.5 vs 4.9) were asserted rather than computed, and whose caption claimed "5-10x less scrutiny" when the plotted pairs give 5.4×, 7.3×, 4.8× and 9.8× — two of the four sit outside the stated range. Both the table and the chart are replaced by a construction where the even spread of excuses is counted from the plotted data.
  - The old seed-shopping chart printed "Overstatement: +0.12" for 0.89 against a mean of `[0.71, 0.74, 0.89, 0.73, 0.76]`; the mean is 0.766 and the gap is 0.124, so the chart's own two labels ("0.89" and "0.77") do not reproduce the third. Section dropped along with the vocabulary.
  - The old "Pipeline Defense" section argued that automation removes bias because "the pipeline doesn't have a hypothesis". That overclaims — whoever specifies the candidate list still chooses. Replaced by section 4, which distinguishes legitimate targeted checking from preference-driven checking instead of promising a process that eliminates the choice.
  - The old page had no boundary section at all: it treated every instance of unequal scrutiny as a defect. Section 4 now shows a case where unequal scrutiny is the better policy (0.00 off the truth versus 0.25 for checking nothing) and names precisely what flips it into a defect.
