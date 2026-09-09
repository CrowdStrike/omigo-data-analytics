# Sports Analytics & Performance Metrics

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Sports Analytics & Performance Metrics - Domain Pitfalls

**Subtitle:** Sports analytics combines tiny samples with extreme selection effects, venue biases, and era adjustments — advanced metrics project a precision that large player valuations don't actually have.

**Determinism rule for this page:** no chart may call `Math.random()`. Every generated series uses the canonical seeded Park-Miller LCG helper `lcg(seed)` declared once in the page script, immediately after `initCanvas`:

```js
// Seeded Park-Miller LCG — deterministic, never Math.random()
function lcg(seed) {
    var s = seed;
    return function () { s = (s * 16807) % 2147483647; return s / 2147483647; };
}
```

Each generating chart takes its own generator with its own fixed integer seed. Every number printed on a chart that is a statistic OF the plotted data is computed in JS at render time from the plotted values — never hardcoded.

**Chart-form rule for this page:** every pitfall uses a *different* chart form, chosen so the form matches the claim being made. A page of ten near-identical bar charts reads as one chart repeated and teaches nothing by its shape. Forms in use, in page order: **dot-and-whisker**, **histogram**, **outcome-sequence strip**, **horizontal diverging bars**, **bump/slope chart**, **causal DAG**, **waffle grid**, **Bland–Altman plot**, **dumbbell chart**, **scatter with identity line**. No form is reused.

**Naming rule for this page:** all athletes are fictional (Alice, Bob, Carol, Dan, Erin); all clubs, venues, and competitions are generic ("Club A", "Venue A", "Sport A (17 games)"). No real athletes, teams, venues, or leagues. Academic citations (Gilovich et al. 1985; Miller & Sanjurjo 2018) are real published work and are kept as citations. Constructed figures are marked "Illustrative Example."

## Sample Size Per Player Per Season Is Tiny

**17 Games, 560 Attempts, and a Salary Decision**

- **The trap:** A 17-game season gives roughly 560 pass attempts — smaller than many medical trials.
- **Stakes:** Yet that sample drives large salary and roster decisions every offseason.
- **Illustrative Example:** Player Alice's "breakout" year was noise on a ~500-attempt sample.
- **The false sequel:** Her next-year "regression" was the expected variance, not any real decline.
- **Confidence intervals ignored:** A rating point estimate over 17 games carries a wide interval.
- **False precision:** That interval is dropped, and the rating gets treated as an exact measurement.
- **Scaling law:** Interval width shrinks as 1/√n, so 162 games is ~3.1× tighter than 17.
- **What the overlap means:** Two ratings 6 points apart are statistically indistinguishable all season.

### Visualization (canvas `canvas1`, 720×200 drawn area) — dot-and-whisker

Horizontal dot-and-whisker plot: Alice's and Bob's rating intervals at three season lengths. **The form is chosen deliberately — the lesson is "you cannot tell these two players apart," and interval *overlap* shows that directly, where a bar of CI width does not.**

- **Title (bold 14px `#1a5276`):** "Can You Tell These Two Players Apart? — Illustrative Example".
- **Point estimates (the only hardcoded ratings):** Alice **78.0**, Bob **72.0** — a 6.0-point gap that
  every panel repeats. Only the interval width changes between panels.
- **Computation:** relative half-width is `rel(n) = 0.28 * Math.sqrt(17) / Math.sqrt(n)` (the 1/√n law,
  anchored at ±28% for n=17, `K = 0.28 * √17 = 1.1545`). Each player's absolute half-width is
  `rel(n) × rating`, so the taller-rated player also carries the wider interval — as a
  proportional-error model requires.
- **Panels** (three groups, season length label bold 11px `#1a5276` at left of each group):
  | Season | rel | Alice interval | Bob interval | Overlap |
  |---|---|---|---|---|
  | Sport A — 17 games | ±28.0% | 56.2 – 99.8 | 51.8 – 92.2 | **75%** of combined span |
  | Sport C — 82 games | ±12.7% | 68.1 – 87.9 | 62.8 – 81.2 | **52%** |
  | Sport E — 162 games | ±9.1% | 70.9 – 85.1 | 65.5 – 78.5 | **39%** |
  All six interval endpoints and all three overlap shares are computed in JS, never typed.
- **Geometry:** value axis spans **48 – 104** rating points across x = 95 → 690 (contains the realized
  min 51.8 and max 99.8 with margin). Group `g` label at `y = 38 + g*52`; Alice's whisker row at
  `+14`, Bob's at `+30`. Axis ticks at 50, 60, 70, 80, 90, 100 in 9px `#666` along the bottom.
- **Whiskers:** 2px horizontal line in the player's color with 1px vertical end caps (8px tall);
  point estimate a filled radius-4 dot. Alice `#1a5276`, Bob `#e67e22`.
- **Overlap shading:** the intersection of the two intervals filled `rgba(231,76,60,0.13)` spanning
  both whisker rows, with the computed overlap share printed inside it in bold 9px `#e74c3c`.
- **Panel annotation (9px `#666`, right of each group):** "same 6.0-pt gap" — the gap is computed as
  `alice − bob`, so it cannot drift from the plotted dots.
- **Computed footer (bold 10px `#e74c3c`):** the intervals separate only when
  `rel(n) × (alice + bob) < gap`, which solves to `rel < 4.00%` → **n ≈ 833 games**. Rendered as
  "Intervals only separate at ~833 games — no season is that long". The 833 is computed from the
  anchor constant and the two ratings.

## Survivorship Bias in Career Statistics

**A Stable .260 League Average With the Cut Players Deleted**

- **The trap:** Career statistics exist only for players who survived cuts, injuries, and competition.
- **The missing rows:** The thousands cut in preseason never enter the data at all.
- **Illustrative Example:** A league's stable ~.260 batting average excludes fringe call-ups entirely.
- **Why they vanish:** Their brief .180 stints never accumulate into anything called a "career."
- **Draft pick analysis:** A quoted "average first-round career" is conditional on making a roster.
- **Bimodal reality:** Careers are short or long, so the mean describes almost no actual player.
- **Hall of Fame illusion:** Comparing current players to enshrined players ignores talents cut short.
- **What cut them short:** Exclusionary eras and untreatable injuries ended those careers early.

### Visualization (canvas `canvas2`, 720×200 drawn area) — histogram

Histogram of career length with a misleading mean marker. **A histogram is the right form here and is kept: the bimodality that makes the mean meaningless *is* a shape claim, and bar heights are how a shape is read. The mean, the tail shares, and the "nobody is average" share are all computed from the plotted bars.**

- **Title (bold 14px `#1a5276`):** "Career Length Distribution (All Drafted Players) — Illustrative Example".
- **Bars** (baseline y=170, height 130, scale max 0.22) for career years 0–15 with fractions
  `[0.15, 0.21, 0.16, 0.06, 0.03, 0.02, 0.02, 0.04, 0.06, 0.07, 0.06, 0.05, 0.03, 0.02, 0.01, 0.01]`
  (sums to 1.00 exactly). Colors: years 0–2 red `#e74c3c`, years 3–5 amber `#f39c12`, years 6+ green `#27ae60`. X labels "0yr"…"15yr" in 10px `#666`.
- **Computed statistics** (derived in JS from the bar heights, printed in the labels):
  - Mean career length `Σ i·p(i) / Σ p(i)` = **4.5 years** — the dashed marker is positioned at this computed mean, not at a hardcoded x.
  - Share at 0–2 years = **52%**.
  - Share at 7+ years = **35%**.
  - Share within ±1 year of the mean = **5%** — this is the number that makes "nobody is average" true.
- **Mean line:** dashed `#1a5276` vertical line (dash 5/5, width 2) at the computed mean, labeled bold 11px `#1a5276` `← "Average career: 4.5 years"` and below in 11px `#666`: `(bimodal: 52% ≤2yr, 35% ≥7yr — only 5% within ±1yr of the mean)`.
- **Annotation (bold 10px red, upper left):** `52% cut within 2 years (invisible in "career stats")` — same computed share as above, so text and chart cannot drift.

## Hot Hand Fallacy vs Real Streaks

**An 8-Point Statistical Artifact Hid a 3-Point Real Effect**

- **The trap:** Gilovich et al. (1985) declared the hot hand an illusion, and that verdict stuck for decades.
- **The correction:** Miller & Sanjurjo (2018) found a statistical error in the original analysis.
- **The error itself:** Selecting shots that *follow* a streak biases the naive success rate downward.
- **Why it biases:** Conditioning on a make-run makes a finite sequence's remaining shots likelier to miss.
- **Size of the artifact:** In a 60-shot memoryless sequence, the bias is about **8 points** downward.
- **What that flipped:** Correcting it turned a measured "no effect" into a small but real effect.
- **Where it landed:** The effect is real, but far smaller than what players and fans perceive.
- **Measurement difficulty:** Detecting a ~3-point effect against the variance of ~45% shooting is hard.
- **Sample needed:** Separating that signal from noise takes thousands of shot observations.
- **Strategic confound:** "Hot" players draw tighter defense, so observed streaks understate the effect.

### Visualization (canvas `canvas3`, 720×200 drawn area) — outcome-sequence strip

A single 60-shot strip from a **memoryless** shooter, with its streaks marked and the naive conditional rate computed off it. **The form is chosen deliberately: the fallacy is about *sequences*, and a grouped bar chart of conditional rates hides the runs that fool the eye. Showing the strip lets the reader see the streak and the bias in the same object.**

- **Title (bold 14px `#1a5276`):** "One Memoryless Shooter, 60 Shots — Illustrative Example".
- **Generator:** `var rnd = lcg(20257137);` — fixed seed **20257137**. Each shot is
  `rnd() < 0.45`, independent of every previous shot. **There is no hot hand in this data by
  construction** — that is the entire point of the figure.
- **Seed selection criteria (documented so the choice is not cherry-picking a punchline):** the seed
  is the first in a scanned range satisfying all of — exactly 27 makes (**45.0%**, the stated base
  rate, so the strip cannot misrepresent the shooter); longest make-run in 5–7 (near the chance
  expectation of **4.62**, so the streak is typical rather than freakish); at least 6 observations in
  each conditional cell; and realized `P(make | 3 makes) < 0.45 < P(make | 3 misses)`, so the strip
  *reproduces* the Miller–Sanjurjo artifact rather than contradicting it.
- **Strip:** 60 cells, 9px wide with 1px gaps, from x=60 to x=660, at y=52, height 24. Make = filled
  `#27ae60`; miss = `#eceff1` fill with a 1px `#cfd8dc` outline. Realized sequence with this seed:
  `.....MM.MMM....MMMMM.....MMM...MMM..M.M....MM.M.M...M..MMMM.`
- **Streak brackets:** every make-run of length ≥3 gets a 2px `#e67e22` bracket drawn above it with
  its length in bold 9px `#e67e22`. Realized runs (descending): **5, 4, 3, 3, 3**, 2, 2, 1, 1, 1, 1, 1
  → **5 runs of 3 or more** in a shooter with no memory whatsoever.
- **Conditional-rate panel (below the strip, two computed rows):** the naive estimator is applied to
  the plotted strip exactly as a 1985-era analyst would apply it —
  - `P(make | previous 3 makes)` = **3 / 8 = 38%** (bold 11px `#e74c3c`)
  - `P(make | previous 3 misses)` = **6 / 12 = 50%** (bold 11px `#27ae60`)
  Both numerators and denominators are counted in JS from the strip.
- **Computed punchline (bold 11px `#1a5276`):** "Naive gap: −12 pts — this shooter has NO memory".
  The −12 is `(pMakeAfterMakes − pMakeAfterMisses) × 100`, computed. Beneath it in 10px `#666`:
  "Longest run 5 vs 4.62 expected by chance — the streak is not evidence either".
- **Boundary note (10px `#666`), stating the general result behind the single strip:** "Averaged over
  many such sequences the bias is −8.5 pts; the real effect is only about +3". The −8.5 is not a
  statistic of this strip — it is the Monte Carlo expectation of the naive estimator under
  p = 0.45, n = 60 (20,000 seeded sequences: E[naive after 3 makes] = 36.5%, E[naive after 3 misses] =
  50.4%). It is therefore **labeled as a cross-sequence expectation, not as a figure read off this
  chart**, and is a legitimate hardcoded constant with its derivation recorded here.

## Park/Venue Effects on Player Comparisons

**A 1.38 Venue Factor Is 28% of That Hitter's Home Runs**

- **The trap:** Venue effects are massive, so face-value cross-venue comparison is comparing physics.
- **Baseball case:** A high-altitude hitter against a marine-air hitter is not a like-for-like matchup.
- **Football case:** Same for a dome kicker measured against an open-air kicker in December.
- **Illustrative Example:** Player Bob's venue-adjusted average (~.295) was still clearly elite.
- **The narrative cost:** The binary "venue-inflated" story cost him years of award consideration.
- **Incomplete adjustments:** Venue factors are seasonal averages applied to heterogeneous conditions.
- **Noise added:** One blanket correction removes some venue effect while injecting its own noise.

### Visualization (canvas `canvas4`, 720×200 drawn area) — horizontal diverging bars

Horizontal diverging bar chart of venue factors around a neutral centerline. **Horizontal orientation is chosen so this chart's silhouette cannot be confused with the histogram two sections above; the ranked venue names also read better on a horizontal axis. The "how much of a player's output is venue" figure is computed from the factor, not asserted.**

- **Title (bold 14px `#1a5276`):** "Venue Factor Effect on Home Runs (100 = Neutral) — Illustrative Example".
- **Layout:** centerline at x=360 drawn as a dotted `#333` vertical line (dash 3/3) spanning the plot;
  nine rows from y=38 down, row pitch 17px, bar height 12px. Factor axis maps 0.70 → x=110 and
  1.50 → x=690, so 1.00 lands on the centerline. Axis ticks 70/85/100/115/130/145 in 9px `#666` at y=192.
- **Bars** (drawn left or right from the centerline): red `#e74c3c` for factor > 1.0, blue `#3498db`
  for < 1.0, gray `#95a5a6` at exactly 1.0. Venue name 9px `#666` right-aligned at x=104 for
  left-extending bars, left-aligned at x=366 otherwise; factor printed on the 100-scale in bold 9px
  `#1a5276` at the free end of each bar.
- **Rows** (in the order drawn): Venue A 138, Venue B 118, Venue C 112, Venue D 108, League Avg 100, Venue E 92, Venue F 88, Venue G 82, Venue H 78.
- **Computed annotation (bold 10px red, top right):** the inflation percent is `(fMax − 1) × 100` = **38%**, and the
  share of a player's home runs attributable to the venue is `(fMax − 1) / fMax` = **28%** — not
  "nearly half", which was an earlier asserted claim and does not follow from a factor of 1.38.
  The spread between the extreme venues, `(fMax / fMin − 1) × 100` = **77%**, is also computed.
  Rendered on two lines as: "Venue A = 38% inflation → 28% of that hitter's HRs are the venue" /
  "(extremes differ 77%)".

## Era Adjustment Problems

**50.4 PPG Becomes 40.0 and the Leaderboard Reorders**

- **The trap:** Cross-era comparison needs simultaneous adjustment for rules, equipment, and training.
- **More confounds:** Tactics also shifted, as did an artificially restricted talent pool of that era.
- **What restricted it:** Exclusionary rules and the absence of international players shrank competition.
- **No formula fits:** No single adjustment covers all five factors at once, so every number is a guess.
- **Illustrative Example:** Alice's 50.4 PPG becomes 40.0 once her era's fast pace is divided out.
- **The rank flips:** One adjustment moves 4 of 5 players and hands the top spot to a different player.
- **Margin is fiction:** The new leader wins by 0.3%, far inside the error of the pace estimate itself.
- **Unfalsifiable basis:** That adjustment rests entirely on unmeasurable counterfactuals.
- **Pace adjustment trap:** Per-possession stats assume production scales linearly with possessions.
- **Why linearity fails:** Fatigue curves are non-linear, so the 100th possession is not like the 10th.

### Visualization (canvas `canvas5`, 720×200 drawn area) — bump / slope chart

Bump chart: raw scoring rank on the left, pace-adjusted rank on the right, one line per player. **The form is chosen deliberately — the pitfall is that a single adjustment *reorders* the leaderboard, and reordering is exactly what a grouped bar chart hides. Every adjusted value and every rank is computed.**

- **Title (bold 14px `#1a5276`):** "One Adjustment Reorders the Leaderboard — Illustrative Example".
- **Inputs (the only hardcoded numbers):** raw PPG and the era's pace (possessions per game) —
  Alice 50.4 @ 126, Bob 37.1 @ 100, Carol 35.4 @ 92, Dan 36.1 @ 90, Erin 33.1 @ 98. Reference pace **100**.
- **Computation:** `adjusted = raw × 100 / pace`; both rank columns are produced by sorting the
  computed values, so a rank can never contradict its own number.
  | Player | Raw | Pace | Adjusted | Raw rank | Adj rank |
  |---|---|---|---|---|---|
  | Alice | 50.4 | 126 | **40.00** | 1 | **2** |
  | Bob | 37.1 | 100 | **37.10** | 2 | **4** |
  | Dan | 36.1 | 90 | **40.11** | 3 | **1** |
  | Carol | 35.4 | 92 | **38.48** | 4 | **3** |
  | Erin | 33.1 | 98 | **33.78** | 5 | 5 |
- **Geometry:** left column x=210, right column x=510; rank `r` sits at `y = 44 + (r−1) * 30`.
  Column headers bold 11px `#1a5276` at y=32: "Raw PPG rank" and "Pace-adjusted rank".
- **Lines:** 2.5px, drawn as a straight segment between the two rank positions. Colored by movement,
  computed from the rank pair: rising `#27ae60`, falling `#e74c3c`, unchanged `#95a5a6`. Radius-5
  filled dots at both ends in the same color.
- **End labels:** left of the left dot, right-aligned 10px `#333`, `"Alice 50.4"`; right of the right
  dot, left-aligned 10px `#333`, `"40.00"`. Both values formatted from the computed arrays.
- **Computed annotations:**
  - bold 10px `#e74c3c` (top): "4 of 5 players change rank" — the count is
    `players.filter(p => rawRank[p] !== adjRank[p]).length`, computed.
  - bold 10px `#1a5276` (bottom): "New #1 Dan beats Alice by 0.11 PPG = 0.28% — inside the error of
    the pace estimate". Both figures derive from the two top adjusted values.
  - 10px `#666`: "Alice's raw lead was 13.3 pts (36%)" — computed from the raw pair, showing a
    36% lead converted into a 0.3% deficit by one correction.

## Draft Pick Value vs Development System

**High Picks Get the Development Money That Proves the Pick**

- **The trap:** Draft-position success conflates selection quality with development investment.
- **The extra help:** High picks get better coaching, more playing time, and much longer leashes.
- **Never tested:** That development system was never run on later-round picks for comparison.
- **Illustrative Example:** Draft value charts encode outcomes that already include preferential development.
- **Self-fulfilling:** The chart then justifies the same investment that produced its own numbers.
- **International comparison:** Draft-free academy systems show far flatter success curves by ranking.
- **What that implies:** The steep draft gradient is therefore partly an allocation artifact, not talent.
- **Why no fit helps:** Regressing on both collinear causes inflates each standard error ~14×.

### Visualization (canvas `canvas6`, 720×200 drawn area) — causal DAG

A small causal diagram of the confound, with the collinearity statistics computed beside it. **The form is chosen deliberately — "the data cannot separate talent from investment" is a claim about *causal structure and identifiability*, not about the shape of a curve. Two overlapping decay lines showed only that the two series look alike; a DAG names why that is fatal.**

- **Title (bold 14px `#1a5276`):** "Why Draft Value Can't Be Attributed — Illustrative Example".
- **Nodes** (rounded rects, 2px border, 11px bold label centered):
  | Node | Position (x, y, w, h) | Border / fill |
  |---|---|---|
  | `Draft position` | 30, 78, 120, 38 | `#1a5276` / `#eaf2f8` |
  | `Scouted talent` (unobserved) | 250, 32, 130, 38 | `#95a5a6` dashed / `#f4f6f6` |
  | `Development investment` | 250, 124, 150, 38 | `#e67e22` / `#fdf2e6` |
  | `Career value` | 520, 78, 120, 38 | `#27ae60` / `#eafaf1` |
  The unobserved node's border is dashed (5/4) and its label carries a 9px `#95a5a6` "(unobserved)"
  second line — the whole problem is that this node has no column in the data.
- **Edges** (2px arrows with 8px filled heads): `Draft position → Scouted talent`,
  `Draft position → Development investment`, `Scouted talent → Career value`,
  `Development investment → Career value`. The two edges into `Career value` are the two competing
  explanations; both are drawn in their source node's color.
- **The fork marker:** a 9px `#e74c3c` label "same cause" on the pair of edges leaving
  `Draft position`, and a bold 10px `#e74c3c` "?" centered on each edge entering `Career value`.
- **Computed statistics box (right of the DAG, x=520 y=140, 10px):** the two candidate causes are
  near-perfectly collinear in the illustrative series
  `careerValue = [8.5, 7.2, 6.1, 5.5, 5.0, 4.2, 3.0, 2.2, 1.5, 1.0, 0.6]` and
  `devInvestment = [9.0, 8.0, 7.0, 6.2, 5.5, 4.5, 3.0, 2.0, 1.2, 0.8, 0.5]`
  (picks #1, #5, #10, #15, #20, #32, #64, #100, #150, #200, #250). Computed in JS at render time:
  - Pearson `r` = **0.9975**
  - `R²` = **0.9950**
  - `VIF = 1 / (1 − R²)` = **200**
  - standard-error inflation `= √VIF` = **14.2×**
  Rendered as "r = 0.9975 → VIF 200 → every coefficient's standard error inflates 14.2×".
- **Computed punchline (bold 10px `#1a5276`, bottom):** "No sample size fixes this — the two causes
  never vary independently". This is a structural statement, not a statistic, and is not presented
  as one.

## Injury Prediction: High False Positive Rate

**A 5% Base Rate Turns 420 Flags Into 40 Real Injuries**

- **The trap:** At a 5% injury base rate, even a sensitive predictor produces mostly false alarms.
- **The arithmetic:** The vast majority of players a model flags never actually get injured.
- **Illustrative Example:** A club rested 35% of its starters weekly on model flags.
- **What the flags were worth:** About a tenth of those flags panned out, and games were lost to precaution.
- **Counterfactual problem:** Resting a flagged player who stays healthy "confirms" the model instead.
- **Unfalsifiable:** With no untreated control, injury prediction can never be disproven in practice.

### Visualization (canvas `canvas7`, 720×200 drawn area) — waffle grid

A 1,000-dot waffle grid, one dot per player, colored by confusion-matrix cell. **The form is chosen deliberately — base-rate neglect is a failure of *proportional intuition*, and four numbers inside four rectangles do not fix it. One dot per player makes the 380-vs-40 ratio a visual fact. Every cell count is computed from four stated inputs, so the arithmetic closes by construction.**

- **Inputs (the only hardcoded numbers):** N = 1,000 players; base rate = 0.05; sensitivity = 0.80; specificity = 0.60.
- **Derived cells (computed in JS):** injured = 50; caught (TP) = 40; missed (FN) = 10; healthy = 950; correctly cleared (TN) = 570; false alarms (FP) = 380; total flags = TP + FP = 420. Check: 40 + 10 + 570 + 380 = 1,000.
- **Derived rates (computed in JS):** precision = 40/420 = **9.5%**; false-discovery = 380/420 = **90.5%**. The two sum to 100.0%.
- **Title (bold 14px `#1a5276`):** 'Injury Prediction: What "80% Accurate" Actually Means — Illustrative Example'.
- **Grid:** exactly **50 columns × 20 rows = 1,000** dots — the grid size is asserted in code as
  `cols * rows === N` so it can never disagree with the stated population. Origin x=70 y=44, column
  pitch 12.4px, row pitch 6.6px, dot radius 2.4px. Filled row-major in reading order, so the first
  row is the 50 players who matter and the remaining 19 rows are the healthy majority.
- **Fill order and colors** (each block's length is the computed count, never a literal):
  - 40 dots green `#27ae60` — caught (true positive)
  - 10 dots amber `#f39c12` — missed (false negative)
  - 380 dots red `#e74c3c` — false alarm (false positive)
  - 570 dots blue-gray `#aebfcc` — correctly cleared (true negative)
- **Row-1 callout (9px `#666`, right of the grid):** "row 1 = all 50 who get injured" — the 50 is the
  computed `injured`, and the claim is exact because row width equals 50 by the grid assertion.
- **Legend (10px, below the grid at y=190):** four swatches with computed counts, e.g. "Caught 40",
  "Missed 10", "False alarm 380", "Cleared 570".
- **Computed summary (bold 12px red, y=32):** 'Of 420 "high risk" flags, only 40 (9.5%) get injured'.
- **Computed summary (10px `#666`):** '90.5% of rested players would have been fine'.

## GPS Tracking Noise in Player Load

**A +6% Systematic Bias Hidden Under a Wider Session Spread**

- **The trap:** GPS "player load" metrics carry ±5-10% measurement error on every single session.
- **Why that matters:** That error is larger than the workload difference triggering recovery protocols.
- **Illustrative Example:** A club's "20% workload spike" forced a mandatory recovery day.
- **The actual cause:** It was satellite bounce off the stadium roof; corrected distance was normal.
- **Bias, not just noise:** Reflection error is one-sided, so it accumulates instead of cancelling out.
- **Two separate faults:** A +6% systematic bias sits underneath a ±9% session-to-session spread.
- **Why that ordering hurts:** The spread is wider than the bias, so no single session reveals either.
- **Acceleration artifacts:** 10-18Hz sampling misses sprint-level dynamics between sample points.
- **Load understated:** True mechanical load is underestimated 15-30% in high-intensity actions.
- **Indoor limitations:** Indoor positioning systems carry error profiles different from outdoor GPS.
- **No comparison:** Cross-system workload comparisons are therefore meaningless for one athlete.

### Visualization (canvas `canvas8`, 720×200 drawn area) — Bland–Altman plot

Bland–Altman agreement plot: percent difference (GPS − true) against the mean of the two measurements, over 40 sessions. **The form is chosen deliberately — this is a method-agreement question, and Bland–Altman is the standard instrument for exactly that. Two cumulative distance lines made the drift look like a small steady gap; separating bias from limits of agreement shows that the per-session spread is wider than the bias itself, which is the operational problem.**

- **Title (bold 14px `#1a5276`):** "GPS vs True Distance: Agreement Across 40 Sessions — Illustrative Example".
- **Generator:** `var rnd = lcg(20250816);` — fixed seed **20250816**, no `Math.random()`.
- **Simulation (40 sessions):** `trueKm = 8.0 + rnd() * 4.0` (uniform on 8–12 km);
  `gpsKm = trueKm * (1 + 0.065 + (rnd() − 0.5) * 0.16)`. The `+0.065` is the one-sided reflection
  bias; the `(rnd() − 0.5) * 0.16` term is symmetric noise about it. Bias is multiplicative, so the
  percent difference is scale-free and the plot's y-axis is honest across session lengths.
- **Plotted quantities:** `x = (gpsKm + trueKm) / 2` (km), `y = (gpsKm − trueKm) / x × 100` (%).
- **Realized statistics with this seed (all computed at render time from the plotted points):**
  - mean bias = **+6.15%** (solid `#e74c3c` horizontal line, labeled "bias +6.15%")
  - SD of differences = **4.41** points
  - limits of agreement `bias ± 1.96·SD` = **−2.49%** to **+14.78%** (dashed `#e67e22` lines, dash 6/4)
  - LoA width = **17.3** points, i.e. half-width **8.6** points against a **6.1**-point bias — the
    spread is **1.4×** the bias, which is the annotated punchline
  - sessions reading high = **36 of 40**; sessions off by more than 5% = **23 of 40**
  - zero **is** inside the limits of agreement, so a single session genuinely cannot establish the bias
  - season totals: true **402.0 km**, recorded **428.2 km**, overstatement **+26.2 km (+6.5%)** —
    the bias only becomes visible once aggregated
- **Geometry:** x axis 7.5 → 13.5 km across x=70 → 690 (contains realized 8.04 – 12.80);
  y axis −6% → +18% across y=176 → 34 (contains realized −0.65 – +12.93). Zero line solid `#95a5a6`
  1px. Axis ticks 8/9/10/11/12/13 km in 9px `#666`; y ticks −5/0/+5/+10/+15 in 9px `#666`.
- **Points:** radius 3.5, `#e74c3c` at 0.55 alpha when the difference exceeds +5%, `#1a5276` at
  0.55 alpha otherwise — the threshold is the same 5% the bullets cite, and the split count is computed.
- **Computed annotations:**
  - bold 10px `#e74c3c`: "Bias +6.15%, but limits of agreement span −2.5% to +14.8%".
  - bold 10px `#1a5276`: "Session spread is 1.4× the bias — no single session detects either".
  - 10px `#666`: "Over a season: 402.0 km true → 428.2 km recorded (+26.2 km)".
  - axis labels 10px `#666`: "Mean of GPS and true distance (km) →" and rotated "GPS − true (%)".

## Team Effects Inseparable from Individual

**A Receiver's Yardage Is Mostly the Passer's Accuracy**

- **The trap:** Individual metrics are contaminated by teammates, coaching systems, and opponent quality.
- **Game script too:** Good teams create leads that change play-calling, inflating or suppressing stats.
- **The clearest case:** A receiver's statistics depend almost entirely on his passer's accuracy.
- **Illustrative Example:** Alice's production collapsed the moment her primary passer left the club.
- **What buyers paid for:** Teams pricing off the solo number overpaid for a system outcome.
- **Plus-minus limitations:** Isolation metrics like +/- need roughly 2,000 minutes to stabilize.
- **Consequence:** Single-season values are noise, yet they drive free-agent decisions anyway.

### Visualization (canvas `canvas9`, 720×200 drawn area) — dumbbell chart

Dumbbell chart: one row per receiver, a connecting bar from their yardage with the worse passer to their yardage with the better one. **The form is chosen deliberately — this is a *paired* comparison of the same player under two conditions, and a dumbbell encodes the pairing in the connector. Four separate bars invited reading the four as four independent players, which is the opposite of the lesson.**

- **Title (bold 14px `#1a5276`):** "Same Receiver, Different Passer — Illustrative Example".
- **Inputs (hardcoded):** Alice — 1,572 yds (115 rec) with Passer 1, 598 yds (42 rec) with Passer 2.
  Bob — 1,553 yds (123 rec) with Passer 1, 897 yds (67 rec) with Passer 2.
- **Geometry:** yardage axis 0 → 1,800 across x=130 → 690; two rows at y=78 and y=134. Axis ticks
  0/600/1,200/1,800 in 9px `#666` at y=180, with 1px `#eceff1` vertical gridlines.
- **Dumbbells:** 7px connector in `rgba(231,76,60,0.30)` between the two values; radius-7 `#e74c3c`
  dot at the Passer-2 value, radius-7 `#27ae60` dot at the Passer-1 value. Player name bold 11px
  `#1a5276` right-aligned at x=124. Yardage printed beyond each dot in bold 9px in the dot's color;
  reception count in 9px `#666` underneath each.
- **Computed drop labels (bold 10px `#e74c3c`, centered on each connector):** Alice
  `1 − 598/1572` = **−62% (−974 yds)**; Bob `1 − 897/1553` = **−42% (−656 yds)**. Both percent and
  absolute figures are computed from the plotted pair.
- **Computed annotations:** bold 11px `#e74c3c` "42–62% of 'individual' production was the passer"
  (range computed as min/max of the two drops); 10px `#666` "Both rows are the same player twice —
  only the passer changed".

## Regression to Mean Confused with Decline

**Teams Pay for the Peak and Receive the Talent Level**

- **The trap:** Exceptional seasons regress toward the career average by mathematical necessity.
- **Not a diagnosis:** The following drop is arithmetic, not evidence of any physical decline.
- **Illustrative Example:** A "cover-athlete curse" and the "sophomore slump" are both pure regression.
- **The selection step:** Athletes chosen for outlier seasons simply revert to their true talent level.
- **Contract disasters:** Teams pay free agents for the contract-year peak, the highest point on record.
- **What they receive:** The expected regression back to the multi-season talent level instead.
- **Why the peak lies:** A career year is talent plus good luck; only the talent part repeats.

### Visualization (canvas `canvas10`, 720×200 drawn area) — scatter with identity line

Scatter plot: career-year performance vs next-year performance, mostly below the identity line. **A scatter against y = x is the right form and is kept: the claim is about where a cloud of players sits relative to "repeats exactly," and that reference line is only expressible in a scatter. Seeded, with mean-zero selection noise, and every reported mean is computed from the plotted points.**

- **Title (bold 14px `#1a5276`):** "Career Year → Next Year: Regression to Mean (Not Decline) — Illustrative Example".
- **Axes:** light gray `#ccc` L-axes; baseline y=170, height 120; value range 0.54–0.76 on both axes (chosen to contain the realized min 0.555 and max 0.748); x-axis label 11px `#666` "Career Year Performance →"; rotated y-axis label "Next Year →".
- **Generator:** `var rnd = lcg(20250812);` — fixed seed **20250812**, no `Math.random()`.
- **Simulation (100 candidate players, then selected):**
  - `trueTalent = 0.40 + rnd() * 0.30` — talent uniform on [0.40, 0.70].
  - `careerYear = trueTalent + (rnd() - 0.5) * 0.15` — **symmetric, mean-zero luck**. This matters:
    a strictly positive luck term would inflate every career year and make the apparent
    "regression" partly a generator artifact. With mean-zero luck, the selection filter — not the
    noise distribution — is what makes the selected group's career year exceed its talent, which is
    exactly the phenomenon being taught.
  - `nextYear = trueTalent + (rnd() - 0.5) * 0.10` — independent mean-zero luck, same talent.
  - **Selection:** keep players with `careerYear > 0.62` (selected on the high tail).
- **Realized statistics with this seed (computed at render time, printed on the chart):**
  - Candidates simulated: 100. Selected: **30**. (The remaining 70 are not plotted.)
  - Mean career year of the selected group = **0.674**
  - Mean true talent of the selected group = **0.645** — below their career year, because selection
    picked up positive luck.
  - Mean next year of the selected group = **0.640** — within noise of their talent, not of their peak.
  - Realized regression = 0.674 − 0.640 = **0.033**, a **5.0%** drop from the peak.
  - Points below the identity line: **24 of 30 = 80%**.
- **Points:** radius 5, red `#e74c3c` when next year < career year, green `#27ae60` otherwise.
- **Identity line:** dashed green `#27ae60` diagonal (dash 5/5, width 1.5) from (ox, oy) to (ox+w, oy−h); valid as the y=x line because both axes use the same 0.54–0.76 range.
- **Computed annotations:** 10px green "— — If no regression" (top right); bold 11px red
  "24 of 30 below the line (80%): mean 0.674 → 0.640, a 5.0% drop"; 10px `#666`
  "Selected group's true talent = 0.645 — next year matches talent, not the peak". Every one of
  those figures is computed from the plotted array.

## Regeneration instructions

- **Layout:** domains detail-page style — h1, `.subtitle` paragraph, then one `<h2>` per pitfall (unnumbered, `border-bottom: 2px solid #2980b9`), each followed by a single-row `.obj-table`: left `<td>` (50%) with `.obj-title` div + `<ul>` of bold-labeled one-sentence bullets, right `<td>` (50%, centered) with the canvas. Even table rows have background `#fafcfe`. No thead, no nav, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; canvas rule `width: 100%; max-width: 720px; height: auto`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) though unused.
- **Canvas:** the HTML attributes declare `width="720" height="300"`, but a shared `initCanvas(id)` helper sizes the backing store to the displayed width × `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates — effective drawing area is 720×200. Charts have white (unfilled) backgrounds and use font family `-apple-system`.
- **Chart-form inventory (enforce on every regeneration — no two pitfalls may share a form):**
  | Canvas | Pitfall | Form |
  |---|---|---|
  | canvas1 | Tiny per-season sample | dot-and-whisker |
  | canvas2 | Survivorship | histogram |
  | canvas3 | Hot hand | outcome-sequence strip |
  | canvas4 | Venue effects | horizontal diverging bars |
  | canvas5 | Era adjustment | bump / slope chart |
  | canvas6 | Draft value | causal DAG |
  | canvas7 | Injury false positives | waffle grid |
  | canvas8 | GPS noise | Bland–Altman plot |
  | canvas9 | Team effects | dumbbell chart |
  | canvas10 | Regression to mean | scatter with identity line |
- **Determinism:** the `lcg(seed)` helper is declared once, right after `initCanvas`. Seeds in use: **20257137** (canvas3, shot sequence), **20250816** (canvas8, GPS agreement), **20250812** (canvas10, regression to mean). No other chart generates data. `Math.random()` must never appear in this page.
- **Computed-label rule:** any figure that is a statistic of generated or plotted data (interval endpoints, overlap shares, run lengths, conditional rates, means, biases, limits of agreement, ranks, drops, counts, shares, correlations, VIF, precision/false-discovery rates) is computed in JS from the plotted values and formatted into the label string. Hardcoded statistics are a defect.
- **Two documented exceptions**, both cross-sequence expectations rather than statistics of a plotted series, and both labeled as such on the page: the **−8.5 pt** Miller–Sanjurjo bias and the **+3 pt** true hot-hand effect on canvas3. Their derivations are recorded in that section's spec.
- **Palette:** primary blue `#1a5276`, blue `#3498db`, green `#27ae60` (dark `#1e8449`), red `#e74c3c` (dark `#c0392b`), orange `#e67e22`, amber `#f39c12`, gray `#95a5a6`, blue-gray `#aebfcc`, text grays `#666`/`#333`/`#999`.
- Card/page links in regenerated HTML use `.html` extensions.
