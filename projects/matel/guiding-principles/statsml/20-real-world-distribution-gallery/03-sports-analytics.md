# Real-World Distributions — Sports: What the Data Overturned

**Page type:** detail page (three-column obj-table layout: text left ~38%, primary canvas middle ~31%, insight canvas right ~31%; one table per section)
**HTML title tag:** Real-World Distributions — Sports: What the Data Overturned

## Penalty Kicks — Why Do Goalkeepers Dive?

**Pitfall label (color `#795548`):** ACTION BIAS

**Belief:** Keepers must dive left or right. Standing still = not trying.

**Data:** Across 286 elite penalties, kicks were spread roughly evenly across left, center, right. Keepers dove 94% of the time — but their save rate when staying center (~33%) was more than double the rate when diving (~13%). Against that kick distribution, the best move is to stay put. They don't — the authors' explanation: "I dove and missed" feels better than "I stood there and missed."

- Optimal strategy is publicly known since 2007. Elite pros still don't follow it.
- Finding the right answer and acting on it are different problems.

*Source: Bar-Eli, Azar, Ritov, Keidar-Levin & Schein (2007), J. Economic Psychology 28(5)*

### Visualization (canvas `canvas1`, 420×340)

Bar chart (shared `drawBarChart` helper): where kicks go vs where keepers go.

- **Title:** "Where Kicks Go vs Where Keepers Go (%)".
- **Data:** Kick Left 32, Kick Center 29, Kick Right 39, Dive Left 49, Stay Center 6, Dive Right 45 (percent, values shown on top of bars with "%" suffix).
- **Bar colors:** kick bars `rgba(41,128,185,0.6)` (×3); Dive Left/Right `rgba(231,76,60,0.6)`; Stay Center `rgba(39,174,96,0.7)`. Bar strokes `#1a5276` 0.5px.
- **Axes:** y "Percentage", max 60%, ticks with "%" suffix at 5 levels; light `#eee` gridlines; axes `#333`; title bold 12px `#1a5276`; x labels `#555` 10px (labels containing `\n` render as stacked lines, 12px apart); white background; padding left 55, right 20, top 45, bottom 55.

### Visualization (canvas `canvas1b`, 400×340)

Bar chart (shared `drawBarChart` helper): save rate by keeper action.

- **Title:** "Save Rate by Keeper Action".
- **Data:** Dive Left 14, Stay Center 33, Dive Right 13 (percent, values shown with "%" suffix).
- **Bar colors:** `rgba(231,76,60,0.5)`, `rgba(39,174,96,0.7)`, `rgba(231,76,60,0.5)`.
- **Axes:** x label "Keeper Decision", y label "Save Rate", y max 45%; same shared-helper styling as above.

## Cricket — Does Winning the Toss Actually Matter?

**Pitfall label (color `#2980b9`):** LOUD ≠ IMPORTANT

**Belief:** Commentators treat the toss as near-decisive. Captains build their strategy around it.

**Data (ODIs):** After controlling for home advantage and team strength, the toss has near-zero predictive value. Home advantage is large and real. The variable everyone talks about barely moves outcomes; the one that dominates gets far less airtime.

**Nuance (T20s):** On deteriorating subcontinental pitches, toss becomes significant — chasing teams win more. Context changes which variables matter. The lesson: always ask "under what conditions?"

- Airtime a variable gets ≠ predictive power it has.
- A finding that holds in one format may not hold in another.

*Source: de Silva & Swartz (1997); Allsopp & Clarke (2004), JRSS-A 167(4)*

### Visualization (canvas `canvas2`, 420×340)

Bar chart (shared `drawBarChart` helper): ODI win % by toss result vs home/away.

- **Title:** "ODI Win % — Toss vs Home Advantage".
- **Data:** Won Toss 50.3, Lost Toss 49.7, Home 59.8, Away 40.2 (percent, values shown with "%" suffix).
- **Bar colors:** toss bars gray `rgba(149,165,166,0.6)` (×2); Home `rgba(39,174,96,0.6)`; Away `rgba(231,76,60,0.5)`.
- **Axes:** y "Win %", max 75%; shared-helper styling.

### Visualization (canvas `canvas2b`, 400×340)

Line chart (shared `drawLineChart` helper): IPL chase win % over seasons.

- **Title:** "IPL: Win % When Chasing (Illustrative)".
- **X labels:** 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024.
- **Lines:** chase win % `[51, 53, 52, 55, 54, 58, 57, 55, 56]` in `rgba(39,174,96,0.9)` 2.5px with 4px dots; flat 50% baseline `[50 ×9]` in `rgba(149,165,166,0.6)` 1.5px dashed (5/3).
- **Legend (top right):** "Chase win %" (green), "50% baseline" (gray dashed).
- **Annotation:** bold 11px `#27ae60` text "T20: toss matters" at (180, 58).
- **Axes:** x "IPL Season", y "Chase Win %", y max 70% with "%" suffix; `#eee` gridlines; axes `#333`; white background; padding left 55, right 25, top 45, bottom 55.

## Soccer — Home Advantage Points at the Referee, Not the Players

**Pitfall label (color `#27ae60`):** NATURAL EXPERIMENT

**Belief:** Home teams win more because the crowd energizes them. Players perform better at home.

**Data:** Referees added more injury time when home teams were behind. Then COVID emptied stadiums — and provided an accidental control group. Player metrics (shots, possession) barely changed. But away-team yellow cards dropped, and home advantage in points shrank. The evidence is consistent with the crowd bending the referee more than lifting the players.

- A pandemic accidentally produced the control group science needed.
- A century-old effect attributed to player psychology may be largely referee psychology.

*Source: Garicano, Palacios-Huerta & Prendergast (2005), Rev. Econ. & Stats 87(2); Bryson et al. (2021), Economics Letters 198*

### Visualization (canvas `canvas3`, 420×340)

Bar chart (shared `drawBarChart` helper): added time vs home team score margin.

- **Title:** "Added Time vs Home Score Margin (Stylized)".
- **Data:** Behind 2+ → 4.2, Behind 1 → 3.8, Tied → 3.1, Ahead 1 → 2.6, Ahead 2+ → 2.3 (minutes, values shown).
- **Bar colors (red→gray→green):** `rgba(231,76,60,0.6)`, `rgba(231,76,60,0.4)`, `rgba(149,165,166,0.4)`, `rgba(39,174,96,0.4)`, `rgba(39,174,96,0.6)`.
- **Axes:** x "Home Team Status at 90 min", y "Avg Added Time (min)", y max 5.5; shared-helper styling.

### Visualization (canvas `canvas3b`, 400×340)

Bar chart (shared `drawBarChart` helper): yellow cards with crowds vs empty stadiums.

- **Title:** "Yellow Cards: Crowds vs Empty Stadiums (Stylized)".
- **Data:** "Crowds Home Cards" 1.5, "Crowds Away Cards" 2.2, "Empty Home Cards" 1.8, "Empty Away Cards" 1.9 (two-line x labels; values shown).
- **Bar colors:** green `rgba(39,174,96,0.5)`, red `rgba(231,76,60,0.5)`, green `rgba(39,174,96,0.5)`, red `rgba(231,76,60,0.5)`.
- **Axes:** y "Cards per Match", max 3.0; shared-helper styling.

## NFL Draft — The #1 Pick Is Not the Most Valuable Asset

**Pitfall label (color `#e74c3c`):** NET-OF-COST THINKING

**Belief:** Higher pick = more valuable. Teams sacrifice future picks to trade up. The entire NFL draft market is priced on this.

**Data:** Top picks produce more on-field output — but not enough more to cover their salary. Surplus value (performance minus compensation) peaks in the late first / early second round. Teams trading up were systematically overpaying. Billion-dollar organizations mispriced their most-discussed asset for decades.

- Gross value ≠ net value. Cost matters.
- The market's implied trade curve was far steeper than the performance curve justified.

*Source: Massey & Thaler (2013), Management Science 59(7)*

### Visualization (canvas `canvas4`, 420×340)

Two-line chart (shared `drawLineChart` helper): market trade value vs actual surplus value across picks 1–60.

- **Title:** "NFL Draft: Market Value vs Actual Surplus".
- **Data:** for picks i = 1…60 — market trade value `3000 * exp(-0.06 * (i-1))` (steep exponential decay); actual surplus value `600 * exp(-0.5 * ((i - 28) / 18)^2)` (Gaussian bump peaking around pick 28).
- **Lines:** trade value `rgba(231,76,60,0.8)` 2.5px; surplus value `rgba(39,174,96,0.8)` 2.5px. Y max 3200, y label "Value (index)", x label "Pick Number", x tick labels drawn every ~8 picks.
- **Legend:** "Market trade value" (red), "Actual surplus value" (green).
- **Annotations:** bold 10px red "Teams overpay here" at (80, 70) left-aligned; bold 10px green "↑ Best value zone" at (200, 150) centered.

### Visualization (canvas `canvas4b`, 400×340)

Bar chart (shared `drawBarChart` helper): overpay factor (market value / surplus value) by pick range.

- **Title:** "Overpay Factor by Pick Range".
- **Data:** pick buckets 1-5, 6-10, 11-15, 16-20, 21-30, 31-40, 41-50, 51-60; value = average trade value / average surplus value per bucket from the canvas4 curves, capped at 12 (values shown). The 1-5 bucket is the largest; ratios fall toward 1 in the mid rounds.
- **Bar colors by ratio:** ratio > 3 → `rgba(231,76,60,0.6)`; ratio > 1.5 → `rgba(230,126,34,0.5)`; otherwise `rgba(39,174,96,0.5)`.
- **Axes:** x "Pick Range", y "Market / Surplus Ratio", y max 14; shared-helper styling.

## NBA — The Hot Hand: Everyone Was Wrong. Twice.

**Pitfall label (color `#8e44ad`):** ESTIMATOR BIAS

**Belief (pre-1985):** Players get "hot" — makes breed makes. Every coach and player believed this.

**Data (Act I, 1985):** Shooting % after a make was no higher than after a miss. "Hot hand" was declared a cognitive illusion. Textbook gospel for 30 years.

**Data (Act II, 2018):** The 1985 study had a hidden bias. Measuring P(hit | previous hit) in finite sequences is systematically biased downward. Correcting the bias: the hot hand reappears at +8 to +11 percentage points. The players were right. The professors had a measurement artifact.

- The debunking was wrong — and nobody caught it for 33 years.
- Lesson: question the estimator, not just the data.

*Source: Gilovich, Vallone & Tversky (1985), Cognitive Psychology 17(3); Miller & Sanjurjo (2018), Econometrica 86(6)*

### Visualization (canvas `canvas5`, 420×340)

Custom histogram: simulated finite-sample bias in P(H|H) for fair coins.

- **Title (bold 12px, `#1a5276`):** "P(Hit | Previous Hit) from Fair Coin, n=20".
- **Data generation:** 5000 simulated sequences of 20 fair coin flips (seeded mulberry32 RNG); for each sequence compute P(hit | previous hit); histogram into 25 bins over [0, 1]; also compute the mean across simulations (lands near ~0.46, below 0.5).
- **Bars:** fill `rgba(41,128,185,0.35)`, stroke `#1a5276` 0.5px; `#eee` gridlines.
- **Reference lines:** vertical dashed (5/3) gray `rgba(149,165,166,0.8)` line at 0.50 labeled "0.50 (naive expectation)" in `#888` 10px; solid red `#e74c3c` 2.5px vertical line at the simulated mean labeled bold red "Actual mean: 0.NNN" (three decimals); horizontal red arrow between the two lines labeled bold red "← BIAS".
- **Axes:** x ticks 0.0–1.0 in 0.2 steps, x title "P(Hit | Previous Hit)", rotated y title "Frequency"; axes `#333`; white background; padding left 55, right 20, top 45, bottom 55.

### Visualization (canvas `canvas5b`, 400×340)

Conceptual bell-curve sketch reinterpreting the 1985 result against the corrected null.

- **Title (bold 12px, `#1a5276`):** "Reinterpreting 1985 — Conceptual Sketch".
- **Curve:** Gaussian null distribution centered at 0.46 with sd 0.08, drawn over x range 0.2–0.8; fill `rgba(41,128,185,0.2)`, stroke `rgba(41,128,185,0.7)` 2px, height scaled to 85% of plot; `#eee` gridlines.
- **Reference lines:** dashed gray `rgba(149,165,166,0.7)` vertical at 0.50 labeled "0.50" below the axis and "(naive null)" near the top; blue `rgba(41,128,185,0.8)` 2px vertical at 0.46 labeled bold "Correct null: 0.46"; red `#e74c3c` 3px vertical at 0.48 labeled bold "Observed: 0.48".
- **Annotations (left-aligned, bold 10px, near bottom of plot):** red "GVT said: 0.48 < 0.50 → no hot hand"; green `#27ae60` "Reality: 0.48 > 0.46 → hot hand IS real".
- **Axes:** x ticks 0.2–0.8 in 0.1 steps, x title "P(Hit | Previous Hit)", rotated y title "Null Distribution Density"; axes `#333`; white background.

## NBA — Publishing a Finding Erased the Bias

**Pitfall label (color `#e67e22`):** MEASUREMENT CHANGES THE SYSTEM

**Belief:** Professional officials are neutral. The league stated as much.

**Data (1991–2004):** Players drew ~4% fewer fouls when the officiating crew's racial composition didn't match their own. Large enough to shift close games.

**After 2007 media coverage:** No policy change was identified. But the bias vanished in subsequent seasons and stayed gone. The measurement itself changed the thing being measured.

- Consistent with unconscious bias rather than intent — it faded once it became visible.
- Analytics isn't a passive observer of the system it studies.

*Source: Price & Wolfers (2010), QJE 125(4); Pope, Price & Wolfers (2018), Management Science 64(4)*

### Visualization (canvas `canvas6`, 420×340)

Line chart (shared `drawLineChart` helper): bias coefficient by season with a 2007 intervention marker.

- **Title:** "Racial Bias Coefficient by Season (Stylized)".
- **X labels (seasons):** '92, '94, '96, '98, '00, '02, '04, '06, '08, '10, '12, '14, '16.
- **Lines:** bias coefficient `[3.8, 4.2, 3.9, 4.5, 4.1, 3.7, 4.0, 3.6, 1.2, 0.5, 0.3, -0.2, 0.1]` in `rgba(231,76,60,0.9)` 2.5px with dots; zero line (13 zeros) in `rgba(149,165,166,0.5)` 1.5px dashed (5/3). Y max 6, y label "Bias Estimate (%)", x label "Season".
- **Vertical marker:** dashed (4/2) purple `#8e44ad` 2px line at x fraction 7.5/12 labeled bold "2007: Published" below the axis.
- **Legend:** "Bias coefficient" (red), "Zero (no bias)" (gray dashed).
- **Annotations:** bold 10px red "Bias present" at (100, 65); bold 10px green `#27ae60` "Bias gone" at (320, 200).

### Visualization (canvas `canvas6b`, 400×340)

Bar chart (shared `drawBarChart` helper): favorable foul calls before vs after publicity.

- **Title:** "Foul Calls Won: Before vs After Publicity (Stylized)".
- **Data (two-line x labels):** "Before 2007 Own-Race Crew" 52.1, "Before 2007 Cross-Race Crew" 48.3, "After 2007 Own-Race Crew" 50.2, "After 2007 Cross-Race Crew" 50.0 (percent, values shown with "%" suffix).
- **Bar colors:** `rgba(39,174,96,0.5)`, `rgba(231,76,60,0.5)`, `rgba(39,174,96,0.5)`, `rgba(39,174,96,0.5)`.
- **Axes:** y "Favorable Calls %", max 58%; shared-helper styling.

## Penalty Shootouts — The Finding That Didn't Hold Up

**Pitfall label (color `#16a085`):** REPLICATION FAILURE

**Published finding (2010):** The team kicking first wins ~60.5% of shootouts (n≈269). Psychological pressure on second kickers. FIFA trialed an alternative "ABBA" kicking order based on this.

**What happened:** Later replications with larger samples found no first-kicker advantage. The confidence interval narrowed and converged on 50%. A real journal, a real policy response — and the finding didn't hold up.

- Same structure as every other example here: surprising finding + story. But this one didn't replicate.
- The discriminator is sample size and independent replication, not how good the story sounds.

*Source: Apesteguia & Palacios-Huerta (2010), AER 100(5); Kocher, Lenz & Sutter (2012), Mgmt Science 58(8); Arrondel et al. (2019), J. Econ. Psychology 70*

### Visualization (canvas `canvas7`, 420×340)

Custom forest-style plot: first-kicker win rate with confidence intervals across three studies.

- **Title (bold 12px, `#1a5276`):** "First-Kicker Win Rate: As Samples Grew".
- **Studies (name, n, rate, CI low, CI high):** Apesteguia 2010, n=269, 60.5% [54.6, 66.4]; Kocher 2012, n=540, 53.3% [49.1, 57.5]; Arrondel 2019, n=1001, 50.4% [47.3, 53.5].
- **Y scale:** 40% to 70%; `#eee` gridlines at 6 intervals; y tick labels every 5% in `#555`.
- **Reference:** horizontal dashed (5/3) gray `rgba(149,165,166,0.7)` line at 50% labeled right-aligned `#888` 9px: "50% (no advantage)".
- **Marks per study:** vertical CI bar 3px with 16px end caps and a 6px point; colors — study 1 red `rgba(231,76,60,0.8/0.9)`, study 2 orange `rgba(230,126,34,0.8/0.9)`, study 3 green `rgba(39,174,96,0.8/0.9)`; bold value label "60.5%"/"53.3%"/"50.4%" in matching solid color (`#e74c3c`/`#e67e22`/`#27ae60`) above each CI; study name and "n=NNN" below the axis in `#333` 10px.
- **Annotation:** thin `#1a5276` diagonal arrow from the 2010 estimate down toward the 50% line, labeled bold 10px `#1a5276`: "Converging to 50%".
- **Axes:** x title "Study", rotated y title "First-Kicker Win %"; axes `#333`; white background; padding left 55, right 25, top 45, bottom 55.

### Visualization (canvas `canvas7b`, 400×340)

Bar chart (shared `drawBarChart` helper): confidence interval width shrinking as sample size grows.

- **Title:** "Confidence Interval Width vs Sample Size".
- **Data (two-line x labels):** "n=269 (2010)" 11.8, "n=540 (2012)" 8.4, "n=1001 (2019)" 6.2 (percentage points, values shown).
- **Bar colors:** `rgba(231,76,60,0.5)`, `rgba(230,126,34,0.5)`, `rgba(39,174,96,0.5)`.
- **Axes:** x "Study (cumulative evidence)", y "CI Width (pp)", y max 15; shared-helper styling.

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per section, single `<tr>` with three `<td>`s: text cell (38%) holding `<span class="pitfall-label">`, `<h3>`, belief/data `<p>` blocks with `<strong>` lead-ins, `<ul>`, and a `<p class="source">` citation line; middle cell (31%, centered) holding the primary canvas (width=420, height=340); right cell (31%, centered) holding the insight canvas (width=400, height=340).
- **Page CSS:** body system sans-serif (-apple-system stack), margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; table cells `1px solid #2980b9` border, 12px padding; h3 `#1a5276` 1.0em weight 700; p 14px line-height 1.6; li 14px line-height 1.5; `.pitfall-label` inline-block bold 0.72em uppercase letter-spacing 0.5px; `.source` 11px `#666` italic; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned in document order from the cycling array `["#795548", "#2980b9", "#27ae60", "#e74c3c", "#8e44ad", "#e67e22", "#16a085", "#d35400", "#c0392b", "#1abc9c"]` via a small script setting `style.color` on each `.pitfall-label`.
- **Charts:** shared `drawBarChart(canvasId, data, options)` helper — white background, title bold 12px `#1a5276` centered, `#eee` gridlines at quarters, bars inset 4px per slot with `#1a5276` 0.5px strokes, optional value labels bold 11px `#333` above bars, x tick labels `#555` 10px, y ticks at quarters with optional suffix, axis titles `#333` 11px (y rotated), padding left 55 / right 20 / top 45 / bottom 55 — and shared `drawLineChart(canvasId, options)` helper (same frame; lines with optional dash/points, dashed vertical markers with labels, legend swatches top right, annotations at pixel coordinates). Charts 5, 5b, and 7 are custom-drawn with the same styling. Seeded mulberry32 RNG (seed 42) with Box-Muller `randn()` available. All canvases scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray `rgba(149,165,166,…)`, bar fill `rgba(26,82,118,0.35)` default.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions.
