# Betting & Gambling — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left ~38%, primary canvas middle ~31%, insight canvas right ~31%; one table per section; centered page subtitle under h1)
**HTML title tag:** Betting & Gambling — Distribution Patterns

**Subtitle (`.page-sub`):** Results that contradict what regular bettors believe. No statistics background assumed — every claim is backed by the chart beside it. All charts are simulations, so you can see the mechanism producing the shape rather than taking anyone's word for it.

## A Fair Coin Keeps You Stuck on One Side

**Pitfall label (color `#2980b9`):** U-SHAPED, NOT BELL-SHAPED

**Belief callout:** **What most people believe:** In a totally fair 50/50 game, you would spend the night hovering around break-even, with the lead swapping back and forth constantly.

The opposite happens. The most likely outcome by far is spending nearly the *whole* night on one side — either ahead almost the entire time, or behind almost the entire time. Hovering near even is the *rarest* outcome, not the default. Nothing is wrong with the coin: this is simply what fair games look like, and it is why a losing night feels like a rigged night.

- The chart is a U, not a hill — the two tallest bars are "behind all night" and "ahead all night"
- Spending roughly half the night ahead, which feels like the normal case, is the least common result of all
- Once a fair game drifts one way, it has no pull back toward zero — there is no force restoring balance
- Bettors read a night spent entirely behind as evidence of a fixed game. In a fair game it is the single most likely night

*Term note:* If you want to look it up: the arcsine law.

### Visualization (canvas `canvas1`, 420×360)

Histogram (shared `drawHistogram` helper) with U shape: share of each night spent in profit.

- **Title:** "How Much of the Night Was Spent Ahead?"; **subtitle (`#7f8c8d` 11px):** "4,000 nights of a perfectly fair 50/50 game, 400 bets each".
- **Data generation:** seeded mulberry32(1101); 4,000 nights × 400 fair ±1 bets; record fraction of bets with a positive running total per night.
- **Bins/axes:** 20 bins over [0, 1]; x tick labels "0%", "20%", "40%", "60%", "80%", "100%"; x-axis label "Share of the night spent in profit"; rotated y title "How many nights"; y axis marked only "most" (top) and "none" (bottom) in `#555` 10px.
- **Colors:** bar fill `rgba(41,128,185,0.35)`, stroke `#1a5276`; smoothed density line `#922b21` 2px with SE band `rgba(231,76,60,0.18)`; axes `#999`; title bold 13px `#1a5276`.
- **Notes (positioned as fractions of plot):** bold two-line "NN% of nights were spent almost" / "entirely on ONE side" (computed share of nights with <10% or >90% time ahead) in default `#c0392b`; bold two-line `#2c3e50` "Only NN% were" / "anywhere near even" (share between 40% and 60%).

### Visualization (canvas `canvas1b`, 400×360)

Spaghetti plot of 24 random-walk paths colored by which side they lived on.

- **Title:** "The Same Nights, Drawn Out"; **subtitle:** "Blue = mostly ahead · Red = mostly behind".
- **Data:** the first 24 simulated night paths (running totals over 400 bets), y clamped to ±60 units.
- **Lines:** 1.2px each — `rgba(41,128,185,0.55)` if the night spent >50% of the time ahead, else `rgba(231,76,60,0.55)`.
- **Break-even line:** horizontal dashed (4/3) `rgba(44,62,80,0.55)` 1.5px at y=0, labeled bold 10px `#2c3e50` "BREAK EVEN".
- **Annotation (right-aligned, `#7f8c8d` 10px):** "Notice how few lines cross back and forth".
- **Axes:** x title "Bets placed through the night"; rotated y title "Up / down (units)"; y tick labels "+60", "0", "-60"; axes `#999`; margins top 52, right 16, bottom 52, left 46.

## Wins 39 Nights out of 40, Loses Money Anyway

**Pitfall label (color `#8e44ad`):** ALMOST ALWAYS WINS, STILL LOSES

**Belief callout:** **What most people believe:** Doubling your stake after every loss has to work — you cannot lose forever, and a single win wipes out the whole losing run.

It genuinely does win on the large majority of nights, which is exactly why the idea refuses to die — everyone who tries it has a stack of small winning nights as proof. The catch is what the rare bad night costs. One run of nine losses in a row wipes out fifty good nights and a bit more. A strategy can be right 97% of the time and still be a losing strategy, because how often you win and how much you win are separate questions.

- Simulated as: walk in with £511, keep doubling after each loss, quit once £10 ahead
- About 39 nights in 40 end exactly as planned — £10 up, home early, feeling clever
- The odd night out loses virtually the entire £511, because nine reds in a row is not rare once you play often enough
- The average night still comes out *down*, so the one tiny bar on the left outweighs the enormous bar on the right

*Term note:* If you want to look it up: the martingale, and why a high win rate is not an edge.

### Visualization (canvas `canvas2`, 420×360)

Histogram (shared `drawHistogram` helper) of martingale night results — two isolated spikes.

- **Title:** "How Each Night Ended"; **subtitle:** "20,000 nights: bring £511, double after every loss, quit £10 up".
- **Data generation:** seeded mulberry32(2202); 20,000 nights; start £511, target +£10, win probability 18/37 (European wheel); double stake after each loss, reset to 1 after a win, stop when the next double cannot be covered; record night profit/loss.
- **Bins/axes:** 54 bins over [-560, 60]; x-axis label "Result for the night (£)", integer x ticks; rotated y title "How many nights"; y marked "most"/"none".
- **Colors:** bar fill `rgba(142,68,173,0.35)`, stroke `#1a5276`; **no density overlay** (`density:false` — the smoothed line + SE band would smear the two isolated spikes that are the whole point of this chart).
- **Reference line:** vertical dashed (5/3) `rgba(44,62,80,0.7)` at £0 labeled "break even" (right-aligned).
- **Notes:** purple `#6c3483` bold "NN.N% of nights end here" with "(+£10, home early)" beneath (near the right spike, ~97-98%); bold "NN.N% end here" / "losing nearly the lot" near the left spike; bold red `#c0392b` 12px "Average night: £-N.NN" (computed mean, negative).

### Visualization (canvas `canvas2b`, 400×360)

Line chart: fifty small wins then one catastrophic loss (sawtooth-and-cliff).

- **Title:** "Fifty Good Nights and One Bad One"; **subtitle:** "Running total across 51 nights".
- **Data:** running total starting at 0, +£10 per night for 50 nights (reaching +£500), then a single -£511 bust night (stakes 1+2+4+…+256 all lost), ending at a net of -£11; y range -60 to 520.
- **Series:** climb segment `rgba(39,174,96,0.9)` 2.2px; cliff segment `rgba(231,76,60,0.95)` 2.8px; green `#27ae60` 4px dot at the peak labeled bold 10px `#1e8449` "+£500 after 50 wins"; red `#c0392b` 4px dot at the end labeled bold 11px two-line "One loss undoes" / "all fifty wins".
- **Zero line:** horizontal dashed (4/3) `rgba(44,62,80,0.5)`.
- **Axes:** x title "Nights played"; rotated y title "Running total (£)"; y tick labels "+500" and "0"; axes `#999`; margins top 52, right 16, bottom 52, left 52.

## Right About Every Match, Still Broke

**Pitfall label (color `#e67e22`):** TOO MUCH OF A GOOD THING

**Belief callout:** **What most people believe:** If you have genuinely worked out an edge over the bookmaker, bet big — that is the whole point of having an edge.

Stake too large a share of your pot and your long-run result turns *negative*, even though every individual bet is in your favour. The reason is that losses bite harder than wins heal: lose 40% then win 40% and you are down, not level. Past roughly twice the ideal stake size, going broke becomes near-certain no matter how good your judgement is. Having an edge and surviving long enough to collect it are two completely separate problems.

- The bettor here wins 55 bets in 100 at even money — a real, large, genuine edge, over 500 bets
- Staking 10% of the pot each time multiplies it about twelvefold; staking 40% destroys it almost every time
- Same picks, same edge, same luck — only the stake size differs, and it decides everything
- Over 99% of the greedy bettors end up poorer than when they started, while still being right more often than wrong

*Term note:* If you want to look it up: the Kelly criterion, and why growth is multiplied, not added.

### Visualization (canvas `canvas3`, 420×360)

Histogram (shared `drawHistogram` helper) of final pot size (log10 scale) for over-staked bettors.

- **Title:** "Where the Greedy Bettor Ends Up"; **subtitle:** "Wins 55 in 100 — a real edge — but stakes 40% of the pot each time".
- **Data generation:** seeded mulberry32(3303); 3,000 runs of 500 even-money bets with win probability 0.55; pot multiplied by 1.4 on a win, 0.6 on a loss (40% stake); plot log10(final pot), floored at 1e-30.
- **Bins/axes:** 46 bins over [-26, 4]; x tick labels "÷10²⁶", "÷10²¹", "÷10¹⁶", "÷10¹¹", "÷10⁶", "÷10", "×10⁴"; x-axis label "Final pot compared with the starting pot"; rotated y title "How many bettors".
- **Colors:** bar fill `rgba(230,126,34,0.35)`, stroke `#1a5276`; density line `#922b21` with band.
- **Reference line:** vertical dashed green `rgba(39,174,96,0.85)` at 0 labeled "walked in with this" (right-aligned).
- **Notes:** bold two-line "NN.N% ended up poorer" / "than they started" (computed, >99%); gray `#7f8c8d` two-line "…while still winning" / "more bets than they lost".

### Visualization (canvas `canvas3b`, 400×360)

Curve chart: long-run growth rate per bet as a function of stake size (the Kelly arch).

- **Title:** "Same Edge, Different Stake Size"; **subtitle:** "Long-run growth per bet, for a bettor who wins 55 in 100".
- **Curve:** `g(f) = 0.55·ln(1+f) + 0.45·ln(1−f)` plotted for stake fraction f from 0.005 to 0.55, in `#e67e22` 2.8px; y range -0.075 to 0.008.
- **Zones:** above the g=0 line shaded `rgba(39,174,96,0.07)`, below shaded `rgba(231,76,60,0.07)`; dashed (4/3) `rgba(44,62,80,0.6)` zero line; bold 10px labels: `#1e8449` "ABOVE THIS LINE: pot grows" (top left) and `#c0392b` "BELOW THIS LINE: pot shrinks to nothing" (below the line).
- **Markers (dot + dotted drop line + two-line label under axis):** 10% "best" in `#27ae60`; 20% "treads water" in `#e67e22`; 40% "wiped out" in `#e74c3c`.
- **Axes:** x title "Share of the pot staked on each bet"; rotated y title "Growth per bet"; axes `#999`; margins top 52, right 18, bottom 52, left 56.

## Your Lucky Numbers Cost You Most of the Jackpot

**Pitfall label (color `#27ae60`):** SAME ODDS, DIFFERENT PRICE

**Belief callout:** **What most people believe:** Which numbers you pick cannot possibly matter, and picking 1-2-3-4-5-6 would be idiotic.

Your chance of winning really is identical whatever you pick — but your *payout* is not, because the jackpot is split between everyone holding the same line. Crowds pile onto birthdays and anniversaries, so almost nothing above 31 gets chosen, and lucky 7 gets hammered. Picking the numbers nobody wants does not improve your odds at all; it improves your price. Choosing 1-2-3-4-5-6 is the one obvious-looking line that hardly anyone actually fills in.

- Nothing here changes your chance of winning — only how many strangers you would have to share with
- The cliff after 31 is the calendar: no month has a 32nd day, so those numbers sit almost unused
- A line made entirely of dates draws roughly 13 times the average crowd; a line of high numbers draws a tiny fraction of it
- This is a case where being right and getting paid properly for being right are different things

*Term note:* Illustrative popularity weights, matching the pattern found in published lottery studies. The size of the gap depends on the weights; the direction does not.

### Visualization (canvas `canvas4`, 420×360)

Histogram (shared `drawHistogram` helper) of jackpot-crowding scores for random lottery lines.

- **Title:** "How Many People Share Your Line?"; **subtitle:** "6,000 random lines — 1× means an averagely popular set of numbers".
- **Data generation:** popularity weight per number 1–49: base 1, ×2.2 if ≤31, ×1.35 if ≤12, ×1.7 if exactly 7, ×0.55 if ≥32. Crowding score of a 6-number line = product of (weight / average weight). Seeded mulberry32(4404); 6,000 random 6-of-49 lines, scores capped at 6 for display. Reference lines computed: 1-2-3-4-5-6; all-birthdays (3,7,11,19,24,31); all-high (34,38,41,44,47,49).
- **Bins/axes:** 44 bins over [0, 6]; x tick labels "0×", "1×", "2×", "3×", "4×", "5×", "6×+"; x-axis label "Share the jackpot with this many times the average crowd"; rotated y title "How many lines".
- **Colors:** bar fill `rgba(39,174,96,0.35)`, stroke `#1a5276`; density line `#922b21` with band.
- **Reference line:** vertical dashed `rgba(44,62,80,0.7)` at 1× labeled "average line".
- **Notes:** bold `#2c3e50` "Every line here has the SAME" / "chance of winning."; bold `#c0392b` "Only the payout differs."; bold `#c0392b` "All birthdays (3,7,11,19,24,31): NN×" (~13×); bold `#1e8449` "All high (34,38,41,44,47,49): 0.NNNN×"; gray `#7f8c8d` "1-2-3-4-5-6: NN× on these weights —" / "in reality almost nobody picks it".

### Visualization (canvas `canvas4b`, 400×360)

7×7 heatmap grid of the numbers 1–49 shaded by pick popularity.

- **Title:** "Which Numbers People Actually Pick"; **subtitle:** "Darker = chosen more often. 1 to 49."
- **Cells:** 7 columns × 7 rows; fill `rgba(26,82,118, 0.10 + 0.80·t)` where t is the normalized popularity weight; white 1px cell borders; number text bold 11px centered — white when t > 0.45, else `#2c3e50`.
- **Cliff outline:** red `#e74c3c` 2.5px stepped border separating numbers ≤31 from ≥32 (32 starts row 5, column 4).
- **Captions (centered at bottom):** bold 11px `#c0392b` "Everything past 31 is barely used —" / "no month has a 32nd day"; gray `#7f8c8d` 10px "and 7 is picked most of all".
- **Layout:** margins top 52, right 16, bottom 64, left 26.

## Big Outsiders Are the Worst Value on the Board

**Pitfall label (color `#d35400`):** STAIRCASE GOING DOWN

**Belief callout:** **What most people believe:** The bookmaker takes the same cut on everything — so a £100 bet on a 50/1 outsider is no worse value than a £100 bet on an even-money favourite.

In this model — and in the pattern reported by decades of racetrack studies — the margin is not spread evenly: the longest prices carry the most of it, where punters cannot feel it. Here a bet at evens returns about £96 of every £100 risked; a bet at 50/1 returns barely £60. The staircase shape is unmistakable: each step out to longer odds costs more of the stake in hidden margin. The very bets that feel the most exciting — the big-price outsiders — are the ones priced hardest against you.

- At short odds (evens to 2/1), £91–£96 of every £100 staked comes back — only a few percent of hidden margin
- At medium odds (5/1 to 10/1), the return drops to £67–£75
- At long odds (20/1 and beyond), only about £60–£63 comes back — nearly 40% of the stake is gone before the race starts
- The direction matches the favourite-longshot bias found in real betting markets; the exact step sizes depend on the margin curve chosen here

*Term note:* If you want to look it up: the favourite-longshot bias, and why overround is not uniform.

### Visualization (canvas `canvas6`, 420×360)

Custom staircase bar chart: simulated money returned per £100 staked by odds bucket.

- **Title:** "Money Back per £100 Staked"; **subtitle:** "By offered odds — 40,000 simulated £100 bets at each price".
- **Model:** margin(p) = 0.03 + 0.65·(1−p)⁶; true win probability solved by fixed-point iteration from decimal price d: p = 1 / (d·(1+margin(p))). Seeded mulberry32(6606); 40,000 £100 bets per bucket.
- **Buckets (label, decimal odds):** 1/1 (2), 2/1 (3), 5/1 (6), 10/1 (11), 20/1 (21), 33/1 (34), 50/1 (51). Bar heights = simulated return per £100 (≈£96 down to ≈£60); value labels bold 11px `#2c3e50` "£NN" above bars.
- **Bar colors by return:** ≥£90 `rgba(39,174,96,0.55)`; ≥£75 `rgba(230,126,34,0.55)`; below `rgba(231,76,60,0.55)`; strokes `#1a5276` 0.6px. Y scale 0–110.
- **Break-even line:** horizontal dashed (4/3) `rgba(44,62,80,0.55)` at £100 labeled bold 10px `#2c3e50` "£100 back = break even".
- **Annotation (top right, bold 10px `#c0392b`):** "The staircase: each step costs you more".
- **Axes:** x title "Offered odds"; rotated y title "Return per £100"; axes `#999`; margins top 52, right 18, bottom 60, left 56.

### Visualization (canvas `canvas6b`, 400×360)

Curve chart of the embedded bookmaker margin by true win probability.

- **Title:** "Where the Bookmaker Hides the Margin"; **subtitle:** "Embedded margin % by true win probability".
- **Curve:** margin(p) = 3% + 65%·(1−p)⁶ plotted for p from 0.02 to 0.60, red `#e74c3c` 2.8px; y range 0–70%.
- **Zones:** p > 0.30 shaded `rgba(39,174,96,0.08)`; p < 0.10 shaded `rgba(231,76,60,0.08)`.
- **Markers (5px dot, bold label above, "NN% margin" below):** Favourite at p=0.50 in `#27ae60`; Mid-rank at p=0.15 in `#e67e22`; Outsider at p=0.04 in `#e74c3c`.
- **Corner note (bold 10px `#2c3e50`):** "Outsiders pay" / "for everyone else".
- **Axes:** x ticks 5%, 10%, 20%, 30%, 40%, 50% with title "True chance of winning"; y ticks 0%–70% in 10% steps; rotated y title "Margin built into the price"; axes `#999`; margins top 52, right 18, bottom 60, left 56.

## The Winners Were Removed Before You Looked

**Pitfall label (color `#c0392b`):** THE TAIL WAS CUT OFF

**Belief callout:** **What most people believe:** Almost nobody beats the bookmaker long-term — just look at what everyone who bets there actually makes.

If you keep beating a bookmaker, they usually do not ban you. They cut the biggest bet they will accept from you — someone who could get £500 on a match is told £2 is now the limit. The account stays open, but the edge is worth nothing because no real money can go behind it. Others are simply closed. So the winners are not losing; they have been quietly taken out of the numbers before anyone counts them, which makes the survivors look like proof that winning is impossible.

- The winning end of the chart does not fade out gradually — it stops dead, exactly where bet sizes get cut
- The faded shape behind is what those same bettors would have made if nobody had touched their bet sizes
- The skilled bettors are still in the data, just frozen in place a little above the cut-off point
- This goes well past betting: when a system quietly removes its own extremes, the leftover data appears to prove the extremes were never there

*Term note:* If you want to look it up: censored data, and survivorship bias.

### Visualization (canvas `canvas5`, 420×360)

Histogram (shared `drawHistogram` helper) of observed lifetime profit on open accounts, with a censored right tail.

- **Title:** "Profit on Accounts That Are Still Open"; **subtitle:** "9,000 bettors, 800 bets each — as the operator's own data would show it".
- **Data generation:** seeded mulberry32(5505); 9,000 accounts × 800 bets at £10 stakes; 4% skilled (edge ~ Normal(0.05, 0.02)), the rest edge ~ Normal(-0.045, 0.018); win probability 0.5 + edge/2. Once observed profit exceeds £200, stake is cut to £1 (operator restriction). Both observed and unrestricted profits tracked.
- **Bins/axes:** 52 bins over [-1400, 520]; x-axis label "Lifetime profit (£)", integer ticks; rotated y title "How many accounts".
- **Colors:** bar fill `rgba(231,76,60,0.35)`, stroke `#1a5276`; density line `#922b21` with band.
- **Reference lines:** vertical dashed `rgba(44,62,80,0.7)` at £0 labeled "break even"; vertical dashed green `rgba(39,174,96,0.9)` at £200 labeled "stakes cut here".
- **Notes:** bold "The winning end stops dead" / "instead of tailing off"; gray `#7f8c8d` "and piles up just past the cut"; bold gray `#7f8c8d` quote ""Look — hardly anyone wins."".

### Visualization (canvas `canvas5b`, 400×360)

Overlaid histogram comparison: observed (censored) vs untouched counterfactual profits, winning end only.

- **Title:** "What Was Removed"; **subtitle:** "Faded = the same bettors, stakes never touched".
- **Data:** both profit arrays binned into 40 bins over [-200, 1400]; ghost bars (unrestricted) drawn first in `rgba(39,174,96,0.20)` with `rgba(39,174,96,0.45)` strokes; observed bars on top in `rgba(231,76,60,0.55)`.
- **Cut-off marker:** vertical dashed (5/3) `#c0392b` 2px line at £200; region to the right shaded `rgba(231,76,60,0.06)`; label bold 10px `#c0392b` "stakes cut"; bold 11px `#1e8449` three-line note: "These winners exist." / "They just never" / "reach the data."
- **Stat line (bold 10px `#2c3e50`, centered):** "N.N% of accounts hit the cut-off" (computed share of unrestricted profits above £200).
- **Legend (bottom left, swatch + 10px `#555` label):** red swatch "what you see"; faded green swatch "what was true".
- **Axes:** x title "Lifetime profit (£) — winning end only"; rotated y title "How many accounts"; axes `#999`; margins top 56, right 18, bottom 58, left 52.

## Regeneration instructions

- **Layout:** h1 + centered `.page-sub` subtitle paragraph, then one `<table class="obj-table">` per section, each a single `<tr>` with three `<td>`s: text cell (38%) holding `<span class="pitfall-label">`, `<h3>`, a `.belief` callout div, paragraph, `<ul>`, and a `.termname` div; middle cell (31%, centered) with the primary canvas (width=420, height=360); right cell (31%, centered) with the insight canvas (width=400, height=360). Section order on the page: arcsine, martingale, over-betting, lottery crowding, favourite-longshot, account limiting.
- **Page CSS:** body system sans-serif (-apple-system stack), margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `.page-sub` centered `#666` 0.95em max-width 900px; table cells `1px solid #2980b9`, 12px padding; h3 `#1a5276` 1.0em weight 700; p/li 14px; `.pitfall-label` inline-block bold 0.72em uppercase letter-spacing 0.5px; `.belief` background `#fdf3f2`, left border `3px solid #e74c3c`, padding 8px 12px, 13.5px, with `b` in `#c0392b`; `.termname` `#7f8c8d` 12.5px italic; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned in document order from the cycling array `["#2980b9", "#8e44ad", "#e67e22", "#27ae60", "#d35400", "#c0392b"]` via a small script setting `style.color` on each `.pitfall-label`.
- **Charts:** all simulations use seeded mulberry32 RNG (seeds 1101, 2202, 3303, 4404, 6606, 5505 per card) with Box-Muller `randn()`. Shared `setupCanvas(id)` scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) and paints a white background. Shared `drawHistogram(canvasId, data, options)` helper: title bold 13px `#1a5276` + optional gray subtitle, axes `#999`, bars with `#1a5276` 0.5px strokes, Gaussian-kernel smoothed density line `#922b21` 2px with 95% SE band `rgba(231,76,60,0.18)` (sigma 1.5, effective N clamped [30, 200]) drawn only when `options.density` is not `false` — the martingale chart (`canvas2`) passes `density:false` because the overlay would smooth across its two isolated spikes — dashed vertical reference lines with labels, free-positioned notes (fractions of plot area), custom x tick labels, y axis marked "most"/"none" with a rotated y title. Helper functions `axes`, `chartTitle`, `yTitle` used by the custom right-column charts.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60` / `#1e8449`, red `#e74c3c` / `#c0392b`, orange `#e67e22`, purple `#8e44ad` / `#6c3483`, dark slate `#2c3e50`, gray `#7f8c8d`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions.
