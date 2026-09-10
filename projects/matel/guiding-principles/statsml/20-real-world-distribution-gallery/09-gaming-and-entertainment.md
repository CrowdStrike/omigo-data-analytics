# Gaming & Entertainment — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, histogram canvas middle 31%, insight canvas right 31%, one table per section)
**HTML title tag:** Gaming & Entertainment — Distribution Patterns

## Session Length — The Game Decides When You Stop

**Pitfall label:** HILL WITH SPIKES AT ROUND BOUNDARIES (color `#795548`)

Most sessions cluster around 25 minutes, but sharp spikes sit at exactly 20, 40, and 60 minutes — match boundaries. The histogram thins out between the spikes, consistent with players quitting at match ends rather than mid-match (penalties, lost progress). Round-number spikes in a duration histogram are a tell that an external clock, not free choice, is setting the stopping points.

- Spikes at 20, 40, 60 min line up with match endings, not "feeling done"
- Between spikes = thin. Mid-match quits are rare — consistent with penalties + sunk effort
- One reading: match length is the lever that sets session length
- The ECDF (right) shows the same thing as visible jumps at each exit gate

### Visualization (canvas `canvas1`, 420×340)

Histogram of session lengths: log-normal body with match-boundary spikes.

- **Data:** seeded RNG mulberry32(101). 8000 log-normal samples `exp(3.2 + 0.5·N(0,1))` (median ~25 min), plus spikes: 1200 samples at 20 ± 0.5 min, 800 at 40 ± 0.5 min, 500 at 60 ± 0.5 min (uniform ±0.5).
- **Chart:** 80 bins over x range 0–90, 6 x ticks (integer format). Bars filled `rgba(231,76,60,0.3)` stroked `#1a5276` (0.5px). Gaussian-smoothed density line (sigma 1.5 bins) in `#922b21` (2px) with 95% SE band filled `rgba(192,57,43,0.18)`. White background, light `#ccc` axes, margins left 50 / right 20 / top 40 / bottom 40; y axis labeled max count and 0.
- **Title (bold 13px `#1a5276`):** "Session Length (minutes)"
- **X-axis label:** "Minutes"

### Visualization (canvas `canvas1b`, 400×340)

ECDF of session lengths with jump annotations at match boundaries.

- **Title (bold 12px `#1a5276`):** "ECDF — Staircase Reveals Exit Gates".
- **Curve:** ECDF of sorted data over x 0–90 min, stroke `rgba(41,128,185,0.9)` width 2.
- **Jump markers at 20, 40, 60 min** (colors `rgba(231,76,60,0.85)`, `rgba(230,126,34,0.85)`, `rgba(142,68,173,0.85)` respectively): a 6px-wide vertical highlight band at 15% alpha, a downward arrow pointing at the ECDF jump, and two-line bold 10px labels "JUMP" / "{t} min" above the arrow.
- **Axes:** y "0%", "50%", "100%"; x ticks 0–90 in steps of 15; x-axis label "Minutes".

## In-App Spending — A Tiny Fraction Funds Everything

**Pitfall label:** MASSIVE ZERO PILE + EXTREME RIGHT TAIL (color `#2980b9`)

95% of simulated players spend exactly $0. Among the 5% who pay, most spend under $20 — but the tail is extreme: the top spender crosses $10,000, and the top 2% of payers account for about two-thirds of all revenue. A distribution this concentrated resembles the shape often described for gambling losses, and it makes "average revenue per user" nearly meaningless.

- 95% pay nothing at all — the "free" in free-to-play is real for almost everyone
- The median payer spends ~$13; a handful spend thousands
- Top 2% of spenders ≈ two-thirds of revenue (cumulative curve, right)
- ARPU averages $0 players with extreme whales — a number that describes nobody

### Visualization (canvas `canvas2`, drawn at 380×360)

Custom split histogram: a truncated $0 spike plus binned spender distribution.

- **Data:** seed 202. 9500 players at exactly $0; 500 spenders from Pareto `5·(1-u)^(-1/0.8)` (x_min=$5, alpha=0.8, heavy tail), capped at $15,000 for display.
- **Zero bin:** 30px-wide solid bar filled `#e74c3c` stroked `#c0392b`, drawn at 95% of plot height with two white break marks near the top; labels bold 11px `#c0392b`: "95%" above and "$0" below the bar.
- **Spender bins:** after a 15px gap, 40 bins over $0–$500, bars `rgba(231,76,60,0.3)` stroked `#c0392b`, scaled to 70% of plot height; x ticks "$0"–"$500" in $100 steps.
- **Annotation (11px `#e67e22`, top of spender region):** "Power-law tail extends to $10K+".
- **Title (bold 13px `#1a5276`):** "In-App Purchase Spending ($)"
- **X-axis label:** "Spending ($) — among spenders"
- Note: this canvas has HTML attributes 420×340 but the drawing code resets it to 380×360.

### Visualization (canvas `canvas2b`, 400×340)

Cumulative revenue concentration curve (spenders ranked by spend, descending).

- **Title:** "Cumulative Revenue — Who Funds the Game?".
- **Curve:** cumulative % of revenue vs % of spenders (sorted descending), stroke `rgba(192,57,43,0.9)` width 2.5, area under curve filled `rgba(231,76,60,0.25)`. Dashed gray diagonal equality line `rgba(100,100,100,0.4)` (dash 4/4).
- **Annotation:** region left of the top-2% point tinted `rgba(231,76,60,0.12)`; red arrow pointing at the curve there with bold 11px `#c0392b` two-line label "Top 2% of spenders" / "= {N}% of revenue" (≈ two-thirds); bold 14px "WHALES" in `rgba(192,57,43,0.8)` centered in the tinted zone.
- **Axes:** y "0%", "50%", "100%"; x "0%", "50%", "100%"; x-axis title "% of Spenders (ranked by spend)".

## Matchmaking Rating — The System Lies at the Edges

**Pitfall label:** BELL CURVE SQUISHED AT BOTH ENDS (color `#27ae60`)

Player ratings look like a bell curve in the middle, but mass piles up at the bottom floor (you can't drop below it) and at the top ceiling. The compression hides information — a player stuck at the floor could be anywhere from terrible to decent-but-new. The rating only tells you something useful in the middle range.

- Middle of the curve = the system actually knows your skill level
- Pile-up at the floor = new and bad players are lumped together, no way to tell them apart
- Pile-up at the ceiling = top players are all crammed into one rank, differences hidden
- Ratings are only meaningful in the middle band — the edges are just walls

### Visualization (canvas `canvas3`, 420×340)

Histogram of MMR: normal body with floor and ceiling pile-ups.

- **Data:** seed 303. 10000 samples of Normal(mean 1500, sd 400); values below floor 800 remapped to `800 + 30·u` and above ceiling 2800 remapped to `2800 - 30·u`. Plus 1500 extra samples at `800 + 25·u` (floor pile-up) and 800 extra at `2800 - 25·u` (ceiling pile-up).
- **Chart:** 60 bins over x range 750–2850, 5 x ticks (integer). Bars `rgba(142,68,173,0.35)` stroked `#1a5276`. Smoothed density line `#922b21` + SE band `rgba(192,57,43,0.18)` (shared helper).
- **Title:** "Matchmaking Rating (MMR/Elo)"
- **X-axis label:** "Rating"

### Visualization (canvas `canvas3b`, 400×340)

Horizontal heatmap strip showing where the rating carries real information.

- **Title:** "Rating Information Density"; subtitle (10px `#666`): "Where does the system ACTUALLY know your skill?".
- **Strip:** 60px-tall horizontal band of 50 cells over rating range 750–2850. Cell color runs red → yellow → green by an "information" score: `min(1, distFromEdge/500)` where distFromEdge = min(rating-800, 2800-rating), multiplied by 0.3 where the bin count exceeds 70% of max (pile-up penalty). RGB ramp: below 0.5 → r=220, g=80+300v, b=60; above 0.5 → r=220-360(v-0.5), g=180+40(v-0.5), b=60+80(v-0.5); alpha 0.75. Strip outlined `#333` 1.5px.
- **Zone labels below strip:** bold 11px `#c0392b` "FLOOR" with 9px "No info" (left); bold 11px `#27ae60` "TRUE SKILL VISIBLE" with 9px "Gaussian = real info" (center); bold 11px `#c0392b` "CEILING" with 9px "No info" (right). Red arrows point down at both edge zones from above.
- **Rating scale above strip:** tick labels 800, 1200, 1500, 1800, 2200, 2800.
- **Legend (bottom):** red swatch "= System is guessing", green swatch "= Skill measured".

## Loot Box Rewards — Engineered Like Slot Machines

**Pitfall label:** HAND-CRAFTED PROBABILITY LADDER (color `#e74c3c`)

The bottom two tiers cover 93% of boxes, and 98% land below Epic. Each tier is dramatically rarer than the last, ending with Legendary at 0.2%. Unlike the other shapes in this gallery, this distribution didn't emerge from anything — every probability was chosen by a designer. At $2.99 per box, the expected cost of a Legendary works out to about $1,495.

- 93% of boxes yield the two filler tiers; 98% land below Epic
- Legendary sits at 0.2% — an expected ~500 boxes (~$1,495) to hit one
- The Epic-to-Legendary cost jump is ~9x — one interpretation: engineered near-miss appeal
- These odds are set by hand, not by any natural process — a designed distribution

### Visualization (canvas `canvas4`, 420×340)

Categorical bar chart of loot box tier probabilities (customBars mode of the histogram helper; no density line).

- **Data (fixed):** tiers Common 75%, Uncommon 18%, Rare 5%, Epic 1.8%, Legendary 0.2%.
- **Bars:** filled `rgba(230,126,34,0.35)` stroked `#1a5276`, 70% of slot width, tier name below each bar (11px `#333`), percentage label above each bar (10px `#1a5276`): "75%", "18%", "5%", "1.8%", "0.2%".
- **Title:** "Loot Box Reward Tier Probabilities"
- **X-axis label:** "Rarity Tier"

### Visualization (canvas `canvas4b`, 400×340)

Horizontal log-scale bar chart of expected dollar cost to obtain each tier.

- **Title:** "Expected $ to Get Each Tier"; subtitle: "Cost per box: $2.99 — how many boxes on average?".
- **Data:** expected cost = (1/probability) × $2.99 per tier → approximately $4 (Common), $17 (Uncommon), $60 (Rare), $166 (Epic), $1,495 (Legendary). Bars scaled by log10 of cost.
- **Bar colors (top to bottom):** `rgba(39,174,96,0.75)`, `rgba(41,128,185,0.7)`, `rgba(142,68,173,0.7)`, `rgba(230,126,34,0.75)`, `rgba(231,76,60,0.8)`; tier name in white bold 11px inside the bar (or in the bar color left of the axis when the bar is under 60px); cost label bold 11px `#333` at bar end — Legendary reads "$1495 !!".
- **Gap annotation:** dashed red connector (`#e74c3c`, dash 3/3) between the Epic and Legendary rows with bold 10px red label "9x jump!".
- **Footnote (9px `#888`, bottom center):** "(log scale)".

## Time to First Kill — The Map's Size Is Written in the Shape

**Pitfall label:** ZERO BELOW A MINIMUM, THEN RAPID DECAY (color `#8e44ad`)

Nobody gets a kill in the first ~3 seconds (spawn protection + travel time). After that minimum, kills happen at a steady random rate — like coin flips. The cutoff point reveals map design: smaller maps = shorter minimum = faster pace. Bigger maps = longer before anything happens.

- Zero kills below ~3 seconds — physics and spawn protection make it impossible
- After the minimum, encounters are random — like bumping into someone in a crowd
- That cutoff point IS a map complexity metric — smaller map = shorter cutoff = faster game
- Compare two maps by just looking at where this shape starts — no other analysis needed

### Visualization (canvas `canvas5`, 420×340)

Histogram of time to first kill: left-truncated exponential.

- **Data:** seed 505. 10000 samples of `3.0 + Exp(λ=0.15)` seconds (truncation at 3 s).
- **Chart:** 60 bins over x range 0–40, 5 x ticks (integer). Bars `rgba(39,174,96,0.35)` stroked `#1a5276`. Smoothed density line `#922b21` + SE band `rgba(192,57,43,0.18)` (shared helper).
- **Title:** "Time to First Kill (seconds)"
- **X-axis label:** "Seconds"

### Visualization (canvas `canvas5b`, 400×340)

Survival curves for three maps with different truncation points.

- **Title:** "Survival Curves — 3 Maps Compared"; subtitle: "P(no kill yet) — truncation = map \"tightness\"".
- **Curves (200 points over t = 0–40 s, width 2.5):** survival = 1 for t < truncation, then `exp(-λ·(t - trunc))`:
  - "Tiny map" — trunc 1.5 s, λ=0.35, `rgba(231,76,60,0.85)`
  - "Medium map" — trunc 3.0 s, λ=0.15, `rgba(41,128,185,0.85)`
  - "Large map" — trunc 8.0 s, λ=0.06, `rgba(39,174,96,0.85)`
- Each truncation point marked with a vertical dotted line (dash 3/3) in the map color and a 4px dot at the 100% level.
- **Annotation:** horizontal double-headed arrow in `#1a5276` between the tiny-map and large-map truncation lines at ~15% height, labeled bold 10px "Map size = truncation shift".
- **Legend (center right):** colored line samples with map names.
- **Axes:** y "0%", "50%", "100%"; x ticks "0s"–"40s" in 8-second steps; x-axis title "Time (seconds)".

## Player Retention — Most People Quit in Predictable Waves

**Pitfall label:** STAIRCASE DROP-OFF (color `#e67e22`)

Players don't leave gradually — they fall away in waves. In this simulation, 58% churn by Day 1, about half of the survivors are gone by Day 7, and roughly 60% of the rest by Day 30, leaving 8% long-term players. The step pattern breaks any smooth-decay assumption: an exponential fit over-predicts retention between every cliff (right chart).

- Day 1 cliff: ~58% gone — consistent with onboarding friction
- Day 7 cliff: ~half of the remaining churn — consistent with exhausting the free content
- Day 30 cliff: ~60% of the rest — paywalls and repetition are common explanations
- The 8% who survive past Day 30 are the group that drives most free-to-play revenue
- Cliffs at days 1/7/30 mean a smooth exponential model misfits everywhere

### Visualization (canvas `canvas6`, 420×340)

Histogram of last-day-played (churn distribution).

- **Data:** seed 606, 2000 samples of "last day played": 1160 uniform in days 0–1, 440 uniform in 1–7, 240 uniform in 7–30, 160 uniform in 30–180.
- **Chart:** 45 bins over x range 0–90, 6 x ticks (integer). Bars `rgba(192,57,43,0.5)` stroked `#c0392b`. Smoothed density line `#922b21` + SE band `rgba(192,57,43,0.18)` (shared helper).
- **Title:** "Player Churn Distribution (Last Day Played)"
- **X-axis label:** "Last Day Active"

### Visualization (canvas `canvas6b`, 400×340)

Actual step-like retention curve vs a smooth exponential assumption.

- **Title:** "Actual Retention vs Smooth Exponential Assumption"; subtitle: "Cliffs at Day 1, 7, 30 — not smooth decay".
- **Actual retention (line + 4px dots, `rgba(192,57,43,0.9)`/`#c0392b`, width 2.5), points (day, %):** (0, 100), (1, 40), (3, 28), (7, 18), (14, 12), (30, 7), (60, 5). X range 0–60 days.
- **Exponential assumption:** `100·exp(-0.05·t)` as a dashed gray curve (`rgba(100,100,100,0.6)`, dash 6/4, width 2).
- **Cliff annotations** at days 1, 7, 30: faint vertical dashed line `rgba(192,57,43,0.4)` plus bold 9px `#c0392b` two-line labels "Day 1" / "-60%", "Day 7" / "-55%", "Day 30" / "-42%".
- **Over-prediction callout:** bold 9px `#e67e22` "Over-predicts!" next to the exponential curve at day 7.
- **Legend (bottom right):** solid red line "Actual retention", dashed gray line "Exponential assumption".
- **Axes:** y "0%", "50%", "100%"; x ticks 0–60 in steps of 10; x-axis title "Days Since Install".

## In-Game Currency — Veterans Get Exponentially Richer

**Pitfall label:** RUNAWAY GAP BETWEEN RICH AND POOR (color `#16a085`)

In this simulated economy, veteran gold compounds while every new player starts at the same fixed 1,000. The right chart shows the modeled distribution by server age: the median wallet grows from ~2K at month 1 to ~8K at month 3, ~36K at month 6, and ~160K at month 12, with a tail stretching into the millions. The fixed newbie starting point becomes marketplace-irrelevant within months — the same widening-gap dynamic as wealth inequality, compressed into a game economy.

- New player always starts at 1,000 gold — a fixed point in a moving distribution
- Median wealth grows several-fold each quarter in this model (~8K → ~36K → ~160K)
- The spread widens too — veteran tails reach millions while newcomers hold 1K
- Marketplace prices track the veteran mass — one reason aging servers get reset

### Visualization (canvas `canvas7`, 420×340)

Histogram of player gold on a 6-month-old server (log-normal mixture).

- **Data:** seed 707. 800 new players `exp(7 + 0.5·N(0,1))`, 700 mid players `exp(10 + 1.0·N(0,1))`, 500 veterans `exp(12 + 1.5·N(0,1))`.
- **Chart:** 40 bins over x range 0–500000, 5 x ticks formatted as "K" thousands. Bars `rgba(230,126,34,0.5)` stroked `#e67e22`. Smoothed density line `#922b21` + SE band `rgba(192,57,43,0.18)` (shared helper).
- **Title:** "Player Gold Distribution (6-Month-Old Server)"
- **X-axis label:** "Gold"

### Visualization (canvas `canvas7b`, 400×340)

Overlaid log-normal density curves by server age on a log-gold axis.

- **Title:** "Wealth Distribution Spreads Exponentially with Server Age"; subtitle: "Each month the distribution widens and right-shifts".
- **Curves (normal PDFs in log-space over ln(gold) 5–16, 200 points, width 2.5, light 0.1-alpha fill under each):**
  - "Month 1" — mu 7.5, sigma 0.8, `rgba(39,174,96,0.8)`
  - "Month 3" — mu 9, sigma 1.2, `rgba(41,128,185,0.8)`
  - "Month 6" — mu 10.5, sigma 1.5, `rgba(230,126,34,0.8)`
  - "Month 12" — mu 12, sigma 2.0, `rgba(192,57,43,0.8)`
- **New-player marker:** vertical dashed line (`#1a5276`, dash 5/3, width 2) at ln(1000) ≈ 6.9, labeled bold 9px "New player" / "start (1K)", with a small downward arrow beneath.
- **Legend (top right):** colored line samples with month names.
- **Axes:** x ticks at ln values 6, 7, 9, 11, 13, 15 labeled "400", "1K", "8K", "60K", "440K", "3.3M"; x-axis title "Gold (log scale)"; y labeled "Density" and "0".

## Regeneration instructions

- **Layout:** one `.obj-table` (full-width, border-collapse) per section, single `<tr>` with three `<td>`: left 38% text (`.pitfall-label` span, `h3`, paragraph, `ul`), middle 31% centered canvas (420×340), right 31% centered insight canvas (400×340). Cell borders `1px solid #2980b9`, padding 12px. Includes viewport meta tag.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `h3` in cells `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase with 0.5px letter-spacing. Canvas CSS `width: 100%; height: auto`. No nav bar, no back/home links.
- **Pitfall label colors:** assigned by a small script cycling through `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` in document order.
- **Data generation:** seeded RNG `mulberry32(seed)` per section (seeds 101, 202, 303, 505, 606, 707), Box-Muller `randn()`, inverse-CDF exponential.
- **Histogram helper:** shared `drawHistogram(canvasId, data, options)` — options bins/title/xLabel/color/strokeColor/min/max/xTicks/xFormat, plus a `customBars` mode (labels/values/pctLabels) for categorical charts like the loot box tiers; white plot background; margins left 50 / right 20 / top 40 / bottom 40; title bold 13px `#1a5276` centered; light `#ccc` axes; y axis shows max count and 0; plus a Gaussian-smoothed density line (`#922b21`, sigma 1.5 bins) with a 95% SE band (`rgba(192,57,43,0.18)`, effective n clamped to [30, 200]) — skipped in customBars mode.
- **Canvas scaling:** all canvases set `max-width` to the intrinsic width, size the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and `ctx.scale` by that combined factor.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`, purple `#8e44ad`, dark reds `#c0392b`/`#922b21`, gray text `#555`/`#666`/`#333`.
- Note: regenerated HTML pages link nowhere (detail page); any grid page linking here uses the `.html` extension.
