# Rental Properties — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left ~38%, histogram canvas middle ~31%, insight canvas right ~31%, one table per section)
**HTML title tag:** Rental Properties — Distribution Patterns

**Subtitle:** 5 simulated distributions from a rental property — compounding rent against a fixed mortgage, fat-tailed repairs, memoryless vacancy

## Annual Rent Increase (5% Creates Divergence)

**Pitfall label:** COMPOUNDING SPREAD (color `#795548`)

A 5% annual rent increase on $2,000/month rent doesn't feel like much at the first anniversary ($100/month). But it's compounding against a FIXED mortgage. With a raise at each anniversary, year N rent is $2,000 × 1.05^(N−1) — by year 15 you're collecting $3,960/month on the same $1,400 fixed payment. The spread is exponential — the gap itself accelerates.

- Year 1: $2,000 rent − $1,400 mortgage = $600/mo spread
- Year 10: $3,103 rent − $1,400 mortgage = $1,703/mo
- Year 20: $5,054 rent − $1,400 mortgage = $3,654/mo
- Year 30: $8,232 rent − $1,400 mortgage = $6,832/mo
- Without increase: stuck at $600/mo for 30 years (inflation eats it)

### Visualization (canvas `canvas1`, 420×340)

Histogram of simulated monthly cash flow at year 15 (seeded mulberry32 RNG, seed 77, shared sequentially across all charts).

- **Title (bold `#1a5276`, top center):** "Monthly Cash Flow at Year 15 (5% Annual Increase)".
- **Data:** 2000 simulations of cashFlow = rent15 − mortgage − insurance − propTax − maintenance, where rentBase = 2000 + Normal(0, 100); rent15 = rentBase × 1.05^14 (year-15 rent, raise at each anniversary); mortgage = 1400; insurance = 150 + Normal(0, 20); propTax = 250 + Normal(0, 30); maintenance = 100 + |Normal(200, 150)| with probability 0.35, else 0.
- **Bins/axes:** 35 bins, x range 1000 to 3500, x tick format "$N"; x-axis label "Monthly Cash Flow ($)". Gray `#999` L-shaped axes; margins top 35 / right 20 / bottom 40 / left 50; white background.
- **Bars:** fill `rgba(39,174,96,0.5)`, border `#27ae60` 0.5px. Gaussian-smoothed density line overlay in `#1a5276` width 2 (smoothing sigma 1.5).

### Visualization (canvas `canvas1b`, 400×340)

Diverging lines chart: monthly cash flow over 30 years, 5% increase vs flat rent vs fixed mortgage baseline.

- **Title (bold `#1a5276`):** "5% Annual Increase vs Flat Rent"; subtitle line (10px `#666`): "Same property, same mortgage — only difference is rent policy".
- **Data:** rentStart $2,000, mortgage $1,400; green curve is monthly cash flow (rentStart × 1.05^yr − mortgage) for yr 0–30, y scaled 0 to max ≈ $7,244 (i.e., 2000×1.05^30 − 1400); red dashed flat line at $600/mo; gray dashed (dash 3/3) mortgage baseline along the x-axis ($0 cash flow). Padding top 40 / right 20 / bottom 55 / left 55.
- **Fill:** area between the 5% curve and the flat line filled `rgba(39,174,96,0.15)`.
- **Lines:** 5% curve `#27ae60` width 3; flat line `#e74c3c` width 3 dashed 6/4; mortgage baseline `#999` width 1.5 dashed 3/3.
- **Annotations (computed from the curve, not hardcoded):** green bold 11px "5% increase" and "$<computed>/mo at yr 30" (≈$6,832) near the upper curve; red bold 11px "Flat rent: $600/mo forever" above the flat line; at year 20, a dark slate `#2c3e50` vertical gap arrow from flat line to 5% curve labeled bold 10px "+$<computed>/mo" (≈+$3,054).
- **Axes:** x ticks "Yr 0" to "Yr 30" every 5 years; x-axis label "Years"; y labels "$0" at bottom and the computed max ("$6.8K") at top (right-aligned `#555`), plus "Cash Flow/mo" above the y-axis.

## Cumulative Cash Flow — 5% vs Flat (20 Years)

**Pitfall label:** WEALTH DIVERGENCE (color `#2980b9`)

The cumulative effect is staggering. Cash flow here is all-in — rent − mortgage − insurance − property tax − maintenance − vacancy, the same definition the histogram uses. With flat rent, costs eat nearly the entire $600/mo rent-minus-mortgage spread: 20 years nets only ~$5K total. With 5% annual increases, the same property generates ~$308K. The curves start identical and slowly diverge, then the gap EXPLODES after year 10.

- Flat rent 20-year total: ~$5K — costs consume almost the whole spread
- 5% increase 20-year total: ~$308K
- At 3% increase: ~$164K
- Difference (5% vs flat): ~$304K — from "just 5%"
- The first 5 years look similar — the magic is in years 10-20

### Visualization (canvas `canvas2`, 420×340)

Histogram of total 20-year cash flow across 2000 Monte Carlo runs.

- **Title:** "Total 20-Year Cash Flow Distribution (5% Increase)".
- **Data:** 2000 runs; per run rentBase = 2000 + Normal(0, 80); for each of 20 years: rent = rentBase × 1.05^yr, mortgage $1,400/mo, annual costs (150 + 250)×12; monthly maintenance drawn 12 times (8% chance of 200 + |Normal(800, 600)|, else 25% chance of 50 + uniform(0, 200)); 15% chance of a vacancy of ceil(1 + uniform(0,2)) months, which removes those months' rent. Total plotted in $K.
- **Bins/axes:** 35 bins, x range 150 to 550, x tick format "$NK"; x-axis label "Total Cash Flow ($K)".
- **Bars:** fill `rgba(26,82,118,0.5)`, border `#1a5276`. Density line overlay `#1a5276`.

### Visualization (canvas `canvas2b`, 400×340)

Cumulative comparison curves: 5% vs 3% vs flat rent over 20 years.

- **Title:** "Cumulative Cash Flow (All-In): 5% vs 3% vs Flat"; subtitle (10px `#666`, computed): '"Just 5%" turns $5K into $309K over 20 years'.
- **Data:** cumulative FULL-COST cash flow — the same definition as the canvas2 histogram: per year, rent × (12 − expected vacancy months 0.375) − mortgage 1400×12 − insurance+tax (150+250)×12 − expected maintenance 117.5×12, with rent = 2000 × (1+rate)^(yr−1) for yr 1–20 and rates 0%, 3%, 5%. Endpoints ≈ $4.8K flat / $164K at 3% / $309K at 5% (matching the canvas2 Monte Carlo mean ~$308K). Y scaled 0 to the 5% endpoint. Padding top 40 / right 15 / bottom 55 / left 55.
- **Fill:** gap between 5% curve and flat curve filled `rgba(39,174,96,0.12)`.
- **Lines:** 5% `#27ae60` width 3; 3% `#e67e22` width 2.5; flat `#e74c3c` width 2.5 dashed 6/4.
- **End-point labels (bold 10px, computed):** "$309K" green, "$164K" orange, "$5K" red at each curve's right end.
- **Gap annotation at year 15 (dark slate `#2c3e50`, computed):** bold 10px "+$153K" over 9px 'from "just 5%"'.
- **Axes:** gray `#999` L axes; x ticks "Yr 0"–"Yr 20" every 5; x-axis label "Years"; y labels "$0" and the computed max ("$309K").
- **Legend (bottom):** green line swatch "5% annual", orange "3% annual", red dashed "Flat".

## Maintenance Cost (Zero-Inflated Pareto)

**Pitfall label:** FAT-TAILED SHOCKS (color `#27ae60`)

Most months cost $0 in maintenance. But when something breaks, the distribution of repair costs is Pareto — heavy right tail. A $200 faucet fix is common; a $15,000 roof replacement is uncommon but NOT rare. Budgeting a flat monthly average fails because the MEDIAN is $0 and the tail drives everything.

- 60% of months: $0 maintenance
- 30% of months: $50-$500 (minor fixes)
- 8% of months: $500-$3,000 (appliances, plumbing)
- 2% of months: $3,000-$20,000 (roof, HVAC, foundation) — chart clipped at $8K
- Mean ≠ typical — plan for the tail, not the average

### Visualization (canvas `canvas3`, 420×340)

Histogram of monthly maintenance cost (zero-inflated with heavy right tail).

- **Title:** "Monthly Maintenance Cost (Zero-Inflated Pareto)".
- **Data:** 2000 draws: 60% exactly $0; then 75% of the remainder 50 + uniform(0, 450); then 80% of the remainder a Pareto-like draw 500 + 2500×(1−u)^−0.8 − 2500; else 3000 + uniform(0, 17000). All values clipped at $8,000 for display.
- **Bins/axes:** 40 bins, x range 0 to 8000, x tick format "$N.NK"; x-axis label "Monthly Cost ($)".
- **Bars:** fill `rgba(230,126,34,0.5)`, border `#e67e22`. Density line overlay `#1a5276`.

### Visualization (canvas `canvas3b`, 400×340)

24-month spiky timeline showing why the mean misleads.

- **Title (bold `#e67e22`):** 'Why "Average Maintenance" Is a Lie'.
- **Data:** 24 monthly costs drawn per month: 60% $0; else 10% of remainder 100 + uniform(0, 300); else next tier 500 + uniform(0, 1500); else 3000 + uniform(0, 12000). Bars scaled to the max month. Padding top 40 / right 15 / bottom 50 / left 15.
- **Bar colors by size:** >$3,000 `rgba(231,76,60,0.7)` with a bold dark-red `#c0392b` "$NK" label above; $500–3,000 `rgba(230,126,34,0.6)`; $1–500 `rgba(241,196,15,0.6)`; $0 `rgba(200,200,200,0.3)`.
- **Mean line:** dashed red `#e74c3c` (dash 5/3, width 2) at the computed mean, labeled bold 10px right-aligned "Mean: $N/mo".
- **Median label:** green `#27ae60` bold 10px "Median: $0/mo" at the baseline.
- **Annotation box:** dark slate `rgba(44,62,80,0.92)` filled rectangle with white text: bold "60% of months = $0" / "2% of months = entire year's profit" and 9px "Budget for the tail, not the mean".
- **X axis:** gray baseline; tick labels "Month 1", "Month 12", "Month 24"; caption "24-Month Timeline (one property)".

## Vacancy Duration (Geometric, Memoryless)

**Pitfall label:** MEMORYLESS RISK (color `#e74c3c`)

Each month a vacant unit has a fixed probability p ≈ 0.4 of being filled (in a good market). The resulting duration is geometric — memoryless. A unit vacant 3 months isn't "due" to be filled; it has the same 40% chance next month as it did the first. This kills the "it's been empty so long, someone will come soon" fallacy.

- Median vacancy: 2 months (not the same as mean)
- Mean vacancy: 2.5 months
- ~8% chance of 6+ month vacancy (market-dependent)
- Each vacant month = full mortgage payment from savings
- 1 month vacancy = losing 8.3% of annual rent income

### Visualization (canvas `canvas4`, 420×340)

Histogram of vacancy durations (geometric distribution).

- **Title:** "Vacancy Duration (Months Until Filled)".
- **Data:** 2000 draws of a geometric waiting time: count months while uniform > p (p = 0.4, capped at 18), plus 1 (at least 1 month vacant before fill).
- **Bins/axes:** 18 bins, x range 0.5 to 12.5, x tick format integer; x-axis label "Months Vacant".
- **Bars:** fill `rgba(142,68,173,0.5)`, border `#8e44ad`. Density line overlay `#1a5276`.

### Visualization (canvas `canvas4b`, 400×340)

Memoryless-property visual: identical 40% bars for every month already vacant.

- **Title (bold `#8e44ad`):** 'Memoryless: "It's Been Empty 3 Months"'; subtitle (10px `#666`): '≠ "it's about to be filled"'.
- **Bars:** 8 identical bars (months 1–8), each 40% of the available height, fill `rgba(142,68,173,0.6)`, border `#8e44ad` 1.5px, white bold "40%" label centered in each, month labels "Mo 1"…"Mo 8" below. Padding top 40 / right 15 / bottom 55 / left 50.
- **Arrow:** dark slate `#2c3e50` horizontal arrow across the bars near the top, labeled bold 10px "SAME chance every month — past doesn't help".
- **Cost callout:** red `rgba(231,76,60,0.9)` filled band at the bottom of the plot with white bold 10px text "Each vacant month = -$1,400 mortgage + -$400 taxes/insurance = -$1,800 from pocket".
- **Axes:** gray L axes; y label "P(fill)" right-aligned; x-axis caption "Months Already Vacant".

## Net Annual Return — Year 1 vs 10 vs 20

**Pitfall label:** DISTRIBUTION SHIFT (color `#8e44ad`)

Return = annual cash flow ÷ the original $60K down payment. The distribution doesn't just shift right — it WIDENS and grows a left tail from big-repair years. Year 1 is tight around 3% (rent barely covers costs). By year 10, rent has compounded against the fixed mortgage, so the whole distribution moves and spreads. By year 20, even the worst simulated years stay solidly profitable.

- Year 1: mean return ~3%, central 95% range −3% to +4% (can lose money)
- Year 10: mean return ~22%, central 95% range 15% to 25%
- Year 20: mean return ~58%, central 95% range 48% to 61%
- Assumes the market bears 5% raises every year — the model's biggest lever
- Not modeled: tax effects (depreciation, interest deduction) that shift real returns up

### Visualization (canvas `canvas5`, 420×340)

Histogram of year-20 annual return on investment.

- **Title:** "Annual Return on Investment — Year 20 (5% Rent Increase)".
- **Data:** 2000 simulations; return = (rent20 − costs20) / 60000 × 100 where rent20 = 2000 × 1.05^19 × 12 and costs20 = 1400×12 + 220×12 + 380×12 + (40% chance Normal(4000, 2500), else Normal(500, 300)). (Year-1 and year-10 series are simulated the same way — year 1: rent 2000×12, costs 1400×12 + 150×12 + 250×12 + (30% Normal(2000,1500) else Normal(300,200)); year 10: rent 2000×1.05^9×12, costs 1400×12 + 180×12 + 300×12 + (35% Normal(3000,2000) else Normal(400,250)) — but only year 20 is shown in this histogram.)
- **Bins/axes:** 35 bins, x range 40 to 70, x tick format "N%"; x-axis label "Return %".
- **Bars:** fill `rgba(39,174,96,0.5)`, border `#27ae60`. Density line overlay `#1a5276`.

### Visualization (canvas `canvas5b`, 400×340)

Three overlaid analytic normal density curves: year 1 vs 10 vs 20 returns.

- **Title:** "Return Distribution Shifts Right Over Time"; subtitle (10px `#666`): "Year 1 can lose money — Year 20 almost can't".
- **Curves (x range −10% to 70%, normalized to the year-1 peak, 90% of plot height):** Year 1 Normal(3, 2) — stroke `#e74c3c` width 2.5, fill `rgba(231,76,60,0.25)`; Year 10 Normal(22, 3) — stroke `#e67e22` width 2.5, fill `rgba(230,126,34,0.2)`; Year 20 Normal(58, 4) — stroke `#27ae60` width 2.5, fill `rgba(39,174,96,0.2)`. Padding top 40 / right 15 / bottom 55 / left 45.
- **Break-even marker:** vertical dashed gray `#999` line at 0% labeled 9px "Break-even".
- **Shift arrow:** dark slate `#2c3e50` horizontal arrow from the year-1 mean to the year-20 mean near the baseline, labeled bold 9px "Distribution walks right every year".
- **Legend (bold 10px):** "Year 1" red, "Year 10" orange, "Year 20" green.
- **Axes:** gray x baseline; x ticks "−10%" to "70%" every 10%; x-axis label "Annual Return on Down Payment".

## Regeneration instructions

- **Layout:** one `.obj-table` per section (five total), each a single `<tr>` with three `<td>`s: text cell 38% (pitfall label span, `<h3>` title, paragraph, `<ul>` bullets), middle cell 31% centered (histogram canvas 420×340), right cell 31% centered (insight canvas 400×340).
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.subtitle` centered `#666` 0.95em; table cells `border: 1px solid #2980b9`, padding 12px, vertical-align top; h3 `#1a5276` 1.0em weight 700; p/li 14px, line-height 1.5–1.6; `.pitfall-label` inline-block bold 0.72em uppercase letter-spacing 0.5px; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by document order from the cycling palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script that colors each `.pitfall-label`.
- **Data:** all simulated with a seeded mulberry32 RNG (seed 77) shared sequentially across charts, plus a Box-Muller `randNormal(mean, std)` helper; a shared `drawHistogram(canvasId, data, options)` helper draws title, axes, bars, a Gaussian-smoothed density line (`#1a5276`, sigma 1.5), and x tick labels (6 evenly spaced, optional `xFormat`).
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus accents `#2980b9`, `#8e44ad`, `#2c3e50`, `#c0392b`, `rgba(241,196,15,0.6)` yellow; gray text `#555`/`#666`/`#333`. No nav bar, no back/home links.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
