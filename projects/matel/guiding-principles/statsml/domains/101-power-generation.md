# Power Generation / Multi-Source Energy

**Page type:** detail page (one h2 per energy-source challenge, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 101. Power Generation / Multi-Source Energy

**Subtitle:** Each energy source has fundamentally different data characteristics — the complexity explodes when the grid must manage all of them simultaneously.

## Solar Intermittency

**Obj-title:** A Cloud Drops Output 50% in Seconds — a Step Function, Not a Trend

- **Step function:** One cloud crossing a solar farm cuts output 50% in SECONDS, not over a trend.
- **Fast recovery:** Output returns to full within seconds of the cloud shadow passing off the panels.
- **Rolling deficit:** At GW scale, a cloud bank crossing 5 solar farms in sequence stacks a 500MW deficit.
- **Moving target:** That deficit does not sit still — it travels geographically with the cloud bank.
- **Scale mismatch:** Solar is "predictable" daily and seasonally; at minute level it is stochastic chaos.

### Visualization (canvas `canvas1`, 720×300 declared; setup renders at 720×200)

Step-function line chart of solar output with sharp cloud drops.

- **Background:** pale yellow `#fef9e7`. Blue L-shaped axes (`#2980b9`, width 1) from (50,15) down to (50,170) and across to (700,170). Axis labels 17px `#1a5276`: "Output (MW)" and "Time (minutes)".
- **Series:** orange line (`#f39c12`, width 2.5), 130 samples at 4.9px spacing from x=55, y = 170 − value. Output at baseline+80 (=120) with square drops to baseline+30..35 (~70–75) during four cloud windows: samples 20–24, 55–61, 85–90, 105–110.
- **Cloud labels (14px `#7f8c8d`, y=155):** "Cloud 1" at x=140, "Cloud 2" at 310, "Cloud 3" at 450, "Cloud 4" at 555.
- **Annotation (15px `#e74c3c` at (135,80)):** "-50% in seconds".

## Wind Ramp Events

**Obj-title:** 100% → 10% in 30 Minutes; Above ~25m/s, More Wind = Zero Power

- **Collapse ramp:** When the wind dies, output can fall from 100% to 10% inside 30 minutes.
- **Surge ramp:** When a weather front arrives, output climbs from 0% to 90% inside 20 minutes.
- **Hardest case:** Ramp events are the hardest thing on the grid to forecast with any confidence.
- **Stability risk:** They are also the most dangerous, since reserves must cover the swing in real time.
- **High-wind cutoff:** Above ~25m/s turbines shut down in protection mode — more wind ≠ more power.

### Visualization (canvas `canvas2`, 720×300 declared; setup renders at 720×200)

Wind turbine power curve with high-wind cutoff.

- **Background:** pale blue `#eaf2f8`. Blue L-shaped axes as above. Axis labels 17px `#1a5276`: "Wind Power (%)" and "Wind Speed (m/s)".
- **Power curve:** blue line (`#2980b9`, width 2.5) over wind speed 0–35 m/s mapped to x = 55 + (ws/35)×635, y = 170 − power×1.4: power = 0 below 3 m/s, linear ramp from 0% at 3 m/s to 100% at 12 m/s, plateau 100% to 25 m/s, then 0 above 25 (vertical drop).
- **Cutoff line:** vertical dashed red (`#e74c3c`, dash 5/4) at 25 m/s.
- **Annotations:** 15px `#e74c3c` "CUTOFF >25 m/s" and "100% to 0%!"; 14px `#27ae60` "Ramp zone" at (120,100).

## Nuclear Baseload Inflexibility

**Obj-title:** The "Reliable" Source Becomes the Grid Management Problem

- **Constant output:** Nuclear runs flat 24/7 by design and by economics, not by operator choice.
- **No fast ramp:** It cannot ramp up or down quickly, so it cannot follow the shape of demand.
- **2am problem:** Demand drops overnight but nuclear keeps producing at the same steady output.
- **Forced disposal:** The excess must be dumped at negative prices, or renewables curtailed instead.
- **Inverted reliability:** Alongside variable renewables, the inflexible source limits the flexible ones.

### Visualization (canvas `canvas3`, 720×300 declared; setup renders at 720×200)

Flat nuclear line against a variable 24h demand curve.

- **Background:** pale purple `#f4ecf7`. Blue L-shaped axes. Axis labels 17px `#1a5276`: "Power (GW)" and "24h (midnight to midnight)".
- **Nuclear:** flat purple line (`#8e44ad`, width 3) at y=85 from x=55 to 695.
- **Demand curve:** orange line (`#e67e22`, width 2), 131 samples: y = 85 − 50·sin(πt) + 20·sin(2πt − 1) where t goes 0→1 (dips at night, peaks in evening); note the drawn y-value is used directly (curve crosses the nuclear line).
- **Labels (14px):** red `#e74c3c` "Excess (negative pricing)" at (100,50); purple "Nuclear (constant)" at (550,80); orange "Demand (variable)" at (550,140).

## Gas Turbine Startup Lag

**Obj-title:** 10-30 Minutes to Start — for a 15-Minute Spike

- **Cold start lag:** A gas peaking plant needs 10-30 minutes to climb from cold to full power.
- **Shorter than the lag:** The demand spike it exists to cover may last only 15 minutes end to end.
- **Expensive hedge:** Keeping turbines spinning at idle buys ~5-minute "hot start" capability instead.
- **Hedge cost:** That idle readiness is paid for in fuel cost and in pollution, around the clock.
- **Data challenge:** Predicting whether to pre-start turbines, or risk being caught cold at the spike.

### Visualization (canvas `canvas4`, 720×300 declared; setup renders at 720×200)

Demand spike vs slow turbine ramp, showing the timing gap.

- **Background:** pale orange `#fef5e7`. Blue L-shaped axes. Axis labels 17px `#1a5276`: "Power (MW)" and "Time (minutes)".
- **Demand spike:** red polyline (`#e74c3c`, width 2.5) through (55,155) → (180,155) → (200,40) → (320,45) → (340,150) → (695,155): sharp rise and 15-minute-wide plateau, then back to baseline.
- **Turbine ramp:** green polyline (`#27ae60`, width 2.5) through (55,155) → (200,155) → (220,150) → (280,120) → (340,60) → (500,55) → (520,155): slow ramp reaching full power well after the spike ends.
- **Labels (14px):** red "Demand spike (15 min)" at (210,35); green "Turbine ramp (30 min cold start)" at (340,105); gray 13px `#7f8c8d` "Gap: spike over before turbine ready" at (380,145).

## Hydro Depends on Snowpack from 6 Months Prior

**Obj-title:** Reservoir Level Today = Snowfall Last Winter

- **6-month lag:** Reservoir level today is a function of how much snow fell last winter upstream.
- **Hard floor:** No amount of demand or price generates hydro power from an empty reservoir.
- **Committed early:** A drought season loses hydro capacity MONTHS before that capacity goes missing.
- **Long causal chain:** A warm winter reduces hydro output across the whole following summer.
- **Uniquely long:** The gap from cause (weather) to effect (capacity) is longer than any other source.

### Visualization (canvas `canvas5`, 720×300 declared; setup renders at 720×200)

Two lagged seasonal curves: snowpack and reservoir level.

- **Background:** pale teal `#e8f8f5`. Blue L-shaped axes. Y label 17px `#1a5276` "Level"; month labels Oct–Sep (13px `#555`) along the bottom at 53px spacing from x=65.
- **Snowpack curve:** light blue (`#5dade2`, width 2.5) through monthly values `[10, 35, 60, 80, 95, 100, 75, 40, 10, 0, 0, 5]` (x = 75 + i×53, y = 165 − value×1.3); peaks in March.
- **Reservoir curve:** dark blue (`#1a5276`, width 2.5) through `[50, 40, 35, 30, 30, 40, 60, 85, 95, 80, 60, 55]`; peaks in June.
- **Lag annotation:** 14px `#e74c3c` "~6 month lag" at (300,60) with a dashed red line (dash 4/3) from (280,55) to (380,55).
- **Legend (13px `#555`):** light blue swatch "Snowpack"; dark blue swatch "Reservoir".

## Grid Frequency Balancing — Generation Must EXACTLY Equal Demand

**Obj-title:** Hold 50Hz Within ±0.5Hz While Renewables Fluctuate Every Second

- **Exact balance:** Generation > demand raises grid frequency; demand > generation drops it.
- **Narrow band:** Frequency must be held within ±0.5Hz of target or connected equipment is damaged.
- **Second-by-second:** Variable renewables fluctuate every second, never settling at a steady level.
- **Continuous ramping:** Every other source must therefore ramp continuously to absorb that jitter.
- **Data challenge:** Predicting net demand — demand minus variable generation — at 1-second resolution.

### Visualization (canvas `canvas6`, 720×300 declared; setup renders at 720×200)

Frequency oscillation trace around a 50Hz target with danger bands.

- **Background:** pale peach `#fdf2e9`. Blue L-shaped axes. Axis labels 17px `#1a5276`: "Frequency (Hz)" and "Time (seconds)".
- **Target line:** dashed green (`#27ae60`, dash 5/5, width 1) at y=95.
- **Danger zones:** `rgba(231,76,60,0.1)` rectangles at top (y=15, h=30) and bottom (y=150, h=20) spanning x=55–695.
- **Oscillation:** dark line (`#2c3e50`, width 2), 640 samples: y = 95 + sin(0.05i)·15 + sin(0.13i)·8 + sin(0.31i)·5 + sin(0.71i)·4 (deterministic composite for reproducibility).
- **Labels (14px, right side):** green "50.0 Hz (target)"; red "50.5 Hz (danger)" and "49.5 Hz (danger)".

## Curtailment = Wasted Energy (But Necessary)

**Obj-title:** Useful Output = Capacity Minus Curtailment

- **Thrown away:** When solar and wind produce more than the grid can absorb, they must SHUT DOWN.
- **The scale:** California curtails 1-5 GWh of solar per DAY through the spring months.
- **Distorted data:** A solar farm's capacity factor ≠ what sunshine alone would predict for that site.
- **Why it differs:** Grid constraints, not weather, cut the actual useful output the farm delivers.
- **Not a fault:** The shutdown is deliberate and necessary, so it looks like weather but is a decision.

### Visualization (canvas `canvas7`, 720×300 declared; setup renders at 720×200)

Available-solar bell curve capped by a grid-capacity line, curtailed region shaded.

- **Background:** pale green `#eafaf1`. Blue L-shaped axes. Axis labels 17px `#1a5276`: "Solar (GW)" and "Hour of day".
- **Available solar:** orange line (`#f39c12`, width 2.5), Gaussian over 24 hours: value = 120·exp(−((h−12)/4)²), x = 55 + h×27, y = 170 − value.
- **Grid capacity limit:** dashed red horizontal line (`#e74c3c`, dash 6/4, width 1.5) at value 70 (y=100).
- **Curtailed region:** area between the available curve and min(available, 70) filled `rgba(231,76,60,0.25)`.
- **Labels (14px):** orange "Available solar" at (400,35); red "Grid capacity limit" at (530,95); red "CURTAILED (wasted)" at (280,70).

## Duck Curve — Demand vs Generation Mismatch

**Obj-title:** Net Demand Goes Minimum → Maximum in 3 Hours

- **Midday trough:** Solar floods the grid and net demand (demand − solar) drops to near-zero.
- **Evening cliff:** Solar disappears just as everyone comes home to AC, cooking and TV.
- **The swing:** Net demand runs from its daily minimum to its daily maximum in about 3 hours.
- **Physical limit:** The required MW/minute ramp exceeds what many generators can physically deliver.
- **Getting worse:** Every year more solar is added, so the duck curve grows more extreme.

### Visualization (canvas `canvas8`, 720×300 declared; setup renders at 720×200)

The classic duck-curve net demand shape over 24 hours.

- **Background:** pale yellow `#fef9e7`. Blue L-shaped axes. Axis labels 17px `#1a5276`: "Net Demand (GW)" and "Hour of day".
- **Duck curve:** orange line (`#e67e22`, width 3) through 24 hourly values `[28, 27, 26, 25, 25, 27, 30, 32, 28, 20, 12, 8, 5, 6, 8, 14, 22, 35, 38, 36, 33, 31, 30, 29]` (x = 60 + h×27, y = 170 − value×3.8).
- **Ramp annotation:** 15px `#e74c3c` "3-hour ramp!" at (430,50) with a red vertical arrow at x=435 from y=55 down to y=140.
- **Belly label (14px `#27ae60`):** "Solar belly" / "(duck shape)" near (260,155).

## Battery Degradation — Non-Linear and State-Dependent

**Obj-title:** Current State Alone Can't Predict Remaining Life

- **Non-linear:** Degradation is slow over the first 20% of life, then faster over the next 20%.
- **The cliff:** After that middle phase capacity falls off a cliff rather than continuing smoothly.
- **Path-dependent:** Each cycle's degradation depends on ALL the cycles that came before it.
- **What accumulates:** Depth of discharge, charge rate, temperature and cycle count all leave a mark.
- **History required:** A battery at "80% capacity" has 5 years or 5 months left depending on its path.

### Visualization (canvas `canvas9`, 720×300 declared; setup renders at 720×200)

Two capacity-fade curves showing path dependence.

- **Background:** pale purple `#f4ecf7`. Blue L-shaped axes. Axis labels 17px `#1a5276`: "Capacity (%)" and "Cycle count".
- **Path A (gentle use):** green curve (`#27ae60`, width 2.5), 131 samples (x = 60 + i×4.8): capacity = 100 − (i/130)·25 − (i/130)³·10, y = 170 − capacity×1.5.
- **Path B (aggressive use):** red curve (`#e74c3c`, width 2.5): capacity = 100 − (i/130)·35 − (i/130)^2.2·40, floored at 20.
- **80% EOL threshold:** dashed gray horizontal line (`#7f8c8d`, dash 5/4) at capacity 80 (y=50).
- **Labels:** 14px green "Path A: gentle use"; 14px red "Path B: aggressive use"; 14px gray "80% EOL threshold"; 13px `#1a5276` "Both at 80% here but very different remaining life" at (140,55).

## Multi-Source Dispatch Optimization — Combinatorial Explosion

**Obj-title:** Six Sources, Uncertain Forecasts, a New Solution Every 5 Minutes

- **The decision:** Every 5 minutes the grid must re-dispatch generation across all six sources.
- **The renewables:** Solar and wind are free but variable; nuclear is cheap but inflexible.
- **The fillers:** Gas is expensive but fast, hydro is reservoir-limited, battery fast but short-duration.
- **Different constraints:** Each source has its own cost, ramp rate, minimum output and start time.
- **More constraints:** Each also carries an emission rate and its own contractual obligations.
- **Stochastic core:** Renewable forecasts are uncertain, so the objective itself is a random variable.
- **Real-time scale:** This is a massive stochastic optimization solved in real-time, 24/7, without pause.

### Visualization (canvas `canvas10`, 720×300 declared; setup renders at 720×200)

Stacked area chart of six generation sources over 24 hours.

- **Background:** pale blue `#eaf2f8`. Blue L-shaped axes. Axis labels 17px `#1a5276`: "Generation (GW)" and "Hour of day".
- **Hourly source formulas (h = 0..23):** Nuclear constant 10; Solar = max(0, 15·exp(−((h−12)/4)²)); Wind = 5 + 3·sin(0.5h + 1); Hydro = 3 + 4·max(0, sin(0.3(h−6))); Gas = max(0, 8·sin(0.28(h−5)) + 10 if 16<h<21 else 0); Battery = 4 if 17<h<21 else 0.
- **Stack order (bottom to top):** Nuclear, Hydro, Wind, Solar, Gas, Battery. Fill colors at 50% alpha (hex + `80`): Nuclear `#8e44ad`, Hydro `#2980b9`, Wind `#5dade2`, Solar `#f1c40f`, Gas `#e67e22`, Battery `#27ae60`. X = 60 + h×27; y = 170 − cumulative×2.8.
- **Legend (12px `#333`, along bottom at y≈178):** color swatches with labels "Nuclear", "Hydro", "Wind", "Solar", "Gas", "Battery" at 105px spacing.

## Regeneration instructions

- **Layout:** detail page — h1 + `.subtitle`, then one `h2` per challenge (1.4em `#1a5276`, bottom border `2px solid #2980b9`), each followed by a single-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (the bold headline given above) and a `<ul>` whose bullets each start with a bold label; right `<td>` (60%, centered) holds the canvas. Even table rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) though unused on this page. No nav bar, no back/home links.
- **Canvas:** each `<canvas>` declares `width="720" height="300"`, but the shared `setupCanvas(id)` helper sets both backing store and CSS size to 720×200 (× `window.devicePixelRatio`, then `ctx.scale` back to logical coordinates; default font 17px system). Each chart fills a full-canvas pastel background and draws L-shaped axes from (50,15)–(50,170)–(700,170).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, light blue `#5dade2`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, amber `#f39c12`, yellow `#f1c40f`, purple `#8e44ad`, dark slate `#2c3e50`, gray text `#555`/`#666`/`#7f8c8d`. Pastel backgrounds: `#fef9e7`, `#eaf2f8`, `#f4ecf7`, `#fef5e7`, `#e8f8f5`, `#fdf2e9`, `#eafaf1`.
