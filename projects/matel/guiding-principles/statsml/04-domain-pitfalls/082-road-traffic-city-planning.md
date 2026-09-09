# Traffic/City Planning - Domain-Specific Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left ~40%, canvas right ~60%)
**HTML title tag:** Traffic/City Planning - Domain-Specific Pitfalls

**Subtitle:** Traffic data lies in specific ways — behavior responds to interventions, sensors see fragments, simulations are too orderly, and the inverse problems are ill-posed.

## Induced Demand

**Build a Lane → Relief → More Driving → Same Congestion in 3-5 Years**

- **The mechanism:** New highway capacity cuts congestion briefly, and that relief activates latent demand.
- **Latent demand:** People who previously avoided driving now drive, so total vehicle volume climbs.
- **The model gap:** Forecasts predict "less congestion" because they hold travel demand fixed.
- **Unmodeled response:** Nothing in the forecast captures the behavioral response to the improvement itself.
- **The timeline:** The relief dip lasts 1-2 years; within 3-5 years congestion is back to its original level.
- **The lesson:** Any intervention that changes incentives invalidates pre-intervention behavior models.

### Visualization (canvas `c1`, 720×300)

Line/area chart: congestion level over 6 years, dipping when a lane is added and recovering.

- **Title (bold 17px `#1a5276`, centered):** "Induced Demand: Congestion Returns After Capacity Expansion".
- **Axes:** L-shaped `#333` axes (width 1.5); margins left 70, right 40, top 40, bottom 50; x labels "0"–"6" (years) in 17px `#555` with axis title "Time (years)"; y labels "High" (top) and "Low" (bottom), right-aligned.
- **Lane-added marker:** dashed red `#e74c3c` vertical line (dash 6/4, width 2) at year 1.5, labeled "Lane added" in 17px red above.
- **Congestion curve (year, level 0–1):** `[[0,0.82],[0.5,0.80],[1.0,0.78],[1.5,0.75],[1.8,0.40],[2.0,0.35],[2.5,0.45],[3.0,0.55],[3.5,0.62],[4.0,0.70],[4.5,0.75],[5.0,0.78],[5.5,0.80],[6.0,0.82]]` — drawn as an orange `#e67e22` line (width 3, round joins) over an area fill `rgba(41,128,185,0.3)` down to the x-axis.
- **Annotations (17px):** green `#27ae60` "Relief dip" beside the trough point (year 2.0); red `#e74c3c` "Back to original level" beside the year 5.5 point.

## Sensor Coverage Gaps

**The "Full Picture" Is Interpolated From Sparse, Biased Measurements**

- **Point sensors:** Cameras and loops sit at intersections, so everything between them is guesswork.
- **Mid-block blindness:** Incidents that happen between sensor points never appear in the data at all.
- **GPS bias:** Phone traces over-represent smartphone owners and app users, not the travelling public.
- **Who goes missing:** Elderly, low-income, and cyclist trips are systematically undercounted.
- **Extrapolated counts:** Manual traffic counts sampled 2 days per year get scaled to annual estimates.
- **Sampling risk:** One atypical count day distorts a whole year's planning number for that road.

### Visualization (canvas `c2`, 720×300)

Schematic road diagram with sensor points and no-data gaps.

- **Title (bold 17px `#1a5276`, centered):** "Sensors See Points. Everything Between Is Interpolated."
- **Road:** thick gray `#999` horizontal line (width 14) at y=90 from x=50 to x=670, overlaid with a white dashed center line (width 2, dash 12/10).
- **Sensors:** green `#27ae60` filled dots (radius 7) above the road at x = 110, 360, 610, each with a short green stem down to the road and the label "sensor" (17px green) above.
- **Gap regions:** translucent red `rgba(231,76,60,0.15)` rectangles (170 wide, 14 tall) over the road centered at x = 235 and x = 485, each labeled bold 17px red "no data" below.
- **Text lines (centered):** bold 17px red "GPS traces: smartphone owners only. Counts: 2 days/year, extrapolated to 365."; 17px `#555` 'The "citywide traffic picture" is interpolation over sparse, biased samples.'

## Simulation ≠ Reality

**SUMO/VISSIM Agents Follow Rules. Real Drivers Don't.**

- **The gap:** Microsimulation agents obey lane discipline, signals, and car-following models.
- **Real streets:** Actual traffic has road rage, illegal U-turns, double parking, and jaywalking.
- **Orderly vs chaotic:** The simulation's clean flow overstates capacity and understates variance.
- **False compliance:** Its "optimal" signal timing assumes a level of compliance that doesn't exist.
- **Policy failure:** A plan validated only in simulation is validated against the rule-following world.
- **Deployment gap:** It therefore degrades or fails outright when deployed to the rule-breaking one.

### Visualization (canvas `c3`, 720×300)

Two-panel schematic comparing orderly simulated traffic vs chaotic real traffic.

- **Title (bold 17px `#1a5276`, centered):** "Simulation Is Orderly. Reality Is Chaotic."
- **Left panel (centered at x≈195):** heading bold 17px `#2980b9` "Simulation (SUMO/VISSIM)"; 3 lanes (light `#ccc` guide lines at y = 75, 107, 139 from x=60 to x=330), each with 5 evenly spaced blue `#2980b9` car rectangles (24×12, 52px apart); caption 17px green `#27ae60` "Even spacing, lane discipline, full compliance".
- **Right panel (centered at x≈525):** heading bold 17px `#e74c3c` "Reality"; same 3 lane guide lines (x=390–660); 10 red `#e74c3c` car rectangles (24×12) scattered and rotated at (x, y, angle rad): (400,63,0.1), (440,70,−0.3), (478,92,0.4), (530,64,0), (548,100,−0.5), (590,128,0.2), (615,68,0.3), (430,130,0.15), (500,132,−0.2), (640,98,0.5); caption 17px red "U-turns, double parking, road rage, jaywalking".
- **Bottom caption (17px `#555`, centered):** 'Signal timing "optimal" in simulation fails on streets where rules are suggestions.'

## Special Event Disruption

**Concerts, Games, Protests, Construction: 50+ Days/Year Off-Model**

- **Pattern override:** A stadium event or protest completely replaces the normal daily traffic pattern.
- **A different regime:** It is not noise around the mean — the whole shape of the day changes.
- **Training blind spot:** A model trained on "normal days" fails on the 50+ event days per year.
- **Worst timing:** Those are exactly the days when congestion management matters most.
- **Two-part problem:** The system must first detect that an event is happening at all.
- **Second half:** Then it must model that event's unique spatial and temporal impact in real time.

### Visualization (canvas `c4`, 720×300)

Two overlaid 24-hour traffic curves: normal day vs event day.

- **Title (bold 17px `#1a5276`, centered):** "Event Day Overrides the Normal Pattern Entirely".
- **Axes:** L-shaped `#333` axes (width 1.5); left margin 60, top 45, chart height 130; x labels "0h", "6h", "12h", "18h", "24h" (17px `#555`).
- **Normal day (solid blue `#2980b9`, width 2.5):** sum of Gaussians over t = 0–24h: exp(−0.5·((t−8)/1.5)²)·0.7 + exp(−0.5·((t−17.5)/1.8)²)·0.8 + 0.08 — two rush-hour humps at 8am and 5:30pm.
- **Event day (dashed red `#e74c3c`, dash 6/4, width 2.5):** exp(−0.5·((t−8)/1.5)²)·0.5 + exp(−0.5·((t−19)/1.0)²)·0.98 + exp(−0.5·((t−22.5)/0.8)²)·0.85 + 0.08 — huge 7pm spike plus a 10:30pm exit spike.
- **Legend (17px, inside top left):** blue "— Normal day (model trained here)"; red "- - Event day (game ends 10pm)".
- **Bottom caption (bold 17px red, centered):** "50+ event days/year: the model is wrong exactly when management matters most."

## Multi-Modal Interaction

**Optimizing for Cars Hurts Everyone Else Sharing the Same Space**

- **Shared space:** Cars, bikes, pedestrians, buses, and scooters all occupy the same streets.
- **Conflicting needs:** Each mode brings different speeds, trajectories, and priority claims.
- **One-mode optimization:** Timing signals for car throughput lengthens pedestrian waits and squeezes cyclists.
- **Silent winner:** The objective function picks a favoured mode without ever declaring it.
- **Data determines policy:** If sensors mostly count cars, the resulting data is car-centric by construction.
- **The requirement:** Model all modes simultaneously, with real measurement coverage for each one.

### Visualization (canvas `c5`, 720×300)

Horizontal index bars: each mode's outcome after car-centric signal retiming (before = 100).

- **Title (bold 17px `#1a5276`, centered):** "Optimize for Cars → Every Other Mode Pays".
- **Heading (bold 17px `#333`, left):** "After car-centric signal retiming (before = 100):".
- **Bars** (starting x=280, scale 1.8 px per unit, 18px tall, rows 36px apart from y=65; mode names 17px `#333` at left; a dotted `#999` baseline tick at value 100 for each row; value labels 17px `#333` at bar end):
  - "Car travel time" — 78, green `#27ae60`
  - "Bus delay" — 125, red `#e74c3c`
  - "Pedestrian wait" — 150, red `#e74c3c`
  - "Cyclist conflicts" — 140, red `#e74c3c`
- **Bottom captions (centered):** bold 17px red "Car-centric sensors → car-centric data → car-centric policy."; 17px `#555` "All modes share the street; the objective function silently picks the winner."

## Origin-Destination Matrix From Partial Data

**You Know WHERE Traffic Is — Not Where It Came From or Where It's Going**

- **The inverse problem:** Estimating an origin-destination matrix from link counts is massively underdetermined.
- **Count of unknowns:** Thousands of OD pairs must be recovered from a few hundred count locations.
- **Non-uniqueness:** Many different OD matrices reproduce the exact same observed link volumes.
- **No discrimination:** The counts therefore cannot distinguish between those competing matrices.
- **Ill-posed:** The chosen solution comes from the prior or regularizer, not from the data.
- **Assumption-driven:** Different assumptions give different "answers" that all fit the counts equally.

### Visualization (canvas `c6`, 720×300)

Flow diagram: three candidate OD matrices feeding one observed link.

- **Title (bold 17px `#1a5276`, centered):** "Many OD Matrices → Same Observed Link Counts".
- **Observed link:** thick blue `#2980b9` horizontal segment (width 8) centered at (360, 110), 120px long, labeled bold 17px blue "Observed: 1,200 veh/hr" above; a thin blue outflow arrow continues right to x=640 with a filled arrowhead.
- **Candidate flows:** three dashed quadratic curves (dash 5/4, width 2) converging from the left onto the link, each with a 17px left-aligned label at x=55:
  - "A→X: 800, B→Y: 400" — green `#27ae60` (from y=55)
  - "A→Y: 300, B→X: 900" — orange `#e67e22` (from y=110)
  - "A→X: 100, C→Y: 1100" — red `#e74c3c` (from y=165)
- **Bottom captions (centered):** bold 17px red "All three OD matrices reproduce the count exactly. Data cannot pick one."; 17px `#555` 'Thousands of OD pairs, hundreds of counts: the "answer" comes from the prior, not the data.'

## Regeneration instructions

- **Layout:** domains detail-page style — h1, `.subtitle` paragraph, then one `<h2>` per pitfall (unnumbered, `border-bottom: 2px solid #2980b9`), each followed by a single-row `.obj-table`: left `<td>` (40%) with `.obj-title` div + `<ul>` of bold-labeled one-sentence bullets, right `<td>` (60%, centered) with the canvas. Even table rows have background `#fafcfe`. No thead, no nav, no cross-page links. The "Simulation ≠ Reality" h2 uses the `&ne;` entity.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) though unused.
- **Canvas:** each declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Charts use font `-apple-system` at 17px (bold 17px for titles and emphasis) and white (unfilled) backgrounds. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#555`/`#333`/`#999`/`#ccc`.
- Card/page links in regenerated HTML use `.html` extensions.
