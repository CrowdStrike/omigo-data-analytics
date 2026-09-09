# Simulation Models — Weather, Earthquakes & Epidemics

**Page type:** grid page (card grid in the `recently-added-misc/03-tracking-data-collection-methods` template style, 4 cards per row)
**HTML title tag:** Simulation Models — Weather, Earthquakes & Epidemics

**Subtitle:** How science simulates nature — weather, earthquakes, wildfires, floods, and epidemics — and the shared techniques that make their forecasts trustworthy.

## Cards

Each card links to a detail page under `simulation-models/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of topic tag pills.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | ATMOSPHERE | Weather Prediction & Chaos | [85-simulation-models-weather-earthquakes-and-epidemics/01-weather-prediction-and-chaos.md](85-simulation-models-weather-earthquakes-and-epidemics/01-weather-prediction-and-chaos.md) | Atmospheric physics on a global grid, where chaos turns forecasts into ensemble probabilities. | physics on a grid, butterfly effect, many runs |
| 2 | ATMOSPHERE | Hurricane Forecasting | [85-simulation-models-weather-earthquakes-and-epidemics/02-hurricane-forecasting.md](85-simulation-models-weather-earthquakes-and-epidemics/02-hurricane-forecasting.md) | Spaghetti ensembles draw the track cone while intensity forecasts lag decades behind. | storm path, cone of uncertainty, storm strength |
| 3 | GEOPHYSICS | Earthquake Models — Hazard, Not Prediction | [85-simulation-models-weather-earthquakes-and-epidemics/03-earthquake-models-hazard-not-prediction.md](85-simulation-models-weather-earthquakes-and-epidemics/03-earthquake-models-hazard-not-prediction.md) | Models forecast shaking odds, not the next quake. | shaking odds, aftershocks, early warning |
| 4 | EPIDEMIC | Compartmental Disease Models | [85-simulation-models-weather-earthquakes-and-epidemics/04-compartmental-disease-models.md](85-simulation-models-weather-earthquakes-and-epidemics/04-compartmental-disease-models.md) | Three equations — S, I, R — generate the epidemic curve, R₀, and herd immunity. | epidemic curve, spread rate, herd immunity |
| 5 | EPIDEMIC | Agent-Based & Network Spread | [85-simulation-models-weather-earthquakes-and-epidemics/05-agent-based-and-network-spread.md](85-simulation-models-weather-earthquakes-and-epidemics/05-agent-based-and-network-spread.md) | Superspreading and contact networks break the averages that compartments assume. | superspreaders, contact webs, computer worms |
| 6 | GEOPHYSICS | Wildfire & Flood Spread | [85-simulation-models-weather-earthquakes-and-epidemics/06-wildfire-and-flood-spread.md](85-simulation-models-weather-earthquakes-and-epidemics/06-wildfire-and-flood-spread.md) | Fire crawls over fuel, slope, and wind; water routes over elevation grids. | fire spread, flood maps, evacuations |
| 7 | MACHINERY | Monte Carlo & Ensemble Methods | [85-simulation-models-weather-earthquakes-and-epidemics/07-monte-carlo-and-ensemble-methods.md](85-simulation-models-weather-earthquakes-and-epidemics/07-monte-carlo-and-ensemble-methods.md) | Sample the uncertainty, run the model many times, read distributions instead of point answers. | random draws, repeated runs, range of outcomes |
| 8 | MACHINERY | Validation & Calibration | [85-simulation-models-weather-earthquakes-and-epidemics/08-validation-and-calibration.md](85-simulation-models-weather-earthquakes-and-epidemics/08-validation-and-calibration.md) | Hindcasting and skill scores decide whether a simulator earns trust — or quietly overfits. | replay the past, report card, over-tuning |
| 9 | TECHNIQUE | Discrete-Event Simulation | [85-simulation-models-weather-earthquakes-and-epidemics/09-discrete-event-simulation.md](85-simulation-models-weather-earthquakes-and-epidemics/09-discrete-event-simulation.md) | Jump the clock from event to event and watch queues form and servers saturate. | waiting lines, clock jumps, busy servers |
| 10 | TECHNIQUE | PDE Solvers — Physics on a Mesh | [85-simulation-models-weather-earthquakes-and-epidemics/10-pde-solvers-physics-on-a-mesh.md](85-simulation-models-weather-earthquakes-and-epidemics/10-pde-solvers-physics-on-a-mesh.md) | Continuous physics discretized onto a mesh — weather, crash tests, aerodynamics. | grid cells, crash tests, airflow |
| 11 | TECHNIQUE | N-body & Molecular Dynamics | [85-simulation-models-weather-earthquakes-and-epidemics/11-n-body-and-molecular-dynamics.md](85-simulation-models-weather-earthquakes-and-epidemics/11-n-body-and-molecular-dynamics.md) | Pairwise forces among millions of particles, from galaxy formation to protein folding. | galaxies, protein folding, clever shortcuts |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** card-grid style of `recently-added-misc/03-tracking-data-collection-methods.html` (no TOC or philosophy box on this page). Single page: h1, `.subtitle` paragraph, then one `.grid` of `.card` anchors.
- **Layout:** `.grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, `margin: 14px 0 30px 0`; responsive: 3 columns below 1100px, 2 below 800px, 1 below 500px.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="card" href="...">` containing `<div class="card-label" style="color:CATEGORY_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), one `<p>` with the one-line description, then `<div class="topics">` with one `<span class="topic-tag">` per topic.
- **Category label colors:** ATMOSPHERE `#1a5276`; GEOPHYSICS `#e67e22`; EPIDEMIC `#e74c3c`; MACHINERY `#8e44ad`; TECHNIQUE `#16a085`.
- **Card style:** background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 16px, transition on box-shadow 0.2s; hover: `box-shadow 0 4px 12px rgba(0,0,0,0.1)`, border `#2980b9`. `.card-label` 0.72em weight 700 uppercase, 0.5px letter-spacing, 4px bottom margin; h3 `#1a5276` 1.0em with 6px bottom margin; description `p` `#555` 0.85em `margin: 0`. `.topic-tag` pills: background `#eef4f8`, border `1px solid #cdd`, radius 4px, padding 2px 6px, 0.7em, color `#555`; `.topics` is a flex row with 4px gap, wrap, 8px top margin.
- **Page style:** body `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`, white background, text `#2a2a2a`, `padding: 40px 20px`, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#1a5276` with 10px bottom margin (no border); subtitle `#666` 1.05em with 24px bottom margin. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
