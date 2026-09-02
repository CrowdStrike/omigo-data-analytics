# Weather Prediction & Chaos

**Page type:** detail page (four titled sections, each a two-column row: text left 50%, canvas right 50%)
**HTML title tag:** Weather Prediction & Chaos — Numerical Forecasting and Its Limits

**Subtitle:** Modern weather forecasting solves the physics of the atmosphere on a planet-sized grid, and chaos sets a hard ceiling on how far ahead any such calculation can see.

**Intro callout (blue-left-border box):** Numerical weather prediction is arguably the most successful simulation enterprise in science: the equations of fluid dynamics and thermodynamics are integrated forward on a global grid, and the result is re-verified against reality every single day. No other simulation gets graded that often, at that scale, in public. The story of how it works — and where chaos stops it cold — is the cleanest case study of what simulation can and cannot deliver.

## 1. The grid: physics integrated forward

A weather model is not a statistical fit to past weather — it is the atmosphere rebuilt as arithmetic, one grid cell and one time step at a time.

- **3D grid:** The atmosphere is sliced into millions of stacked cells roughly 10-25 km wide in global models.
- **Primitive equations:** Fluid dynamics and thermodynamics are stepped forward in time inside every cell.
- **Initial state:** Data assimilation blends satellites, balloons, aircraft, and surface stations into one starting snapshot.
- **Assimilation matters:** The forecast can only be as good as the estimate of what the atmosphere is doing right now.
- **Sub-grid physics:** Clouds and turbulence happen at scales smaller than a cell and cannot be computed directly.
- **Parameterizations:** These sub-grid processes are approximated by tuned formulas, a main source of model error.
- **Time stepping:** The model advances in small increments, each step feeding the next for days of simulated weather.

Key point: A forecast is a giant physics computation, not a lookup of similar past days — and its two weakest links are the initial snapshot and the parameterized processes too small for the grid to see.

### Visualization (canvas `c1`, 720×360)

Cross-section of the atmosphere as a grid of cells with wind arrows, plus an observation column feeding a data-assimilation box that initializes the grid.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The atmosphere as a grid: physics stepped forward cell by cell"
- **Grid:** 8 columns × 5 rows of 48×38 cells starting at (50, 70) — outline each cell 1px `#ccc`; outer border of the whole 384×190 block 1.5px `#999`.
- **Surface:** filled `#ccc` rect (50, 260, 384, 12); 10px `#999` centered label "surface" at (242, 288).
- **Wind arrows:** 2px `#2980b9` arrows with filled arrowheads inside five cells — from (85, 105) to (115, 95); from (180, 140) to (212, 135); from (300, 100) to (330, 108); from (135, 215) to (165, 210); from (350, 185) to (382, 178).
- **Cell-size tag:** bold 11px `#1a5276` centered at (242, 56): "grid cells ~10-25 km wide in global models".
- **Parameterization note:** bold 11px `#e67e22` centered at (242, 316): "clouds & turbulence are smaller than a cell — approximated by parameterizations".
- **Observation column (right):** 11px `#555` left-aligned labels at x=500 — "satellites" (y=84), "weather balloons" (y=106), "aircraft" (y=128), "surface stations" (y=150); each with a thin 1px `#bbb` connector line from x=592 at (label y − 4) to the assimilation box's left edge.
- **Assimilation box:** 110×110 rect at (600, 60), white fill, 1.5px dashed `#8e44ad` border; bold 12px `#8e44ad` centered "DATA" (x=655, y=105) and "ASSIMILATION" (y=121); 10px `#666` centered "one best estimate" (y=143) and "of the state now" (y=157).
- **Init arrow:** 2px `#8e44ad` arrow with filled arrowhead from (600, 190) to (450, 220); bold 10px `#8e44ad` centered at (545, 212): "initial state".
- **Caption (12px `#999`, centered, y = h−14):** "Physics inside every cell, observations pinning down the start — both have to be right"

## 2. Chaos: the butterfly effect is a measured fact

In 1963 Edward Lorenz re-ran a weather computation from rounded-off intermediate values and got completely different weather — the discovery that founded chaos theory.

- **Lorenz 1963:** A tiny rounding difference in the starting numbers produced an entirely different simulated weather pattern.
- **Exponential growth:** Small errors in the initial conditions do not stay small — they roughly double every few days.
- **No perfect start:** Observations can never pin down every gust, so some initial error is always present.
- **Hard ceiling:** Error growth caps useful daily weather forecasts at a horizon of roughly two weeks.
- **Property, not bug:** Chaos belongs to the atmosphere itself, not to any flaw in the model that simulates it.
- **Better data helps:** Sharper initial conditions buy extra days of skill but can never abolish the ceiling.
- **Scale matters:** Large slow patterns stay predictable longer than small fast ones like individual thunderstorms.

Key point: The forecast horizon is limited by physics, not by engineering: even a perfect model of the atmosphere would lose track of the real one within weeks, because unmeasurably small differences grow exponentially.

### Visualization (canvas `c2`, 720×340)

Two forecast trajectories launched from nearly identical starting points: indistinguishable at first, then diverging into completely different weather.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Two runs, almost the same start — completely different weather"
- **Axis:** 1.5px `#999` horizontal line at y=290 from x=60 to x=660; 10px `#999` centered label "forecast time (days)" at (360, 308); 10px `#999` tick labels "0" at (60, 306) — omitted if crowded, keep only the axis caption.
- **Trajectory A:** 2px `#1a5276` polyline over x=60..660 in 200 steps: `y = 170 + 55·sin((x−60)/45)`.
- **Trajectory B:** 2px `#e74c3c` polyline over the same range: `y = 170 + 55·sin((x−60)/45 + 0.002·exp((x−60)/85))` — the phase error grows exponentially, so the curves overlap for the first half then separate dramatically.
- **Start annotation:** bold 11px `#27ae60` left-aligned at (68, 68): "initial difference: 0.0001"; thin 1px `#27ae60` connector line from (110, 74) down to (75, 160).
- **Divergence region:** 1.5px dashed `#999` vertical line from (430, 60) to (430, 290); bold 11px `#e67e22` left-aligned at (440, 72): "errors grow exponentially —"; second line at (440, 88): "the runs stop agreeing".
- **Legend (top-left block starting (70, 100), 16px line spacing):** 22px line swatch + 11px `#555` labels: `#1a5276` "run 1 — measured initial state", `#e74c3c` "run 2 — same state, rounded".
- **Caption (12px `#999`, centered, y = h−14):** "Lorenz 1963: rounding the starting numbers rewrote the weather — chaos is measured, not metaphor"

## 3. Ensembles: turning chaos into probabilities

If one run cannot be trusted past a few days, run many: modern centers launch ~50 copies of the model and read the forecast off the whole population.

- **Perturbed copies:** Each of ~50 ensemble members starts from a slightly different initial state.
- **Varied physics:** Members also vary the parameterizations, sampling model error as well as data error.
- **Tight spread:** When members agree, the atmosphere is in a predictable regime and confidence is high.
- **Wide spread:** When members fan out, the situation is genuinely uncertain and the forecast says so.
- **Rain odds:** A "70% chance of rain" is literally the fraction of ensemble members producing rain.
- **Mean beats one:** The ensemble average verifies better than any single deterministic run on average.
- **Spread as product:** The uncertainty estimate is itself a forecast output, verified like any other.

Key point: Ensembles do not defeat chaos — they price it: the spread of the members converts an unavoidable limit on certainty into an honest, verifiable probability statement.

### Visualization (canvas `c3`, 720×360)

Fan chart of twelve ensemble temperature trajectories over ten days: tightly bundled early, spreading wide late, with the spread band shaded.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Fifty copies of the forecast — agreement early, honest spread late"
- **Axes:** 1.5px `#999` lines — horizontal at y=300 from x=60 to x=660, vertical at x=60 from y=50 to y=300; 10px `#999` centered label "forecast day" at (360, 318); 10px `#999` tick labels "0", "2", "4", "6", "8", "10" centered under the axis at x = 60 + day×60, y=314; rotated or plain 10px `#999` label "temperature" left of the vertical axis is omitted — keep the axis clean.
- **Members:** twelve 1px `#bbb` polylines over day d=0..10 (x = 60 + d×60, 100 steps): `y_i = 190 − 40·sin(d·0.55) + (i − 5.5)·0.9·(exp(d/3.5) − 1)` for i = 0..11.
- **Spread band:** fill `rgba(26,82,118,0.12)` between the i=0 and i=11 member curves over the full range (drawn before the member lines).
- **Ensemble mean:** 2.5px `#1a5276` polyline: `y = 190 − 40·sin(d·0.55)`.
- **Early annotation:** bold 11px `#27ae60` centered at (170, 70): "tight spread —"; second line at (170, 86): "high confidence".
- **Late annotation:** bold 11px `#e67e22` centered at (560, 70): "wide spread —"; second line at (560, 86): "low confidence".
- **Legend (left-aligned block starting (80, 330), horizontal):** 22px 2.5px `#1a5276` line swatch at (80, 326) + 11px `#555` label "ensemble mean" at (108, 330); 22px 1px `#bbb` swatch at (230, 326) + 11px `#555` label "individual members" at (258, 330).
- **Caption (12px `#999`, centered, y = h−14):** "A 70% chance of rain is a count: the fraction of members that rained"

## 4. The quiet revolution in forecast skill

While nobody was watching, forecasts got dramatically better — a steady, compounding improvement that meteorologists call the quiet revolution.

- **The headline:** A 5-day forecast today is about as accurate as a 3-day forecast was decades ago.
- **Roughly a day per decade:** Each decade of work has extended the useful forecast range by about one day.
- **Better observations:** New satellite instruments feed assimilation far more data about the current atmosphere.
- **Finer grids:** Resolution has climbed steadily as supercomputers grew, shrinking the parameterized residue.
- **ML emulators:** Graph neural networks trained on decades of reanalysis now match physics models on many scores.
- **Compute collapse:** The ML emulators run in seconds on one chip versus hours on a supercomputer.
- **Hybrid frontier:** Physics for the dynamics plus learned components for the rest is the current research edge.

Key point: Forecast skill improved for fifty straight years without any single breakthrough — compounding gains in observations, resolution, and assimilation did it, and ML emulators are now bending the cost curve rather than the skill ceiling.

### Visualization (canvas `c4`, 720×340)

Two rising skill curves over four decades — 3-day and 5-day forecast skill — with a dashed reference showing today's 5-day forecast matching the 3-day skill of decades ago.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The quiet revolution: today's 5-day forecast, yesterday's 3-day accuracy"
- **Axes:** 1.5px `#999` lines — horizontal at y=290 from x=60 to x=660, vertical at x=60 from y=50 to y=290; 10px `#999` year labels centered at y=306: "1981" (x=80), "1990" (x=200), "2000" (x=333), "2010" (x=467), "2023" (x=640); 10px `#999` label "forecast skill (%)" left-aligned at (64, 46).
- **3-day skill:** 2.5px `#27ae60` polyline through (80, 160), (200, 137), (333, 114), (467, 91), (640, 72); bold 11px `#27ae60` label "3-day forecast" left-aligned at (480, 76).
- **5-day skill:** 2.5px `#1a5276` polyline through (80, 233), (200, 206), (333, 167), (467, 133), (640, 98); bold 11px `#1a5276` label "5-day forecast" left-aligned at (480, 150).
- **Reference line:** 1.5px dashed `#e67e22` horizontal line at y=160 from x=80 to x=360; filled `#e67e22` dots (radius 4) at (80, 160) and (360, 160).
- **Annotation:** bold 11px `#e67e22` left-aligned at (100, 190): "the 5-day forecast reaches the skill"; second line at (100, 206): "the 3-day forecast had decades earlier".
- **Caption (12px `#999`, centered, y = h−14):** "Fifty years of compounding gains — about one extra day of skill per decade"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (50%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Bullet style:** each bullet is a bold colored label + one short complete sentence that fits on one line at normal page width; split dense content into more bullets rather than longer sentences. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 360/340/360/340 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent `#2980b9`; grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links, no cross-references to other pages, no index number in the h1.
