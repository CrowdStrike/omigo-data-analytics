# Drones & UAV

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: labeled bullets left ~40%, canvas right ~60%)
**HTML title tag:** Drones & UAV — Domain-Specific Pitfalls

**Subtitle:** Domain-specific data and modeling pitfalls in drone and UAV operations.

## GPS-Denied Environments

**Obj-title:** GPS-Denied Environments

- **The loss:** Indoors, urban canyons, and electronic warfare zones remove the primary navigation source.
- **Drift accumulation:** Fallback visual-inertial or pure inertial navigation compounds small measurement errors.
- **Unbounded growth:** Without external references, position uncertainty grows every second of the flight.
- **Non-linear growth:** 1-meter accuracy under GPS becomes 10+ meters of uncertainty within minutes of denial.
- **Return-to-home danger:** The drone no longer knows where "home" actually is relative to its position.

### Visualization (canvas `canvas1`, 720×200)

Growing uncertainty cone after GPS loss.

- **Background:** `#f8fbfe`. **Title (17px `#1a5276`, centered at y=18):** "Position Uncertainty Over Time (GPS Lost at t=0)".
- **Axes:** gray `#666` 1px L-shape from (60,20) to (60,170) to (680,170). Labels (13px `#555`): "Time (seconds)" centered at (370,195); "Uncertainty (m)" rotated -90° at (15,100). Time ticks (11px): `0s, 30s, 60s, 90s, 120s` at x = 100 + i·140.
- **GPS-lost marker:** dashed (5/3) red `#e74c3c` 2px vertical line at x=100, labeled "GPS Lost" in 12px red at (104,35).
- **Pre-loss segment:** flat green `#27ae60` 2.5px line just above the baseline (y=165) from x=60 to x=100, labeled "GPS Active" in 12px green at (80,145).
- **Uncertainty growth:** from x=100 to x=660, upper bound rises as t^1.5 up to 130px above the baseline; curve stroked red `#e74c3c` 2.5px, cone under it filled `rgba(231,76,60,0.15)`. Region label in 12px red at (400,80): "Inertial Only — Drift Accumulates".

## Wind as Unmodeled Disturbance

**Obj-title:** Wind as Unmodeled Disturbance

- **Stochastic disturbance:** Turbulence near buildings creates unpredictable vortices and thermals shift altitude.
- **Vertical gradient:** Wind shear changes conditions with every meter of ascent, so no single wind vector holds.
- **No repeatability:** The same flight path at the same time on different days yields a different trajectory.
- **Control cost:** Fighting wind disturbances consumes extra battery and creates jerky motion for camera payloads.
- **Simulation gap:** Wind models rarely capture true spatial and temporal variability of the real airspace.
- **False confidence:** That gap yields overconfident mission time estimates and inadequate safety margins.

### Visualization (canvas `canvas2`, 720×200)

Planned straight path overlaid with seven divergent actual flight trajectories.

- **Background:** `#f8fbfe`. **Title (17px `#1a5276`, centered at y=18):** "Planned Path vs Actual Trajectories (Wind Disturbance)".
- **Planned path:** dashed (8/4) `#1a5276` 3px straight horizontal line at y=100 from x=60 to x=680, with 5px `#1a5276` dots at both ends labeled "Start" (60,125) and "End" (680,125) in 11px.
- **Actual trajectories:** 7 lines in colors `#e74c3c, #e67e22, #9b59b6, #2ecc71, #3498db, #f39c12, #1abc9c`, 1.5px at 70% alpha; each a seeded random walk (LCG seeded traj·137+42) starting at (60,100): velocity gets ±1.5 noise per 3px step with 0.97 damping, occasional strong gusts (2% chance, ±6), soft bounds at y=35 and y=165 — same plan, different wind each flight.
- **Legend (12px, bottom):** dashed `#1a5276` swatch + "Planned Path"; red swatch + "Actual Flights (same plan, different wind)".

## Battery State Uncertainty

**Obj-title:** Battery State Uncertainty

- **"40%" isn't 40%:** Usable capacity depends on temperature, cell age, discharge rate, and cell imbalance.
- **Temperature gap:** 40% at -10°C does not equal 40% at 25°C, yet the flight computer reads one number.
- **Cold sag:** Cold batteries deliver less energy and voltage-sag under load — emergency landing, no warning.
- **Aging:** Internal resistance rises and capacity degrades non-uniformly across cells as cycles accumulate.
- **Range fiction:** The computer may believe 2km to home base while real energy supports only 1.2km.

### Visualization (canvas `canvas3`, 720×200)

Actual capacity vs reported battery percentage at four temperatures.

- **Background:** `#f8fbfe`. **Title (17px `#1a5276`, centered at y=18):** "Battery Capacity vs Reported % at Different Temperatures".
- **Axes:** gray `#666` 1px from (80,30) to (80,170) to (670,170). Y label (12px `#555`, rotated): "Actual Capacity (Wh)". X label: "Reported Battery %". X ticks (11px): `100%, 80%, 60%, 40%, 20%, 0%` left to right (reported % decreasing rightward across a 590px width).
- **Curves (2.5px, actual = f(reported)·100Wh over a 135px plot height):**
  - 25°C (ideal): green `#27ae60`, linear, factor 1.0.
  - 10°C: yellow-orange `#f39c12`, factor 0.85, mildly non-linear.
  - 0°C: orange `#e67e22`, factor 0.7, more non-linear.
  - -10°C: red `#e74c3c`, factor 0.5, steepest — cold curves use actual = (reported)^(1+(1−factor)·0.5)·100·factor, dropping off hardest at low %.
- **"40% reported" marker:** dashed (4/3) `#888` 1.5px vertical line at the 40% position, labeled '"40% reported"' in 11px `#888`.
- **Divergence bracket:** dark red `#c0392b` 1.5px bracket to the right of the 40% line spanning from the 25°C curve down to the -10°C curve, labeled "Real gap!" in 10px `#c0392b`.
- **Legend (11px, at x=450, y=38 in 16px steps):** color line swatches + "25°C (ideal)", "10°C", "0°C", "-10°C".

## No-Fly Zone Dynamic Updates

**Obj-title:** No-Fly Zone Dynamic Updates

- **Real-time rules:** Wildfire TFRs, moving VIP no-fly zones, and medical-helicopter corridors appear mid-flight.
- **Stale geofence:** A database loaded at takeoff is outdated within minutes of the drone leaving the ground.
- **Silent violation:** The drone can then enter controlled airspace without any awareness that it did so.
- **Unreliable links:** Update channels fail exactly where TFRs are common — airports, bases, disaster zones.
- **High stakes:** Entering active emergency airspace carries severe legal penalties for the operator.
- **Manned traffic:** It can also interfere with the manned aircraft operations the restriction protects.

### Visualization (canvas `canvas4`, 720×200)

Gantt-style timeline of airspace restriction zones appearing mid-flight.

- **Background:** `#f8fbfe`. **Title (17px `#1a5276`, centered at y=18):** "Dynamic Airspace Restrictions Over Time".
- **Timeline:** gray `#666` 1.5px axis at y=175 from x=60 to x=680, ticks + 11px labels `T+0, T+5min, T+10min, T+15min, T+20min, T+25min` at 124px spacing.
- **Zone bands (each a translucent rect with 1.5px colored border and 11px colored label):**
  - Airport Class B (permanent): `rgba(231,76,60,0.25)` / `#e74c3c`, full width (60,35,620×25), label in `#c0392b`.
  - "TFR — Wildfire (appears T+5)": `rgba(243,156,18,0.3)` / `#e67e22`, from x=184 (T+5) to end, (184,65,496×22), with a dashed orange drop line down to the timeline at x=184.
  - "VIP TFR (T+10 to T+20)": `rgba(142,68,173,0.25)` / `#8e44ad`, (308,92,248×22).
  - "HEMS Corridor (T+15 onward)": `rgba(41,128,185,0.25)` / `#2980b9`, (432,119,248×22).
- **Drone marker:** `#1a5276` "✈" glyph (18px) at (370,155) with 10px caption "Drone (stale geofence data)"; red `#e74c3c` "⚠" glyph (14px) at (395,105).

## Swarm Coordination Data Explosion

**Obj-title:** Swarm Coordination Data Explosion

- **The scale:** 100 drones producing 30 sensor readings at 10Hz is 30,000 messages per second to process.
- **O(N²) mesh:** Full mesh at 100 drones means 9,900 bidirectional links between the aircraft.
- **Per-link payload:** Every one of those links carries position, velocity, intent, and sensor data.
- **Fails at the worst time:** Bandwidth saturates, latency spikes, and packets collide under load.
- **Worst case:** That happens exactly when coordination matters most — dense formations, obstacle fields.
- **No free lunch:** Hierarchical schemes cut bandwidth but add single points of failure at the parent node.
- **Gossip tradeoff:** Gossip protocols scale instead by giving up consistency of the shared swarm state.

### Visualization (canvas `canvas5`, 720×200)

Message-load scaling curves: quadratic full mesh vs near-linear hierarchical, against a bandwidth limit.

- **Background:** `#f8fbfe`. **Title (17px `#1a5276`, centered at y=18):** "Communication Load: Full Mesh vs Hierarchical".
- **Axes:** gray `#666` 1px from (80,30) to (80,165) to (670,165). Y label (12px `#555`, rotated): "Messages/sec (thousands)"; X label: "Swarm Size (number of drones)". X ticks (11px): `10, 25, 50, 75, 100, 150, 200` scaled to max N=200 over 580px. Y ticks (10px): `0M, 3M, 6M, 9M, 12M` over a 125px height (max 12,000 thousand).
- **Full mesh curve:** red `#e74c3c` 2.5px, messages = N·(N−1)·300/1000 (thousands) for N=2..200 — quadratic blow-up; area under it filled `rgba(231,76,60,0.08)`.
- **Hierarchical curve:** green `#27ae60` 2.5px, messages = N·log₂(N)·30/1000 — nearly flat at this scale.
- **Bandwidth limit:** dashed (6/4) yellow-orange `#f39c12` 1.5px horizontal line at the 3M level, labeled "Radio bandwidth limit" in 11px `#f39c12`.
- **Legend (12px, top):** red line swatch + "Full Mesh O(N²)"; green line swatch + "Hierarchical O(N log N)".

## Payload-Dependent Flight Dynamics

**Obj-title:** Payload-Dependent Flight Dynamics

- **Different aircraft:** 0kg vs 5kg shifts the center of gravity and increases moment of inertia.
- **Thrust and vibration:** The loaded airframe requires higher thrust and shifts its vibration modes.
- **Tuning breaks:** A control model tuned on an empty drone oscillates, overshoots, or goes sluggish with load.
- **Unknown mass:** A package labeled 3kg may actually weigh 3.4kg, so the assumed mass is already wrong.
- **Slung loads:** Suspended cargo creates pendulum dynamics that couple back into the flight controller.
- **Needs online adaptation:** Without real-time gain adjustment, empty-safe maneuvers turn dangerous loaded.

### Visualization (canvas `canvas6`, 720×200)

Second-order step responses to an altitude command at three payload weights.

- **Background:** `#f8fbfe`. **Title (17px `#1a5276`, centered at y=18):** "Step Response: Altitude Command at Different Payload Weights".
- **Axes:** gray `#666` 1px from (80,30) to (80,170) to (670,170). Y label (12px `#555`, rotated): "Altitude Response"; X label: "Time (ms)". Time ticks (11px): `0, 500, 1000, 1500, 2000` starting at x=120.
- **Target line:** dashed (6/4) `#888` 1px horizontal at y=60, labeled "Target" in 11px `#888` at the right edge.
- **Step command:** dashed (3/2) `#aaa` 1px path from baseline y=160 stepping up to the target at x=120.
- **Response curves (2.5px, classic underdamped second-order response 1 − e^(−ζωt)(cos ω_d t + (ζ/√(1−ζ²))·sin ω_d t) over 2 seconds, amplitude 100px from baseline 160 to target 60):**
  - 0 kg (empty): green `#27ae60`, ωn=12, ζ=0.6 — fast, well damped.
  - 2 kg: yellow-orange `#f39c12`, ωn=8, ζ=0.35 — slower, some overshoot.
  - 5 kg: red `#e74c3c`, ωn=5, ζ=0.2 — sluggish with large overshoot and oscillation.
- **Annotation (10px red, at (230,32) with a small pointer line):** "Overshoot & oscillation".
- **Legend (12px, at x=430, y=130 in 16px steps):** color line swatches + "0 kg (empty)", "2 kg", "5 kg".

## Regeneration instructions

- **Layout:** standard domains detail page. h1, `.subtitle` paragraph, then one `h2` per pitfall (six total) followed by a `.obj-table` (full-width, border-collapse) with a single `<tr>`: left `<td>` (40%) holding `.obj-title` div + a `<ul>` of labeled bullets (each `<li>` starting with a bold `<strong>` label), right `<td>` (60%, centered) holding the canvas. No nav bar, no back/home links, no thead, no badges.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6. h1 1.8em `#1a5276`. h2 1.4em `#1a5276` with 2px bottom border `#2980b9`, padding-bottom 8px, margin 40px 0 15px. `.subtitle` `#666` 1.05em. `ul` 0.9em `#333`. `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px; even rows background `#fafcfe`. `.obj-title` 1.05em weight 600 `#1a5276`. `strong` `#1a5276`. `.philosophy` style defined but unused.
- **Canvases:** each 720×200 with inline `style="width:720px;height:200px"`; shared `setupCanvas(id)` sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Chart titles 17px system font. Random trajectories use seeded LCG generators (multiplier 1664525, increment 1013904223) for determinism.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, yellow-orange `#f39c12`, purple `#8e44ad`/`#9b59b6`, secondary blues `#2980b9`/`#3498db`, teal `#1abc9c`, bright green `#2ecc71`, dark red `#c0392b`, gray text `#555`/`#666`/`#888`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
