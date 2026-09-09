# Robotics — Domain-Specific Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: labeled bullets left ~40%, canvas right ~60%)
**HTML title tag:** Robotics — Domain-Specific Pitfalls

## Sim-to-Real Transfer Gap

**Obj-title:** Sim-to-Real Transfer Gap

- **Perfect physics:** The simulator has no friction variation, mechanical wear, or cable sag.
- **Clean sensors:** It also has no non-Gaussian sensor noise, so a policy there hits 100% task success.
- **Reality:** Deploy that same policy on real hardware and performance drops to roughly 60%.
- **Unmodeled dynamics:** Manufacturing tolerances and temperature-dependent friction differ per unit.
- **More sources:** Air resistance, cable tension, and backlash add dynamics the simulator never modeled.
- **Not a bug:** The gap is a mismatch between simplified physics and the physical world.
- **Partial fixes:** Domain randomization and system identification help but never fully close it.

### Visualization (canvas `canvas1`, 720×200)

Two-bar comparison of task success in simulation vs on real hardware, with a gap bracket.

- **Background:** `#fdfefe`. **Title (bold 17px `#1a5276`, centered):** "Performance: Simulation vs Real Hardware".
- **Bars:** width 80px, baseline y=175, max height 130px. Simulation bar at x=180: 100% full height, green `#27ae60`, value label "100%" bold green above, caption "Simulation" in `#1a5276` below. Real bar at x=420: 60% height, red `#e74c3c`, value label "60%" bold red above, caption "Real Robot" below.
- **Gap bracket:** orange `#e67e22` 2px bracket at x≈270-280 spanning from the top of the sim bar to the top of the real bar, with bold orange label "40% Gap" at x=288, vertically centered in the bracket.
- **Factor list (13px `#7f8c8d`, left-aligned at x=560, y=60-140 in 20px steps):** "Friction variation", "Mechanical wear", "Sensor noise", "Cable dynamics", "Thermal effects".

## Proprioception Noise/Drift

**Obj-title:** Proprioception Noise/Drift

- **Drift sources:** Joint encoders drift with thermal expansion; torque sensors saturate under high loads.
- **Creep:** Strain gauges creep under sustained load, so the zero point moves while the robot works.
- **IMU bias:** Gyroscope bias accumulates at roughly 0.01-0.1 degrees per second.
- **Compounding:** Orientation estimates can therefore be off by several degrees after 10 minutes.
- **Continuous degradation:** The robot's sense of its own body degrades steadily over a session.
- **Recalibration:** Only homing, known contact points, or visual-inertial fusion hold the estimate steady.
- **Consequence:** A policy relying on accurate joint positions fails as the session progresses.
- **No trigger:** This happens with no external disturbance at all — nothing visibly changes.

### Visualization (canvas `canvas2`, 720×200)

Line chart of sensor readings drifting away from a flat true value over session time.

- **Background:** `#fdfefe`. **Title (bold 17px `#1a5276`, centered at y=22):** "Sensor Reading Drift Over Session Time".
- **Plot area:** margins left 70, right 30, top 40, bottom 35. Axes `#2c3e50` 1.5px L-shape. Light vertical gridlines `#ecf0f1` at 6 tick positions.
- **Axis labels (14px `#2c3e50`):** "Session Time (minutes)" centered below; "Angle (deg)" rotated -90° on the left. X ticks (12px): `0, 2, 4, 6, 8, 10`.
- **True value:** flat green `#27ae60` 2.5px horizontal line at plot mid-height.
- **Encoder reading:** red `#e74c3c` 2.5px line, 100 points, seeded pseudo-random walk (seed 42, LCG 1664525/1013904223) drifting steadily upward from the true line (drift increment ~0.4-0.55 per step, scaled 0.7, noise ±2) — ends several degrees above true.
- **IMU estimate:** purple `#8e44ad` 2px dashed (dash 6/4) line, seeded random walk (seed 123) drifting below the true line at a slower rate (drift ~0.25 + sinusoidal modulation, scaled 0.5, noise ±1.5).
- **Legend (13px, upper right, 20×3 swatches):** green "True Value", red "Encoder Reading", purple "IMU Estimate".

## Actuator Wear Changes Dynamics

**Obj-title:** Actuator Wear Changes Dynamics

- **The aging:** By month 6 backlash has grown from 0.1 to 0.3 degrees on the same joint.
- **By month 12:** Friction has shifted, the torque-speed curve has flattened, dead-band zones have grown.
- **Changing plant:** The robot's physics literally change as it ages — the hardware is a moving target.
- **Not a bug:** This is neither a software defect nor a sensor issue, so debugging code finds nothing.
- **Silent failure:** A model that worked perfectly three months ago begins to fail as properties drift.
- **The fix:** Adaptive control or periodic re-identification is essential to track the changing plant.
- **Blind spot:** It is rarely implemented, since most research runs on brand-new hardware.

### Visualization (canvas `canvas3`, 720×200)

Three motor torque-speed curves at different hardware ages, showing decay and growing dead-band.

- **Background:** `#fdfefe`. **Title (bold 17px `#1a5276`, centered at y=22):** "Motor Torque Curves at Different Ages".
- **Plot area:** margins left 80, right 30, top 40, bottom 35. Axes `#2c3e50` 1.5px. Axis labels (14px): "Motor Speed (RPM)" below, "Torque (Nm)" rotated left. X ticks (12px): `0, 500, 1000, 1500, 2000, 2500`.
- **Curves:** each is torque = (1 − decay·t²) scaled to 85% plot height, ramping from 0 within an initial dead-band fraction of the speed range:
  - Month 1 (new): green `#27ae60`, solid 2.5px, decay 0.7, deadband 0.02.
  - Month 6 (worn): orange `#e67e22`, dashed 8/4, decay 0.85, deadband 0.06.
  - Month 12 (degraded): red `#e74c3c`, dashed 4/4, decay 1.05, deadband 0.12.
- **Legend (13px, upper left inside plot):** line swatches + labels "Month 1 (new)" green, "Month 6 (worn)" orange, "Month 12 (degraded)" red.
- **Annotation (italic 12px `#7f8c8d`, bottom right of plot):** "Increased deadband + reduced peak torque".

## Environment Non-Stationarity

**Obj-title:** Environment Non-Stationarity

- **Constant change:** The table was bumped 2cm, the light color temperature shifted overnight.
- **More drift:** A new object appeared on the bench, and the freshly mopped floor is more slippery.
- **Accumulation:** Between Monday's data collection and Friday's deployment, dozens of changes stack up.
- **Fragile policies:** Position-memorizing policies fail the moment anything in the scene shifts.
- **Vision and grasp:** Visual policies break with lighting; grasp policies fail when a new object occludes.
- **No reset:** Unlike simulation, reality has no reset button to restore a known world state.
- **Compounding variance:** Every episode starts slightly differently, and that variance grows over weeks.

### Visualization (canvas `canvas4`, 720×200)

Expanding variance-envelope chart: state deviation growing across episodes around a flat expected state.

- **Background:** `#fdfefe`. **Title (bold 17px `#1a5276`, centered at y=22):** "Environment State Variance Across Episodes".
- **Plot area:** margins left 70, right 30, top 42, bottom 35. Axes `#2c3e50` 1.5px. Labels (14px): "Episode Number" below, "State Deviation" rotated left.
- **Envelope:** symmetric cone around plot mid-height, spreading linearly with episode index from 0 to ±42% of plot height across 50 episodes; fill `rgba(231,76,60,0.15)`, borders red `#e74c3c` 1.5px dashed (4/3) on both upper and lower edges.
- **Episode dots:** 50 blue `#2980b9` dots (radius 3), one per episode, at seeded-random (seed 99) vertical positions within ±38% of the growing envelope.
- **Expected state:** green `#27ae60` 2px horizontal line at center.
- **Annotations (12px):** "Expected state" in green at left just above the center line; "Variance envelope" in red at upper right.

## Human-in-the-Loop Correction Bias

**Obj-title:** Human-in-the-Loop Correction Bias

- **Biased teachers:** Human corrections are conservative and style-specific — always approach from the left.
- **Reaction lag:** Corrections arrive 200-400ms late, and operators optimize comfort over efficiency.
- **Inherited flaws:** Imitation learning or DAgger-style training bakes every suboptimality into the policy.
- **Multimodal mess:** Different operators introduce different biases, creating inconsistent training data.
- **Worse than any:** The policy averages incompatible strategies, beating no single individual operator.

### Visualization (canvas `canvas5`, 720×200)

Trajectory diagram: three start-to-goal paths around an obstacle.

- **Background:** `#fdfefe`. **Title (bold 17px `#1a5276`, centered at y=22):** "Trajectory Comparison: Optimal vs Human vs Learned".
- **Markers:** black `#2c3e50` dots (radius 6) at START (left, x=margin+20, mid-height) and GOAL (right), with bold 14px labels "START" and "GOAL" below.
- **Obstacle:** gray circle (radius 25, fill `rgba(149,165,166,0.4)`, stroke `#7f8c8d` 2px) at 50% width / 40% height, labeled "obstacle" in 11px `#7f8c8d`.
- **Paths (bezier curves from start to goal, all passing below the obstacle):**
  - Optimal: green `#27ae60`, solid 2.5px, efficient curve passing close under the obstacle (control points ~65-70% height).
  - Human-corrected: orange `#e67e22`, dashed 8/4, wide conservative arc far below (control points ~90-95% height).
  - Learned policy: purple `#8e44ad`, dashed 4/3, in between (control points ~78-83% height).
- **Legend (13px, upper right):** line swatches + "Optimal" green, "Human-corrected" orange, "Learned policy" purple.

## Multi-Modal Sensor Alignment

**Obj-title:** Multi-Modal Sensor Alignment

- **Different clocks:** Camera at 30fps, tactile at 1000Hz, encoders at 500Hz, force-torque at 200Hz.
- **Per-stream skew:** Each carries its own processing delay, USB jitter, and independent time source.
- **The cost:** A 10ms camera/force misalignment pairs the wrong visual state with the wrong contact force.
- **In distance:** At 1m/s hand speed that 10ms gap is a full 1cm of position error.
- **Weak fixes:** Hardware synchronization via trigger lines or PTP clocks helps, but only partly.
- **Still unreliable:** Software timestamps remain untrustworthy even with synchronized hardware.
- **Common mistake:** Learning pipelines naively stack the latest reading from each sensor together.
- **The assumption:** That stack pretends four readings from different moments share one instant.

### Visualization (canvas `canvas6`, 720×200)

Timeline diagram of four sensor streams sampled at different rates, with misalignment zones highlighted.

- **Background:** `#fdfefe`. **Title (bold 17px `#1a5276`, centered at y=22):** "Multi-Modal Sensor Streams with Temporal Misalignment".
- **Time axis:** horizontal `#2c3e50` line at bottom of plot (margins: left 90, right 30, top 38, bottom 25), ticks every 10ms from 0 to 100 with 11px labels; axis caption "Time (ms)" (13px).
- **Sensor rows (right-aligned 12px colored labels at left, tick marks per sample along each row, faint horizontal guide line at 30% alpha):**
  - "Camera (30fps)" blue `#2980b9`, sample interval 33ms, offset 15ms, row y=top+15.
  - "Joint (500Hz)" green `#27ae60`, interval 2ms, offset 1ms, row y=top+50.
  - "Tactile (1kHz)" purple `#8e44ad`, interval 1ms, offset 0, row y=top+85.
  - "F/T (200Hz)" orange `#e67e22`, interval 5ms, offset 3ms, row y=top+120.
- **Misalignment highlights:** two vertical bands at t=33ms and t=66ms — 16px-wide fill `rgba(231,76,60,0.12)` spanning full plot height, plus red `#e74c3c` dashed (3/2) center line; each annotated in bold 11px red with two lines "~10ms" / "misalign".

## Regeneration instructions

- **Layout:** standard domains detail page. h1, then one `h2` per pitfall (six total) followed by a `.obj-table` (full-width, border-collapse) with a single `<tr>`: left `<td>` (40%) holding `.obj-title` div + a `<ul>` of labeled bullets (each `<li>` starting with a bold `<strong>` label), right `<td>` (60%, centered) holding the canvas. No subtitle paragraph on this page. No nav bar, no back/home links, no thead, no badges.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6. h1 1.8em `#1a5276`. h2 1.4em `#1a5276` with 2px bottom border `#2980b9`, padding-bottom 8px, margin 40px 0 15px. `ul` 0.9em `#333`, margin 8px 0 8px 20px. `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px; even rows background `#fafcfe`. `.obj-title` 1.05em weight 600 `#1a5276`. `strong` `#1a5276`. `.subtitle` and `.philosophy` styles defined but unused.
- **Canvases:** each 720×200 with inline `style="width:720px;height:200px"`; shared `setupCanvas(id)` sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates, and presets 17px system chart font. Random-looking series use seeded LCG generators (multiplier 1664525, increment 1013904223) so charts are deterministic.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, axis dark `#2c3e50`, muted gray `#7f8c8d`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
