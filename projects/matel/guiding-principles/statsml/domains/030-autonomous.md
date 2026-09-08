# Autonomous Systems / Robotics

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 30. Autonomous Systems / Robotics — Domain Pitfalls

**Subtitle:** Data pitfalls unique to self-driving vehicles, drones, and robotic systems operating in uncontrolled environments.

## Callout (philosophy box)

Autonomous systems face a fundamental challenge: they must operate safely in an open world using models trained on a closed dataset. The real world is infinitely varied, sensors are imperfect, and the cost of failure can be fatal. Every pitfall here stems from this gap between training and deployment reality.

## Sim-to-Real Gap

**Simulator performance does not transfer to reality**

- Simulator: perfect roads, consistent lighting, no weather variation, predictable physics
- Real world: potholes, rain, sun glare, unexpected objects, variable surfaces
- Models trained in simulation achieve near-perfect scores that collapse in deployment
- Performance gap typically ranges from 30-50% degradation
- Domain randomization helps but cannot fully close the gap

**Impact:** Teams celebrate 98% simulator accuracy, then face 60% real-world performance. The gap is not a bug — it reflects fundamental differences between synthetic and real environments that no amount of simulation fidelity fully resolves.

### Visualization (canvas `c1`, 720×300)

Two horizontal accuracy bars (simulator vs. real world) with a gap arrow and a list of real-world factors.

- **Title (bold 17px `#1a5276`, centered, y=25):** "Sim-to-Real Performance Gap".
- **Simulator bar:** label "Simulator" (15px `#333`) above; track 200×50 at (80, 60) fill `#e8f5e9`; fill portion 98% in `#27ae60`; centered bold 17px white label "98% Accuracy".
- **Real World bar:** label "Real World" above; track 200×50 at (80, 170) fill `#ffebee`; fill portion 62% in `#e74c3c`; bold 17px white label "62% Accuracy".
- **Gap arrow:** vertical dashed red double-headed arrow (`#e74c3c`, width 2, dash 5/3) between the two bar midlines at x=(bar right + 40); to its right, bold 15px red two-line label: "30-50%" / "degradation".
- **Real-world factors list** (starting at x=450, y=80): heading 14px `#555` "Real-world factors:", then four rows of 22px emoji icon + 13px `#555` label at 30px spacing: 🌧 "Rain", ☀ "Sun glare" (`#f39c12` icon), ⚠ "Potholes", ❓ "Unexpected objects".
- **Bottom note (italic 13px `#888`, centered):** "Sim performance ≠ Real performance".

## Long-Tail Edge Cases

**Rare scenarios dominate failure modes**

- 99.9% of driving is routine: lane following, normal traffic, standard intersections
- The 0.1% includes: construction zones, animals crossing, unusual vehicles, fallen debris
- Each edge case is unique — never appears in training data because it happened once
- Standard datasets cannot capture infinite variety of rare events
- These rare scenarios are precisely where accidents and failures occur

**Impact:** You cannot test for what you have never seen. The long tail is where fatalities happen, yet it is statistically invisible in training data and aggregate metrics.

### Visualization (canvas `c2`, 720×300)

Long-tail frequency bar chart: one tall routine bar followed by an exponentially decaying tail of rare-scenario bars.

- **Title (bold 17px `#1a5276`, centered, y=25):** "Long-Tail Distribution of Driving Scenarios".
- **Axes:** L-shaped `#333` axes, origin at x=60, baseline at height−50, chart height 180. Y-axis label (rotated, 13px `#555`): "Frequency". X-axis label (13px `#555`): "Scenario type (sorted by frequency)".
- **Routine bar:** 80px wide green bar (`#27ae60`) at 95% of chart height, white bold 14px labels inside: "99.9%" / "Routine"; small green 13px label "Safe" above it.
- **Tail:** 25 bars decaying exponentially (height = chartH × 0.15 × e^(−0.2i), minimum 3px); first 5 bars `#f39c12`, the rest `#e74c3c`.
- **Crash annotation:** dashed red horizontal line (`#e74c3c`, width 1.5, dash 4/3) over the tail region at 40% chart height; red dots (radius 5) at every 3rd tail bar from index 6; bold 14px red label above: "← Where crashes happen →".

## Sensor Fusion Timestamp Alignment

**Multi-sensor data arrives at different times**

- Camera frame: 30ms old by the time it is processed
- Lidar scan: 50ms old due to sweep time and computation
- Radar return: 10ms old, fastest but lowest resolution
- At 60mph, 30ms of latency = 2.6 feet of object movement
- Naive fusion places the same object at three different positions

**Impact:** Without precise temporal alignment, sensor fusion creates ghost objects, mislocates real ones, or fails to associate detections across modalities. A 2.6-foot error at highway speed can mean the difference between stopping and collision.

### Visualization (canvas `c3`, 720×300)

Three horizontal sensor-lag timelines showing reported vs. true object positions.

- **Title (bold 17px `#1a5276`, centered, y=25):** "Sensor Timestamp Misalignment at 60 mph".
- **Time axis:** dashed gray line (`#aaa`, dash 3/3) near the bottom from x=180 to x=width−40, with 12px `#666` labels: "50ms ago" (left end), "30ms ago" (40%), "10ms ago" (80%), "NOW" (right end).
- **True position line:** solid vertical `#333` line (width 2) at the right end (NOW), labeled bold 13px "TRUE Position" at top.
- **Sensor rows** (each: right-aligned bold 14px colored name label, light `#f0f0f0` track bar 24px tall, filled 10px-radius dot at the reported position = lag/50 of the timeline back from NOW, colored dashed line from dot to the true-position line, 12px error label above the dashed line "X.X ft error" where error = lag/30 × 2.6):
  - "Radar (10ms)" — `#27ae60`, y=75 — "0.9 ft error"
  - "Camera (30ms)" — `#2980b9`, y=140 — "2.6 ft error"
  - "Lidar (50ms)" — `#8e44ad`, y=205 — "4.3 ft error"
- **Bottom annotation (italic 13px `#e74c3c`, centered):** "At 60 mph: 30ms = 2.6 ft of movement. Fusion must time-align all sensors."

## Safety-Critical Asymmetric Cost

**False negatives and false positives have vastly different consequences**

- False negative (missed pedestrian): potential fatality — irreversible
- False positive (phantom braking): brief inconvenience — recoverable
- Cost ratio is effectively infinity to one
- Must operate at extreme recall, accepting many false positives
- Standard accuracy, F1, or balanced metrics are dangerously wrong here

**Impact:** Optimizing for standard metrics kills people. The system must be tuned to never miss a real obstacle, even if it means braking for shadows, plastic bags, and sensor noise dozens of times per trip.

### Visualization (canvas `c4`, 720×300)

2×2 confusion-matrix diagram with color-coded cost cells.

- **Title (bold 17px `#1a5276`, centered, y=25):** "Asymmetric Cost: Confusion Matrix".
- **Matrix:** 2×2 grid of 200×90 cells starting at (100, 55). Column headers (bold 14px `#555`): "Predicted: No Object", "Predicted: Object". Row headers (rotated): "Actual: Object" (top), "Actual: No Object" (bottom).
- **Cells:**
  - Top-left (FN): fill `#e74c3c`, white text — bold 18px "FALSE NEGATIVE", bold 22px "☠ FATALITY", 13px "Cost: ∞".
  - Top-right (TP): fill `#27ae60`, white text — bold 16px "TRUE POSITIVE", 14px "✓ Correct detection", "Cost: 0".
  - Bottom-left (TN): fill `#dff0d8`, `#333` text — bold 16px "TRUE NEGATIVE", 14px "✓ Correct clear", "Cost: 0".
  - Bottom-right (FP): fill `#fff3cd`, `#856404` text — bold 16px "FALSE POSITIVE", 14px "Phantom braking", "Cost: Minor".
- **Bottom annotations (centered):** bold 17px red "Cost ratio:  ∞ : 1  (FN vs FP)"; 12px `#888` "Standard metrics optimize for balance — deadly wrong here".

## Distributional Shift in Deployment

**Training geography and culture do not generalize**

- Trained in California: sunshine, wide roads, grid street layout, predictable drivers
- Deployed in Boston: snow, narrow streets, irregular intersections, aggressive drivers
- Or trained on US driving behavior, deployed in India: completely different driving culture
- Lane markings, sign styles, road rules, pedestrian behavior all shift
- Model confidence stays high even when predictions are wrong

**Impact:** A model cannot know what it has not seen. Deploying across geographies without retraining creates silent failures — the model confidently makes wrong decisions because the world no longer matches its training distribution.

### Visualization (canvas `c5`, 720×300)

Two side-by-side geography boxes (training vs. deployment) with a deploy arrow and confidence bars.

- **Title (bold 17px `#1a5276`, centered, y=25):** "Distributional Shift: Training vs Deployment".
- **Left box** (260×170 at (50, 50), border 2px `#27ae60`): heading bold 15px green "TRAINING: California"; interior sketch of grid roads (gray `#bbb` lines: 4 vertical, 3 horizontal); orange sun disc (`#f39c12`, radius 15) in top-right; full-width green confidence bar at bottom (10px tall) with bold 9px white label "95% Confidence".
- **Right box** (260×170 at (380, 50), border 2px `#e74c3c`): heading bold 15px red "DEPLOYMENT: Boston"; interior sketch of winding bezier roads plus one narrow irregular diagonal intersection (gray `#bbb`); blue snowflake "❄" (`#5dade2`, 24px) top-right; confidence bar filled 63% red with the remainder `#f0f0f0`, bold 9px white label "60% Confidence".
- **Deploy arrow:** red horizontal arrow (`#e74c3c`, width 2) between the boxes at mid-height, 12px red label "Deploy" above it.
- **Bottom annotations (centered):** bold 15px red "95% → 60% confidence drop"; italic 13px `#888` "Model confidence stays high even when predictions are wrong in new geography".

## Rare but Critical Sensor Failures

**The system must detect when its own sensors break**

- Lidar gets wet or dirty: returns garbage point clouds
- Camera lens cracked or fogged: partial or full occlusion
- Radar calibration drifts: reports phantom objects or misses real ones
- Model must detect its OWN sensor failure and switch to degraded mode
- Training data almost never includes examples of broken sensor input

**Impact:** A model that cannot recognize bad input will confidently act on corrupted data. Without self-diagnosis, a single sensor failure cascades into catastrophic decisions because the system trusts garbage as if it were truth.

### Visualization (canvas `c6`, 720×300)

Three-phase signal timeline: clean signal, corrupted noise, degraded-mode signal, with a quality meter below.

- **Title (bold 17px `#1a5276`, centered, y=25):** "Sensor Failure Detection & Degraded Mode".
- **Timeline:** spans x=50 to x=width−50 around y=130; failure point at 45% of the span, detection point at 65%.
- **Phase labels (bold 13px, y=50):** "Normal Operation" in `#27ae60` (over first segment), "Sensor Corrupted" in `#e74c3c` (middle), "Degraded Mode" in `#f39c12` (last).
- **Signals:** first segment — smooth green sine wave (`#27ae60`, width 2, amplitude ~25px plus a slow 10px component); middle segment — red seeded pseudo-random noise (`#e74c3c`, width 2, amplitude ±45px); last segment — dashed orange low-amplitude sine (`#f39c12`, width 2, dash 5/3, amplitude 10px).
- **Dividers:** vertical dashed lines (dash 4/4, width 2) at the failure point (red) and detection point (orange), from y=55 to y=180.
- **Event labels (12px, centered below signal):** red "⚠ Sensor fails" at the failure point; bold orange "✓ Failure detected" / "Switch to backup" at the detection point.
- **Decision box:** 100×35 box under the detection point, fill `#fff3cd`, border 2px `#f39c12`; text `#856404`: bold 11px "SELF-DIAGNOSIS", 11px "Engage fallback".
- **Signal quality meter** (15px-tall stacked bar near the bottom, prefixed by 12px `#555` label "Signal Quality:"): green segment labeled "GOOD" (`#27ae60`), red segment "GARBAGE" (`#e74c3c`), orange segment "REDUCED" (`#f39c12`), 10px white labels.
- **Bottom note (italic 13px `#888`, centered):** "Training data rarely includes \"my own sensor is broken\" — yet the system must recognize it".

## Regeneration instructions

- **Layout:** domains detail-page template — h1, `.subtitle`, one `.philosophy` callout, then one `<h2 id="...">` per pitfall followed by a single-row `.obj-table`: left `<td>` (45%) with `.obj-title` div, `<ul>` bullets and an **Impact:** paragraph; right `<td>` (55%, centered) holding one canvas. No thead, no nav, no badges, no numbering on h2s.
- **Section ids:** `sim-to-real`, `long-tail`, `sensor-fusion`, `asymmetric-cost`, `distributional-shift`, `sensor-failure`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px `#2980b9`, padding 12px 16px, 0.9em; `.obj-table` full width, collapsed borders, cells `1px solid #e0e0e0` with 20px 24px padding, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`.
- **Canvases:** each declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Default chart font 17px system sans. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`, gray text `#666`/`#555`/`#333`/`#888`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
