# Manufacturing Domain: Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Manufacturing Domain - Data Pitfalls in Statistical ML

**Subtitle:** Drifting sensors, censored failures, machine-specific baselines, and aliased signals undermine ML built on factory data.

## Sensor Drift

**Obj-title:** Sensor Drift

- Sensors degrade over time - readings gradually shift from true values
- A +2°C drift over 6 months looks like a process change but is pure measurement error
- Models trained on drifted data learn the wrong baseline
- Calibration schedules exist but are often delayed or skipped
- Multiple sensors drifting independently create phantom correlations

**Example:** A thermocouple in a furnace drifts +0.3°C/month over 6 months. The ML model flags "process trending hot" and recommends reducing power. In reality, the furnace is running perfectly - the sensor is lying. Post-calibration, the "anomaly" disappears instantly.

### Visualization (canvas `canvas1`, 720×240)

Time-series line chart: a noisy sensor reading drifting upward over 6 months vs the constant true temperature.

- **Data:** 180 daily readings; true process temperature constant at 850°C; drift rate +2°C over 180 days (linear); noise from a repeating pattern `[0.12, -0.08, 0.15, -0.11, 0.09, -0.14, 0.07, -0.05, 0.13, -0.10]` modulated by `(1 + sin(i × 0.3) × 0.5)`; reading = 850 + drift + noise.
- **Series:** sensor reading dark red `#c0392b` width 2; true temperature horizontal dashed green `#27ae60` line (dash 6/4, width 2) at 850°C.
- **Axes:** y-axis 849°C–853°C with 1°C labels and `#eee` gridlines; x-axis month labels Jan–Jun.
- **Calibration marker:** purple `#8e44ad` vertical line at the right edge dropping from the drifted level (~852) back to the true line, with a 5px purple dot at the true value.
- **Legend (17px, upper left):** dark red "Sensor Reading (drifting)"; green "True Process Temp (850°C)".
- **Annotation (purple 14px, right-aligned upper right):** "+2°C drift" / "(not real!)".

## Run-to-Failure Rarity

**Obj-title:** Run-to-Failure Rarity

- Preventive maintenance replaces parts BEFORE they fail
- You almost never observe actual failure - making failure prediction nearly impossible
- Survival analysis is right-censored by design - you only know "survived until replaced"
- Run-to-failure experiments are expensive and risky in production
- The failure mode you want to predict is the one you've successfully prevented from occurring

**Example:** A bearing replacement policy triggers at 10,000 hours. In 5 years of data (200 bearings), only 3 actually failed before replacement. You have 3 failure observations vs 197 censored observations - nowhere near enough to model the failure distribution reliably.

### Visualization (canvas `canvas2`, 720×240)

Timeline diagram of bearing lifecycles with preventive replacements, rare failures, and sawtooth degradation curves.

- **Chart title (17px `#1a5276`):** "Bearing Lifecycle Timeline".
- **Timeline:** horizontal axis line `#333` width 2 at y=130 from x=50 to x=700.
- **Preventive maintenance events:** blue `#3498db` downward triangles with vertical ticks at x = `[80, 155, 230, 305, 380, 455, 530, 605]` (8 events).
- **Actual failures:** dark red `#c0392b` X marks (width 3) below the timeline at x = `[265, 490, 670]` (3 events).
- **Degradation curves:** orange `#e67e22` quadratic sawtooth segments rising from the timeline up to ~40px before each PM event, resetting at each replacement — they never reach the threshold.
- **Failure threshold:** horizontal dashed dark red line (dash 5/5, width 1.5) at y=65, labeled right-aligned in dark red 14px: "Failure Threshold".
- **Legend (13px):** blue "▼ Preventive Replacement (8 events)"; dark red "✕ Actual Failure (only 3 events!)"; orange "— Degradation (never reaches threshold in PM)".
- **Stats box:** blue `#2980b9` outlined rectangle (bottom right) containing `#1a5276` 13px text: "3 failures / 200 bearings = 1.5% observed".

## Multi-Machine Generalization

**Obj-title:** Multi-Machine Generalization

- Same model machine, same manufacturer, same year - but different behavior
- Each machine develops its own "personality" from wear patterns and calibration
- A model trained on Machine A fails silently on Machine B
- Transfer learning between machines requires domain adaptation
- Fleet-level models average out important machine-specific signals

**Example:** Three identical CNC mills from the same batch show vibration baselines of 2.1, 2.8, and 3.4 mm/s respectively. A threshold of 4.0 mm/s (trained on Machine A) gives Machine C only 0.6 mm/s of headroom before alarm - a 70% reduction in useful detection range.

### Visualization (canvas `canvas3`, 720×240)

Violin-style plot: three machines' vibration baseline distributions against a single alarm threshold, with headroom arrows.

- **Title (top center, 17px `#1a5276`):** "Same Model, Different Baselines".
- **Machines (violin-shaped gaussian spreads, width ±40px, centered at the baseline; 5px baseline dot; fill = machine color at 20% alpha (`color + '33'`), stroke = machine color width 2):**
  - Machine A — baseline 2.1 mm/s, blue `#2980b9`, x=150.
  - Machine B — baseline 2.8 mm/s, green `#27ae60`, x=370.
  - Machine C — baseline 3.4 mm/s, orange `#e67e22`, x=570.
- **Threshold:** horizontal dashed dark red `#c0392b` line (dash 6/4, width 2) at 4.0 mm/s, labeled in dark red 13px: "Alarm Threshold (4.0 mm/s)".
- **Headroom arrows:** double-headed vertical arrow in each machine's color from its baseline to the threshold, labeled at midpoint with the headroom value: "1.9 mm/s", "1.2 mm/s", "0.6 mm/s".
- **Axes:** y-axis 0.0–5.0 mm/s with labels every 1.0 mm/s and `#eee` gridlines; machine names 14px below the axis, baselines "(2.1 mm/s)" etc. 12px beneath.

## High-Frequency Aliasing

**Obj-title:** High-Frequency Aliasing

- Nyquist theorem: must sample at 2x the signal frequency to avoid aliasing
- A 50Hz vibration sampled at 80Hz produces a phantom 30Hz signal
- The aliased signal is indistinguishable from a real 30Hz component
- Anti-aliasing filters must be applied BEFORE digitization (analog domain)
- Changing sample rates mid-project creates incomparable datasets

**Example:** A motor runs at 3000 RPM (50Hz fundamental). Vibration sensor samples at 80 samples/sec. The 50Hz signal aliases to 80-50=30Hz. Engineers "discover" a 30Hz resonance that doesn't exist - it's a ghost from undersampling. Increasing sample rate to 200Hz makes it vanish.

### Visualization (canvas `canvas4`, 720×240)

Two-row aliasing demonstration: true 50 Hz sine wave with 80 Hz sample points (top), reconstructed 30 Hz ghost wave through the same points (bottom).

- **Top wave:** true signal sin(2π × 50 × t) over a 100 ms window, blue `#2980b9` width 2, amplitude 40px around y=55, with a gray `#333` midline at y=110.
- **Sample points:** 5px dark red `#c0392b` dots at 80 Hz sample instants (9 samples over 100 ms) on the true wave.
- **Bottom wave:** aliased signal sin(2π × 30 × t), dashed dark red (dash 4/3, width 2), amplitude 35px around y=180, with matching 3px sample dots; faint dashed gray `#999` vertical connectors (dash 2/2, width 0.5) linking each sample point on the true wave to the same value on the alias.
- **Labels (17px):** blue "True Signal: 50 Hz" top left; dark red "Aliased "Ghost": 30 Hz" bottom left; right-aligned `#555` 13px: "Sample rate: 80 Hz" (top) and "Alias = |80 - 50| = 30 Hz" (bottom).
- **Divider label (dark red 12px, center):** "● Sample points (same values on both signals!)".

## Physics Constraints as Regularization

**Obj-title:** Physics Constraints as Regularization

- Physical laws provide hard bounds that ML models should never violate
- Temperature cannot be below -273.15°C (absolute zero)
- Pressure in a sealed vessel is always ≥ 0 (gauge) or ≥ 0 (absolute)
- Conservation laws (mass, energy) constrain valid predictions
- Physics violations in predictions are guaranteed errors - free anomaly detection

**Example:** A neural network predicting reactor temperature outputs -285°C during an extrapolation. Any physicist knows this is impossible. Using physics constraints as regularization would have caught this. Similarly, a mass-balance model predicting output > input violates conservation.

### Visualization (canvas `canvas5`, 720×240)

Two side-by-side panels: model predictions crossing physical bounds for temperature (left) and pressure (right).

- **Left panel — "Temperature" (title 17px `#1a5276`):**
  - Prediction sequence `[200, 150, 80, 20, -50, -150, -250, -285, -300]` °C plotted as a blue `#2980b9` line (width 2) with 4px blue dots; y range −350 to 250.
  - Absolute-zero bound: horizontal dashed dark red `#c0392b` line (dash 5/3, width 2) at −273.15°C, labeled "-273.15°C (Absolute Zero)" in dark red 14px; region below shaded `rgba(192,57,43,0.1)` with 12px label "IMPOSSIBLE".
  - Predictions below −273.15°C marked with 6px dark red dots overlaid with X marks.
- **Right panel — "Pressure" (title 17px `#1a5276`):**
  - Prediction sequence `[500, 420, 350, 250, 150, 50, -20, -80, -50]` kPa plotted as a green `#27ae60` line (width 2) with 4px green dots; y range −150 to 600.
  - Zero-pressure bound: horizontal dashed dark red line at 0 kPa, labeled "0 kPa (impossible below)" in dark red 14px; region below shaded `rgba(192,57,43,0.1)` with 12px label "IMPOSSIBLE".
  - Negative predictions marked with 6px dark red dots overlaid with X marks.
- **Bottom annotation (`#555` 13px, center):** "Physics violations = guaranteed prediction errors (free anomaly detection)".

## Regeneration instructions

- **Layout:** standard detail-page structure: h1, `.subtitle` paragraph, then per pitfall an `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a `.obj-table` with a single `<tr>`: left `<td>` (40%) contains `.obj-title` + `<ul>` bullets + `.example` callout, right `<td>` (60%, centered) contains the canvas. Even table rows have background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; bullets 0.9em `#333`; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Example callout:** on this page `.example` is styled as background `#eaf2f8`, padding 10px, border-radius 5px, italic, 0.9em (no left border). (A `.philosophy` class also exists in the stylesheet but is unused on this page.)
- **Canvas:** all canvases 720×240, declared with intrinsic `width`/`height` attributes and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), one IIFE per chart. Chart headline text uses a 17px -apple-system font.
- **Palette:** primary blue `#1a5276`, chart blues `#2980b9`/`#3498db`, green `#27ae60`, dark red `#c0392b`, orange `#e67e22`, purple `#8e44ad`, red `#e74c3c` (project palette, not used directly here), text `#333`/`#555`/`#666`.
